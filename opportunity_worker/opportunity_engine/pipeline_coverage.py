"""Whole-company-universe coverage diagnostics. A target never creates demand."""
import hashlib,json
from pathlib import Path
import duckdb
POLICY='whole-market-priced-coverage-v1'

def build(directory):
 r=Path(directory);m=json.loads((r/'manifest.json').read_text())
 required=('company_profiles','company_activity_baselines','company_matches','opportunities','demand_decisions','company_item_valuations','company_work_matches','opportunity_quality_reviews','company_notice_item_prices')
 if not all(n in m['tables'] for n in required):return
 c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='3GB';SET preserve_insertion_order=false");c.execute("SET temp_directory='"+str(r/'coverage_tmp').replace("'","''")+"'")
 for n in required:c.read_parquet(m['tables'][n]['path']).create_view(n)
 if 'company_reporting_lag_baselines' in m['tables']:
  c.read_parquet(m['tables']['company_reporting_lag_baselines']['path']).create_view('lag_baselines')
  c.execute("""CREATE TEMP VIEW benchmark_activity AS SELECT coalesce(a.company_id,l.company_id) company_id,a.* EXCLUDE(company_id),
  CASE WHEN l.lag_adjusted_annual_dod_net_obligations>0 THEN l.lag_adjusted_annual_net_obligations ELSE a.annual_net_obligations END benchmark_net,
  CASE WHEN l.lag_adjusted_annual_dod_net_obligations>0 THEN l.lag_adjusted_period_start ELSE a.period_start END benchmark_start,
  CASE WHEN l.lag_adjusted_annual_dod_net_obligations>0 THEN l.lag_adjusted_period_end ELSE a.period_end END benchmark_end
  FROM company_activity_baselines a FULL JOIN lag_baselines l USING(company_id)""")
 else:c.execute('CREATE TEMP VIEW benchmark_activity AS SELECT *,annual_net_obligations benchmark_net,period_start benchmark_start,period_end benchmark_end FROM company_activity_baselines')
 candidates_sql="""SELECT DISTINCT m.company_id,o.episode_id,o.record_hash,o.lane,o.customer_name,
 (o.lane IN ('published_item_procurement','source_item_forecasts') OR coalesce(q.category_override='items',false)) item_demand,
 coalesce(json_extract_string(o.value_inputs_json,'$.niin'),right(json_extract_string(o.value_inputs_json,'$.nsn'),9),right(json_extract_string(np.items_json,'$[0].nsn'),9)) niin,
 json_extract_string(o.value_inputs_json,'$.purchase_request_number') purchase_request,
 CASE WHEN json_array_length(np.items_json)=1 THEN coalesce(try_cast(json_extract_string(np.items_json,'$[0].government_reference_lot_scenario') AS DOUBLE),try_cast(json_extract_string(np.items_json,'$[0].supplier_average_lot_scenario') AS DOUBLE)) END notice_lot,
 CASE WHEN o.lane='published_item_procurement' AND try_cast(json_extract_string(o.value_inputs_json,'$.quantity') AS DOUBLE)>0 AND try_cast(json_extract_string(o.value_inputs_json,'$.observed_unit_price') AS DOUBLE)>0 AND nullif(json_extract_string(o.value_inputs_json,'$.request_unit'),'') IS NOT NULL THEN try_cast(json_extract_string(o.value_inputs_json,'$.quantity') AS DOUBLE)*try_cast(json_extract_string(o.value_inputs_json,'$.observed_unit_price') AS DOUBLE) END request_lot,
 o.value_median historical_or_source_value,
 CASE WHEN v.valuation_status='catalogue_EA_assumed_forecast_unit_not_stated' THEN coalesce(v.market_price_lot_scenario,v.government_reference_lot_scenario) END forecast_lot,
 coalesce(v.market_price_lot_scenario,v.government_reference_lot_scenario) raw_forecast_value
 FROM company_matches m JOIN opportunities o ON m.episode_id=o.episode_id AND m.opportunity_hash=o.record_hash JOIN demand_decisions d ON d.episode_id=o.episode_id
 LEFT JOIN company_work_matches cw ON cw.company_id=m.company_id AND cw.episode_id=o.episode_id AND cw.opportunity_hash=o.record_hash
 LEFT JOIN opportunity_quality_reviews q ON q.episode_id=o.episode_id AND q.record_hash=o.record_hash
 LEFT JOIN company_item_valuations v ON v.company_id=m.company_id AND v.episode_id=o.episode_id AND v.record_hash=o.record_hash
 LEFT JOIN company_notice_item_prices np ON np.company_id=m.company_id AND np.episode_id=o.episode_id AND np.record_hash=o.record_hash
 WHERE hash(m.company_id)%32=COMPANY_PARTITION AND d.stage NOT IN ('resolved_or_elapsed','evidence_gap','planning_context','historical_watch','repeat_demand_inference') AND coalesce(cw.review_category,'')<>'market_context' AND (o.lane<>'source_item_forecasts' OR coalesce(v.valuation_status,'')<>'negative_source_quantity')
 QUALIFY row_number() OVER(PARTITION BY m.company_id,CASE WHEN o.source_name='sam_notices' AND length(o.native_identifier)>4 THEN concat_ws('|',o.source_name,upper(o.customer_name),regexp_replace(upper(o.native_identifier),'-00$',''),regexp_replace(upper(o.title),'[^A-Z0-9]','','g')) ELSE o.episode_id END ORDER BY o.notice_posted_date DESC NULLS LAST,o.episode_id)=1"""
 # Keep each company's full notice families together while bounding the wide
 # joins/window state. Partitioning never truncates the company universe.
 for partition in range(32):
  c.execute(('CREATE TEMP TABLE candidates AS ' if partition==0 else 'INSERT INTO candidates ')+candidates_sql.replace('COMPANY_PARTITION',str(partition)))
  print('Company coverage partition',partition+1,'/ 32',flush=True)
 c.execute("""CREATE TEMP TABLE request_groups AS SELECT company_id,
 CASE WHEN nullif(purchase_request,'') IS NOT NULL AND regexp_full_match(coalesce(niin,''),'[0-9]{9}') AND nullif(customer_name,'') IS NOT NULL THEN 'pr|'||regexp_replace(upper(customer_name),'[^A-Z0-9]','','g')||'|'||purchase_request||'|'||niin
 WHEN regexp_full_match(coalesce(niin,''),'[0-9]{9}') AND nullif(customer_name,'') IS NOT NULL THEN 'item|'||regexp_replace(upper(customer_name),'[^A-Z0-9]','','g')||'|'||niin ELSE episode_id END requirement_key,
 max(coalesce(notice_lot,request_lot)) value_usd FROM candidates WHERE coalesce(notice_lot,request_lot)>0 GROUP BY company_id,requirement_key""")
 c.execute('CREATE TEMP TABLE published AS SELECT company_id,count(*)::BIGINT priced_published_requirements,sum(value_usd) priced_published_demand FROM request_groups GROUP BY company_id')
 c.execute('CREATE TEMP TABLE forecast AS SELECT company_id,count(*)::BIGINT priced_forecast_items,sum(value_usd) full_horizon_forecast_scenario FROM (SELECT company_id,niin,max(forecast_lot) value_usd FROM candidates WHERE forecast_lot>0 GROUP BY company_id,niin) GROUP BY company_id')
 c.execute("""CREATE TEMP TABLE counts AS SELECT c.company_id,count(*)::BIGINT matched_opportunities,count(*) FILTER(WHERE NOT item_demand)::BIGINT work_opportunities,count(*) FILTER(WHERE item_demand)::BIGINT item_opportunities,
 count(*) FILTER(WHERE coalesce(notice_lot,request_lot,historical_or_source_value,raw_forecast_value,0)>=CASE WHEN a.benchmark_net>=100000000 THEN greatest(1000000,a.benchmark_net*0.01) ELSE 1000000 END)::BIGINT prospects_with_material_value_guide,
 count(*) FILTER(WHERE NOT item_demand AND coalesce(notice_lot,request_lot) IS NULL)::BIGINT work_without_current_price
 FROM candidates c LEFT JOIN benchmark_activity a ON a.company_id=c.company_id GROUP BY c.company_id""")
 sql="""WITH keys AS(SELECT company_id FROM company_profiles UNION SELECT company_id FROM benchmark_activity)
 SELECT k.company_id,coalesce(p.company_name,a.company_name) company_name,a.annual_net_obligations,a.annual_dod_net_obligations,a.annual_award_count,a.period_start,a.period_end,
 a.benchmark_net benchmark_annual_net_obligations,a.benchmark_start benchmark_period_start,a.benchmark_end benchmark_period_end,
 CASE WHEN a.benchmark_net>0 THEN 3*a.benchmark_net END coverage_benchmark,
 coalesce(c.matched_opportunities,0)::BIGINT matched_opportunities,coalesce(c.work_opportunities,0)::BIGINT work_opportunities,coalesce(c.item_opportunities,0)::BIGINT item_opportunities,
 coalesce(c.prospects_with_material_value_guide,0)::BIGINT prospects_with_material_value_guide,coalesce(c.work_without_current_price,0)::BIGINT work_without_current_price,
 coalesce(x.priced_published_requirements,0)::BIGINT priced_published_requirements,x.priced_published_demand,coalesce(f.priced_forecast_items,0)::BIGINT priced_forecast_items,f.full_horizon_forecast_scenario,
 CASE WHEN a.benchmark_net IS NULL OR a.benchmark_net<=0 THEN 'annual_business_baseline_not_established'
 WHEN x.priced_published_demand IS NULL AND f.full_horizon_forecast_scenario IS NULL THEN 'priced_coverage_not_measurable'
 WHEN coalesce(x.priced_published_demand,0)+coalesce(f.full_horizon_forecast_scenario,0)<3*a.benchmark_net THEN 'priced_coverage_below_benchmark_before_overlap'
 ELSE 'overlap_and_company_access_review_required' END coverage_status,
 CASE WHEN a.benchmark_net>=100000000 AND coalesce(c.work_opportunities,0)=0 THEN 'large_active_site_without_matched_work' WHEN a.annual_award_count>=20 AND coalesce(c.prospects_with_material_value_guide,0)=0 THEN 'material_opportunity_coverage_gap' ELSE 'inspect_source_backed_matches' END review_priority,
 'ASOF' assessed_as_of,'POLICY_VERSION' policy_version,
 'Diagnostic only. The 3x benchmark does not create or inflate recommendations. Published requirements are grouped conservatively; forecasts retain their full stated horizon and remain separate because they may overlap. Historical guides, programme funding and reseller sales are not added. Unpriced work is unknown, not zero.' interpretation
 FROM keys k LEFT JOIN company_profiles p USING(company_id) LEFT JOIN benchmark_activity a USING(company_id) LEFT JOIN counts c USING(company_id) LEFT JOIN published x USING(company_id) LEFT JOIN forecast f USING(company_id)""".replace('ASOF',m['as_of']).replace('POLICY_VERSION',POLICY)
 p=r/'company_pipeline_coverage.parquet';c.execute('CREATE TEMP VIEW raw_result AS '+sql)
 desc=c.execute('DESCRIBE raw_result').fetchall();projection=','.join('cast("'+n+'" AS '+('DOUBLE' if t.startswith('DECIMAL') else 'BIGINT' if t=='HUGEINT' else t)+') "'+n+'"' for n,t,*_ in desc);c.execute('CREATE TEMP VIEW result AS SELECT '+projection+' FROM raw_result')
 cols=[dict(Name=n,Type={'VARCHAR':'string','DOUBLE':'double','BIGINT':'bigint'}[t]) for n,t,*_ in c.execute('DESCRIBE result').fetchall()];c.execute("COPY result TO '"+str(p).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)");count=c.execute('SELECT count(*) FROM result').fetchone()[0]
 m['tables']['company_pipeline_coverage']=dict(path=str(p),rows=count,sha256=hashlib.sha256(p.read_bytes()).hexdigest(),columns=cols);m['company_pipeline_coverage_policy']=POLICY;(r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close();print('Full-market company coverage checks',count,flush=True)
