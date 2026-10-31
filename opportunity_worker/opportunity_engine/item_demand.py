"""Whole-market item demand, dated prices and supply context. No sales probability."""
import hashlib,json
from pathlib import Path
import duckdb

POLICY='whole-market-item-demand-v2'

def prepare(sources,folder,as_of):
 """Refresh DIBBS episodes from the held source and materialise global prices."""
 if not all(n in sources for n in ('dla_solicitations','item_market_price_lines','item_reference_facts','demand_candidates','dla_forecasts')):return {}
 folder=Path(folder);folder.mkdir(parents=True,exist_ok=True)
 c=duckdb.connect();c.execute("SET threads=4;SET memory_limit='6GB';SET preserve_insertion_order=false")
 c.execute("SET temp_directory='"+str(folder/'tmp').replace("'","''")+"'")
 for n in ('dla_solicitations','item_market_price_lines','item_reference_facts','demand_candidates','dla_forecasts'):
  c.read_parquet([f['path'] for f in sources[n]['files']]).create_view(n)
 results={}
 def emit(name,sql):
  p=folder/(name+'.parquet');c.execute("COPY ("+sql+") TO '"+str(p).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)")
  sha=hashlib.sha256(p.read_bytes()).hexdigest();results[name]=dict(path=str(p),files=[dict(path=str(p),sha256=sha)],sha256=sha,rows=c.execute('SELECT count(*) FROM read_parquet(?)',[str(p)]).fetchone()[0],as_of=as_of,kind=POLICY,status='SUCCEEDED')
  c.read_parquet(str(p)).create_view('new_'+name);print('Item demand output',name,results[name]['rows'],flush=True)
 # Exact financial source IDs prevent repeated equipment mappings adding prices.
 c.execute("""CREATE TEMP TABLE price_lines AS SELECT * EXCLUDE(rn) FROM (
 SELECT *,row_number() OVER(PARTITION BY coalesce(nullif(source_record_id,''),pricing_record_id) ORDER BY pricing_record_id) rn
 FROM item_market_price_lines WHERE unit_price_usd>0 AND quantity>0 AND include_in_mimir_range
 AND regexp_full_match(coalesce(niin,''),'[0-9]{9}') AND nullif(trim(quantity_unit),'') IS NOT NULL
 AND try_cast(substr(transaction_date,1,10) AS DATE)<=DATE '"""+as_of+"' AND try_cast(substr(transaction_date,1,10) AS DATE)>=DATE '"+as_of+"'-INTERVAL 3 YEAR) WHERE rn=1")
 emit('item_market_price_evidence','SELECT * FROM price_lines')
 c.execute("""CREATE TEMP TABLE market AS SELECT niin,upper(trim(quantity_unit)) unit,count(*) observations,count(DISTINCT supplier_cage) sellers,
 min(unit_price_usd)::DOUBLE price_low,median(unit_price_usd)::DOUBLE price_median,max(unit_price_usd)::DOUBLE price_high,
 min(transaction_date) first_price_date,max(transaction_date) latest_price_date,
 to_json(list(struct_pack(pricing_record_id:=pricing_record_id,source_record_id:=source_record_id,unit_price_usd:=unit_price_usd,quantity:=quantity,unit:=quantity_unit,date:=transaction_date,seller_cage:=supplier_cage,source_url:=source_url,source_s3_uri:=source_s3_uri,source_row_locator:=source_row_locator,source_sha256:=source_sha256) ORDER BY transaction_date DESC,pricing_record_id)[:12]) price_preview_json
 FROM price_lines GROUP BY niin,upper(trim(quantity_unit))""")
 c.execute("CREATE TEMP TABLE wanted AS SELECT DISTINCT right(nsn,9) niin FROM dla_solicitations WHERE regexp_full_match(coalesce(nsn,''),'[0-9]{13}') UNION SELECT DISTINCT niin FROM dla_forecasts WHERE forecast_qty>0 AND substr(cast(forecast_month AS VARCHAR),1,10)>="+"'"+as_of+"'")
 # All catalogue sources may price an item. No candidate CAGE is needed for price.
 c.execute("""CREATE TEMP TABLE catalogue AS SELECT niin,CASE WHEN count(DISTINCT nullif(trim(unit_of_issue),''))=1 THEN min(nullif(trim(unit_of_issue),'')) END catalogue_unit,
 CASE WHEN count(DISTINCT nullif(trim(unit_of_issue),''))=1 AND count(DISTINCT govt_estimated_price) FILTER(WHERE govt_estimated_price>0)=1 AND count(*) FILTER(WHERE govt_estimated_price>0 AND nullif(trim(unit_of_issue),'') IS NULL)=0 THEN min(govt_estimated_price) FILTER(WHERE govt_estimated_price>0) END government_reference_unit_price,
 count(*) reference_rows,min(evidence_id) sample_reference_evidence,min(source_snapshot_sha256) source_snapshot_sha256
 FROM item_reference_facts SEMI JOIN wanted USING(niin) GROUP BY niin""")
 emit('item_price_references',"""SELECT coalesce(c.niin,m.niin) niin,coalesce(m.unit,c.catalogue_unit) unit,c.catalogue_unit,c.government_reference_unit_price,
 m.price_low market_price_low,m.price_median market_price_median,m.price_high market_price_high,m.observations market_observations,m.sellers market_sellers,m.first_price_date,m.latest_price_date,m.price_preview_json,
 c.reference_rows,c.sample_reference_evidence,c.source_snapshot_sha256,
 'Dated same-NIIN, same-unit observed procurement lines; sample prices are not current quotes. Government reference has no retained effective date. Configuration and seller workshare remain unconfirmed.' interpretation
 FROM catalogue c FULL JOIN market m ON m.niin=c.niin AND m.unit=c.catalogue_unit""")
 # Preserve existing stable episode identifiers when the exact native line returns.
 c.execute("""CREATE TEMP TABLE prior AS SELECT source_locator,min(episode_id) episode_id FROM demand_candidates WHERE lane='published_item_procurement' GROUP BY source_locator HAVING count(DISTINCT episode_id)=1""")
 c.execute("""CREATE TEMP TABLE requests AS SELECT *,right(nsn,9) niin,concat_ws('|',solicitation_number,line_item_num,nsn) locator,
 sha256(to_json(struct_pack(solicitation:=solicitation_number,line:=line_item_num,nsn:=nsn,quantity:=quantity,unit:=unit_of_issue,deadline:=closing_date,set_aside:=set_aside_code,delivery_days:=delivery_days,pr:=purchase_request_number))) request_hash
 FROM dla_solicitations WHERE regexp_full_match(coalesce(nsn,''),'[0-9]{13}') AND quantity>0 AND nullif(unit_of_issue,'') IS NOT NULL""")
 c.execute("""CREATE TEMP TABLE priced_requests AS SELECT r.*,p.episode_id prior_episode_id,
 coalesce(v.market_price_median,v.government_reference_unit_price) estimate_unit_price,
 v.market_price_low,v.market_price_high,v.market_observations,v.latest_price_date,v.price_preview_json,v.government_reference_unit_price,
 CASE WHEN v.market_price_median>0 THEN 'Recent observed same-item unit-price median' WHEN v.government_reference_unit_price>0 THEN 'Government reference, effective date unknown' ELSE 'No same-unit price' END price_basis
 FROM requests r LEFT JOIN prior p ON p.source_locator=r.locator LEFT JOIN new_item_price_references v ON v.niin=r.niin AND v.unit=r.unit_of_issue""")
 ex={
 'episode_id':"coalesce(prior_episode_id,sha256('native-dibbs-line-v1|'||locator))",'record_hash':"sha256(concat_ws('|',request_hash,cast(estimate_unit_price AS VARCHAR),latest_price_date,price_preview_json))",'lane':"'published_item_procurement'",
 'title':"coalesce(nullif(nomenclature,''),'NSN '||nsn)||' / '||solicitation_number",'scope_text':"coalesce(nomenclature,'Item supply')||'. NSN '||nsn||'; quantity '||cast(quantity AS VARCHAR)||' '||unit_of_issue||'; DIBBS line '||line_item_num||'; purchase request '||coalesce(purchase_request_number,'not retained')||'; set-aside code '||coalesce(set_aside_code,'not retained')||'.'",
 'customer_name':"'Defense Logistics Agency'",'psc':"substr(nsn,1,4)",'window_end':"substr(closing_date,1,10)",'date_basis':"'Source-stated DIBBS closing date; amendments must be checked on DIBBS'",
 'value_low':'quantity*coalesce(market_price_low,estimate_unit_price)','value_median':'quantity*estimate_unit_price','value_high':'quantity*coalesce(market_price_high,estimate_unit_price)',
 'value_basis':"CASE WHEN estimate_unit_price>0 THEN 'exact_item_quantity_times_unit_price' ELSE 'unpriced_same_unit_reference_missing' END",'value_scope':"'Requested quantity times a dated same-item unit-price median or government reference. Gross purchase estimate, not company revenue.'",
 'value_inputs_json':"to_json(struct_pack(nsn:=nsn,niin:=niin,quantity:=quantity,request_unit:=unit_of_issue,request_evidence:=request_hash,observed_unit_price:=estimate_unit_price,observed_price_date:=latest_price_date,price_reason_type:=price_basis,price_unit:=unit_of_issue,price_evidence:=json(price_preview_json),government_reference_unit_price:=government_reference_unit_price,market_observations:=market_observations,purchase_request_number:=purchase_request_number,delivery_days:=delivery_days,set_aside_code:=set_aside_code,source_table:='market_intel_silver.fact_solicitations',source_freshness_note:='Upstream publication or amendment time is not retained. Conflicting native line variants are excluded. Deadline and retained source content are the evidence.'))",
 'source_name':"'dla_solicitations'",'source_url':"'https://www.dibbs.bsm.dla.mil/RFQ/RFQRec.aspx?sn='||solicitation_number",'source_evidence_id':'request_hash','source_locator':'locator','native_identifier':'solicitation_number',
 'procurement_state':"'procurement_published'",'reconciled_state':"'outcome_not_established'",'access_state':"'Exact item association does not establish eligibility, configuration, manufacturing rights or quotation authority'",
 'rationale':"'A retained DIBBS request states this item, quantity and closing date. The company association comes from its exact NIIN reference; price comparisons use exact item and unit.'",
 'limitations':"'Confirm DIBBS amendments, set-aside, technical data and source requirements. An unexpired source date is not proof the request remains open; catalogue association is not manufacturing qualification.'"}
 projection=','.join('cast('+ex.get(n,'NULL')+' AS '+typ+') "'+n+'"' for n,typ,*_ in c.execute('DESCRIBE demand_candidates').fetchall())
 # Keep missing prior rows as source history; never infer cancellation by absence.
 emit('demand_candidates',"SELECT * FROM demand_candidates WHERE lane<>'published_item_procurement' OR NOT EXISTS(SELECT 1 FROM requests r WHERE r.locator=demand_candidates.source_locator) UNION ALL SELECT "+projection+' FROM priced_requests')
 c.close();return results


def augment(directory):
 """Add annual forecast values and latest inventory context to every company link."""
 r=Path(directory);m=json.loads((r/'manifest.json').read_text())
 if not all(n in m['sources'] for n in ('item_price_references','item_market_price_evidence')):return
 c=duckdb.connect();c.execute("SET threads=2;SET memory_limit='6GB';SET preserve_insertion_order=false")
 c.execute("SET temp_directory='"+str(r/'item_context_tmp').replace("'","''")+"'")
 for n in ('opportunities','company_matches','dla_forecasts','dla_inventory'):
  c.read_parquet(m['tables'][n]['path']).create_view(n)
 for n in ('item_price_references','item_market_price_evidence','dla_reorder_points'):
  # Quality passes reuse downloaded immutable tables, not paths from the
  # previous worker's filesystem. Reorder evidence is a retained source.
  table=m['tables'].get(n,{})
  if table.get('path') and Path(table['path']).is_file():c.read_parquet(table['path']).create_view(n)
  elif n in m['sources']:c.read_parquet([f['path'] for f in m['sources'][n]['files']]).create_view(n)
 def emit(n,sql):
  p=r/(n+'.parquet');tmp=r/(n+'.expanded.parquet');c.execute('CREATE OR REPLACE TEMP VIEW raw_result AS '+sql)
  portable=[]
  for name,typ,*_ in c.execute('DESCRIBE raw_result').fetchall():
   target='DOUBLE' if typ.startswith('DECIMAL') else 'BIGINT' if typ=='HUGEINT' else 'VARCHAR' if typ in ('DATE','TIMESTAMP','JSON') else typ
   portable.append('cast("'+name+'" AS '+target+') "'+name+'"')
  c.execute('CREATE OR REPLACE TEMP VIEW result AS SELECT '+','.join(portable)+' FROM raw_result')
  types={'VARCHAR':'string','DOUBLE':'double','BIGINT':'bigint','INTEGER':'bigint','BOOLEAN':'boolean'}
  columns=[dict(Name=name,Type=types[typ]) for name,typ,*_ in c.execute('DESCRIBE result').fetchall()]
  c.execute("COPY result TO '"+str(tmp).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)");count=c.execute('SELECT count(*) FROM read_parquet(?)',[str(tmp)]).fetchone()[0];tmp.replace(p)
  m['tables'][n]=dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),rows=count,columns=columns)
  if n in m['sources']:c.read_parquet(str(p)).create_view(n,replace=True)
  c.read_parquet(str(p)).create_view('new_'+n,replace=True);print('Expanded item evidence',n,m['tables'][n]['rows'],flush=True)
 emit('item_market_price_evidence','SELECT * FROM item_market_price_evidence')
 emit('item_price_references','SELECT * FROM item_price_references')
 # Filter the 15m-row inventory and reorder history to actual forecast items.
 # Keeping every catalogue item's JSON in memory crowded out the forecast join.
 c.execute("""CREATE TEMP TABLE forecast_keys AS SELECT episode_id,record_hash,json_extract_string(value_inputs_json,'$.niin') niin,window_start,window_end FROM opportunities WHERE lane='source_item_forecasts'""")
 c.execute("CREATE TEMP TABLE wanted_forecast_items AS SELECT DISTINCT niin FROM forecast_keys")
 c.execute("""CREATE TEMP TABLE latest_inventory AS SELECT niin,max(snapshot_date) snapshot_date FROM dla_inventory SEMI JOIN wanted_forecast_items USING(niin) GROUP BY niin""")
 c.execute("""CREATE TEMP TABLE inventory AS SELECT i.niin,min(i.snapshot_date) inventory_date,
 CASE WHEN count(DISTINCT i.total_stock)=1 THEN min(try_cast(i.total_stock AS DOUBLE)) END stock_on_hand,
 CASE WHEN count(DISTINCT i.backorder_qty)=1 THEN min(try_cast(i.backorder_qty AS DOUBLE)) END backorder_quantity,
 CASE WHEN count(DISTINCT i.annual_demand_quantity)=1 THEN min(try_cast(i.annual_demand_quantity AS DOUBLE)) END annual_demand_quantity,
 to_json(list(struct_pack(stock:=i.total_stock,backorders:=i.backorder_qty,annual_demand:=i.annual_demand_quantity,condition:=i.condition_code,date:=i.snapshot_date,source_release:=i.source_release,source_file:=i.source_file,public_release_eligible:=i.public_release_eligible))) inventory_evidence_json
 FROM dla_inventory i JOIN latest_inventory l ON i.niin=l.niin AND i.snapshot_date=l.snapshot_date GROUP BY i.niin""")
 if 'dla_reorder_points' in m['sources']:
  c.execute("""CREATE TEMP TABLE reorder AS SELECT niin,max(snapshot_date) reorder_date,
 CASE WHEN count(DISTINCT reorder_point) FILTER(WHERE latest_rank=1)=1 THEN min(reorder_point) FILTER(WHERE latest_rank=1) END reorder_point,
 CASE WHEN count(DISTINCT total_stock) FILTER(WHERE latest_rank=1)=1 THEN min(total_stock) FILTER(WHERE latest_rank=1) END reorder_stock,
 CASE WHEN count(DISTINCT total_stock-reorder_point) FILTER(WHERE latest_rank=2)=1 THEN min(total_stock-reorder_point) FILTER(WHERE latest_rank=2) END previous_reorder_gap,
 to_json(list(struct_pack(stock:=total_stock,reorder_point:=reorder_point,date:=snapshot_date,condition:=condition_code,source_release:=source_release,source_file:=source_file))) reorder_evidence_json
 FROM (SELECT *,dense_rank() OVER(PARTITION BY niin ORDER BY snapshot_date DESC) latest_rank FROM dla_reorder_points SEMI JOIN wanted_forecast_items USING(niin)) GROUP BY niin""")
 else:c.execute("CREATE TEMP TABLE reorder AS SELECT NULL::VARCHAR niin,NULL::VARCHAR reorder_date,NULL::DOUBLE reorder_point,NULL::DOUBLE reorder_stock,NULL::DOUBLE previous_reorder_gap,NULL::VARCHAR reorder_evidence_json WHERE false")
 # Preserve every monthly row. Conflicts, negatives and unknown units block totals.
 # Ordered lists retain per-group state that cannot spill. Partition complete
 # NIIN groups so the all-market rollup stays within the existing worker budget.
 for partition in range(32):
  prefix='CREATE TEMP TABLE forecast AS ' if partition==0 else 'INSERT INTO forecast '
  c.execute(prefix+"""SELECT o.episode_id,o.record_hash,o.niin,
 min(d.forecast_month) first_month,max(d.forecast_month) last_month,count(*) month_count,count(DISTINCT d.forecast_month) distinct_months,
 sum(d.forecast_qty)::BIGINT quantity,count(*) FILTER(WHERE d.forecast_qty<0) negative_months,
 to_json(list(struct_pack(month:=d.forecast_month,quantity:=d.forecast_qty,source_release:=d.source_release,source_file:=d.source_file,source_item_id:=d.source_item_id) ORDER BY d.forecast_month)) monthly_evidence_json
 FROM forecast_keys o JOIN dla_forecasts d ON d.niin=o.niin AND substr(cast(d.forecast_month AS VARCHAR),1,10)>='"""+m['as_of']+"' WHERE hash(o.niin)%32="+str(partition)+" GROUP BY o.episode_id,o.record_hash,o.niin")
  print('Forecast context partition',partition+1,'/ 32',flush=True)
 c.execute("""CREATE TEMP TABLE years AS SELECT f.episode_id,f.record_hash,substr(d.forecast_month,1,4) calendar_year,sum(d.forecast_qty)::BIGINT quantity,
 round(sum(d.forecast_qty)*p.government_reference_unit_price,2) government_reference_scenario,
 round(sum(d.forecast_qty)*p.market_price_median,2) market_price_scenario
 FROM forecast f JOIN dla_forecasts d ON d.niin=f.niin AND d.forecast_month BETWEEN f.first_month AND f.last_month LEFT JOIN item_price_references p ON p.niin=f.niin AND p.unit=p.catalogue_unit
 WHERE f.negative_months=0 AND f.month_count=f.distinct_months AND p.unit='EA'
 GROUP BY f.episode_id,f.record_hash,substr(d.forecast_month,1,4),p.government_reference_unit_price,p.market_price_median""")
 for partition in range(32):
  prefix='CREATE TEMP TABLE year_json AS ' if partition==0 else 'INSERT INTO year_json '
  c.execute(prefix+"SELECT episode_id,record_hash,to_json(list(struct_pack(calendar_year:=calendar_year,quantity:=quantity,government_reference_scenario:=government_reference_scenario,market_price_scenario:=market_price_scenario) ORDER BY calendar_year)) annual_scenarios_json FROM years WHERE hash(episode_id)%32="+str(partition)+" GROUP BY episode_id,record_hash")
 emit('item_demand_context',"""SELECT f.*,p.unit catalogue_unit,p.government_reference_unit_price,p.market_price_low,p.market_price_median,p.market_price_high,p.market_observations,p.market_sellers,p.first_price_date,p.latest_price_date,p.price_preview_json,p.sample_reference_evidence,p.source_snapshot_sha256,
 CASE WHEN f.negative_months=0 AND f.month_count=f.distinct_months AND p.unit='EA' THEN round(f.quantity*p.government_reference_unit_price,2) END government_reference_lot_scenario,
 CASE WHEN f.negative_months=0 AND f.month_count=f.distinct_months AND p.unit='EA' THEN round(f.quantity*p.market_price_median,2) END market_price_lot_scenario,
 CASE WHEN f.negative_months>0 THEN 'negative_source_quantity' WHEN f.month_count<>f.distinct_months THEN 'duplicate_source_months' WHEN p.unit<>'EA' OR p.unit IS NULL THEN 'forecast_unit_conversion_unverified' ELSE 'catalogue_EA_assumed_forecast_unit_not_stated' END valuation_status,
 y.annual_scenarios_json,i.inventory_date,i.stock_on_hand,i.backorder_quantity,i.annual_demand_quantity,i.inventory_evidence_json,z.reorder_date,z.reorder_point,z.reorder_stock,z.previous_reorder_gap,z.reorder_evidence_json,
 CASE WHEN z.reorder_stock<z.reorder_point AND z.previous_reorder_gap>=0 THEN 'crossed_below_reorder_point' WHEN z.reorder_stock<z.reorder_point THEN 'below_reorder_point' WHEN i.backorder_quantity>0 THEN 'reported_backorders' ELSE 'forecast_without_verified_stock_pressure' END inventory_signal,
 'Forecast scenario, not an order or company revenue. The forecast source has no unit; only consistent catalogue EA is multiplied. Non-EA conversions require review. Stock is context, not subtracted from a procurement forecast. Snapshot dates and actual item prices remain linked.' interpretation
 FROM forecast f LEFT JOIN item_price_references p ON p.niin=f.niin AND p.unit=p.catalogue_unit LEFT JOIN year_json y USING(episode_id,record_hash) LEFT JOIN inventory i ON i.niin=f.niin LEFT JOIN reorder z ON z.niin=f.niin""")
 # Existing reader fields remain backwards compatible; add dated market scenarios.
 emit('company_item_valuations',"""SELECT cm.company_id,x.episode_id,x.record_hash,x.niin,x.quantity,x.catalogue_unit,x.first_month,x.last_month,x.month_count,x.monthly_evidence_json,x.government_reference_unit_price,
 NULL::DOUBLE supplier_average_unit_price,x.government_reference_lot_scenario,NULL::DOUBLE supplier_average_lot_scenario,NULL::VARCHAR supplier_average_last_sale,
 to_json(struct_pack(sample_reference_evidence:=x.sample_reference_evidence,source_snapshot_sha256:=x.source_snapshot_sha256,price_preview:=json(x.price_preview_json))) catalogue_evidence_json,
 x.interpretation,x.market_price_low,x.market_price_median,x.market_price_high,x.market_observations,x.market_sellers,x.first_price_date,x.latest_price_date,x.market_price_lot_scenario,x.annual_scenarios_json,x.valuation_status,x.inventory_date,x.stock_on_hand,x.backorder_quantity,x.annual_demand_quantity,x.inventory_signal,x.reorder_date,x.reorder_point,x.reorder_stock,x.previous_reorder_gap,x.inventory_evidence_json,x.reorder_evidence_json
 FROM (SELECT DISTINCT company_id,episode_id,opportunity_hash FROM company_matches) cm JOIN new_item_demand_context x ON x.episode_id=cm.episode_id AND x.record_hash=cm.opportunity_hash""")
 m['item_demand_policy']=POLICY;(r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close()
