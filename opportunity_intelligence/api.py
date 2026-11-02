"""Authenticated FastAPI adapter for the shared AWS read model.
The application must inject its existing verified-principal dependency.
"""
import contextvars,datetime,hashlib,json,re,time,math
from pathlib import Path
from functools import lru_cache
from .aws import AWS,BUCKET,PREFIX,DATABASE
from . import rules
from .company_index import IndexUnavailable
from .notice_facts import quantity_fact,price_scenario_fact
from opportunity_worker.opportunity_engine.financial_readiness import guard as guard_finance,risk_sql as finance_risk_sql,status as finance_status
from .priority import POLICY as INVENTORY_PRIORITY_POLICY,POLICY_HASH as INVENTORY_PRIORITY_POLICY_HASH
from opportunity_worker.opportunity_engine.discovery_read_model import DiscoveryRepositoryMixin,register_routes as register_discovery_routes
_REQUEST_RELEASE=contextvars.ContextVar("mimir_opportunity_request_release",default=None)

class Repository(DiscoveryRepositoryMixin):
 def __init__(self,aws=None):self.aws=aws or AWS();self._manifest=None;self._expires=0
 def manifest(self):
  pinned=_REQUEST_RELEASE.get()
  if pinned and pinned[0] is self:return pinned[1]
  if self._manifest is None or time.monotonic()>=self._expires:
   p=json.loads(self.aws.s3.get_object(Bucket=BUCKET,Key=PREFIX+'/current.json')['Body'].read());key=p['manifest_key']
   if not key.startswith(PREFIX+'/releases/'):raise ValueError('Invalid engine release pointer')
   self._manifest=json.loads(self.aws.s3.get_object(Bucket=BUCKET,Key=key)['Body'].read());self._manifest['_company_index']=p.get('company_index');self._expires=time.monotonic()+30
  return self._manifest
 def indexed_company(self,company):
  from .company_index import CompanyIndex
  if not hasattr(self,'_company_index'):self._company_index=CompanyIndex(self)
  return self._company_index.load(company)
 def table(self,name):
  t=(self.manifest().get('source_tables',{}) if name=='company_work' else self.manifest()['tables'])[name]['table']
  if not re.fullmatch(r'oi_[a-z0-9_]+',t):raise ValueError('Invalid engine table binding')
  return DATABASE+'.'+t
 def rows(self,sql):return self.aws.result(sql)[0]
 def quality_projection(self):
  extra=',qr.source_entry_kind,qr.verdict source_quality_verdict,qr.reason source_quality_reason,qr.passages_json source_quality_passages_json,qr.body_deadline source_body_deadline'
  columns={x['Name'] for x in self.manifest()['tables'].get('opportunity_quality_reviews',{}).get('columns',[])}
  for name in ('required_delivery_mode','delivery_mode_basis','requirement_psc','psc_reference_url'):
   if name in columns:extra+=',qr.'+name
  return extra
 def refresh_status(self):
  try:
   value=json.loads(self.aws.s3.get_object(Bucket=BUCKET,Key=PREFIX+'/operations/last_run.json')['Body'].read())
   status=value.get('status')
   if status not in ('running','succeeded','failed_preserved_previous_current_pointer'):return {'status':'unavailable'}
   if status=='running':
    started=datetime.datetime.fromisoformat(value['started_at'].replace('Z','+00:00'))
    if started.tzinfo is None:return {'status':'unavailable'}
    if (datetime.datetime.now(datetime.timezone.utc)-started).total_seconds()>4*3600:status='status_stale'
   # Never expose exception strings, stack traces or infrastructure details.
   return dict(status=status,**{k:value[k] for k in ('started_at','finished_at','refresh_mode') if isinstance(value.get(k),str) and len(value[k])<=128})
  except Exception:return {'status':'unavailable'}
 def source_status(self):
  m=self.manifest();result={name:dict(value) for name,value in m.get('source_status',{}).items()}
  # Preserve edition/health separately from build dates. Never expose source
  # paths, archive locations or retrieval exception text through this view.
  for name,source in m.get('sources',{}).items():
   if name not in result:continue
   if isinstance(source.get('source_release'),str):result[name]['source_release']=source['source_release'][:128]
   health=source.get('refresh_health')
   if isinstance(health,dict):result[name]['refresh_health']={k:health[k][:512] for k in ('status','attempted_at','source_release','interpretation') if isinstance(health.get(k),str)}
  return result
 def company(self,cage):
  if not re.fullmatch('[A-Z0-9]{5}',cage):raise ValueError('Invalid CAGE')
  payload=self.indexed_company('CAGE:'+cage)
  if payload is not None and payload.get('company'):return [dict(payload['company'])]
  return self.rows('SELECT * FROM '+self.table('company_registry')+" WHERE cage_code='"+cage+"'")
 def relationships(self,cage,limit=50,offset=0):
  if not re.fullmatch('[A-Z0-9]{5}',cage):raise ValueError('Invalid CAGE')
  if not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid pagination')
  if 'company_registry' not in self.manifest()['tables']:return []
  registry=self.table('company_registry')
  sql="""WITH candidates AS(SELECT * FROM REGISTRY WHERE cage_code='CAGEVALUE' OR upper(trim(parent_cage))='CAGEVALUE' OR upper(trim(replacement_cage))='CAGEVALUE'),
  edges AS(SELECT cage_code subject_cage,upper(trim(parent_cage)) object_cage,company_name subject_name,dla_last_update source_last_update,'registered_parent_cage' relationship_type,'parent_cage' evidence_field FROM candidates
  UNION ALL SELECT cage_code,upper(trim(replacement_cage)),company_name,dla_last_update,'replacement_cage','replacement_cage' FROM candidates)
  SELECT e.*,p.company_name object_name,CASE WHEN p.cage_code IS NULL THEN 'referenced_cage_not_in_registry' ELSE 'both_registry_records_available' END record_coverage
  FROM edges e LEFT JOIN REGISTRY p ON p.cage_code=e.object_cage
  WHERE regexp_like(e.object_cage,'^[A-Z0-9]{5}$') AND e.object_cage NOT IN ('00000','99999','XXXXX','ZZZZZ') AND e.subject_cage<>e.object_cage AND (e.subject_cage='CAGEVALUE' OR e.object_cage='CAGEVALUE')
  ORDER BY e.relationship_type,e.subject_cage,e.object_cage OFFSET OFFSETVALUE LIMIT LIMITVALUE""".replace('REGISTRY',registry).replace('CAGEVALUE',cage).replace('OFFSETVALUE',str(offset)).replace('LIMITVALUE',str(limit))
  rows=self.rows(sql)
  for row in rows:
   key='|'.join(str(row.get(k) or '') for k in ('relationship_type','subject_cage','object_cage'))
   row['relationship_id']=hashlib.sha256(key.encode()).hexdigest();row['record_hash']=hashlib.sha256(json.dumps(row,sort_keys=True).encode()).hexdigest()
  return rows
 def companies(self,query,limit=20):
  if not query or len(query)>100 or not 1<=limit<=100:raise ValueError('Invalid company search')
  term=query.upper().replace("'","''");exact=bool(re.fullmatch('[A-Z0-9]{5}',query.upper()))
  name_condition="starts_with(upper(company_name),'"+term+"')"
  condition="(cage_code='"+term+"' OR "+name_condition+")" if exact else name_condition
  ordering="CASE WHEN cage_code='"+term+"' THEN 0 ELSE 1 END," if exact else ''
  return self.rows('SELECT company_id,cage_code,company_name,city,region,country,cage_status,coverage_state,primary_naics,sam_last_update,dla_last_update FROM '+self.table('company_registry')+' WHERE '+condition+' ORDER BY '+ordering+'coverage_state,company_name,cage_code LIMIT '+str(limit))
 def innovation(self,limit=20):
  if not 1<=limit<=100:raise ValueError('Invalid limit')
  return self.rows('SELECT * FROM '+self.table('innovation_signals')+' ORDER BY posted_date DESC,title LIMIT '+str(limit))
 def opportunities(self,company=None,limit=20,offset=0,monitoring_only=False,kind='all',minimum_value=0.0,_query=False,topic=None,psc=None,_source_groups=True,_materialize=False,_plan_links=False):
  if not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid pagination')
  if not math.isfinite(minimum_value) or not 0<=minimum_value<=1000000000000:raise ValueError('Invalid minimum value')
  if _materialize and (company!='CAGE:00000' or not _query or _source_groups):raise ValueError('Invalid serving projection compilation')
  if kind not in ('all','work','items'):raise ValueError('Invalid opportunity kind')
  if company and not re.fullmatch(r'(?:CAGE:[A-Z0-9]{5}|UEI:[A-Z0-9]{12})',company):raise ValueError('Invalid company identifier')
  if company and not _query and not monitoring_only and not topic and not psc:
   payload=self.indexed_company(company)
   if payload is not None:
    from .company_index import eligible,rank,number
    rows=[guard_finance(r,self.manifest()) for r in eligible(payload,kind)]
    rows=[r for r in rows if not minimum_value or (number(r.get('commercial_value')) or 0)>=minimum_value]
    rows.sort(key=rank);records=rows[offset:offset+limit]
    for record in records:record.update(quantity_fact(record));record.update(price_scenario_fact(record))
    return records
  o=self.table('opportunities');d=self.table('demand_decisions');where="d.stage IN ('historical_watch','repeat_demand_inference')" if monitoring_only else "d.stage NOT IN ('resolved_or_elapsed','evidence_gap','planning_context','historical_watch','repeat_demand_inference')";join='';extra=''
  if kind not in ('all','work','items'):raise ValueError('Invalid opportunity kind')
  if company and not re.fullmatch(r'(?:CAGE:[A-Z0-9]{5}|UEI:[A-Z0-9]{12})',company):raise ValueError('Invalid company identifier')
  quality='opportunity_quality_reviews' in self.manifest()['tables']
  if topic and not re.fullmatch('[a-z][a-z0-9.-]{0,79}',topic):raise ValueError('Invalid work topic')
  if psc and not re.fullmatch('[A-Z0-9]{1,4}',psc):raise ValueError('Invalid PSC prefix')
  vocabulary='opportunity_vocabulary' in self.manifest()['tables']
  if topic and not vocabulary:return [] if not _query else 'SELECT * FROM '+o+' WHERE false'
  item_condition="(o.lane IN ('published_item_procurement','source_item_forecasts','stock_supported_item_demand')"+(" OR coalesce(qr.category_override='items',false)" if quality else '')+")"
  category_where=(' AND '+item_condition if kind=='items' else " AND o.lane NOT IN ('published_item_procurement','source_item_forecasts','stock_supported_item_demand')"+(" AND coalesce(qr.category_override,'')<>'items'" if quality else '') if kind=='work' else '')
  if company:
   if not re.fullmatch(r'(?:CAGE:[A-Z0-9]{5}|UEI:[A-Z0-9]{12})',company):raise ValueError('Invalid company identifier')
   join=' JOIN '+self.table('company_matches')+" m ON m.episode_id=o.episode_id AND m.company_id='"+company+"'";where+=' AND m.opportunity_hash=o.record_hash'
   if not monitoring_only and 'agency_forecast_incumbents' in self.manifest()['tables']:
    # A company-matched official plan for the exact incumbent award supersedes
    # its generic inferred follow-on in the feed. Both source records survive.
    planned=self.table('agency_forecast_incumbents');pm=self.table('company_matches');pw=self.table('company_work_matches') if 'company_work_matches' in self.manifest()['tables'] else None
    plan_join=(' LEFT JOIN '+pw+' pcw ON pcw.company_id=pm.company_id AND pcw.episode_id=po.episode_id AND pcw.opportunity_hash=po.record_hash') if pw else ''
    plan_access=" AND coalesce(pcw.review_category,'')<>'market_context'" if pw else ''
    published_plan="SELECT i.award_key FROM "+planned+" i JOIN "+o+" po ON po.episode_id=i.episode_id JOIN "+d+" pd ON pd.episode_id=po.episode_id JOIN "+pm+" pm ON pm.episode_id=po.episode_id AND pm.opportunity_hash=po.record_hash AND pm.company_id=i.company_id"+plan_join+" WHERE i.company_id='"+company+"' AND nullif(i.award_key,'') IS NOT NULL AND pd.stage NOT IN ('resolved_or_elapsed','evidence_gap','planning_context','historical_watch','repeat_demand_inference') AND try_cast(po.window_end AS date)>=current_date"+plan_access
    if _plan_links:
     return published_plan.replace('SELECT i.award_key','SELECT i.company_id serving_company_id,i.award_key,po.window_end,po.value_inputs_json').replace("i.company_id='CAGE:00000' AND ",'').replace(' AND try_cast(po.window_end AS date)>=current_date','')
    if not _materialize:where+=" AND NOT (o.lane='funded_contract_follow_on' AND coalesce(json_extract_scalar(o.value_inputs_json,'$.record_id'),'') IN ("+published_plan+"))"
   if _materialize:
    join=join.replace("m.company_id='CAGE:00000'","regexp_like(m.company_id,'^CAGE:[A-Z0-9]{5}$')")
    where=where.replace("coalesce(json_extract_scalar(o.value_inputs_json,'$.record_id'),'') IN (SELECT i.award_key","(m.company_id,coalesce(json_extract_scalar(o.value_inputs_json,'$.record_id'),'')) IN (SELECT i.company_id,i.award_key").replace("i.company_id='CAGE:00000' AND ", '')
   notice_lot="coalesce(try_cast(json_extract_scalar(np.items_json,'$[0].government_reference_lot_scenario') AS double),try_cast(json_extract_scalar(np.items_json,'$[0].supplier_average_lot_scenario') AS double))" if 'company_notice_item_prices' in self.manifest()['tables'] else 'NULL'
   if 'company_notice_item_prices' in self.manifest()['tables']:notice_lot="CASE WHEN json_array_length(np.items_json)=1 THEN "+notice_lot+" END"
   notice_lot="CASE WHEN NOT regexp_like(lower(coalesce(o.title,'')),'\\b(repair\\w*|overhaul\\w*|remanufactur\\w*)\\b') AND NOT regexp_like(coalesce(o.psc,''),'^[JNYZ]') THEN "+notice_lot+' END'
   from opportunity_worker.opportunity_engine.request_values import reference_request_value_sql
   request_lot=reference_request_value_sql()
   forecast_lot='coalesce(least(v.government_reference_lot_scenario,v.supplier_average_lot_scenario),v.government_reference_lot_scenario,v.supplier_average_lot_scenario)' if 'company_item_valuations' in self.manifest()['tables'] else 'NULL'
   valuation_columns={x['Name'] for x in self.manifest()['tables'].get('company_item_valuations',{}).get('columns',[])}
   if 'market_price_lot_scenario' in valuation_columns:forecast_lot='coalesce(v.market_price_lot_scenario,v.government_reference_lot_scenario)'
   if 'company_item_valuations' in self.manifest()['tables']:forecast_lot="CASE WHEN v.catalogue_unit='EA' THEN "+forecast_lot+' END'
   annual_lot="CASE WHEN o.lane='stock_supported_item_demand' AND v.valuation_status='reported_annual_usage_EA_assumed_not_source_forecast' THEN "+forecast_lot+' END' if 'valuation_status' in valuation_columns else 'NULL'
   plan_value="CASE WHEN o.value_basis='source_planned_package_band_including_options' THEN coalesce(o.value_median,o.value_low) END"
   size_limit="regexp_like(lower(coalesce(o.scope_text,'')),'this\\s+(?:proposed\\s+)?(?:acquisition|requirement|purchase|contract|action)\\s+is\\s+(?:not\\s+expected\\s+to\\s+exceed|below)\\s+(?:the\\s+)?simplified\\s+acquisition\\s+threshold')"
   comparable="CASE WHEN NOT ("+size_limit+") THEN o.value_median END"
   ranking_value='coalesce('+notice_lot+','+request_lot+','+annual_lot+','+plan_value+','+comparable+','+forecast_lot+',0.0)'
   forecast_year='NULL';unit_price='NULL'
   if 'company_item_valuations' in self.manifest()['tables']:
    asof=self.manifest().get('as_of')
    if not asof or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',asof):raise ValueError('Invalid release as-of date')
    unit_price='coalesce(v.market_price_median,v.government_reference_unit_price)' if 'market_price_median' in valuation_columns else 'v.government_reference_unit_price'
    months="reduce(try(cast(json_parse(v.monthly_evidence_json) AS array(json))),cast(0 AS double),(total,x)->total+CASE WHEN try_cast(substr(json_extract_scalar(x,'$.month'),1,10) AS date)>=DATE '"+asof+"' AND try_cast(substr(json_extract_scalar(x,'$.month'),1,10) AS date)<date_add('month',12,DATE '"+asof+"') THEN coalesce(try_cast(json_extract_scalar(x,'$.quantity') AS double),0.0) ELSE 0.0 END,total->total)"
    forecast_year="CASE WHEN o.lane='source_item_forecasts' AND v.catalogue_unit='EA' AND ("+forecast_lot+")>0 THEN "+months+'*'+unit_price+' END'
   from .priority import inventory_priority
   inventory_points,inventory_basis=inventory_priority(valuation_columns,self.manifest()['as_of'],forecast_year,unit_price)
   recurrence_year="try_cast(json_extract_scalar(o.value_inputs_json,'$.next_12_month_median') AS double)"
   duration="greatest(1.0,try_cast(date_diff('day',try_cast(json_extract_scalar(o.value_inputs_json,'$.period_start') AS date),try_cast(coalesce(json_extract_scalar(o.value_inputs_json,'$.potential_end'),json_extract_scalar(o.value_inputs_json,'$.period_end')) AS date)) AS double)/365.25)"
   lead="greatest(0.0,try_cast(date_diff('day',DATE '"+self.manifest()['as_of']+"',try_cast(o.window_start AS date)) AS double)/365.25)"
   followon="coalesce(o.value_median/"+duration+"/power(1.0+"+lead+",2.0),o.value_median,0.0)"
   priority_amount="CASE WHEN o.lane='source_item_forecasts' THEN coalesce("+forecast_year+",0.0) WHEN o.value_basis='estimated_recurring_buying_flow_36m' THEN coalesce("+recurrence_year+",0.0) WHEN o.lane='funded_contract_follow_on' THEN "+followon+" ELSE "+ranking_value+' END'
   finance_risk=finance_risk_sql(self.manifest())
   ranking_value='CASE WHEN '+finance_risk+' THEN cast(NULL AS double) ELSE '+ranking_value+' END'
   priority_amount='CASE WHEN '+finance_risk+' THEN 0.0 ELSE '+priority_amount+' END'
   value_kind="CASE WHEN o.value_basis='estimated_recurring_buying_flow_36m' THEN 'recurring_demand_scenario' WHEN o.lane IN ('recurring_government_work','recurring_subcontract_work','funded_contract_follow_on') THEN 'historical_package_benchmark' WHEN "+notice_lot+">0 THEN 'item_reference_estimate' WHEN "+request_lot+">0 THEN 'historical_unit_price_estimate' WHEN "+annual_lot+">0 THEN 'annual_usage_scenario' WHEN o.value_basis='source_planned_package_band_including_options' AND coalesce(o.value_median,o.value_low,o.value_high)>0 THEN 'planned_package_reference' WHEN o.lane='source_item_forecasts' AND "+forecast_lot+">0 THEN 'forecast_scenario' WHEN o.value_median>0 AND ("+size_limit+") THEN 'historical_context_source_limit' WHEN o.value_median>0 THEN 'historical_comparison' ELSE 'unknown' END"
   value_kind="CASE WHEN "+finance_risk+" THEN 'unvalidated_subcontract_benchmark' ELSE "+value_kind+' END'
   extra=',m.match_id,m.match_route,m.matched_terms,m.sample_evidence_id,m.company_evidence_caveat,'+ranking_value+' commercial_value,'+value_kind+' commercial_value_kind,'+item_condition+' item_demand,'+forecast_year+' forecast_next_12m_value'
   extra+=','+inventory_points+' inventory_priority_points,'+inventory_basis+' inventory_priority_basis'
   extra+=",(CASE d.stage WHEN 'published_requirement' THEN 12 WHEN 'qualification_opening' THEN 10 WHEN 'teaming_opening' THEN 10 WHEN 'emerging_demand' THEN 6 WHEN 'expected_demand' THEN 3 WHEN 'source_forecast' THEN 2 ELSE 0 END)+least(20.0,m.score/5.0)+18.0*ln(1.0+greatest("+priority_amount+",0.0)/100000.0)+(CASE WHEN d.stage='published_requirement' AND try_cast(o.window_end AS date) BETWEEN DATE '"+self.manifest()['as_of']+"' AND date_add('day',30,DATE '"+self.manifest()['as_of']+"') THEN 12 WHEN o.window_end IS NOT NULL THEN 3 ELSE 0 END)+("+inventory_points+") priority_score,'Ranks by scope fit, commercial size and engagement timing. Item forecasts and recurring work use next-year buying. Follow-on guides are compared on annualized historical scale and discounted for a distant planning window. Published requests closing soon receive priority. Historical sizes are comparisons. The score is not a win probability or company revenue' priority_basis"
   if 'company_work_matches' in self.manifest()['tables']:
    join+=' LEFT JOIN '+self.table('company_work_matches')+' cw ON cw.company_id=m.company_id AND cw.episode_id=o.episode_id AND cw.opportunity_hash=o.record_hash'
    extra+=',cw.doc_id matched_doc_id,cw.work_text matched_work_text,cw.source_kind matched_source_kind,cw.last_observed matched_work_date,cw.review_category'
    if monitoring_only:where="(d.stage IN ('historical_watch','repeat_demand_inference') OR (cw.review_category='market_context' AND d.stage NOT IN ('resolved_or_elapsed','evidence_gap','planning_context'))) AND m.opportunity_hash=o.record_hash"
    else:where+=" AND (cw.review_category IS NULL OR cw.review_category<>'market_context')"
    if any(x['Name']=='company_entry_status' for x in self.manifest()['tables']['company_work_matches'].get('columns',[])):extra+=',cw.company_entry_status'
    if any(x['Name']=='fit_verdict' for x in self.manifest()['tables']['company_work_matches'].get('columns',[])):extra+=',cw.fit_verdict,cw.fit_reason'
    work_columns={x['Name'] for x in self.manifest()['tables']['company_work_matches'].get('columns',[])}
    for name in ('observed_delivery_mode','observed_work_psc','work_classification_evidence_json','mode_reason','shared_scope_terms_json'):
     if name in work_columns:extra+=',cw.'+name
   if 'company_item_matches' in self.manifest()['tables']:
    join+=' LEFT JOIN '+self.table('company_item_matches')+' ci ON ci.company_id=m.company_id AND ci.episode_id=o.episode_id AND ci.opportunity_hash=o.record_hash'
    extra+=',ci.niin item_reference_niin,ci.reference_description item_reference_description,ci.reference_part_number item_reference_part_number'
    for name in ('agency_approval_quote','agency_approval_source_url','agency_approval_part_number','agency_other_sources_json'):
     if any(x['Name']==name for x in self.manifest()['tables']['company_item_matches'].get('columns',[])):extra+=',ci.'+name
   if 'company_item_reference_summaries' in self.manifest()['tables']:
    join+=' LEFT JOIN '+self.table('company_item_reference_summaries')+' ir ON ir.company_id=m.company_id AND ir.episode_id=o.episode_id AND ir.opportunity_hash=o.record_hash'
    extra+=',ir.company_reference_count,ir.company_reference_digest,ir.company_reference_preview_json'
   if 'company_item_valuations' in self.manifest()['tables']:
    join+=' LEFT JOIN '+self.table('company_item_valuations')+' v ON v.company_id=m.company_id AND v.episode_id=o.episode_id AND v.record_hash=o.record_hash'
    extra+=',v.quantity forecast_quantity,v.catalogue_unit forecast_catalogue_unit,v.government_reference_unit_price,v.supplier_average_unit_price,v.government_reference_lot_scenario,v.supplier_average_lot_scenario,v.supplier_average_last_sale,v.monthly_evidence_json,v.catalogue_evidence_json'
    for name in ('market_price_low','market_price_median','market_price_high','market_observations','market_sellers','first_price_date','latest_price_date','market_price_lot_scenario','annual_scenarios_json','valuation_status','inventory_date','stock_on_hand','backorder_quantity','annual_demand_quantity','inventory_signal','reorder_date','reorder_point','reorder_stock','previous_reorder_gap','inventory_evidence_json','reorder_evidence_json','current_request_count','next_request_deadline','related_requests_json'):
     if name in valuation_columns:extra+=',v.'+name
   if 'company_notice_item_prices' in self.manifest()['tables']:
    join+=' LEFT JOIN '+self.table('company_notice_item_prices')+' np ON np.company_id=m.company_id AND np.episode_id=o.episode_id AND np.record_hash=o.record_hash'
    extra+=',np.items_json notice_item_pricing_json,np.interpretation notice_item_price_interpretation'
   if 'company_notice_source_evidence' in self.manifest()['tables']:
    join+=' LEFT JOIN (SELECT company_id,episode_id,json_format(cast(array_agg(cast(row(evidence_id,niin,part_number,rncc_codes,rnvc_codes,rnsc_codes,source_snapshot_sha256,source_row,interpretation) AS row(evidence_id varchar,niin varchar,part_number varchar,rncc_codes varchar,rnvc_codes varchar,rnsc_codes varchar,source_snapshot_sha256 varchar,source_row bigint,interpretation varchar))) AS json)) source_evidence_json FROM '+self.table('company_notice_source_evidence')+' GROUP BY company_id,episode_id) se ON se.company_id=m.company_id AND se.episode_id=o.episode_id'
    extra+=',se.source_evidence_json company_source_access_evidence_json'
  if quality:
   join+=' LEFT JOIN '+self.table('opportunity_quality_reviews')+' qr ON qr.episode_id=o.episode_id AND qr.record_hash=o.record_hash'
   extra+=self.quality_projection()
  where+=category_where
  where+=" AND NOT(o.source_name='market_work_history' AND regexp_like(coalesce(o.psc,''),'^[A-Z]') AND cardinality(split(coalesce(json_extract_scalar(o.value_inputs_json,'$.scope_key'),''),'|'))<3)"
  # Enforce exact source clocks at request time so a daily snapshot cannot keep
  # expired ISO deadlines open until the next nightly build. Date-only notices
  # retain calendar precision and do not acquire a midnight deadline.
  if not monitoring_only and not _materialize:
   where+=" AND (d.stage NOT IN ('published_requirement','qualification_opening','teaming_opening') OR try_cast(substr(o.window_end,1,10) AS date) IS NULL OR try_cast(substr(o.window_end,1,10) AS date)>=current_date)"
   where+=" AND (coalesce(json_extract_scalar(o.value_inputs_json,'$.source_response_deadline_precision'),'')<>'timestamp_utc' OR try(from_iso8601_timestamp(json_extract_scalar(o.value_inputs_json,'$.source_response_deadline_utc'))) IS NULL OR try(from_iso8601_timestamp(json_extract_scalar(o.value_inputs_json,'$.source_response_deadline_utc')))>=current_timestamp)"
  if 'dla_forecasts' in self.manifest()['tables']:
   asof=self.manifest().get('as_of')
   if not asof or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',asof):raise ValueError('Invalid release as-of date')
   # Negative monthly source quantities are unresolved data, not purchases or
   # offsets that can silently be netted into a positive sales scenario.
   join+=' LEFT JOIN (SELECT DISTINCT niin FROM '+self.table('dla_forecasts')+" WHERE forecast_qty<0 AND try_cast(substr(cast(forecast_month AS varchar),1,10) AS date)>=DATE '"+asof+"') fq ON fq.niin=json_extract_scalar(o.value_inputs_json,'$.niin')"
   where+=" AND (o.lane<>'source_item_forecasts' OR fq.niin IS NULL)"
  if vocabulary:
   join+=' LEFT JOIN '+self.table('opportunity_vocabulary')+' ov ON ov.episode_id=o.episode_id AND ov.record_hash=o.record_hash'
   extra+=',ov.normalized_scope_label,ov.topic_ids_json,ov.work_types_json,ov.annotation_json work_vocabulary_json,ov.vocabulary_version'
   for field in ('work_scope_topic_ids_json','context_topic_ids_json','psc_scope','work_psc','parent_prime_psc','parent_prime_psc_label'):
    if any(col['Name']==field for col in self.manifest()['tables']['opportunity_vocabulary'].get('columns',[])):extra+=',ov.'+field
   if topic:
    topics=self.table('work_vocabulary_topics')
    allowed="(SELECT array_agg(topic_id) FROM "+topics+" WHERE topic_id='"+topic+"' OR parent_id='"+topic+"' OR parent_id IN (SELECT topic_id FROM "+topics+" WHERE parent_id='"+topic+"'))"
    where+=' AND arrays_overlap(cast(json_parse(ov.topic_ids_json) AS array(varchar)),'+allowed+')'
  if psc:
   psc_field='ov.work_psc' if vocabulary and any(col['Name']=='work_psc' for col in self.manifest()['tables']['opportunity_vocabulary'].get('columns',[])) else 'o.psc'
   where+=" AND starts_with(coalesce("+psc_field+",''),'"+psc+"')"
  selected='SELECT DISTINCT '+('m.company_id serving_company_id,' if _materialize else '')+'o.*,d.stage,d.decision_json'+extra+' FROM '+o+' o JOIN '+d+' d ON o.episode_id=d.episode_id'+join+' WHERE '+where
  if _query and not _source_groups:return selected
  ordering=("CASE q.stage WHEN 'published_requirement' THEN 0 WHEN 'qualification_opening' THEN 1 WHEN 'teaming_opening' THEN 1 ELSE 2 END,CASE WHEN q.commercial_value>=1000000 THEN 0 ELSE 1 END,q.priority_score DESC," if company else '')+"CASE q.stage WHEN 'qualification_opening' THEN 0 WHEN 'teaming_opening' THEN 0 WHEN 'published_requirement' THEN 1 WHEN 'emerging_demand' THEN 2 ELSE 3 END,CASE WHEN q.lane='source_item_forecasts' THEN 1 ELSE 0 END,q.window_end ASC NULLS LAST,q.episode_id"
  # Reposted notices sometimes append -00 to the same procurement identifier.
  # Collapse only that narrow family with the same buyer and normalized title;
  # different solicitations for the same item remain separate. Keep every link.
  family="CASE WHEN c.source_name='sam_notices' AND length(c.native_identifier)>4 THEN concat_ws('|',c.source_name,upper(c.customer_name),regexp_replace(upper(c.native_identifier),'-00$',''),regexp_replace(upper(c.title),'[^A-Z0-9]','')) ELSE c.episode_id END"
  related="json_format(cast(array_agg(cast(row(c.episode_id,c.native_identifier,c.source_url,c.source_evidence_id,c.notice_posted_date) AS row(episode_id varchar,native_identifier varchar,source_url varchar,source_evidence_id varchar,posted_date varchar))) OVER (PARTITION BY "+family+") AS json)) related_source_records_json"
  grouped="WITH candidates AS ("+selected+"), families AS (SELECT c.*,row_number() OVER (PARTITION BY "+family+" ORDER BY c.notice_posted_date DESC NULLS LAST,c.episode_id) source_family_rank,"+related+" FROM candidates c) SELECT q.* FROM families q WHERE q.source_family_rank=1"
  if minimum_value>0:
   if not company:raise ValueError('A company is required for commercial-size filtering')
   grouped+=' AND q.commercial_value>='+str(float(minimum_value))
  if _query:return grouped
  grouped+=' ORDER BY '+ordering+' OFFSET '+str(offset)+' LIMIT '+str(limit)
  records=self.rows(grouped)
  for record in records:
   record.update(quantity_fact(record));record.update(price_scenario_fact(record))
  return [guard_finance(record,self.manifest()) for record in records]

 def _overview_window(self,result,total_items=None):
  import calendar
  start=datetime.date.fromisoformat(self.manifest()['as_of'])
  end=start.replace(year=start.year+1,day=min(start.day,calendar.monthrange(start.year+1,start.month)[1]))
  forecast=result['forecast']
  forecast.update(next_12_month_window_start=start.isoformat(),next_12_month_window_end_exclusive=end.isoformat(),price_basis='Exact-item observed market prices where available, otherwise government reference prices. Forecast units use the catalogue EA basis; these are planning estimates, not current quotes or company revenue.')
  if total_items is not None:forecast['matched_forecast_items']=int(total_items)
  from .company_index import number
  forecast['next_12_month_positive_priced_items']=sum((number(row.get('forecast_next_12m_value')) or 0)>0 for row in forecast.get('value_components',[]))
  return result

 def overview(self,cage):
  if not re.fullmatch('[A-Z0-9]{5}',cage):raise ValueError('Invalid CAGE')
  from .overview import summary
  payload=self.indexed_company('CAGE:'+cage)
  if payload is not None:
   from .company_index import overview
   result=overview(payload,self.manifest())
   # Match the existing warehouse summary projection. Full source bodies and
   # company matching payloads belong to evidence/card reads, not every total.
   fields=set(('annual_scenarios_json valuation_status market_price_median market_price_lot_scenario latest_price_date stock_on_hand inventory_date inventory_signal window_start window_end value_low value_high value_basis forecast_catalogue_unit episode_id record_hash title customer_name source_url source_evidence_id commercial_value commercial_value_kind item_demand item_reference_niin notice_item_pricing_json value_inputs_json forecast_next_12m_value included_value niin aggregation').split())
   for name in ('forecast','replenishment','planned_packages'):
    result[name]['value_components']=[{k:v for k,v in row.items() if k in fields} for row in result[name].get('value_components',[])]
   from .company_index import eligible
   return self._overview_window(result,sum(row.get('lane')=='source_item_forecasts' for row in eligible(payload)))
  query=self.opportunities('CAGE:'+cage,kind='all',_query=True)
  scale="(SELECT max(observed_prime_net_obligations) FROM "+self.table('company_history_coverage')+" WHERE company_id='CAGE:"+cage+"')" if 'company_history_coverage' in self.manifest()['tables'] else 'NULL'
  published_stage="stage IN ('published_requirement','qualification_opening','teaming_opening')"
  priced_kind="commercial_value_kind IN ('item_reference_estimate','historical_unit_price_estimate') AND commercial_value>0"
  count_sql="SELECT count_if(lane='source_item_forecasts') forecast_items_total,"+'count_if(NOT item_demand) work_count,count_if(item_demand) items_count,count_if(NOT item_demand AND commercial_value>=1000000) work_larger_count,count_if(item_demand AND commercial_value>=1000000) items_larger_count,'+scale+' observed_prime_net_obligations,'+   'count_if('+published_stage+') published_records_count,count_if(('+published_stage+') AND ('+priced_kind+')) published_priced_records_count,'+   "count_if(("+published_stage+") AND commercial_value_kind IN ('historical_comparison','historical_context_source_limit')) published_historical_guide_records_count,"+   'min(CASE WHEN '+published_stage+' THEN window_end END) published_first_deadline,max(CASE WHEN '+published_stage+' THEN window_end END) published_last_deadline FROM ('+query+')'
  forecast_columns={x['Name'] for x in self.manifest()['tables'].get('company_item_valuations',{}).get('columns',[])}
  forecast_extra=','.join(name if name in forecast_columns else 'NULL '+name for name in ('annual_scenarios_json','valuation_status','market_price_median','market_price_lot_scenario','latest_price_date','stock_on_hand','inventory_date','inventory_signal'))
  priced_sql="SELECT "+forecast_extra+",window_start,window_end,value_low,value_high,value_basis,forecast_catalogue_unit,episode_id,record_hash,title,customer_name,source_url,source_evidence_id,commercial_value,commercial_value_kind,item_demand,item_reference_niin,notice_item_pricing_json,value_inputs_json,forecast_next_12m_value FROM ("+query+") WHERE commercial_value_kind IN ('item_reference_estimate','historical_unit_price_estimate','forecast_scenario','annual_usage_scenario','planned_package_reference','recurring_demand_scenario','historical_package_benchmark') AND (commercial_value>0 OR commercial_value_kind='planned_package_reference')"
  from concurrent.futures import ThreadPoolExecutor
  with ThreadPoolExecutor(max_workers=3) as pool:
   count_task=pool.submit(self.rows,count_sql);price_task=pool.submit(self.rows,priced_sql);business_task=pool.submit(contextvars.copy_context().run,self.business_context,cage);counts=count_task.result();priced=price_task.result();business=business_task.result()
  result=dict(summary(counts[0] if counts else {},priced,self.manifest()['run_id'],business),company_id='CAGE:'+cage,cage_code=cage)
  return self._overview_window(result,(counts[0] if counts else {}).get('forecast_items_total',0))

 def market_work_evidence(self,episode_id,limit=25,offset=0,cage=None):
  if not re.fullmatch('[a-f0-9]{64}',episode_id) or not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid market-work evidence request')
  if cage is not None and not re.fullmatch('[A-Z0-9]{5}',cage):raise ValueError('Invalid evidence CAGE')
  if 'market_work_history' not in self.manifest()['tables']:return []
  if cage:
   return [guard_finance(r,self.manifest()) for r in self.rows('SELECT h.* FROM '+self.table('market_work_history')+' h JOIN '+self.table('company_work_matches')+" cw ON cw.company_id=h.company_id AND cw.sample_evidence_id=h.evidence_id WHERE cw.company_id='CAGE:"+cage+"' AND cw.episode_id='"+episode_id+"' ORDER BY h.event_date DESC,h.record_id OFFSET "+str(offset)+' LIMIT '+str(limit))]
  return [guard_finance(r,self.manifest()) for r in self.rows("SELECT h.*,CASE WHEN h.source_kind='prime_award' THEN h.amount END observed_net_obligations,CASE WHEN h.source_kind='subcontract_report' THEN h.amount END reported_amount,h.event_date first_action_date,h.last_observed latest_action_date,CASE WHEN starts_with(h.company_id,'CAGE:') THEN substr(h.company_id,6) END cage_code,h.customer_name buyer_name,h.customer_name prime_name FROM "+self.table('market_work_history')+' h JOIN '+self.table('opportunities')+" o ON h.cohort_id=json_extract_scalar(o.value_inputs_json,'$.cohort_id') WHERE o.episode_id='"+episode_id+"' AND (o.lane='funded_contract_follow_on' AND h.record_id=json_extract_scalar(o.value_inputs_json,'$.record_id') OR o.lane<>'funded_contract_follow_on' AND try_cast(substr(h.event_date,1,10) AS date) BETWEEN date_add('year',-3,try_cast(json_extract_scalar(o.value_inputs_json,'$.history_cutoff') AS date)) AND try_cast(json_extract_scalar(o.value_inputs_json,'$.history_cutoff') AS date)) ORDER BY h.event_date DESC,h.record_id OFFSET "+str(offset)+' LIMIT '+str(limit))]
 def demand_progression(self,episode_id,limit=25,offset=0):
  if not re.fullmatch('[a-f0-9]{64}',episode_id) or not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid progression request')
  if 'market_demand_progression' not in self.manifest()['tables']:return []
  return self.rows('SELECT * FROM '+self.table('market_demand_progression')+" WHERE episode_id='"+episode_id+"' ORDER BY relationship_kind,notice_episode_id OFFSET "+str(offset)+' LIMIT '+str(limit))

 def work_vocabulary(self,psc_prefix=None,limit=100,offset=0):
  if psc_prefix and not re.fullmatch('[A-Z0-9]{1,4}',psc_prefix):raise ValueError('Invalid PSC prefix')
  if not 1<=limit<=100 or not 0<=offset<=10000:raise ValueError('Invalid pagination')
  if 'work_vocabulary_reference' not in self.manifest()['tables']:return {'status':'not_available','topics':[],'psc_reference':[]}
  where=" WHERE starts_with(psc,'"+psc_prefix+"')" if psc_prefix else ''
  facet='facet' if any(c['Name']=='facet' for c in self.manifest()['tables']['work_vocabulary_topics'].get('columns',[])) else "cast(NULL AS varchar) facet"
  return dict(status='available',policy=self.manifest().get('work_vocabulary_policy'),topics=self.rows('SELECT topic_id,label,parent_id,vocabulary_version,'+facet+' FROM '+self.table('work_vocabulary_topics')+' ORDER BY parent_id NULLS FIRST,label'),psc_reference=self.rows('SELECT * FROM '+self.table('work_vocabulary_reference')+where+' ORDER BY psc OFFSET '+str(offset)+' LIMIT '+str(limit)),release=self.manifest()['run_id'])

 def capability_vocabulary(self,cage,limit=25,offset=0):
  if not re.fullmatch('[A-Z0-9]{5}',cage) or not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid company vocabulary request')
  if 'company_capability_vocabulary' not in self.manifest()['tables']:return []
  return self.rows('SELECT c.*,v.annotation_json FROM '+self.table('company_capability_vocabulary')+' c JOIN '+self.table('work_vocabulary_scopes')+" v ON v.scope_id=c.scope_id WHERE c.company_id='CAGE:"+cage+"' ORDER BY CASE c.scope_status WHEN 'explicit_scope_labels' THEN 0 WHEN 'reported_psc_only' THEN 1 WHEN 'context_only' THEN 2 WHEN 'unclassified_scope' THEN 3 ELSE 4 END,c.observed_records DESC,c.capability_id OFFSET "+str(offset)+' LIMIT '+str(limit))

 def matching_profile(self,cage,limit=25,offset=0):
  if not re.fullmatch('[A-Z0-9]{5}',cage) or not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid matching profile request')
  if 'company_matching_profiles' not in self.manifest()['tables']:return {'profile':None,'capabilities':[]}
  profile=self.rows('SELECT * FROM '+self.table('company_matching_profiles')+" WHERE company_id='CAGE:"+cage+"'")
  capabilities=self.rows('SELECT * FROM '+self.table('company_capability_history')+" WHERE company_id='CAGE:"+cage+"' ORDER BY observed_records DESC,capability_id OFFSET "+str(offset)+' LIMIT '+str(limit))
  observed=self.rows('SELECT * FROM '+self.table('company_profiles')+" WHERE company_id='CAGE:"+cage+"'") if 'company_profiles' in self.manifest()['tables'] else []
  return dict(profile=profile[0] if profile else None,observed_profile=observed[0] if observed else None,capabilities=capabilities,release=self.manifest()['run_id'])

 def item_source_references(self,cage,eid,limit=25,offset=0):
  if not re.fullmatch('[A-Z0-9]{5}',cage) or not re.fullmatch('[a-f0-9]{64}',eid) or not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid item source evidence request')
  if 'company_item_reference_evidence' not in self.manifest()['tables']:return []
  return self.rows('SELECT * FROM '+self.table('company_item_reference_evidence')+" WHERE company_id='CAGE:"+cage+"' AND episode_id='"+eid+"' ORDER BY part_number,evidence_id OFFSET "+str(offset)+' LIMIT '+str(limit))

 def business_context(self,cage):
  if not re.fullmatch('[A-Z0-9]{5}',cage):raise ValueError('Invalid CAGE')
  selects=[];joins=[]
  for alias,name in [('a','company_activity_baselines'),('c','company_catalogue_sales'),('l','company_reporting_lag_baselines'),('p','company_product_market_coverage')]:
   if name not in self.manifest()['tables']:continue
   cols=[x['Name'] for x in self.manifest()['tables'][name]['columns'] if x['Name'] not in ('company_id','company_name')]
   if not all(re.fullmatch('[a-z_0-9]+',x) for x in cols):raise ValueError('Invalid business-context projection')
   selects.extend(alias+'.'+x for x in cols);joins.append(' LEFT JOIN '+self.table(name)+' '+alias+' ON '+alias+'.company_id=k.company_id')
  if not selects:return {}
  rows=self.rows("SELECT "+','.join(selects)+" FROM (SELECT 'CAGE:"+cage+"' company_id) k"+''.join(joins))
  return rows[0] if rows else {}

 def business_evidence(self,cage,kind='actions',limit=25,offset=0):
  if not re.fullmatch('[A-Z0-9]{5}',cage) or kind not in ('actions','lag-adjusted-actions','catalogue-sales','item-references') or not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid business evidence request')
  name={'actions':'company_activity_actions','lag-adjusted-actions':'company_reporting_lag_actions','catalogue-sales':'catalogue_sales_evidence','item-references':'item_reference_facts'}[kind]
  if name not in self.manifest()['tables']:return []
  order='niin,part_number,evidence_id' if kind=='item-references' else 'transaction_date DESC,pricing_record_id' if kind=='catalogue-sales' else 'action_date DESC,transaction_key'
  identity="cage_code='"+cage+"'" if kind=='item-references' else "company_id='CAGE:"+cage+"'"
  return self.rows('SELECT * FROM '+self.table(name)+' WHERE '+identity+' ORDER BY '+order+' OFFSET '+str(offset)+' LIMIT '+str(limit))

 def related_item_requests(self,eid,limit=25,offset=0):
  if not re.fullmatch('[a-f0-9]{64}',eid) or not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid item request relationship')
  if 'item_request_progression' not in self.manifest()['tables']:return []
  return self.rows('SELECT * FROM '+self.table('item_request_progression')+" WHERE forecast_episode_id='"+eid+"' ORDER BY response_deadline,request_episode_id OFFSET "+str(offset)+' LIMIT '+str(limit))

 def company_markets(self,cage,level='psc',limit=8):
  from .company_markets import query
  sql=query(self,cage,level,limit)
  payload=self.indexed_company('CAGE:'+cage)
  if payload is not None and 'markets' in payload and level=='psc' and limit<=8:return payload['markets'][:limit]
  return self.rows(sql)

 def related_item_notices(self,eid,limit=25,offset=0):
  if not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid pagination')
  from .item_notice_activity import query
  return self.rows(query(self,eid)+' ORDER BY notice_posted_date DESC NULLS LAST,notice_episode_id OFFSET '+str(offset)+' LIMIT '+str(limit))

 def item_market_prices(self,eid,limit=25,offset=0):
  if not re.fullmatch('[a-f0-9]{64}',eid) or not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid item price request')
  if 'item_market_price_evidence' not in self.manifest()['tables']:return []
  return self.rows('SELECT p.* FROM '+self.table('item_market_price_evidence')+' p JOIN '+self.table('opportunities')+" o ON p.niin=coalesce(json_extract_scalar(o.value_inputs_json,'$.niin'),substr(json_extract_scalar(o.value_inputs_json,'$.nsn'),5,9)) WHERE o.episode_id='"+eid+"' ORDER BY p.transaction_date DESC,p.pricing_record_id OFFSET "+str(offset)+' LIMIT '+str(limit))

 def detail(self,eid):
  if not re.fullmatch('[a-f0-9]{64}',eid):raise ValueError('Invalid opportunity identifier')
  extra='';join=''
  if 'opportunity_quality_reviews' in self.manifest()['tables']:
   join=' LEFT JOIN '+self.table('opportunity_quality_reviews')+' qr ON qr.episode_id=o.episode_id AND qr.record_hash=o.record_hash'
   extra=self.quality_projection()
  if 'opportunity_vocabulary' in self.manifest()['tables']:
   join+=' LEFT JOIN '+self.table('opportunity_vocabulary')+' ov ON ov.episode_id=o.episode_id AND ov.record_hash=o.record_hash'
   extra+=',ov.normalized_scope_label,ov.topic_ids_json,ov.work_types_json,ov.annotation_json work_vocabulary_json,ov.vocabulary_version'
   for field in ('work_scope_topic_ids_json','context_topic_ids_json','psc_scope','work_psc','parent_prime_psc','parent_prime_psc_label'):
    if any(col['Name']==field for col in self.manifest()['tables']['opportunity_vocabulary'].get('columns',[])):extra+=',ov.'+field
  if 'company_notice_item_prices' in self.manifest()['tables']:
   # New global item references are identical across candidates; legacy
   # company-specific rows must never be promoted to global source evidence.
   join+=" LEFT JOIN (SELECT DISTINCT episode_id,record_hash,items_json,interpretation FROM "+self.table('company_notice_item_prices')+" WHERE json_extract_scalar(items_json,'$[0].price_scope')='global_exact_niin') np ON np.episode_id=o.episode_id AND np.record_hash=o.record_hash"
   extra+=',np.items_json notice_item_pricing_json,np.interpretation notice_item_price_interpretation'
  records=self.rows('SELECT o.*,d.stage,d.decision_json'+extra+' FROM '+self.table('opportunities')+' o JOIN '+self.table('demand_decisions')+" d ON o.episode_id=d.episode_id"+join+" WHERE o.episode_id='"+eid+"'")
  for record in records:
   record.update(quantity_fact(record));record.update(price_scenario_fact(record))
  return [guard_finance(record,self.manifest()) for record in records]
 def company_match_review(self,cage,eid):
  if not re.fullmatch('[A-Z0-9]{5}',cage) or not re.fullmatch('[a-f0-9]{64}',eid):raise ValueError('Invalid company/source identifier')
  if 'company_match_assessments' not in self.manifest()['tables']:return []
  return self.rows("""SELECT a.*,o.title,o.source_url,d.stage source_stage,
  CASE WHEN a.source_verdict<>'usable' OR d.stage IN ('evidence_gap','resolved_or_elapsed') THEN 'source_not_actionable'
   WHEN a.verdict='rejected' THEN 'not_recommended' WHEN a.verdict='review' OR cw.review_category='market_context' THEN 'review_required'
   WHEN m.match_id IS NULL THEN 'not_currently_served' WHEN d.stage IN ('historical_watch','repeat_demand_inference','planning_context') THEN 'context_only'
   ELSE 'current_scope_match_access_unconfirmed' END engine_company_fit_status
  FROM """+self.table('company_match_assessments')+' a JOIN '+self.table('opportunities')+' o ON a.episode_id=o.episode_id AND a.opportunity_hash=o.record_hash JOIN '+self.table('company_profiles')+' p ON p.company_id=a.company_id AND p.profile_hash=a.profile_hash JOIN '+self.table('demand_decisions')+' d ON d.episode_id=o.episode_id LEFT JOIN '+self.table('company_matches')+' m ON m.company_id=a.company_id AND m.episode_id=a.episode_id AND m.opportunity_hash=o.record_hash LEFT JOIN '+self.table('company_work_matches')+" cw ON cw.company_id=a.company_id AND cw.episode_id=a.episode_id AND cw.opportunity_hash=o.record_hash WHERE a.company_id='CAGE:"+cage+"' AND a.episode_id='"+eid+"' LIMIT 1")
 def research(self,cage,limit=20):
  if not re.fullmatch('[A-Z0-9]{5}',cage):raise ValueError('Invalid CAGE')
  if not 1<=limit<=100:raise ValueError('Invalid limit')
  return self.rows('SELECT award_id,company_id,company_name,title,phase,program_type,topic_code,branch,award_date,end_date,observed_research_award_usd,value_interpretation,source_url,source_sha256,company_link_basis FROM '+self.table('research_awards')+" WHERE evidence_state='observed_research_activity' AND cage_code='"+cage+"' ORDER BY award_date DESC,award_id LIMIT "+str(limit))
 def research_calls(self,cage,limit=20):
  if not re.fullmatch('[A-Z0-9]{5}',cage):raise ValueError('Invalid CAGE')
  return self.rows('SELECT l.*,r.match_basis,r.applicant_eligibility FROM '+self.table('company_research_calls')+' r JOIN '+self.table('research_notice_links')+" l ON r.notice_id=l.notice_id AND r.topic_code=l.topic_code WHERE r.company_id='CAGE:"+cage+"' ORDER BY l.notice_id LIMIT "+str(max(1,min(limit,100))))


 def topics(self,cage=None,limit=20):
  if not 1<=limit<=100:raise ValueError('Invalid limit')
  join='';where=''
  if cage:
   if not re.fullmatch('[A-Z0-9]{5}',cage):raise ValueError('Invalid CAGE')
   join=' JOIN '+self.table('company_topic_matches')+" m ON m.topic_id=t.topic_id AND m.company_id='CAGE:"+cage+"'";where=',m.evidence_json,m.score,m.applicant_eligibility,m.technical_qualification'
  return self.rows('SELECT t.*'+where+' FROM '+self.table('research_topics_current')+' t'+join+' ORDER BY t.close_date,t.topic_id LIMIT '+str(limit))
 def work_evidence(self,doc_id):
  if not re.fullmatch('[a-f0-9]{64}',doc_id):raise ValueError('Invalid document identifier')
  return self.rows('SELECT * FROM '+self.table('company_work')+" WHERE doc_id='"+doc_id+"' LIMIT 1")
 def work_sources(self,doc_id,limit=25,offset=0):
  if not re.fullmatch('[a-f0-9]{64}',doc_id):raise ValueError('Invalid document identifier')
  if not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid pagination')
  docs=self.work_evidence(doc_id)
  if not docs:return []
  doc=docs[0];kind=doc.get('source_kind');cid=doc.get('company_id') or ''
  if kind not in ('prime_award','latest_prime_action','subaward_report') or not re.fullmatch(r'(?:CAGE:[A-Z0-9]{5}|UEI:[A-Z0-9]{12})',cid):return []
  name='company_subcontract_facts' if kind=='subaward_report' else 'company_award_facts'
  if name not in self.manifest()['tables']:return []
  if kind=='subaward_report':condition="a.company_id='"+cid+"'";ordering='a.event_date DESC,a.report_key'
  else:
   condition=("a.cage_code='"+cid[5:]+"'") if cid.startswith('CAGE:') else ("a.uei='"+cid[4:]+"'")
   ordering='a.latest_action_date DESC,a.award_key'
   for field in ('psc','naics'):
    condition+=" AND coalesce(a."+field+",'')='"+str(doc.get(field) or '').replace("'","''")+"'"
  field='latest_description' if kind=='latest_prime_action' else 'work_text'
  norm=lambda value:"regexp_replace(upper(regexp_replace(coalesce("+value+",''),'^[0-9]{8,12}!\\s*','')),'[^A-Z0-9]','')"
  text="'"+str(doc.get('work_text') or '').replace("'","''")+"'"
  condition+=' AND '+norm('a.'+field)+'='+norm(text)
  return [guard_finance(r,self.manifest(),source_report=kind=='subaward_report') for r in self.rows('SELECT a.* FROM '+self.table(name)+' a WHERE '+condition+' ORDER BY '+ordering+' OFFSET '+str(offset)+' LIMIT '+str(limit))]
 def price_lines(self,episode_id):
  if not re.fullmatch('[a-f0-9]{64}',episode_id):raise ValueError('Invalid opportunity identifier')
  return self.rows('SELECT * FROM '+self.table('notice_price_links')+" WHERE episode_id='"+episode_id+"' ORDER BY pricing_record_id LIMIT 100")

 def value_evidence(self,episode_id):
  rows=self.detail(episode_id)
  if not rows:return {}
  o=rows[0]
  return dict(financial_validation=o.get('financial_validation'),financial_validation_reason=o.get('financial_validation_reason'),unvalidated_financial_evidence_json=o.get('unvalidated_financial_evidence_json'),episode_id=episode_id,record_hash=o['record_hash'],value_low=o.get('value_low'),value_median=o.get('value_median'),value_high=o.get('value_high'),value_basis=o.get('value_basis'),value_scope=o.get('value_scope'),inputs=rules.decode(o.get('value_inputs_json')),source_url=o.get('source_url'),source_evidence_id=o.get('source_evidence_id'))

 def supplier_evidence(self,episode_id,kind='primes',limit=25,offset=0):
  if not re.fullmatch('[a-f0-9]{64}',episode_id):raise ValueError('Invalid opportunity identifier')
  names={'primes':'supplier_pull_prime_evidence','reports':'supplier_pull_report_evidence','placements':'supplier_pull_placement_evidence'}
  if kind not in names or not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid evidence request')
  name=names[kind]
  if name not in self.manifest()['tables']:return []
  ordering='e.first_action_date DESC,e.award_key' if kind=='primes' else 'e.event_date DESC,e.evidence_id'
  selection='e.*';join=''
  profile_columns={c['Name'] for c in self.manifest()['tables'].get('company_profiles',{}).get('columns',[])}
  if kind!='primes' and 'role_assessment' in profile_columns:
   join=' LEFT JOIN '+self.table('company_profiles')+' p ON p.company_id=e.supplier_company_id'
   for column in ('role_assessment','manufacturer_declared_awards','manufacturing_naics_awards','wholesale_naics_awards','manufacturer_uses_distributors_rows','manufacturing_data_rights','manufacturing_source_digest','profile_hash'):
    if column in profile_columns:selection+=',p.'+column+' supplier_'+column
  return [guard_finance(r,self.manifest(),source_report=kind!='primes') for r in self.rows('SELECT '+selection+' FROM '+self.table(name)+" e"+join+" WHERE e.episode_id='"+episode_id+"' ORDER BY "+ordering+' OFFSET '+str(offset)+' LIMIT '+str(limit))]

 def cohort_awards(self,cohort_id,limit=25,offset=0):
  if not re.fullmatch('[a-f0-9]{64}',cohort_id):raise ValueError('Invalid value cohort')
  if not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid pagination')
  if 'contract_value_observations' not in self.manifest()['tables']:return []
  return self.rows('SELECT * FROM '+self.table('contract_value_observations')+" WHERE cohort_id='"+cohort_id+"' ORDER BY latest_action_date DESC,award_key OFFSET "+str(offset)+' LIMIT '+str(limit))

 def company_history(self,cage,kind,limit=50):
  if not re.fullmatch('[A-Z0-9]{5}',cage):raise ValueError('Invalid CAGE')
  if kind not in ('awards','subcontracts','coverage'):raise ValueError('Invalid history kind')
  name={'awards':'company_award_facts','subcontracts':'company_subcontract_facts','coverage':'company_history_coverage'}[kind]
  if name not in self.manifest()['tables']:return []
  ordering={'awards':'latest_action_date DESC,award_key','subcontracts':'event_date DESC,report_key','coverage':'company_id'}[kind]
  field='cage_code' if kind=='awards' and self.manifest()['tables'][name].get('remote_binding') and not any(c['Name']=='company_id' for c in self.manifest()['tables'][name]['columns']) else 'company_id'
  value=cage if field=='cage_code' else 'CAGE:'+cage
  return [guard_finance(r,self.manifest(),source_report=kind=='subcontracts') for r in self.rows('SELECT * FROM '+self.table(name)+" WHERE "+field+"='"+value+"' ORDER BY "+ordering+' LIMIT '+str(max(1,min(limit,100))))]

 def company_events(self,cage,episode_id=None,limit=50,offset=0):
  if not re.fullmatch('[A-Z0-9]{5}',cage):raise ValueError('Invalid CAGE')
  if episode_id is not None and not re.fullmatch('[a-f0-9]{64}',episode_id):raise ValueError('Invalid opportunity identifier')
  if not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid pagination')
  if 'company_opportunity_events' not in self.manifest()['tables']:return []
  where="e.company_id='CAGE:"+cage+"'"+(" AND e.episode_id='"+episode_id+"'" if episode_id else '')
  return [guard_finance(r,self.manifest()) for r in self.rows('SELECT e.*,o.lane FROM '+self.table('company_opportunity_events')+' e LEFT JOIN '+self.table('opportunities')+' o ON o.episode_id=e.episode_id'+' WHERE '+where+' ORDER BY event_date DESC,release_id DESC,event_id OFFSET '+str(offset)+' LIMIT '+str(limit))]
 def award(self,award_id):
  if not re.fullmatch('[a-f0-9]{64}',award_id):raise ValueError('Invalid award identifier')
  return self.rows('SELECT * FROM '+self.table('research_awards')+" WHERE award_id='"+award_id+"'")
 def benchmark(self,topic_id):
  if not re.fullmatch('[0-9]{1,12}',topic_id):raise ValueError('Invalid topic identifier')
  return self.rows('SELECT * FROM '+self.table('topic_historical_benchmarks')+" WHERE topic_id='"+topic_id+"'")

def create_router(verify_principal,repository=None):
 if verify_principal is None:raise ValueError('A verified-principal dependency is required; header presence is not authentication')
 from fastapi import APIRouter,Depends,HTTPException,Query
 from starlette.concurrency import run_in_threadpool
 repo=repository or Repository()
 def require_principal(principal=Depends(verify_principal)):
  if not principal or isinstance(principal,bool):raise HTTPException(status_code=401,detail='Verified principal required')
  return principal
 async def pin_source_release():
  # The deployed mount defers AWS initialization through LazyRepository. Pin
  # the concrete instance that its forwarded methods actually read from.
  resolve=getattr(type(repo),'resolve_repository',None)
  concrete=await run_in_threadpool(resolve,repo) if callable(resolve) else repo
  if not isinstance(concrete,Repository):
   yield;return
  # Pin metadata for the entire request, including slow Athena reads and the
  # response release tag. Atomic publication must not mix table generations.
  manifest=await run_in_threadpool(concrete.manifest);token=_REQUEST_RELEASE.set((concrete,manifest))
  try:yield
  finally:_REQUEST_RELEASE.reset(token)
 router=APIRouter(prefix='/opportunity-intelligence',tags=['opportunity-intelligence'],dependencies=[Depends(require_principal),Depends(pin_source_release)])
 def call(fn,*args):
  try:return fn(*args)
  except ValueError as e:raise HTTPException(status_code=400,detail=str(e))
  except IndexUnavailable:raise HTTPException(status_code=503,detail='Company feed is temporarily unavailable')
 @router.get('/companies')
 def companies(q:str=Query(...,min_length=1,max_length=100),limit:int=Query(20,ge=1,le=100)):return {'records':call(repo.companies,q,limit)}
 @router.get('/companies/{cage}')
 def company(cage:str):
  records=call(repo.company,cage)
  if not records:raise HTTPException(status_code=404,detail='CAGE not found in the bound sources')
  return records[0]
 @router.get('/companies/{cage}/opportunities/{episode_id}/item-source-references')
 def item_source_references(cage:str,episode_id:str,limit:int=Query(25,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000)):
  return {'records':call(repo.item_source_references,cage,episode_id,limit,offset),'release':repo.manifest()['run_id'],'interpretation':'Exact selected CAGE and NIIN catalogue part references. Codes are retained as recorded; they do not by themselves establish current manufacturing data rights, notice approval or interchangeability.'}
 @router.get('/companies/{cage}/business-evidence')
 def business_evidence(cage:str,kind:str='actions',limit:int=Query(25,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000)):return {'records':call(repo.business_evidence,cage,kind,limit,offset),'release':repo.manifest()['run_id'],'kind':kind,'interpretation':'Observed transactions or physical product-market purchase lines. These historical amounts are not added to opportunity value; exact part references do not prove the delivered manufacturer.'}
 @router.get('/companies/{cage}/matching-profile')
 def matching_profile(cage:str,limit:int=Query(25,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000)):return call(repo.matching_profile,cage,limit,offset)
 @router.get('/companies/{cage}/capability-vocabulary')
 def capability_vocabulary(cage:str,limit:int=Query(25,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000)):
  return {'records':call(repo.capability_vocabulary,cage,limit,offset),'release':repo.manifest()['run_id'],'interpretation':'Reported PSC and source-backed work concepts; classification is not qualification.'}
 @router.get('/work-vocabulary')
 def work_vocabulary(psc_prefix:str=None,limit:int=Query(100,ge=1,le=100),offset:int=Query(0,ge=0,le=10000)):
  return call(repo.work_vocabulary,psc_prefix,limit,offset)
 @router.get('/opportunities/{episode_id}/related-item-notices')
 def related_item_notices(episode_id:str,limit:int=Query(25,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000)):
  return {'records':call(repo.related_item_notices,episode_id,limit,offset),'release':repo.manifest()['run_id'],'interpretation':'Current and earlier notices for the exact item. Quantities and statuses remain separate from forecasts.'}
 @router.get('/companies/{cage}/markets')
 def company_markets(cage:str,level:str='psc',limit:int=Query(8,ge=1,le=25)):
  return {'records':call(repo.company_markets,cage,level,limit),'release':repo.manifest()['run_id'],'classification_level':level,'cage_code':cage,'scope':'Selected CAGE, held DoD obligations in the reporting-lag-adjusted year. Award PSC classification is not a verified capability or opportunity.'}
 @router.get('/companies/{cage}/overview')
 def overview(cage:str):return call(repo.overview,cage)
 @router.get('/companies/{cage}/relationships')
 def relationships(cage:str,limit:int=Query(50,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000)):
  m=repo.manifest();table=m['tables'].get('company_registry',{})
  return {'records':call(repo.relationships,cage,limit,offset),'release':m['run_id'],'availability':'available' if table else 'not_published','source_provenance':{'table':'company_registry','table_sha256':table.get('sha256'),'sources':{k:v for k,v in m.get('source_status',{}).items() if k in ('cage_registry','sam_company_capabilities')}},'interpretation':'Exact registry relationship candidates from parent_cage or replacement_cage. Replacements are not ownership. No ultimate-parent consolidation, name similarity or transfer of technical qualifications is inferred.'}
 @router.get('/companies/{cage}/opportunities/{episode_id}/match-review')
 def company_match_review(cage:str,episode_id:str):
  m=repo.manifest()
  return {'records':call(repo.company_match_review,cage,episode_id),'release':m['run_id'],'availability':'available' if 'company_match_assessments' in m['tables'] else 'not_published','interpretation':'Current engine assessment for this exact company and source/profile version, including rejected candidates. This is not technical qualification, applicant eligibility or a user-owned assessment. An empty assessment does not establish absence of capability; item-reference matching is separate.'}
 @router.get('/innovation-signals')
 def innovation(limit:int=Query(20,ge=1,le=100)):return {'records':call(repo.innovation,limit),'status':'Published agency engagement evidence; no company qualification or award probability inferred'}
 @router.get('/methodology')
 def methodology():
  m=repo.manifest();return dict(m['rules'],release=m['run_id'],financial_readiness=finance_status(m),processing_logic_sha256=m.get('processing_logic_sha256'),processing_logic_files=m.get('processing_logic_files',{}),market_demand_policy=m.get('market_demand_policy'),work_vocabulary_policy=m.get('work_vocabulary_policy'),future_work_methods={'recurring_buying':'At least three observed awards across three buying months and a year of history establish an observed buying stream. Conditional 12/36-month values require at least three completed prime packages; cadence and package-size assumptions remain linked. This is not a calibrated award probability.', 'contract_follow_on':'A funded contract with a recorded future end supports account planning. The engagement window starts up to two years before that recorded end, capped at the assessment month; this is an illustrative planning lead, not an estimated solicitation date. Historical package value is a size guide, not forecast company revenue.', 'subcontract_work':'Revision-resolved reported work at multiple parent awards can establish repeat downstream buying. Reported subcontract amounts remain historical package guides; they are not projected prime-contract workshare.', 'notice_progression':'An exact incumbent-contract reference and a candidate same-office/scope association are recorded separately. Neither independently confirms a successor procurement or closes the historical buying stream.', 'normalization':'Complete meaningful scope terms, PSC and work mode form conservative exact capability signatures. Classification overlap alone does not establish commercial fit, and hardware qualification is not inherited.'},annual_activity_policy=m.get('annual_activity_policy'),reporting_lag_policy=m.get('reporting_lag_policy'),agency_approved_notice_policy=m.get('agency_approved_notice_policy'),request_presence_policy=m.get('item_request_presence_policy'),item_demand_policy=m.get('item_demand_policy'),product_market_policy=m.get('product_market_policy'),replenishment_policy=m.get('replenishment_policy'),agency_forecast_policy=m.get('agency_forecast_policy'),additional_source_methods={'linked_market':'Direct observed obligations plus other-seller exact item-identifying part purchases in the same stated period. Gross coverage reference only; no revenue attribution or missing-month annualization.','annual_usage':'Recent public DLA stock pressure and positive annual-demand quantity, consistent catalogue EA and exact pricing; excludes any future source forecast row. Conditional gross usage, not a request or net new procurement.','agency_plans':'Official requirement-level source bands and fiscal periods; no invented midpoint or response deadline. Exact unique-site prior contracts establish an incumbent-retention route. Restricted or unqualified hardware comparisons remain review context.'},commercial_ranking={'policy':'commercial-materiality-v4','inventory_policy':INVENTORY_PRIORITY_POLICY,'inventory_policy_sha256':INVENTORY_PRIORITY_POLICY_HASH,'rule':'Size, observed fit and engagement timing inform priority. Forecast priority uses next-twelve-month priced demand, while the complete stated horizon remains visible. Published requests due within thirty days receive a timing boost. For forecasts only, nonfuture stock snapshots no older than ninety days add ten points for reported backorders or six for stock below the stated reorder point; stock covering next-year forecast quantity reduces priority by eight points. Values are unchanged and no net new purchases are inferred. Historical size guides are not counted in demand totals.','default_size_focus':'Sites above $100m of observed annual obligations or aligned linked-part market reference focus on values above 1% of the larger reference, with a $1m floor. Where annual evidence is smaller or missing, $100m lifetime obligations retain a $1m focus. All smaller and unpriced prospects remain available. A 3x annual-activity coverage benchmark diagnoses shortfalls; it does not create demand.'},company_matching={'evidence':'Observed award and reported supplier work, exact item references, program/vehicle links and specific technical scope. Classification overlap alone is insufficient.','entry_routes':'Named prior suppliers, defined service work or explicit development/source-opening context take priority. Other hardware similarity is market context until an entry route is evidenced.','qualification':'A company/item association is separate from manufacturing rights, procurement approval, current configuration, eligibility and capacity.'},source_quality={'policy_version':m.get('quality_policy_version'),'policy_hash':m.get('source_quality_logic_sha256'),'source_conflicts':'Conflicting title/nomenclature or past response dates without a current deadline override are held for source review, with quoted passages.','company_scope':'Administrative phrases, postal-location fragments, unspecific labels and a single shared product/work word cannot establish the complete deliverable. Matching words in incidental body text are insufficient.','delivery_mode':'Component supply, repair/overhaul, equipment installation, onsite services and facility construction are assessed separately. Selected delivery statements and their exact document PSC are retained. A manufacturing NAICS or similar product noun does not establish onsite-service capability.','psc_reference':'https://origin-www.acquisition.gov/sites/default/files/manual/PSC%20Manual%20April%202025.pdf','item_access':'Existing-OEM sourcing is distinct from an invitation to qualify a new manufacturer. Exact named part, NIIN and nonconflicting procurement reference codes are retained separately from manufacturing data rights.','research_eligibility':'Research relevance is separate from SBIR/STTR applicant eligibility or a permitted teaming route.','plan_precedence':'A current official plan matched to this company and the exact incumbent award replaces its generic inferred follow-on in the served feed. Both evidence records are retained.', 'notice_grouping':'The same buyer, complete normalized title and identifier with a trailing -00 variant may be grouped for display. Every notice source is retained; different procurement identifiers remain separate and values are not summed.'},values={'source_quantities':'A quoted explicit quantity is exposed only for a single-item notice with one consistent quantity and unit. Estimates are not orders; ranges, multiple quantities and annual/monthly rates are not collapsed. No total is inferred from a different NSN or part.', 'historical_benchmarks':'Revision-resolved award net obligations and reported outsourced-package amounts. Approximate median and observed range use the complete comparison group; recent/extreme awards form a short preview.','unit_prices':'Exact notice/item reference prices can come from another supplier and remain separate from candidate eligibility. Quoted single-item quantities and matching units produce reference demand estimates. Exact company/item reference prices are unit evidence. Missing quantities do not produce totals. Forecast quantity × price is an alternative gross scenario, not company revenue.','allocation':'Prime/program funding is context. Company-addressable value remains unallocated without workshare evidence.','source_quality':'Item forecasts with negative future monthly quantities are withheld from actionable feeds until reconciled; published purchase requests remain separate evidence.'})
 @router.get('/opportunities')
 def opportunities(company:str=None,limit:int=Query(20,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000),kind:str=Query('all',pattern='^(all|work|items)$'),min_value:float=Query(0,ge=0,le=1000000000000),topic:str=None,psc:str=None):
  return {'records':call(repo.opportunities,company,limit,offset,False,kind,min_value,False,topic,psc),'release':repo.manifest()['run_id'],'status':'authenticated_evidence_feed','eligibility':'Company fit, access and qualification require evidence; these are not win probabilities.'}
 @router.get('/companies/{cage}/monitoring')
 def monitoring(cage:str,limit:int=Query(20,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000)):
  if not re.fullmatch('[A-Z0-9]{5}',cage):raise HTTPException(status_code=400,detail='Invalid CAGE')
  return {'records':call(repo.opportunities,'CAGE:'+cage,limit,offset,True),'release':repo.manifest()['run_id'],'status':'Market context and historical signals; company entry route or fresh demand remains unestablished'}
 @router.get('/opportunities/{episode_id}')
 def opportunity(episode_id:str):
  r=call(repo.detail,episode_id)
  if not r:raise HTTPException(status_code=404,detail='Opportunity not found')
  return dict(r[0],release=repo.manifest()['run_id'],source_check_scope='Demand evidence only; company fit, access and eligibility are not revalidated by this company-independent endpoint')
 @router.get('/companies/{cage}/research')
 def research(cage:str,limit:int=Query(20,ge=1,le=100)):return {'awards':call(repo.research,cage,limit),'topic_calls':call(repo.research_calls,cage,limit)}
 @router.get('/opportunities/{episode_id}/progression')
 def progression(episode_id:str,limit:int=Query(25,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000)):return {'records':call(repo.demand_progression,episode_id,limit,offset),'release':repo.manifest()['run_id'],'interpretation':'Explicit contract references and candidate buyer/scope associations remain distinct. A linked notice does not independently establish a successor award or company eligibility.'}
 @router.get('/opportunities/{episode_id}/work-evidence')
 def market_work_evidence(episode_id:str,limit:int=Query(25,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000),cage:str=None):return {'records':call(repo.market_work_evidence,episode_id,limit,offset,cage),'release':repo.manifest()['run_id'],'interpretation':'Actual award and subcontract history for this exact buyer/work stream. Conditional projections remain separate from observations and company revenue.'}
 @router.get('/opportunities/{episode_id}/supplier-evidence')
 def supplier_evidence(episode_id:str,kind:str='primes',limit:int=Query(25,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000)):
  return {'records':call(repo.supplier_evidence,episode_id,kind,limit,offset),'release':repo.manifest()['run_id'],'kind':kind,'interpretation':'Complete source lineage, paginated. Role indicators reuse the exact company profile in this release; they are company-level evidence, not proof of manufacturing or approval for this package. Performance locations are not assumed to be procurement or production sites.'}
 @router.get('/opportunities/{episode_id}/related-requests')
 def related_item_requests(episode_id:str,limit:int=Query(25,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000)):
  return {'records':call(repo.related_item_requests,episode_id,limit,offset),'release':repo.manifest()['run_id'],'interpretation':'Same-item published requests, not proof of forecast conversion, an award or additional additive demand. Each request retains its own deadline, quantity, unit, source and eligibility.'}
 @router.get('/opportunities/{episode_id}/item-market-prices')
 def item_market_prices(episode_id:str,limit:int=Query(25,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000)):
  return {'records':call(repo.item_market_prices,episode_id,limit,offset),'release':repo.manifest()['run_id'],'interpretation':'Exact NIIN dated procurement price lines, not manufacturing competitors or current quotes. Physical financial lines are deduplicated; each row retains source identifiers and location.'}
 @router.get('/opportunities/{episode_id}/price-evidence')
 def prices(episode_id:str):return {'records':call(repo.price_lines,episode_id),'release':repo.manifest()['run_id'],'interpretation':'Historical line evidence; no current requirement total or company value is inferred'}
 @router.get('/opportunities/{episode_id}/value-evidence')
 def values(episode_id:str):return dict(call(repo.value_evidence,episode_id),release=repo.manifest()['run_id'])
 @router.get('/value-cohorts/{cohort_id}/awards')
 def cohort_awards(cohort_id:str,limit:int=Query(25,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000)):
  return {'records':call(repo.cohort_awards,cohort_id,limit,offset),'cohort_id':cohort_id,'release':repo.manifest()['run_id'],'interpretation':'All historical award observations in this comparison group, paginated; no future company revenue is inferred'}
 @router.get('/companies/{cage}/awards')
 def company_awards(cage:str,limit:int=Query(50,ge=1,le=100)):
  m=repo.manifest();return {'records':call(repo.company_history,cage,'awards',limit),'release':m['run_id'],'availability':'available' if 'company_award_facts' in m['tables'] else 'not_published'}
 @router.get('/companies/{cage}/subcontracts')
 def company_subcontracts(cage:str,limit:int=Query(50,ge=1,le=100)):
  m=repo.manifest();return {'records':call(repo.company_history,cage,'subcontracts',limit),'release':m['run_id'],'availability':'available' if 'company_subcontract_facts' in m['tables'] else 'not_published'}
 @router.get('/companies/{cage}/coverage')
 def company_coverage(cage:str):return {'records':call(repo.company_history,cage,'coverage',1),'release':repo.manifest()['run_id'],'availability':'available' if 'company_history_coverage' in repo.manifest()['tables'] else 'not_published'}
 @router.get('/companies/{cage}/events')
 def company_events(cage:str,episode_id:str=None,limit:int=Query(50,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000)):
  return {'records':call(repo.company_events,cage,episode_id,limit,offset),'release':repo.manifest()['run_id'],'availability':'available' if 'company_opportunity_events' in repo.manifest()['tables'] else 'not_published','interpretation':'Source and matching changes, not user-managed sales stages; a missing snapshot is not a lost award'}
 @router.get('/work-evidence/{doc_id}')
 def work_evidence(doc_id:str):return {'records':call(repo.work_evidence,doc_id),'release':repo.manifest()['run_id']}
 @router.get('/work-evidence/{doc_id}/sources')
 def work_sources(doc_id:str,limit:int=Query(25,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000)):
  m=repo.manifest()
  return {'records':call(repo.work_sources,doc_id,limit,offset),'release':m['run_id'],'availability':'available' if 'company_award_facts' in m['tables'] and 'company_subcontract_facts' in m['tables'] else 'not_published','interpretation':'Underlying native records for this exact retained company scope, paginated. Award net obligations are lifetime observed award amounts; supplier reports and revisions are not additive revenue or a future opportunity value.'}
 @router.get('/research-topics')
 def topics(cage:str=None,limit:int=Query(20,ge=1,le=100)):return {'records':call(repo.topics,cage,limit),'fit':'Capability retrieval; applicant eligibility is not established'}
 @router.get('/research-awards/{award_id}')
 def award(award_id:str):return {'records':call(repo.award,award_id)}
 @router.get('/research-topics/{topic_id}/benchmarks')
 def benchmark(topic_id:str):return {'records':call(repo.benchmark,topic_id)}
 @router.get('/sources')
 def sources():
  m=repo.manifest();return {'release':m['run_id'],'as_of':m['as_of'],'refresh':repo.refresh_status(),'sources':repo.source_status(),'limitations':m['limitations']+['Forecasts containing negative future monthly quantities are withheld from actionable feeds pending source reconciliation; positive months are not silently priced alone.']}
 register_discovery_routes(router,repo,call)
 return router
