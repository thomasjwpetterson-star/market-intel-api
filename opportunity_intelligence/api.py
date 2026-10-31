"""Authenticated FastAPI adapter for the shared AWS read model.
The application must inject its existing verified-principal dependency.
"""
import contextvars,datetime,hashlib,json,re,time,math
from pathlib import Path
from functools import lru_cache
from .aws import AWS,BUCKET,PREFIX,DATABASE
from . import rules
from .notice_facts import quantity_fact,price_scenario_fact
_REQUEST_RELEASE=contextvars.ContextVar("mimir_opportunity_request_release",default=None)

class Repository:
 def __init__(self,aws=None):self.aws=aws or AWS();self._manifest=None;self._expires=0
 def manifest(self):
  pinned=_REQUEST_RELEASE.get()
  if pinned and pinned[0] is self:return pinned[1]
  if self._manifest is None or time.monotonic()>=self._expires:
   p=json.loads(self.aws.s3.get_object(Bucket=BUCKET,Key=PREFIX+'/current.json')['Body'].read());key=p['manifest_key']
   if not key.startswith(PREFIX+'/releases/'):raise ValueError('Invalid engine release pointer')
   self._manifest=json.loads(self.aws.s3.get_object(Bucket=BUCKET,Key=key)['Body'].read());self._expires=time.monotonic()+30
  return self._manifest
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
 def company(self,cage):
  if not re.fullmatch('[A-Z0-9]{5}',cage):raise ValueError('Invalid CAGE')
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
 def opportunities(self,company=None,limit=20,offset=0,monitoring_only=False,kind='all',minimum_value=0.0,_query=False):
  if not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid pagination')
  if not math.isfinite(minimum_value) or not 0<=minimum_value<=1000000000000:raise ValueError('Invalid minimum value')
  o=self.table('opportunities');d=self.table('demand_decisions');where="d.stage IN ('historical_watch','repeat_demand_inference')" if monitoring_only else "d.stage NOT IN ('resolved_or_elapsed','evidence_gap','planning_context','historical_watch','repeat_demand_inference')";join='';extra=''
  if kind not in ('all','work','items'):raise ValueError('Invalid opportunity kind')
  if company and not re.fullmatch(r'(?:CAGE:[A-Z0-9]{5}|UEI:[A-Z0-9]{12})',company):raise ValueError('Invalid company identifier')
  quality='opportunity_quality_reviews' in self.manifest()['tables']
  item_condition="(o.lane IN ('published_item_procurement','source_item_forecasts')"+(" OR coalesce(qr.category_override='items',false)" if quality else '')+")"
  category_where=(' AND '+item_condition if kind=='items' else " AND o.lane NOT IN ('published_item_procurement','source_item_forecasts')"+(" AND coalesce(qr.category_override,'')<>'items'" if quality else '') if kind=='work' else '')
  if company:
   if not re.fullmatch(r'(?:CAGE:[A-Z0-9]{5}|UEI:[A-Z0-9]{12})',company):raise ValueError('Invalid company identifier')
   join=' JOIN '+self.table('company_matches')+" m ON m.episode_id=o.episode_id AND m.company_id='"+company+"'";where+=' AND m.opportunity_hash=o.record_hash'
   notice_lot="coalesce(try_cast(json_extract_scalar(np.items_json,'$[0].government_reference_lot_scenario') AS double),try_cast(json_extract_scalar(np.items_json,'$[0].supplier_average_lot_scenario') AS double))" if 'company_notice_item_prices' in self.manifest()['tables'] else 'NULL'
   if 'company_notice_item_prices' in self.manifest()['tables']:notice_lot="CASE WHEN json_array_length(np.items_json)=1 THEN "+notice_lot+" END"
   request_lot="CASE WHEN o.lane='published_item_procurement' AND try_cast(json_extract_scalar(o.value_inputs_json,'$.quantity') AS double)>0 AND try_cast(json_extract_scalar(o.value_inputs_json,'$.observed_unit_price') AS double)>0 AND nullif(json_extract_scalar(o.value_inputs_json,'$.request_unit'),'') IS NOT NULL THEN try_cast(json_extract_scalar(o.value_inputs_json,'$.quantity') AS double)*try_cast(json_extract_scalar(o.value_inputs_json,'$.observed_unit_price') AS double) END"
   forecast_lot='coalesce(least(v.government_reference_lot_scenario,v.supplier_average_lot_scenario),v.government_reference_lot_scenario,v.supplier_average_lot_scenario)' if 'company_item_valuations' in self.manifest()['tables'] else 'NULL'
   valuation_columns={x['Name'] for x in self.manifest()['tables'].get('company_item_valuations',{}).get('columns',[])}
   if 'market_price_lot_scenario' in valuation_columns:forecast_lot='coalesce(v.market_price_lot_scenario,v.government_reference_lot_scenario)'
   if 'company_item_valuations' in self.manifest()['tables']:forecast_lot="CASE WHEN v.catalogue_unit='EA' THEN "+forecast_lot+' END'
   ranking_value='coalesce('+notice_lot+','+request_lot+',o.value_median,'+forecast_lot+',0.0)'
   forecast_year='NULL'
   if 'company_item_valuations' in self.manifest()['tables']:
    asof=self.manifest().get('as_of')
    if not asof or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',asof):raise ValueError('Invalid release as-of date')
    unit_price='coalesce(v.market_price_median,v.government_reference_unit_price)' if 'market_price_median' in valuation_columns else 'v.government_reference_unit_price'
    months="reduce(try(cast(json_parse(v.monthly_evidence_json) AS array(json))),cast(0 AS double),(total,x)->total+CASE WHEN try_cast(substr(json_extract_scalar(x,'$.month'),1,10) AS date)>=DATE '"+asof+"' AND try_cast(substr(json_extract_scalar(x,'$.month'),1,10) AS date)<date_add('month',12,DATE '"+asof+"') THEN coalesce(try_cast(json_extract_scalar(x,'$.quantity') AS double),0.0) ELSE 0.0 END,total->total)"
    forecast_year="CASE WHEN o.lane='source_item_forecasts' AND v.catalogue_unit='EA' AND ("+forecast_lot+")>0 THEN "+months+'*'+unit_price+' END'
   priority_amount="CASE WHEN o.lane='source_item_forecasts' THEN coalesce("+forecast_year+",0.0) ELSE "+ranking_value+' END'
   value_kind="CASE WHEN "+notice_lot+">0 THEN 'item_reference_estimate' WHEN "+request_lot+">0 THEN 'historical_unit_price_estimate' WHEN o.lane='source_item_forecasts' AND "+forecast_lot+">0 THEN 'forecast_scenario' WHEN o.value_median>0 THEN 'historical_comparison' ELSE 'unknown' END"
   extra=',m.match_id,m.match_route,m.matched_terms,m.sample_evidence_id,m.company_evidence_caveat,'+ranking_value+' commercial_value,'+value_kind+' commercial_value_kind,'+item_condition+' item_demand,'+forecast_year+' forecast_next_12m_value'
   extra+=",(CASE d.stage WHEN 'published_requirement' THEN 12 WHEN 'qualification_opening' THEN 10 WHEN 'teaming_opening' THEN 10 WHEN 'emerging_demand' THEN 6 WHEN 'source_forecast' THEN 2 ELSE 0 END)+least(20.0,m.score/5.0)+18.0*ln(1.0+greatest("+priority_amount+",0.0)/100000.0)+(CASE WHEN d.stage='published_requirement' AND try_cast(o.window_end AS date) BETWEEN DATE '"+self.manifest()['as_of']+"' AND date_add('day',30,DATE '"+self.manifest()['as_of']+"') THEN 12 WHEN o.window_end IS NOT NULL THEN 3 ELSE 0 END) priority_score,'Commercial size, observed fit and engagement timing. Forecast priority uses next-12-month priced demand; published requests due within 30 days receive a timing boost. Exact quantity/price estimates precede historical comparisons; not a win probability or company revenue' priority_basis"
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
  # Enforce exact source clocks at request time so a daily snapshot cannot keep
  # expired ISO deadlines open until the next nightly build. Date-only notices
  # retain calendar precision and do not acquire a midnight deadline.
  if not monitoring_only:
   where+=" AND (coalesce(json_extract_scalar(o.value_inputs_json,'$.source_response_deadline_precision'),'')<>'timestamp_utc' OR try(from_iso8601_timestamp(json_extract_scalar(o.value_inputs_json,'$.source_response_deadline_utc'))) IS NULL OR try(from_iso8601_timestamp(json_extract_scalar(o.value_inputs_json,'$.source_response_deadline_utc')))>=current_timestamp)"
  if 'dla_forecasts' in self.manifest()['tables']:
   asof=self.manifest().get('as_of')
   if not asof or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',asof):raise ValueError('Invalid release as-of date')
   # Negative monthly source quantities are unresolved data, not purchases or
   # offsets that can silently be netted into a positive sales scenario.
   join+=' LEFT JOIN (SELECT DISTINCT niin FROM '+self.table('dla_forecasts')+" WHERE forecast_qty<0 AND try_cast(substr(cast(forecast_month AS varchar),1,10) AS date)>=DATE '"+asof+"') fq ON fq.niin=json_extract_scalar(o.value_inputs_json,'$.niin')"
   where+=" AND (o.lane<>'source_item_forecasts' OR fq.niin IS NULL)"
  selected='SELECT DISTINCT o.*,d.stage,d.decision_json'+extra+' FROM '+o+' o JOIN '+d+' d ON o.episode_id=d.episode_id'+join+' WHERE '+where
  ordering=("CASE WHEN q.commercial_value>=1000000 THEN 0 ELSE 1 END,q.priority_score DESC," if company else '')+"CASE q.stage WHEN 'qualification_opening' THEN 0 WHEN 'teaming_opening' THEN 0 WHEN 'published_requirement' THEN 1 WHEN 'emerging_demand' THEN 2 ELSE 3 END,CASE WHEN q.lane='source_item_forecasts' THEN 1 ELSE 0 END,q.window_end ASC NULLS LAST,q.episode_id"
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
  return records

 def overview(self,cage):
  if not re.fullmatch('[A-Z0-9]{5}',cage):raise ValueError('Invalid CAGE')
  from .overview import summary
  query=self.opportunities('CAGE:'+cage,kind='all',_query=True)
  scale="(SELECT max(observed_prime_net_obligations) FROM "+self.table('company_history_coverage')+" WHERE company_id='CAGE:"+cage+"')" if 'company_history_coverage' in self.manifest()['tables'] else 'NULL'
  count_sql='SELECT count_if(NOT item_demand) work_count,count_if(item_demand) items_count,count_if(NOT item_demand AND commercial_value>=1000000) work_larger_count,count_if(item_demand AND commercial_value>=1000000) items_larger_count,'+scale+' observed_prime_net_obligations FROM ('+query+')'
  forecast_columns={x['Name'] for x in self.manifest()['tables'].get('company_item_valuations',{}).get('columns',[])}
  forecast_extra=','.join(name if name in forecast_columns else 'NULL '+name for name in ('annual_scenarios_json','valuation_status','market_price_median','market_price_lot_scenario','latest_price_date','stock_on_hand','inventory_date','inventory_signal'))
  priced_sql="SELECT "+forecast_extra+",window_start,window_end,forecast_catalogue_unit,episode_id,record_hash,title,customer_name,source_url,source_evidence_id,commercial_value,commercial_value_kind,item_demand,item_reference_niin,notice_item_pricing_json,value_inputs_json,forecast_next_12m_value FROM ("+query+") WHERE commercial_value_kind IN ('item_reference_estimate','historical_unit_price_estimate','forecast_scenario') AND commercial_value>0"
  from concurrent.futures import ThreadPoolExecutor
  with ThreadPoolExecutor(max_workers=3) as pool:
   count_task=pool.submit(self.rows,count_sql);price_task=pool.submit(self.rows,priced_sql);business_task=pool.submit(contextvars.copy_context().run,self.business_context,cage);counts=count_task.result();priced=price_task.result();business=business_task.result()
  return dict(summary(counts[0] if counts else {},priced,self.manifest()['run_id'],business),company_id='CAGE:'+cage,cage_code=cage)

 def item_source_references(self,cage,eid,limit=25,offset=0):
  if not re.fullmatch('[A-Z0-9]{5}',cage) or not re.fullmatch('[a-f0-9]{64}',eid) or not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid item source evidence request')
  if 'company_item_reference_evidence' not in self.manifest()['tables']:return []
  return self.rows('SELECT * FROM '+self.table('company_item_reference_evidence')+" WHERE company_id='CAGE:"+cage+"' AND episode_id='"+eid+"' ORDER BY part_number,evidence_id OFFSET "+str(offset)+' LIMIT '+str(limit))

 def business_context(self,cage):
  if not re.fullmatch('[A-Z0-9]{5}',cage):raise ValueError('Invalid CAGE')
  selects=[];joins=[]
  for alias,name in [('a','company_activity_baselines'),('c','company_catalogue_sales'),('l','company_reporting_lag_baselines')]:
   if name not in self.manifest()['tables']:continue
   cols=[x['Name'] for x in self.manifest()['tables'][name]['columns'] if x['Name'] not in ('company_id','company_name')]
   if not all(re.fullmatch('[a-z_0-9]+',x) for x in cols):raise ValueError('Invalid business-context projection')
   selects.extend(alias+'.'+x for x in cols);joins.append(' LEFT JOIN '+self.table(name)+' '+alias+' ON '+alias+'.company_id=k.company_id')
  if not selects:return {}
  rows=self.rows("SELECT "+','.join(selects)+" FROM (SELECT 'CAGE:"+cage+"' company_id) k"+''.join(joins))
  return rows[0] if rows else {}

 def business_evidence(self,cage,kind='actions',limit=25,offset=0):
  if not re.fullmatch('[A-Z0-9]{5}',cage) or kind not in ('actions','lag-adjusted-actions','catalogue-sales') or not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid business evidence request')
  name={'actions':'company_activity_actions','lag-adjusted-actions':'company_reporting_lag_actions','catalogue-sales':'catalogue_sales_evidence'}[kind]
  if name not in self.manifest()['tables']:return []
  order='action_date DESC,transaction_key' if kind!='catalogue-sales' else 'transaction_date DESC,pricing_record_id'
  return self.rows('SELECT * FROM '+self.table(name)+" WHERE company_id='CAGE:"+cage+"' ORDER BY "+order+' OFFSET '+str(offset)+' LIMIT '+str(limit))

 def related_item_requests(self,eid,limit=25,offset=0):
  if not re.fullmatch('[a-f0-9]{64}',eid) or not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid item request relationship')
  if 'item_request_progression' not in self.manifest()['tables']:return []
  return self.rows('SELECT * FROM '+self.table('item_request_progression')+" WHERE forecast_episode_id='"+eid+"' ORDER BY response_deadline,request_episode_id OFFSET "+str(offset)+' LIMIT '+str(limit))

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
  if 'company_notice_item_prices' in self.manifest()['tables']:
   # New global item references are identical across candidates; legacy
   # company-specific rows must never be promoted to global source evidence.
   join+=" LEFT JOIN (SELECT DISTINCT episode_id,record_hash,items_json,interpretation FROM "+self.table('company_notice_item_prices')+" WHERE json_extract_scalar(items_json,'$[0].price_scope')='global_exact_niin') np ON np.episode_id=o.episode_id AND np.record_hash=o.record_hash"
   extra+=',np.items_json notice_item_pricing_json,np.interpretation notice_item_price_interpretation'
  records=self.rows('SELECT o.*,d.stage,d.decision_json'+extra+' FROM '+self.table('opportunities')+' o JOIN '+self.table('demand_decisions')+" d ON o.episode_id=d.episode_id"+join+" WHERE o.episode_id='"+eid+"'")
  for record in records:
   record.update(quantity_fact(record));record.update(price_scenario_fact(record))
  return records
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
  return self.rows('SELECT a.* FROM '+self.table(name)+' a WHERE '+condition+' ORDER BY '+ordering+' OFFSET '+str(offset)+' LIMIT '+str(limit))
 def price_lines(self,episode_id):
  if not re.fullmatch('[a-f0-9]{64}',episode_id):raise ValueError('Invalid opportunity identifier')
  return self.rows('SELECT * FROM '+self.table('notice_price_links')+" WHERE episode_id='"+episode_id+"' ORDER BY pricing_record_id LIMIT 100")

 def value_evidence(self,episode_id):
  rows=self.detail(episode_id)
  if not rows:return {}
  o=rows[0]
  return dict(episode_id=episode_id,record_hash=o['record_hash'],value_low=o.get('value_low'),value_median=o.get('value_median'),value_high=o.get('value_high'),value_basis=o.get('value_basis'),value_scope=o.get('value_scope'),inputs=rules.decode(o.get('value_inputs_json')),source_url=o.get('source_url'),source_evidence_id=o.get('source_evidence_id'))

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
  return self.rows('SELECT '+selection+' FROM '+self.table(name)+" e"+join+" WHERE e.episode_id='"+episode_id+"' ORDER BY "+ordering+' OFFSET '+str(offset)+' LIMIT '+str(limit))

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
  return self.rows('SELECT * FROM '+self.table(name)+" WHERE "+field+"='"+value+"' ORDER BY "+ordering+' LIMIT '+str(max(1,min(limit,100))))

 def company_events(self,cage,episode_id=None,limit=50,offset=0):
  if not re.fullmatch('[A-Z0-9]{5}',cage):raise ValueError('Invalid CAGE')
  if episode_id is not None and not re.fullmatch('[a-f0-9]{64}',episode_id):raise ValueError('Invalid opportunity identifier')
  if not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid pagination')
  if 'company_opportunity_events' not in self.manifest()['tables']:return []
  where="company_id='CAGE:"+cage+"'"+(" AND episode_id='"+episode_id+"'" if episode_id else '')
  return self.rows('SELECT * FROM '+self.table('company_opportunity_events')+' WHERE '+where+' ORDER BY event_date DESC,release_id DESC,event_id OFFSET '+str(offset)+' LIMIT '+str(limit))

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
  m=repo.manifest();return dict(m['rules'],release=m['run_id'],processing_logic_sha256=m.get('processing_logic_sha256'),processing_logic_files=m.get('processing_logic_files',{}),annual_activity_policy=m.get('annual_activity_policy'),reporting_lag_policy=m.get('reporting_lag_policy'),request_presence_policy=m.get('item_request_presence_policy'),commercial_ranking={'policy':'commercial-materiality-v2','rule':'Size, observed fit and engagement timing inform priority. Forecast priority uses next-twelve-month priced demand, while the complete stated horizon remains visible. Published requests due within thirty days receive a timing boost. Historical size guides are not counted in demand totals.','default_size_focus':'Sites above $100m observed annual obligations focus on values above 1% of that baseline, with a $1m floor. Where annual evidence is smaller or missing, $100m lifetime obligations retain a $1m focus. All smaller and unpriced prospects remain available. A 3x annual-activity coverage benchmark diagnoses shortfalls; it does not create demand.'},company_matching={'evidence':'Observed award and reported supplier work, exact item references, program/vehicle links and specific technical scope. Classification overlap alone is insufficient.','entry_routes':'Named prior suppliers, defined service work or explicit development/source-opening context take priority. Other hardware similarity is market context until an entry route is evidenced.','qualification':'A company/item association is separate from manufacturing rights, procurement approval, current configuration, eligibility and capacity.'},source_quality={'policy_version':m.get('quality_policy_version'),'policy_hash':m.get('source_quality_logic_sha256'),'source_conflicts':'Conflicting title/nomenclature or past response dates without a current deadline override are held for source review, with quoted passages.','company_scope':'Administrative phrases, postal-location fragments, unspecific labels and a single shared product/work word cannot establish the complete deliverable. Matching words in incidental body text are insufficient.','delivery_mode':'Component supply, repair/overhaul, equipment installation, onsite services and facility construction are assessed separately. Selected delivery statements and their exact document PSC are retained. A manufacturing NAICS or similar product noun does not establish onsite-service capability.','psc_reference':'https://origin-www.acquisition.gov/sites/default/files/manual/PSC%20Manual%20April%202025.pdf','item_access':'Existing-OEM sourcing is distinct from an invitation to qualify a new manufacturer. Exact named part, NIIN and nonconflicting procurement reference codes are retained separately from manufacturing data rights.','research_eligibility':'Research relevance is separate from SBIR/STTR applicant eligibility or a permitted teaming route.','notice_grouping':'The same buyer, complete normalized title and identifier with a trailing -00 variant may be grouped for display. Every notice source is retained; different procurement identifiers remain separate and values are not summed.'},values={'source_quantities':'A quoted explicit quantity is exposed only for a single-item notice with one consistent quantity and unit. Estimates are not orders; ranges, multiple quantities and annual/monthly rates are not collapsed. No total is inferred from a different NSN or part.', 'historical_benchmarks':'Revision-resolved award net obligations and reported outsourced-package amounts. Approximate median and observed range use the complete comparison group; recent/extreme awards form a short preview.','unit_prices':'Exact notice/item reference prices can come from another supplier and remain separate from candidate eligibility. Quoted single-item quantities and matching units produce reference demand estimates. Exact company/item reference prices are unit evidence. Missing quantities do not produce totals. Forecast quantity × price is an alternative gross scenario, not company revenue.','allocation':'Prime/program funding is context. Company-addressable value remains unallocated without workshare evidence.','source_quality':'Item forecasts with negative future monthly quantities are withheld from actionable feeds until reconciled; published purchase requests remain separate evidence.'})
 @router.get('/opportunities')
 def opportunities(company:str=None,limit:int=Query(20,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000),kind:str=Query('all',pattern='^(all|work|items)$'),min_value:float=Query(0,ge=0,le=1000000000000)):
  return {'records':call(repo.opportunities,company,limit,offset,False,kind,min_value),'release':repo.manifest()['run_id'],'status':'authenticated_evidence_feed','eligibility':'Company fit, access and qualification require evidence; these are not win probabilities.'}
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
  m=repo.manifest();return {'release':m['run_id'],'as_of':m['as_of'],'refresh':repo.refresh_status(),'sources':m['source_status'],'limitations':m['limitations']+['Forecasts containing negative future monthly quantities are withheld from actionable feeds pending source reconciliation; positive months are not silently priced alone.']}
 return router
