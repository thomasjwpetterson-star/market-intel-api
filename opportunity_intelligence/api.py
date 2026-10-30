"""Authenticated FastAPI adapter for the shared AWS read model.
The application must inject its existing verified-principal dependency.
"""
import hashlib,json,re,time
from pathlib import Path
from functools import lru_cache
from .aws import AWS,BUCKET,PREFIX,DATABASE
from . import rules
class Repository:
 def __init__(self,aws=None):self.aws=aws or AWS();self._manifest=None;self._expires=0
 def manifest(self):
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
 def opportunities(self,company=None,limit=20,offset=0,monitoring_only=False,kind='all'):
  if not 1<=limit<=100 or not 0<=offset<=1000000:raise ValueError('Invalid pagination')
  o=self.table('opportunities');d=self.table('demand_decisions');where="d.stage IN ('historical_watch','repeat_demand_inference')" if monitoring_only else "d.stage NOT IN ('resolved_or_elapsed','evidence_gap','planning_context','historical_watch','repeat_demand_inference')";join='';extra=''
  if kind not in ('all','work','items'):raise ValueError('Invalid opportunity kind')
  if kind=='items':where+=" AND o.lane IN ('published_item_procurement','source_item_forecasts')"
  if kind=='work':where+=" AND o.lane NOT IN ('published_item_procurement','source_item_forecasts')"
  if company:
   if not re.fullmatch(r'(?:CAGE:[A-Z0-9]{5}|UEI:[A-Z0-9]{12})',company):raise ValueError('Invalid company identifier')
   join=' JOIN '+self.table('company_matches')+" m ON m.episode_id=o.episode_id AND m.company_id='"+company+"'";where+=' AND m.opportunity_hash=o.record_hash'
   ranking_value='coalesce(o.value_median,least(v.government_reference_lot_scenario,v.supplier_average_lot_scenario),v.government_reference_lot_scenario,v.supplier_average_lot_scenario,0.0)' if 'company_item_valuations' in self.manifest()['tables'] else 'coalesce(o.value_median,0.0)'
   extra=',m.match_id,m.match_route,m.matched_terms,m.sample_evidence_id,m.company_evidence_caveat'
   extra+=",least(100.0,(CASE d.stage WHEN 'published_requirement' THEN 70 WHEN 'qualification_opening' THEN 65 WHEN 'teaming_opening' THEN 65 WHEN 'emerging_demand' THEN 35 WHEN 'source_forecast' THEN 20 ELSE 10 END)+least(15.0,m.score/6.0)+least(20.0,5.0*ln(1.0+greatest("+ranking_value+",0.0)/100000.0))+(CASE WHEN o.window_end IS NOT NULL THEN 5 ELSE 0 END)) priority_score,'Demand stage, observed company scope fit, supported package benchmarks or item-price scenarios, and dated engagement window; not a win probability or company revenue' priority_basis"
   if 'company_work_matches' in self.manifest()['tables']:
    join+=' LEFT JOIN '+self.table('company_work_matches')+' cw ON cw.company_id=m.company_id AND cw.episode_id=o.episode_id AND cw.opportunity_hash=o.record_hash'
    extra+=',cw.doc_id matched_doc_id,cw.work_text matched_work_text,cw.source_kind matched_source_kind,cw.last_observed matched_work_date,cw.review_category'
    if monitoring_only:where="(d.stage IN ('historical_watch','repeat_demand_inference') OR (cw.review_category='market_context' AND d.stage NOT IN ('resolved_or_elapsed','evidence_gap','planning_context'))) AND m.opportunity_hash=o.record_hash"
    else:where+=" AND (cw.review_category IS NULL OR cw.review_category<>'market_context')"
    if any(x['Name']=='company_entry_status' for x in self.manifest()['tables']['company_work_matches'].get('columns',[])):extra+=',cw.company_entry_status'
   if 'company_item_matches' in self.manifest()['tables']:
    join+=' LEFT JOIN '+self.table('company_item_matches')+' ci ON ci.company_id=m.company_id AND ci.episode_id=o.episode_id AND ci.opportunity_hash=o.record_hash'
    extra+=',ci.niin item_reference_niin,ci.reference_description item_reference_description,ci.reference_part_number item_reference_part_number'
   if 'company_item_valuations' in self.manifest()['tables']:
    join+=' LEFT JOIN '+self.table('company_item_valuations')+' v ON v.company_id=m.company_id AND v.episode_id=o.episode_id AND v.record_hash=o.record_hash'
    extra+=',v.quantity forecast_quantity,v.catalogue_unit forecast_catalogue_unit,v.government_reference_unit_price,v.supplier_average_unit_price,v.government_reference_lot_scenario,v.supplier_average_lot_scenario,v.supplier_average_last_sale,v.monthly_evidence_json,v.catalogue_evidence_json'
   if 'company_notice_item_prices' in self.manifest()['tables']:
    join+=' LEFT JOIN '+self.table('company_notice_item_prices')+' np ON np.company_id=m.company_id AND np.episode_id=o.episode_id AND np.record_hash=o.record_hash'
    extra+=',np.items_json notice_item_pricing_json,np.interpretation notice_item_price_interpretation'
  if 'dla_forecasts' in self.manifest()['tables']:
   asof=self.manifest().get('as_of')
   if not asof or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',asof):raise ValueError('Invalid release as-of date')
   # Negative monthly source quantities are unresolved data, not purchases or
   # offsets that can silently be netted into a positive sales scenario.
   join+=' LEFT JOIN (SELECT DISTINCT niin FROM '+self.table('dla_forecasts')+" WHERE forecast_qty<0 AND try_cast(substr(cast(forecast_month AS varchar),1,10) AS date)>=DATE '"+asof+"') fq ON fq.niin=json_extract_scalar(o.value_inputs_json,'$.niin')"
   where+=" AND (o.lane<>'source_item_forecasts' OR fq.niin IS NULL)"
  selected='SELECT DISTINCT o.*,d.stage,d.decision_json'+extra+' FROM '+o+' o JOIN '+d+' d ON o.episode_id=d.episode_id'+join+' WHERE '+where
  ordering=("q.priority_score DESC," if company else '')+"CASE q.stage WHEN 'qualification_opening' THEN 0 WHEN 'teaming_opening' THEN 0 WHEN 'published_requirement' THEN 1 WHEN 'emerging_demand' THEN 2 ELSE 3 END,CASE WHEN q.lane='source_item_forecasts' THEN 1 ELSE 0 END,q.window_end ASC NULLS LAST,q.episode_id"
  return self.rows('SELECT q.* FROM ('+selected+') q ORDER BY '+ordering+' OFFSET '+str(offset)+' LIMIT '+str(limit))

 def detail(self,eid):
  if not re.fullmatch('[a-f0-9]{64}',eid):raise ValueError('Invalid opportunity identifier')
  return self.rows('SELECT o.*,d.decision_json FROM '+self.table('opportunities')+' o JOIN '+self.table('demand_decisions')+" d ON o.episode_id=d.episode_id WHERE o.episode_id='"+eid+"'")
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
  ordering='first_action_date DESC,award_key' if kind=='primes' else 'event_date DESC,evidence_id'
  return self.rows('SELECT * FROM '+self.table(name)+" WHERE episode_id='"+episode_id+"' ORDER BY "+ordering+' OFFSET '+str(offset)+' LIMIT '+str(limit))

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
 repo=repository or Repository()
 def require_principal(principal=Depends(verify_principal)):
  if not principal or isinstance(principal,bool):raise HTTPException(status_code=401,detail='Verified principal required')
  return principal
 router=APIRouter(prefix='/opportunity-intelligence',tags=['opportunity-intelligence'],dependencies=[Depends(require_principal)])
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
 @router.get('/companies/{cage}/relationships')
 def relationships(cage:str,limit:int=Query(50,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000)):
  m=repo.manifest();table=m['tables'].get('company_registry',{})
  return {'records':call(repo.relationships,cage,limit,offset),'release':m['run_id'],'availability':'available' if table else 'not_published','source_provenance':{'table':'company_registry','table_sha256':table.get('sha256'),'sources':{k:v for k,v in m.get('source_status',{}).items() if k in ('cage_registry','sam_company_capabilities')}},'interpretation':'Exact registry relationship candidates from parent_cage or replacement_cage. Replacements are not ownership. No ultimate-parent consolidation, name similarity or transfer of technical qualifications is inferred.'}
 @router.get('/innovation-signals')
 def innovation(limit:int=Query(20,ge=1,le=100)):return {'records':call(repo.innovation,limit),'status':'Published agency engagement evidence; no company qualification or award probability inferred'}
 @router.get('/methodology')
 def methodology():
  m=repo.manifest();return dict(m['rules'],release=m['run_id'],company_matching={'evidence':'Observed award and reported supplier work, exact item references, program/vehicle links and specific technical scope. Classification overlap alone is insufficient.','entry_routes':'Named prior suppliers, defined service work or explicit development/source-opening context take priority. Other hardware similarity is market context until an entry route is evidenced.','qualification':'A company/item association is separate from manufacturing rights, procurement approval, current configuration, eligibility and capacity.'},values={'historical_benchmarks':'Revision-resolved award net obligations and reported outsourced-package amounts. Approximate median and observed range use the complete comparison group; recent/extreme awards form a short preview.','unit_prices':'Exact company/item reference prices are unit evidence. Missing quantities do not produce totals. Forecast quantity × price is an alternative gross scenario, not company revenue.','allocation':'Prime/program funding is context. Company-addressable value remains unallocated without workshare evidence.','source_quality':'Item forecasts with negative future monthly quantities are withheld from actionable feeds until reconciled; published purchase requests remain separate evidence.'})
 @router.get('/opportunities')
 def opportunities(company:str=None,limit:int=Query(20,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000),kind:str=Query('all',pattern='^(all|work|items)$')):
  return {'records':call(repo.opportunities,company,limit,offset,False,kind),'release':repo.manifest()['run_id'],'status':'authenticated_evidence_feed','eligibility':'Company fit, access and qualification require evidence; these are not win probabilities.'}
 @router.get('/companies/{cage}/monitoring')
 def monitoring(cage:str,limit:int=Query(20,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000)):
  if not re.fullmatch('[A-Z0-9]{5}',cage):raise HTTPException(status_code=400,detail='Invalid CAGE')
  return {'records':call(repo.opportunities,'CAGE:'+cage,limit,offset,True),'release':repo.manifest()['run_id'],'status':'Market context and historical signals; company entry route or fresh demand remains unestablished'}
 @router.get('/opportunities/{episode_id}')
 def opportunity(episode_id:str):
  r=call(repo.detail,episode_id)
  if not r:raise HTTPException(status_code=404,detail='Opportunity not found')
  return r[0]
 @router.get('/companies/{cage}/research')
 def research(cage:str,limit:int=Query(20,ge=1,le=100)):return {'awards':call(repo.research,cage,limit),'topic_calls':call(repo.research_calls,cage,limit)}
 @router.get('/opportunities/{episode_id}/supplier-evidence')
 def supplier_evidence(episode_id:str,kind:str='primes',limit:int=Query(25,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000)):
  return {'records':call(repo.supplier_evidence,episode_id,kind,limit,offset),'release':repo.manifest()['run_id'],'kind':kind,'interpretation':'Complete source lineage, paginated. Performance locations are not assumed to be procurement or production sites.'}
 @router.get('/opportunities/{episode_id}/price-evidence')
 def prices(episode_id:str):return {'records':call(repo.price_lines,episode_id),'interpretation':'Historical line evidence; no current requirement total or company value is inferred'}
 @router.get('/opportunities/{episode_id}/value-evidence')
 def values(episode_id:str):return call(repo.value_evidence,episode_id)
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
 def work_evidence(doc_id:str):return {'records':call(repo.work_evidence,doc_id)}
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
  m=repo.manifest();return {'release':m['run_id'],'as_of':m['as_of'],'sources':m['source_status'],'limitations':m['limitations']+['Forecasts containing negative future monthly quantities are withheld from actionable feeds pending source reconciliation; positive months are not silently priced alone.']}
 return router
