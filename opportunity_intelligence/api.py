"""Authenticated FastAPI adapter for the shared AWS read model.
The application must inject its existing verified-principal dependency.
"""
import json,re,time
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
   extra=',m.match_id,m.match_route,m.matched_terms,m.sample_evidence_id,m.company_evidence_caveat'
   if 'company_work_matches' in self.manifest()['tables']:
    join+=' LEFT JOIN '+self.table('company_work_matches')+' cw ON cw.company_id=m.company_id AND cw.episode_id=o.episode_id AND cw.opportunity_hash=o.record_hash'
    extra+=',cw.doc_id matched_doc_id,cw.work_text matched_work_text,cw.source_kind matched_source_kind,cw.last_observed matched_work_date'
   if 'company_item_matches' in self.manifest()['tables']:
    join+=' LEFT JOIN '+self.table('company_item_matches')+' ci ON ci.company_id=m.company_id AND ci.episode_id=o.episode_id AND ci.opportunity_hash=o.record_hash'
    extra+=',ci.niin item_reference_niin,ci.reference_description item_reference_description,ci.reference_part_number item_reference_part_number'
   if 'company_item_valuations' in self.manifest()['tables']:
    join+=' LEFT JOIN '+self.table('company_item_valuations')+' v ON v.company_id=m.company_id AND v.episode_id=o.episode_id AND v.record_hash=o.record_hash'
    extra+=',v.quantity forecast_quantity,v.catalogue_unit forecast_catalogue_unit,v.government_reference_unit_price,v.supplier_average_unit_price,v.government_reference_lot_scenario,v.supplier_average_lot_scenario,v.supplier_average_last_sale,v.monthly_evidence_json,v.catalogue_evidence_json'
  selected='SELECT DISTINCT o.*,d.stage,d.decision_json'+extra+' FROM '+o+' o JOIN '+d+' d ON o.episode_id=d.episode_id'+join+' WHERE '+where
  ordering="CASE q.stage WHEN 'qualification_opening' THEN 0 WHEN 'teaming_opening' THEN 0 WHEN 'published_requirement' THEN 1 WHEN 'emerging_demand' THEN 2 ELSE 3 END,CASE WHEN q.lane='source_item_forecasts' THEN 1 ELSE 0 END,q.window_end ASC NULLS LAST,q.episode_id"
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
 def price_lines(self,episode_id):
  if not re.fullmatch('[a-f0-9]{64}',episode_id):raise ValueError('Invalid opportunity identifier')
  return self.rows('SELECT * FROM '+self.table('notice_price_links')+" WHERE episode_id='"+episode_id+"' ORDER BY pricing_record_id LIMIT 100")

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
 @router.get('/innovation-signals')
 def innovation(limit:int=Query(20,ge=1,le=100)):return {'records':call(repo.innovation,limit),'status':'Published agency engagement evidence; no company qualification or award probability inferred'}
 @router.get('/methodology')
 def methodology():return repo.manifest()['rules']
 @router.get('/opportunities')
 def opportunities(company:str=None,limit:int=Query(20,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000),kind:str=Query('all',pattern='^(all|work|items)$')):
  return {'records':call(repo.opportunities,company,limit,offset,False,kind),'release':repo.manifest()['run_id'],'status':'internal_candidate','eligibility':'Company fit, access and qualification require evidence; these are not win probabilities.'}
 @router.get('/companies/{cage}/monitoring')
 def monitoring(cage:str,limit:int=Query(20,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000)):
  if not re.fullmatch('[A-Z0-9]{5}',cage):raise HTTPException(status_code=400,detail='Invalid CAGE')
  return {'records':call(repo.opportunities,'CAGE:'+cage,limit,offset,True),'status':'Historical account and recurrence signals; not current opportunities'}
 @router.get('/opportunities/{episode_id}')
 def opportunity(episode_id:str):
  r=call(repo.detail,episode_id)
  if not r:raise HTTPException(status_code=404,detail='Opportunity not found')
  return r[0]
 @router.get('/companies/{cage}/research')
 def research(cage:str,limit:int=Query(20,ge=1,le=100)):return {'awards':call(repo.research,cage,limit),'topic_calls':call(repo.research_calls,cage,limit)}
 @router.get('/opportunities/{episode_id}/price-evidence')
 def prices(episode_id:str):return {'records':call(repo.price_lines,episode_id),'interpretation':'Historical line evidence; no current requirement total or company value is inferred'}
 @router.get('/work-evidence/{doc_id}')
 def work_evidence(doc_id:str):return {'records':call(repo.work_evidence,doc_id)}
 @router.get('/research-topics')
 def topics(cage:str=None,limit:int=Query(20,ge=1,le=100)):return {'records':call(repo.topics,cage,limit),'fit':'Capability retrieval; applicant eligibility is not established'}
 @router.get('/research-awards/{award_id}')
 def award(award_id:str):return {'records':call(repo.award,award_id)}
 @router.get('/research-topics/{topic_id}/benchmarks')
 def benchmark(topic_id:str):return {'records':call(repo.benchmark,topic_id)}
 @router.get('/sources')
 def sources():
  m=repo.manifest();return {'release':m['run_id'],'as_of':m['as_of'],'sources':m['source_status'],'limitations':m['limitations']}
 return router
