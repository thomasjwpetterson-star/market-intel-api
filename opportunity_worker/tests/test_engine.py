import json
import pytest
from opportunity_engine import rules
from opportunity_engine.api import Repository,create_router
from opportunity_engine.matching import tokens

def row(**kw):return dict(episode_id='a'*64,record_hash='b'*64,lane='published_procurement',title='Research',scope_text='Research work',customer_name='Navy',source_evidence_id='c'*64,window_end='2026-11-25',**kw)
def test_sbir_is_not_automatic_eligibility():
 r=rules.evaluate(row(source_name='sbir_topics'),'2026-10-29');assert r['rule_id']=='PR16';assert r['company_addressable_value'] is None;assert any('eligibility' in s for s in r['unresolved'])
@pytest.mark.parametrize('state',['cancelled','awarded_or_ordered'])
def test_negative_notice_wins(state):
 r=rules.evaluate(row(source_name='sbir_topics',reconciled_state=state),'2026-10-29');assert r['stage']=='resolved_or_elapsed'
def test_expired_not_promoted():
 o=row(source_name='sbir_topics');o['window_end']='2026-10-01';assert rules.evaluate(o,'2026-10-29')['stage']=='resolved_or_elapsed'
def test_vehicle_requires_entry():
 o=row(source_name='sam_notices');o['title']='IDIQ task-order competition';r=rules.evaluate(o,'2026-10-29');assert r['rule_id']=='PR18';assert any('vehicle holder' in x for x in r['unresolved'])
def test_registry_has_disruption_and_operational_rules():assert {r['id'] for r in rules.catalogue()['rules']}=={'PR%02d'%n for n in range(1,24)}
def test_tokens_function_not_platform():assert 'manipulation' in tokens('Robotic cable manipulation for UAS');assert not ({'counter'}&tokens('UAS support services'))
def test_no_auth_dependency_fails():
 with pytest.raises(ValueError):create_router(None)
@pytest.mark.parametrize('bad',["45934' OR 1=1 --",'abc','CAGE:45934;DROP TABLE x'])
def test_invalid_identifier_before_aws(bad):
 r=Repository.__new__(Repository)
 with pytest.raises(ValueError):r.research(bad)
def test_api_auth_and_pagination():
 from fastapi import FastAPI,HTTPException,Header
 from fastapi.testclient import TestClient
 class Fake:
  def manifest(self):return {'run_id':'test','rules':rules.catalogue()}
  def opportunities(self,*args):return []
 def auth(authorization:str=Header(None)):
  if authorization!='Bearer verified-test':raise HTTPException(status_code=401)
  return {'id':'verified-test-user'}
 app=FastAPI();app.include_router(create_router(auth,Fake()));client=TestClient(app)
 assert client.get('/opportunity-intelligence/opportunities').status_code==401
 assert client.get('/opportunity-intelligence/opportunities',headers={'Authorization':'Bearer verified-test'}).status_code==200
 assert client.get('/opportunity-intelligence/opportunities?limit=101',headers={'Authorization':'Bearer verified-test'}).status_code==422

def test_new_topic_matching_requires_function_and_retains_provenance(tmp_path):
 import duckdb
 from opportunity_engine.matching import match_topics
 c=duckdb.connect();c.execute('CREATE TABLE company_work(doc_id VARCHAR,company_id VARCHAR,source_kind VARCHAR,work_text VARCHAR,last_observed VARCHAR,sample_evidence_id VARCHAR)');c.execute("INSERT INTO company_work VALUES ('good','CAGE:12345','prime_award','Robotic wire manipulation for unmanned aircraft','2026-01-01','native-good'),('bad','CAGE:ABCDE','prime_award','Unmanned aircraft wire maintenance support','2026-01-01','native-bad')");c.execute('CREATE TABLE oi_research_awards(award_id VARCHAR,company_id VARCHAR,title VARCHAR,abstract VARCHAR,award_date VARCHAR,evidence_state VARCHAR)')
 path,m=match_topics(c,[{'topic_id':'1','title':'Agile robotic wire manipulation for unmanned aircraft'}],tmp_path,'2026-10-29');rows=[json.loads(x) for x in path.read_text().splitlines()];assert [x['company_id'] for x in rows]==['CAGE:12345'];assert json.loads(rows[0]['evidence_json'])[0]['evidence_id']=='native-good';assert rows[0]['applicant_eligibility']=='not_established'

def test_publish_readback_failure_cannot_advance_pointer(tmp_path,monkeypatch):
 import hashlib
 import opportunity_engine.publish as publisher
 data=tmp_path/'data.parquet';data.write_bytes(b'fixture');m={'run_id':'test_release','tables':{'x':{'path':str(data),'sha256':hashlib.sha256(data.read_bytes()).hexdigest(),'rows':1,'columns':[]}},'sources':{},'sbir_source':{'source_s3_uri':'s3://fixture'}}
 (tmp_path/'manifest.json').write_text(json.dumps(m));(tmp_path/'validation.json').write_text(json.dumps({'passed':1,'failed':0}));writes=[]
 class Fake:
  class S3:
   class exceptions:
    class NoSuchKey(Exception):pass
   def get_object(self,**kw):raise self.exceptions.NoSuchKey()
  s3=S3()
  def upload(self,*a):pass
  def register(self,*a):pass
  def result(self,*a):return ([{'name':'x','records':'0'}],{'QueryExecutionId':'test','Statistics':{}})
  def put_json(self,key,value):writes.append(key)
 monkeypatch.setattr(publisher,'AWS',Fake)
 with pytest.raises(ValueError,match='readback'):publisher.publish(tmp_path)
 assert not writes

@pytest.mark.parametrize('invalid',[True,False,None,{}])
def test_boolean_or_empty_auth_is_rejected(invalid):
 from fastapi import FastAPI
 from fastapi.testclient import TestClient
 class Fake:
  def manifest(self):return {'rules':{}}
 def invalid_auth():return invalid
 app=FastAPI();app.include_router(create_router(invalid_auth,Fake()));assert TestClient(app).get('/opportunity-intelligence/methodology').status_code==401

def test_refresh_worker_orchestrates_native_sources_before_publication(tmp_path,monkeypatch):
 import hashlib,io,sys
 import opportunity_engine.__main__ as worker
 calls=[];raw=b'native-source';sha=hashlib.sha256(raw).hexdigest();source={'source_s3_uri':'s3://'+worker.BUCKET+'/'+worker.PREFIX+'/source.csv','source_sha256':sha}
 manifest={'sources':{},'sbir_source':source}
 class S3:
  def get_object(self,**kw):return {'Body':io.BytesIO(json.dumps({'manifest_key':worker.PREFIX+'/releases/prior/manifest.json'} if kw['Key'].endswith('/current.json') else manifest).encode())}
  def download_file(self,bucket,key,target):Path(target).write_bytes(raw)
 from pathlib import Path
 class Fake:
  s3=S3()
 monkeypatch.setattr(worker,'AWS',Fake)
 def execute(command,env,check):
  name=command[2].rsplit('.',1)[-1];calls.append(name);root=Path(env['MIMIR_OPPORTUNITY_WORKDIR']);(root/'inputs').mkdir(exist_ok=True)
  if name=='refresh_sbir':
   p=root/'source.csv';p.write_bytes(raw);(root/'inputs/sbir_source.json').write_text(json.dumps(dict(source,path=str(p))))
  else:
   pointer={'refresh_topics':'topics','refresh_shared_sources':'shared_sources','refresh_commercial_sources':'commercial','connect_cloud_evidence':'connections','refresh_company_registry':'company_registry','refresh_company_history':'company_history','refresh_innovation':'innovation'}[name];p=root/('input_'+pointer+'.json');p.write_text(json.dumps({'sources':{}}));(root/('inputs/'+pointer+'_latest.json')).write_text(json.dumps({'path':str(p)}))
 monkeypatch.setattr(worker.subprocess,'run',execute)
 monkeypatch.setattr(worker,'build',lambda *a,**kw:calls.append('prebuild' if kw.get('skip_matching') else 'build'))
 monkeypatch.setattr(worker,'enrich',lambda *a:calls.append('enrich'))
 monkeypatch.setattr(worker,'validate',lambda *a:{'passed':1,'failed':0})
 monkeypatch.setattr(worker,'publish',lambda *a:calls.append('publish'))
 monkeypatch.setattr(sys,'argv',['worker','--from-current','--workdir',str(tmp_path),'--as-of','2026-10-29','--refresh','--publish']);worker.main()
 assert calls==['refresh_sbir','refresh_topics','refresh_shared_sources','refresh_commercial_sources','refresh_company_registry','refresh_company_history','refresh_innovation','prebuild','connect_cloud_evidence','build','enrich','publish']
