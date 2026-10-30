import json
import duckdb
import pytest
from opportunity_engine.validate import validate_demand_integrity
from opportunity_engine.checkpoint import archive_failed_attempt

@pytest.mark.parametrize('identity',[None,'a'*64])
def test_early_gate_rejects_unidentifiable_or_duplicated_demand(tmp_path,identity):
 c=duckdb.connect();tables={}
 for name,query in [('opportunities',"SELECT ?::VARCHAR episode_id,'hash' record_hash UNION ALL SELECT ?,'hash'"),('demand_decisions',"SELECT ?::VARCHAR episode_id,'hash' source_record_hash UNION ALL SELECT ?,'hash'")]:
  path=tmp_path/(name+'.parquet');c.execute('CREATE OR REPLACE TABLE rows AS '+query,[identity,identity]);c.execute("COPY rows TO '"+str(path)+"' (FORMAT PARQUET)");tables[name]={'path':str(path)}
 (tmp_path/'manifest.json').write_text(json.dumps({'tables':tables}))
 with pytest.raises(ValueError,match='before company matching'):validate_demand_integrity(tmp_path)
 report=json.loads((tmp_path/'demand_integrity.json').read_text());assert report['opportunities_duplicates']>0
 if identity is None:assert report['opportunities_invalid_identity']==2 and report['missing_decisions']==2

def test_failed_checkpoint_preserves_outputs_without_replacing_current_pointer(tmp_path):
 p=tmp_path/'data.parquet';p.write_bytes(b'fixture');manifest={'run_id':'attempt_1','sources':{'derived':{'files':[{'path':str(p),'sha256':'sourcehash'}]}},'tables':{'local':{'path':str(p),'sha256':'outputhash'},'native':{'remote_binding':True,'table':'oi_existing','s3_uri':'s3://existing'}},'sbir_source':{}}
 (tmp_path/'manifest.json').write_text(json.dumps(manifest));(tmp_path/'validation.json').write_text('{"failed":1}')
 class Fake:
  def __init__(self):self.uploads=[];self.documents={}
  def upload(self,path,key):self.uploads.append(key)
  def put_json(self,key,value):self.documents[key]=value
 aws=Fake();key=archive_failed_attempt(tmp_path,aws,'prior');saved=aws.documents[key]
 assert saved['checkpoint_parent_release']=='prior' and saved['tables']['local']['recovery_s3_uri']
 assert 'recovery_s3_uri' not in saved['tables']['native']
 assert all('/current.json' not in k for k in aws.documents)
 assert list(aws.documents)[-1]==key
