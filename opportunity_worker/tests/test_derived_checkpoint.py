import hashlib,io,json,shutil
from types import SimpleNamespace
import duckdb
import pytest
from opportunity_engine.aws import BUCKET,PREFIX
from opportunity_engine.checkpoint import load_derived_checkpoint
from opportunity_engine import quality
from opportunity_engine import checkpoint as checkpoint_module

KEY=PREFIX+'/operations/checkpoints/20261031_121235_35a70b/manifest.json'

def checkpoint():
 names='opportunities demand_decisions published_calls company_profiles company_registry company_scope_features company_work_matches company_item_matches company_matches dla_forecasts dla_inventory'.split()
 return dict(run_id='derived',checkpoint_phase='unpublished_completed_derivation',checkpoint_parent_release='live',tables={n:dict(rows=1,sha256='a'*64,recovery_s3_uri='s3://'+BUCKET+'/'+PREFIX+'/operations/checkpoints/derived/'+n+'.parquet') for n in names})

def test_recovery_requires_completed_matching_from_the_actual_current_parent():
 m=checkpoint();aws=SimpleNamespace(s3=SimpleNamespace(get_object=lambda **_:dict(Body=io.BytesIO(json.dumps(m).encode()))))
 assert load_derived_checkpoint(aws,KEY,dict(run_id='live'))['resumed_checkpoint_key']==KEY
 for bad in ('other/prefix/manifest.json',PREFIX+'/operations/checkpoints/../manifest.json'):
  with pytest.raises(ValueError):load_derived_checkpoint(aws,bad,dict(run_id='live'))
 with pytest.raises(ValueError):load_derived_checkpoint(aws,KEY,dict(run_id='another-live-release'))
 del m['tables']['company_work_matches']
 with pytest.raises(ValueError):load_derived_checkpoint(aws,KEY,dict(run_id='live'))

def test_recovery_rejects_foreign_or_unfingerprinted_output_objects():
 m=checkpoint();aws=SimpleNamespace(s3=SimpleNamespace(get_object=lambda **_:dict(Body=io.BytesIO(json.dumps(m).encode()))))
 m['tables']['opportunities']['recovery_s3_uri']='s3://another-bucket/private/data'
 with pytest.raises(ValueError):load_derived_checkpoint(aws,KEY,dict(run_id='live'))
 m=checkpoint();m['tables']['opportunities']['sha256']='missing'
 with pytest.raises(ValueError):load_derived_checkpoint(aws,KEY,dict(run_id='live'))


@pytest.mark.parametrize('corrupt',[False,True])
def test_validated_restore_requires_all_gates_and_exact_table_bytes(tmp_path,monkeypatch,corrupt):
 m=checkpoint();blob=b'immutable output';sha=hashlib.sha256(blob).hexdigest();m.update(sources={},logic_sha256='logic')
 for t in m['tables'].values():t['sha256']=sha
 names={'opportunities_unique','demand_decisions_unique','company_profiles_unique','decision_population','decision_record_version','no_invented_company_value','public_not_silently_released'}
 for name in m['tables']:names.update({name+'_content_hash',name+'_count'})
 v=dict(failed=0,passed=len(names),logic_sha256='logic',checks=[dict(name=name,passed=True) for name in names]);checked=[]
 class S3:
  def get_object(self,**kw):return {'Body':io.BytesIO(json.dumps(v if kw['Key'].endswith('/validation.json') else m).encode())}
  def download_file(self,bucket,key,path):__import__('pathlib').Path(path).write_bytes(b'changed' if corrupt else blob)
 monkeypatch.setattr(__import__('opportunity_engine.validate',fromlist=['validate']),'validate',lambda directory:checked.append(directory))
 aws=SimpleNamespace(s3=S3())
 if corrupt:
  with pytest.raises(ValueError,match='table changed'):checkpoint_module.restore_validated_checkpoint(aws,KEY,{'run_id':'live'},tmp_path)
  assert not checked;return
 out=checkpoint_module.restore_validated_checkpoint(aws,KEY,{'run_id':'live'},tmp_path);saved=json.loads((out/'manifest.json').read_text());assert checked==[out]
 assert 'checkpoint_phase' not in saved and saved['published_from_validated_checkpoint_key']==KEY
 v['checks']=v['checks'][:1];v['passed']=1
 with pytest.raises(ValueError,match='complete matching passed validation'):checkpoint_module.restore_validated_checkpoint(aws,KEY,{'run_id':'live'},tmp_path/'rejected')

@pytest.mark.parametrize('recover',[True,False])
@pytest.mark.parametrize('fail',[True,False])
def test_review_downloads_immutable_bindings_without_treating_a_checkpoint_as_published(tmp_path,monkeypatch,recover,fail):
 source=tmp_path/'source.parquet';c=duckdb.connect();c.execute("COPY (SELECT 1 AS sample_value) TO '"+str(source)+"' (FORMAT PARQUET)");c.close();sha=hashlib.sha256(source.read_bytes()).hexdigest();calls=[]
 def download(bucket,key,path):calls.append((bucket,key));shutil.copyfile(source,path)
 for module,name in [('sam_approved','augment'),('notice_prices','refresh'),('item_presence','hold'),('item_requests','link')]:monkeypatch.setattr(__import__('opportunity_engine.'+module,fromlist=[name]),name,lambda *_:None)
 def assess(*_):
  if fail:raise RuntimeError('review interrupted')
 monkeypatch.setattr(quality,'apply',assess)
 saved=[];monkeypatch.setattr(checkpoint_module,'archive_failed_attempt',lambda directory,aws,parent:saved.append(parent))
 meta=dict(run_id='base',sources={},tables={'opportunities':dict(rows=1,sha256=sha,**({'recovery_s3_uri':'s3://'+BUCKET+'/'+PREFIX+'/operations/checkpoints/base/table.parquet'} if recover else {'s3_uri':'s3://'+BUCKET+'/'+PREFIX+'/releases/base/table/data/'}))})
 if recover:meta.update(checkpoint_phase='unpublished_completed_derivation',checkpoint_parent_release='live')
 if fail:
  with pytest.raises(RuntimeError,match='review interrupted'):quality.review_current(meta,tmp_path/'run',SimpleNamespace(s3=SimpleNamespace(download_file=download)))
  assert saved==['live' if recover else 'base'];return
 out=quality.review_current(meta,tmp_path/'run',SimpleNamespace(s3=SimpleNamespace(download_file=download)));m=json.loads((out/'manifest.json').read_text())
 assert hashlib.sha256(open(m['tables']['opportunities']['path'],'rb').read()).hexdigest()==sha
 assert calls==[(BUCKET,PREFIX+('/operations/checkpoints/base/table.parquet' if recover else '/releases/base/table/data/part-00000.parquet'))]
 assert bool(m['tables']['opportunities'].get('reuse_parent_binding')) is not recover
 assert (m.get('reuse_parent_release')=='base') is not recover
 assert 'checkpoint_phase' not in m
