"""Preserve completed derivations after a late failure, without publishing them."""
import json,re
from pathlib import Path
from .aws import BUCKET,PREFIX

def load_derived_checkpoint(aws,key,current):
 """Resume owned, completed derivations; this never promotes a checkpoint."""
 pattern=re.escape(PREFIX)+r'/operations/checkpoints/[A-Za-z0-9_]+/manifest\.json'
 if not re.fullmatch(pattern,key):raise ValueError('Invalid derived checkpoint key')
 m=json.loads(aws.s3.get_object(Bucket=BUCKET,Key=key)['Body'].read())
 required={'opportunities','demand_decisions','published_calls','company_profiles','company_registry','company_scope_features','company_work_matches','company_item_matches','company_matches','dla_forecasts','dla_inventory'}
 if m.get('checkpoint_phase')!='unpublished_completed_derivation' or m.get('checkpoint_parent_release')!=current['run_id'] or not required.issubset(m.get('tables',{})):raise ValueError('Checkpoint is not a complete matching derivation of the current release')
 for name,t in m['tables'].items():
  if t.get('remote_binding'):continue
  uri=t.get('recovery_s3_uri') or t.get('s3_uri')
  if not uri or not uri.startswith('s3://'+BUCKET+'/'+PREFIX+'/') or not re.fullmatch('[a-f0-9]{64}',t.get('sha256','')):raise ValueError('Invalid checkpoint table provenance: '+name)
 m['resumed_checkpoint_key']=key;return m

def archive_failed_attempt(directory,aws,parent_release):
 r=Path(directory);m=json.loads((r/'manifest.json').read_text())
 if not re.fullmatch('[a-zA-Z0-9_]+',m['run_id']):raise ValueError('Invalid checkpoint identity')
 base=PREFIX+'/operations/checkpoints/'+m['run_id']
 # Archive derived input bindings using the same content-addressed convention
 # as publication. Existing immutable source objects are only referenced.
 for source in m['sources'].values():
  for f in source.get('files',[]):
   if not f.get('s3_uri'):
    key=PREFIX+'/sources/content/sha256='+f['sha256']+'/source.parquet';aws.upload(Path(f['path']),key);f['s3_uri']='s3://'+BUCKET+'/'+key
 sbir=m.get('sbir_source',{})
 if sbir and not sbir.get('source_s3_uri'):
  key=PREFIX+'/sources/sbir/sha256='+sbir['source_sha256']+'/source.csv';aws.upload(Path(sbir['path']),key);sbir['source_s3_uri']='s3://'+BUCKET+'/'+key
 for name,t in m['tables'].items():
  if t.get('remote_binding') or t.get('reuse_parent_binding'):continue
  source=m['sources'].get(t.get('source_binding'),{});files=source.get('files',[])
  if len(files)==1 and files[0]['sha256']==t['sha256']:t['recovery_s3_uri']=files[0]['s3_uri']
  else:
   key=base+'/tables/'+name+'/'+t['sha256']+'.parquet';aws.upload(Path(t['path']),key);t['recovery_s3_uri']='s3://'+BUCKET+'/'+key
 for name in ('validation','demand_integrity'):
  path=r/(name+'.json')
  if path.exists():aws.put_json(base+'/'+name+'.json',json.loads(path.read_text()))
 m['checkpoint_parent_release']=parent_release;m['checkpoint_phase']='unpublished_completed_derivation'
 key=base+'/manifest.json';aws.put_json(key,m);print('Unpublished derivation checkpoint:',key,flush=True);return key
