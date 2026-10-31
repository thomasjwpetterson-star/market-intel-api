"""Preserve completed derivations after a late failure, without publishing them."""
import hashlib,json,re
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

def restore_validated_checkpoint(aws,key,current,root,retain_prior_innovation=False):
 """Restore the exact passed outputs for publication; never rerun derivations."""
 m=load_derived_checkpoint(aws,key,current)
 v=json.loads(aws.s3.get_object(Bucket=BUCKET,Key=key.rsplit('/',1)[0]+'/validation.json')['Body'].read())
 checks=v.get('checks',[])
 required_checks={'opportunities_unique','demand_decisions_unique','company_profiles_unique','decision_population','decision_record_version','no_invented_company_value','public_not_silently_released'}
 for name,t in m['tables'].items():
  required_checks.add(name+('_distributed_source_integrity' if t.get('remote_binding') else '_content_hash'))
  if not t.get('remote_binding'):required_checks.add(name+'_count')
 if v.get('failed') or not v.get('passed') or len(checks)!=v['passed'] or not all(x.get('passed') is True for x in checks) or not required_checks.issubset({x['name'] for x in checks}) or v.get('logic_sha256')!=m.get('logic_sha256'):raise ValueError('Checkpoint does not have complete matching passed validation')
 r=Path(root)/'runs'/m['run_id'];r.mkdir(parents=True,exist_ok=True)
 for name,t in m['tables'].items():
  if t.get('remote_binding'):continue
  path=r/(name+'.parquet');uri=t.get('recovery_s3_uri') or t.get('s3_uri')
  if not path.exists() or hashlib.sha256(path.read_bytes()).hexdigest()!=t['sha256']:aws.s3.download_file(BUCKET,uri.split('/',3)[3],str(path))
  if hashlib.sha256(path.read_bytes()).hexdigest()!=t['sha256']:raise ValueError('Validated checkpoint table changed: '+name)
  t['path']=str(path);t.pop('reuse_parent_binding',None)
  print('Restored validated output',name,t['rows'],flush=True)
 if not m.get('source_tables') and m['sources'].get('company_work'):
  for i,f in enumerate(m['sources']['company_work']['files']):
   path=r/('source_company_work_'+str(i)+'.parquet');aws.s3.download_file(BUCKET,f['s3_uri'].split('/',3)[3],str(path))
   if hashlib.sha256(path.read_bytes()).hexdigest()!=f['sha256']:raise ValueError('Company work source changed')
   f['path']=str(path)
 # Cohort queries are local release gates; raw award/action facts remain
 # verified immutable distributed bindings and need no workstation download.
 cohort=m['sources'].get('contract_value_cohorts')
 if cohort:
  for i,f in enumerate(cohort['files']):
   path=r/('source_value_cohort_'+str(i)+'.parquet');aws.s3.download_file(BUCKET,f['s3_uri'].split('/',3)[3],str(path))
   if hashlib.sha256(path.read_bytes()).hexdigest()!=f['sha256']:raise ValueError('Value cohort source changed')
   f['path']=str(path)
 if retain_prior_innovation:
  import copy,duckdb
  c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='1GB'")
  for name in ('opportunities','published_calls'):
   c.read_parquet(m['tables'][name]['path']).create_view(name)
   if c.execute("SELECT count(*) FROM "+name+" WHERE source_name='innovation_signals' OR lower(coalesce(source_url,'')) LIKE '%diu.mil%' OR lower(coalesce(source_url,'')) LIKE '%darpa.mil/rss/%'").fetchone()[0]:raise ValueError('Auxiliary feed is referenced by opportunity outputs; cannot retain it independently')
  c.close()
  source=copy.deepcopy(current['sources']['innovation_signals']);t=copy.deepcopy(current['tables']['innovation_signals']);path=r/'innovation_signals.parquet';uri=t['s3_uri']+'part-00000.parquet'
  if t.get('source_binding'):uri=next(f['s3_uri'] for f in source['files'] if f['sha256']==t['sha256'])
  aws.s3.download_file(BUCKET,uri.split('/',3)[3],str(path))
  if hashlib.sha256(path.read_bytes()).hexdigest()!=t['sha256']:raise ValueError('Retained auxiliary source content changed')
  source.update(status='retained_prior_snapshot_raw_archive_unavailable',source_gap='The newer DIU detail raw document was not archived before its worker ended. The complete prior archived feed is retained with its original dates; no opportunity or monetary outputs depend on this standalone feed.',retained_from_release=current['run_id'])
  t['path']=str(path);t.pop('reuse_parent_binding',None);m['sources']['innovation_signals']=source;m['tables']['innovation_signals']=t
  m['auxiliary_source_retention']=dict(source='innovation_signals',prior_release=current['run_id'],reason=source['source_gap'])
 for field in ('checkpoint_phase','checkpoint_parent_release','reuse_parent_release'):m.pop(field,None)
 m['published_from_validated_checkpoint_key']=key
 m['publication_logic_sha256']=hashlib.sha256(Path(__file__).read_bytes()+Path(__file__).with_name('publish.py').read_bytes()+Path(__file__).with_name('raw_archives.py').read_bytes()).hexdigest()
 (r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');(r/'validation.json').write_text(json.dumps(v,indent=2)+'\n')
 from .raw_archives import bind as bind_raw_archives
 bind_raw_archives(aws,m);(r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n')
 from .validate import validate
 validate(r)
 return r

def archive_failed_attempt(directory,aws,parent_release):
 r=Path(directory);m=json.loads((r/'manifest.json').read_text())
 if not re.fullmatch('[a-zA-Z0-9_]+',m['run_id']):raise ValueError('Invalid checkpoint identity')
 base=PREFIX+'/operations/checkpoints/'+m['run_id']
 from .raw_archives import bind as bind_raw_archives
 bind_raw_archives(aws,m,strict=False)
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
