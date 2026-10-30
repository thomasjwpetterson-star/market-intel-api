"""Publish a validated immutable release; no production source-table replacements."""
import hashlib,json,re
from pathlib import Path
from .aws import AWS,BUCKET,PREFIX,DATABASE

def publish(directory):
 r=Path(directory);m=json.loads((r/'manifest.json').read_text());v=json.loads((r/'validation.json').read_text())
 if v.get('failed') or not v.get('passed'):raise ValueError('Validation must pass before publication')
 a=AWS();run=re.sub('[^a-z0-9_]','_',m['run_id'].lower());base=PREFIX+'/releases/'+run
 # Repeated publication of a completed release is safe; changed content needs a new run ID.
 try:
  prior=json.loads(a.s3.get_object(Bucket=BUCKET,Key=base+'/manifest.json')['Body'].read())
 except a.s3.exceptions.NoSuchKey:prior=None
 if prior is not None:
  if prior['logic_sha256']!=m['logic_sha256'] or {n:t['sha256'] for n,t in prior['tables'].items()}!={n:t['sha256'] for n,t in m['tables'].items()}:raise ValueError('Completed release is immutable; use a new run ID')
  return prior
 # Archive refreshed native SBIR as an immutable private source, never a derived contact table.
 sbir=m['sbir_source']
 if not sbir.get('source_s3_uri'):
  p=Path(sbir['path'])
  if hashlib.sha256(p.read_bytes()).hexdigest()!=sbir['source_sha256']:raise ValueError('SBIR source hash changed')
  key=PREFIX+'/sources/sbir/sha256='+sbir['source_sha256']+'/source.csv';a.upload(p,key);sbir['source_s3_uri']='s3://'+BUCKET+'/'+key
 # Archive all input bindings so subsequent builds need no research workstation.
 for name,source in m['sources'].items():
  files=source.get('files') or [source]
  for f in files:
   if not f.get('s3_uri'):
    p=Path(f['path'])
    if hashlib.sha256(p.read_bytes()).hexdigest()!=f['sha256']:raise ValueError('Input hash changed: '+name)
    key=PREFIX+'/sources/content/sha256='+f['sha256']+'/source.parquet';a.upload(p,key);f['s3_uri']='s3://'+BUCKET+'/'+key
  if 'files' not in source:source['files']=[{k:source[k] for k in ('path','sha256','s3_uri')}]
 # Register the archived company statements for bounded evidence drill-downs.
 if 'company_work' in m['sources'] and not m.get('source_tables'):
  import duckdb
  source=m['sources']['company_work'];f=source['files'][0];description=duckdb.connect().execute('DESCRIBE SELECT * FROM read_parquet(?)',[f['path']]).fetchall();columns=[dict(Name=n,Type='string' if typ=='VARCHAR' else 'bigint' if typ in ('BIGINT','INTEGER') else 'double' if typ=='DOUBLE' else 'string') for n,typ,*_ in description]
  name='oi_'+run+'_source_company_work';location=f['s3_uri'].rsplit('/',1)[0]+'/'
  a.register(name,location,columns);m['source_tables']={'company_work':dict(table=name,rows=source['rows'])}
 # Topic HTML is part of the evidence bundle, including listing coverage.
 topic=m['sources'].get('sbir_topics')
 if topic and not topic.get('raw_archives'):
  folder=Path(topic['files'][0]['path']).parent;archives=[]
  for p in sorted(folder.glob('*.html')):
   sha=hashlib.sha256(p.read_bytes()).hexdigest();key=PREFIX+'/sources/content/sha256='+sha+'/source.html';a.upload(p,key);archives.append(dict(sha256=sha,s3_uri='s3://'+BUCKET+'/'+key))
  if archives:topic['raw_archives']=archives
 # Unchanged immutable tables may be reused only after matching their parent release metadata.
 parent=None
 if m.get('reuse_parent_release'):
  parent_key=PREFIX+'/releases/'+m['reuse_parent_release']+'/manifest.json'
  parent=json.loads(a.s3.get_object(Bucket=BUCKET,Key=parent_key)['Body'].read())
 for source in m['sources'].values():
  for raw in source.get('raw_archives',[]):
   if raw.get('s3_uri'):continue
   p=Path(raw['path']);sha=hashlib.sha256(p.read_bytes()).hexdigest()
   if sha!=raw['sha256']:raise ValueError('Raw source changed')
   key=PREFIX+'/sources/content/sha256='+sha+'/source.archive';a.upload(p,key);raw['s3_uri']='s3://'+BUCKET+'/'+key
 for name,t in m['tables'].items():
  if t.get('reuse_parent_binding'):
   original=parent['tables'][name] if parent else None
   if not original or any(t.get(k)!=original.get(k) for k in ('sha256','rows','table','s3_uri','columns')):raise ValueError('Invalid immutable parent binding: '+name)
   print('Reused verified immutable table',name,t['rows'],flush=True);continue
  if t.get('remote_binding'):
   source=m['sources'][t['source_binding']]
   if any(t.get(k)!=source.get(k) for k in ('rows','sha256','table','s3_uri','columns')):raise ValueError('Distributed serving binding changed: '+name)
   print('Reused complete distributed serving table',name,t['rows'],flush=True);continue
  if t.get('source_binding'):
   source=m['sources'][t['source_binding']];files=source['files']
   if len(files)!=1 or files[0]['sha256']!=t['sha256'] or source['rows']!=t['rows']:raise ValueError('Serving source binding mismatch: '+name)
   t['table']='oi_'+run+'_'+name;t['s3_uri']=files[0]['s3_uri'].rsplit('/',1)[0]+'/'
   a.register(t['table'],t['s3_uri'],t['columns']);print('Registered complete source binding',name,t['rows'],flush=True);continue
  p=Path(t['path'])
  if hashlib.sha256(p.read_bytes()).hexdigest()!=t['sha256']:raise ValueError('Output hash changed: '+name)
  key=base+'/tables/'+name+'/data/part-00000.parquet';a.upload(p,key)
  t['table']='oi_'+run+'_'+name;t['s3_uri']='s3://'+BUCKET+'/'+base+'/tables/'+name+'/data/'
  a.register(t['table'],t['s3_uri'],t['columns']);print('Registered',t['table'],t['rows'],flush=True)
 # Verify remote Athena reads each actual S3 table before moving the pointer.
 sql=' UNION ALL '.join("SELECT '"+n+"' name,count(*) records FROM "+DATABASE+'.'+t['table'] for n,t in m['tables'].items())
 remote,e=a.result(sql);observed={x['name']:int(x['records']) for x in remote}
 if observed!={n:t['rows'] for n,t in m['tables'].items()}:raise ValueError('AWS readback count mismatch')
 if 'company_award_facts' in m['tables'] and m['tables']['company_award_facts'].get('remote_binding'):
  t=m['tables']['company_award_facts'];facts,evidence_query=a.result('SELECT count(*)-count(DISTINCT award_key) duplicates,count_if(substr(latest_action_date,1,10)>'+"'"+m['as_of']+"'"+') future_rows FROM '+DATABASE+'.'+t['table'])
  if int(facts[0]['duplicates']) or int(facts[0]['future_rows']):raise ValueError('Distributed award integrity check failed')
  m['distributed_award_validation']=dict(query_id=evidence_query['QueryExecutionId'],checks=facts)
 m['remote_validation']={'query_id':e['QueryExecutionId'],'counts':observed,'scanned_bytes':e['Statistics'].get('DataScannedInBytes',0)}
 m['source_status']={n:{'rows':s.get('rows'),'as_of':s.get('as_of'),'kind':s.get('kind','live_athena_extract'),'query_id':s.get('query_id'),'status':s.get('status','retained_versioned_source'),'partition_coverage':s.get('partition_coverage'),'source_gap':s.get('source_gap'),'identity_quality':s.get('forecast_identity_quality')} for n,s in m['sources'].items()}
 m['source_status']['sbir_awards']={k:m['sbir_source'].get(k) for k in ['source_url','source_sha256','source_s3_uri','retrieved_at','bytes']}
 m['limitations']=['Authenticated opportunity intelligence; publication permission is separate from company access','SBIR phase history is observed research, not a Phase III forecast','SBA listing coverage was verified at retrieval; agency amendments and eligibility still require agency verification','Company eligibility, technical access and unplaced supplier work remain evidence gates','Each source retains its own observation and refresh dates; a build timestamp does not advance historical source dates']
 key=base+'/manifest.json';a.put_json(key,m);a.put_json(base+'/validation.json',v);a.put_json(base+'/rules.json',m['rules'])
 a.put_json(PREFIX+'/current.json',dict(run_id=m['run_id'],manifest_key=key,logic_sha256=m['logic_sha256']))
 (r/'published.json').write_text(json.dumps(m,indent=2,default=str)+'\n');print('Shared release published:',key,flush=True);return m
