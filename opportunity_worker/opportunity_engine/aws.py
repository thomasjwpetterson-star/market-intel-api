"""AWS I/O confined to the existing lake and an isolated opportunity namespace."""
import hashlib,json,re,time,uuid
from pathlib import Path
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
BUCKET='a-and-d-intel-lake-newaccount'
DATABASE='market_intel_platform_candidate'
PREFIX='governance/opportunity_intelligence'
class AWS:
 def __init__(self):
  cfg=Config(connect_timeout=10,read_timeout=60,retries={'max_attempts':4,'mode':'standard'})
  self.s3=boto3.client('s3',region_name='us-east-1',config=cfg);self.glue=boto3.client('glue',region_name='us-east-1',config=cfg);self.athena=boto3.client('athena',region_name='us-east-1',config=cfg)
  account=boto3.client('sts',region_name='us-east-1',config=cfg).get_caller_identity()['Account']
  if account!='868631722720':raise ValueError('Wrong AWS account for Mimir lake')
 def query(self,sql,timeout=600):
  qid=self.athena.start_query_execution(QueryString=sql,QueryExecutionContext={'Database':DATABASE},ResultConfiguration={'OutputLocation':'s3://'+BUCKET+'/athena-query-results/'},WorkGroup='primary')['QueryExecutionId'];started=time.monotonic()
  while True:
   e=self.athena.get_query_execution(QueryExecutionId=qid)['QueryExecution'];state=e['Status']['State']
   if state in ('SUCCEEDED','FAILED','CANCELLED'):break
   if time.monotonic()-started>timeout:
    self.athena.stop_query_execution(QueryExecutionId=qid);raise TimeoutError('Athena query timed out: '+qid)
   time.sleep(1)
  if state!='SUCCEEDED':raise RuntimeError('Athena '+qid+': '+e['Status'].get('StateChangeReason','failed'))
  return e
 def result(self,sql):
  e=self.query(sql);out=[];names=None
  for pg in self.athena.get_paginator('get_query_results').paginate(QueryExecutionId=e['QueryExecutionId']):
   for r in pg['ResultSet']['Rows']:
    values=[x.get('VarCharValue') for x in r['Data']]
    if names is None:names=values;continue
    out.append(dict(zip(names,values)))
  return out,e
 def put_json(self,key,value):
  if not key.startswith(PREFIX+'/'):raise ValueError('Outside opportunity namespace')
  self.s3.put_object(Bucket=BUCKET,Key=key,Body=json.dumps(value,indent=2,default=str).encode(),ContentType='application/json',ServerSideEncryption='AES256')
 def upload(self,path,key):
  if not key.startswith(PREFIX+'/'):raise ValueError('Outside opportunity namespace')
  self.s3.upload_file(str(path),BUCKET,key,ExtraArgs={'ServerSideEncryption':'AES256'},Config=TransferConfig(max_concurrency=2,multipart_chunksize=8*1024*1024))
 def register(self,name,location,columns,description='Mimir versioned opportunity evidence; internal candidate, not a validated public recommendation'):
  if not re.fullmatch(r'oi_[a-z0-9_]+',name) or not location.startswith('s3://'+BUCKET+'/'+PREFIX+'/'):raise ValueError('Unsafe candidate table target')
  table=dict(Name=name,Description=description,TableType='EXTERNAL_TABLE',Parameters={'classification':'parquet','EXTERNAL':'TRUE','mimir_public_release_approved':'false'},StorageDescriptor=dict(Columns=columns,Location=location,InputFormat='org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',OutputFormat='org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',SerdeInfo={'SerializationLibrary':'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'}))
  try:self.glue.create_table(DatabaseName=DATABASE,TableInput=table)
  except self.glue.exceptions.AlreadyExistsException:
   existing=self.glue.get_table(DatabaseName=DATABASE,Name=name)['Table']['StorageDescriptor']
   if existing['Location']!=location or existing['Columns']!=columns:raise ValueError('Existing immutable table binding differs: '+name)
 def extract(self,name,select,root,run_id):
  outside_literals=re.sub(r"'(?:''|[^'])*'","''",select)
  if not re.fullmatch('[a-z0-9_]+',name) or not select.lstrip().upper().startswith(('SELECT','WITH')) or ';' in outside_literals:raise ValueError('Read-only source SELECT required')
  target=Path(root)/name;target.mkdir(parents=True,exist_ok=True);cached=target/'manifest.json'
  if cached.exists():
   m=json.loads(cached.read_text())
   if m['query_sha256']==hashlib.sha256(select.encode()).hexdigest() and all(Path(f['path']).exists() and hashlib.sha256(Path(f['path']).read_bytes()).hexdigest()==f['sha256'] for f in m['files']):return m
  prefix=PREFIX+'/sources/'+run_id+'/'+name+'/'+uuid.uuid4().hex[:12]+'/data/'
  e=self.query("UNLOAD ("+select+") TO 's3://"+BUCKET+'/'+prefix+"' WITH (format='PARQUET',compression='ZSTD')")
  target=Path(root)/name;target.mkdir(parents=True,exist_ok=True);files=[]
  for pg in self.s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET,Prefix=prefix):
   for o in pg.get('Contents',[]):
    p=target/(o['Key'].split('/')[-1]+'.parquet');self.s3.download_file(BUCKET,o['Key'],str(p));files.append(dict(path=str(p),s3_uri='s3://'+BUCKET+'/'+o['Key'],bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()))
  m=dict(name=name,files=files,query_id=e['QueryExecutionId'],query_sha256=hashlib.sha256(select.encode()).hexdigest(),scanned_bytes=e.get('Statistics',{}).get('DataScannedInBytes',0),status='SUCCEEDED',sql=select)
  cached.write_text(json.dumps(m,indent=2)+'\n');return m
