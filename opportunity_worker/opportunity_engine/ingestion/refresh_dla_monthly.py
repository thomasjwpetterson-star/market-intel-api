"""Refresh validated public monthly DLA inputs, retaining the last complete release."""
import datetime,hashlib,json,os,re,urllib.parse,urllib.request
from pathlib import Path
from bs4 import BeautifulSoup
from opportunity_engine.aws import AWS,BUCKET,PREFIX
from opportunity_engine.dla_monthly import PAGE,compile_sources

POINTER=PREFIX+'/sources/dla_monthly/current.json'
FAMILIES={'forecast':'24monfcst','inventory':'OHandBO','reorder':'OHandROP','procurement':'prochist','parts':'contracthist'}


def load(aws,root):
 pointer=json.loads(aws.s3.get_object(Bucket=BUCKET,Key=POINTER)['Body'].read());key=pointer['manifest_key']
 if not key.startswith(PREFIX+'/sources/dla_monthly/'):raise ValueError('Invalid monthly source pointer')
 m=json.loads(aws.s3.get_object(Bucket=BUCKET,Key=key)['Body'].read());r=Path(root)/'sources';r.mkdir(parents=True,exist_ok=True)
 for source in m['sources'].values():
  for f in source['files']:
   path=r/(f['sha256']+'.parquet');uri=f['s3_uri']
   if not uri.startswith('s3://'+BUCKET+'/'+PREFIX+'/sources/'):raise ValueError('Monthly source outside owned namespace')
   if not path.exists():aws.s3.download_file(BUCKET,uri.split('/',3)[3],str(path))
   if hashlib.sha256(path.read_bytes()).hexdigest()!=f['sha256']:raise ValueError('Monthly source checksum changed')
   f['path']=str(path)
  source['path']=source['files'][0]['path']
 return m


def install(aws,sources,release,validation):
 from opportunity_engine.raw_archives import bind
 m={'sources':sources};bind(aws,m)
 for source in sources.values():
  for f in source['files']:
   key=PREFIX+'/sources/content/sha256='+f['sha256']+'/source.parquet';aws.upload(Path(f['path']),key);f['s3_uri']='s3://'+BUCKET+'/'+key
 key=PREFIX+'/sources/dla_monthly/'+release+'/'+hashlib.sha256(json.dumps({n:s['sha256'] for n,s in sources.items()},sort_keys=True).encode()).hexdigest()+'/manifest.json'
 m.update(source_release=release,source_page=PAGE,validation=validation,processing_sha256=hashlib.sha256(Path(__file__).parent.parent.joinpath('dla_monthly.py').read_bytes()).hexdigest())
 aws.put_json(key,m);aws.put_json(POINTER,dict(manifest_key=key,source_release=release));return m


def refresh(root):
 root=Path(root);root.mkdir(parents=True,exist_ok=True);aws=AWS();previous=None
 try:previous=load(aws,root)
 except aws.s3.exceptions.NoSuchKey:pass
 now=datetime.datetime.now(datetime.timezone.utc);r=root/'inputs'/('dla_monthly_'+now.strftime('%Y%m%dT%H%M%SZ'));r.mkdir(parents=True,exist_ok=True)
 try:
  def fetch(url,path,limit):
   if urllib.parse.urlparse(url).hostname!='www.dla.mil':raise ValueError('DLA source host outside allowlist')
   with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'Mimir public evidence ingestion'}),timeout=45) as response,path.open('wb') as target:
    if urllib.parse.urlparse(response.url).hostname!='www.dla.mil':raise ValueError('DLA source redirected outside official host')
    total=0
    while True:
     chunk=response.read(1024*1024)
     if not chunk:break
     total+=len(chunk)
     if total>limit:raise ValueError('Public source exceeds size bound')
     target.write(chunk)
   if not total:raise ValueError('Empty public source')
  listing=r/'index.html';fetch(PAGE,listing,5*1024*1024);soup=BeautifulSoup(listing.read_bytes(),'html.parser');links={}
  for family,suffix in FAMILIES.items():
   matches={urllib.parse.urljoin(PAGE,a['href']) for a in soup.select('a[href]') if re.search(r'20\d{2}-\d{2}-\d{2}-\d{4}_'+suffix+r'\.zip(?:\?|$)',a['href'],re.I)}
   if len(matches)!=1:raise ValueError('Monthly '+family+' archive not uniquely identified')
   links[family]=next(iter(matches))
  releases={re.search(r'20\d{2}-\d{2}-\d{2}-\d{4}',url)[0] for url in links.values()}
  if len(releases)!=1:raise ValueError('Mixed monthly source releases')
  release=next(iter(releases))
  if previous and previous['source_release']==release:m=previous
  else:
   archives={}
   for family,url in links.items():
    path=r/(family+'.zip');fetch(url,path,150*1024*1024);archives[family]=dict(path=str(path),url=url)
   sources=compile_sources(archives,r/'compiled',release,now.date().isoformat());validation=json.loads((r/'compiled/source-validation.json').read_text());m=install(aws,sources,release,validation)
  health=dict(status='official_listing_checked',attempted_at=now.isoformat(),source_release=release)
 except Exception as error:
  if previous is None:raise
  m=previous;health=dict(status='retained_complete_monthly_snapshot_refresh_failed',attempted_at=now.isoformat(),source_release=m['source_release'],reason=type(error).__name__,interpretation='Original file and business dates remain unchanged; retrieval failure is not zero demand.')
 for source in m['sources'].values():source['refresh_health']=health
 manifest=r/'manifest.json';manifest.write_text(json.dumps(m,indent=2)+'\n');(root/'inputs/dla_monthly_latest.json').write_text(json.dumps({'path':str(manifest)})+'\n');print('DLA monthly refresh',json.dumps(health),flush=True);return m


if __name__=='__main__':refresh(os.environ.get('MIMIR_OPPORTUNITY_WORKDIR',str(Path.cwd())))
