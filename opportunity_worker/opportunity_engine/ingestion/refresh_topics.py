import os
"""Official SBA HTML topic fallback. The agency instructions remain authoritative."""
import concurrent.futures,datetime,hashlib,json,re,urllib.parse,urllib.request,uuid
from pathlib import Path
from bs4 import BeautifulSoup
import duckdb
R=Path(os.environ.get('MIMIR_OPPORTUNITY_WORKDIR',str(Path(__file__).resolve().parent)));run='topics_'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');out=R/'inputs'/run;out.mkdir(parents=True)
now=datetime.datetime.now(datetime.timezone.utc).isoformat()
def fetch(url):
 if urllib.parse.urlparse(url).hostname not in ('www.sbir.gov','sbir.gov'):raise ValueError('Unexpected source host')
 with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'Mimir opportunity evidence ingestion'}),timeout=30) as r:b=r.read(4*1024*1024)
 if not b or len(b)>=4*1024*1024:raise ValueError('Unexpected topic page size')
 sha=hashlib.sha256(b).hexdigest();p=out/(sha+'.html');p.write_bytes(b);return BeautifulSoup(b,'html.parser'),sha,str(p)
def date(s):
 try:return datetime.datetime.strptime(s.strip(),'%B %d, %Y').date().isoformat()
 except ValueError:return None
url='https://www.sbir.gov/topics?'+urllib.parse.urlencode({'status':'Open','agency[DOD]':'DOD'})
from opportunity_engine.ingestion.topic_listing import collect
links,pages,expected=collect(fetch,url)
print('Topic listing',len(links),'records across',len(pages),'archived pages; expected',expected,flush=True)
def topic(url):
 soup,sha,path=fetch(url);main=soup.find('main') or soup;txt=main.get_text('\n',strip=True)
 def capture(pattern):
  m=re.search(pattern,txt,re.I);return m.group(1).strip() if m else None
 agency=capture(r'Funding Agency\s*\n([^\n]+)');branch=capture(r'Funding Agency\s*\n[^\n]+\n([^\n]+)')
 if agency not in ('DOD','DOW'):return None
 header=main.find('h2');title=header.get_text(' ',strip=True) if header else None
 desc=main.find(lambda tag:tag.name in ('h2','h3') and tag.get_text(' ',strip=True)=='Description');parts=[]
 if desc:
  for sibling in desc.next_siblings:
   if getattr(sibling,'name',None) in ('h2','h3','footer'):break
   if hasattr(sibling,'get_text'):parts.append(sibling.get_text(' ',strip=True))
 official=next((a.get('href') for a in main.find_all('a',href=True) if 'View Official Solicitation' in a.get_text()),None)
 row=dict(topic_id=url.rsplit('/',1)[1],topic_code=capture(r'Topic Number:\s*([^\n]+)'),solicitation_number=capture(r'Solicitation Number:\s*([^\n]+)'),title=title,agency=agency,branch=branch,phase_raw=capture(r'Tagged as:\s*\n(?:SBIR|STTR)\s*\n([^\n]+)'),program='STTR' if re.search(r'Tagged as:\s*\nSTTR',txt) else 'SBIR',source_status=capture(r'Solicitation Status:\s*([^\n]+)'),release_date=date(capture(r'Release Date\s*\n([^\n]+)') or ''),open_date=date(capture(r'Open Date\s*\n([^\n]+)') or ''),close_date=date(capture(r'Close Date\s*\n([^\n]+)') or ''),description=txt.split('\nDescription\n',1)[-1].strip() if '\nDescription\n' in txt else ' '.join(parts),source_url=url,official_solicitation_url=official,source_sha256=sha,retrieved_at=now,source_archive_path=path,source_authority='SBA copy; current agency instructions must be checked')
 if not row['topic_code'] or not row['title'] or not row['description'] or not row['close_date']:raise ValueError('Incomplete topic fields '+url)
 return row
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:rows=[x for x in pool.map(topic,sorted(links)) if x]
if not rows:raise ValueError('No defence topics parsed')
f=out/'topics.jsonl';f.write_text(''.join(json.dumps(x)+'\n' for x in rows));c=duckdb.connect();p=out/'topics.parquet';c.execute("COPY (SELECT * FROM read_json_auto(?)) TO '"+str(p).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)",[str(f)]);c.close()
manifest=dict(run_id=run,as_of=datetime.datetime.now(datetime.timezone.utc).date().isoformat(),listing_pages=pages,sources={'sbir_topics':dict(name='sbir_topics',status='SUCCEEDED_SBA_HTML_FALLBACK',as_of=datetime.datetime.now(datetime.timezone.utc).date().isoformat(),files=[dict(path=str(p.resolve()),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),bytes=p.stat().st_size)],rows=len(rows),agency_verification='Agency DSIP retrieval returned HTTP403; do not claim agency-current certification')})
(out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n');(R/'inputs/topics_latest.json').write_text(json.dumps(dict(path=str((out/'manifest.json').resolve())),indent=2)+'\n');print('Defence topics captured',len(rows),flush=True)
