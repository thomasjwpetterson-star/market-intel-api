"""Official innovation feeds; publication, event and response dates stay separate."""
import concurrent.futures,datetime,email.utils,hashlib,json,os,re,urllib.parse,urllib.request,xml.etree.ElementTree as ET
from pathlib import Path
import duckdb
from bs4 import BeautifulSoup
R=Path(os.environ.get('MIMIR_OPPORTUNITY_WORKDIR',str(Path(__file__).resolve().parents[2])));run='innovation_'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');out=R/'inputs'/run;out.mkdir(parents=True,exist_ok=True);now=datetime.datetime.now(datetime.timezone.utc).isoformat();health=[];archives=[]
def fetch(url):
 host=urllib.parse.urlparse(url).hostname
 if host not in ('www.darpa.mil','www.diu.mil'):raise ValueError('Unexpected source host')
 with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'Mimir public evidence ingestion'}),timeout=90) as response:raw=response.read(5*1024*1024)
 if not raw or len(raw)>=5*1024*1024:raise ValueError('Source size outside bound')
 sha=hashlib.sha256(raw).hexdigest();p=out/(sha+'.source');p.write_bytes(raw);archives.append(dict(url=url,path=str(p),sha256=sha));return raw,sha

def darpa():
 url='https://www.darpa.mil/rss/opportunities.xml';raw,sha=fetch(url);items=[]
 for item in ET.fromstring(raw).findall('./channel/item'):
  title=item.findtext('title') or '';description=BeautifulSoup(item.findtext('description') or '','html.parser');links=[a['href'] for a in description.select('a[href]')];guid=item.findtext('guid') or title;pub=item.findtext('pubDate');posted=email.utils.parsedate_to_datetime(pub).isoformat() if pub else None
  items.append(dict(signal_id=hashlib.sha256(('darpa|'+guid).encode()).hexdigest(),native_id=guid,title=title,customer='DARPA',scope=description.get_text(' ',strip=True),source_url=url,detail_url=links[-1] if links else item.findtext('link'),posted_date=posted,response_deadline=None,stage='published_engagement_or_research_context',rule_id='PR17',next_action='Read the linked agency notice or event. Verify response/registration deadlines and technical scope; feed publication is not a response deadline.',eligibility='Not established',value_context_json='[]',source_sha256=sha,retrieved_at=now,record_hash=hashlib.sha256(ET.tostring(item)).hexdigest()))
  # Each RSS entry is independently identified; no items are merged by a generic landing URL.
 health.append(dict(source='darpa_opportunities_rss',status='retrieved',records=len(items),source_url=url));return items

def diu():
 url='https://www.diu.mil/work-with-us/open-solicitations';raw,listing_sha=fetch(url);soup=BeautifulSoup(raw,'html.parser');links=sorted(set(urllib.parse.urljoin(url,a['href']) for a in soup.select('a[href]') if '/work-with-us/submit-solution/' in a['href']))
 if not links and not re.search(r'(?:0 openings|no opportunities open)',soup.get_text(' ',strip=True),re.I):raise ValueError('Unrecognised DIU listing; cannot infer zero openings')
 rows=[]
 for link in links[:100]:
  raw,sha=fetch(link);doc=BeautifulSoup(raw,'html.parser');h=doc.find('h1');title=h.get_text(' ',strip=True) if h else None;text=doc.get_text(' ',strip=True);m=re.search(r'([0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9]{2}:[0-9]{2}:[0-9]{2}\s+US/Eastern Time)',text);deadline=m.group(1) if m else None
  if not title:raise ValueError('Missing DIU project title')
  # Keep only the published project narrative; exclude submission-form fields.
  scope=text.split('Project Description',1)[-1].split('Before You Submit',1)[0].strip();values=[]
  for amount in re.finditer(r'\$[0-9,]+(?:\.[0-9]+)?\s*[MB]?',scope):values.append(dict(source_wording=scope[max(0,amount.start()-90):amount.end()+130],interpretation='Source prize/programme funding context; not company-addressable contract value'))
  rows.append(dict(signal_id=hashlib.sha256(('diu|'+link.rsplit('/',1)[-1]).encode()).hexdigest(),native_id=link.rsplit('/',1)[-1],title=title,customer='Defense Innovation Unit',scope=scope,source_url=link,detail_url=link,posted_date=None,response_deadline=deadline,stage='published_challenge' if deadline else 'engagement_window_unverified',rule_id='PR17',next_action='Review integrated-solution and production-readiness conditions. Consider a teamed submission where explicitly allowed. Verify any CAGE/JCP/CRADA requirements before requesting controlled data.',eligibility='Notice-specific conditions; participation not established for any particular company',value_context_json=json.dumps(values),source_sha256=sha,retrieved_at=now,record_hash=sha))
 health.append(dict(source='diu_pathways',status='retrieved',records=len(rows),source_url=url));return rows
rows=[]
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
 futures={pool.submit(f):n for n,f in [('darpa_opportunities_rss',darpa),('diu_pathways',diu)]}
 for future in concurrent.futures.as_completed(futures):
  try:rows.extend(future.result())
  except Exception as e:health.append(dict(source=futures[future],status='source_unavailable_not_zero_demand',error=str(e)))
if not rows:raise RuntimeError('No innovation source retrieved; preserve the last published release')
# A rolling feed or source outage is not a cancellation/award event.
previous=os.environ.get('MIMIR_OPPORTUNITY_PREVIOUS_MANIFEST')
if previous and Path(previous).exists():
 old_source=json.loads(Path(previous).read_text()).get('sources',{}).get('innovation_signals')
 if old_source:
  prior_query=duckdb.connect().execute('SELECT * FROM read_parquet(?)',[[f['path'] for f in old_source['files']]]);prior=[dict(zip([d[0] for d in prior_query.description],row)) for row in prior_query.fetchall()];seen={x['signal_id'] for x in rows};failed={h['source'] for h in health if h['status']=='source_unavailable_not_zero_demand'}
  for old in prior:
   if old['signal_id'] in seen:continue
   family='diu_pathways' if old['customer']=='Defense Innovation Unit' else 'darpa_opportunities_rss'
   old['stage']='source_freshness_unverified' if family in failed else 'absent_from_latest_open_listing' if family=='diu_pathways' else 'outside_latest_feed_window'
   rows.append(old)
  archives.extend(old_source.get('raw_archives',[]))
p=out/'signals.jsonl';p.write_text(''.join(json.dumps(row)+'\n' for row in rows));c=duckdb.connect();target=out/'signals.parquet';c.execute("COPY (SELECT * FROM read_json_auto(?)) TO '"+str(target).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)",[str(p)]);source=dict(files=[dict(path=str(target),sha256=hashlib.sha256(target.read_bytes()).hexdigest())],as_of=now,kind='official_agency_feeds',health=health,raw_archives=archives,rows=len(rows));manifest=dict(run_id=run,sources={'innovation_signals':source});m=out/'manifest.json';m.write_text(json.dumps(manifest,indent=2)+'\n');(R/'inputs/innovation_latest.json').write_text(json.dumps(dict(path=str(m)),indent=2)+'\n');print(json.dumps(health,indent=2),flush=True)
