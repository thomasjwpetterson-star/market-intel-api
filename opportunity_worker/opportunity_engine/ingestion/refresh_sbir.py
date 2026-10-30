import os
"""Refresh the existing public UEI-bearing SBIR bulk source, with bounded bytes."""
import hashlib,json,os,urllib.request,datetime
from pathlib import Path
R=Path(os.environ.get('MIMIR_OPPORTUNITY_WORKDIR',str(Path(__file__).resolve().parent)))/'inputs';R.mkdir(exist_ok=True)
u='https://data.www.sbir.gov/mod_awarddatapublic/award_data.csv';p=R/'sbir_awards.csv.partial';h=hashlib.sha256();size=0
with urllib.request.urlopen(urllib.request.Request(u,headers={'User-Agent':'Mimir source ingestion (public SBIR awards)'}),timeout=45) as response,p.open('wb') as out:
 expected=int(response.headers.get('Content-Length') or 0)
 if expected>500*1024*1024:raise ValueError('Unexpected SBIR source size')
 while True:
  block=response.read(4*1024*1024)
  if not block:break
  size+=len(block)
  if size>500*1024*1024:raise ValueError('Source exceeds bound')
  out.write(block);h.update(block)
  if size%(40*1024*1024)==0:print('SBIR downloaded',size,flush=True)
 assert not expected or expected==size
sha=h.hexdigest();final=R/('sbir_awards_'+sha+'.csv');os.replace(p,final)
m=dict(source_url=u,source_sha256=sha,bytes=size,retrieved_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),path=str(final),status='retrieved_public_bulk_source',freshness='Retrieval time is not an award publication date')
(R/'sbir_source.json').write_text(json.dumps(m,indent=2)+'\n');print(json.dumps(m,indent=2),flush=True)
