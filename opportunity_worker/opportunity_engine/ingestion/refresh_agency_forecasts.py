"""Refresh official requirement-level forecasts without changing canonical data."""
import datetime
import hashlib
import json
import os
import urllib.parse
import urllib.request
from pathlib import Path

from bs4 import BeautifulSoup
from opportunity_engine.agency_forecasts import (INDEX,WORKBOOK,WEAPONS,AIRCRAFT,
    workbook_rows,weapons_rows,normalize,write_source)


def refresh(root):
    now=datetime.datetime.now(datetime.timezone.utc).isoformat()
    run='agency_forecasts_'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out=Path(root)/'inputs'/run;out.mkdir(parents=True,exist_ok=True)
    archives=[];health=[];records=[]
    def fetch(url):
        if urllib.parse.urlparse(url).hostname!='www.navair.navy.mil':
            raise ValueError('Agency forecast source host is outside the allowlist')
        with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'Mimir public evidence ingestion'}),timeout=45) as response:
            if urllib.parse.urlparse(response.url).hostname!='www.navair.navy.mil':
                raise ValueError('Agency source redirected outside its official host')
            data=response.read(25*1024*1024+1)
        if not data or len(data)>25*1024*1024:
            raise ValueError('Agency source exceeds size bound or is empty')
        sha=hashlib.sha256(data).hexdigest();path=out/(sha+'.source');path.write_bytes(data)
        archives.append(dict(url=url,path=str(path),sha256=sha));return data,sha
    url=WORKBOOK
    listing_label=None
    try:
        listing,_=fetch(INDEX);soup=BeautifulSoup(listing,'html.parser')
        links={urllib.parse.urljoin(INDEX,a['href']) for a in soup.select('a[href]') if '.xlsx' in a['href'].lower() and 'LRAE' in (a.get_text(' ',strip=True)+' '+a['href']).upper()}
        if len(links)!=1:raise ValueError('The current NAVAIR workbook is not uniquely identified')
        url=next(iter(links))
        listing_label=next((a.get_text(' ',strip=True) for a in soup.select('a[href]') if urllib.parse.urljoin(INDEX,a['href'])==url),None)
    except Exception as error:
        health.append(dict(source='navair_index',url=INDEX,status='listing_unavailable_using_known_document_url',reason=str(error)))
    try:
        raw,sha=fetch(url)
        batch=[normalize(row,url,sha,locator,now,'navair_consolidated') for row,locator in workbook_rows(raw)]
        records.extend(batch)
        health.append(dict(source='navair_consolidated',url=url,status='retrieved_validated_layout',rows=len(records),edition_label=listing_label or 'Known consolidated December 2025 document; current listing unverified'))
    except Exception as error:
        health.append(dict(source='navair_consolidated',url=url,status='source_unavailable_not_zero_demand',reason=str(error)))
    try:
        raw,sha=fetch(WEAPONS);start=len(records)
        batch=[normalize(row,WEAPONS,sha,locator,now,'nawcwd') for row,locator in weapons_rows(raw)]
        records.extend(batch)
        health.append(dict(source='nawcwd',url=WEAPONS,status='retrieved_validated_layout',rows=len(records)-start,source_edition='2026-01-13'))
    except Exception as error:
        health.append(dict(source='nawcwd',url=WEAPONS,status='source_unavailable_not_zero_demand',reason=str(error)))
    # This newer book contains outlined glyphs rather than extractable text.
    # Archive it and expose the missing coverage instead of accepting an empty
    # parse or falsely claiming the older consolidated rows are its latest copy.
    try:
        raw,sha=fetch(AIRCRAFT)
        health.append(dict(source='nawcad_current',url=AIRCRAFT,source_sha256=sha,status='document_archived_requirement_OCR_pending',rows=None,reason='The October 2026 document requires a validated OCR/table extraction path; consolidated rows are not treated as its successor.'))
    except Exception as error:
        health.append(dict(source='nawcad_current',url=AIRCRAFT,status='source_unavailable_not_zero_demand',reason=str(error)))
    previous=os.environ.get('MIMIR_OPPORTUNITY_PREVIOUS_MANIFEST')
    if previous and Path(previous).exists():
        old=json.loads(Path(previous).read_text()).get('sources',{}).get('agency_acquisition_forecasts')
        if old:
            import duckdb
            failed={x['source'] for x in health if x['status']=='source_unavailable_not_zero_demand'}
            c=duckdb.connect();q=c.read_parquet([f['path'] for f in old['files']]);names=q.columns
            for values in q.fetchall():
                row=dict(zip(names,values))
                if row['source_family'] in failed:records.append(row)
            c.close();archives.extend(old.get('raw_archives',[]))
    source=write_source(records,out,health,archives,now)
    manifest=out/'manifest.json';manifest.write_text(json.dumps(dict(run_id=run,sources={'agency_acquisition_forecasts':source}),indent=2)+'\n')
    (Path(root)/'inputs/agency_forecasts_latest.json').write_text(json.dumps(dict(path=str(manifest)))+'\n')
    print('Agency forecast coverage',json.dumps(health),flush=True)
    return source


if __name__=='__main__':
    refresh(os.environ.get('MIMIR_OPPORTUNITY_WORKDIR',str(Path.cwd())))
