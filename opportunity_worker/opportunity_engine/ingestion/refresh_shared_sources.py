import os
"""Read the live SAM and budget sources into immutable AWS engine snapshots."""
import argparse,concurrent.futures,datetime,json,uuid
from pathlib import Path
from opportunity_engine.aws import AWS
ap=argparse.ArgumentParser();ap.add_argument('--as-of',default=datetime.datetime.now(datetime.timezone.utc).date().isoformat());args=ap.parse_args()
R=Path(os.environ.get('MIMIR_OPPORTUNITY_WORKDIR',str(Path(__file__).resolve().parent)));asof=datetime.date.fromisoformat(args.as_of);run=asof.strftime('%Y%m%d')+'_'+uuid.uuid4().hex[:12];root=R/'inputs'/run;root.mkdir(parents=True)
from opportunity_engine.shared_source_queries import queries as source_queries
queries=source_queries(asof.isoformat())
aws=AWS()
def one(item):
 name,sql=item;print('Extracting',name,flush=True);r=aws.extract(name,sql,root,run);r['as_of']=asof.isoformat();print('Extracted',name,r['scanned_bytes'],'bytes scanned',flush=True);return name,r
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:results=dict(pool.map(one,queries.items()))
m=dict(run_id=run,as_of=asof.isoformat(),sources=results);(root/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');(R/'inputs/shared_sources_latest.json').write_text(json.dumps(dict(path=str(root/'manifest.json')),indent=2)+'\n');aws.put_json('governance/opportunity_intelligence/sources/'+run+'/manifest.json',m);print(json.dumps(m,indent=2),flush=True)
