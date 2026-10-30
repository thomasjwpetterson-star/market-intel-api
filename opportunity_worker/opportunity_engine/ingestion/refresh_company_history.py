"""Refresh whole-warehouse award and supplier evidence into immutable bindings."""
import argparse,concurrent.futures,datetime,json,os,uuid
from pathlib import Path
from opportunity_engine.aws import AWS
from opportunity_engine.native_sources import queries

def refresh(root,as_of):
    run='company_history_'+as_of.replace('-','')+'_'+uuid.uuid4().hex[:8]
    root=Path(root);folder=root/'inputs'/run;folder.mkdir(parents=True,exist_ok=True)
    def one(item):
        name,sql=item
        print('Refreshing whole-warehouse '+name,flush=True)
        result=AWS().extract(name,sql,folder,run)
        result.update(as_of=as_of,kind='native_warehouse_complete_projection',status='SUCCEEDED')
        print('Refreshed '+name,flush=True)
        return name,result
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        sources=dict(pool.map(one,queries(as_of).items()))
    result=dict(run_id=run,as_of=as_of,sources=sources)
    path=folder/'manifest.json';path.write_text(json.dumps(result,indent=2)+'\n')
    (root/'inputs/company_history_latest.json').write_text(json.dumps(dict(path=str(path)))+'\n')
    AWS().put_json('governance/opportunity_intelligence/sources/'+run+'/manifest.json',result)
    return result

if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('--as-of',default=datetime.datetime.now(datetime.timezone.utc).date().isoformat());args=ap.parse_args()
    refresh(os.environ.get('MIMIR_OPPORTUNITY_WORKDIR',str(Path.cwd())),args.as_of)
