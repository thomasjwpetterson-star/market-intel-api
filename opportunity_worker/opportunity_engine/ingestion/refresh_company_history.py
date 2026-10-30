"""Refresh whole-warehouse award and supplier evidence into immutable bindings."""
import argparse,concurrent.futures,datetime,json,os,uuid
from pathlib import Path
from opportunity_engine.aws import AWS
from opportunity_engine.native_sources import queries

def refresh(root,as_of):
    from opportunity_engine.distributed_history import refresh as distributed_refresh
    prior_path=os.environ.get('MIMIR_OPPORTUNITY_PREVIOUS_MANIFEST')
    previous=json.loads(Path(prior_path).read_text()).get('sources',{}) if prior_path else {}
    result=distributed_refresh(AWS(),root,as_of,queries(as_of),previous)
    path=Path(root)/'inputs'/result['run_id']/'manifest.json'
    (Path(root)/'inputs/company_history_latest.json').write_text(json.dumps(dict(path=str(path)))+'\n')
    return result

if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('--as-of',default=datetime.datetime.now(datetime.timezone.utc).date().isoformat());args=ap.parse_args()
    refresh(os.environ.get('MIMIR_OPPORTUNITY_WORKDIR',str(Path.cwd())),args.as_of)
