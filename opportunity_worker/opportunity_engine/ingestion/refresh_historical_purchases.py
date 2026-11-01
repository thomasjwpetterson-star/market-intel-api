"""Refresh held annual purchase evidence, with no upstream writes."""
import argparse,json,os
from pathlib import Path
from opportunity_engine.aws import AWS
from opportunity_engine.historical_purchases import refresh

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--as-of',required=True);args=p.parse_args()
    root=Path(os.environ['MIMIR_OPPORTUNITY_WORKDIR']);previous=json.loads(Path(os.environ['MIMIR_OPPORTUNITY_PREVIOUS_MANIFEST']).read_text())
    result=refresh(previous,root,AWS(),args.as_of)
    path=root/'inputs/historical_purchases_latest_manifest.json';path.write_text(json.dumps({'sources':{'historical_item_purchase_lines':result}},indent=2)+'\n')
    (root/'inputs/historical_purchases_latest.json').write_text(json.dumps({'path':str(path)})+'\n')
