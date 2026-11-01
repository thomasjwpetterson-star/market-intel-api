"""Refresh existing company/item data from the promoted Main release."""
import json,os
from pathlib import Path
from opportunity_engine.aws import AWS
from opportunity_engine.company_references import refresh

if __name__=='__main__':
    root=Path(os.environ['MIMIR_OPPORTUNITY_WORKDIR']);previous=json.loads(Path(os.environ['MIMIR_OPPORTUNITY_PREVIOUS_MANIFEST']).read_text())
    result=refresh(previous['sources']['item_reference_facts'],root,AWS())
    path=root/'inputs/company_references_latest_manifest.json';path.write_text(json.dumps({'sources':{'item_reference_facts':result}},indent=2)+'\n')
    (root/'inputs/company_references_latest.json').write_text(json.dumps({'path':str(path)})+'\n')
