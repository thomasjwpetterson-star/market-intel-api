import json
from opportunity_engine.ingestion import refresh_company_history as h

def test_native_refresh_does_not_reuse_same_day_warehouse_projection(tmp_path,monkeypatch):
 import opportunity_engine.distributed_history as distributed
 prior=tmp_path/'previous.json';prior.write_text(json.dumps({'sources':{'old':'snapshot'}}));monkeypatch.setenv('MIMIR_OPPORTUNITY_PREVIOUS_MANIFEST',str(prior));monkeypatch.delenv('MIMIR_OPPORTUNITY_REUSE_SOURCE_SNAPSHOT',raising=False)
 seen=[];monkeypatch.setattr(h,'AWS',lambda:None)
 def run(aws,root,asof,queries,previous):
  seen.append(previous);(tmp_path/'inputs').mkdir(exist_ok=True);return {'run_id':'run'}
 monkeypatch.setattr(distributed,'refresh',run)
 h.refresh(tmp_path,'2026-10-31');assert seen==[{}]
 monkeypatch.setenv('MIMIR_OPPORTUNITY_REUSE_SOURCE_SNAPSHOT','1');h.refresh(tmp_path,'2026-10-31');assert seen[1]=={'old':'snapshot'}
