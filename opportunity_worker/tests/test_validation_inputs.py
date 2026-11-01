import hashlib,json
from pathlib import Path
from types import SimpleNamespace
import pytest
from opportunity_engine.validation_inputs import prepare
from opportunity_engine.aws import BUCKET,PREFIX

def test_stale_distributed_validation_paths_are_restored_and_verified(tmp_path):
    payload=b'exact immutable source';sha=hashlib.sha256(payload).hexdigest();calls=[]
    def download(bucket,key,destination):calls.append((bucket,key));Path(destination).write_bytes(payload)
    source={'files':[dict(path='/missing/prior-worker/links.parquet',s3_uri='s3://'+BUCKET+'/'+PREFIX+'/sources/links.parquet',sha256=sha)]}
    m={'tables':{'market_capability_customer_links':dict(remote_binding=True,source_binding='links'),'company_award_facts':dict(remote_binding=True)},'sources':{'links':source}}
    prepare(tmp_path,m,SimpleNamespace(s3=SimpleNamespace(download_file=download)))
    assert Path(source['files'][0]['path']).read_bytes()==payload and len(calls)==1
    prepare(tmp_path,m,SimpleNamespace(s3=SimpleNamespace(download_file=download)))
    assert len(calls)==1  # No full award-fact download or repeated transfer.

def test_validation_input_corruption_cannot_reach_release_gates(tmp_path):
    def download(bucket,key,destination):Path(destination).write_bytes(b'changed')
    m={'tables':{'company_subcontract_facts':dict(remote_binding=True,source_binding='sub')},'sources':{'sub':{'files':[dict(s3_uri='s3://'+BUCKET+'/'+PREFIX+'/sources/sub.parquet',sha256='a'*64)]}}}
    with pytest.raises(ValueError,match='checksum mismatch'):prepare(tmp_path,m,SimpleNamespace(s3=SimpleNamespace(download_file=download)))

def test_unpublished_recovery_reruns_validation_and_propagates_failure(tmp_path,monkeypatch):
    from opportunity_engine import checkpoint,logic_provenance,validate
    payload=b'completed output';sha=hashlib.sha256(payload).hexdigest();calls=[]
    m={'run_id':'unpublished_test','tables':{'opportunities':dict(sha256=sha,rows=1,recovery_s3_uri='s3://'+BUCKET+'/'+PREFIX+'/operations/checkpoints/test/output.parquet')},'checkpoint_phase':'unpublished_completed_derivation','checkpoint_parent_release':'prior'}
    monkeypatch.setattr(checkpoint,'load_derived_checkpoint',lambda *args:m)
    def download(bucket,key,destination):Path(destination).write_bytes(payload)
    monkeypatch.setattr(logic_provenance,'record',lambda r:calls.append('record'))
    monkeypatch.setattr(validate,'validate_demand_integrity',lambda r:calls.append('integrity'))
    def reject(r,aws):calls.append('all_release_gates');raise ValueError('semantic gate failed')
    monkeypatch.setattr(validate,'validate',reject)
    with pytest.raises(ValueError,match='semantic gate failed'):
        checkpoint.revalidate_derived_checkpoint(SimpleNamespace(s3=SimpleNamespace(download_file=download)),'checkpoint',{'run_id':'prior'},tmp_path)
    assert calls==['record','integrity','all_release_gates']
    assert (tmp_path/'runs/unpublished_test/opportunities.parquet').read_bytes()==payload
