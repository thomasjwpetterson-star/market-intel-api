from copy import deepcopy

import pytest
from platform_reference.contract_lookup_release import with_contract_lookup
from platform_reference.main_release_cache import REQUIRED_FILES, PLATFORM_FILES, validate_manifest


def fixtures():
    rows=[]
    for name in sorted(REQUIRED_FILES):
        row={'local_path':name,'s3_key':'source/'+name,'s3_version_id':'v1','size':10,'sha256':'a'*64}
        if name in PLATFORM_FILES:row['platform_reference_release_id']='platform-v7'
        rows.append(row)
    source=next(row for row in rows if row['local_path']=='contracts_rolled.parquet')
    manifest={'schema_version':2,'release_id':'source-main','platform_reference_release_id':'platform-v7','files':rows}
    proof={'state':'ALL_PARTS_EXACT_PARITY_PASSED','release_id':'source-main','parts':[
        {'index':0,'source':deepcopy(source),'all_values_equal':True,'duplicate_id_selection_order_preserved':True,
         'exit_code':0,'stop_guard':None,'rows_compared':100,
         'candidate':{'s3_key':'candidate/part.parquet','s3_version_id':'v2','etag':'e','size':8,'sha256':'b'*64}}]}
    return manifest,proof


def test_candidate_preserves_every_analytical_asset_and_does_not_mutate_input():
    manifest,proof=fixtures();before=deepcopy(manifest)
    candidate=with_contract_lookup(manifest,proof,'new-main')
    assert manifest==before
    assert candidate['files'][:-1]==manifest['files']
    assert candidate['contract_lookup_validation']['rows_compared']==100
    assert candidate['customer_facing'] is False
    validate_manifest(candidate)


@pytest.mark.parametrize('bad', ['state','release','source','guard','parity','missing'])
def test_incomplete_or_unrelated_evidence_cannot_make_lookup_candidate(bad):
    manifest,proof=fixtures()
    if bad=='state':proof['state']='BUILDING'
    if bad=='release':proof['release_id']='other'
    if bad=='source':proof['parts'][0]['source']['s3_version_id']='other'
    if bad=='guard':proof['parts'][0]['stop_guard']='RSS'
    if bad=='parity':proof['parts'][0]['all_values_equal']=False
    if bad=='missing':proof['parts']=[]
    with pytest.raises(ValueError):with_contract_lookup(manifest,proof,'new-main')
