import hashlib,io,json
from pathlib import Path
import duckdb
import pytest
from opportunity_engine.company_references import resolve,project,refresh,ROOT


def fixture():
    part=dict(s3_key='governance/platform_reference/candidates/release/cache/main/output/nsn_cage_reference/part',s3_version_id='part-version',etag='etag',size=100)
    main=dict(release_id='mimir-main-platform-v7-fixture',platform_reference_release_id='platform-v7',files=[dict(local_path='nsn_cage_reference.parquet',parts=[part],platform_reference_release_id='platform-v7')])
    raw=json.dumps(main).encode();key='mimir/releases/mimir-main-platform-v7-fixture/manifest.json'
    root=dict(schema_version=2,promotion_ready=True,components={'main':dict(release_id=main['release_id'],immutable_manifest_key=key,immutable_manifest_version_id='manifest-version',immutable_manifest_sha256=hashlib.sha256(raw).hexdigest()),'platform_reference':dict(release_id='platform-v7')})
    class S3:
        def __init__(self):self.calls=[]
        def get_object(self,**kwargs):
            self.calls.append(kwargs);data=json.dumps(root).encode() if kwargs['Key']==ROOT else raw
            return dict(Body=io.BytesIO(data),VersionId='root-version')
    return S3(),root,main


def test_promoted_catalogue_uses_exact_manifest_version_and_rejects_mixed_or_unpinned_sources():
    s,root,m=fixture();result=resolve(s)
    assert s.calls[1]['VersionId']=='manifest-version'
    assert result['root_version_id']=='root-version' and result['main_release_id']==m['release_id']
    root['components']['platform_reference']['release_id']='different'
    with pytest.raises(ValueError,match='Mixed'):resolve(s)
    root['components']['platform_reference']['release_id']='platform-v7';root['components']['main']['immutable_manifest_sha256']='0'*64
    with pytest.raises(ValueError,match='hash differs'):resolve(s)


def test_reference_projection_retains_per_part_codes_prices_and_physical_provenance(tmp_path):
    c=duckdb.connect();p=tmp_path/'main.parquet'
    c.execute("COPY (SELECT * FROM (VALUES ('45934','012345678','2915012345678','Fuel boost pump','PN-1','EA',125.5,100.0,'2026-08-01','3','2','D'),('45934','012345679','2915012345679','Old pump','PN-2','EA',10.0,0.0,NULL,'5','9','B'),('BAD','012345680','2915012345680','Invalid identity','PN-3','EA',5.0,0.0,NULL,'3','2','K')) r(cage,niin,nsn,description,part_number,unit_of_issue,govt_estimated_price,avg_unit_price,last_sold_date,rncc_codes,rnvc_codes,rnsc_codes)) TO ? (FORMAT PARQUET)",[str(p)])
    sha=hashlib.sha256(p.read_bytes()).hexdigest();binding=dict(main_release_id='main',fingerprint='binding')
    result=project([dict(path=str(p),sha256=sha)],tmp_path/'projection',binding)
    assert result['rows']==2 and result['projection_counts']['excluded_invalid_identity_rows']==1
    rows=c.execute('SELECT part_number,rnvc_codes,rnsc_codes,supplier_average_price,source_snapshot_sha256,source_row FROM read_parquet(?) ORDER BY source_row',[result['path']]).fetchall()
    assert rows==[('PN-1','2','D',100.0,sha,1),('PN-2','9','B',None,sha,2)]
    assert c.execute('SELECT count(DISTINCT evidence_id) FROM read_parquet(?)',[result['path']]).fetchone()[0]==2
    assert 'not a transfer' in result['interpretation']


def test_unavailable_upstream_keeps_original_reference_dates_hashes_and_health(tmp_path):
    class S3:
        def get_object(self,**kwargs):raise PermissionError('private diagnostic')
    class AWS:s3=S3()
    prior=dict(as_of='original-native-dates',sha256='original-hash',files=[dict(path='original',sha256='original-hash')])
    result=refresh(prior,tmp_path,AWS())
    assert result['sha256']=='original-hash' and result['as_of']=='original-native-dates'
    assert result['refresh_health']['status']=='retained_company_catalogue_upstream_check_failed'
    assert 'private diagnostic' not in json.dumps(result)
    assert 'refresh_health' not in prior


def test_unchanged_promoted_object_pins_do_not_redownload_or_redate_reference(tmp_path):
    s,_,_=fixture();binding=resolve(s)
    class AWS:s3=s
    prior=dict(as_of='original-native-dates',files=[dict(path='already-loaded')],upstream_binding=binding)
    result=refresh(prior,tmp_path,AWS())
    assert result['as_of']=='original-native-dates'
    assert result['refresh_health']['status']=='promoted_catalogue_checked_unchanged'
