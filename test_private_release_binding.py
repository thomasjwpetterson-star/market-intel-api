"""A data cutover must select and verify a private authority before readiness."""
import ast
import asyncio
import copy
import hashlib
import io
import json
import os
import hmac
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
import private_platform_reference as authority


@pytest.fixture
def releases(tmp_path,monkeypatch):
    source={'s3_key':authority.PREFIX+'base/reference.json','s3_version_id':'base-v1','sha256':'b'*64}
    main={'release_id':'main-new','platform_reference_release_id':'v7','files':[
        {'local_path':'transactions.parquet','s3_key':'transactions','s3_version_id':'actions-v1'}]}
    manifest={'schema_version':'platform-award-remap-cloud-candidate-v1','release_id':'authority-new',
        'accepted_main_release_id':'main-new','base_platform_reference_release_id':'v7',
        'customer_publication_eligible':False,'parent_platform_reference_manifest':source}
    raw=json.dumps(manifest).encode()
    binding={'accepted_main_release_id':'main-new','base_platform_reference_release_id':'v7',
        'manifest':{'bucket':authority.BUCKET,'key':authority.PREFIX+'new/manifest.json',
            's3_version_id':'manifest-v1','sha256':hashlib.sha256(raw).hexdigest()}}
    path=tmp_path/'private_v7_binding.json';path.write_text(json.dumps({'schema_version':2,'bindings':[binding]}))
    monkeypatch.setattr(authority,'BINDING_PATH',path,raising=False)
    client=Mock();client.get_object.return_value={'Body':io.BytesIO(raw),'VersionId':'manifest-v1'}
    bundle={'platform':{'schema_version':2},'components':{'main':main,
        'platform_reference':{'release_id':'v7','rebuilt_main_release_id':'main-new','source_reference':source}}}
    return path,binding,manifest,main,bundle,client


def test_exact_catalog_selection_preserves_old_and_new_rollback_bindings(releases):
    path,binding,*_=releases
    old={**binding,'accepted_main_release_id':'main-old'}
    path.write_text(json.dumps({'schema_version':2,'bindings':[old,binding]}))
    assert authority.binding_for('main-new')==binding
    assert authority.binding_for('main-old')==old
    with pytest.raises(RuntimeError):authority.binding_for('main-unreviewed')


def test_legacy_single_binding_remains_supported(releases):
    path,binding,*_=releases;path.write_text(json.dumps(binding))
    assert authority.binding_for('main-new')==binding
    with pytest.raises(RuntimeError):authority.binding_for(None)


@pytest.mark.parametrize('config',[{'schema_version':2,'bindings':[]},
    {'schema_version':2,'bindings':'wrong'},{'schema_version':3,'bindings':[]}])
def test_invalid_catalog_fails_closed(releases,config):
    path,*_=releases;path.write_text(json.dumps(config))
    with pytest.raises(RuntimeError):authority.binding_for('main-new')


def test_duplicate_main_binding_is_rejected(releases):
    path,binding,*_=releases;path.write_text(json.dumps({'schema_version':2,'bindings':[binding,binding]}))
    with pytest.raises(RuntimeError):authority.binding_for('main-new')


def test_source_fingerprint_changes_for_actions_but_not_lookup_rebuild(releases):
    _,_,_,main,*_=releases
    original=authority.main_source_fingerprint(main)
    lookup=copy.deepcopy(main);lookup['files'].append({'local_path':'contracts_lookup.parquet','sha256':'different'})
    assert authority.main_source_fingerprint(lookup)==original
    changed=copy.deepcopy(main);changed['files'][0]['s3_version_id']='actions-v2'
    assert authority.main_source_fingerprint(changed)!=original


def test_preflight_checks_lineage_without_loading_parquet(releases):
    path,binding,manifest,main,bundle,client=releases
    binding['main_source_fingerprint']=authority.main_source_fingerprint(main)
    manifest['main_source_fingerprint']=binding['main_source_fingerprint']
    raw=json.dumps(manifest).encode();binding['manifest']['sha256']=hashlib.sha256(raw).hexdigest()
    client.get_object.return_value={'Body':io.BytesIO(raw),'VersionId':'manifest-v1'}
    path.write_text(json.dumps({'schema_version':2,'bindings':[binding]}))
    result=authority.validate_release_binding(bundle,client)
    assert result['main_release_id']=='main-new'
    assert result['mapping_release_id']=='authority-new'
    assert client.get_object.call_count==1


def test_root_pinned_private_authority_selects_new_main_and_restores_old_catalog(releases):
    path,binding,manifest,main,bundle,client=releases
    old={**binding,'accepted_main_release_id':'main-old'}
    path.write_text(json.dumps({'schema_version':2,'bindings':[old]}))
    manifest['main_source_fingerprint']=authority.main_source_fingerprint(main)
    manifest['compatibility_validation']={'state':'PASSED'}
    raw=json.dumps(manifest).encode()
    pin={**binding['manifest'],'sha256':hashlib.sha256(raw).hexdigest()}
    bundle['platform']['private_v7_authority_manifest']=pin
    client.get_object.return_value={'Body':io.BytesIO(raw),'VersionId':'manifest-v1'}
    try:
        assert authority.validate_release_binding(bundle,client)['mapping_release_id']=='authority-new'
        assert authority.binding_for('main-new')['manifest']==pin
        del bundle['platform']['private_v7_authority_manifest']
        bundle['components']['main']['release_id']='main-old'
        bundle['components']['platform_reference']['rebuilt_main_release_id']='main-old'
        old_manifest={**manifest,'accepted_main_release_id':'main-old'}
        old_manifest['main_source_fingerprint']=authority.main_source_fingerprint(main)
        old_raw=json.dumps(old_manifest).encode()
        old['manifest']={**pin,'sha256':hashlib.sha256(old_raw).hexdigest()}
        old['main_source_fingerprint']=old_manifest['main_source_fingerprint']
        path.write_text(json.dumps({'schema_version':2,'bindings':[old]}))
        client.get_object.return_value={'Body':io.BytesIO(old_raw),'VersionId':'manifest-v1'}
        assert authority.validate_release_binding(bundle,client)['main_release_id']=='main-old'
        assert authority.binding_for('main-old')==old
    finally:
        authority._selected_binding=None
        authority._cached=None


@pytest.mark.parametrize('change',['wrong_main','bad_hash','unreviewed'])
def test_root_pinned_private_authority_fails_closed(releases,change):
    path,binding,manifest,main,bundle,client=releases
    path.write_text(json.dumps({'schema_version':2,'bindings':[]}))
    manifest['main_source_fingerprint']=authority.main_source_fingerprint(main)
    manifest['compatibility_validation']={'state':'PASSED'}
    if change=='wrong_main':manifest['accepted_main_release_id']='other-main'
    if change=='unreviewed':manifest['compatibility_validation']['state']='PENDING'
    raw=json.dumps(manifest).encode()
    pin={**binding['manifest'],'sha256':hashlib.sha256(raw).hexdigest()}
    if change=='bad_hash':pin['sha256']='0'*64
    bundle['platform']['private_v7_authority_manifest']=pin
    client.get_object.return_value={'Body':io.BytesIO(raw),'VersionId':'manifest-v1'}
    with pytest.raises(authority.PrivateReferenceUnavailable):
        authority.validate_release_binding(bundle,client)


def test_current_item_alias_annex_requires_exact_source_and_zero_financial_allocation(releases):
    _,binding,manifest,_,_,client=releases
    rows=[{'review_link_id':f'link-{i if i<6 else 5}',
           'prior_transaction_key':f'old-{i}',
           'current_transaction_key':f'new-{i}',
           'financial_allocation_eligible':False,
           'pricing_evidence_eligible':False} for i in range(7)]
    annex={'status':'VERIFIED_SEVEN_ACTION_ALIASES_AND_SIX_REVIEWED_ITEM_RELATIONSHIPS',
           'main_release_id':'main-new','etl_run_id':'run-new',
           'summary':{'source_amount_change_usd':'0.00'},'aliases':rows}
    raw=json.dumps(annex).encode()
    manifest['current_action_alias_annex']={**binding['manifest'],
        'sha256':hashlib.sha256(raw).hexdigest()}
    client.get_object.return_value={'Body':io.BytesIO(raw),'VersionId':'manifest-v1'}
    selected=authority._current_item_aliases(client,manifest,'main-new','run-new')
    assert len(selected)==6 and sum(map(len,selected.values()))==7
    with pytest.raises(ValueError):authority._current_item_aliases(
        Mock(get_object=Mock(return_value={'Body':io.BytesIO(raw),'VersionId':'manifest-v1'})),
        manifest,'wrong-main','run-new')
    rows[0]['financial_allocation_eligible']=True
    bad=json.dumps(annex).encode()
    manifest['current_action_alias_annex']['sha256']=hashlib.sha256(bad).hexdigest()
    with pytest.raises(ValueError):authority._current_item_aliases(
        Mock(get_object=Mock(return_value={'Body':io.BytesIO(bad),'VersionId':'manifest-v1'})),
        manifest,'main-new','run-new')


@pytest.mark.parametrize('change',['actions','base_reference','source_reference','authority_hash','unselected'])
def test_preflight_rejects_incompatible_pairs(releases,change):
    path,binding,manifest,main,bundle,client=releases
    binding['main_source_fingerprint']=authority.main_source_fingerprint(main)
    if change=='actions':main['files'][0]['s3_version_id']='unreviewed'
    if change=='base_reference':main['platform_reference_release_id']='other-v7'
    if change=='source_reference':bundle['components']['platform_reference']['source_reference']['sha256']='c'*64
    if change=='authority_hash':binding['manifest']['sha256']='0'*64
    if change=='unselected':main['release_id']='unselected'
    path.write_text(json.dumps({'schema_version':2,'bindings':[binding]}))
    with pytest.raises(RuntimeError):authority.validate_release_binding(bundle,client)


def extracted(name,ns):
    tree=ast.parse(Path('main.py').read_text())
    node=next(n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))and n.name==name)
    node.decorator_list=[]
    exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),ns)
    return ns[name]


def test_private_release_incompatibility_is_503_not_an_unhandled_500(releases):
    ns=dict(Dict=Dict,Any=Any,Request=Request,Response=Response,HTTPException=HTTPException,
        JSONResponse=JSONResponse,os=os,hmac=hmac,logger=Mock(),
        GLOBAL_CACHE={'last_loaded':1,'is_loading':False,'platform_release':{'components':{'main':{'release_id':'unselected'}}}},
        programme_read_df=Mock(),df_sanitize_for_json=lambda df:df)
    app=FastAPI()
    handler=extracted('private_reference_unavailable',ns)
    app.add_exception_handler(authority.PrivateReferenceUnavailable,handler)
    app.post('/api/research/aggregate')(extracted('private_research_aggregate',ns))
    app.post('/api/research/reference-context')(extracted('private_reference_context',ns))
    client=TestClient(app)
    with patch.dict(os.environ,{'MIMIR_EXPORT_PROXY_SECRET':'fixture'}):
        for route,body in [('aggregate',{'dataset':'contract_actions','group_by':['year'],'filters':{'program':'AMRAAM'}}),
            ('reference-context',{'kind':'program_search','identifier':'AMRAAM'})]:
            assert client.post('/api/research/'+route,json=body).status_code==401
            response=client.post('/api/research/'+route,json=body,headers={'X-Mimir-Export-Key':'fixture'})
            assert response.status_code==503 and response.headers['Cache-Control']=='private, no-store'
    ns['programme_read_df'].assert_not_called()


def test_readiness_does_not_hide_failed_private_binding():
    cache={'last_loaded':1,'is_loading':False,'profiles_df':pd.DataFrame({'x':[1]}),'private_reference_ready':False,'dashboard_projection_ready':True}
    ns=dict(Dict=Dict,Any=Any,GLOBAL_CACHE=cache,pd=pd,DUCK_CONN=object())
    state=extracted('get_readiness_state',ns)()
    assert state['ready'] is False and state['private_reference_ok'] is False
    cache['private_reference_ready']=True
    assert ns['get_readiness_state']()['ready'] is True
    cache['dashboard_projection_ready']=False
    assert ns['get_readiness_state']()['ready'] is False
    assert ns['get_readiness_state']()['dashboard_projection_ok'] is False


@pytest.mark.parametrize('reject_binding',[False,True])
def test_actual_api_lifespan_checks_selected_private_authority_before_staging(
    tmp_path,monkeypatch,reject_binding
):
    events=[]
    bundle={'platform':{'schema_version':2,'release_id':'candidate'},
            'components':{'main':{'release_id':'main-new',
                                  'platform_reference_release_id':'v7'}}}
    async def immediate(function,*args,**kwargs):
        return function(*args,**kwargs)
    def select(*args,**kwargs):
        events.append('select')
        return bundle
    def bind(*args,**kwargs):
        events.append('bind')
        if reject_binding:
            raise authority.PrivateReferenceUnavailable('source differs')
        return {'main_release_id':'main-new','mapping_release_id':'authority-new'}
    def stage(*args,**kwargs):
        events.append('stage')
        return tmp_path/'main-releases'/'main-new'
    def cancel_background(coroutine):
        coroutine.close()
        events.append('background-scheduled')
    namespace=dict(FastAPI=FastAPI,Path=Path,os=os,logger=Mock(),platform_manifest_enabled=lambda:True,
        boto3=SimpleNamespace(client=lambda *a,**k:Mock()),BotoConfig=lambda **k:None,
        asyncio=SimpleNamespace(to_thread=immediate,create_task=cancel_background),
        resolve_platform_release=select,DEFAULT_PLATFORM_MANIFEST_KEY='current.json',
        LOCAL_CACHE_DIR=tmp_path/'cache',DUCKDB_PATH=tmp_path/'cache'/'mimir.duckdb',
        RELEASE_PINNED_CACHE=False,PINNED_PLATFORM_BUNDLE=None,stage_main_cache=stage,
        anyio=SimpleNamespace(to_thread=SimpleNamespace(
            current_default_thread_limiter=lambda:SimpleNamespace(total_tokens=0))),
        ensure_duck_conn=lambda:events.append('duckdb'),gc=Mock(),
        _close_all_duck_read_conns=Mock(),DUCK_CONN=None)
    lifespan=extracted('lifespan',namespace)
    monkeypatch.setenv('MIMIR_MAIN_FOLLOW_ROOT','1')
    monkeypatch.setenv('MIMIR_USE_PLATFORM_MANIFEST','1')
    with patch.object(authority,'validate_release_binding',side_effect=bind):
        async def run():
            generator=lifespan(FastAPI())
            if reject_binding:
                with pytest.raises(authority.PrivateReferenceUnavailable):
                    await generator.__anext__()
            else:
                await generator.__anext__()
                assert namespace['PINNED_PLATFORM_BUNDLE'] is bundle
                assert namespace['RELEASE_PINNED_CACHE'] is True
                await generator.aclose()
        asyncio.run(run())
    if reject_binding:
        assert events==['select','bind']
    else:
        assert events[:3]==['select','bind','stage']
