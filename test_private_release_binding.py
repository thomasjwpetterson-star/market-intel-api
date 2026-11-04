"""A data cutover must select and verify a private authority before readiness."""
import ast
import copy
import hashlib
import io
import json
import os
import hmac
from pathlib import Path
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
    cache={'last_loaded':1,'is_loading':False,'profiles_df':pd.DataFrame({'x':[1]}),'private_reference_ready':False}
    ns=dict(Dict=Dict,Any=Any,GLOBAL_CACHE=cache,pd=pd,DUCK_CONN=object())
    state=extracted('get_readiness_state',ns)()
    assert state['ready'] is False and state['private_reference_ok'] is False
    cache['private_reference_ready']=True
    assert ns['get_readiness_state']()['ready'] is True
