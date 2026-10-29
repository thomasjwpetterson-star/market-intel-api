import base64
from datetime import datetime, timezone
import json
import httpx
import pytest
from mimir_distribution.access import AccessError
from mimir_distribution.supabase_auth import SupabaseOAuthResolver, profile_tier

BASE="https://example.supabase.co"
def grant(**overrides):
    claims={"iss":BASE+"/auth/v1","aud":"authenticated","sub":"existing-user","client_id":"registered-client","exp":9999999999,**overrides}
    return "Bearer header."+base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")+".signature"

@pytest.mark.parametrize("profile,tier",[
 ({"subscription_status":"active","plan_tier":"professional"},"professional"),
 ({"subscription_status":"canceled","plan_tier":"professional"},"free"),
 ({"subscription_status":"trialing","trial_ends_at":"2000-01-01T00:00:00Z"},"free"),
 ({"subscription_status":"trialing","trial_ends_at":"2099-01-01T00:00:00Z"},"trial"),
 ({"subscription_status":"active","plan_tier":"unexpected"},"free"),
])
def test_live_profile_policy(profile,tier):
    assert profile_tier(profile)==tier

async def test_verified_oauth_grant_maps_to_existing_website_subject():
    paths=[]
    def handler(request):
        paths.append(request.url.path)
        if request.url.path.endswith('/user'):
            return httpx.Response(200,json={"id":"existing-user","user_metadata":{"plan_tier":"enterprise"}})
        if request.url.path.endswith('/grants'):
            return httpx.Response(200,json=[{'client':{'id':'registered-client'},'scopes':['openid','profile']}])
        assert request.url.params['id']=='eq.existing-user'
        return httpx.Response(200,json=[{"plan_tier":"free","subscription_status":"inactive"}])
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        resolver=SupabaseOAuthResolver(client,BASE,'public-key',['registered-client'])
        identity=await resolver.resolve(grant())
        assert identity.subject=='user:existing-user' and identity.tier=='free'
        assert paths==['/auth/v1/user','/auth/v1/user/oauth/grants','/rest/v1/profiles']

@pytest.mark.parametrize("overrides",[{"client_id":None},{"client_id":"unregistered"},{"aud":"wrong"},{"sub":"another-user"},{"exp":0},{"iss":"https://attacker.example/auth/v1"}])
async def test_browser_sessions_and_wrong_grants_rejected(overrides):
    def handler(request):
        assert request.url.path.endswith('/user')
        return httpx.Response(200,json={"id":"existing-user"})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        resolver=SupabaseOAuthResolver(client,BASE,'public-key',['registered-client'])
        with pytest.raises(AccessError) as exc:
            await resolver.resolve(grant(**overrides))
        assert exc.value.status==401

async def test_revoked_token_is_not_used_for_profile_query():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request:httpx.Response(401,json={"msg":"revoked"}))) as client:
        with pytest.raises(AccessError) as exc:
            await SupabaseOAuthResolver(client,BASE,'public-key',['registered-client']).resolve(grant())
        assert exc.value.status==401

async def test_live_revoked_grant_blocks_still_valid_access_token():
    def handler(request):
        if request.url.path.endswith('/user'):
            return httpx.Response(200,json={"id":"existing-user"})
        assert request.url.path.endswith('/grants')
        return httpx.Response(200,json=[])
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(AccessError) as exc:
            await SupabaseOAuthResolver(client,BASE,'public-key',['registered-client']).resolve(grant())
        assert exc.value.code=='invalid_token'

def test_composed_host_reuses_website_ledger_and_preserves_ask_routes(tmp_path):
    import importlib.util
    import sys
    from pathlib import Path
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from mimir_distribution.asgi_host import compose_with_ask
    source=Path(__file__).resolve().parents[1]/'beta_controls.py'
    spec=importlib.util.spec_from_file_location('compose_beta_controls',source)
    controls=importlib.util.module_from_spec(spec);sys.modules[spec.name]=controls;spec.loader.exec_module(controls)
    store=controls.BetaStateStore(tmp_path/'existing-ledger.sqlite3')
    access=controls.AccessContext('user:existing-user','free',True)
    store.reserve('website-query',access,'website','company')
    store.complete('website-query',latency_ms=None,estimated_cost_usd=None)
    state={'started':False}
    @asynccontextmanager
    async def life(app):
        state['started']=True
        yield
        state['started']=False
    parent=FastAPI(lifespan=life)
    @parent.get('/api/health')
    async def health():return {'existing_ask':state['started']}
    def auth_handler(request):
        if request.url.path.endswith('/user'):return httpx.Response(200,json={'id':'existing-user'})
        if request.url.path.endswith('/grants'):return httpx.Response(200,json=[{'client':{'id':'registered-client'}}])
        return httpx.Response(200,json=[{'plan_tier':'free','subscription_status':'inactive'}])
    auth=httpx.AsyncClient(transport=httpx.MockTransport(auth_handler))
    data=httpx.AsyncClient(transport=httpx.MockTransport(lambda request:httpx.Response(200,json={'name':'PUBLIC SUPPLIER','cage':'1EGB0'})))
    app=compose_with_ask(parent,SimpleNamespace(beta_state=store),controls,auth,data,project_url=BASE,publishable_key='public-key',allowed_client_ids=['registered-client'],public_url='https://mimir.example/distribution',backend_origin='https://source.example')
    with TestClient(app,base_url='http://localhost') as client:
        assert client.get('/api/health').json()=={'existing_ask':True}
        metadata=client.get('/distribution/.well-known/oauth-protected-resource').json()
        assert metadata['resource']=='https://mimir.example/distribution/mcp'
        assert metadata['scopes_supported']==['openid','profile']
        headers={'Authorization':grant()}
        result=client.post('/distribution/v1/intelligence/query',headers=headers,json={'kind':'company','identifier':'1EGB0'})
        assert result.status_code==200
        assert store.used_today(access.subject_id)==2
        limited=client.post('/distribution/v1/intelligence/query',headers=headers,json={'kind':'company','identifier':'1EGB0'})
        assert limited.status_code==429
    assert state['started'] is False
    store.connection.close()
