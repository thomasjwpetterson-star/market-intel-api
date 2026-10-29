import asyncio
import importlib.util
from pathlib import Path
import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from mimir_distribution.access import AccessError, Identity, SharedAskLedger, IntrospectionResolver
from mimir_distribution.app import create_app
from mimir_distribution.models import Query, Opportunity
from mimir_distribution.service import Service, PublicBackend
from mimir_distribution.crm import export_selected

class Resolver:
    async def resolve(self, authorization):
        if authorization != "Bearer verified":
            raise AccessError("authentication_required", "Connect Mimir", 401)
        return Identity("user:existing-user", "free", True)

class Ledger:
    def __init__(self, maximum=30):
        self.count, self.maximum, self.pending = 0, maximum, set()
    async def reserve(self, request_id, identity, operation):
        if self.count >= self.maximum:
            raise AccessError("usage_limit", "Existing limit reached", 429)
        self.count += 1
        self.pending.add(request_id)
    async def finish(self, request_id, success):
        self.pending.remove(request_id)
        if not success:
            self.count -= 1

class Backend:
    def __init__(self):
        self.calls = 0
    async def fetch(self, query):
        self.calls += 1
        return {"name": "Public Supplier", "cage": query.identifier, "top_platforms": [{"name": str(i)} for i in range(15)], "private_customer": "SECRET", "description": "Observed supplier"}

def service(maximum=30):
    return Service(Backend(), Resolver(), Ledger(maximum))

@pytest.mark.parametrize("kind,identifier", [("company", "../etc"), ("nsn", "123"), ("platform", "../reload"), ("award", "a?b"), ("company", "0000/5")])
def test_path_injection_rejected(kind, identifier):
    with pytest.raises(ValidationError):
        Query(kind=kind, identifier=identifier)

@pytest.mark.parametrize("limit", [0, 11, 100000])
def test_result_limit_rejected(limit):
    with pytest.raises(ValidationError):
        Query(kind="company", identifier="12345", limit=limit)

async def test_cache_still_charges_same_account_and_bounds():
    s = service(2)
    q = Query(kind="company", identifier="12345", limit=2)
    first = await s.query(q, "Bearer verified", "api")
    second = await s.query(q, "Bearer verified", "mcp")
    assert s.backend.calls == 1 and s.ledger.count == 2
    assert first.sources[0].retrieved_at == second.sources[0].retrieved_at
    assert len(first.data["top_platforms"]) == 2 and first.truncated
    assert "private_customer" not in first.data
    with pytest.raises(AccessError, match="usage_limit"):
        await s.query(q, "Bearer verified", "mcp")

async def test_failed_source_refunds():
    s = service()
    async def fail(q):
        raise AccessError("source_unavailable", "Unavailable", 503)
    s.backend.fetch = fail
    with pytest.raises(AccessError):
        await s.query(Query(kind="company", identifier="12345"), "Bearer verified")
    assert s.ledger.count == 0 and not s.ledger.pending

async def test_oversized_upstream_and_no_token_passthrough():
    async def handle(request):
        assert "authorization" not in request.headers
        return httpx.Response(200, content=b"x" * 262145)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(AccessError) as exc:
            await PublicBackend(client, "https://example.org").fetch(Query(kind="company", identifier="12345"))
        assert exc.value.code == "result_too_large"

async def test_introspection_requires_audience_scope_live_tier():
    body = {"exp": 9999999999, "active": True, "aud": "https://example.org/mcp", "scope": "mimir:read", "ledger_subject": "user:same-website-user", "tier": "professional"}
    async def handle(request):
        return httpx.Response(200, json=body)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        resolver = IntrospectionResolver(client, "https://auth.example.org/introspect", "test-service-secret", body["aud"])
        assert (await resolver.resolve("Bearer test")).subject == body["ledger_subject"]
        body["aud"] = "wrong"
        with pytest.raises(AccessError):
            await resolver.resolve("Bearer test")

def test_api_default_fails_closed_and_does_not_trust_tier_headers():
    with TestClient(create_app()) as client:
        response = client.post("/v1/intelligence/query", json={"kind": "company", "identifier": "12345"}, headers={"x-ask-mimir-tier": "enterprise", "x-ask-mimir-subject": "attacker"})
        assert response.status_code == 503
        assert client.get("/health").json()["research_configured"] is False
        assert client.get("/v1/companies/12345/opportunities").status_code == 501

def test_actual_sdk_protocol_and_cross_channel_quota():
    s = service(1)
    with TestClient(create_app(s), base_url="http://localhost") as client:
        headers = {"Accept": "application/json, text/event-stream", "Authorization": "Bearer verified"}
        initialized = client.post("/mcp", headers=headers, json={"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "integration-tests", "version": "1"}}})
        assert initialized.status_code == 200
        assert initialized.json()["result"]["protocolVersion"]
        tools = client.post("/mcp", headers=headers, json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}).json()["result"]["tools"]
        assert len(tools) == 5
        assert all(t["annotations"]["readOnlyHint"] for t in tools)
        response = client.post("/mcp", headers=headers, json={"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "get_defense_company_profile", "arguments": {"cage": "12345"}}})
        assert response.json()["result"]["structuredContent"]["data"]["name"] == "Public Supplier"
        exhausted = client.post("/v1/intelligence/query", headers=headers, json={"kind": "company", "identifier": "12345"})
        assert exhausted.status_code == 429
        assert "verified" not in str(s.audit.snapshot())

def test_sdk_auth_error_has_challenge():
    with TestClient(create_app(service()), base_url="http://localhost") as client:
        response = client.post("/mcp", headers={"Accept": "application/json, text/event-stream"}, json={"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "get_defense_company_profile", "arguments": {"cage": "12345"}}})
        result = response.json()["result"]
        assert result["isError"] and "mcp/www_authenticate" in result["_meta"]

async def test_shared_existing_ask_ledger_enforces_website_use(tmp_path):
    path = Path(__file__).resolve().parents[1] / "beta_controls.py"
    spec = importlib.util.spec_from_file_location("existing_beta_controls", path)
    import sys
    controls = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = controls
    spec.loader.exec_module(controls)
    store = controls.BetaStateStore(tmp_path / "existing-website-ledger.sqlite3")
    access = controls.AccessContext("user:existing-user", "free", True)
    store.reserve("website-1", access, "website", "company")
    store.complete("website-1", latency_ms=None, estimated_cost_usd=None)
    s = Service(Backend(), Resolver(), SharedAskLedger(store, controls))
    await s.query(Query(kind="company", identifier="12345"), "Bearer verified")
    with pytest.raises(AccessError) as exc:
        await s.query(Query(kind="company", identifier="12345"), "Bearer verified")
    assert exc.value.code == "usage_limit"
    store.connection.close()

def test_crm_csv_formula_and_duplicate_safety():
    row = Opportunity(external_id="source:stable-123", title=" =CMD()", description="Observed", updated_at="2026-10-29T00:00:00Z", evidence=["https://sam.gov/"])
    assert "' =CMD()" in export_selected([row])
    with pytest.raises(ValueError):
        export_selected([row, row])

async def test_concurrent_load_never_exceeds_four_origin_calls():
    s = service(100)
    active, maximum = 0, 0
    async def slow_fetch(q):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        try:
            await asyncio.sleep(0.03)
            return {"name": "Supplier", "cage": q.identifier}
        finally:
            active -= 1
    s.backend.fetch = slow_fetch
    results = await asyncio.gather(*(s.query(Query(kind="company", identifier=f"{i:05d}"), "Bearer verified") for i in range(10)), return_exceptions=True)
    assert maximum <= 4
    assert sum(not isinstance(r, Exception) for r in results) == 10
    assert not s.ledger.pending

async def test_burst_limit_applies_to_cache_hits():
    s = service(100)
    q = Query(kind="company", identifier="12345")
    for _ in range(10):
        await s.query(q, "Bearer verified")
    with pytest.raises(AccessError) as exc:
        await s.query(q, "Bearer verified")
    assert exc.value.code == "rate_limit"
    assert s.backend.calls == 1

async def test_source_failure_and_injected_secret_never_reach_audit():
    s = service()
    async def fail(q):
        raise RuntimeError("SECRET credential and raw customer data")
    s.backend.fetch = fail
    with pytest.raises(AccessError) as exc:
        await s.query(Query(kind="company", identifier="12345"), "Bearer verified")
    assert "SECRET" not in str(exc.value)
    assert "SECRET" not in str(s.audit.snapshot())

def test_mcp_rejects_invalid_arguments_and_untrusted_host():
    with TestClient(create_app(service()), base_url="http://localhost") as client:
        headers={"Accept": "application/json, text/event-stream", "Authorization": "Bearer verified"}
        response=client.post("/mcp",headers=headers,json={"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_defense_company_profile","arguments":{"cage":"../dump"}}})
        assert response.json()["result"]["isError"]
        response=client.post("/mcp",headers={**headers,"Host":"attacker.example"},json={"jsonrpc":"2.0","id":2,"method":"tools/list"})
        assert response.status_code == 421

def test_large_request_body_is_rejected_before_research():
    with TestClient(create_app(service())) as client:
        response = client.post("/v1/intelligence/query", content=b"x" * 16385, headers={"Content-Type":"application/json"})
        assert response.status_code == 413

def test_mounted_public_url_uses_origin_without_resource_path():
    with TestClient(create_app(service(),public_url="https://mimir.example/distribution"),base_url="https://mimir.example") as client:
        payload={"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}
        headers={"Accept":"application/json, text/event-stream","Origin":"https://mimir.example"}
        assert client.post('/mcp',headers=headers,json=payload).status_code==200
        assert client.post('/mcp',headers={**headers,"Origin":"https://attacker.example"},json=payload).status_code==403
