from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Annotated
from urllib.parse import urlsplit
import os
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import CallToolResult, TextContent, ToolAnnotations
from pydantic import Field
from .access import AccessError, DenyAccess
from .models import Query, Result
from .service import PublicBackend, Service

authorization_context = ContextVar("mimir_authorization", default=None)
ANNOTATIONS = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True)


def create_app(service=None, public_url="https://mimir-distribution.invalid", authorization_server=None, scopes=None, extra_origins=None, security_schemes=None):
    scopes = scopes or ["mimir:read"]
    scope_text = " ".join(scopes)
    security_schemes = security_schemes or [{"type": "oauth2", "scopes": scopes}]
    parsed_url = urlsplit(public_url)
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc or parsed_url.query or parsed_url.fragment:
        raise ValueError("Public URL must be an absolute HTTP(S) resource base")
    public_origin = f"{parsed_url.scheme}://{parsed_url.netloc}"
    owned_client = httpx.AsyncClient(limits=httpx.Limits(max_connections=4, max_keepalive_connections=4)) if service is None else None
    service = service or Service(PublicBackend(owned_client, "https://market-intel-api-jmil.onrender.com"), DenyAccess())
    mcp = FastMCP("Mimir — US Defense Market & Supply Chain Intelligence",
        instructions="Research observed US defense records with evidence. Public snapshots contain limited results. Never infer revenue, approved-source status or future awards beyond returned evidence. Company-specific opportunity forecasts and bulk exports are not supported.",
        stateless_http=True, json_response=True, max_request_body_size=16384,
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=True,
            allowed_hosts=["localhost", "127.0.0.1", "localhost:*", "127.0.0.1:*", parsed_url.netloc],
            allowed_origins=[public_origin, *(extra_origins or [])]))

    async def call(kind, identifier, limit):
        try:
            result = await service.query(Query(kind=kind, identifier=identifier, limit=limit), authorization_context.get(), "mcp")
            return CallToolResult(content=[TextContent(type="text", text=result.model_dump_json())], structuredContent=result.model_dump(mode="json"))
        except AccessError as exc:
            meta = {}
            if exc.status == 401:
                meta = {"mcp/www_authenticate": [f'Bearer resource_metadata="{public_url}/.well-known/oauth-protected-resource", scope="{scope_text}"']}
            return CallToolResult(isError=True, content=[TextContent(type="text", text=exc.message)], structuredContent={"error": {"code": exc.code, "message": exc.message}}, _meta=meta)

    @mcp.tool(annotations=ANNOTATIONS, description="Find public US defense company, NSN, platform or award records by name or identifier. Use before a profile lookup when CAGE or platform slug is unknown. Returns bounded candidate matches, not a complete dataset.", meta={"securitySchemes": security_schemes})
    async def search_defense_records(query: Annotated[str, Field(min_length=2, max_length=128)], limit: Annotated[int, Field(ge=1, le=10)] = 5) -> CallToolResult:
        return await call("search", query, limit)

    @mcp.tool(annotations=ANNOTATIONS, description="Get a US defense supplier facility by its five-character CAGE, including observed obligations, capabilities and selected relationships. A facility is not its corporate parent. Search by company name first when CAGE is unknown.", meta={"securitySchemes": security_schemes})
    async def get_defense_company_profile(cage: Annotated[str, Field(pattern=r"^[A-Za-z0-9]{5}$")], limit: Annotated[int, Field(ge=1, le=10)] = 5) -> CallToolResult:
        return await call("company", cage, limit)

    @mcp.tool(annotations=ANNOTATIONS, description="Get a public NSN or nine-digit NIIN snapshot with selected manufacturer/source references, platform associations and recent procurement evidence. Manufacturer references do not establish approval. Historical unit prices may be unavailable; never substitute contract value for unit price.", meta={"securitySchemes": security_schemes})
    async def get_nsn_manufacturers_and_prices(nsn: Annotated[str, Field(pattern=r"^(?:[0-9]{9}|[0-9]{13}|[0-9]{4}-[0-9]{2}-[0-9]{3}-[0-9]{4})$")], limit: Annotated[int, Field(ge=1, le=10)] = 5) -> CallToolResult:
        return await call("nsn", nsn, limit)

    @mcp.tool(annotations=ANNOTATIONS, description="Get selected observed contractors, parts, award evidence and historical obligations for a US defense platform using a platform slug from search. Mappings are evidence of associations, not a complete bill of materials or future opportunity forecast.", meta={"securitySchemes": security_schemes})
    async def get_program_supply_chain(platform_slug: Annotated[str, Field(pattern=r"^[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*$", max_length=128)], limit: Annotated[int, Field(ge=1, le=10)] = 5) -> CallToolResult:
        return await call("platform", platform_slug, limit)

    @mcp.tool(annotations=ANNOTATIONS, description="Look up a known US defense contract award identifier, observed net obligations, reported values, contractor and dated action evidence. Observed obligation totals are distinct from contract ceilings and company revenue.", meta={"securitySchemes": security_schemes})
    async def get_defense_contract_award(contract_id: Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]+$", min_length=2, max_length=128)], limit: Annotated[int, Field(ge=1, le=10)] = 5) -> CallToolResult:
        return await call("award", contract_id, limit)

    mcp_app = mcp.streamable_http_app()

    @asynccontextmanager
    async def lifespan(app):
        async with mcp.session_manager.run():
            yield
        if owned_client:
            await owned_client.aclose()

    app = FastAPI(title="Mimir External Intelligence API", version="1.0", lifespan=lifespan)
    app.state.service, app.state.mcp = service, mcp

    @app.middleware("http")
    async def context(request, call_next):
        if request.method == "POST":
            body = bytearray()
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > 16384:
                    return JSONResponse({"error": {"code": "request_too_large", "message": "Request exceeds 16 KiB."}}, status_code=413)
            request._body = bytes(body)
        # Never trust user-provided subject, tier, proxy or organization headers.
        token = authorization_context.set(request.headers.get("authorization"))
        try:
            response = await call_next(request)
            response.headers["Cache-Control"] = "no-store"
            response.headers["X-Content-Type-Options"] = "nosniff"
            return response
        finally:
            authorization_context.reset(token)

    @app.exception_handler(AccessError)
    async def error_handler(request, exc):
        headers = {"Retry-After": "60"} if exc.status == 429 else {}
        if exc.status == 401:
            headers["WWW-Authenticate"] = f'Bearer resource_metadata="{public_url}/.well-known/oauth-protected-resource", scope="{scope_text}"'
        return JSONResponse({"error": {"code": exc.code, "message": exc.message}}, status_code=exc.status, headers=headers)

    @app.get("/health")
    async def health():
        return {"status": "ok", "research_configured": service.ledger is not None and not isinstance(service.resolver, DenyAccess)}

    @app.get("/.well-known/oauth-protected-resource")
    async def protected_resource():
        if not authorization_server:
            raise AccessError("oauth_not_configured", "Mimir OAuth is not configured.", 503)
        return {"resource": public_url + "/mcp", "authorization_servers": [authorization_server], "scopes_supported": scopes, "bearer_methods_supported": ["header"]}

    @app.post("/v1/intelligence/query", response_model=Result)
    async def research(query: Query, request: Request):
        return await service.query(query, request.headers.get("authorization"), "api")

    @app.get("/v1/companies/{cage}/opportunities")
    async def opportunities(cage: str):
        raise AccessError("capability_unavailable", "Company opportunity intelligence is not connected to this API yet.", 501)

    app.mount("/", mcp_app)
    return app

app = create_app(public_url=os.environ.get("MIMIR_DISTRIBUTION_URL", "https://mimir-distribution.invalid"))
