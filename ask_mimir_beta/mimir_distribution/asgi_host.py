"""ASGI composition that mounts MCP beside the actual running Ask application.

The caller supplies the current runtime; it is never imported/rebuilt here.
Both applications use runtime.beta_state, preserving existing credit accounting.
"""
from contextlib import asynccontextmanager
from starlette.applications import Starlette
from starlette.routing import Mount
from .access import SharedAskLedger
from .app import create_app
from .service import PublicBackend, Service
from .supabase_auth import SupabaseOAuthResolver


def compose_with_ask(ask_app, runtime, controls, auth_client, data_client, *, project_url,
                     publishable_key, allowed_client_ids, public_url, backend_origin):
    resolver = SupabaseOAuthResolver(auth_client, project_url, publishable_key, allowed_client_ids)
    service = Service(PublicBackend(data_client, backend_origin), resolver, SharedAskLedger(runtime.beta_state, controls))
    child = create_app(service, public_url=public_url, authorization_server=project_url.rstrip('/')+'/auth/v1', scopes=["openid", "profile"])
    @asynccontextmanager
    async def lifespan(app):
        async with ask_app.router.lifespan_context(ask_app):
            async with child.router.lifespan_context(child):
                yield
    app = Starlette(routes=[Mount('/distribution', child), Mount('/', ask_app)], lifespan=lifespan)
    app.state.distribution = service
    return app
