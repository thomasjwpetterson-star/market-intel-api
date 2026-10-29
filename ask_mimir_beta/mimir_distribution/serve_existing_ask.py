"""Production composition entry point; requires the existing Ask code/runtime.

Install this package into the existing Ask image and run this module instead of
serve.py. It bootstraps through the existing code and reuses lab_api.runtime.
No opportunity modules or existing database schemas are changed.
"""
from contextlib import asynccontextmanager
import importlib
import os
from pathlib import Path
import sys
import httpx
from .asgi_host import compose_with_ask


def required(name):
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Required distribution configuration missing: {name}")
    return value


def build_app():
    code_root = Path(required("ASK_MIMIR_CODE_ROOT")).resolve()
    if not (code_root / "lab_api.py").is_file():
        raise RuntimeError("ASK_MIMIR_CODE_ROOT must point at the existing Ask application")
    # Verify configuration before bootstrapping the existing application.
    public_url = required("MIMIR_DISTRIBUTION_URL").rstrip("/")
    project_url = required("SUPABASE_URL").rstrip("/")
    key = required("SUPABASE_PUBLISHABLE_KEY")
    backend_origin = required("MIMIR_PUBLIC_API_ORIGIN")
    clients = [value.strip() for value in required("MIMIR_MCP_CLIENT_IDS").split(",") if value.strip()]
    if not public_url.startswith("https://") or not project_url.startswith("https://") or not backend_origin.startswith("https://") or not clients:
        raise RuntimeError("HTTPS URLs and registered private MCP clients are required")
    sys.path.insert(0, str(code_root))
    os.environ["ASK_MIMIR_ALLOW_TEST_IDENTITIES"] = "0"
    importlib.import_module("bootstrap_data").bootstrap()
    lab = importlib.import_module("lab_api")
    controls = importlib.import_module("beta_controls")
    auth = httpx.AsyncClient(limits=httpx.Limits(max_connections=4))
    data = httpx.AsyncClient(limits=httpx.Limits(max_connections=4))
    app = compose_with_ask(lab.app, lab.runtime, controls, auth, data,
        project_url=project_url, publishable_key=key, allowed_client_ids=clients,
        public_url=public_url, backend_origin=backend_origin)
    parent_lifespan = app.router.lifespan_context
    @asynccontextmanager
    async def lifespan(app):
        try:
            async with parent_lifespan(app):
                yield
        finally:
            await auth.aclose()
            await data.aclose()
    app.router.lifespan_context = lifespan
    return app

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("mimir_distribution.serve_existing_ask:build_app", factory=True,
        host="0.0.0.0", port=int(os.environ.get("PORT", "10000")), workers=1)
