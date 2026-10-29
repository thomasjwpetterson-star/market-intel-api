"""Factory for mounting beside the existing Ask service, reusing its live ledger."""
import httpx
from .access import IntrospectionResolver, SharedAskLedger
from .app import create_app
from .service import PublicBackend, Service

def attach_to_existing_ask(*, state_store, beta_controls, auth_client, data_client,
                           introspection_endpoint, introspection_credential,
                           public_url, authorization_server, backend_origin):
    """All clients/stores are host-owned; host must close them at shutdown.

    OAuth issuer must implement the documented introspection contract, including
    verified website ledger identity and current billing tier. This module does
    not create a parallel account, ledger, policy table or opportunity schema.
    """
    resolver = IntrospectionResolver(auth_client, introspection_endpoint, introspection_credential, public_url.rstrip("/") + "/mcp")
    service = Service(PublicBackend(data_client, backend_origin), resolver, SharedAskLedger(state_store, beta_controls))
    return create_app(service, public_url.rstrip("/"), authorization_server)
