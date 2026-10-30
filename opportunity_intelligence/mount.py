"""Mount only authenticated reads; ingestion remains the existing AWS worker."""
import hmac
import logging
import os
import threading
import uuid
from fastapi import HTTPException, Request
from .api import Repository, create_router


def verified_application_principal(request: Request):
    # Same server-to-server trust boundary as Mimir's existing export proxy.
    # The Next.js route calls Supabase getUser() before supplying this identity.
    expected = os.getenv('MIMIR_EXPORT_PROXY_SECRET', '').strip()
    supplied = request.headers.get('X-Mimir-Export-Key', '')
    identity = request.headers.get('X-Mimir-Opportunity-User', '')
    if not expected:
        raise HTTPException(503, 'Opportunity authentication is unavailable')
    if not supplied or not hmac.compare_digest(expected, supplied):
        raise HTTPException(401, 'Sign in through Mimir to access opportunity intelligence')
    try:
        return {'id': str(uuid.UUID(identity)), 'verified_by': 'mimir_signed_in_proxy'}
    except (ValueError, TypeError):
        raise HTTPException(401, 'Verified application identity required')


class LazyRepository:
    """Do not make AWS availability a condition of the main API starting."""
    def __init__(self):
        self.repository = None
        self.lock = threading.Lock()

    def __getattr__(self, name):
        with self.lock:
            if self.repository is None:
                self.repository = Repository()
            return getattr(self.repository, name)


def mount(app):
    app.include_router(create_router(verified_application_principal, LazyRepository()))
