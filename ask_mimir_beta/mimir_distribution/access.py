"""Host-injected identity and existing Ask ledger, never client-provided plan headers."""
from dataclasses import dataclass
from typing import Protocol
import asyncio
import time
import httpx

@dataclass(frozen=True)
class Identity:
    subject: str
    tier: str
    authenticated: bool
    organisation: str | None = None

class AccessError(Exception):
    def __init__(self, code, message, status=403):
        self.code, self.message, self.status = code, message, status

class Resolver(Protocol):
    async def resolve(self, authorization: str | None) -> Identity: ...

class DenyAccess:
    async def resolve(self, authorization):
        raise AccessError("accounting_not_configured", "Shared Mimir identity and usage accounting are not connected.", 503)

class IntrospectionResolver:
    """Trusted auth service verifies OAuth audience, expiry, scope and live entitlement.

    This is NOT token passthrough to the dataset API. The introspection service must
    return the exact website ledger subject, including its existing 'user:' prefix.
    """
    def __init__(self, client: httpx.AsyncClient, endpoint: str, credential: str, resource: str):
        if not endpoint.startswith("https://") or not resource.startswith("https://") or not credential:
            raise ValueError("HTTPS introspection endpoint, resource and service credential required")
        self.client, self.endpoint, self.credential, self.resource = client, endpoint, credential, resource

    async def resolve(self, authorization):
        if not authorization or not authorization.startswith("Bearer ") or len(authorization) > 8192:
            raise AccessError("authentication_required", "Connect your Mimir account to use this tool.", 401)
        try:
            response = await self.client.post(self.endpoint, data={"token": authorization[7:], "resource": self.resource}, headers={"Authorization": f"Bearer {self.credential}"}, timeout=5)
            response.raise_for_status()
            body = response.json()
        except (httpx.HTTPError, ValueError):
            raise AccessError("identity_unavailable", "Account verification is temporarily unavailable.", 503) from None
        if body.get("active") is not True or body.get("aud") != self.resource or not isinstance(body.get("exp"), (int, float)) or body["exp"] <= time.time() or "mimir:read" not in str(body.get("scope", "")).split():
            raise AccessError("invalid_token", "Mimir access token is invalid or lacks research permission.", 401)
        subject, tier = body.get("ledger_subject"), body.get("tier")
        if not isinstance(subject, str) or not subject.startswith("user:") or len(subject) > 170 or tier not in {"free", "trial", "lite", "professional", "enterprise"}:
            raise AccessError("invalid_identity", "Mimir account entitlements could not be verified.", 401)
        return Identity(subject, tier, True, body.get("organisation_id"))

class SharedAskLedger:
    """Pass the EXISTING running BetaStateStore and beta_controls module.

    Never create another ledger file for this integration. All calls, including
    cache hits, reserve one existing Ask credit. Existing policies remain in force.
    """
    def __init__(self, store, controls):
        self.store, self.controls = store, controls

    async def reserve(self, request_id, identity, operation):
        access = self.controls.AccessContext(identity.subject, identity.tier, identity.authenticated)
        try:
            await asyncio.to_thread(self.store.reserve, request_id, access, "distribution-v1", operation)
        except self.controls.DailyQuotaExceeded:
            raise AccessError("usage_limit", "Your existing Mimir research allowance has been reached.", 429) from None

    async def finish(self, request_id, success):
        await asyncio.to_thread(self.store.complete, request_id, latency_ms=None, estimated_cost_usd=None, billable=success)
