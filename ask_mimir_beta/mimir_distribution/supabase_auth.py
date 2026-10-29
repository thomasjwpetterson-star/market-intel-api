"""Verify Supabase OAuth grants against the existing Mimir account and profile.

Only registered MCP client IDs are accepted. Ordinary website bearer sessions
cannot be substituted for an OAuth grant. No customer metadata defines a plan.
"""
import base64
import json
import time
from datetime import datetime, timezone
import httpx
from .access import AccessError, Identity


def profile_tier(profile, now=None):
    now = now or datetime.now(timezone.utc)
    if profile.get("is_admin") is True:
        return "enterprise"
    status = str(profile.get("subscription_status") or "").lower()
    if status == "trialing":
        end = profile.get("trial_ends_at")
        try:
            active = not end or datetime.fromisoformat(str(end).replace("Z", "+00:00")) > now
        except (ValueError, TypeError):
            active = False
        if active:
            return "trial"
    if status != "active":
        return "free"
    tier = str(profile.get("plan_tier") or "free").lower()
    return tier if tier in {"free", "lite", "professional", "enterprise"} else "free"


class SupabaseOAuthResolver:
    def __init__(self, client, project_url, publishable_key, allowed_client_ids, audience="authenticated"):
        if not project_url.startswith("https://") or not publishable_key or not allowed_client_ids:
            raise ValueError("Supabase project, publishable key and registered MCP client IDs required")
        self.client, self.url, self.key = client, project_url.rstrip("/"), publishable_key
        self.allowed_clients = frozenset(allowed_client_ids)
        self.audience = audience

    async def resolve(self, authorization):
        if not authorization or not authorization.startswith("Bearer ") or len(authorization) > 8192:
            raise AccessError("authentication_required", "Connect your Mimir account to research defense records.", 401)
        token = authorization[7:]
        headers = {"apikey": self.key, "Authorization": authorization}
        try:
            # getUser validates the exact token with the issuer, including signature
            # and live user status. Decode claims only after that verification.
            user_response = await self.client.get(self.url + "/auth/v1/user", headers=headers, timeout=5)
            if user_response.status_code in (401, 403):
                raise AccessError("invalid_token", "Mimir account access expired or was denied.", 401)
            user_response.raise_for_status()
            user = user_response.json()
            segment = token.split(".")[1]
            claims = json.loads(base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4)))
            if not isinstance(claims, dict) or not isinstance(user, dict):
                raise ValueError("Invalid issuer response")
            audience = claims.get("aud")
            valid_audience = audience == self.audience or isinstance(audience, list) and self.audience in audience
            if not valid_audience or claims.get("iss") != self.url + "/auth/v1" or claims.get("sub") != user.get("id") or claims.get("client_id") not in self.allowed_clients:
                raise AccessError("invalid_token", "This token is not a registered Mimir MCP authorization.", 401)
            if not isinstance(claims.get("exp"), (int,float)) or claims["exp"] <= time.time():
                raise AccessError("invalid_token", "Mimir account access has expired.", 401)
            if not isinstance(user.get("id"), str) or not user["id"]:
                raise ValueError("Missing verified user")
            grants_response = await self.client.get(self.url + "/auth/v1/user/oauth/grants", headers=headers, timeout=5)
            grants_response.raise_for_status()
            grants = grants_response.json()
            if not isinstance(grants, list) or not any(isinstance(grant, dict) and isinstance(grant.get("client"), dict) and grant["client"].get("id") == claims["client_id"] for grant in grants):
                raise AccessError("invalid_token", "This Mimir app connection has been revoked.", 401)
            profile_response = await self.client.get(self.url + "/rest/v1/profiles", headers=headers, params={"id": "eq." + user["id"], "select": "plan_tier,subscription_status,trial_ends_at,is_admin", "limit": "1"}, timeout=5)
            profile_response.raise_for_status()
            profiles = profile_response.json()
            if not isinstance(profiles,list) or len(profiles) != 1:
                raise AccessError("identity_unavailable", "Your current Mimir plan could not be verified.", 503)
            return Identity("user:" + user["id"], profile_tier(profiles[0]), True)
        except AccessError:
            raise
        except (httpx.HTTPError, ValueError, IndexError, TypeError, KeyError):
            raise AccessError("identity_unavailable", "Mimir account verification is temporarily unavailable.", 503) from None
