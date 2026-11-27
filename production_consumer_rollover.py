"""Refresh and verify the two in-memory consumers after a platform pointer change."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import boto3


MAIN_SERVICE_ID = "srv-d59jle1r0fns73frev1g"
ASK_SERVICE_ID = "srv-daci5sifngtc73djb43g"
MAIN_READY_URL = "https://market-intel-api-jmil.onrender.com/ready"
ASK_HEALTH_URL = "https://ask-mimir-beta.onrender.com/api/health"
HOOK_SECRETS = {
    "main": "mimir/render/market-intel-api/deploy-hook",
    "ask_mimir": "mimir/render/ask-mimir-beta/deploy-hook",
}


def deployment_hooks(secrets_client: Any = None) -> dict[str, str]:
    hooks = {
        "main": os.getenv("MIMIR_MAIN_RENDER_DEPLOY_HOOK_URL", "").strip(),
        "ask_mimir": os.getenv("ASK_MIMIR_RENDER_DEPLOY_HOOK_URL", "").strip(),
    }
    for name in hooks:
        if not hooks[name]:
            secrets_client = secrets_client or boto3.client("secretsmanager", region_name="us-east-1")
            try:
                raw = secrets_client.get_secret_value(SecretId=HOOK_SECRETS[name])["SecretString"].strip()
                try:
                    secret = json.loads(raw)
                    if isinstance(secret, dict):
                        raw = secret.get("url") or secret.get("deploy_hook_url") or ""
                except json.JSONDecodeError:
                    pass
                hooks[name] = raw
            except Exception:
                raise RuntimeError(f"{name} Render deploy hook could not be read from Secrets Manager") from None
    for name, service_id in (("main", MAIN_SERVICE_ID), ("ask_mimir", ASK_SERVICE_ID)):
        parsed = urllib.parse.urlparse(hooks[name])
        if (parsed.scheme != "https" or parsed.hostname != "api.render.com"
                or parsed.path != f"/deploy/{service_id}"
                or not urllib.parse.parse_qs(parsed.query).get("key")):
            raise RuntimeError(f"A valid {name} Render deploy hook is required before promotion")
    return hooks


def _read_json(url: str, *, timeout: int = 15) -> dict[str, Any] | None:
    request = urllib.request.Request(url, headers={"Cache-Control": "no-store"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if response.status != 200:
                return None
            value = json.load(response)
            return value if isinstance(value, dict) else None
    except (OSError, ValueError, urllib.error.HTTPError):
        return None


def consumer_state(*, main_url: str = MAIN_READY_URL, ask_url: str = ASK_HEALTH_URL) -> dict[str, Any]:
    return {"main": _read_json(main_url), "ask_mimir": _read_json(ask_url)}


def matches_release(state: dict[str, Any], platform: dict[str, Any]) -> bool:
    main = state.get("main") or {}
    ask = state.get("ask_mimir") or {}
    components = platform["components"]
    return bool(
        main.get("ready") is True
        and main.get("platform_release_id") == platform["release_id"]
        and main.get("main_release_id") == components["main"]["release_id"]
        and ask.get("status") == "ok"
        and ask.get("platform_manifest_enabled") is True
        and ask.get("platform_release_id") == platform["release_id"]
        and ask.get("runtime_release_id") == components["ask_mimir"]["release_id"]
    )


def trigger_deploy(name: str, hook: str) -> str | None:
    request = urllib.request.Request(hook, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            if response.status not in {200, 202}:
                raise RuntimeError(f"{name} Render deploy hook returned HTTP {response.status}")
            payload = json.load(response) if response.status == 200 else {}
            return str((payload.get("deploy") or {}).get("id") or "") or None
    except urllib.error.HTTPError as error:
        # HTTPError can contain the secret hook URL; never surface it.
        raise RuntimeError(f"{name} Render deploy hook returned HTTP {error.code}") from None
    except OSError:
        raise RuntimeError(f"{name} Render deploy hook could not be reached") from None


def refresh_consumers(platform: dict[str, Any], hooks: dict[str, str], *, timeout_seconds: int = 1200) -> dict[str, Any]:
    deploys = {name: trigger_deploy(name, hooks[name]) for name in ("main", "ask_mimir")}
    deadline = time.monotonic() + timeout_seconds
    last_state: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last_state = consumer_state()
        if matches_release(last_state, platform):
            return {"deploy_ids": deploys, "verified": True,
                    "platform_release_id": platform["release_id"]}
        time.sleep(10)
    raise RuntimeError(
        "Consumer rollover did not reach the promoted release within "
        f"{timeout_seconds}s; deploy IDs: {deploys}; "
        f"Main release: {(last_state.get('main') or {}).get('main_release_id')}; "
        f"Ask release: {(last_state.get('ask_mimir') or {}).get('runtime_release_id')}"
    )
