"""The pointer cutover must leave both in-memory consumers on one release."""

from unittest.mock import patch

import pytest

from production_consumer_rollover import (
    ASK_SERVICE_ID,
    MAIN_SERVICE_ID,
    deployment_hooks,
    matches_release,
    refresh_consumers,
)


PLATFORM = {
    "release_id": "platform-new",
    "components": {
        "main": {"release_id": "main-new"},
        "ask_mimir": {"release_id": "ask-new"},
    },
}
READY = {
    "main": {"ready": True, "platform_release_id": "platform-new", "main_release_id": "main-new"},
    "ask_mimir": {"status": "ok", "platform_manifest_enabled": True,
                  "platform_release_id": "platform-new", "runtime_release_id": "ask-new"},
}


def test_hooks_are_required_and_bound_to_the_intended_services(monkeypatch):
    monkeypatch.delenv("MIMIR_MAIN_RENDER_DEPLOY_HOOK_URL", raising=False)
    monkeypatch.delenv("ASK_MIMIR_RENDER_DEPLOY_HOOK_URL", raising=False)
    class MissingSecrets:
        def get_secret_value(self, **_kwargs):
            raise KeyError("missing")
    with pytest.raises(RuntimeError, match="main Render deploy hook"):
        deployment_hooks(MissingSecrets())
    monkeypatch.setenv("MIMIR_MAIN_RENDER_DEPLOY_HOOK_URL", f"https://api.render.com/deploy/{MAIN_SERVICE_ID}?key=main-secret")
    monkeypatch.setenv("ASK_MIMIR_RENDER_DEPLOY_HOOK_URL", f"https://api.render.com/deploy/{MAIN_SERVICE_ID}?key=wrong-service")
    with pytest.raises(RuntimeError, match="ask_mimir Render deploy hook"):
        deployment_hooks()
    monkeypatch.setenv("ASK_MIMIR_RENDER_DEPLOY_HOOK_URL", f"https://api.render.com/deploy/{ASK_SERVICE_ID}?key=ask-secret")
    assert set(deployment_hooks()) == {"main", "ask_mimir"}


def test_existing_aws_secrets_supply_both_service_hooks(monkeypatch):
    monkeypatch.delenv("MIMIR_MAIN_RENDER_DEPLOY_HOOK_URL", raising=False)
    monkeypatch.delenv("ASK_MIMIR_RENDER_DEPLOY_HOOK_URL", raising=False)
    class ExistingSecrets:
        def get_secret_value(self, *, SecretId):
            service = MAIN_SERVICE_ID if "market-intel-api" in SecretId else ASK_SERVICE_ID
            return {"SecretString": f"https://api.render.com/deploy/{service}?key=secret"}
    assert set(deployment_hooks(ExistingSecrets())) == {"main", "ask_mimir"}


def test_both_consumers_must_report_exact_release_and_ask_root_mode():
    assert matches_release(READY, PLATFORM)
    assert not matches_release({**READY, "main": {**READY["main"], "main_release_id": "old"}}, PLATFORM)
    assert not matches_release({**READY, "ask_mimir": {**READY["ask_mimir"], "runtime_release_id": "old"}}, PLATFORM)
    assert not matches_release({**READY, "ask_mimir": {**READY["ask_mimir"], "platform_manifest_enabled": False}}, PLATFORM)


def test_rollover_triggers_both_and_waits_for_exact_release():
    old = {**READY, "main": {**READY["main"], "main_release_id": "old"}}
    with patch("production_consumer_rollover.trigger_deploy", side_effect=["main-deploy", "ask-deploy"]) as trigger, patch(
        "production_consumer_rollover.consumer_state", side_effect=[old, READY]
    ), patch("production_consumer_rollover.time.sleep"):
        result = refresh_consumers(PLATFORM, {"main": "main-hook", "ask_mimir": "ask-hook"}, timeout_seconds=30)
    assert result == {"deploy_ids": {"main": "main-deploy", "ask_mimir": "ask-deploy"},
                      "verified": True, "platform_release_id": "platform-new"}
    assert trigger.call_count == 2
