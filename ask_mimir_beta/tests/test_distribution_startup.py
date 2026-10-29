"""Exercise the actual production entry point without fetching runtime data."""
import os
from pathlib import Path
import runpy
import sys
from types import SimpleNamespace
from unittest.mock import patch


def start(enabled):
    events = []
    uvicorn = SimpleNamespace(
        config=SimpleNamespace(LOGGING_CONFIG={"loggers": {}}),
        run=lambda application, **options: events.append((application, options)),
    )
    bootstrap = SimpleNamespace(bootstrap=lambda: events.append("bootstrap"))
    with patch.dict(sys.modules, {"uvicorn": uvicorn, "bootstrap_data": bootstrap}), patch.dict(os.environ, {"MIMIR_DISTRIBUTION_ENABLED": enabled}, clear=True):
        runpy.run_path(str(Path(__file__).resolve().parents[1] / "serve.py"), run_name="__main__")
        assert os.environ["ASK_MIMIR_ALLOW_TEST_IDENTITIES"] == "0"
        assert os.environ["ASK_MIMIR_STRICT_CITATIONS"] == "1"
    return events


def test_default_entrypoint_preserves_existing_ask_and_bootstraps_once():
    events = start("0")
    assert events[0] == "bootstrap"
    assert events[1][0] == "lab_api:app"
    assert events[1][1]["factory"] is False
    assert events[1][1]["workers"] == 1


def test_enabled_entrypoint_uses_composition_factory_without_double_bootstrap():
    events = start("1")
    assert len(events) == 1
    assert events[0][0] == "mimir_distribution.serve_existing_ask:build_app"
    assert events[0][1]["factory"] is True
    assert events[0][1]["host"] == "0.0.0.0"
