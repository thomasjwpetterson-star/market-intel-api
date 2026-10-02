"""Production entry point for the isolated Ask Mimir beta service."""

from __future__ import annotations

import os
from copy import deepcopy

import uvicorn

from bootstrap_data import bootstrap


def service_log_config():
    """Emit content-free lifecycle records even when no browser keeps polling."""
    config = deepcopy(uvicorn.config.LOGGING_CONFIG)
    config["loggers"]["ask-mimir"] = {
        "handlers": ["default"], "level": "INFO", "propagate": False,
    }
    return config


if __name__ == "__main__":
    # Reuse verified files and fetch only changed objects from the atomic release.
    bootstrap()
    os.environ["ASK_MIMIR_ALLOW_TEST_IDENTITIES"] = "0"
    os.environ.setdefault("ASK_MIMIR_STRICT_CITATIONS", "1")
    uvicorn.run(
        "lab_api:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
        workers=1,
        log_config=service_log_config(),
    )
