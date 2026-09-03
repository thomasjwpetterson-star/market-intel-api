"""Production entry point for the isolated Ask Mimir beta service."""

from __future__ import annotations

import os

import uvicorn

from bootstrap_data import bootstrap


if __name__ == "__main__":
    bootstrap()
    os.environ["ASK_MIMIR_ALLOW_TEST_IDENTITIES"] = "0"
    os.environ.setdefault("ASK_MIMIR_STRICT_CITATIONS", "1")
    uvicorn.run(
        "lab_api:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
        workers=1,
    )
