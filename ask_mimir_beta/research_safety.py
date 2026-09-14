"""Concurrency boundaries for shared evidence stores (not model generation)."""

from __future__ import annotations

import copy
import json
import logging
import os
from pathlib import Path
import tempfile
from logging.handlers import RotatingFileHandler
from functools import wraps
from typing import Any


def configure_duckdb_scratch(connection: Any, label: str) -> Path:
    """Give each connection its own spill directory, including across processes."""
    root = Path(os.getenv('ASK_MIMIR_DUCKDB_TEMP', os.getenv('ASK_MIMIR_DUCKDB_TEMP_DIR', '/tmp/ask-mimir-duckdb')))
    root.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix=label + '-', dir=str(root)))
    connection.execute('SET temp_directory = ?', [str(directory)])
    return directory


def write_bounded_audit_record(path: Any, record: dict[str, Any], *, max_bytes: int = 20 * 1024 * 1024, backups: int = 4) -> None:
    """Keep audit history bounded; the caller serializes writes to this path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(path, maxBytes=max_bytes, backupCount=backups, encoding="utf-8")
    try:
        handler.emit(logging.LogRecord("mimir.audit", logging.INFO, "", 0, json.dumps(record, default=str), (), None))
    finally:
        handler.close()


class SynchronizedStore:
    """Keep a complete evidence operation, including cursor fetching, atomic.

    Related stores share an RLock so nested retrieval remains safe. Returned
    packs are detached from mutable caches before request-specific enrichment.
    Model calls happen outside this boundary and can still run concurrently.
    """

    def __init__(self, store: Any, lock: Any, immutable_methods: frozenset[str] = frozenset()) -> None:
        self._store = store
        self._lock = lock
        self._immutable_methods = immutable_methods

    def __getattr__(self, name: str) -> Any:
        value = getattr(self._store, name)
        if not callable(value):
            return value

        @wraps(value)
        def guarded(*args: Any, **kwargs: Any) -> Any:
            if name in self._immutable_methods:
                # Catalog-only operations have no cursor/cache mutation and must
                # remain responsive while a queued worker scans large evidence.
                return copy.deepcopy(value(*args, **kwargs))
            with self._lock:
                result = value(*args, **kwargs)
                for cache_name in ("_cache", "_dynamic_contexts"):
                    cache = getattr(self._store, cache_name, None)
                    if isinstance(cache, dict):
                        sizes = [(key, len(json.dumps(pack, default=str))) for key, pack in cache.items()]
                        total = sum(size for _, size in sizes)
                        for key, size in sizes:
                            if len(cache) <= 16 and total <= 32 * 1024 * 1024:
                                break
                            cache.pop(key, None)
                            total -= size
                return copy.deepcopy(result) if isinstance(result, (dict, list)) else result

        return guarded
