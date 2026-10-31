"""Admit public analytical work before it consumes a shared worker thread.

Rate-limit exemptions are identities, not capacity exemptions. Keep one of the
two DuckDB readers and AnyIO worker tokens available for customer requests.
There is deliberately no pending queue: callers retry using normal HTTP caching.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("mimir-api")


class PublicServingCapacityMiddleware:
    def __init__(self, app, limit=1):
        self.app = app
        self.limit = limit
        self.active = 0
        self.rejected = 0

    async def __call__(self, scope, receive, send):
        if (scope["type"] != "http" or scope.get("method") not in {"GET", "HEAD"}
                or not scope.get("path", "").startswith("/api/public/intelligence/")):
            return await self.app(scope, receive, send)
        # No await between checking and acquiring: atomic within the ASGI loop.
        if self.active >= self.limit:
            self.rejected += 1
            if self.rejected == 1 or self.rejected % 100 == 0:
                logger.warning("Public serving capacity exhausted: active=%s limit=%s rejected=%s pending=0",
                               self.active, self.limit, self.rejected)
            body = b'{"detail":"Public intelligence is busy. Please retry shortly."}'
            await send({"type": "http.response.start", "status": 503, "headers": [
                (b"content-type", b"application/json"),
                (b"cache-control", b"no-store"), (b"retry-after", b"2"),
                (b"x-mimir-public-capacity", b"busy"),
            ]})
            await send({"type": "http.response.body", "body": body})
            return
        self.active += 1
        try:
            await self.app(scope, receive, send)
        finally:
            self.active -= 1
