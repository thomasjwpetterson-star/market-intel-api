"""Admit public analytical work before it consumes a shared worker thread.

Rate-limit exemptions are identities, not capacity exemptions. Keep one of the
two DuckDB readers and AnyIO worker tokens available for customer requests.
There is deliberately no pending queue: callers retry using normal HTTP caching.
"""
from __future__ import annotations

import logging
import asyncio

logger = logging.getLogger("mimir-api")


class PublicServingCapacityMiddleware:
    def __init__(self, app, limit=1):
        self.app = app
        self.limit = limit
        self.active = 0
        self.rejected = 0
        self.inflight = set()

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
        # A disconnected/cancelled caller cannot stop a running synchronous
        # worker. Keep its slot until downstream actually finishes, otherwise
        # repeated cancellations could admit an unbounded number of queries.
        task = asyncio.create_task(self._serve(scope, receive, send))
        self.inflight.add(task)
        task.add_done_callback(self._finished)
        await asyncio.shield(task)

    async def _serve(self, scope, receive, send):
        try:
            await self.app(scope, receive, send)
        finally:
            self.active -= 1

    def _finished(self, task):
        self.inflight.discard(task)
        # Retrieve exceptions even if the client no longer awaits the result.
        # Attached callers still receive downstream exceptions through shield.
        if not task.cancelled():
            task.exception()
