"""Bound expensive HTTP reads before they occupy threads or database readers."""
import asyncio
from contextvars import ContextVar
import hmac
import logging
import os
import time

ANALYTICAL_DEADLINE = ContextVar("mimir_analytical_deadline", default=None)
logger = logging.getLogger("mimir-api")
READ_BUDGETS = {
    ("POST", "/api/research/aggregate"): 38,
    ("POST", "/api/research/query"): 38,
    ("POST", "/api/research/item-history"): 28,
    ("POST", "/api/research/programme-coverage"): 28,
    ("POST", "/api/research/programme-parts"): 28,
    ("POST", "/api/research/programme-records"): 28,
    ("POST", "/api/explorer/page"): 16,
    ("POST", "/api/explorer/count"): 16,
    ("GET", "/api/platform/parts"): 28,
    ("GET", "/api/platform/parts/count"): 28,
    ("GET", "/api/search/global"): 16,
}


class AnalyticalAdmissionMiddleware:
    def __init__(self, app, pending_limit=4, pending_timeout=8, budgets=None):
        self.app = app
        self.pending_limit, self.pending_timeout = pending_limit, pending_timeout
        self.budgets = READ_BUDGETS if budgets is None else budgets
        self.slot = asyncio.Semaphore(1)
        self.active = self.waiting = 0
        self.inflight = set()

    async def __call__(self, scope, receive, send):
        budget = self.budgets.get((scope.get("method"), scope.get("path")))
        if self.budgets is READ_BUDGETS and scope.get("method")=="GET" and scope.get("path", "").startswith("/api/platform/"):
            budget = 28
        if scope["type"] != "http" or budget is None:
            return await self.app(scope, receive, send)
        # Invalid research credentials must not reserve analytical capacity.
        # The route retains its normal 401 response and constant-time check.
        if scope["path"].startswith("/api/research/"):
            expected = os.getenv("MIMIR_EXPORT_PROXY_SECRET", "").strip()
            supplied = dict(scope.get("headers", [])).get(b"x-mimir-export-key", b"").decode("latin1").strip()
            if not expected or not supplied or not hmac.compare_digest(expected, supplied):
                return await self.app(scope, receive, send)
        started = time.monotonic()
        if self.slot.locked() and self.waiting >= self.pending_limit:
            return await self._busy(scope, send, "queue_full", started)
        self.waiting += 1
        try:
            async with asyncio.timeout(min(self.pending_timeout, budget)):
                await self.slot.acquire()
        except TimeoutError:
            return await self._busy(scope, send, "queue_timeout", started)
        finally:
            self.waiting -= 1
        self.active += 1
        # A cancelled HTTP caller cannot release a still-running sync query.
        task = asyncio.create_task(self._serve(scope, receive, send, started + budget))
        self.inflight.add(task)
        task.add_done_callback(self._finished)
        await asyncio.shield(task)

    async def _serve(self, scope, receive, send, deadline):
        token = ANALYTICAL_DEADLINE.set(deadline)
        try:
            await self.app(scope, receive, send)
        finally:
            ANALYTICAL_DEADLINE.reset(token)
            self.active -= 1
            self.slot.release()

    async def _busy(self, scope, send, reason, started):
        logger.warning("Analytical admission rejected: path=%s reason=%s wait_ms=%.1f active=%s pending=%s",
                       scope["path"], reason, (time.monotonic()-started)*1000, self.active, self.waiting)
        await send({"type": "http.response.start", "status": 503, "headers": [
            (b"content-type", b"application/json"), (b"cache-control", b"private, no-store"),
            (b"retry-after", b"2"), (b"x-mimir-analytical-capacity", b"busy")]})
        await send({"type": "http.response.body", "body": b'{"detail":"Analytical capacity is busy. Retry shortly."}'})

    def _finished(self, task):
        self.inflight.discard(task)
        if not task.cancelled():
            task.exception()
