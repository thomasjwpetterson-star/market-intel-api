"""Bound expensive HTTP reads before they occupy threads or database readers."""
import asyncio
from contextvars import ContextVar
import hmac
import logging
import os
import re
import time

ANALYTICAL_DEADLINE = ContextVar("mimir_analytical_deadline", default=None)
ANALYTICAL_TRACE_ID = ContextVar("mimir_analytical_trace_id", default=None)
logger = logging.getLogger("mimir-api")
READ_BUDGETS = {
    ("POST", "/api/research/aggregate"): 38,
    ("POST", "/api/research/query"): 38,
    ("POST", "/api/research/item-history"): 28,
    ("POST", "/api/research/action-history"): 38,
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
    def __init__(self, app, pending_limit=4, pending_timeout=8, budgets=None, execution_reserve=2):
        self.app = app
        self.pending_limit, self.pending_timeout = pending_limit, pending_timeout
        self.budgets = READ_BUDGETS if budgets is None else budgets
        self.execution_reserve = execution_reserve
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
        trace_id = None
        queue_timeout = min(self.pending_timeout, budget)
        if scope["path"].startswith("/api/research/"):
            expected = os.getenv("MIMIR_EXPORT_PROXY_SECRET", "").strip()
            headers = dict(scope.get("headers", []))
            supplied = headers.get(b"x-mimir-export-key", b"").decode("latin1").strip()
            if not expected or not supplied or not hmac.compare_digest(expected, supplied):
                return await self.app(scope, receive, send)
            raw_trace = headers.get(b"x-mimir-research-trace", b"").decode("latin1")
            if re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", raw_trace):
                trace_id = raw_trace
            # Only authenticated research callers can opt into deadline-aware
            # waiting. Older clients and non-research routes retain their queue.
            caller_budget = headers.get(b"x-mimir-research-budget-ms")
            if caller_budget is not None:
                if not re.fullmatch(rb"[0-9]{1,6}", caller_budget):
                    return await self._response(send, 400, b'{"detail":"Invalid research time budget."}')
                budget = min(budget, int(caller_budget) / 1000)
                caller_deadline = headers.get(b"x-mimir-research-deadline-ms")
                if caller_deadline is not None:
                    if not re.fullmatch(rb"[0-9]{1,16}", caller_deadline):
                        return await self._response(send, 400, b'{"detail":"Invalid research deadline."}')
                    # Absolute expiry also charges network transit to the caller;
                    # neither header may extend the existing route budget.
                    budget = min(budget, int(caller_deadline)/1000 - time.time())
                if budget <= 0:
                    return await self._response(send, 504, b'{"detail":"The research deadline has expired."}')
                queue_timeout = max(0, budget - self.execution_reserve)
        started = time.monotonic()
        if self.slot.locked() and self.waiting >= self.pending_limit:
            return await self._busy(scope, send, "queue_full", started, trace_id)
        self.waiting += 1
        try:
            async with asyncio.timeout(queue_timeout):
                await self.slot.acquire()
        except TimeoutError:
            return await self._busy(scope, send, "queue_timeout", started, trace_id)
        finally:
            self.waiting -= 1
        self.active += 1
        if trace_id:
            logger.info("Analytical admission granted: path=%s wait_ms=%.1f budget_ms=%.1f active=%s pending=%s trace=%s",
                        scope["path"], (time.monotonic()-started)*1000, budget*1000, self.active, self.waiting, trace_id)
        # A cancelled HTTP caller cannot release a still-running sync query.
        task = asyncio.create_task(self._serve(scope, receive, send, started + budget, trace_id))
        self.inflight.add(task)
        task.add_done_callback(self._finished)
        await asyncio.shield(task)

    async def _serve(self, scope, receive, send, deadline, trace_id):
        token = ANALYTICAL_DEADLINE.set(deadline)
        trace_token = ANALYTICAL_TRACE_ID.set(trace_id)
        try:
            await self.app(scope, receive, send)
        finally:
            ANALYTICAL_DEADLINE.reset(token)
            ANALYTICAL_TRACE_ID.reset(trace_token)
            self.active -= 1
            self.slot.release()

    async def _busy(self, scope, send, reason, started, trace_id=None):
        logger.warning("Analytical admission rejected: path=%s reason=%s wait_ms=%.1f active=%s pending=%s trace=%s",
                       scope["path"], reason, (time.monotonic()-started)*1000, self.active, self.waiting, trace_id or "-")
        await send({"type": "http.response.start", "status": 503, "headers": [
            (b"content-type", b"application/json"), (b"cache-control", b"private, no-store"),
            (b"retry-after", b"2"), (b"x-mimir-analytical-capacity", b"busy")]})
        await send({"type": "http.response.body", "body": b'{"detail":"Analytical capacity is busy. Retry shortly."}'})

    async def _response(self, send, status, body):
        await send({"type":"http.response.start", "status":status, "headers":[
            (b"content-type",b"application/json"), (b"cache-control",b"private, no-store")]})
        await send({"type":"http.response.body", "body":body})

    def _finished(self, task):
        self.inflight.discard(task)
        if not task.cancelled():
            task.exception()
