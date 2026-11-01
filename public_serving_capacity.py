"""Admit public analytical work before it consumes a shared worker thread.

Rate-limit exemptions are identities, not capacity exemptions. Keep one of the
two DuckDB readers and AnyIO worker tokens available for customer requests.
Only duplicate public reads may wait briefly for shared work. Different cold
records have no admission queue; cached public records can still be served.
"""
from __future__ import annotations

import logging
import asyncio
import re
import time
from collections import OrderedDict

logger = logging.getLogger("mimir-api")


class PublicServingCapacityMiddleware:
    def __init__(self, app, limit=1, cache_generation=None, max_entries=256,
                 max_bytes=32 * 1024 * 1024, ttl=300, stale_ttl=3600,
                 max_waiters=16, wait_timeout=2, clock=time.monotonic):
        self.app = app
        self.limit = limit
        self.active = 0
        self.rejected = 0
        self.inflight = set()
        self.cache_generation = cache_generation
        self.generation = object()
        self.cache = OrderedDict()
        self.cache_bytes = 0
        self.max_entries, self.max_bytes = max_entries, max_bytes
        self.ttl, self.stale_ttl = ttl, stale_ttl
        self.max_waiters, self.wait_timeout = max_waiters, wait_timeout
        self.waiters = 0
        self.flights = {}
        self.clock = clock

    async def __call__(self, scope, receive, send):
        if (scope["type"] != "http" or scope.get("method") not in {"GET", "HEAD"}
                or not scope.get("path", "").startswith("/api/public/intelligence/")):
            return await self.app(scope, receive, send)
        key = None
        if self.cache_generation is not None:
            generation = self.cache_generation()
            if generation != self.generation:
                self.cache.clear()
                self.cache_bytes = 0
                self.generation = generation
            headers = dict(scope.get('headers', []))
            if (generation is not None and scope.get('method') == 'GET'
                    and not {b'authorization', b'cookie'}.intersection(headers)
                    and re.fullmatch(r'/api/public/intelligence/(?:awards|companies|nsn|platforms|solicitations)/[^/]+', scope['path'])):
                key = (generation, scope['path'], scope.get('query_string', b''))
        cached = self.cache.get(key) if key is not None else None
        age = self.clock() - cached[0] if cached else 0
        if cached and age <= self.ttl:
            self.cache.move_to_end(key)
            return await self._respond(cached, send, 'hit')
        if cached and age <= self.stale_ttl and self.active >= self.limit:
            return await self._respond(cached, send, 'stale')
        # Only identical public reads can wait; total waiters and wait time are
        # bounded. Shield keeps a timed-out/disconnected follower from cancelling
        # the one real query. There is no queue of different cold records.
        if key in self.flights and self.waiters < self.max_waiters:
            self.waiters += 1
            try:
                result = await asyncio.wait_for(asyncio.shield(self.flights[key]), self.wait_timeout)
                if result is not None:
                    return await self._respond(result, send, 'coalesced')
            except Exception as error:
                if not isinstance(error, TimeoutError):
                    logger.warning('Coalesced public request failed: %s', type(error).__name__)
            finally:
                self.waiters -= 1
        # No await between checking and acquiring: atomic within the ASGI loop.
        if self.active >= self.limit:
            self.rejected += 1
            if self.rejected == 1 or self.rejected % 100 == 0:
                logger.warning("Public serving capacity exhausted: active=%s limit=%s rejected=%s pending=%s",
                               self.active, self.limit, self.rejected, self.waiters)
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
        task = asyncio.create_task(self._serve(scope, receive, send, key))
        self.inflight.add(task)
        if key is not None:
            self.flights[key] = task
            task.add_done_callback(lambda completed: self.flights.pop(key, None))
        task.add_done_callback(self._finished)
        await asyncio.shield(task)

    async def _serve(self, scope, receive, send, key):
        messages, size = [], 0
        async def capture(message):
            nonlocal size
            size += len(message.get('body', b''))
            if size <= min(self.max_bytes, 2 * 1024 * 1024):
                messages.append(message.copy())
            else:
                messages.clear()
            await send(message)
        try:
            await self.app(scope, receive, capture if key is not None else send)
            if (key is not None and key[0] == self.generation and messages
                    and messages[0].get('status') == 200
                    and messages[-1]['type'] == 'http.response.body'
                    and not messages[-1].get('more_body', False)):
                headers = dict(messages[0].get('headers', []))
                directives = {part.strip() for part in headers.get(b'cache-control', b'').lower().split(b',')}
                if (b'public' in directives and not {b'private', b'no-store'}.intersection(directives)
                        and b'set-cookie' not in headers and b'vary' not in headers):
                    body = b''.join(message.get('body', b'') for message in messages[1:])
                    entry = (self.clock(), messages[0]['headers'], body)
                    weight = len(body) + sum(len(k) + len(v) for k, v in entry[1]) + len(str(key))
                    if weight <= self.max_bytes:
                        previous = self.cache.pop(key, None)
                        if previous is not None:
                            self.cache_bytes -= previous[3]
                        while self.cache and (len(self.cache) >= self.max_entries or self.cache_bytes + weight > self.max_bytes):
                            _, old = self.cache.popitem(last=False)
                            self.cache_bytes -= old[3]
                        entry = (*entry, weight)
                        self.cache[key] = entry
                        self.cache_bytes += weight
                        return entry
        finally:
            self.active -= 1

    async def _respond(self, entry, send, status):
        headers = [(k, v) for k, v in entry[1] if k.lower() not in {b'age', b'x-mimir-public-cache'}]
        headers.extend([(b'age', str(int(self.clock() - entry[0])).encode()),
                        (b'x-mimir-public-cache', status.encode())])
        if status == 'stale':
            headers.append((b'warning', b'110 - "Response is stale"'))
        await send({'type': 'http.response.start', 'status': 200, 'headers': headers})
        await send({'type': 'http.response.body', 'body': entry[2]})

    def _finished(self, task):
        self.inflight.discard(task)
        # Retrieve exceptions even if the client no longer awaits the result.
        # Attached callers still receive downstream exceptions through shield.
        if not task.cancelled():
            task.exception()
