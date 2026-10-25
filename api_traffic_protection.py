"""Origin-side extraction limits; these are not product view allowances."""
from __future__ import annotations

import hmac
import ipaddress
import json
import logging
import math
import os
import threading
import time
import urllib.request
from collections import OrderedDict
from pathlib import Path

logger = logging.getLogger("mimir-api")
RANGE_URLS = {
    "google": "https://developers.google.com/static/crawling/ipranges/common-crawlers.json",
    "bing": "https://www.bing.com/toolbox/bingbot.json",
}
READ_POSTS = frozenset({
    "/api/explorer/preview", "/api/explorer/page", "/api/explorer/count",
    "/api/explorer/subcontract-descriptions",
})
SAFE_STATUS = frozenset({"/api/dashboard/status"})


def positive_env(name, default):
    try:
        return max(1, int(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return default


def parse_ranges(document):
    ranges = []
    for entry in document["prefixes"]:
        value = entry.get("ipv4Prefix") or entry.get("ipv6Prefix")
        if value:
            network = ipaddress.ip_network(value)
            # Never accept a malformed feed that exempts the whole internet.
            if network.prefixlen < (8 if network.version == 4 else 16):
                raise ValueError("Unexpectedly broad crawler network")
            ranges.append(network)
    if not ranges:
        raise ValueError("Empty crawler range feed")
    return tuple(ranges)


class VerifiedCrawlers:
    def __init__(self, refresh=True):
        self.ranges = {}
        try:
            snapshot = json.loads(Path(__file__).with_name("crawler_ip_ranges.json").read_text())
            self.ranges = {name: parse_ranges(document) for name, document in snapshot.items()}
        except (OSError, ValueError, KeyError, TypeError):
            logger.warning("Bundled crawler ranges unavailable; using normal generous API limits")
        if refresh:
            threading.Thread(target=self._refresh_loop, daemon=True, name="crawler-ip-ranges").start()

    def _refresh_loop(self):
        while True:
            for name, url in RANGE_URLS.items():
                try:
                    request = urllib.request.Request(url, headers={"User-Agent": "MimirCrawlerVerification/1.0"})
                    with urllib.request.urlopen(request, timeout=5) as response:
                        document = json.loads(response.read(1_000_001))
                    ranges = parse_ranges(document)
                    self.ranges = {**self.ranges, name: ranges}
                except Exception:
                    # Retain last-known-good ranges. No per-request DNS/network lookups.
                    logger.warning("Crawler range refresh unavailable for %s; retaining bundled/last good ranges", name)
            threading.Event().wait(86400)

    def verified(self, address, user_agent):
        try:
            address = ipaddress.ip_address(address)
        except ValueError:
            return False
        agent = user_agent.lower()
        provider = "google" if "googlebot" in agent or "google-inspectiontool" in agent else "bing" if "bingbot" in agent else None
        return bool(provider and any(address.version == network.version and address in network
                                     for network in self.ranges.get(provider, ())))


class RequestBudgets:
    """Bounded per-process, per-peer fixed windows for this single-worker service."""
    def __init__(self, clock=time.monotonic, max_clients=50000, limits=None):
        self.clock = clock
        self.max_clients = max_clients
        self.clients = OrderedDict()
        self.lock = threading.Lock()
        self.limits = limits or {
            "all": ((60, positive_env("MIMIR_API_REQUESTS_PER_MINUTE", 240)),
                    (3600, positive_env("MIMIR_API_REQUESTS_PER_HOUR", 6000))),
            "bulk": ((60, positive_env("MIMIR_API_BULK_REQUESTS_PER_MINUTE", 60)),
                     (3600, positive_env("MIMIR_API_BULK_REQUESTS_PER_HOUR", 1800))),
        }

    def consume(self, address, bulk=False):
        now = self.clock()
        with self.lock:
            # LRU pruning prevents an unbounded map from rotating residential IPs.
            while self.clients and (len(self.clients) >= self.max_clients or
                                    next(iter(self.clients.values()))[0] < now - 3600):
                if address in self.clients and len(self.clients) == self.max_clients:
                    break
                self.clients.popitem(last=False)
            _, counts = self.clients.pop(address, (now, {}))
            checks = []
            for group in (("all", "bulk") if bulk else ("all",)):
                for window, limit in self.limits[group]:
                    bucket = int(now // window)
                    old_bucket, count = counts.get((group, window), (bucket, 0))
                    count = count if old_bucket == bucket else 0
                    checks.append(((group, window), bucket, count, limit))
            blocked = [max(1, math.ceil((bucket + 1) * key[1] - now))
                       for key, bucket, count, limit in checks if count >= limit]
            if not blocked:
                for key, bucket, count, _ in checks:
                    counts[key] = (bucket, count + 1)
            self.clients[address] = (now, counts)
            return max(blocked) if blocked else 0


class ApiTrafficProtectionMiddleware:
    def __init__(self, app, budgets=None, crawlers=None):
        self.app = app
        self.budgets = budgets if budgets is not None else RequestBudgets()
        self.crawlers = crawlers if crawlers is not None else VerifiedCrawlers()
        self._trusted_read_seen = False

    async def __call__(self, scope, receive, send):
        path, method = scope.get("path", ""), scope.get("method", "")
        protected = (path.startswith("/api/") and path not in SAFE_STATUS and
                     (method == "GET" or method == "POST" and path in READ_POSTS))
        if scope["type"] != "http" or not protected or os.getenv("MIMIR_API_TRAFFIC_LIMITS", "on") == "off":
            return await self.app(scope, receive, send)
        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        original_send = send

        async def policy_send(message):
            if message["type"] == "http.response.start":
                message = {**message, "headers": [*message.get("headers", []), (b"x-mimir-traffic-policy", b"v1")]}
            await original_send(message)
        # Reuse the existing server-only export credential for public SSR reads.
        # A client Origin/Referer/Authorization header is never an exemption.
        expected = os.getenv("MIMIR_EXPORT_PROXY_SECRET", "").strip().encode()
        trusted_read = (method == "GET" and path.startswith("/api/public/") and bool(expected) and
                        hmac.compare_digest(headers.get(b"x-mimir-export-key", b""), expected))
        # Uvicorn/Render supplies the peer. Never trust client-supplied forwarding
        # headers or a claimed JA4, nor user-agent text without verified IP ranges.
        address = (scope.get("client") or ("unknown", 0))[0]
        if trusted_read or method == "GET" and self.crawlers.verified(address, headers.get(b"user-agent", b"").decode("latin-1")):
            if trusted_read and not self._trusted_read_seen:
                self._trusted_read_seen = True
                logger.info("API traffic protection: authenticated public server reads verified")
            return await self.app(scope, receive, policy_send)
        retry_after = self.budgets.consume(address, bulk=path in READ_POSTS)
        if retry_after:
            body = json.dumps({"detail": "API request rate exceeded. Please retry shortly."}).encode()
            await send({"type": "http.response.start", "status": 429, "headers": [
                (b"content-type", b"application/json"),
                (b"cache-control", b"private, no-store"),
                (b"retry-after", str(retry_after).encode()),
                (b"x-mimir-traffic-policy", b"v1"),
            ]})
            await send({"type": "http.response.body", "body": body})
            return
        return await self.app(scope, receive, policy_send)
