import asyncio
import json
import os
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from httpx import ASGITransport, AsyncClient

from api_traffic_protection import ApiTrafficProtectionMiddleware, RequestBudgets, VerifiedCrawlers, parse_ranges


class BudgetTests(unittest.TestCase):
    def setUp(self):
        self.now = 0

    def budget(self, limits, max_clients=50000):
        return RequestBudgets(clock=lambda: self.now, limits=limits, max_clients=max_clients)

    def test_burst_limit_recovers_at_next_minute_without_resetting_hour(self):
        budget = self.budget({"all": ((60, 2), (3600, 3)), "bulk": ((60, 1),)})
        self.assertEqual([budget.consume("one") for _ in range(3)], [0, 0, 60])
        self.now = 60
        self.assertEqual(budget.consume("one"), 0)
        self.assertEqual(budget.consume("one"), 3540)
        self.now = 3600
        self.assertEqual(budget.consume("one"), 0)

    def test_bulk_and_normal_clients_have_separate_budgets(self):
        budget = self.budget({"all": ((60, 10),), "bulk": ((60, 1),)})
        self.assertEqual(budget.consume("one", bulk=True), 0)
        self.assertGreater(budget.consume("one", bulk=True), 0)
        self.assertEqual(budget.consume("one"), 0)
        self.assertEqual(budget.consume("two", bulk=True), 0)

    def test_state_is_bounded_and_expired_clients_are_removed(self):
        budget = self.budget({"all": ((60, 2),)}, max_clients=2)
        for ip in ("one", "two", "three", "three", "three"):
            budget.consume(ip)
            self.assertLessEqual(len(budget.clients), 2)
        self.assertGreater(budget.consume("three"), 0)
        self.now = 7200
        budget.consume("fresh")
        self.assertEqual(list(budget.clients), ["fresh"])

    def test_simultaneous_requests_do_not_overrun_the_limit(self):
        from concurrent.futures import ThreadPoolExecutor
        budget = self.budget({"all": ((60, 10),)})
        with ThreadPoolExecutor(max_workers=10) as pool:
            results = list(pool.map(lambda _: budget.consume("same"), range(100)))
        self.assertEqual(results.count(0), 10)


class CrawlerTests(unittest.TestCase):
    def test_official_google_and_bing_ranges_are_bundled_and_verified(self):
        verifier = VerifiedCrawlers(refresh=False)
        snapshot = json.loads(Path("crawler_ip_ranges.json").read_text())
        for provider, agent in (("google", "Googlebot/2.1"), ("bing", "bingbot/2.0")):
            for prefix in snapshot[provider]["prefixes"]:
                import ipaddress
                net = ipaddress.ip_network(prefix.get("ipv4Prefix") or prefix["ipv6Prefix"])
                self.assertTrue(verifier.verified(str(net.network_address + 1), agent))
        self.assertFalse(verifier.verified("192.0.2.10", "Googlebot/2.1"))
        self.assertFalse(verifier.verified("192.0.2.10", "bingbot/2.0"))
        self.assertFalse(verifier.verified("not-an-ip", "Googlebot/2.1"))

    def test_empty_or_whole_internet_feed_is_rejected(self):
        for prefixes in ([], [{"ipv4Prefix": "0.0.0.0/0"}], [{"ipv6Prefix": "::/0"}]):
            with self.assertRaises(ValueError):
                parse_ranges({"prefixes": prefixes})


class MiddlewareTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.app = FastAPI()
        for path in ("/api/nsn/profile", "/api/public/intelligence/nsn/123", "/api/dashboard/status", "/live"):
            self.app.get(path)(lambda: {"real": "record-content"})
        self.app.post("/api/explorer/page")(lambda: [{"real": "record-content"}])
        self.app.post("/auth/native-signup")(lambda: {"signup": "available"})
        self.budgets = RequestBudgets(clock=lambda: 0, limits={"all": ((60, 2),), "bulk": ((60, 1),)})
        self.verifier = VerifiedCrawlers(refresh=False)
        self.app.add_middleware(ApiTrafficProtectionMiddleware, budgets=self.budgets, crawlers=self.verifier)
        self.app.add_middleware(CORSMiddleware, allow_origins=["https://www.mimiradvisors.org"], allow_methods=["GET", "POST"], allow_headers=["Authorization", "Content-Type"])
        self.env = patch.dict(os.environ, {"MIMIR_API_TRAFFIC_LIMITS": "on", "MIMIR_EXPORT_PROXY_SECRET": "server-only-test-secret"})
        self.env.start()

    async def asyncTearDown(self):
        self.env.stop()

    def client(self, ip="192.0.2.10"):
        return AsyncClient(transport=ASGITransport(app=self.app, client=(ip, 123)), base_url="https://api.test")

    async def test_normal_data_unchanged_then_rate_limit_has_retry_and_cors(self):
        async with self.client() as client:
            for _ in range(2):
                response = await client.get("/api/nsn/profile")
                self.assertEqual(response.json(), {"real": "record-content"})
            response = await client.get("/api/nsn/profile", headers={"Origin": "https://www.mimiradvisors.org"})
            self.assertEqual(response.status_code, 429)
            self.assertEqual(response.headers["retry-after"], "60")
            self.assertEqual(response.headers["cache-control"], "private, no-store")
            self.assertEqual(response.headers["access-control-allow-origin"], "https://www.mimiradvisors.org")

    async def test_fake_crawler_authorization_forwarding_and_ja4_do_not_bypass(self):
        async with self.client() as client:
            headers = {"User-Agent": "Googlebot/2.1", "Authorization": "Bearer invented", "X-Forwarded-For": "66.249.66.1", "X-Vercel-JA4-Digest": "claimed"}
            statuses = [(await client.get("/api/nsn/profile", headers=headers)).status_code for _ in range(3)]
            self.assertEqual(statuses, [200, 200, 429])

    async def test_verified_indexers_can_render_without_limits(self):
        for provider, agent in (("google", "Googlebot/2.1"), ("bing", "bingbot/2.0")):
            network = next(net for net in self.verifier.ranges[provider] if net.version == 4)
            async with self.client(str(network.network_address + 1)) as client:
                for _ in range(5):
                    self.assertEqual((await client.get("/api/nsn/profile", headers={"User-Agent": agent})).status_code, 200)

    async def test_signed_public_ssr_is_exempt_but_wrong_key_is_not(self):
        async with self.client() as client:
            for _ in range(5):
                response = await client.get("/api/public/intelligence/nsn/123", headers={"X-Mimir-Export-Key": "server-only-test-secret"})
                self.assertEqual(response.status_code, 200)
            for _ in range(2):
                await client.get("/api/public/intelligence/nsn/123", headers={"X-Mimir-Export-Key": "wrong"})
            self.assertEqual((await client.get("/api/public/intelligence/nsn/123")).status_code, 429)

    async def test_server_credential_does_not_exempt_private_reads(self):
        async with self.client() as client:
            headers = {"X-Mimir-Export-Key": "server-only-test-secret"}
            statuses = [(await client.get("/api/nsn/profile", headers=headers)).status_code for _ in range(3)]
            self.assertEqual(statuses, [200, 200, 429])

    async def test_bulk_limit_and_signup_health_and_options_are_independent(self):
        async with self.client() as client:
            self.assertEqual((await client.post("/api/explorer/page")).status_code, 200)
            self.assertEqual((await client.post("/api/explorer/page")).status_code, 429)
            for _ in range(4):
                self.assertEqual((await client.post("/auth/native-signup")).status_code, 200)
                self.assertEqual((await client.get("/api/dashboard/status")).status_code, 200)
                self.assertEqual((await client.get("/live")).status_code, 200)
            response = await client.options("/api/nsn/profile", headers={"Origin": "https://www.mimiradvisors.org", "Access-Control-Request-Method": "GET"})
            self.assertEqual(response.status_code, 200)


if __name__ == "__main__":
    unittest.main()
