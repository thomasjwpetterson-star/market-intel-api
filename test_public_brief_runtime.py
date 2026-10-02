import asyncio
from copy import deepcopy
import unittest
from unittest.mock import AsyncMock

from public_brief_runtime import PublicBriefRuntime, brief_cache_key, evidence_only_brief


class BriefCacheKeyTests(unittest.TestCase):
    def test_key_tracks_evidence_instructions_model_and_settings(self):
        payload = {"model": "model-a", "temperature": 0.2, "max_tokens": 400,
                   "messages": [{"role": "system", "content": "Use evidence"},
                                {"role": "user", "content": "Company A: $10"}]}
        key = brief_cache_key(payload)
        self.assertEqual(len(key), 64)
        self.assertEqual(key, brief_cache_key(dict(reversed(list(payload.items())))))
        for field, value in (("model", "model-b"), ("temperature", 0.4), ("max_tokens", 200)):
            changed = {**payload, field: value}
            self.assertNotEqual(key, brief_cache_key(changed))
        for index in (0, 1):
            changed = deepcopy(payload)
            changed["messages"][index]["content"] += " changed"
            self.assertNotEqual(key, brief_cache_key(changed))

    def test_fallback_preserves_unknown_coverage_and_separate_measures(self):
        unknown = evidence_only_brief(name="Reference Company", prime_value=None,
                                      prime_period=None, sub_value=None, sub_basis="Tracked awards",
                                      agency_summary="Agency information is unavailable.")
        self.assertNotIn("$0", unknown)
        self.assertIn("prime contract-value coverage is unavailable", unknown)
        self.assertIn("subcontract-award breakdown is unavailable", unknown)
        self.assertIn("evidence-only", unknown)
        observed = evidence_only_brief(name="Company", prime_value=0.0, prime_period="FY2024–FY2025",
                                       sub_value=100.0, sub_basis="Displayed partners only",
                                       agency_summary="Agency information is unavailable.")
        self.assertIn("$0.00 in observed prime contract value", observed)
        self.assertIn("USAspending net obligations and DLA procurement-line values", observed)
        self.assertIn("can overlap and should not be added together", observed)
        self.assertIn("FY2024–FY2025", observed)
        self.assertIn("Displayed partners only: $100.00, reported separately", observed)


class PublicBriefRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_identical_concurrent_requests_share_one_nonblocking_generation(self):
        runtime = PublicBriefRuntime()
        entered = asyncio.Event()
        release = asyncio.Event()

        async def slow_provider():
            entered.set()
            await release.wait()
            return "Generated brief"

        provider = AsyncMock(side_effect=slow_provider)
        first = asyncio.create_task(runtime.get_or_generate("same", provider, "fallback"))
        await asyncio.wait_for(entered.wait(), timeout=1)
        second = asyncio.create_task(runtime.get_or_generate("same", provider, "fallback"))
        # This scheduled callback must execute while generation is still waiting.
        tick = asyncio.Event()
        asyncio.get_running_loop().call_soon(tick.set)
        await asyncio.wait_for(tick.wait(), timeout=1)
        self.assertFalse(first.done())
        self.assertFalse(second.done())
        provider.assert_awaited_once()
        release.set()
        self.assertEqual(await asyncio.gather(first, second), ["Generated brief"] * 2)
        self.assertEqual(await runtime.get_or_generate("same", provider, "fallback"), "Generated brief")
        provider.assert_awaited_once()

    async def test_busy_distinct_request_uses_fallback_without_queue_or_provider_call(self):
        runtime = PublicBriefRuntime(max_concurrent=1)
        entered, release = asyncio.Event(), asyncio.Event()

        async def slow_provider():
            entered.set()
            await release.wait()
            return "first"

        first = asyncio.create_task(runtime.get_or_generate("first", slow_provider, "fallback"))
        await asyncio.wait_for(entered.wait(), timeout=1)
        other_provider = AsyncMock(return_value="second")
        self.assertEqual(await asyncio.wait_for(runtime.get_or_generate("other", other_provider, "source summary"), timeout=0.1), "source summary")
        other_provider.assert_not_awaited()
        release.set()
        await first
        self.assertEqual(await runtime.get_or_generate("other", other_provider, "source summary"), "second")

    async def test_cache_ttl_and_size_are_bounded(self):
        now = [100.0]
        runtime = PublicBriefRuntime(max_entries=2, ttl_seconds=10, clock=lambda: now[0])
        provider = AsyncMock(return_value="brief")
        for key in ("one", "two", "three"):
            await runtime.get_or_generate(key, provider, "fallback")
        self.assertEqual(len(runtime._cache), 2)
        self.assertNotIn("one", runtime._cache)
        await runtime.get_or_generate("three", provider, "fallback")
        self.assertEqual(provider.await_count, 3)
        now[0] += 10
        await runtime.get_or_generate("three", provider, "fallback")
        self.assertEqual(provider.await_count, 4)

    async def test_failed_call_cools_down_then_retries_without_logging_request_text(self):
        now = [100.0]
        runtime = PublicBriefRuntime(failure_cooldown_seconds=5, clock=lambda: now[0])
        provider = AsyncMock(side_effect=[RuntimeError("PRIVATE PROMPT TEXT"), "recovered"])
        with self.assertLogs("mimir-api", level="WARNING") as captured:
            self.assertEqual(await runtime.get_or_generate("key", provider, "source summary"), "source summary")
        self.assertNotIn("PRIVATE PROMPT TEXT", " ".join(captured.output))
        self.assertEqual(await runtime.get_or_generate("key", provider, "fresh fallback"), "fresh fallback")
        provider.assert_awaited_once()
        now[0] += 5
        self.assertEqual(await runtime.get_or_generate("key", provider, "fallback"), "recovered")
        self.assertEqual(provider.await_count, 2)
        self.assertEqual(runtime._inflight, {})

    async def test_provider_timeout_returns_fallback_and_releases_capacity(self):
        cancelled = asyncio.Event()

        async def never_finishes():
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        runtime = PublicBriefRuntime(timeout_seconds=0.01, max_concurrent=1)
        with self.assertLogs("mimir-api", level="WARNING"):
            result = await asyncio.wait_for(runtime.get_or_generate("timeout", never_finishes, "fallback"), timeout=0.5)
        self.assertEqual(result, "fallback")
        self.assertTrue(cancelled.is_set())
        self.assertEqual(runtime._inflight, {})
        self.assertEqual(await runtime.get_or_generate("next", AsyncMock(return_value="next brief"), "fallback"), "next brief")

    async def test_disconnected_waiter_cannot_cancel_shared_generation(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def slow_provider():
            entered.set()
            await release.wait()
            return "shared brief"

        runtime = PublicBriefRuntime()
        provider = AsyncMock(side_effect=slow_provider)
        first = asyncio.create_task(runtime.get_or_generate("same", provider, "fallback"))
        await entered.wait()
        second = asyncio.create_task(runtime.get_or_generate("same", provider, "fallback"))
        await asyncio.sleep(0)
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        self.assertFalse(second.done())
        release.set()
        self.assertEqual(await second, "shared brief")
        provider.assert_awaited_once()

    async def test_empty_and_oversized_results_use_short_failure_cooldown(self):
        for value in (None, "", "  ", "x" * 21):
            with self.subTest(value=value):
                runtime = PublicBriefRuntime(max_text_chars=20)
                provider = AsyncMock(return_value=value)
                with self.assertLogs("mimir-api", level="WARNING"):
                    self.assertEqual(await runtime.get_or_generate("key", provider, "fallback"), "fallback")
                self.assertIsNone(runtime._cache["key"][1])
                self.assertEqual(await runtime.get_or_generate("key", provider, "fallback"), "fallback")
                provider.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
