import asyncio
from copy import deepcopy
import unittest
import json
from pathlib import Path
from unittest.mock import AsyncMock

from public_brief_runtime import (
    PUBLIC_BRIEF_METHODOLOGY, PublicBriefRuntime, brief_cache_key,
    brief_evidence_context, evidence_only_brief, format_brief_currency, validate_brief_narrative,
)


CAPTURED_UNSUPPORTED_BRIEF = (
    "**Position:** DOMMES CONSULTING INC has an observed prime contract value of $500.00 during the fiscal years 2025 to 2026. "
    "This value is derived from USAspending net obligations and DLA procurement-line values, though the specific source split is not provided. "
    "The Mimir-adjusted reported subcontract value is $7,090,483.20, but the observation period for this measure is not established.\n\n"
    "**Dependency:** The awarding agency for the observed prime contract is the Department of the Navy, with a contract classification in engineering services valued at $500.00. "
    "There are no Mimir platform mappings associated with the observed prime contract value.\n\n"
    "**Implication:** The data provided is limited to specific observations and does not imply comprehensive coverage or current activity. "
    "Before making any commercial decisions, it is crucial to verify the information through direct source confirmation. "
    "The concentration of agency awards does not inherently indicate sole-source status or predict future demand."
)


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
        evidence = {"identity": {"name": "Reference Company", "cage": "6FH39"},
                    "prime_value": None, "prime_period": None, "sub_value": None,
                    "sub_basis": "Tracked awards"}
        unknown = evidence_only_brief(evidence)
        self.assertNotIn("$0", unknown)
        self.assertIn("prime contract values are not available", unknown.lower())
        self.assertNotIn("UNKNOWN", unknown)
        self.assertNotIn("methodology", unknown)
        observed = evidence_only_brief({**evidence, "prime_value": 0.0, "prime_period": "FY2024–FY2025",
                                       "sub_value": 100.0, "sub_basis": "Displayed partners only"})
        self.assertIn("$0 in observed prime contract value", observed)
        self.assertNotIn("source composition", observed)
        self.assertNotIn("revenue", observed)
        self.assertIn("FY2024–FY2025", observed)
        self.assertIn("Tracked subcontract value as a subcontractor totals $100 from displayed partners", observed)
        self.assertNotIn(PUBLIC_BRIEF_METHODOLOGY, observed)

    def test_fact_rich_fallbacks_have_distinct_style_and_preserve_site_and_roles(self):
        evidence = self.rich_evidence()
        public = evidence_only_brief(evidence)
        dashboard = evidence_only_brief(evidence, "operational_profile")
        self.assertEqual(len(public.split("\n\n")), 2)
        self.assertNotIn("\n", dashboard)
        self.assertEqual(public.replace("\n\n", " "), dashboard)
        for text in (public, dashboard):
            for fact in ("CAGE 6FH39", "Arlington, VA", "$500", "N001-TEST",
                         "Technical engineering support", "5310001860967", "Washers", "Prime Customer",
                         "Component Supplier", "Mimir platform mappings include F-35"):
                self.assertIn(fact, text)
            for absent in ("**Position:", "**Dependency:", "**Implication:", "qualify sales", "sole-source", "SHIP-ARBITRARY"):
                self.assertNotIn(absent, text)
        parent = {**evidence, "identity": {"name": "Parent", "scope": "corporate aggregate", "city": "Wrong child city", "cage": "AGGREGATE"}}
        parent_text = evidence_only_brief(parent)
        self.assertIn("Parent is shown as a corporate aggregate", parent_text)
        self.assertNotIn("Wrong child city", parent_text)
        self.assertNotIn("CAGE AGGREGATE", parent_text)

    @staticmethod
    def rich_evidence():
        return {
            "identity": {"name": "Site Company", "cage": "6FH39", "city": "Arlington", "state": "VA", "scope": "CAGE site record"},
            "activity": {"contract_count": 1}, "prime_value": 500.0, "prime_period": "FY2025–FY2026",
            "sub_value": 7090483.2, "sub_basis": "Mimir-adjusted reported subcontract value",
            "capabilities": [{"name": "Engineering Services", "spend": 500.0}],
            "agencies": [{"name": "Department Of The Navy", "spend": 500.0}],
            "platforms": [{"platform_family": "F-35", "spend": 300.0}],
            "contracts": [{"contract_id": "N001-TEST", "agency": "Department Of The Navy", "desc": "Technical engineering support", "spend": 500.0, "date": "2026-02-01"}],
            "nsns": [{"nsn": "5310001860967", "desc": "Washers", "spend": 200.0}], "nsn_period": "FY2022–FY2026",
            "prime_customers": [{"name": "Prime Customer", "total": 7090483.2, "platform": "SHIP-ARBITRARY"}],
            "subcontractors": [{"name": "Component Supplier", "total": 50.0}],
        }

    def test_context_keeps_rankings_independent_and_omits_arbitrary_partner_platform(self):
        evidence = self.rich_evidence()
        before = deepcopy(evidence)
        text = brief_evidence_context(evidence)
        self.assertEqual(evidence, before)
        for fact in ("independent_agency_ranking", "independent_Mimir_platform_mapping_ranking", "largest_observed_award_actions_not_recent_ranking",
                     "reported_upstream_prime_customers", "reported_downstream_subcontractors", "Technical engineering support", "$7.1M"):
            self.assertIn(fact, text)
        self.assertNotIn("SHIP-ARBITRARY", text)
        for style in ("public", "operational_profile"):
            with self.assertRaises(ValueError):
                validate_brief_narrative("**Position:** Generic old format", brief_style=style)

    def test_quality_gate_rejects_short_unsupported_claims_and_style_overflow(self):
        for text in ("This site is a major player.", "The agency indicates a strong focus on naval aviation projects.",
                     "The company is producing components.", "It supplies switch assemblies.",
                     "Operations include manufacturing in hardware.", "It has a $14.1B contract.",
                     "It has a $14.1B contract value.", "It has a contract worth $14.1B."):
            with self.subTest(text=text), self.assertRaises(ValueError):
                validate_brief_narrative(text, brief_style="public")
        with self.assertRaises(ValueError):
            validate_brief_narrative(" ".join(["Record facts"] * 100), brief_style="public")
        five = "One record. Two records. Three records. Four records. Five records."
        self.assertEqual(validate_brief_narrative(five, brief_style="public"), five)
        with self.assertRaises(ValueError):
            validate_brief_narrative(five, brief_style="operational_profile")
        factual = 'A $14.1B contract record describes aircraft long lead items. Item records include switch assemblies.'
        self.assertEqual(validate_brief_narrative(factual, brief_style="operational_profile"), factual)

    def test_currency_display_is_compact_with_signs_and_small_values_preserved(self):
        for amount, expected in ((175766233950.51, "$175.8B"), (7090483.20, "$7.1M"),
                                 (500, "$500"), (0, "$0"), (12.34, "$12.34"),
                                 (-1234567, "-$1.2M"), (999999, "$1.0M")):
            with self.subTest(amount=amount):
                self.assertEqual(format_brief_currency(amount), expected)
        for invalid in (float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                format_brief_currency(invalid)
        self.assertEqual(validate_brief_narrative("Value: $175,766,233,950.51; separately $7,090,483.20. Net change: -$1,200.00."),
                         "Value: $175.8B; separately $7.1M. Net change: -$1.2K.")
        self.assertEqual(validate_brief_narrative("$500, alongside $7.1M."), "$500, alongside $7.1M.")
        self.assertEqual(validate_brief_narrative("$7.1 million; $1000 million; $175.8 B; $-7,000; -$7,000."),
                         "$7.1M; $1.0B; $175.8B; -$7.0K; -$7.0K.")

    def test_source_guard_rejects_captured_claim_and_format_variants(self):
        for narrative in (CAPTURED_UNSUPPORTED_BRIEF,
                          "Its value includes USA-spending records.",
                          "This is based on U.S.A. Spending data.",
                          "DLA procurement\u2011line values contributed to this amount.",
                          "The amount equals net_price times ordered_quantity.",
                          "Company source composition is UNKNOWN.",
                          "The source-specific split is unavailable."):
            with self.subTest(narrative=narrative), self.assertRaises(ValueError):
                validate_brief_narrative(narrative)

    def test_source_guard_allows_agencies_without_methodology_prose(self):
        for narrative in (
            "**Dependency:** Defense Logistics Agency is the largest observed awarding agency ($100).",
            "**Dependency:** DLA appears in the supplied agency mix. This does not establish sole-source status.",
            "**Position:** The observed prime contract value is $0. Subcontract values are not available.",
        ):
            self.assertEqual(validate_brief_narrative(narrative), narrative)

    def test_narrative_budget_and_policy_change_are_part_of_safeguards(self):
        self.assertEqual(validate_brief_narrative("x" * 6000), "x" * 6000)
        with self.assertRaises(ValueError):
            validate_brief_narrative("x" * 6001)
        key = brief_cache_key({"completion": {}, "source_policy": "v1", "methodology": PUBLIC_BRIEF_METHODOLOGY})
        self.assertNotEqual(key, brief_cache_key({"completion": {}, "source_policy": "v2", "methodology": PUBLIC_BRIEF_METHODOLOGY}))
        self.assertNotEqual(key, brief_cache_key({"completion": {}, "source_policy": "v1", "methodology": "Updated note"}))


class PublicBriefRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_both_captured_live_responses_are_rejected_and_use_factual_fallback(self):
        fixture = json.loads((Path(__file__).parent / "test_fixtures/site_brief_rejected_outputs.json").read_text())
        for row in fixture["captured_outputs"]:
            with self.subTest(style=row["style"]):
                runtime = PublicBriefRuntime()
                fallback = evidence_only_brief(fixture["evidence"], row["style"])
                async def provider():
                    return validate_brief_narrative(row["text"], brief_style=row["style"])
                with self.assertLogs("mimir-api", level="WARNING"):
                    actual = await runtime.get_or_generate(row["style"], provider, fallback)
                self.assertEqual(actual, fallback)
                self.assertLessEqual(len(actual.split()), 160)
                self.assertNotIn("major player", actual)
                self.assertNotIn("naval aviation", actual)
                self.assertNotIn("producing", actual)
                self.assertIn("contract record", actual)
                self.assertIn("as a subcontractor totals $442.8M", actual)
                self.assertIn("separately, reported downstream subcontractors", actual)
                self.assertIn("Item records (FY2022–FY2026)", actual)
                self.assertEqual(actual.count("\n\n"), 1 if row["style"] == "public" else 0)

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
