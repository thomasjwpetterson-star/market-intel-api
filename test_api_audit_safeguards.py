import asyncio
import os
import threading
import unittest
from contextlib import ExitStack
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
from fastapi import HTTPException
from starlette.requests import Request

import main
from test_public_brief_runtime import CAPTURED_UNSUPPORTED_BRIEF


class ReloadAuthorizationTests(unittest.IsolatedAsyncioTestCase):
    def request(self, key=None):
        headers = [] if key is None else [(b"x-mimir-reload-key", key.encode())]
        return Request({"type": "http", "headers": headers})

    async def test_unconfigured_reload_fails_closed_before_scheduling_work(self):
        with patch.dict(os.environ, {"MIMIR_RELOAD_SECRET": ""}), patch.object(main.asyncio, "create_task") as schedule:
            with self.assertRaises(HTTPException) as raised:
                await main.trigger_reload(self.request("arbitrary"))
            self.assertEqual(raised.exception.status_code, 503)
            schedule.assert_not_called()

    async def test_missing_or_wrong_key_cannot_reload(self):
        with patch.dict(os.environ, {"MIMIR_RELOAD_SECRET": "test-service-secret"}), patch.object(main.asyncio, "create_task") as schedule:
            for supplied in (None, "wrong", "non-ascii-é"):
                with self.subTest(supplied=supplied), self.assertRaises(HTTPException) as raised:
                    await main.trigger_reload(self.request(supplied))
                self.assertEqual(raised.exception.status_code, 403)
            schedule.assert_not_called()

    async def test_authorized_reload_schedules_once_and_respects_existing_load(self):
        # Close the coroutine without executing the reload or touching data.
        def discard(coroutine):
            coroutine.close()
        with patch.dict(os.environ, {"MIMIR_RELOAD_SECRET": "test-service-secret"}), patch.object(main, "RELOAD_LOCK", Mock(locked=Mock(return_value=False))), patch.object(main, "GLOBAL_CACHE", {"is_loading": False}), patch.object(main.asyncio, "create_task", side_effect=discard) as schedule:
            result = await main.trigger_reload(self.request("test-service-secret"))
            self.assertEqual(result, {"message": "Reloading..."})
            schedule.assert_called_once()
            main.GLOBAL_CACHE["is_loading"] = True
            result = await main.trigger_reload(self.request("test-service-secret"))
            self.assertEqual(result, {"message": "Reload already running"})
            schedule.assert_called_once()


class CompanySnapshotIsolationTests(unittest.TestCase):
    def snapshot(self, platform_rows, failing_group=None):
        def query(where, params, select_sql, **kwargs):
            group = kwargs.get("group_by_sql")
            if failing_group and group == failing_group:
                raise RuntimeError("simulated module failure")
            if group == "platform_family":
                return pd.DataFrame(platform_rows)
            if group == "sub_agency":
                return pd.DataFrame([{"sub_agency": "DEFENSE LOGISTICS AGENCY", "spend": 500.0}])
            if group == "psc_description":
                return pd.DataFrame([{"psc_description": "ENGINEERING SERVICES", "spend": 500.0}])
            return pd.DataFrame([{"first_year": 2025, "last_year": 2026}])

        def duck_query(sql, *args, **kwargs):
            if "GROUP BY nsn" in sql:
                return pd.DataFrame([{"nsn": "5310001860967", "description": "WASHER", "spend": 500.0}])
            return pd.DataFrame()

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "get_company_profile", return_value={"found": True, "name": "TEST COMPANY", "cage": "6FH39", "total_obligations": 500.0}))
            stack.enter_context(patch.object(main, "get_company_network", return_value={"primes": [], "subs": []}))
            stack.enter_context(patch.object(main, "build_summary_where", return_value=("cage = ?", ["6FH39"])))
            stack.enter_context(patch.object(main, "query_summary_df", side_effect=query))
            stack.enter_context(patch.object(main, "duck_fetch_df", side_effect=duck_query))
            stack.enter_context(patch.object(main, "get_public_intelligence_manifest_entry", return_value=None))
            return main.build_public_company_snapshot(cage="6FH39")

    def assert_independent_sections(self, result):
        self.assertEqual(result["government_customers"][0]["name"], "Defense Logistics Agency")
        self.assertEqual(result["top_capabilities"], ["Engineering Services"])
        self.assertEqual(result["top_nsns"][0]["nsn"], "5310001860967")

    def test_positive_prime_without_valid_platform_keeps_other_sections(self):
        for platform in (None, "", " "):
            with self.subTest(platform=platform):
                result = self.snapshot([{"platform_family": platform, "spend": 500.0}])
                self.assertEqual(result["top_platforms"], [])
                self.assert_independent_sections(result)

    def test_nonpositive_platforms_keep_other_sections(self):
        result = self.snapshot([{"platform_family": "TEST PLATFORM", "spend": -10.0}])
        self.assertEqual(result["top_platforms"], [])
        self.assert_independent_sections(result)

    def test_platform_query_failure_does_not_skip_other_sections(self):
        with self.assertLogs("mimir-api", level="ERROR"):
            result = self.snapshot([], failing_group="platform_family")
        self.assert_independent_sections(result)

    def test_valid_platform_still_reports_prime_obligation_share(self):
        result = self.snapshot([{"platform_family": "TEST PLATFORM", "spend": 125.0}])
        self.assertEqual(result["top_platforms"], [{"name": "TEST PLATFORM", "share": 25.0}])
        self.assert_independent_sections(result)


class LegacyBriefEvidenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        runtime_patch = patch.object(main, "PUBLIC_BRIEF_RUNTIME", main.PublicBriefRuntime())
        runtime_patch.start()
        self.addCleanup(runtime_patch.stop)
        client_patch = patch.object(main.aclient, "with_options", return_value=main.aclient)
        self.provider_options = client_patch.start()
        self.addCleanup(client_patch.stop)

    async def test_database_work_shares_the_service_thread_limit(self):
        entered = threading.Event()
        release = threading.Event()
        thread_ids = []
        event_loop_thread = threading.get_ident()

        def blocked_profile(**kwargs):
            thread_ids.append(threading.get_ident())
            entered.set()
            release.wait(timeout=2)
            return {"found": False}

        async def wait_until_entered():
            while not entered.is_set():
                await asyncio.sleep(0.001)

        limiter = main.anyio.to_thread.current_default_thread_limiter()
        previous_tokens = limiter.total_tokens
        limiter.total_tokens = 1
        tasks = []
        try:
            with patch.object(main, "get_company_profile", side_effect=blocked_profile):
                for index in range(2):
                    request = SimpleNamespace(json=AsyncMock(return_value={"cage": "XXXXX"}))
                    tasks.append(asyncio.create_task(main.generate_unlocked_brief(request)))
                    if index == 0:
                        await asyncio.wait_for(wait_until_entered(), timeout=1)
                # The loop stays responsive while one query occupies the single
                # service worker; the second query must wait for that worker.
                await asyncio.sleep(0.03)
                self.assertEqual(len(thread_ids), 1)
                self.assertNotEqual(thread_ids[0], event_loop_thread)
                release.set()
                results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=2)
                self.assertEqual(len(thread_ids), 2)
                self.assertTrue(all(result["success"] is False for result in results))
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
            limiter.total_tokens = previous_tokens

    async def test_client_financials_cannot_override_server_evidence(self):
        request = SimpleNamespace(json=AsyncMock(return_value={"cage": "6FH39", "name": "FORGED NAME", "prime_exposure": 999999999999, "sub_exposure": 888888888888}))
        completion = AsyncMock(return_value=SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="Observed brief"))]))

        def query(where, params, select_sql, **kwargs):
            if "first_year" in select_sql:
                return pd.DataFrame([{"first_year": 2024, "last_year": 2025}])
            return pd.DataFrame()

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "get_company_profile", return_value={"found": True, "name": "CANONICAL COMPANY", "cage": "6FH39", "total_obligations": 500.0}))
            stack.enter_context(patch.object(main, "build_summary_where", return_value=("cage = ?", ["6FH39"])))
            stack.enter_context(patch.object(main, "query_summary_df", side_effect=query))
            parts = stack.enter_context(patch.object(main, "get_company_parts", return_value=[]))
            stack.enter_context(patch.object(main, "latest_completed_federal_fiscal_year", return_value=2026))
            stack.enter_context(patch.object(main, "get_subset_from_disk", return_value=pd.DataFrame()))
            stack.enter_context(patch.object(main, "get_company_network", return_value={"primes": [{"total": 10, "network_total": 1200}], "subs": []}))
            stack.enter_context(patch.object(main.aclient.chat.completions, "create", completion))
            result = await main.generate_unlocked_brief(request)

        self.assertTrue(result["success"])
        self.assertEqual(result["ai_brief"], "Observed brief")
        self.assertEqual(result["brief_mode"], "generated")
        self.assertEqual(result["methodology"], main.PUBLIC_BRIEF_METHODOLOGY)
        self.assertIn("not available", result["headline_metric"])
        self.assertEqual(result["deep_data"]["nsn_period_label"], "FY2022–FY2026")
        self.assertEqual(parts.call_args.kwargs["years"], [2022, 2023, 2024, 2025, 2026])
        messages = completion.call_args.kwargs["messages"]
        evidence = messages[1]["content"]
        self.assertIn("CANONICAL COMPANY", evidence)
        self.assertIn("FY2024–FY2025", evidence)
        self.assertIn("Observed prime contract value: $500", evidence)
        self.assertNotIn("UNKNOWN", evidence)
        self.assertNotIn("USAspending", evidence)
        self.assertIn("Mimir-adjusted reported subcontract value across all tracked partners: $1.2K", evidence)
        self.assertNotIn("FORGED NAME", evidence)
        self.assertNotIn("999999999999", evidence)
        self.assertNotIn("FY18", evidence)
        self.assertNotIn("Total Mapped Revenue", evidence)
        self.assertIn("Do not assert those conclusions", messages[0]["content"])
        self.provider_options.assert_called_once_with(timeout=20, max_retries=0)

    async def test_provider_failure_preserves_reference_only_preview_and_response_contract(self):
        request = SimpleNamespace(json=AsyncMock(return_value={"cage": "6FH39"}))
        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "get_company_profile", return_value={"found": True, "name": "REFERENCE COMPANY", "cage": "6FH39", "total_obligations": 0.0, "profile_source": "CAGE_REFERENCE_ONLY"}))
            stack.enter_context(patch.object(main, "query_summary_df", return_value=pd.DataFrame()))
            stack.enter_context(patch.object(main, "get_company_parts", return_value=[]))
            stack.enter_context(patch.object(main, "get_subset_from_disk", return_value=pd.DataFrame()))
            stack.enter_context(patch.object(main, "get_company_network", return_value={"primes": [], "subs": []}))
            completion = stack.enter_context(patch.object(main.aclient.chat.completions, "create", AsyncMock(side_effect=RuntimeError("private provider diagnostic"))))
            with self.assertLogs("mimir-api", level="WARNING"):
                result = await main.generate_unlocked_brief(request)
        self.assertEqual(set(result), {"success", "ai_brief", "brief_mode", "methodology", "headline_metric", "deep_data"})
        self.assertEqual(result["brief_mode"], "evidence")
        self.assertIs(result["success"], True)
        self.assertEqual(set(result["deep_data"]), {"platforms", "agencies", "nsns", "nsn_period_label", "contracts", "network", "network_title"})
        self.assertIn("prime contract values are not available", result["ai_brief"])
        self.assertNotIn("$0", result["ai_brief"])
        self.assertNotIn("private provider diagnostic", result["ai_brief"])
        self.assertNotIn(main.PUBLIC_BRIEF_METHODOLOGY, result["ai_brief"])
        self.assertEqual(result["methodology"], main.PUBLIC_BRIEF_METHODOLOGY)
        evidence = completion.call_args.kwargs["messages"][1]["content"]
        self.assertIn("Unavailable in the loaded financial records", evidence)
        self.assertNotIn("$0", evidence)

    async def test_captured_unsupported_source_claim_returns_evidence_fallback(self):
        request = SimpleNamespace(json=AsyncMock(return_value={"cage": "6FH39"}))
        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "get_company_profile", return_value={"found": True, "name": "DOMMES CONSULTING INC", "cage": "6FH39", "total_obligations": 500.0}))
            stack.enter_context(patch.object(main, "query_summary_df", return_value=pd.DataFrame()))
            stack.enter_context(patch.object(main, "get_company_parts", return_value=[]))
            stack.enter_context(patch.object(main, "get_subset_from_disk", return_value=pd.DataFrame()))
            stack.enter_context(patch.object(main, "get_company_network", return_value={"primes": [], "subs": []}))
            completion = stack.enter_context(patch.object(main.aclient.chat.completions, "create", AsyncMock(return_value=SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=CAPTURED_UNSUPPORTED_BRIEF))]))))
            with self.assertLogs("mimir-api", level="WARNING"):
                first = await main.generate_unlocked_brief(request)
            second = await main.generate_unlocked_brief(request)
        for result in (first, second):
            self.assertIs(result["success"], True)
            self.assertEqual(result["brief_mode"], "evidence")
            self.assertIn("$500 in observed prime contract value", result["ai_brief"])
            self.assertNotIn("UNKNOWN", result["ai_brief"])
            self.assertNotIn("source composition", result["ai_brief"])
            self.assertNotIn("This value is derived from", result["ai_brief"])
            self.assertNotIn(main.PUBLIC_BRIEF_METHODOLOGY, result["ai_brief"])
            self.assertEqual(result["methodology"], main.PUBLIC_BRIEF_METHODOLOGY)
            self.assertIn("deep_data", result)
        completion.assert_awaited_once()

    async def test_compact_presentation_preserves_precise_data_and_cache_identity(self):
        request = SimpleNamespace(json=AsyncMock(return_value={"cage": "81755"}))
        value = 175766233950.51

        def query(where, params, select_sql, **kwargs):
            if kwargs.get("group_by_sql") == "sub_agency":
                return pd.DataFrame([{"sub_agency": "AIR FORCE", "spend": value}])
            if not kwargs.get("group_by_sql"):
                return pd.DataFrame([{"first_year": 2018, "last_year": 2026}])
            return pd.DataFrame()

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "get_company_profile", side_effect=[
                {"found": True, "name": "TEST COMPANY", "cage": "81755", "total_obligations": amount}
                for amount in (value, value + 0.01)]))
            stack.enter_context(patch.object(main, "query_summary_df", side_effect=query))
            stack.enter_context(patch.object(main, "get_company_parts", return_value=[]))
            stack.enter_context(patch.object(main, "get_subset_from_disk", return_value=pd.DataFrame()))
            stack.enter_context(patch.object(main, "get_company_network", return_value={"primes": [{"total": 7090483.20, "network_total": 7090483.20}], "subs": []}))
            completion = stack.enter_context(patch.object(main.aclient.chat.completions, "create", AsyncMock(return_value=SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="**Position:** $175,766,233,950.51 in prime contract value; separately $7,090,483.20 in tracked subcontract value."))]))))
            first = await main.generate_unlocked_brief(request)
            second = await main.generate_unlocked_brief(request)
        for result in (first, second):
            self.assertTrue(result["success"])
            self.assertEqual(result["ai_brief"], "**Position:** $175.8B in prime contract value; separately $7.1M in tracked subcontract value.")
            self.assertEqual(result["headline_metric"], "Top awarding agency: Air Force ($175.8B; FY2018–FY2026).")
            self.assertEqual(result["deep_data"]["agencies"][0]["spend"], value)
            self.assertEqual(result["deep_data"]["network"][0]["network_total"], 7090483.20)
            self.assertNotIn("methodology", result["ai_brief"])
            self.assertNotIn("UNKNOWN", result["ai_brief"])
        self.assertEqual(completion.await_count, 2)
        # The displayed evidence rounds identically, but raw evidence changed.
        self.assertEqual(completion.call_args_list[0].kwargs["messages"], completion.call_args_list[1].kwargs["messages"])

    async def test_repeated_evidence_reuses_inference_but_refreshes_deep_data(self):
        request = SimpleNamespace(json=AsyncMock(return_value={"cage": "6FH39", "name": "IGNORED CLIENT LABEL"}))
        profiles = [{"found": True, "name": "CANONICAL COMPANY", "cage": "6FH39", "total_obligations": value}
                    for value in (500.0, 500.0, 1000.0)]
        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "get_company_profile", side_effect=profiles))
            stack.enter_context(patch.object(main, "query_summary_df", return_value=pd.DataFrame()))
            stack.enter_context(patch.object(main, "get_company_parts", side_effect=[
                [{"nsn": "5310001860967", "description": "First part"}],
                [{"nsn": "5310001860967", "description": "Fresh part"}],
                [],
            ]))
            stack.enter_context(patch.object(main, "get_subset_from_disk", return_value=pd.DataFrame()))
            stack.enter_context(patch.object(main, "get_company_network", return_value={"primes": [], "subs": []}))
            completion = stack.enter_context(patch.object(main.aclient.chat.completions, "create", AsyncMock(return_value=SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="Generated brief"))]))))
            first = await main.generate_unlocked_brief(request)
            second = await main.generate_unlocked_brief(request)
            self.assertTrue(first["success"] and second["success"])
            self.assertEqual(second["deep_data"]["nsns"][0]["desc"], "Fresh Part")
            self.assertEqual(first["deep_data"]["nsns"][0]["desc"], "First Part")
            completion.assert_awaited_once()
            third = await main.generate_unlocked_brief(request)
            self.assertTrue(third["success"])
            self.assertEqual(completion.await_count, 2)
            self.assertIn("Observed prime contract value: $1.0K", completion.call_args.kwargs["messages"][1]["content"])

    async def test_unknown_company_does_not_generate_a_brief(self):
        request = SimpleNamespace(json=AsyncMock(return_value={"cage": "XXXXX"}))
        with patch.object(main, "get_company_profile", return_value={"found": False}), patch.object(main.aclient.chat.completions, "create", new_callable=AsyncMock) as completion:
            result = await main.generate_unlocked_brief(request)
        self.assertFalse(result["success"])
        completion.assert_not_awaited()

    def test_completed_federal_fiscal_year_changes_on_october_first(self):
        self.assertEqual(main.latest_completed_federal_fiscal_year(date(2026, 9, 30)), 2025)
        self.assertEqual(main.latest_completed_federal_fiscal_year(date(2026, 10, 1)), 2026)
        self.assertEqual(main.latest_completed_federal_fiscal_year(date(2027, 1, 1)), 2026)


class CompanyProfileContractTests(unittest.TestCase):
    def test_existing_award_backed_child_paths_supply_brief_identity(self):
        cache = {"profiles_df": pd.DataFrame([{
            "cage_code": "6FH39", "vendor_name": "CANONICAL COMPANY",
            "total_lifetime_spend": 500.0,
        }])}
        with patch.object(main, "GLOBAL_CACHE", cache), patch.object(main, "_calc_child_kpis_from_kpis_disk", return_value={"has_kpis": False}), patch.object(main, "get_parent_aggregate_stats", return_value=None):
            for lookup in ({"cage": "6fh39"}, {"name": "Canonical Company"}):
                with self.subTest(lookup=lookup):
                    result = main.get_company_profile(**lookup, years=None)
                    self.assertIs(result["found"], True)
                    self.assertEqual(result["cage"], "6FH39")
                    self.assertEqual(result["name"], "CANONICAL COMPANY")
                    self.assertEqual(result["total_obligations"], 500.0)

    def test_reference_only_and_parent_profiles_are_accepted_without_fake_financials(self):
        reference = pd.DataFrame([{"cage_code": "6FH39", "vendor_name": "REFERENCE COMPANY"}])
        with patch.object(main, "GLOBAL_CACHE", {}), patch.object(main, "duck_fetch_df", return_value=reference):
            result = main.get_company_profile(cage="6FH39", years=None)
            self.assertIs(result["found"], True)
            self.assertEqual(result["profile_source"], "CAGE_REFERENCE_ONLY")
            self.assertEqual(result["total_obligations"], 0.0)
        stats = {"total_obligations": 700.0, "total_contracts": 2,
                 "last_active": 2026, "top_naics": [], "top_platforms": []}
        with patch.object(main, "GLOBAL_CACHE", {}), patch.object(main, "get_parent_aggregate_stats", return_value=stats):
            result = main.get_company_profile(name="Parent Company", years=None)
            self.assertIs(result["found"], True)
            self.assertEqual(result["cage"], "AGGREGATE")
            self.assertEqual(result["total_obligations"], 700.0)


class PublicNsnFallbackTests(unittest.TestCase):
    def test_supply_uncertainty_and_source_dates_survive_fallback(self):
        for below in (None, False, True):
            with self.subTest(below_reorder_point=below), ExitStack() as stack:
                supply = {
                    "below_reorder_point": below,
                    "reorder_assessment_stock": 13.0,
                    "inventory_snapshot_date": "2026-09-10",
                    "reorder_point_snapshot_date": "2026-09-09",
                    "forecast_start_month": "2026-09-01",
                    "source_retrieval_date": "2026-09-24",
                    "source_release": "release-test",
                    "source_product": "SOURCE-TEST",
                }
                stack.enter_context(patch.object(main, "get_nsn_profile", return_value={"found": True, "fsc_code": "5310", "supply_state": supply}))
                stack.enter_context(patch.object(main, "nsn_ref_profile_lookup", return_value={}))
                stack.enter_context(patch.object(main, "nsn_ref_supplier_lookup", return_value={}))
                stack.enter_context(patch.object(main, "get_nsn_platforms", return_value=[]))
                stack.enter_context(patch.object(main, "duck_fetch_df", return_value=pd.DataFrame()))
                result = main.build_public_nsn_snapshot("5310001860967")
                teaser = result["demand_supply_teaser"]
                self.assertIs(teaser["below_reorder_point"], below)
                for key, value in supply.items():
                    self.assertEqual(teaser[key], value)


if __name__ == "__main__":
    unittest.main()
