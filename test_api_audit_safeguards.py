import os
import unittest
from contextlib import ExitStack
from unittest.mock import Mock, patch

import pandas as pd
from fastapi import HTTPException
from starlette.requests import Request

import main


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
