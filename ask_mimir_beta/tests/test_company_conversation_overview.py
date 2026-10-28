import unittest
import tempfile
import threading
from pathlib import Path
from unittest.mock import Mock

from company_context import CompanyContextBuilder
from company_context_store import CompanyContextStore, LINK_VERSION, FYDP_PLATFORM_LINKAGE_VERSION


class CompanyConversationOverviewTests(unittest.TestCase):
    def test_overview_build_skips_unused_dossier_queries(self):
        builder = CompanyContextBuilder.__new__(CompanyContextBuilder)
        builder._site_identities = Mock(return_value=[{"cage": "45934"}])
        builder._source_manifest = Mock(return_value={"files": {}})
        for name in (
            "_product_and_part_evidence", "_place_of_performance_activity",
            "_missile_program_trajectory", "_location_footprint",
            "_future_demand_context", "_opportunity_candidates", "_quality_summary",
            "_site_capability_evidence", "_top_awards", "_site_financials",
            "_customer_context",
        ):
            setattr(builder, name, Mock(side_effect=AssertionError(name)))
        for name in (
            "_capability_evidence",
            "_network_context", "_platform_exposure", "_financial_summary",
            "_annual_activity",
        ):
            setattr(builder, name, Mock(return_value=[]))
        builder._capability_evidence.return_value = {}
        context = builder._build_context(
            scope_type="company_site", scope_id="45934", scope_name="Example",
            cages=["45934"], fiscal_years=[2025, 2026],
            site_definitions=[], conversation_overview=True,
        )
        self.assertEqual(context["scope"]["scope_id"], "45934")
        self.assertEqual(context["product_and_part_evidence"], {})
        self.assertEqual(context["platform_exposure"], [])
        self.assertEqual(context["site_financials"], [])

    def test_overview_cache_cannot_replace_full_dossier(self):
        with tempfile.TemporaryDirectory() as directory:
            store = CompanyContextStore.__new__(CompanyContextStore)
            store.contexts = []
            store._context_paths = {}
            store._dynamic_contexts = {}
            store._dynamic_overviews = {}
            store._dynamic_groups = {}
            store._dynamic_lock = threading.Lock()
            store._directory_search = Mock(return_value=[{"scope_id": "45934"}])
            store.dynamic_cache_dir = Path(directory)
            store.data_root = Path(directory)
            store._dynamic_builder = Mock()
            store._dynamic_builder.build_site.side_effect = lambda cage, conversation_overview=False: {
                "scope": {"scope_type": "company_site", "scope_id": cage},
                "depth": "overview" if conversation_overview else "full",
            }
            overview = store.get_raw("company_site", "45934", conversation_overview=True)
            full = store.get_raw("company_site", "45934")
            self.assertEqual(overview["depth"], "overview")
            self.assertEqual(full["depth"], "full")
            self.assertTrue((Path(directory) / "company_site-45934-overview.json").exists())
            self.assertTrue((Path(directory) / "company_site-45934.json").exists())

    def test_overview_preserves_financial_summaries_and_labels_omitted_detail(self):
        context = {
            "context_id": "company-test", "evidence_fingerprint": "hash",
            "calculation_version": "v1", "generated_at": "2026-10-09",
            "scope": {"scope_type": "company_parent", "scope_id": "PARENT_TEST",
                      "fiscal_years": [2025, 2026], "observation_window": "FY2025-FY2026"},
            "identity": {"sites": [{"cage": "12345", "vendor_name": "Example Co",
                                     "city": "Dayton", "state": "OH"}],
                         "site_count": 1, "resolved_cages": ["12345"]},
            "observed_financials": [{"measure_type": "prime_obligations", "net_value_usd": 150.0}],
            "annual_activity": [
                {"fiscal_year": 2025, "measure_type": "prime_obligations", "net_value_usd": 100.0},
                {"fiscal_year": 2026, "measure_type": "prime_obligations", "net_value_usd": 50.0},
            ],
            "site_financials": [{"cage": "12345", "measure_type": "prime_obligations",
                                  "net_value_usd": 150.0, "distinct_awards": 2}],
            "site_capability_evidence": [{"cage": "12345", "prime_award_and_dla_examples": [
                {"platform_family": "F-16", "reported_description": "Radar support"}
            ]}],
            "product_and_part_evidence": {
                "third_party_dla_route_summary": {"aggregate_version": "full-universe-v1"},
                "third_party_dla_procurement_routes": [],
            },
            "future_demand_context": {"definition_version": FYDP_PLATFORM_LINKAGE_VERSION},
            "platform_exposure": [], "top_awards": [{"contract_id": "AWARD-1"}],
            "source_manifest": {}, "platform_link_version": LINK_VERSION,
        }
        store = CompanyContextStore.__new__(CompanyContextStore)
        store.get_raw = Mock(return_value=context)

        overview = store.get("company_parent", "PARENT_TEST", "conversation_overview")
        full = store.get("company_parent", "PARENT_TEST", "full_dossier")

        self.assertEqual(overview["annual_financial_summary"], full["annual_financial_summary"])
        self.assertEqual(overview["material_site_summary"], full["material_site_summary"])
        self.assertEqual(overview["observed_financials"], full["observed_financials"])
        self.assertEqual(overview["top_awards"], full["top_awards"])
        self.assertNotIn("site_capability_evidence", overview)
        self.assertIn("site_capability_evidence", overview["evidence_scope"]["omitted_lanes"])
        self.assertEqual(full["site_capability_evidence"][0]["cage"], "12345")

        award_sites = store.get("company_parent", "PARENT_TEST", "conversation_award_sites")
        self.assertEqual(award_sites["site_financials"], full["site_financials"])
        self.assertEqual(award_sites["top_awards"], full["top_awards"])
        self.assertEqual(award_sites["material_site_summary"], full["material_site_summary"])
        self.assertNotIn("product_and_part_evidence", award_sites)
        self.assertEqual(award_sites["evidence_scope"]["depth"], "award_sites")


if __name__ == "__main__":
    unittest.main()
