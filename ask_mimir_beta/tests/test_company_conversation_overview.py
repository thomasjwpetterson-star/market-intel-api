import unittest
from unittest.mock import Mock

from company_context_store import CompanyContextStore, LINK_VERSION, FYDP_PLATFORM_LINKAGE_VERSION


class CompanyConversationOverviewTests(unittest.TestCase):
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
