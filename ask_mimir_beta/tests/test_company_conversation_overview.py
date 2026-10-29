import unittest
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from company_context import CompanyContextBuilder
from company_quick_context import build_site_conversation, compact_company_conversation_pack
from company_context_store import CompanyContextStore, LINK_VERSION, FYDP_PLATFORM_LINKAGE_VERSION


class CompanyConversationOverviewTests(unittest.TestCase):
    def test_quick_site_keeps_signed_lanes_and_subcontract_count_separate(self):
        transactions = [
            {"source_system": "USA_SPENDING", "award_key": "A1", "transaction_key": "T1",
             "contract_id": "C1", "action_date": "2026-04-01", "year": 2026,
             "spend_amount": 100.0, "platform_family": "F-16",
             "base_award_description": "Radar control", "description": "Radar control"},
            {"source_system": "USA_SPENDING", "award_key": "A1", "transaction_key": "T2",
             "contract_id": "C1", "action_date": "2026-05-01", "year": 2026,
             "spend_amount": -20.0, "platform_family": "F-16",
             "base_award_description": "Radar control", "description": "Radar control"},
            {"source_system": "DLA", "award_key": "D1", "transaction_key": "T3",
             "contract_id": "D1", "action_date": "2026-06-01", "year": 2026,
             "spend_amount": 30.0, "platform_family": "F-16", "niin": "123456789",
             "nsn": "1560-00-123-4567", "description": "Power module"},
        ]
        network = [
            {"prime_cage": "22222", "prime_name": "Prime", "contract_id": "P1",
             "award_key": "PA1", "source_dedup_key": "S1", "subaward_value": 40.0,
             "subaward_value_raw": 40.0, "action_date": "2026-02-01",
             "year": 2026, "platform_family": "F-16", "description": "Control unit"},
            {"prime_cage": "22222", "prime_name": "Prime", "contract_id": "P1",
             "award_key": "PA1", "source_dedup_key": "S2", "subaward_value": -5.0,
             "subaward_value_raw": -5.0, "action_date": "2026-03-01",
             "year": 2026, "platform_family": "F-16", "description": "Control unit"},
        ]
        builder = SimpleNamespace(
            identity={"version": "test", "parents": []},
            paths={"transactions": Path("transactions.parquet"), "network": Path("network.parquet")},
            connection=SimpleNamespace(execute=Mock(return_value=object())),
            _site_identities=Mock(return_value=[{"cage": "45934", "vendor_name": "Example", "city": "Dayton", "state": "OH"}]),
            _source_manifest=Mock(return_value={"files": {}}),
            _evidence_index=CompanyContextBuilder._evidence_index,
        )
        with patch("company_quick_context.rows_as_dicts", side_effect=[transactions, network]):
            context = build_site_conversation(builder, "45934", [2026])
        totals = {row["measure_type"]: row["net_value_usd"] for row in context["observed_financials"]}
        self.assertEqual(totals, {"prime_obligations": 80.0, "dla_procurement_value": 30.0})
        sub = next(row for row in context["annual_activity"] if row["measure_type"] == "mimir_modelled_reported_subcontract_value")
        self.assertEqual(sub["net_value_usd"], 35.0)
        self.assertEqual(sub["distinct_awards"], 1)
        self.assertEqual(context["top_awards"][0]["net_prime_obligations_usd"], 80.0)
        self.assertEqual(context["reported_subcontract_relationships"]["as_subcontractor_to"][0]["mimir_modelled_subcontract_value_usd"], 35.0)
        model_pack = compact_company_conversation_pack(context)
        self.assertEqual(model_pack["observed_financials"][0]["net_value_usd"], 80.0)
        self.assertEqual(model_pack["prime_awards"][0]["contract_id"], "C1")
        self.assertEqual(model_pack["dla_items"][0]["niin"], "123456789")
        self.assertEqual(model_pack["reported_subcontract_routes"][0]["mimir_modelled_subcontract_value_usd"], 35.0)
        self.assertNotIn("evidence_index", model_pack)

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
            with patch(
                "company_quick_context.build_site_conversation",
                return_value={
                    "scope": {"scope_type": "company_site", "scope_id": "45934"},
                    "depth": "overview",
                },
            ):
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
