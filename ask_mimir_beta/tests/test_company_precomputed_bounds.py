import unittest

from company_context_store import (
    _bounded_precomputed_context,
    _company_annual_financial_summary,
    _company_forward_context,
    _company_material_site_summary,
    _customer_product_evidence,
    _matching_parent_scope_ids,
)


class CompanyPrecomputedBoundsTests(unittest.TestCase):
    def test_evidence_rows_are_bounded_without_changing_aggregates(self):
        rows = [{"id": index, "contract_ids": [str(index)] * 30} for index in range(6)]
        context = {
            "observed_financials": {"net_prime_obligations_usd": 123.0},
            "identity": {"sites": rows},
            "site_financials": rows,
            "site_capability_evidence": rows,
            "location_footprint": {
                "registered_or_contracting_sites": rows,
                "prime_award_places_of_performance": rows,
                "reported_subaward_locations": rows,
            },
            "place_of_performance_activity": {"records": rows},
            "capability_evidence": {"psc": rows},
            "product_and_part_evidence": {
                "niin_financial_observations": rows,
                "part_number_references": rows,
                "qualified_source_context": {"items": rows},
                "third_party_dla_procurement_routes": rows,
                "summary": {"part_number_reference_count": 6},
            },
            "reported_subcontract_relationships": {
                "as_subcontractor_to": rows,
                "reported_subcontractors": rows,
            },
            "platform_exposure": rows,
            "customer_context": rows,
            "top_awards": rows,
            "open_solicitation_candidates": {"candidates": rows},
            "evidence_index": {"records": rows, "record_count": 6},
        }

        bounded = _bounded_precomputed_context(context, row_limit=2)

        self.assertEqual(bounded["observed_financials"], context["observed_financials"])
        self.assertEqual(len(bounded["identity"]["sites"]), 2)
        self.assertEqual(
            len(bounded["product_and_part_evidence"]["part_number_references"]),
            2,
        )
        self.assertEqual(
            len(
                bounded["product_and_part_evidence"][
                    "third_party_dla_procurement_routes"
                ]
            ),
            2,
        )
        self.assertEqual(
            bounded["product_and_part_evidence"]["summary"][
                "part_number_reference_count"
            ],
            6,
        )
        self.assertEqual(
            len(
                bounded["product_and_part_evidence"][
                    "niin_financial_observations"
                ][0]["contract_ids"]
            ),
            20,
        )
        self.assertEqual(bounded["evidence_index"]["record_count"], 6)
        self.assertEqual(bounded["precomputed_row_limit_per_table"], 2)

    def test_parent_rebuild_replaces_every_matching_legacy_scope(self):
        candidates = [
            {"scope_id": "TRANSDIGM_GROUP_INC"},
            {"scope_id": "PARENT_7ABF4B0054D95B83A7C1"},
        ]
        entries = [
            {
                "scope": {
                    "scope_type": "company_parent",
                    "scope_id": "TRANSDIGM_GROUP_INC",
                    "scope_name": "TRANSDIGM GROUP INCORPORATED",
                }
            },
            {
                "scope": {
                    "scope_type": "company_parent",
                    "scope_id": "PARENT_7ABF4B0054D95B83A7C1",
                    "scope_name": "TRANSDIGM GROUP INCORPORATED",
                }
            },
            {
                "scope": {
                    "scope_type": "company_parent",
                    "scope_id": "UNRELATED_PARENT",
                    "scope_name": "UNRELATED PARENT",
                }
            },
            {"scope": {"scope_type": "company_site", "scope_id": "19645"}},
        ]
        replacement_scope_ids = _matching_parent_scope_ids(
            [candidates[1]],
            "parent_7abf4b0054d95b83a7c1",
            entries,
            "TRANSDIGM GROUP INCORPORATED",
        )

        retained = [
            entry
            for entry in entries
            if not (
                entry.get("scope", {}).get("scope_type") == "company_parent"
                and str(entry.get("scope", {}).get("scope_id") or "").upper()
                in replacement_scope_ids
            )
        ]

        self.assertEqual(len(retained), 2)
        self.assertEqual(
            {entry["scope"]["scope_id"] for entry in retained},
            {"UNRELATED_PARENT", "19645"},
        )

    def test_company_profile_summaries_are_customer_ready(self):
        context = {
            "scope": {
                "fiscal_years": [2025, 2026],
                "observation_window": "FY2025-FY2026 observed records",
            },
            "identity": {
                "sites": [
                    {"cage": "AAAA1", "vendor_name": "Alpha", "city": "A", "state": "VA"},
                    {"cage": "BBBB2", "vendor_name": "Beta", "city": "B", "state": "CA"},
                ]
            },
            "observed_financials": [
                {"measure_type": "prime_obligations", "net_value_usd": 1000.0},
                {"measure_type": "dla_procurement_value", "net_value_usd": 100.0},
            ],
            "annual_activity": [
                {"fiscal_year": 2025, "measure_type": "prime_obligations", "net_value_usd": 600.0},
                {"fiscal_year": 2026, "measure_type": "prime_obligations", "net_value_usd": 400.0},
                {
                    "fiscal_year": 2025,
                    "measure_type": "mimir_modelled_reported_subcontract_value",
                    "net_value_usd": 50.0,
                },
            ],
            "site_financials": [
                {"cage": "AAAA1", "measure_type": "prime_obligations", "net_value_usd": 800.0, "distinct_awards": 2},
                {"cage": "BBBB2", "measure_type": "prime_obligations", "net_value_usd": 200.0, "distinct_awards": 1},
            ],
            "future_demand_context": {
                "programs": [
                    {
                        "program_id": "PROGRAM_A",
                        "company_platform_evidence": [
                            {
                                "evidence_layer": "prime_or_dla_action",
                                "source_system": "USA_SPENDING",
                                "observed_value_usd": 250.0,
                            }
                        ],
                        "budget_projection_rows": [
                            {"fiscal_year": 2027, "measure_type": "net_procurement_p1", "amount_usd": 100.0, "source_document_title": "DoD P-1"},
                            {"fiscal_year": 2031, "measure_type": "net_procurement_p1", "amount_usd": 150.0, "source_document_title": "DoD P-1"},
                        ],
                    }
                ]
            },
        }

        annual = _company_annual_financial_summary(context)
        sites = _company_material_site_summary(context)
        forward = _company_forward_context(context)

        self.assertEqual(annual[0]["net_prime_obligations_usd"], 600.0)
        self.assertIsNone(annual[1]["reported_subcontract_value_usd"])
        self.assertEqual(sites[0]["cage"], "AAAA1")
        self.assertEqual(sites[0]["share_of_company_net_prime_obligations_pct"], 80.0)
        program = forward["programs"][0]
        self.assertEqual(
            program["historical_company_exposure"][
                "share_of_company_prime_obligations_pct"
            ],
            25.0,
        )
        self.assertEqual(program["forward_funding_summary"]["direction"], "growing")
        self.assertEqual(program["forward_funding_summary"]["change_pct"], 50.0)

    def test_product_summary_counts_full_niin_universe_and_ranks_examples(self):
        product = _customer_product_evidence(
            {
                "summary": {"observed_financial_niin_count": 2},
                "qualified_source_context": {
                    "summary": {
                        "target_sole_active_source_niin_count": 3,
                        "target_multi_source_niin_count": 2,
                        "target_not_active_authorized_source_niin_count": 5,
                    }
                },
                "niin_financial_observations": [
                    {"niin": "000000001", "dla_procurement_value_usd": 75.0},
                    {"niin": "000000002", "dla_procurement_value_usd": 25.0},
                ],
            }
        )

        self.assertEqual(product["summary"]["supplier_referenced_niin_count"], 10)
        self.assertEqual(product["summary"]["active_authorized_niin_count"], 5)
        self.assertEqual(
            product["representative_niin_examples"][0][
                "share_of_observed_dla_procurement_pct"
            ],
            75.0,
        )


if __name__ == "__main__":
    unittest.main()
