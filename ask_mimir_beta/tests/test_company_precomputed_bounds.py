import unittest

from company_context_store import _bounded_precomputed_context


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


if __name__ == "__main__":
    unittest.main()
