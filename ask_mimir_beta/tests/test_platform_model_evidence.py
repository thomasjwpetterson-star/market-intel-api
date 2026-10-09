import copy
import unittest

from platform_model_evidence import (
    compact_platform_model_evidence,
    expand_platform_model_evidence,
)


class PlatformModelEvidenceTests(unittest.TestCase):
    def test_supplier_years_round_trip_without_changing_full_dossier(self):
        pack = {
            "scope": {"platform_id": "AMRAAM"},
            "reported_supplier_sites": [
                {
                    "cage": "12345",
                    "supplier_name": "Example supplier",
                    "annual_reported_subcontract_activity": [
                        {
                            "cage": "12345", "fiscal_year": 2025,
                            "mimir_modelled_reported_subcontract_value_usd": 100.0,
                            "source_reported_value_usd": 110.0,
                            "selected_report_count": 2,
                            "prime_award_count": 1,
                        },
                        {
                            "cage": "12345", "fiscal_year": 2026,
                            "mimir_modelled_reported_subcontract_value_usd": -25.0,
                            "source_reported_value_usd": -25.0,
                            "selected_report_count": 0,
                            "prime_award_count": 0,
                            "observation_status": "NOT_OBSERVED",
                        },
                    ],
                },
                {
                    "cage": "54321",
                    "annual_reported_subcontract_activity": [{
                        "cage": "DIFFERENT", "fiscal_year": 2026,
                        "mimir_modelled_reported_subcontract_value_usd": 5,
                        "source_reported_value_usd": 5,
                        "selected_report_count": 1,
                        "prime_award_count": 1,
                    }],
                },
            ],
        }
        original = copy.deepcopy(pack)
        compact = compact_platform_model_evidence(pack)
        self.assertEqual(pack, original)
        self.assertEqual(compact["supplier_annual_activity_table"]["rows_converted"], 1)
        self.assertEqual(compact["reported_supplier_sites"][0]["annual_reported_subcontract_activity"][1][1], -25.0)
        self.assertEqual(expand_platform_model_evidence(compact), original)

    def test_unknown_annual_fields_are_left_unmodified(self):
        pack = {"reported_supplier_sites": [{
            "cage": "12345",
            "annual_reported_subcontract_activity": [{
                "cage": "12345", "fiscal_year": 2026,
                "mimir_modelled_reported_subcontract_value_usd": 5,
                "source_reported_value_usd": 5,
                "selected_report_count": 1,
                "prime_award_count": 1,
                "new_source_field": "retain me",
            }],
        }]}
        self.assertIs(compact_platform_model_evidence(pack), pack)


if __name__ == "__main__":
    unittest.main()
