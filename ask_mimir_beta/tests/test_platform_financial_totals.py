import unittest
from unittest.mock import Mock

from platform_context import PlatformContextStore


class PlatformFinancialTotalsTests(unittest.TestCase):
    def test_completed_and_partial_prime_obligations_are_separate(self):
        store = object.__new__(PlatformContextStore)
        store.paths = {"network": "/tmp/network.parquet"}
        store._platform_members = Mock(return_value=["AMRAAM"])
        store.connection = Mock()
        store.connection.execute.return_value.fetchone.return_value = [25.0]
        annual = {
            "records": [
                {
                    "source_system": "USA_SPENDING",
                    "fiscal_year": 2024,
                    "net_prime_obligations_usd": 100.0,
                    "positive_prime_obligations_usd": 110.0,
                    "prime_deobligations_usd": -10.0,
                },
                {
                    "source_system": "USA_SPENDING",
                    "fiscal_year": 2025,
                    "net_prime_obligations_usd": 80.0,
                    "positive_prime_obligations_usd": 80.0,
                    "prime_deobligations_usd": 0.0,
                },
                {
                    "source_system": "USA_SPENDING",
                    "fiscal_year": 2026,
                    "net_prime_obligations_usd": 40.0,
                    "positive_prime_obligations_usd": 42.0,
                    "prime_deobligations_usd": -2.0,
                },
                {
                    "source_system": "DLA",
                    "fiscal_year": 2026,
                    "net_prime_obligations_usd": 0.0,
                    "attributed_dla_procurement_value_usd": 7.0,
                },
            ]
        }

        totals = store._financial_totals("AMRAAM", annual)

        self.assertEqual(totals["completed_year_net_prime_obligations_usd"], 180.0)
        self.assertEqual(totals["partial_year_net_prime_obligations_usd"], 40.0)
        self.assertEqual(totals["net_prime_obligations_usd"], 220.0)
        self.assertEqual(totals["completed_fiscal_years"], [2021, 2022, 2023, 2024, 2025])
        self.assertEqual(totals["partial_fiscal_year"], 2026)
        self.assertEqual(totals["attributed_dla_procurement_value_usd"], 7.0)
        self.assertEqual(totals["mimir_modelled_reported_subcontract_value_usd"], 25.0)


if __name__ == "__main__":
    unittest.main()
