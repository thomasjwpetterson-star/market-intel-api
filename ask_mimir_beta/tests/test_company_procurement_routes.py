from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from company_context import CompanyContextBuilder


class CompanyProcurementRouteTests(unittest.TestCase):
    def test_design_control_company_links_other_recipient_as_potential_intermediary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = duckdb.connect()
            connection.execute(
                """
                COPY (
                    SELECT * FROM (VALUES
                        ('000000042', '1234-00-000-0042', 'TARGET', 'P-42', 'CONTROL',
                         'Active authorized source', true, true, '3'),
                        ('000000042', '1234-00-000-0042', 'OTHER', 'P-OTHER', 'CONTROL',
                         'Reference relationship only', false, false, '5'),
                        ('000000043', '1234-00-000-0043', 'TARGET', 'P-43', 'ACTUATOR',
                         'Active authorized source', true, true, '3'),
                        ('000000043', '1234-00-000-0043', 'ALTERNATE', 'P-ALT', 'ACTUATOR',
                         'Active authorized source', true, true, '3')
                    ) AS t(niin, nsn, cage, part_number, description, supplier_status,
                           is_procurement_authorized, is_active_authorized_source, rncc_codes)
                ) TO ? (FORMAT PARQUET)
                """,
                [str(root / "reference.parquet")],
            )
            connection.execute(
                """
                COPY (
                    SELECT * FROM (VALUES
                        ('DLA', 'OTHER', 'Observed Supply Co', '000000042', 2024,
                         125.0, 'A1', 'T1', DATE '2024-03-15'),
                        ('DLA', 'OTHER', 'Observed Supply Co', '000000042', 2025,
                         75.0, 'A2', 'T2', DATE '2025-04-20'),
                        ('DLA', 'ALTERNATE', 'Alternate Manufacturer', '000000043', 2025,
                         50.0, 'A3', 'T3', DATE '2025-05-20')
                    ) AS t(source_system, vendor_cage, vendor_name, niin, year,
                           spend_amount, award_key, transaction_key, action_date)
                ) TO ? (FORMAT PARQUET)
                """,
                [str(root / "transactions.parquet")],
            )
            connection.execute(
                """
                COPY (
                    SELECT * FROM (VALUES
                        ('OTHER', 'Observed Supply Co', 'Dayton', 'OH'),
                        ('ALTERNATE', 'Alternate Manufacturer', 'Phoenix', 'AZ')
                    ) AS t(cage_code, vendor_name, city, state)
                ) TO ? (FORMAT PARQUET)
                """,
                [str(root / "geo.parquet")],
            )

            builder = object.__new__(CompanyContextBuilder)
            builder.connection = connection
            builder.paths = {
                "nsn_reference": root / "reference.parquet",
                "transactions": root / "transactions.parquet",
                "geo": root / "geo.parquet",
            }

            rows = builder._third_party_dla_procurement_routes(
                ["TARGET"], [2024, 2025]
            )

            self.assertEqual(len(rows), 2)
            by_cage = {row["recipient_cage"]: row for row in rows}
            distributor = by_cage["OTHER"]
            alternate = by_cage["ALTERNATE"]
            self.assertEqual(distributor["dla_procurement_value_usd"], 200.0)
            self.assertEqual(distributor["route_universe_recipient_count"], 2)
            self.assertEqual(distributor["route_universe_niin_count"], 2)
            self.assertEqual(
                distributor["route_universe_procurement_value_usd"], 250.0
            )
            self.assertTrue(distributor["target_has_design_control_reference"])
            self.assertTrue(distributor["target_is_only_active_authorized_source"])
            self.assertEqual(
                distributor["relationship_interpretation"],
                "Potential distributor or procurement intermediary",
            )
            self.assertEqual(
                alternate["relationship_interpretation"],
                "Observed DLA recipient with its own design-control reference",
            )


if __name__ == "__main__":
    unittest.main()
