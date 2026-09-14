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
            summary = builder._third_party_dla_procurement_routes(["TARGET"], [2024, 2025], summary=True)
            self.assertEqual(summary["observed_dla_procurement_value_usd"], 250.0)
            self.assertEqual(summary["potential_intermediary_procurement_value_usd"], 200.0)
            self.assertEqual(summary["alternate_source_procurement_value_usd"], 50.0)
            self.assertEqual(summary["leading_recipients"][0]["observed_dla_procurement_value_usd"], 200.0)
            connection.close()

    def test_full_recipient_summary_includes_routes_below_5000_row_cap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            con = duckdb.connect()
            con.execute("""COPY (SELECT lpad(i::varchar,9,'0') AS niin, 'NSN' AS nsn,
                'TARGET' AS cage, 'P' AS part_number, 'CONTROL' AS description,
                'Active authorized source' AS supplier_status, true AS is_procurement_authorized,
                true AS is_active_authorized_source, '3' AS rncc_codes FROM range(5002) t(i)
            ) TO ? (FORMAT PARQUET)""",[str(root/'reference.parquet')])
            con.execute("""COPY (SELECT 'DLA' AS source_system,
                CASE WHEN i<5000 THEN 'AAAAA' ELSE 'BBBBB' END AS vendor_cage,
                'Supplier' AS vendor_name, lpad(i::varchar,9,'0') AS niin, 2025 AS year,
                CASE WHEN i<5000 THEN 2.0 ELSE 1.5 END AS spend_amount,
                i::varchar AS award_key, i::varchar AS transaction_key, DATE '2025-01-01' AS action_date
                FROM range(5002) t(i)) TO ? (FORMAT PARQUET)""",[str(root/'transactions.parquet')])
            con.execute("""COPY (SELECT 'AAAAA' AS cage_code, 'Dayton' AS city, 'OH' AS state)
                TO ? (FORMAT PARQUET)""",[str(root/'geo.parquet')])
            builder = object.__new__(CompanyContextBuilder)
            builder.connection = con
            builder.paths = {'nsn_reference':root/'reference.parquet','transactions':root/'transactions.parquet','geo':root/'geo.parquet'}
            rows = builder._third_party_dla_procurement_routes(['TARGET'],[2025])
            summary = builder._third_party_dla_procurement_routes(['TARGET'],[2025],summary=True)
            self.assertEqual(len(rows),5000)
            self.assertEqual(summary['observed_dla_procurement_value_usd'],10003)
            self.assertEqual(summary['potential_intermediary_procurement_value_usd'],10003)
            self.assertEqual(summary['recipient_count'],2)
            self.assertEqual(summary['leading_recipients'][1]['recipient_cage'],'BBBBB')
            self.assertEqual(summary['leading_recipients'][1]['observed_dla_procurement_value_usd'],3)
            con.close()


if __name__ == "__main__":
    unittest.main()
