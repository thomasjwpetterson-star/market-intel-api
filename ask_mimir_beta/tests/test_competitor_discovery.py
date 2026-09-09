import tempfile
import unittest
from pathlib import Path

import duckdb

from competitor_discovery import CompetitorDiscoveryStore


class CompetitorDiscoveryFinancialTests(unittest.TestCase):
    def test_duplicate_reference_rows_do_not_repeat_niin_supplier_value(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            references = root / "references.parquet"
            suppliers = root / "suppliers.parquet"
            writer = duckdb.connect()
            writer.execute(
                """
                CREATE TABLE refs AS SELECT * FROM (VALUES
                    ('000000001', 'TARG1', 'Target', 'Fuel control', 'F-16', 'F-16', true, 'Authorized'),
                    ('000000001', 'PEE01', 'Peer', 'Fuel control', 'F-16', 'F-16', true, 'Authorized'),
                    ('000000001', 'PEE01', 'Peer', 'Fuel control', 'F-16', 'F-16', true, 'Authorized')
                ) AS t(niin, cage, vendor_name, description, platform_families,
                       platform_family, is_active_authorized_source, supplier_status)
                """
            )
            writer.execute(
                """
                CREATE TABLE suppliers AS SELECT * FROM (VALUES
                    ('000000001', 'TARG1', 'Target', 'F-16', 'F-16', 50.0, 2025),
                    ('000000001', 'PEE01', 'Peer', 'F-16', 'F-16', 100.0, 2025)
                ) AS t(niin, cage, vendor, platform_families, platform_family,
                       total_revenue, year)
                """
            )
            writer.execute(f"COPY refs TO '{references}' (FORMAT PARQUET)")
            writer.execute(f"COPY suppliers TO '{suppliers}' (FORMAT PARQUET)")
            writer.close()

            store = CompetitorDiscoveryStore.__new__(CompetitorDiscoveryStore)
            store.connection = duckdb.connect()
            store.paths = {"references": references, "suppliers": suppliers}
            try:
                rows = store._relationship_rows(["TARG1"])
            finally:
                store.connection.close()

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["cage"], "PEE01")
            self.assertEqual(rows[0]["peer_observed_spend"], 100.0)
            self.assertTrue(rows[0]["target_observed"])
            self.assertTrue(rows[0]["peer_observed"])


if __name__ == "__main__":
    unittest.main()
