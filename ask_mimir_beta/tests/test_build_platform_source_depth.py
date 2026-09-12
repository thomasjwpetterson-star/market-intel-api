from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from build_platform_source_depth import build_platform_source_depth
from platform_context import PlatformContextStore


class PlatformSourceDepthBuildTests(unittest.TestCase):
    def test_keeps_authorized_and_manufacturer_reference_depth_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = duckdb.connect()
            connection.execute(
                """
                CREATE TABLE platform_bom(platform_family VARCHAR, niin VARCHAR);
                INSERT INTO platform_bom VALUES
                    ('TEST', '000000001'),
                    ('TEST', '000000002'),
                    ('TEST', '000000003');
                """
            )
            connection.execute(
                f"COPY platform_bom TO '{root / 'platform_bom.parquet'}' (FORMAT PARQUET)"
            )
            connection.execute(
                """
                CREATE TABLE reference(
                    niin VARCHAR,
                    cage VARCHAR,
                    is_active_authorized_source BOOLEAN,
                    rncc_codes VARCHAR,
                    rnvc_codes VARCHAR,
                    cage_status_codes VARCHAR,
                    vendor_name VARCHAR,
                    nsn VARCHAR,
                    description VARCHAR,
                    fsc_code VARCHAR
                );
                INSERT INTO reference VALUES
                    ('000000001', 'AUTH1', true,  '3', '2', 'A', 'Authorized Manufacturer', '1000-00-000-0001', 'One', '1000'),
                    ('000000002', 'MFG02', false, '3', '2', 'A', 'Recognized Manufacturer', '1000-00-000-0002', 'Two', '1000'),
                    ('000000003', 'OLD03', false, '3', '2', 'H', 'Inactive Reference', '1000-00-000-0003', 'Three', '1000');
                """
            )
            connection.execute(
                f"COPY reference TO '{root / 'nsn_cage_reference.parquet'}' (FORMAT PARQUET)"
            )
            connection.close()

            result = build_platform_source_depth(root)
            self.assertEqual(result["niins_with_active_manufacturer_reference"], 2)

            reader = duckdb.connect()
            platform = reader.execute(
                "SELECT * FROM read_parquet(?) WHERE platform_family = 'TEST'",
                [str(root / "platform_source_depth.parquet")],
            ).fetchone()
            columns = [column[0] for column in reader.description]
            summary = dict(zip(columns, platform))
            self.assertEqual(summary["associated_niin_count"], 3)
            self.assertEqual(summary["niin_count_with_one_active_authorized_source"], 1)
            self.assertEqual(summary["niin_count_with_one_active_manufacturer_reference"], 2)
            self.assertEqual(
                summary[
                    "niin_count_without_active_authorized_but_with_active_manufacturer_reference"
                ],
                1,
            )

            manufacturer_only = reader.execute(
                """
                SELECT active_authorized_source_count,
                       active_manufacturer_reference_count,
                       active_manufacturer_reference_names,
                       manufacturer_reference_depth
                FROM read_parquet(?) WHERE niin = '000000002'
                """,
                [str(root / "niin_source_depth.parquet")],
            ).fetchone()
            self.assertEqual(manufacturer_only[0], 0)
            self.assertEqual(manufacturer_only[1], 1)
            self.assertEqual(manufacturer_only[2], "Recognized Manufacturer")
            self.assertEqual(
                manufacturer_only[3],
                "One active item-identifying manufacturer reference",
            )
            reader.close()

            store = object.__new__(PlatformContextStore)
            store.connection = duckdb.connect()
            store.paths = {
                "niin_source_depth": root / "niin_source_depth.parquet",
                "platform_source_depth": root / "platform_source_depth.parquet",
            }
            store.niin_source_depth_columns = {
                row[0].lower()
                for row in store.connection.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?)",
                    [str(root / "niin_source_depth.parquet")],
                ).fetchall()
            }
            context = {
                "scope": {
                    "platform_id": "TEST",
                    "included_platform_records": ["TEST"],
                },
                "item_and_component_evidence": {
                    "authorized_source_depth": {},
                    "top_items": [{"niin": "000000002"}],
                },
            }
            store._refresh_precomputed_source_depth(context)
            refreshed = context["item_and_component_evidence"]
            self.assertEqual(
                refreshed["authorized_source_depth"]
                ["niin_count_with_one_active_manufacturer_reference"],
                2,
            )
            self.assertEqual(
                refreshed["top_items"][0]["active_manufacturer_reference_names"],
                "Recognized Manufacturer",
            )
            store.connection.close()


if __name__ == "__main__":
    unittest.main()
