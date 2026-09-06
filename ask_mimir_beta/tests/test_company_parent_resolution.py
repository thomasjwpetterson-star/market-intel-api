import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from company_context_store import CompanyContextStore


class CompanyParentResolutionTests(unittest.TestCase):
    def _store(self, root: Path, *, include_parent_columns: bool = True):
        context_dir = root / "contexts"
        data_root = root / "data"
        context_dir.mkdir()
        data_root.mkdir()
        (context_dir / "manifest.json").write_text(json.dumps({"contexts": []}))

        connection = duckdb.connect()
        if include_parent_columns:
            connection.execute(
                """
                COPY (
                    SELECT * FROM (VALUES
                        ('11111', 'GARRETT AEROSPACE SERVICES', 'HONEYWELL INTERNATIONAL INC.', 'HONEYWELLUEI1', 100.0, 10.0),
                        ('22222', 'ALLIEDSIGNAL AVIONICS', 'HONEYWELL INTERNATIONAL INC.', 'HONEYWELLUEI2', 200.0, 20.0),
                        ('33333', 'ONTIC ENGINEERING', 'ONTIC ENGINEERING', 'ONTICUEI0001', 50.0, 5.0),
                        ('44444', 'THE BOEING COMPANY', 'THE BOEING', 'BOEINGUEI001', 500.0, 50.0),
                        ('55555', 'BELL BOEING JOINT PROJECT OFFICE', 'BELL BOEING JOINT PROJECT OFFICE', 'BELLBOEING01', 100.0, 10.0),
                        ('66666', 'CURTISS-WRIGHT CONTROLS, INC.', 'CURTISS-WRIGHT CORPORATION', 'CURTISSUEI1', 300.0, 30.0),
                        ('77777', 'MOOG INC.', 'MOOG INC.', 'MOOGUEI0001', 400.0, 40.0),
                        ('88888', 'MOOG INC.', 'MOOG INC.', 'MOOGUEI0002', 350.0, 35.0),
                        ('9EME1', 'CURTISS VILLAGE OF', 'CURTISS VILLAGE OF', 'CURTISSVILL1', 1.0, 0.0)
                    ) AS t(
                        cage_code,
                        vendor_name,
                        ultimate_parent_name,
                        ultimate_parent_uei,
                        total_lifetime_spend,
                        network_flow_total
                    )
                ) TO ? (FORMAT PARQUET)
                """,
                [str(data_root / "profiles.parquet")],
            )
        else:
            connection.execute(
                """
                COPY (
                    SELECT * FROM (VALUES
                        ('11111', 'HONEYWELL AEROSPACE', 100.0, 10.0)
                    ) AS t(
                        cage_code,
                        vendor_name,
                        total_lifetime_spend,
                        network_flow_total
                    )
                ) TO ? (FORMAT PARQUET)
                """,
                [str(data_root / "profiles.parquet")],
            )
        connection.execute(
            """
            COPY (
                SELECT * FROM (VALUES
                    ('11111', 'GARRETT AEROSPACE SERVICES', 'PHOENIX', 'AZ', 'A', NULL),
                    ('22222', 'ALLIEDSIGNAL AVIONICS', 'CLEARWATER', 'FL', 'A', NULL),
                    ('33333', 'ONTIC ENGINEERING', 'CHATSWORTH', 'CA', 'A', NULL),
                    ('44444', 'THE BOEING COMPANY', 'ARLINGTON', 'VA', 'A', NULL),
                    ('55555', 'BELL BOEING JOINT PROJECT OFFICE', 'AMARILLO', 'TX', 'A', NULL),
                    ('66666', 'CURTISS-WRIGHT CONTROLS, INC.', 'ASHBURN', 'VA', 'A', NULL),
                    ('77777', 'MOOG INC.', 'BLACKSBURG', 'VA', 'A', NULL),
                    ('88888', 'MOOG INC.', 'EAST AURORA', 'NY', 'A', NULL),
                    ('9EME1', 'CURTISS VILLAGE OF', 'CURTISS', 'WI', 'A', NULL)
                ) AS t(cage_code, vendor_name, city, state, cage_status, replacement_cage)
            ) TO ? (FORMAT PARQUET)
            """,
            [str(data_root / "cage_locations.parquet")],
        )
        connection.close()
        return CompanyContextStore(context_dir=context_dir, data_root=data_root)

    def test_reported_parent_collects_sites_with_different_legal_names_and_ueis(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(Path(directory))
            result = store.search("Honeywell", limit=10)

            parent = next(
                row for row in result["matches"] if row["scope_type"] == "company_parent"
            )
            self.assertEqual(parent["scope_name"], "HONEYWELL INTERNATIONAL INC.")
            self.assertEqual(parent["site_count"], 2)
            self.assertEqual(parent["resolved_cages"], ["11111", "22222"])
            self.assertEqual(parent["group_kind"], "reported_ultimate_parent")

    def test_parent_scope_identifier_is_stable_for_the_normalized_parent(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(Path(directory))
            first = store.search("Honeywell", limit=10)
            second = store.search("Honeywell International", limit=10)

            first_parent = next(
                row for row in first["matches"] if row["scope_type"] == "company_parent"
            )
            second_parent = next(
                row for row in second["matches"] if row["scope_type"] == "company_parent"
            )
            self.assertEqual(first_parent["scope_id"], second_parent["scope_id"])

    def test_exact_corporate_name_wins_over_a_longer_substring_match(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(Path(directory))
            result = store.search("Boeing", limit=10)

            parents = [
                row for row in result["matches"] if row["scope_type"] == "company_parent"
            ]
            self.assertEqual(len(parents), 1)
            self.assertEqual(parents[0]["scope_name"], "THE BOEING COMPANY")
            self.assertEqual(parents[0]["resolved_cages"], ["44444"])

    def test_old_profiles_without_parent_columns_remain_searchable(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(Path(directory), include_parent_columns=False)
            result = store.search("Honeywell", limit=10)

            sites = [
                row for row in result["matches"] if row["scope_type"] == "company_site"
            ]
            self.assertEqual([row["scope_id"] for row in sites], ["11111"])

    def test_multi_token_company_name_does_not_fall_back_to_a_weak_first_token(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(Path(directory))
            result = store.search("Curtiss-Wright", limit=10)

            self.assertNotIn(
                "9EME1",
                [row["scope_id"] for row in result["matches"]],
            )
            parent = next(
                row
                for row in result["matches"]
                if row["scope_type"] == "company_parent"
            )
            self.assertEqual(parent["scope_name"], "CURTISS-WRIGHT CORPORATION")
            self.assertEqual(parent["resolved_cages"], ["66666"])

    def test_location_follow_up_resolves_only_within_active_company_cages(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(Path(directory))
            result = store.resolve_site_reference(
                ["77777", "88888"],
                "Which platforms does the Blacksburg site support?",
                parent_name="MOOG INC.",
            )

            self.assertIsNotNone(result)
            self.assertEqual(result["scope_type"], "company_site")
            self.assertEqual(result["scope_id"], "77777")
            self.assertEqual(result["city"], "BLACKSBURG")


if __name__ == "__main__":
    unittest.main()
