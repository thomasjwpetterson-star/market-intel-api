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
                        ('9EME1', 'CURTISS VILLAGE OF', 'CURTISS VILLAGE OF', 'CURTISSVILL1', 1.0, 0.0),
                        ('W1111', 'WOODWARD, INC.', NULL, NULL, 600.0, 60.0),
                        ('W2222', 'WOODWARD HRT, INC.', NULL, NULL, 500.0, 50.0),
                        ('W3333', 'WOODWARD COUNTY', NULL, NULL, 20.0, 2.0),
                        ('C1111', 'ROCKWELL COLLINS, INC.', NULL, NULL, 700.0, 70.0),
                        ('C2222', 'COLLINS AEROSPACE, INC.', NULL, NULL, 650.0, 65.0),
                        ('C3333', 'COLLINS CONSULTING, INC.', NULL, NULL, 30.0, 3.0),
                        ('E1111', 'EATON AEROSPACE, LLC', 'EATON AEROSPACE,', 'EATONUEI001', 800.0, 80.0),
                        ('E2222', 'EATON-AEROQUIP LLC.', 'EATON CORPORATION PUBLIC LIMITED', 'EATONUEI002', 700.0, 70.0),
                        ('E3333', 'EATON CORPORATION', 'EATON', 'EATONUEI003', 600.0, 60.0),
                        ('E4444', 'EUROFINS EATON ANALYTICAL, LLC', 'EUROFINS EATON ANALYTICAL', 'EUROFINS001', 500.0, 50.0),
                        ('T1111', 'DATA DEVICE CORPORATION', 'TRANSDIGM GROUP INCORPORATED', 'TRANSDIGM001', 900.0, 90.0),
                        ('T2222', 'ARMTEC DEFENSE PRODUCTS CO.', 'TRANSDIGM', 'TRANSDIGM002', 800.0, 80.0),
                        ('T3333', 'TRANSDIGM INC.', NULL, NULL, 700.0, 70.0)
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
                    ('O4444', 'ONTIC ENGINEERING AND MANUFACTURING, INC.', 'MIRAMAR', 'FL', 'A', NULL),
                    ('O5555', 'ONTIC ENGINEERING & MANUFACTURING UK LIMITED', 'CHELTENHAM', '', 'A', NULL),
                    ('O6666', 'ONTIC TECHNOLOGIES INC', 'AUSTIN', 'TX', 'A', NULL),
                    ('O7777', 'ONTIC ENGINEERING & MFG INC', 'SAN ANTONIO', 'TX', 'R', '33333'),
                    ('44444', 'THE BOEING COMPANY', 'ARLINGTON', 'VA', 'A', NULL),
                    ('55555', 'BELL BOEING JOINT PROJECT OFFICE', 'AMARILLO', 'TX', 'A', NULL),
                    ('66666', 'CURTISS-WRIGHT CONTROLS, INC.', 'ASHBURN', 'VA', 'A', NULL),
                    ('77777', 'MOOG INC.', 'BLACKSBURG', 'VA', 'A', NULL),
                    ('88888', 'MOOG INC.', 'EAST AURORA', 'NY', 'A', NULL),
                    ('9EME1', 'CURTISS VILLAGE OF', 'CURTISS', 'WI', 'A', NULL)
                    ,('W1111', 'WOODWARD, INC.', 'FORT COLLINS', 'CO', 'A', NULL)
                    ,('W2222', 'WOODWARD HRT, INC.', 'SKOKIE', 'IL', 'A', NULL)
                    ,('W3333', 'WOODWARD COUNTY', 'WOODWARD', 'OK', 'A', NULL)
                    ,('C1111', 'ROCKWELL COLLINS, INC.', 'CEDAR RAPIDS', 'IA', 'A', NULL)
                    ,('C2222', 'COLLINS AEROSPACE, INC.', 'CEDAR RAPIDS', 'IA', 'A', NULL)
                    ,('C3333', 'COLLINS CONSULTING, INC.', 'CEDAR RAPIDS', 'IA', 'A', NULL)
                    ,('E1111', 'EATON AEROSPACE, LLC', 'JACKSON', 'MS', 'A', NULL)
                    ,('E2222', 'EATON-AEROQUIP LLC.', 'JACKSON', 'MI', 'A', NULL)
                    ,('E3333', 'EATON CORPORATION', 'IRVINE', 'CA', 'A', NULL)
                    ,('E4444', 'EUROFINS EATON ANALYTICAL, LLC', 'POMONA', 'CA', 'A', NULL)
                    ,('T1111', 'DATA DEVICE CORPORATION', 'BOHEMIA', 'NY', 'A', NULL)
                    ,('T2222', 'ARMTEC DEFENSE PRODUCTS CO.', 'COACHELLA', 'CA', 'A', NULL)
                    ,('T3333', 'TRANSDIGM INC.', 'CLEVELAND', 'OH', 'A', NULL)
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

    def test_reviewed_ontic_group_includes_reference_only_sites(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(Path(directory))
            result = store.search("Ontic", limit=20)

            parent = next(
                row for row in result["matches"] if row["scope_type"] == "company_parent"
            )
            self.assertEqual(parent["resolved_cages"], ["33333", "O4444", "O5555"])
            self.assertNotIn("O6666", parent["resolved_cages"])
            self.assertNotIn("O7777", parent["resolved_cages"])

    def test_reviewed_woodward_group_excludes_unrelated_surname_entities(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(Path(directory))
            result = store.search("Woodward", limit=20)

            scopes = {row["scope_id"] for row in result["matches"]}
            self.assertIn("W1111", scopes)
            self.assertIn("W2222", scopes)
            self.assertNotIn("W3333", scopes)
            parent = next(
                row for row in result["matches"] if row["scope_type"] == "company_parent"
            )
            self.assertEqual(parent["resolved_cages"], ["W1111", "W2222"])

    def test_reviewed_collins_group_and_city_scope_exclude_unrelated_firms(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(Path(directory))
            result = store.search("Collins Aerospace", limit=20)

            parent = next(
                row for row in result["matches"] if row["scope_type"] == "company_parent"
            )
            self.assertEqual(parent["resolved_cages"], ["C1111", "C2222"])
            facility = store.resolve_site_reference(
                parent["resolved_cages"],
                "Collins Aerospace in Cedar Rapids, Iowa",
                parent_name=parent["scope_name"],
            )
            self.assertEqual(facility["resolved_cages"], ["C1111", "C2222"])
            self.assertNotIn("C3333", facility["resolved_cages"])

    def test_reviewed_eaton_group_combines_parent_variants_only(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(Path(directory))
            result = store.search("Eaton", limit=20)

            parent = next(
                row for row in result["matches"] if row["scope_type"] == "company_parent"
            )
            self.assertEqual(parent["resolved_cages"], ["E1111", "E2222", "E3333"])
            self.assertNotIn("E4444", parent["resolved_cages"])

    def test_reported_parent_variants_and_direct_named_sites_are_consolidated(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(Path(directory))
            result = store.search("TransDigm", limit=20)

            parents = [
                row for row in result["matches"] if row["scope_type"] == "company_parent"
            ]
            self.assertEqual(len(parents), 1)
            self.assertEqual(
                parents[0]["resolved_cages"], ["T1111", "T2222", "T3333"]
            )
            self.assertEqual(parents[0]["site_count"], 3)


if __name__ == "__main__":
    unittest.main()
