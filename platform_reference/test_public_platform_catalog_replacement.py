"""Regression checks for explicit platform cohort replacement in Public builds."""

import unittest

import duckdb

from public_intelligence_release import stage_public_intelligence_manifest


class PublicPlatformCatalogReplacementTests(unittest.TestCase):
    def setUp(self):
        self.db = duckdb.connect()
        self.db.execute("""
            CREATE TABLE public_intelligence_manifest AS
            SELECT * FROM (VALUES
              ('platform','epa-superfund','/platforms/epa-superfund','EPA SUPERFUND',
               10.0,'prior','2026-10-13','prior-release',3,'old-platform','v3'),
              ('cage_company','81755','/companies/81755','LOCKHEED MARTIN',
               10.0,'prior','2026-10-13','prior-release',3,'old-company','v3')
            ) v(entity_type,entity_id,canonical_path,display_name,richness_score,
                decision_reasons,last_modified,release_id,schema_version,
                content_fingerprint,quality_gate_version)
        """)
        self.db.execute("""
            CREATE TABLE public_intelligence_manifest_candidates_next AS
            SELECT * FROM (VALUES
              ('platform','f-35','/platforms/f-35','F-35',
               20.0,'current','2026-10-29','candidate',3,'new-platform','v3')
            ) v(entity_type,entity_id,canonical_path,display_name,richness_score,
                decision_reasons,last_modified,release_id,schema_version,
                content_fingerprint,quality_gate_version)
        """)

    def tearDown(self):
        self.db.close()

    def test_default_retains_prior_platform_and_company(self):
        stage_public_intelligence_manifest(
            self.db, release_id="next", generated_date="2026-10-29"
        )
        rows = set(self.db.execute(
            "SELECT entity_type,entity_id FROM public_intelligence_manifest_next"
        ).fetchall())
        self.assertEqual(rows, {
            ("platform", "epa-superfund"), ("platform", "f-35"),
            ("cage_company", "81755"),
        })

    def test_explicit_replacement_removes_only_prior_platform(self):
        report = stage_public_intelligence_manifest(
            self.db, release_id="next", generated_date="2026-10-29",
            replace_entity_types=("platform",),
        )
        rows = set(self.db.execute(
            "SELECT entity_type,entity_id FROM public_intelligence_manifest_next"
        ).fetchall())
        self.assertEqual(rows, {("platform", "f-35"), ("cage_company", "81755")})
        self.assertEqual(report["replaced_entity_types"], ["platform"])
        self.assertEqual(report["retained_without_refresh"], 1)

    def test_other_cohort_replacement_is_rejected(self):
        with self.assertRaises(ValueError):
            stage_public_intelligence_manifest(
                self.db, release_id="next", generated_date="2026-10-29",
                replace_entity_types=("cage_company",),
            )

    def test_empty_platform_replacement_is_rejected(self):
        self.db.execute("DELETE FROM public_intelligence_manifest_candidates_next")
        with self.assertRaisesRegex(RuntimeError, "remove too much"):
            stage_public_intelligence_manifest(
                self.db, release_id="next", generated_date="2026-10-29",
                replace_entity_types=("platform",),
            )


if __name__ == "__main__":
    unittest.main()
