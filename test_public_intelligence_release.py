import unittest

import duckdb

from public_intelligence_release import (
    PUBLIC_AWARD_MIN_ACTIONS,
    PUBLIC_AWARD_MIN_DESCRIPTION_LENGTH,
    PUBLIC_AWARD_MIN_OBSERVED_VALUE,
    PUBLIC_COMPANY_MIN_OBSERVED_VALUE,
    PUBLIC_HEALTHCARE_DOMINANCE_SHARE,
    PUBLIC_INTELLIGENCE_QUALITY_GATE_VERSION,
    PUBLIC_INTELLIGENCE_SITEMAP_BATCH_SIZE,
    PUBLIC_NSN_MIN_CONTEXT_COUNT,
    PUBLIC_NSN_MIN_OBSERVED_VALUE,
    PUBLIC_NSN_UNMAPPED_MIN_OBSERVED_VALUE,
    PUBLIC_SOLICITATION_MIN_DESCRIPTION_LENGTH,
    PUBLIC_SOLICITATION_MIN_METADATA_FIELDS,
    PUBLIC_SOLICITATION_MIN_TITLE_LENGTH,
    append_active_opportunity_nsn_candidates,
    last_known_good_public_release,
    preserve_projection_for_unrefreshed_entities,
    previous_publication_entries,
    public_content_fingerprint,
    public_entity_slug,
    public_release_table_columns,
    stage_public_intelligence_manifest,
)


class PublicIntelligenceReleaseTests(unittest.TestCase):
    def test_release_table_columns_use_the_supplied_connection(self):
        first = duckdb.connect()
        second = duckdb.connect()
        first.execute("CREATE TABLE isolated_release_view (niin VARCHAR, stock BIGINT)")
        self.assertEqual(
            public_release_table_columns(first, "isolated_release_view"),
            {"niin", "stock"},
        )
        self.assertEqual(
            public_release_table_columns(second, "isolated_release_view"),
            set(),
        )

    def test_active_opportunity_only_nsn_is_admitted_without_displacing_existing_pages(self):
        connection = duckdb.connect()
        connection.execute("""
            CREATE TABLE public_nsn_manifest_candidates_next (
                entity_type VARCHAR, entity_id VARCHAR, canonical_path VARCHAR,
                display_name VARCHAR, richness_score DOUBLE, decision_reasons VARCHAR,
                last_modified VARCHAR, release_id VARCHAR, schema_version INTEGER,
                content_fingerprint VARCHAR, quality_gate_version VARCHAR,
                quality_gate_matches BIGINT
            );
            INSERT INTO public_nsn_manifest_candidates_next VALUES
            ('nsn', '5310001860967', '/intelligence/nsn/5310001860967',
             'WASHER,KEY', 99, 'procurement-history', '2026-09-25', 'release',
             3, 'existing-fingerprint', 'gate', 1);
            CREATE TABLE v_nsn_profile_lookup (
                niin VARCHAR, nsn VARCHAR, item_name VARCHAR, fsc_code VARCHAR
            );
            INSERT INTO v_nsn_profile_lookup VALUES
            ('000013841', '2910000013841', 'ANCHOR,CAP', '2910');
            CREATE TABLE v_nsn_opportunity_summary (
                niin VARCHAR, nsn VARCHAR, active_solicitation_count INTEGER,
                next_response_deadline TIMESTAMP, next_solicitation_number VARCHAR,
                next_quantity DOUBLE
            );
            INSERT INTO v_nsn_opportunity_summary VALUES
            ('000013841', '2910000013841', 1, '2026-09-28', 'SPE7L526T5482', 603);
        """)

        stats = append_active_opportunity_nsn_candidates(
            connection,
            remaining_slots=10,
            generated_date="2026-09-25",
            release_id="corrected-public",
            schema_version=3,
            quality_gate_version="gate",
        )

        self.assertEqual(stats, {"added": 1, "quality_gate_matches": 2})
        rows = connection.execute("""
            SELECT entity_id, display_name, decision_reasons, release_id,
                   quality_gate_matches
            FROM public_nsn_manifest_candidates_next
            ORDER BY entity_id
        """).fetchall()
        self.assertEqual(rows, [
            ('2910000013841', 'ANCHOR,CAP', 'active-solicitation',
             'corrected-public', 2),
            ('5310001860967', 'WASHER,KEY', 'procurement-history', 'release', 2),
        ])

    def test_content_fingerprint_is_stable_and_sensitive(self):
        self.assertEqual(
            public_content_fingerprint("CAGE1", 10, ["F-35"]),
            public_content_fingerprint("CAGE1", 10, ["F-35"]),
        )
        self.assertNotEqual(
            public_content_fingerprint("CAGE1", 10, ["F-35"]),
            public_content_fingerprint("CAGE1", 11, ["F-35"]),
        )

    def test_previous_manifest_only_preserves_versioned_fingerprints(self):
        connection = duckdb.connect()
        connection.execute("""
            CREATE TABLE public_intelligence_manifest (
                entity_type VARCHAR,
                entity_id VARCHAR,
                content_fingerprint VARCHAR,
                last_modified VARCHAR
            )
        """)
        connection.execute(
            "INSERT INTO public_intelligence_manifest VALUES (?, ?, ?, ?)",
            ["nsn", "123456789", "abc", "2026-09-21"],
        )
        self.assertEqual(
            previous_publication_entries(connection)["nsn:123456789"],
            {"content_fingerprint": "abc", "last_modified": "2026-09-21"},
        )

    def test_sitemap_contract_stays_within_google_limit(self):
        self.assertLessEqual(PUBLIC_INTELLIGENCE_SITEMAP_BATCH_SIZE, 50_000)
        self.assertTrue(PUBLIC_INTELLIGENCE_QUALITY_GATE_VERSION)

    def test_quality_gate_thresholds_are_explicit(self):
        self.assertEqual(PUBLIC_COMPANY_MIN_OBSERVED_VALUE, 10_000)
        self.assertEqual(PUBLIC_HEALTHCARE_DOMINANCE_SHARE, 0.8)
        self.assertEqual(PUBLIC_AWARD_MIN_OBSERVED_VALUE, 1_000_000)
        self.assertEqual(PUBLIC_AWARD_MIN_ACTIONS, 2)
        self.assertEqual(PUBLIC_AWARD_MIN_DESCRIPTION_LENGTH, 30)
        self.assertEqual(PUBLIC_SOLICITATION_MIN_TITLE_LENGTH, 12)
        self.assertEqual(PUBLIC_SOLICITATION_MIN_DESCRIPTION_LENGTH, 150)
        self.assertEqual(PUBLIC_SOLICITATION_MIN_METADATA_FIELDS, 1)
        self.assertEqual(PUBLIC_NSN_MIN_OBSERVED_VALUE, 100)
        self.assertEqual(PUBLIC_NSN_UNMAPPED_MIN_OBSERVED_VALUE, 1_000)
        self.assertEqual(PUBLIC_NSN_MIN_CONTEXT_COUNT, 2)

    def test_slug_matches_public_route_rules(self):
        self.assertEqual(
            public_entity_slug("STEAMCO VENTILATION & EXHAUST SYSTEMS INC Meridian ID"),
            "steamco-ventilation-and-exhaust-systems-inc-meridian-id",
        )
        self.assertLessEqual(len(public_entity_slug("A" * 120)), 90)

    @staticmethod
    def _create_candidate_manifest(connection):
        connection.execute("""
            CREATE TABLE public_intelligence_manifest_candidates_next (
                entity_type VARCHAR,
                entity_id VARCHAR,
                canonical_path VARCHAR,
                display_name VARCHAR,
                richness_score DOUBLE,
                decision_reasons VARCHAR,
                last_modified VARCHAR,
                release_id VARCHAR,
                schema_version INTEGER,
                content_fingerprint VARCHAR,
                quality_gate_version VARCHAR
            )
        """)

    @staticmethod
    def _create_active_manifest(connection):
        connection.execute("""
            CREATE TABLE public_intelligence_manifest (
                entity_type VARCHAR,
                entity_id VARCHAR,
                canonical_path VARCHAR,
                display_name VARCHAR,
                richness_score DOUBLE,
                decision_reasons VARCHAR,
                last_modified VARCHAR,
                release_id VARCHAR,
                schema_version INTEGER,
                content_fingerprint VARCHAR,
                quality_gate_version VARCHAR,
                sitemap_batch INTEGER
            )
        """)

    def test_incomplete_refresh_cannot_remove_a_published_page(self):
        connection = duckdb.connect()
        self._create_candidate_manifest(connection)
        self._create_active_manifest(connection)
        connection.execute("""
            INSERT INTO public_intelligence_manifest VALUES
            ('nsn', '000000001', '/intelligence/nsn/000000001', 'Old item', 10,
             'previously-published', '2026-09-01', 'old-release', 2, 'old-fp', 'v1', 1)
        """)

        stats = stage_public_intelligence_manifest(
            connection,
            release_id="new-release",
            generated_date="2026-09-24",
            sitemap_batch_size=5_000,
        )

        retained = connection.execute("""
            SELECT release_id, last_modified, display_name
            FROM public_intelligence_manifest_next
            WHERE entity_type = 'nsn' AND entity_id = '000000001'
        """).fetchone()
        self.assertEqual(retained, ("new-release", "2026-09-01", "Old item"))
        self.assertEqual(stats["retained_without_refresh"], 1)

    def test_first_release_stages_candidates_with_expected_column_order(self):
        connection = duckdb.connect()
        self._create_candidate_manifest(connection)
        connection.execute("""
            INSERT INTO public_intelligence_manifest_candidates_next VALUES
            ('platform', 'a-10', '/intelligence/platforms/a-10', 'A-10', 12,
             'quality-pass', 'candidate-date', 'candidate-release', 2, 'fp', 'v2')
        """)

        stats = stage_public_intelligence_manifest(
            connection,
            release_id="first-release",
            generated_date="2026-09-24",
        )

        row = connection.execute("""
            SELECT entity_type, entity_id, last_modified, release_id,
                   schema_version, content_fingerprint, quality_gate_version,
                   sitemap_batch
            FROM public_intelligence_manifest_next
        """).fetchone()
        self.assertEqual(
            row,
            ("platform", "a-10", "2026-09-24", "first-release", 2, "fp", "v2", 1),
        )
        self.assertEqual(stats["previous_count"], 0)

    def test_duplicate_candidate_keys_fail_closed(self):
        connection = duckdb.connect()
        self._create_candidate_manifest(connection)
        connection.execute("""
            INSERT INTO public_intelligence_manifest_candidates_next VALUES
            ('solicitation', 'OPP-1', '/one', 'First', 10, 'quality-pass',
             'candidate-date', 'candidate-release', 2, 'fp-1', 'v2'),
            ('solicitation', 'OPP-1', '/two', 'Second', 9, 'quality-pass',
             'candidate-date', 'candidate-release', 2, 'fp-2', 'v2')
        """)
        with self.assertRaisesRegex(RuntimeError, "solicitation.*OPP-1"):
            stage_public_intelligence_manifest(
                connection,
                release_id="first-release",
                generated_date="2026-09-24",
            )

    def test_refreshed_entity_updates_without_changing_truthful_lastmod(self):
        connection = duckdb.connect()
        self._create_candidate_manifest(connection)
        self._create_active_manifest(connection)
        connection.execute("""
            INSERT INTO public_intelligence_manifest VALUES
            ('cage_company', 'ABC12', '/old', 'Old name', 10, 'old reason',
             '2026-09-01', 'old-release', 2, 'same-fp', 'v1', 1)
        """)
        connection.execute("""
            INSERT INTO public_intelligence_manifest_candidates_next VALUES
            ('cage_company', 'ABC12', '/new', 'New name', 11, 'new reason',
             '2026-09-24', 'candidate-release', 2, 'same-fp', 'v2')
        """)

        stage_public_intelligence_manifest(
            connection,
            release_id="new-release",
            generated_date="2026-09-24",
        )
        row = connection.execute("""
            SELECT canonical_path, display_name, last_modified, release_id
            FROM public_intelligence_manifest_next
        """).fetchone()
        self.assertEqual(row, ("/new", "New name", "2026-09-01", "new-release"))

    def test_retained_entity_keeps_complete_last_known_good_projection(self):
        connection = duckdb.connect()
        self._create_candidate_manifest(connection)
        self._create_active_manifest(connection)
        connection.execute("""
            INSERT INTO public_intelligence_manifest VALUES
            ('solicitation', 'OPP-1', '/opportunity/OPP-1', 'Old opportunity', 5,
             'published', '2026-09-01', 'old-release', 2, 'old-fp', 'v1', 1)
        """)
        stage_public_intelligence_manifest(
            connection,
            release_id="new-release",
            generated_date="2026-09-24",
        )
        connection.execute("""
            CREATE TABLE public_solicitation_profile (
                opportunity_id VARCHAR, title VARCHAR, description VARCHAR
            )
        """)
        connection.execute("""
            INSERT INTO public_solicitation_profile VALUES
            ('OPP-1', 'Old opportunity', 'Last-known-good description')
        """)
        connection.execute("""
            CREATE TABLE public_solicitation_profile_next (
                opportunity_id VARCHAR, title VARCHAR, description VARCHAR
            )
        """)
        connection.execute("""
            INSERT INTO public_solicitation_profile_next VALUES
            ('OPP-1', 'Partial refresh', NULL)
        """)

        preserved = preserve_projection_for_unrefreshed_entities(
            connection,
            next_table="public_solicitation_profile_next",
            active_table="public_solicitation_profile",
            entity_type="solicitation",
            entity_column="opportunity_id",
        )

        self.assertEqual(preserved, 1)
        self.assertEqual(
            connection.execute(
                "SELECT title, description FROM public_solicitation_profile_next"
            ).fetchone(),
            ("Old opportunity", "Last-known-good description"),
        )

    def test_failed_refresh_reuses_an_isolated_last_known_good_release(self):
        previous = {"release_id": "stable", "counts": {"nsn": 10}}
        retained = last_known_good_public_release(previous)
        retained["counts"]["nsn"] = 9
        self.assertEqual(previous["counts"]["nsn"], 10)
        self.assertEqual(retained["release_id"], "stable")


if __name__ == "__main__":
    unittest.main()
