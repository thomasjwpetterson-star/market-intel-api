import unittest

import duckdb

from public_intelligence_release import (
    PUBLIC_INTELLIGENCE_QUALITY_GATE_VERSION,
    PUBLIC_INTELLIGENCE_SITEMAP_BATCH_SIZE,
    previous_publication_entries,
    public_content_fingerprint,
    public_entity_slug,
)


class PublicIntelligenceReleaseTests(unittest.TestCase):
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

    def test_slug_matches_public_route_rules(self):
        self.assertEqual(
            public_entity_slug("STEAMCO VENTILATION & EXHAUST SYSTEMS INC Meridian ID"),
            "steamco-ventilation-and-exhaust-systems-inc-meridian-id",
        )
        self.assertLessEqual(len(public_entity_slug("A" * 120)), 90)


if __name__ == "__main__":
    unittest.main()
