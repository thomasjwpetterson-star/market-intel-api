import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import publish_runtime_release
from market_record_search import MarketRecordSearchStore


class RecentAwardsEnrichmentTests(unittest.TestCase):
    def test_build_keeps_announcement_values_separate_from_obligations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            announcement_dir = root / "dod-contract-announcements"
            announcement_dir.mkdir()
            connection = duckdb.connect()
            connection.execute(
                """
                COPY (
                    SELECT 'USASPEND-1' contract_id, 'AWARD-1' award_key,
                           'USA VENDOR' vendor_name, '12345' vendor_cage,
                           'DEPARTMENT OF DEFENSE' parent_agency, 'AIR FORCE' sub_agency,
                           '1510' psc, '336411' naics_code, 'F-16' platform_family,
                           125.0 total_spend, DATE '2026-08-01' last_action_date,
                           'AIRCRAFT MODIFICATION' base_award_description,
                           'LATEST ACTION' latest_action_description,
                           'AIRCRAFT MODIFICATION' description,
                           'USA_SPENDING' source_system, 2026 AS "year"
                ) TO ? (FORMAT PARQUET)
                """,
                [str(root / "contracts_rolled.parquet")],
            )
            connection.execute(
                """
                COPY (
                    SELECT 'ANN-1' announcement_id, DATE '2026-09-10' announcement_date,
                           'AIR FORCE' service, 1 entry_index, 'award' entry_type,
                           'ANNOUNCED VENDOR, DAYTON, OHIO' recipient_text,
                           'FA0000-26-C-0001' primary_contract_id,
                           ['FA0000-26-C-0001'] contract_ids,
                           500000000.0 announced_value_usd,
                           25000000.0 obligated_at_announcement_usd,
                           'Work will be performed in Dayton, Ohio.' work_locations,
                           'Expected completion is September 2030.' completion_text,
                           'Two offers were received.' competition_text,
                           'Air Force office is the contracting activity.' contracting_activity,
                           'MISSILE PRODUCTION AWARD' description,
                           'MISSILE PRODUCTION AWARD' search_text,
                           '4596373' source_article_id,
                           'Contracts for Sept. 10, 2026' source_title,
                           'https://www.defense.gov/News/Contracts/Contract/Article/4596373/' source_url,
                           '2026-09-10T21:00:00+00:00' source_published_at,
                           'https://r.jina.ai/http://www.defense.gov/example' source_fetch_url,
                           'text_renderer' source_fetch_method,
                           'hash' source_content_sha256,
                           '2026-09-11T00:00:00+00:00' retrieved_at,
                           'MISSILE PRODUCTION AWARD' raw_text
                ) TO ? (FORMAT PARQUET)
                """,
                [str(announcement_dir / "dod_contract_announcements.parquet")],
            )
            connection.close()

            with patch.object(publish_runtime_release, "DATA_ROOT", root):
                output = publish_runtime_release.build_recent_awards_search()

            rows = duckdb.sql(
                """
                SELECT source_type, total_spend, announced_value_usd,
                       obligated_at_announcement_usd
                FROM read_parquet(?) ORDER BY source_type
                """,
                params=[str(output)],
            ).fetchall()
            self.assertEqual(rows[0], ("DOD_CONTRACT_ANNOUNCEMENT", None, 500000000.0, 25000000.0))
            self.assertEqual(rows[1], ("USA_SPENDING", 125.0, None, None))

    def test_search_returns_official_announcement_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = duckdb.connect()
            connection.execute(
                """
                COPY (
                    SELECT 'FA0000-26-C-0001' contract_id, 'ANN-1' award_key,
                           'ANNOUNCED VENDOR' vendor_name, NULL::VARCHAR vendor_cage,
                           'DEPARTMENT OF DEFENSE' parent_agency, 'AIR FORCE' sub_agency,
                           NULL::VARCHAR psc, NULL::VARCHAR naics_code,
                           NULL::VARCHAR platform_family, NULL::DOUBLE total_spend,
                           '2026-09-10' last_action_date,
                           'MISSILE PROPULSION PRODUCTION' base_award_description,
                           'MISSILE PROPULSION PRODUCTION' latest_action_description,
                           'MISSILE PROPULSION PRODUCTION' description,
                           'MISSILE PROPULSION PRODUCTION' search_text,
                           'DOD_CONTRACT_ANNOUNCEMENT' source_type,
                           'https://www.defense.gov/example' source_url,
                           500000000.0 announced_value_usd,
                           25000000.0 obligated_at_announcement_usd,
                           'AIR FORCE' service_section,
                           'Work will be performed in Dayton.' work_locations,
                           'Expected completion is 2030.' completion_text,
                           'Two offers were received.' competition_text,
                           'Air Force office is the contracting activity.' contracting_activity
                ) TO ? (FORMAT PARQUET)
                """,
                [str(root / "recent_awards_search.parquet")],
            )
            connection.execute(
                """
                COPY (SELECT 'USA-1' contract_id, 'A1' award_key, 'OLD' vendor_name,
                             '11111' vendor_cage, 'DOD' parent_agency, 'AIR FORCE' sub_agency,
                             '1234' psc, '123456' naics_code, NULL platform_family,
                             1.0 total_spend, '2025-01-01' last_action_date,
                             'OTHER' base_award_description, 'OTHER' latest_action_description,
                             'OTHER' description, 'USA_SPENDING' source_system, 2025 AS "year")
                TO ? (FORMAT PARQUET)
                """,
                [str(root / "contracts_rolled.parquet")],
            )
            connection.execute(
                """
                COPY (SELECT '11111' cage_code, 'OLD' vendor_name, 'DAYTON' city, 'OH' state)
                TO ? (FORMAT PARQUET)
                """,
                [str(root / "cage_locations.parquet")],
            )
            connection.execute(
                """
                COPY (SELECT 'PSC' classification_type, '1234' code, 'Other' description)
                TO ? (FORMAT PARQUET)
                """,
                [str(root / "classification_reference.parquet")],
            )
            connection.execute(
                """
                COPY (SELECT 'OPP' id, 'SOL' sol_num, 'Other' title, 'DOD' agency,
                             'AIR FORCE' sub_agency, '2027-01-01' deadline, '1234' psc,
                             123456 naics, NULL set_aside_type, 'OH' state,
                             'https://sam.gov' url, 'Other' description, 'OTHER' search_text)
                TO ? (FORMAT PARQUET)
                """,
                [str(root / "opportunities.parquet")],
            )
            connection.close()

            result = MarketRecordSearchStore(root).get(
                {
                    "record_type": "award",
                    "notice_type": "ANY",
                    "subject": "missile propulsion",
                    "terms": ["MISSILE", "PROPULSION"],
                }
            )
            row = result["records"][0]
            self.assertEqual(row["source_type"], "DOD_CONTRACT_ANNOUNCEMENT")
            self.assertEqual(
                row["source_name"],
                "Official U.S. Department of Defense contract announcement",
            )
            self.assertEqual(row["source_url"], "https://www.defense.gov/example")
            self.assertEqual(row["announced_value_usd"], 500000000.0)
            self.assertIsNone(row["net_prime_obligations_usd"])


if __name__ == "__main__":
    unittest.main()
