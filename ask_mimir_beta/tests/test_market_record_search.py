import tempfile
import unittest
from pathlib import Path

import duckdb

from market_record_search import MarketRecordSearchStore


class MarketRecordSearchTests(unittest.TestCase):
    def test_recent_award_artifact_is_preferred_over_full_contract_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            connection = duckdb.connect()
            connection.execute(
                """
                COPY (
                    SELECT * FROM (VALUES
                        ('FULL-ONLY', 'A1', 'OLD VENDOR', '11111', 'DOD', 'AIR FORCE',
                         '1234', '123456', NULL, 10.0, '2026-01-01',
                         'RADAR FULL HISTORY', 'RADAR', 'RADAR', 'USA_SPENDING', 2026)
                    ) AS t(contract_id, award_key, vendor_name, vendor_cage, parent_agency,
                           sub_agency, psc, naics_code, platform_family, total_spend,
                           last_action_date, base_award_description,
                           latest_action_description, description, source_system, year)
                ) TO ? (FORMAT PARQUET)
                """,
                [str(root / "contracts_rolled.parquet")],
            )
            connection.execute(
                """
                COPY (
                    SELECT * FROM (VALUES
                        ('RECENT-1', 'A2', 'CURRENT VENDOR', '22222', 'DOD', 'NAVY',
                         '1234', '123456', 'TEST PLATFORM', 20.0, '2026-02-01',
                         'AIRBORNE RADAR', 'RADAR SYSTEM', 'RADAR SYSTEM',
                         'AIRBORNE RADAR RADAR SYSTEM')
                    ) AS t(contract_id, award_key, vendor_name, vendor_cage, parent_agency,
                           sub_agency, psc, naics_code, platform_family, total_spend,
                           last_action_date, base_award_description,
                           latest_action_description, description, search_text)
                ) TO ? (FORMAT PARQUET)
                """,
                [str(root / "recent_awards_search.parquet")],
            )
            connection.execute(
                """
                COPY (SELECT '22222' cage_code, 'CURRENT VENDOR' vendor_name,
                             'DALLAS' city, 'TX' state)
                TO ? (FORMAT PARQUET)
                """,
                [str(root / "cage_locations.parquet")],
            )
            connection.execute(
                """
                COPY (SELECT 'PSC' classification_type, '1234' code,
                             'Radar equipment' description)
                TO ? (FORMAT PARQUET)
                """,
                [str(root / "classification_reference.parquet")],
            )
            connection.execute(
                """
                COPY (SELECT 'OPP-1' id, 'SOL-1' sol_num, 'Placeholder' title,
                             'DOD' agency, 'NAVY' sub_agency, '2027-01-01' deadline,
                             '1234' psc, '123456' naics, NULL set_aside_type,
                             'TX' state, 'https://example.gov' url,
                             'Placeholder' description, 'PLACEHOLDER' search_text)
                TO ? (FORMAT PARQUET)
                """,
                [str(root / "opportunities.parquet")],
            )
            connection.close()

            result = MarketRecordSearchStore(root).get(
                {
                    "record_type": "award",
                    "notice_type": "ANY",
                    "subject": "radar",
                    "terms": ["RADAR"],
                }
            )

            self.assertEqual(result["coverage"]["matching_records"], 1)
            self.assertEqual(result["records"][0]["record_id"], "RECENT-1")
            self.assertEqual(result["records"][0]["city"], "DALLAS")


if __name__ == "__main__":
    unittest.main()
