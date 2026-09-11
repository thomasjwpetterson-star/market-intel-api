import sys
import tempfile
import unittest
from pathlib import Path

import duckdb


sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dod_contract_enrichment import (
    lookup_contract_announcements,
    lookup_scope_announcements,
)


class DodContractEnrichmentTests(unittest.TestCase):
    def test_matches_hyphenated_primary_and_secondary_contract_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "announcements.parquet"
            connection = duckdb.connect()
            connection.execute(
                """
                COPY (
                    SELECT 'ANN-1' announcement_id,
                           DATE '2026-09-10' announcement_date,
                           'AIR FORCE' service,
                           1 entry_index,
                           'award' entry_type,
                           'Example Corp.' recipient_text,
                           'FA8504-22-C-0001' primary_contract_id,
                           ['FA8504-22-C-0001', 'FA8504-22-D-0002'] contract_ids,
                           500000000.0 announced_value_usd,
                           25000000.0 obligated_at_announcement_usd,
                           'Work will be performed in Ohio.' work_locations,
                           'Expected completion is 2030.' completion_text,
                           'Two offers were received.' competition_text,
                           'The Air Force is the contracting activity.' contracting_activity,
                           'Aircraft engine support.' description,
                           'Contracts for Sept. 10, 2026' source_title,
                           'https://www.defense.gov/example' source_url,
                           '2026-09-10T21:00:00+00:00' source_published_at
                ) TO ? (FORMAT PARQUET)
                """,
                [str(path)],
            )
            connection.close()

            primary = lookup_contract_announcements(path, "FA850422C0001")
            secondary = lookup_contract_announcements(path, "FA850422D0002")

        self.assertEqual(primary[0]["announcement_id"], "ANN-1")
        self.assertEqual(secondary[0]["announcement_id"], "ANN-1")
        self.assertEqual(primary[0]["announced_value_usd"], 500000000.0)
        self.assertEqual(
            primary[0]["obligated_at_announcement_usd"], 25000000.0
        )

    def test_missing_dataset_is_an_empty_optional_enrichment(self):
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "missing.parquet"
            self.assertEqual(
                lookup_contract_announcements(missing, "FA850422C0001"),
                [],
            )

    def test_scope_lookup_uses_contract_ids_for_company_and_platform(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            announcements = root / "announcements.parquet"
            transactions = root / "transactions.parquet"
            connection = duckdb.connect()
            connection.execute(
                """
                COPY (
                    SELECT 'ANN-1' announcement_id,
                           DATE '2026-09-10' announcement_date,
                           'AIR FORCE' service,
                           1 entry_index,
                           'award' entry_type,
                           'Example Corp.' recipient_text,
                           'FA8504-22-C-0001' primary_contract_id,
                           ['FA8504-22-C-0001'] contract_ids,
                           500000000.0 announced_value_usd,
                           25000000.0 obligated_at_announcement_usd,
                           'Work will be performed in Ohio.' work_locations,
                           'Expected completion is 2030.' completion_text,
                           'Two offers were received.' competition_text,
                           'The Air Force is the contracting activity.' contracting_activity,
                           'Aircraft engine support.' description,
                           'Contracts for Sept. 10, 2026' source_title,
                           'https://www.defense.gov/example' source_url,
                           '2026-09-10T21:00:00+00:00' source_published_at
                ) TO ? (FORMAT PARQUET)
                """,
                [str(announcements)],
            )
            connection.execute(
                """
                COPY (
                    SELECT 'FA850422C0001' contract_id,
                           'EXAMPLE CORP.' vendor_name,
                           '1ABCD' vendor_cage,
                           'F-15' platform_family
                    UNION ALL
                    SELECT 'W000000000001', 'OTHER CORP.', '2BCDE', 'F-16'
                ) TO ? (FORMAT PARQUET)
                """,
                [str(transactions)],
            )
            connection.close()

            by_cage = lookup_scope_announcements(
                announcements, transactions, cage="1abcd"
            )
            by_name = lookup_scope_announcements(
                announcements, transactions, company_name="Example Corp"
            )
            by_platform = lookup_scope_announcements(
                announcements, transactions, platform="f-15"
            )
            unrelated = lookup_scope_announcements(
                announcements, transactions, platform="f-16"
            )

        self.assertEqual(by_cage[0]["announcement_id"], "ANN-1")
        self.assertEqual(by_name[0]["announcement_id"], "ANN-1")
        self.assertEqual(by_platform[0]["announcement_id"], "ANN-1")
        self.assertEqual(unrelated, [])


if __name__ == "__main__":
    unittest.main()
