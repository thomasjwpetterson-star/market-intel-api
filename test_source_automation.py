import csv
from datetime import date
import io
import tempfile
import unittest
from pathlib import Path
import zipfile

from source_automation.catalog import SOURCES
from source_automation.usaspending_archive import (
    bulk_awards_request,
    candidate_prefix,
    classify_columns,
    current_fiscal_year,
    fiscal_year_range,
    inspect_archive,
    reconciliation_fiscal_years,
)


PRIME_HEADER = [
    "contract_transaction_unique_key",
    "action_date_fiscal_year",
    "last_modified_date",
    "recipient_name",
]
SUBAWARD_HEADER = [
    "subaward_sam_report_id",
    "subaward_action_date_fiscal_year",
    "subaward_sam_report_last_modified_date",
    "prime_award_unique_key",
    "subawardee_name",
]


def csv_bytes(header, rows):
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(header)
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


class SourceAutomationTests(unittest.TestCase):
    def test_fiscal_year_boundary(self):
        self.assertEqual(current_fiscal_year(date(2026, 9, 30)), 2026)
        self.assertEqual(current_fiscal_year(date(2026, 10, 1)), 2027)
        self.assertEqual(reconciliation_fiscal_years(date(2026, 9, 24)), (2026, 2025))

    def test_bulk_request_asks_for_all_full_prime_and_procurement_subawards(self):
        request = bulk_awards_request(2026)
        filters = request["filters"]
        self.assertEqual(fiscal_year_range(2026), ("2025-10-01", "2026-09-30"))
        self.assertEqual(filters["agencies"][0]["name"], "All")
        self.assertEqual(filters["sub_award_types"], ["procurement"])
        self.assertIn("IDV_E", filters["prime_award_types"])
        self.assertEqual(request["columns"], [])

    def test_headers_classify_prime_and_subaward_without_filename_assumptions(self):
        self.assertEqual(classify_columns(PRIME_HEADER), "prime_contracts")
        self.assertEqual(classify_columns(SUBAWARD_HEADER), "sub_contracts")
        self.assertIsNone(classify_columns(["unrelated", "columns"]))

    def test_archive_requires_and_profiles_both_datasets(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            archive = Path(temp_dir) / "archive.zip"
            with zipfile.ZipFile(archive, "w") as zf:
                zf.writestr(
                    "prime.csv",
                    csv_bytes(PRIME_HEADER, [["p1", "2026", "2026-09-01", "Prime"]]),
                )
                zf.writestr(
                    "nested/sub.csv",
                    csv_bytes(
                        SUBAWARD_HEADER,
                        [["s1", "2026", "2026-09-02", "p1", "Sub"]],
                    ),
                )
            artifacts = inspect_archive(archive)
        self.assertEqual({item["dataset"] for item in artifacts}, {"prime_contracts", "sub_contracts"})
        self.assertTrue(all(item["row_count"] == 1 for item in artifacts))
        self.assertTrue(all(len(item["sha256"]) == 64 for item in artifacts))

    def test_candidate_path_cannot_alias_production_bronze(self):
        prefix = candidate_prefix("run-123")
        self.assertTrue(prefix.startswith("mimir/raw-source-candidates/"))
        self.assertNotIn("/app_cache/", prefix)
        self.assertNotEqual(prefix.split("/", 1)[0], "bronze")

    def test_source_roles_protect_canonical_and_leading_indicator_boundaries(self):
        self.assertEqual(SOURCES["usaspending-prime-daily"].role, "canonical")
        self.assertEqual(SOURCES["sam-contract-awards"].role, "enrichment")
        announcements = SOURCES["dod-contract-announcements"]
        self.assertEqual(announcements.cadence, "daily")
        self.assertEqual(announcements.status, "live-protected")
        self.assertEqual(announcements.downstream, ("ask-mimir",))

    def test_infrastructure_defaults_raw_source_automation_to_disabled(self):
        template = (
            Path(__file__).parent / "infrastructure" / "automated_etl_refresh.yaml"
        ).read_text()
        parameter = template.split("RawSourceScheduleState:", 1)[1].split(
            "UsaSpendingCandidateScriptKey:", 1
        )[0]
        self.assertIn("Default: DISABLED", parameter)
        self.assertIn("source-usaspending-archive", template)
        self.assertIn("mimir/raw-source-candidates/usaspending-contract-archive", template)
        self.assertNotIn("bronze/usaspending", template)
        self.assertNotIn("silver/usaspending", template)


if __name__ == "__main__":
    unittest.main()
