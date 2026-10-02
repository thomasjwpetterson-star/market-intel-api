import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent
ACQUIRE = ROOT / "source_automation" / "glue_usaspending_prime_api_to_bronze.py"
TRANSFORM = ROOT / "source_automation" / "glue_usaspending_bronze_to_silver.py"


class UsaSpendingDailyPipelineTests(unittest.TestCase):
    def test_acquisition_uses_last_modified_date_and_records_the_cursor_type(self):
        source = ACQUIRE.read_text()
        ast.parse(source)
        self.assertIn('DATE_TYPE = "last_modified_date"', source)
        self.assertIn("OVERLAP_DAYS = 3", source)
        self.assertNotIn("timedelta(days=45)", source)
        self.assertIn('"date_type": DATE_TYPE', source)
        self.assertNotIn('"date_type": "action_date"', source)
        self.assertIn('"date_type": DATE_TYPE,', source)
        self.assertIn('status_url = info.get("status_url") or STATUS_ENDPOINT', source)
        self.assertIn("DOWNLOAD_RETRY_STATUS = {403, 404, 429, 500, 502, 503, 504}", source)
        self.assertIn("download_generated_zip(file_url)", source)

    def test_silver_merge_fails_closed_if_existing_partitions_cannot_be_read(self):
        source = TRANSFORM.read_text()
        ast.parse(source)
        self.assertIn("missing required columns", source)
        self.assertIn("rows without transaction keys", source)
        self.assertIn("allowed_partitions = {str(current_fy), str(current_fy - 1)}", source)
        self.assertIn("archive reconciliation owns older years", source)
        self.assertIn("df_existing = (", source)
        self.assertIn("df_existing.unionByName(df_new", source)
        self.assertNotIn("Writing new only", source)
        self.assertNotIn("except Exception as e", source)


if __name__ == "__main__":
    unittest.main()
