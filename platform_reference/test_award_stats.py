import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from platform_reference.award_stats import count_contract_rows


class AwardStatsTests(unittest.TestCase):
    def write_part(self, path: Path, rows: int) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"contract_id": list(range(rows))}), path)

    def test_single_file(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.write_part(root / "contracts_rolled.parquet", 3)
            self.assertEqual(count_contract_rows(root), 3)

    def test_release_directory(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.write_part(root / "contracts_rolled.parquet" / "part-00000.parquet", 3)
            self.write_part(root / "contracts_rolled.parquet" / "part-00001.parquet", 5)
            self.assertEqual(count_contract_rows(root), 8)

    def test_legacy_directory(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.write_part(root / "contracts_rolled" / "part-00000.parquet", 4)
            self.assertEqual(count_contract_rows(root), 4)

    def test_missing_cache_fails_closed(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(FileNotFoundError):
                count_contract_rows(Path(folder))

    def test_empty_cache_fails_closed(self):
        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / "contracts_rolled.parquet").mkdir()
            with self.assertRaises(FileNotFoundError):
                count_contract_rows(Path(folder))

    def test_zero_row_file_fails_closed(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.write_part(root / "contracts_rolled.parquet", 0)
            with self.assertRaises(ValueError):
                count_contract_rows(root)


if __name__ == "__main__":
    unittest.main()
