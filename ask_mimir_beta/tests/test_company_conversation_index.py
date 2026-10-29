import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from company_conversation_index import (
    INDEX_FILENAME,
    META_FILENAME,
    build_transaction_index,
    usable_transaction_index,
)
from company_context import file_sha256


class CompanyConversationIndexTests(unittest.TestCase):
    def test_sorted_index_requires_same_verified_transaction_release(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / "data"
            data.mkdir()
            raw = data / "transactions.parquet"
            connection = duckdb.connect()
            connection.execute(f"""
                COPY (
                    SELECT * FROM (VALUES
                        ('ZZZZZ', 'DLA', 'A2', 'T2', 'C2', DATE '2026-01-02',
                         'Navy', 'DOD', NULL, 'Motor', 20.0, '1234', 2026,
                         '123456789', '6105-00-123-4567',
                         CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)),
                        ('11111', 'USA_SPENDING', 'A1', 'T1', 'C1', DATE '2026-01-01',
                         'Air Force', 'DOD', 'Aircraft control', 'Control', 10.0,
                         '5678', 2026, NULL, NULL,
                         CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR))
                    ) t(vendor_cage, source_system, award_key, transaction_key,
                        contract_id, action_date, sub_agency, parent_agency,
                        base_award_description, description, spend_amount, psc,
                        year, niin, nsn, platform_family, action_description)
                ) TO '{raw}' (FORMAT PARQUET)
            """)
            connection.close()
            metadata = build_transaction_index(
                data, memory_limit="256MB", temp_directory=root / "scratch"
            )
            index = data / INDEX_FILENAME
            self.assertEqual(metadata["source_sha256"], file_sha256(raw))
            with duckdb.connect() as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT vendor_cage FROM read_parquet(?) ORDER BY vendor_cage",
                        [str(index)],
                    ).fetchall(),
                    [("11111",), ("ZZZZZ",)],
                )
            files = []
            for path in (raw, index, data / META_FILENAME):
                files.append({
                    "local_path": f"data/{path.name}",
                    "size": path.stat().st_size,
                    "sha256": file_sha256(path),
                })
            manifest = {"release_id": "index-test", "files": files}
            payload = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
            (root / "runtime_manifest.json").write_text(json.dumps(manifest))
            marker = {
                "release_id": "index-test",
                "manifest_fingerprint": hashlib.sha256(payload).hexdigest(),
            }
            (root / ".verified-release.json").write_text(json.dumps(marker))
            self.assertEqual(usable_transaction_index(data), index.resolve())
            metadata["source_sha256"] = "stale"
            (data / META_FILENAME).write_text(json.dumps(metadata))
            self.assertIsNone(usable_transaction_index(data))


if __name__ == "__main__":
    unittest.main()
