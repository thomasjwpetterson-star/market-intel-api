import hashlib
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

if importlib.util.find_spec("fastapi") is None:
    raise unittest.SkipTest("FastAPI runtime dependencies are not installed")

import main


class FileBackedS3:
    def __init__(self, objects):
        self.objects = objects
        self.calls = []

    def download_file(self, bucket, key, destination, ExtraArgs=None):
        self.calls.append((bucket, key, ExtraArgs))
        Path(destination).write_bytes(self.objects[key])


class PlatformConsumerTests(unittest.TestCase):
    def test_public_child_is_hash_verified_schema_checked_and_installed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            builder = duckdb.connect()
            artifacts = []
            objects = {}
            for table_name in sorted(main.PUBLIC_PLATFORM_TABLES):
                path = source / f"{table_name}.parquet"
                builder.execute(
                    "COPY (SELECT CAST(1 AS INTEGER) AS value) TO ? (FORMAT PARQUET)",
                    [str(path)],
                )
                body = path.read_bytes()
                key = f"rehearsal/public/{path.name}"
                objects[key] = body
                schema = [
                    {"name": row[0], "type": row[1], "nullable": row[2]}
                    for row in builder.execute(
                        "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
                    ).fetchall()
                ]
                artifacts.append(
                    {
                        "table_name": table_name,
                        "filename": path.name,
                        "s3_key": key,
                        "s3_version_id": "version-1",
                        "size": len(body),
                        "sha256": hashlib.sha256(body).hexdigest(),
                        "row_count": 1,
                        "schema": schema,
                    }
                )
            builder.close()
            manifest = {
                "release_id": "public-run-1",
                "etl_run_id": "run-1",
                "release": {"release_id": "public-run-1", "total_entries": 1},
                "artifacts": artifacts,
            }
            serving = duckdb.connect()
            fake_s3 = FileBackedS3(objects)

            with patch.object(main, "LOCAL_CACHE_DIR", root / "runtime"), patch.object(
                main, "s3", fake_s3
            ):
                release = main.load_public_platform_release(serving, manifest)

            self.assertEqual(release["release_id"], "public-run-1")
            self.assertEqual(
                serving.execute(
                    "SELECT COUNT(*) FROM public_intelligence_manifest"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(len(fake_s3.calls), len(main.PUBLIC_PLATFORM_TABLES))
            serving.close()

    def test_main_component_rejects_nested_or_duplicate_runtime_paths(self):
        with self.assertRaisesRegex(main.PlatformManifestError, "Unsafe"):
            main._manifest_file_map(
                {"files": [{"local_path": "../summary.parquet"}]}
            )
        with self.assertRaisesRegex(main.PlatformManifestError, "Duplicate"):
            main._manifest_file_map(
                {
                    "files": [
                        {"local_path": "summary.parquet"},
                        {"local_path": "summary.parquet"},
                    ]
                }
            )


if __name__ == "__main__":
    unittest.main()
