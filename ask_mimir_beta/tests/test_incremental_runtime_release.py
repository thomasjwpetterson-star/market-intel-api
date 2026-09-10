import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import publish_runtime_release
from publish_runtime_release import (
    DATA_FILES,
    artifact_files,
    manifest_file_map,
    parse_domains,
    remove_domain_entries,
    validate_release_manifest,
)


class FakeManifestS3:
    def __init__(self, sizes: dict[str, int]):
        self.sizes = sizes

    def head_object(self, **kwargs):
        return {"ContentLength": self.sizes[kwargs["Key"]]}


class IncrementalManifestTests(unittest.TestCase):
    def test_parses_named_domains(self):
        self.assertEqual(
            parse_domains("companies, platforms,companies"),
            {"companies", "platforms"},
        )

    def test_rejects_unknown_domain(self):
        with self.assertRaisesRegex(ValueError, "Unknown release domain"):
            parse_domains("companies,unknown")

    def test_removes_only_selected_domain_entries(self):
        entries = {
            "artifacts/company-context/manifest.json": {"value": 1},
            "artifacts/platform-contexts/manifest.json": {"value": 2},
            "data/transactions.parquet": {"value": 3},
        }
        remove_domain_entries(entries, "companies")
        self.assertNotIn("artifacts/company-context/manifest.json", entries)
        self.assertIn("artifacts/platform-contexts/manifest.json", entries)
        self.assertIn("data/transactions.parquet", entries)

    def test_validates_reused_and_new_manifest_entries(self):
        files = []
        sizes = {}
        for index, filename in enumerate(DATA_FILES, start=1):
            key = f"objects/{filename}"
            sizes[key] = index
            files.append(
                {
                    "local_path": f"data/{filename}",
                    "s3_key": key,
                    "size": index,
                    "sha256": "hash",
                    "s3_version_id": "version-1",
                }
            )
        manifest = {"release_id": "release-1", "files": files}
        validate_release_manifest(FakeManifestS3(sizes), "bucket", manifest)
        self.assertEqual(len(manifest_file_map(manifest)), len(DATA_FILES))

    def test_empty_artifact_selection_publishes_no_artifacts(self):
        with TemporaryDirectory() as directory:
            source = Path(directory)
            (source / "manifest.json").write_text("{}")
            with patch.object(
                publish_runtime_release,
                "artifact_directories",
                return_value=((source, "company-context"),),
            ):
                self.assertEqual(list(artifact_files(set())), [])


if __name__ == "__main__":
    unittest.main()
