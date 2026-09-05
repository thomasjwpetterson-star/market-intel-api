import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bootstrap_data import (
    DEFAULT_CURRENT_MANIFEST_KEY,
    manifest_fingerprint,
    selected_manifest_key,
    verified_release_is_ready,
    write_verified_release_marker,
)


class BootstrapReleaseMarkerTests(unittest.TestCase):
    def test_current_release_pointer_is_the_default(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(selected_manifest_key(), DEFAULT_CURRENT_MANIFEST_KEY)

    def test_current_release_pointer_ignores_legacy_pin_by_default(self):
        with patch.dict(
            os.environ,
            {"ASK_MIMIR_MANIFEST_KEY": "legacy/release.json"},
            clear=True,
        ):
            self.assertEqual(selected_manifest_key(), DEFAULT_CURRENT_MANIFEST_KEY)

    def test_explicit_rollback_uses_immutable_pin(self):
        with patch.dict(
            os.environ,
            {
                "ASK_MIMIR_FOLLOW_CURRENT_RELEASE": "0",
                "ASK_MIMIR_PINNED_MANIFEST_KEY": "releases/known-good.json",
            },
            clear=True,
        ):
            self.assertEqual(selected_manifest_key(), "releases/known-good.json")

    def test_explicit_rollback_requires_a_pin(self):
        with patch.dict(
            os.environ,
            {"ASK_MIMIR_FOLLOW_CURRENT_RELEASE": "0"},
            clear=True,
        ):
            with self.assertRaises(RuntimeError):
                selected_manifest_key()

    def test_verified_release_marker_reuses_only_matching_complete_release(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / "data" / "sample.parquet"
            data.parent.mkdir(parents=True)
            data.write_bytes(b"verified-data")
            manifest = {
                "release_id": "release-a",
                "files": [
                    {
                        "local_path": "data/sample.parquet",
                        "size": len(b"verified-data"),
                        "sha256": "abc",
                        "s3_key": "release/sample.parquet",
                    }
                ],
            }
            marker = root / ".verified-release.json"

            self.assertFalse(verified_release_is_ready(root, manifest, marker))
            write_verified_release_marker(marker, manifest)
            self.assertTrue(verified_release_is_ready(root, manifest, marker))

            data.write_bytes(b"wrong")
            self.assertFalse(verified_release_is_ready(root, manifest, marker))

    def test_manifest_fingerprint_changes_with_release_contents(self):
        first = {"release_id": "a", "files": [{"local_path": "a", "size": 1}]}
        second = json.loads(json.dumps(first))
        second["files"][0]["size"] = 2
        self.assertNotEqual(manifest_fingerprint(first), manifest_fingerprint(second))


if __name__ == "__main__":
    unittest.main()
