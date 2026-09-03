import json
import tempfile
import unittest
from pathlib import Path

from bootstrap_data import (
    manifest_fingerprint,
    verified_release_is_ready,
    write_verified_release_marker,
)


class BootstrapReleaseMarkerTests(unittest.TestCase):
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
