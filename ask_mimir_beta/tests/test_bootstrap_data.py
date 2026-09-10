import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bootstrap_data import (
    DEFAULT_CURRENT_MANIFEST_KEY,
    _download_verified,
    manifest_entry_signature,
    manifest_fingerprint,
    selected_manifest_key,
    verified_release_is_ready,
    write_verified_release_marker,
)


class NoDownloadS3:
    def download_file(self, *_args, **_kwargs):
        raise AssertionError("unchanged pinned file should not be downloaded")


class RecordingDownloadS3:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.calls = []

    def download_file(self, bucket, key, destination, **kwargs):
        self.calls.append((bucket, key, kwargs))
        Path(destination).write_bytes(self.payload)


class BootstrapReleaseMarkerTests(unittest.TestCase):
    def test_reuses_unchanged_version_pinned_file_without_sha256(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / "data" / "sample.parquet"
            data.parent.mkdir(parents=True)
            data.write_bytes(b"version-pinned")
            entry = {
                "local_path": "data/sample.parquet",
                "s3_key": "app_cache/sample.parquet",
                "s3_version_id": "version-2",
                "s3_etag": "etag-2",
                "size": len(b"version-pinned"),
            }

            result = _download_verified(
                NoDownloadS3(),
                "bucket",
                entry,
                root,
                manifest_entry_signature(entry),
            )

            self.assertEqual(result.resolve(), data.resolve())

    def test_downloads_changed_version_pinned_file_without_sha256(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = root / "data" / "sample.parquet"
            data.parent.mkdir(parents=True)
            data.write_bytes(b"old-version")
            old_entry = {
                "local_path": "data/sample.parquet",
                "s3_key": "app_cache/sample.parquet",
                "s3_version_id": "version-1",
                "s3_etag": "etag-1",
                "size": len(b"old-version"),
            }
            new_entry = {
                **old_entry,
                "s3_version_id": "version-2",
                "s3_etag": "etag-2",
            }
            s3 = RecordingDownloadS3(b"new-version")

            _download_verified(
                s3,
                "bucket",
                new_entry,
                root,
                manifest_entry_signature(old_entry),
            )

            self.assertEqual(data.read_bytes(), b"new-version")
            self.assertEqual(
                s3.calls,
                [
                    (
                        "bucket",
                        "app_cache/sample.parquet",
                        {"ExtraArgs": {"VersionId": "version-2"}},
                    )
                ],
            )

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
