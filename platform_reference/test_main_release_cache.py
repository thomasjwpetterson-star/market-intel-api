import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from main_release_cache import (PLATFORM_FILES, REQUIRED_FILES,
                                get_candidate_main_manifest, get_main_manifest,
                                stage_main_cache, validate_manifest)


class FakeS3:
    def __init__(self, objects):
        self.objects = objects

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key]["body"])}

    def head_object(self, Bucket, Key, VersionId):
        obj = self.objects[Key]
        if VersionId != obj["version"]:
            raise ValueError("wrong S3 version")
        return {"ContentLength": len(obj["body"]), "ETag": '"' + obj["etag"] + '"'}

    def download_file(self, bucket, key, destination, ExtraArgs):
        obj = self.objects[key]
        if ExtraArgs["VersionId"] != obj["version"]:
            raise ValueError("wrong S3 version")
        Path(destination).write_bytes(obj["body"])


class MainReleaseCacheTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        sample = Path(self.temp.name) / "sample.parquet"
        duckdb.connect().execute(f"COPY (SELECT 1 AS x) TO '{sample}' (FORMAT PARQUET)")
        body = sample.read_bytes()
        self.objects = {}
        files = []
        for name in sorted(REQUIRED_FILES):
            key = f"cache/{name}"
            self.objects[key] = {"body": body, "version": "v1", "etag": "etag1"}
            base = {"s3_key": key, "s3_version_id": "v1", "size": len(body),
                    "sha256": hashlib.sha256(body).hexdigest()}
            if name in PLATFORM_FILES:
                base["platform_reference_release_id"] = "platform-v7"
            if name == "transactions.parquet":
                files.append({"local_path": name, "platform_reference_release_id": "platform-v7",
                              "parts": [{"s3_key": key, "s3_version_id": "v1",
                                         "size": len(body), "etag": "etag1"}]})
            else:
                files.append({"local_path": name, **base})
        self.manifest = {"schema_version": 2, "release_id": "main-v7",
                         "release_status": "CANDIDATE_ONLY", "customer_facing": False,
                         "platform_reference_release_id": "platform-v7", "files": files}

    def tearDown(self):
        self.temp.cleanup()

    def test_stage_complete_versioned_release_with_multipart_cache(self):
        root = Path(self.temp.name) / "releases"
        release = stage_main_cache(FakeS3(self.objects), "bucket", self.manifest, root)
        self.assertTrue((release / ".verified-manifest.json").exists())
        self.assertEqual(duckdb.connect().execute(
            "SELECT x FROM read_parquet(?)", [str(release / "transactions.parquet")]).fetchall(), [(1,)])
        self.assertEqual(stage_main_cache(FakeS3(self.objects), "bucket", self.manifest, root), release)

    def test_verified_marker_does_not_hide_missing_cache_file(self):
        root = Path(self.temp.name) / "missing-releases"
        release = stage_main_cache(FakeS3(self.objects), "bucket", self.manifest, root)
        (release / "products.parquet").unlink()
        with self.assertRaisesRegex(ValueError, "existing unverified"):
            stage_main_cache(FakeS3(self.objects), "bucket", self.manifest, root)

    def test_failed_parallel_download_never_selects_partial_release(self):
        root = Path(self.temp.name) / "failed-releases"
        key = "cache/products.parquet"
        original = self.objects[key]["body"]
        self.objects[key]["body"] = b"BAD!" + original[4:]
        with self.assertRaises(ValueError):
            stage_main_cache(FakeS3(self.objects), "bucket", self.manifest, root)
        self.assertFalse((root / "main-v7").exists())
        self.assertFalse((root / "main-v7.staging").exists())

    def test_missing_or_mismatched_platform_file_fails_closed(self):
        wrong = json.loads(json.dumps(self.manifest))
        next(row for row in wrong["files"] if row["local_path"] in PLATFORM_FILES)[
            "platform_reference_release_id"] = "old"
        with self.assertRaises(ValueError):
            validate_manifest(wrong)
        missing = json.loads(json.dumps(self.manifest))
        missing["files"].pop()
        with self.assertRaises(ValueError):
            validate_manifest(missing)

    def test_root_and_main_release_must_agree(self):
        self.objects["mimir/platform/current_manifest.json"] = {
            "body": json.dumps({"schema_version": 2, "promotion_ready": True,
                "components": {"main": {"release_id": "main-v7",
                    "immutable_manifest_key": "mimir/releases/main-v7/manifest.json"},
                    "platform_reference": {"release_id": "platform-v7"}}}).encode(),
            "version": "v1", "etag": "etag1"}
        self.objects["mimir/releases/main-v7/manifest.json"] = {
            "body": json.dumps(self.manifest).encode(), "version": "v1", "etag": "etag1"}
        s3 = FakeS3(self.objects)
        self.assertEqual(get_main_manifest(s3, "bucket", "mimir/platform/current_manifest.json"), self.manifest)
        self.manifest["platform_reference_release_id"] = "other"
        self.objects["mimir/releases/main-v7/manifest.json"]["body"] = json.dumps(self.manifest).encode()
        with self.assertRaises(ValueError):
            get_main_manifest(s3, "bucket", "mimir/platform/current_manifest.json")

    def test_candidate_preview_requires_immutable_candidate_manifest(self):
        key = "mimir/releases/main-v7/manifest.json"
        self.objects[key] = {"body": json.dumps(self.manifest).encode(),
                             "version": "v1", "etag": "etag1"}
        s3 = FakeS3(self.objects)
        self.assertEqual(get_candidate_main_manifest(s3,"bucket",key),self.manifest)
        with self.assertRaises(ValueError):
            get_candidate_main_manifest(s3,"bucket","mimir/current_manifest.json")


if __name__ == "__main__":
    unittest.main()
