"""Exercise conditional promotion, rollback, and conflicting pointer changes."""

import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from release_pointer_control import (REQUIRED_COMPONENTS, REQUIRED_GATES,
                                     promote, rollback, snapshot, verify_snapshot)
from main_release_cache import PLATFORM_FILES, REQUIRED_FILES


def child_body(name):
    child = {"release_id": name + "-v2", "etl_run_id": "run-new"}
    if name == "main":
        child.update(schema_version=2, platform_reference_release_id="platform_reference-v2",
                     files=[{"local_path": filename, "s3_key": "cache/" + filename,
                             "s3_version_id": "v1", "size": 10, "sha256": "a" * 64,
                             **({"platform_reference_release_id": "platform_reference-v2"}
                                if filename in PLATFORM_FILES else {})}
                            for filename in sorted(REQUIRED_FILES)])
    elif name == "public":
        child["source_main_release_id"] = "main-v2"
    elif name == "ask_mimir":
        child.update(metric_release_id="metric-v2", files=[{"local_path": "data/a"}])
    else:
        child.update(downstream_cache_rebuild_status="BUILT_FRIDAY",
                     rebuilt_main_release_id="main-v2", financial_allocation_rows=0)
    return json.dumps(child, sort_keys=True).encode()


class FakeS3:
    def __init__(self, body: bytes):
        self.body = body
        self.version = 1

    def get_object(self, **kwargs):
        key = kwargs.get("Key", "")
        if key.startswith("releases/"):
            name = key.split("/")[1]
            if kwargs.get("VersionId") != "version-" + name:
                raise ValueError("wrong child version")
            body = child_body(name)
            return {"Body": io.BytesIO(body), "VersionId": "version-" + name}
        return {"Body": io.BytesIO(self.body),
                "ETag": '"' + hashlib.md5(self.body).hexdigest() + '"',
                "VersionId": str(self.version)}

    def put_object(self, *, Body, IfMatch, **_kwargs):
        expected = '"' + hashlib.md5(self.body).hexdigest() + '"'
        if IfMatch != expected:
            raise RuntimeError("conditional write conflict")
        self.body = Body
        self.version += 1
        return {"VersionId": str(self.version)}


def candidate(ready=True):
    return {"schema_version": 2, "release_id": "release-new",
            "etl_run_id": "run-new", "ask_metric_release_id": "metric-v2",
            "promotion_ready": ready,
            "components": {name: {"release_id": name + "-v2",
                                  "etl_run_id": "run-new",
                                  "immutable_manifest_key": "releases/" + name + "/manifest.json",
                                  "immutable_manifest_version_id": "version-" + name,
                                  "immutable_manifest_sha256": hashlib.sha256(child_body(name)).hexdigest()}
                           for name in REQUIRED_COMPONENTS},
            "promotion_gates": {name: True for name in REQUIRED_GATES}}


class PointerControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.journal = Path(self.temp.name) / "rollback.json"
        self.pointer = Path(self.temp.name) / "candidate.json"
        self.old = json.dumps({"release_id": "release-old", "components": {}}).encode()
        self.s3 = FakeS3(self.old)

    def test_candidate_promotes_and_exact_old_body_is_restored(self):
        snapshot(self.s3, "bucket", "current.json", self.journal)
        self.pointer.write_text(json.dumps(candidate()))
        result = promote(self.s3, self.pointer, self.journal)
        self.assertEqual(result["version_id"], "2")
        restored = rollback(self.s3, self.journal)
        self.assertEqual(restored["version_id"], "3")
        self.assertEqual(self.s3.body, self.old)

    def test_incomplete_release_cannot_promote(self):
        snapshot(self.s3, "bucket", "current.json", self.journal)
        self.pointer.write_text(json.dumps(candidate(ready=False)))
        with self.assertRaisesRegex(ValueError, "promotion-ready"):
            promote(self.s3, self.pointer, self.journal)
        self.assertEqual(self.s3.body, self.old)

    def test_child_manifest_change_cannot_promote(self):
        snapshot(self.s3, "bucket", "current.json", self.journal)
        root = candidate()
        root["components"]["public"]["immutable_manifest_sha256"] = "0" * 64
        self.pointer.write_text(json.dumps(root))
        with self.assertRaisesRegex(ValueError, "manifest hash"):
            promote(self.s3, self.pointer, self.journal)
        self.assertEqual(self.s3.body, self.old)

    def test_snapshot_is_read_only_and_cannot_overwrite_a_journal(self):
        snapshot(self.s3, "bucket", "current.json", self.journal)
        original = self.journal.read_bytes()
        self.assertTrue(verify_snapshot(self.s3, self.journal)["pointer_unchanged_since_snapshot"])
        with self.assertRaises(FileExistsError):
            snapshot(self.s3, "bucket", "current.json", self.journal)
        self.assertEqual(self.journal.read_bytes(), original)

    def test_stale_snapshot_cannot_overwrite_new_pointer(self):
        snapshot(self.s3, "bucket", "current.json", self.journal)
        self.s3.put_object(Body=b'{"release_id":"another"}', IfMatch='"' + hashlib.md5(self.old).hexdigest() + '"')
        self.pointer.write_text(json.dumps(candidate()))
        with self.assertRaisesRegex(RuntimeError, "changed after snapshot"):
            promote(self.s3, self.pointer, self.journal)

    def test_rollback_refuses_a_later_release(self):
        snapshot(self.s3, "bucket", "current.json", self.journal)
        self.pointer.write_text(json.dumps(candidate()))
        promote(self.s3, self.pointer, self.journal)
        current = self.s3.body
        self.s3.put_object(Body=b'{"release_id":"another"}', IfMatch='"' + hashlib.md5(current).hexdigest() + '"')
        with self.assertRaisesRegex(RuntimeError, "no longer contains"):
            rollback(self.s3, self.journal)


if __name__ == "__main__":
    unittest.main()
