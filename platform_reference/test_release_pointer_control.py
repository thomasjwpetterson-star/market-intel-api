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
from main_release_cache import asset_fingerprint


def lookup_evidence(main):
    source = next(row for row in main["files"] if row["local_path"] == "contracts_rolled.parquet")
    lookup = next(row for row in main["files"] if row["local_path"] == "contracts_lookup.parquet")
    return {"source_asset_sha256": asset_fingerprint(source), "exact_parity": {
        "state": "ALL_PARTS_EXACT_PARITY_PASSED", "release_id": main["release_id"],
        "parts": [{"index": 0, "source": source, "candidate": lookup["parts"][0],
            "all_values_equal": True, "duplicate_id_selection_order_preserved": True,
            "rows_compared": 2, "exit_code": 0, "stop_guard": None}]},
        "offline_queries": {"state": "PASSED", "all_values_equal": True,
            "analytic_source_unchanged": True, "actual_profile_dto_parity": 3,
            "mixed": {"count": 2}}, "query_resources": {"exit_code": 0,
            "stop_guard": None, "peak_process_rss_bytes": 10}}


def child_body(name, reference_status="BUILT_FRIDAY"):
    child = {"release_id": name + "-v2", "etl_run_id": "run-new"}
    if name == "main":
        child.update(schema_version=2, platform_reference_release_id="platform_reference-v2",
                     files=[{"local_path": filename, "s3_key": "cache/" + filename,
                             "s3_version_id": "v1", "size": 10, "sha256": "a" * 64,
                             **({"platform_reference_release_id": "platform_reference-v2"}
                                if filename in PLATFORM_FILES else {})}
                            for filename in sorted(REQUIRED_FILES)])
        source = next(row for row in child["files"] if row["local_path"] == "contracts_rolled.parquet")
        child["files"].append({"local_path": "contracts_lookup.parquet",
            "platform_reference_release_id": child["platform_reference_release_id"],
            "source_asset_sha256": asset_fingerprint(source), "parts": [{
                "s3_key": "lookup/part.parquet", "s3_version_id": "lookup-v1",
                "size": 10, "etag": "lookup", "sha256": "b" * 64}]})
    elif name == "public":
        child["source_main_release_id"] = "main-v2"
    elif name == "ask_mimir":
        child.update(metric_release_id="metric-v2", files=[{"local_path": "data/a"}])
    else:
        child.update(downstream_cache_rebuild_status=reference_status,
                     rebuilt_main_release_id="main-v2", financial_allocation_rows=0)
    return json.dumps(child, sort_keys=True).encode()


class FakeS3:
    def __init__(self, body: bytes, reference_status="BUILT_FRIDAY"):
        self.body = body
        self.version = 1
        self.reference_status = reference_status

    def get_object(self, **kwargs):
        key = kwargs.get("Key", "")
        if key == "lookup-proof.json":
            return {"Body": io.BytesIO(json.dumps(lookup_evidence(json.loads(child_body("main")))).encode())}
        if key.startswith("releases/"):
            name = key.split("/")[1]
            if kwargs.get("VersionId") != "version-" + name:
                raise ValueError("wrong child version")
            body = child_body(name, self.reference_status)
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


def candidate(ready=True, reference_status="BUILT_FRIDAY"):
    return {"schema_version": 2, "release_id": "release-new",
            "etl_run_id": "run-new", "ask_metric_release_id": "metric-v2",
            "promotion_ready": ready,
            "contract_lookup_validation": {"s3_key": "lookup-proof.json", "s3_version_id": "proof-v1",
                "sha256": hashlib.sha256(json.dumps(lookup_evidence(json.loads(child_body("main")))).encode()).hexdigest()},
            "components": {name: {"release_id": name + "-v2",
                                  "etl_run_id": "run-new",
                                  "immutable_manifest_key": "releases/" + name + "/manifest.json",
                                  "immutable_manifest_version_id": "version-" + name,
                                  "immutable_manifest_sha256": hashlib.sha256(
                                      child_body(name, reference_status)).hexdigest()}
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
        result = promote(self.s3, self.pointer, self.journal,
                         approved_release_id="release-new")
        self.assertEqual(result["version_id"], "2")
        restored = rollback(self.s3, self.journal)
        self.assertEqual(restored["version_id"], "3")
        self.assertEqual(self.s3.body, self.old)

    def test_later_same_run_cache_rebuild_is_accepted(self):
        self.s3 = FakeS3(self.old, reference_status="BUILT_SAME_RUN")
        snapshot(self.s3, "bucket", "current.json", self.journal)
        self.pointer.write_text(json.dumps(candidate(reference_status="BUILT_SAME_RUN")))
        result = promote(self.s3, self.pointer, self.journal,
                         approved_release_id="release-new")
        self.assertEqual(result["state"], "PROMOTED")

    def test_missing_same_run_cache_rebuild_is_rejected(self):
        self.s3 = FakeS3(self.old, reference_status="MISSING")
        snapshot(self.s3, "bucket", "current.json", self.journal)
        self.pointer.write_text(json.dumps(candidate(reference_status="MISSING")))
        with self.assertRaisesRegex(ValueError, "same-run cache rebuild"):
            promote(self.s3, self.pointer, self.journal,
                    approved_release_id="release-new")

    def test_incomplete_release_cannot_promote(self):
        snapshot(self.s3, "bucket", "current.json", self.journal)
        self.pointer.write_text(json.dumps(candidate(ready=False)))
        with self.assertRaisesRegex(ValueError, "promotion-ready"):
            promote(self.s3, self.pointer, self.journal,
                    approved_release_id="release-new")
        self.assertEqual(self.s3.body, self.old)

    def test_child_manifest_change_cannot_promote(self):
        snapshot(self.s3, "bucket", "current.json", self.journal)
        root = candidate()
        root["components"]["public"]["immutable_manifest_sha256"] = "0" * 64
        self.pointer.write_text(json.dumps(root))
        with self.assertRaisesRegex(ValueError, "manifest hash"):
            promote(self.s3, self.pointer, self.journal,
                    approved_release_id="release-new")
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
            promote(self.s3, self.pointer, self.journal,
                    approved_release_id="release-new")

    def test_rollback_refuses_a_later_release(self):
        snapshot(self.s3, "bucket", "current.json", self.journal)
        self.pointer.write_text(json.dumps(candidate()))
        promote(self.s3, self.pointer, self.journal,
                approved_release_id="release-new")
        current = self.s3.body
        self.s3.put_object(Body=b'{"release_id":"another"}', IfMatch='"' + hashlib.md5(current).hexdigest() + '"')
        with self.assertRaisesRegex(RuntimeError, "no longer contains"):
            rollback(self.s3, self.journal)

    def test_promotion_requires_matching_explicit_release_approval(self):
        snapshot(self.s3, "bucket", "current.json", self.journal)
        self.pointer.write_text(json.dumps(candidate()))
        for approval in (None, "", "another-release"):
            with self.assertRaisesRegex(ValueError, "explicit approval"):
                promote(self.s3, self.pointer, self.journal,
                        approved_release_id=approval)
            self.assertEqual(self.s3.body, self.old)
            self.assertEqual(json.loads(self.journal.read_text())["state"],
                             "SNAPSHOT_ONLY")


if __name__ == "__main__":
    unittest.main()
