"""The new four-component root must resolve as one pinned release."""

import hashlib
import io
import json
import unittest

from platform_manifest import PlatformManifestError, resolve_platform_release


class FakeS3:
    def __init__(self, objects):
        self.objects = objects

    def get_object(self, *, Bucket, Key, VersionId=None):
        body, version = self.objects[Key]
        if VersionId is not None and VersionId != version:
            raise ValueError("incorrect object version")
        return {"Body": io.BytesIO(body), "VersionId": version}


def encoded(value):
    return json.dumps(value, sort_keys=True).encode()


class PlatformManifestV2Tests(unittest.TestCase):
    def setUp(self):
        self.run_id = "friday"
        self.objects = {}
        references = {}
        for name in ("main", "public", "ask_mimir", "platform_reference"):
            release_id = f"{name}-v7"
            key = f"mimir/releases/{release_id}/manifest.json"
            body = encoded({"release_id": release_id, "etl_run_id": self.run_id})
            self.objects[key] = (body, f"version-{name}")
            references[name] = {
                "release_id": release_id,
                "etl_run_id": self.run_id,
                "immutable_manifest_key": key,
                "immutable_manifest_version_id": f"version-{name}",
                "immutable_manifest_sha256": hashlib.sha256(body).hexdigest(),
            }
        self.root = {
            "schema_version": 2, "release_id": "platform-friday-v7",
            "etl_run_id": self.run_id, "promotion_ready": False,
            "components": references,
        }
        self.write_root()

    def write_root(self):
        self.objects["candidate/root.json"] = (encoded(self.root), "root-v1")

    def resolve(self, **kwargs):
        return resolve_platform_release(FakeS3(self.objects), "bucket",
                                        platform_key="candidate/root.json", **kwargs)

    def test_private_candidate_requires_explicit_preview(self):
        with self.assertRaisesRegex(PlatformManifestError, "promotion-ready"):
            self.resolve(components=("main",))
        result = self.resolve(components=("main", "public", "ask_mimir", "platform_reference"),
                              allow_candidate_preview=True)
        self.assertEqual(result["components"]["main"]["release_id"], "main-v7")
        self.assertEqual(len(result["components"]), 4)

    def test_child_change_or_missing_version_fails_closed(self):
        self.root["components"]["main"]["immutable_manifest_sha256"] = "0" * 64
        self.write_root()
        with self.assertRaisesRegex(PlatformManifestError, "SHA-256"):
            self.resolve(components=("main",), allow_candidate_preview=True)
        self.root["components"]["main"].pop("immutable_manifest_version_id")
        self.write_root()
        with self.assertRaisesRegex(PlatformManifestError, "version or SHA-256"):
            self.resolve(components=("main",), allow_candidate_preview=True)

    def test_promotion_ready_root_resolves_without_preview(self):
        self.root["promotion_ready"] = True
        self.write_root()
        self.assertEqual(self.resolve(components=("ask_mimir",))["platform"]["release_id"],
                         "platform-friday-v7")


if __name__ == "__main__":
    unittest.main()
