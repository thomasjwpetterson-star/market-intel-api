import hashlib
import io
import json
import unittest
from unittest.mock import Mock, patch

from opportunity_intelligence.api import Repository
from opportunity_intelligence.aws import PREFIX
from opportunity_intelligence.company_index import IndexUnavailable
from opportunity_intelligence.identity import _repository as main_identity_repository
from opportunity_intelligence.mount import mount


class FakeStorage:
    def __init__(self):
        self.keys = []
        self.contents = {}

    def get_object(self, *, Bucket, Key):
        self.keys.append(Key)
        return {"Body": io.BytesIO(self.contents[Key])}

    def write(self, key, value):
        raw = json.dumps(value, sort_keys=True).encode()
        self.contents[key] = raw
        return hashlib.sha256(raw).hexdigest()


class PrivatePointerTest(unittest.TestCase):
    def setUp(self):
        self.aws = type("AWS", (), {"s3": FakeStorage()})()
        for pointer, release in (("current.json", "legacy"), ("releases/private-current.json", "canonical")):
            key = f"{PREFIX}/releases/{release}/manifest.json"
            digest = self.aws.s3.write(key, {"run_id": release, "public_release_approved": False})
            self.aws.s3.write(f"{PREFIX}/{pointer}", {"run_id": release, "manifest_key": key,
                                                        "manifest_sha256": digest, "private": True})

    def test_private_mount_and_main_identity_have_independent_release_selectors(self):
        main = Repository(self.aws)
        private = Repository(self.aws, private=True)
        self.assertEqual(main.manifest()["run_id"], "legacy")
        self.assertEqual(private.manifest()["run_id"], "canonical")
        self.assertFalse(main_identity_repository.private)
        with patch("opportunity_intelligence.mount.create_router") as create_router:
            mount(Mock())
        self.assertTrue(create_router.call_args.args[1].private)
        self.assertIn(PREFIX + "/current.json", self.aws.s3.keys)
        self.assertIn(PREFIX + "/releases/private-current.json", self.aws.s3.keys)

    def test_private_release_rejects_manifest_bytes_changed_after_verification(self):
        self.aws.s3.write(f"{PREFIX}/releases/canonical/manifest.json",
                          {"run_id": "canonical", "public_release_approved": False, "changed": True})
        with self.assertRaises(IndexUnavailable):
            Repository(self.aws, private=True).manifest()
        self.assertEqual(Repository(self.aws).manifest()["run_id"], "legacy")

    def test_private_release_rejects_public_release(self):
        key = f"{PREFIX}/releases/canonical/manifest.json"
        digest = self.aws.s3.write(key, {"run_id": "canonical", "public_release_approved": True})
        self.aws.s3.write(f"{PREFIX}/releases/private-current.json",
                          {"run_id": "canonical", "manifest_key": key, "manifest_sha256": digest, "private": True})
        with self.assertRaises(IndexUnavailable):
            Repository(self.aws, private=True).manifest()
