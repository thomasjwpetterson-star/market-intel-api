import io
import json
import unittest

from platform_manifest import PlatformManifestError, resolve_platform_release


class FakeS3:
    def __init__(self, objects):
        self.objects = objects
        self.calls = []

    def get_object(self, **request):
        self.calls.append(request)
        value = self.objects[request["Key"]]
        return {
            "Body": io.BytesIO(json.dumps(value).encode()),
            "VersionId": request.get("VersionId") or "resolved-version",
            "ETag": '"etag"',
        }


def platform_fixture(run_id="run-1"):
    references = {}
    objects = {}
    for component in ("main", "public", "ask_mimir"):
        release_id = f"{component}-{run_id}"
        key = f"mimir/{component}/releases/{release_id}/manifest.json"
        references[component] = {
            "release_id": release_id,
            "etl_run_id": run_id,
            "immutable_manifest_key": key,
        }
        objects[key] = {
            "release_id": release_id,
            "etl_run_id": run_id,
        }
    objects["test/platform.json"] = {
        "schema_version": 1,
        "release_id": f"platform-{run_id}",
        "etl_run_id": run_id,
        "components": references,
    }
    return objects


class PlatformManifestTests(unittest.TestCase):
    def test_resolves_one_versioned_root_and_matching_immutable_children(self):
        s3 = FakeS3(platform_fixture())

        resolved = resolve_platform_release(
            s3,
            "bucket",
            platform_key="test/platform.json",
            platform_version_id="platform-version-7",
            components=("main", "public", "ask_mimir"),
        )

        self.assertEqual(resolved["platform"]["etl_run_id"], "run-1")
        self.assertEqual(set(resolved["components"]), {"main", "public", "ask_mimir"})
        self.assertEqual(s3.calls[0]["VersionId"], "platform-version-7")
        self.assertTrue(
            all(
                manifest["etl_run_id"] == "run-1"
                for manifest in resolved["components"].values()
            )
        )

    def test_rejects_child_whose_body_does_not_match_the_root(self):
        objects = platform_fixture()
        ask_key = objects["test/platform.json"]["components"]["ask_mimir"][
            "immutable_manifest_key"
        ]
        objects[ask_key]["etl_run_id"] = "other-run"

        with self.assertRaisesRegex(PlatformManifestError, "other-run"):
            resolve_platform_release(
                FakeS3(objects),
                "bucket",
                platform_key="test/platform.json",
                components=("ask_mimir",),
            )

    def test_rejects_mutable_child_manifest_reference(self):
        objects = platform_fixture()
        objects["test/platform.json"]["components"]["main"][
            "immutable_manifest_key"
        ] = "mimir/runtime/candidate_manifest.json"

        with self.assertRaisesRegex(PlatformManifestError, "immutable"):
            resolve_platform_release(
                FakeS3(objects),
                "bucket",
                platform_key="test/platform.json",
                components=("main",),
            )


if __name__ == "__main__":
    unittest.main()
