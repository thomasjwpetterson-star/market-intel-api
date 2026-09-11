import base64
import hashlib
import json
import struct
import tempfile
import unittest
import zlib
from pathlib import Path

from publish_runtime_release import (
    CAPABILITY_DEFINITIONS,
    refresh_prebuilt_capability_metadata,
    remote_serving_manifest_entry,
    validate_prebuilt_capability_bundle,
    verified_serving_manifest_entry,
)


def crc32_base64(value: bytes) -> str:
    return base64.b64encode(
        struct.pack(">I", zlib.crc32(value) & 0xFFFFFFFF)
    ).decode()


class FakeS3:
    def __init__(self, payload: bytes, part_sizes: list[int]):
        self.payload = payload
        self.part_sizes = part_sizes

    def get_object_attributes(self, **_kwargs):
        parts = []
        offset = 0
        for number, size in enumerate(self.part_sizes, start=1):
            value = self.payload[offset : offset + size]
            parts.append(
                {
                    "PartNumber": number,
                    "Size": size,
                    "ChecksumCRC32": crc32_base64(value),
                }
            )
            offset += size
        return {
            "VersionId": "version-1",
            "ObjectSize": len(self.payload),
            "Checksum": {"ChecksumCRC32": crc32_base64(self.payload)},
            "ObjectParts": {"Parts": parts, "IsTruncated": False},
        }

    def head_object(self, **_kwargs):
        return {
            "ContentLength": len(self.payload),
            "VersionId": "version-1",
            "ETag": '"etag-1"',
            "ChecksumCRC32": crc32_base64(self.payload),
            "ChecksumType": "COMPOSITE",
            "Metadata": {},
        }


class VerifiedServingManifestEntryTests(unittest.TestCase):
    def test_pins_remote_object_without_downloading_it(self):
        payload = b"remote-release-input"
        entry = remote_serving_manifest_entry(
            FakeS3(payload, [len(payload)]),
            "bucket",
            "app_cache/input.parquet",
            "data/input.parquet",
        )

        self.assertEqual(entry["s3_version_id"], "version-1")
        self.assertEqual(entry["s3_etag"], "etag-1")
        self.assertEqual(entry["size"], len(payload))
        self.assertNotIn("sha256", entry)

    def test_verifies_and_pins_a_multipart_local_file(self):
        payload = b"atomic-release-input"
        with tempfile.TemporaryDirectory() as directory:
            local_file = Path(directory) / "input.parquet"
            local_file.write_bytes(payload)

            entry = verified_serving_manifest_entry(
                FakeS3(payload, [7, len(payload) - 7]),
                "bucket",
                "app_cache/input.parquet",
                "data/input.parquet",
                local_file,
            )

        self.assertEqual(entry["s3_version_id"], "version-1")
        self.assertEqual(entry["size"], len(payload))
        self.assertEqual(entry["local_path"], "data/input.parquet")

    def test_rejects_same_size_but_different_local_content(self):
        remote = b"authoritative-data"
        local = b"stale-local-data!!"
        self.assertEqual(len(remote), len(local))
        with tempfile.TemporaryDirectory() as directory:
            local_file = Path(directory) / "input.parquet"
            local_file.write_bytes(local)
            with self.assertRaisesRegex(RuntimeError, "does not match"):
                verified_serving_manifest_entry(
                    FakeS3(remote, [len(remote)]),
                    "bucket",
                    "app_cache/input.parquet",
                    "data/input.parquet",
                    local_file,
                )


class PrebuiltCapabilityBundleTests(unittest.TestCase):
    def _write_bundle(self, directory: Path) -> None:
        ontology_path = Path(__file__).resolve().parents[1] / "capability_ontology.json"
        ontology_body = ontology_path.read_bytes()
        (directory / "ontology.json").write_bytes(ontology_body)
        entries = []
        for capability_id, definition in CAPABILITY_DEFINITIONS.items():
            pack_path = directory / f"{capability_id}.json"
            pack_path.write_text(
                json.dumps(
                    {
                        "scope": {"capability_id": capability_id},
                        "coverage": {"matching_niins": 1},
                    }
                )
            )
            entries.append(
                {
                    "capability_id": capability_id,
                    "display_name": definition["display_name"],
                    "path": pack_path.name,
                    "matching_niins": 1,
                }
            )
        (directory / "manifest.json").write_text(
            json.dumps(
                {
                    "ontology_sha256": hashlib.sha256(ontology_body).hexdigest(),
                    "capabilities": entries,
                }
            )
        )

    def test_accepts_complete_bundle_matching_source_ontology(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            self._write_bundle(path)
            manifest = validate_prebuilt_capability_bundle(path)
        self.assertEqual(len(manifest["capabilities"]), len(CAPABILITY_DEFINITIONS))

    def test_rejects_bundle_missing_a_capability(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            self._write_bundle(path)
            manifest_path = path / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            manifest["capabilities"].pop()
            manifest_path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(RuntimeError, "contents do not match"):
                validate_prebuilt_capability_bundle(path)

    def test_refreshes_platform_breadth_without_rebuilding_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            self._write_bundle(path)
            fuel_pack = path / "aircraft_fuel_systems.json"
            pack = json.loads(fuel_pack.read_text())
            pack["top_platform_activity"] = [
                {"platform": "F-15", "matching_niin_count": 10},
                {"platform": "C-130", "matching_niin_count": 9},
                {"platform": "F-16", "matching_niin_count": 8},
            ]
            fuel_pack.write_text(json.dumps(pack))

            manifest = refresh_prebuilt_capability_metadata(path)
            refreshed = json.loads(fuel_pack.read_text())

        self.assertIn("metadata_refreshed_at", manifest)
        self.assertEqual(refreshed["platform_breadth"]["platforms_shown"], 3)
        self.assertFalse(
            refreshed["platform_breadth"][
                "single_platform_dominates_associations_shown"
            ]
        )


if __name__ == "__main__":
    unittest.main()
