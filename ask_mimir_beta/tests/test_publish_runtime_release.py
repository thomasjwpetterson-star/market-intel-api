import base64
import struct
import tempfile
import unittest
import zlib
from pathlib import Path

from publish_runtime_release import verified_serving_manifest_entry


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


class VerifiedServingManifestEntryTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
