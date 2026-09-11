import io
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from glue_dod_contract_announcements_daily import (
    ANNOUNCEMENT_LOCAL_PATH,
    APP_CACHE_KEY,
    publish_runtime_manifest,
    run,
)


class GlueDodContractAnnouncementTests(unittest.TestCase):
    def test_publish_replaces_only_the_announcement_entry(self):
        s3 = MagicMock()
        base = {
            "release_id": "base-release",
            "metric_release_id": "metrics-1",
            "derived_release": {"kept": True},
            "files": [
                {
                    "local_path": "data/summary.parquet",
                    "s3_key": "app_cache/summary.parquet",
                    "size": 10,
                    "s3_version_id": "summary-v1",
                    "s3_etag": "summary-etag",
                },
                {
                    "local_path": ANNOUNCEMENT_LOCAL_PATH,
                    "s3_key": APP_CACHE_KEY,
                    "size": 5,
                    "s3_version_id": "old-v1",
                },
            ],
        }
        s3.get_object.return_value = {"Body": io.BytesIO(json.dumps(base).encode())}

        def head_object(**request):
            if request["Key"] == APP_CACHE_KEY:
                return {
                    "ContentLength": 20,
                    "VersionId": "announcement-v2",
                    "ETag": '"announcement-etag"',
                    "ChecksumCRC32": "checksum",
                    "ChecksumType": "FULL_OBJECT",
                }
            return {"ContentLength": 10, "ETag": '"summary-etag"'}

        s3.head_object.side_effect = head_object
        result = publish_runtime_manifest(s3, "bucket")

        self.assertEqual(result["base_release_id"], "base-release")
        self.assertEqual(s3.put_object.call_count, 3)
        current_call = s3.put_object.call_args_list[-1].kwargs
        manifest = json.loads(current_call["Body"])
        self.assertEqual(manifest["updated_domains"], ["announcements"])
        self.assertEqual(manifest["derived_release"], {"kept": True})
        announcement = next(
            item for item in manifest["files"]
            if item["local_path"] == ANNOUNCEMENT_LOCAL_PATH
        )
        self.assertEqual(announcement["s3_version_id"], "announcement-v2")

    @patch("glue_dod_contract_announcements_daily._trigger_render_deploy")
    @patch("glue_dod_contract_announcements_daily._secret_value")
    @patch("glue_dod_contract_announcements_daily.publish_runtime_manifest")
    @patch("glue_dod_contract_announcements_daily.ingest")
    @patch("glue_dod_contract_announcements_daily._download_existing")
    @patch("glue_dod_contract_announcements_daily.boto3.Session")
    def test_run_publishes_before_triggering_consumers(
        self,
        session,
        download_existing,
        ingest,
        publish_runtime,
        secret_value,
        trigger,
    ):
        s3 = MagicMock()
        secrets = MagicMock()
        session.return_value.client.side_effect = [s3, secrets]
        download_existing.return_value = True

        def write_output(output_dir, **_kwargs):
            (output_dir / "dod_contract_announcements.parquet").write_bytes(b"data")
            return {"total_entries": 12}

        ingest.side_effect = write_output
        publish_runtime.return_value = {"release_id": "release-2"}
        secret_value.side_effect = ["https://example.test/ask", "https://example.test/api"]
        trigger.side_effect = [202, 201]

        result = run(
            bucket="bucket",
            ask_mimir_hook_secret="ask-secret",
            main_api_hook_secret="api-secret",
        )

        self.assertTrue(result["history_loaded"])
        s3.upload_file.assert_called_once()
        self.assertEqual(s3.upload_file.call_args.args[2], APP_CACHE_KEY)
        self.assertEqual(result["publication"]["release_id"], "release-2")
        self.assertEqual(trigger.call_count, 2)


if __name__ == "__main__":
    unittest.main()
