import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


from refresh_dod_contract_announcements import refresh


class RefreshDodContractAnnouncementsTests(unittest.TestCase):
    @patch("refresh_dod_contract_announcements.trigger_render_deploy")
    @patch("refresh_dod_contract_announcements.publish")
    @patch("refresh_dod_contract_announcements.ingest")
    @patch("refresh_dod_contract_announcements._download_existing")
    @patch("refresh_dod_contract_announcements._session")
    def test_refresh_merges_publishes_and_restarts_both_consumers(
        self,
        session,
        download_existing,
        ingest,
        publish,
        trigger_render_deploy,
    ):
        s3 = MagicMock()
        session.return_value.client.return_value = s3
        download_existing.return_value = True
        publish.return_value = {"release_id": "release-1"}
        trigger_render_deploy.return_value = 201

        def write_output(output_dir, **_kwargs):
            (output_dir / "dod_contract_announcements.parquet").write_bytes(b"parquet")
            return {"total_entries": 12, "release_id": "announcements-1"}

        ingest.side_effect = write_output

        with tempfile.TemporaryDirectory() as directory:
            result = refresh(
                Path(directory),
                bucket="example-bucket",
                promote=True,
                ask_mimir_deploy_hook_url="https://example.test/ask",
                main_api_deploy_hook_url="https://example.test/api",
            )

        self.assertTrue(result["history_loaded"])
        self.assertEqual(result["publication"]["release_id"], "release-1")
        ingest.assert_called_once()
        self.assertTrue(ingest.call_args.kwargs["fail_on_fetch_error"])
        s3.upload_file.assert_called_once()
        self.assertEqual(
            s3.upload_file.call_args.args[2],
            "app_cache/dod_contract_announcements.parquet",
        )
        publish.assert_called_once_with(
            "example-bucket",
            None,
            deploy_hook_url="https://example.test/ask",
            domains={"announcements"},
            promote=True,
            verify_local_inputs=False,
        )
        trigger_render_deploy.assert_called_once_with("https://example.test/api")

    @patch("refresh_dod_contract_announcements.ingest")
    @patch("refresh_dod_contract_announcements._download_existing")
    @patch("refresh_dod_contract_announcements._session")
    def test_refresh_rejects_an_empty_serving_result(
        self,
        session,
        download_existing,
        ingest,
    ):
        session.return_value.client.return_value = MagicMock()
        download_existing.return_value = False
        ingest.return_value = {"total_entries": 0}

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(RuntimeError, "produced no serving records"):
                refresh(Path(directory), bucket="example-bucket")


if __name__ == "__main__":
    unittest.main()
