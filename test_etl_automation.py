import json
import io
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from etl_automation.baseline import manual_environment, seed_manual_control
from etl_automation.athena_retry import is_retryable_athena_reason
from etl_automation.athena_view_repair import (
    assert_preserves_schemas,
    repair_source_sql,
    replace_relation,
    schema_delta,
)
from etl_automation.comparison import DEEP_COMPARE_FILES, _deep_metrics, _metric_deltas
from etl_automation.metadata import parquet_object_metadata
from etl_automation.orchestrator import (
    phase_environment,
    publish_ask_candidate,
    refresh_operational_candidate,
    stage_operational_sidecars,
    staging_cache_prefix,
)
from etl_automation.platform_release import (
    PLATFORM_CURRENT_KEY,
    PlatformReleaseValidationError,
    build_platform_manifest,
    promote_platform_manifest,
    publish_platform_candidate,
    rollback_platform_manifest,
)
from etl_automation.preflight import (
    GlueGate,
    UpstreamValidationError,
    validate_athena_serving_views,
    validate_glue_jobs,
    validate_state_machine,
)
from etl_automation.release import (
    Artifact,
    CandidateValidationError,
    MAIN_CURRENT_KEY,
    build_candidate_manifest,
    publish_candidate_manifest,
)


class FakeS3:
    def __init__(self, objects):
        self.objects = objects
        self.puts = []
        self.copies = []

    def head_object(self, Bucket, Key, ChecksumMode=None, VersionId=None):
        del Bucket, ChecksumMode, VersionId
        if Key not in self.objects:
            raise KeyError(Key)
        return self.objects[Key]

    def put_object(self, **kwargs):
        self.puts.append(kwargs)
        return {"VersionId": f"put-version-{len(self.puts)}"}

    def get_object(self, Bucket, Key):
        del Bucket
        value = self.objects[Key]
        body = value.get("Body", value)
        if isinstance(body, str):
            body = body.encode("utf-8")
        return {"Body": io.BytesIO(body)}

    def copy(self, source, bucket, key, ExtraArgs=None):
        self.copies.append(
            {
                "source": source,
                "bucket": bucket,
                "key": key,
                "extra_args": ExtraArgs,
            }
        )

    def copy_object(self, **kwargs):
        self.copies.append(kwargs)
        return {"VersionId": f"copy-version-{len(self.copies)}"}


class FakeGlue:
    def __init__(self, runs):
        self.runs = runs

    def get_job_runs(self, JobName, MaxResults):
        self.max_results = MaxResults
        return {"JobRuns": self.runs.get(JobName, [])}


class FakeStepFunctions:
    def __init__(self, executions):
        self.executions = executions

    def list_executions(self, **_kwargs):
        return {"executions": self.executions}


class FakeAthena:
    def __init__(self, state, reason=None):
        self.state = state
        self.reason = reason

    def start_query_execution(self, **kwargs):
        self.start = kwargs
        return {"QueryExecutionId": "query-123"}

    def get_query_execution(self, QueryExecutionId):
        self.query_id = QueryExecutionId
        status = {"State": self.state}
        if self.reason:
            status["StateChangeReason"] = self.reason
        return {"QueryExecution": {"Status": status}}


def remote_object(run_id, modified, row_count=10):
    return {
        "VersionId": "version-1",
        "ContentLength": 123,
        "LastModified": modified,
        "ETag": '"etag-1"',
        "Metadata": {
            "sha256": "a" * 64,
            "row-count": str(row_count),
            "schema-sha256": "b" * 64,
            "etl-run-id": run_id,
            "generated-at": modified.isoformat(),
        },
    }


class EtlAutomationTests(unittest.TestCase):
    @staticmethod
    def _platform_children(run_id="run-123"):
        return {
            "main_manifest": {
                "release_id": f"mimir-main-{run_id}",
                "etl_run_id": run_id,
            },
            "main_manifest_key": f"mimir/releases/{run_id}/manifest.json",
            "public_manifest": {
                "release_id": f"public-intelligence-{run_id}",
                "etl_run_id": run_id,
            },
            "public_manifest_key": f"mimir/public/releases/{run_id}/manifest.json",
            "ask_mimir_manifest": {
                "release_id": f"ask-mimir-{run_id}",
                "etl_run_id": run_id,
            },
            "ask_mimir_manifest_key": (
                f"ask_mimir/releases/ask-mimir-{run_id}/runtime_manifest.json"
            ),
        }

    def test_platform_root_binds_all_surfaces_to_one_etl_run(self):
        manifest = build_platform_manifest(
            etl_run_id="run-123",
            **self._platform_children(),
        )
        self.assertEqual(set(manifest["components"]), {"main", "public", "ask_mimir"})
        self.assertTrue(manifest["validation"]["all_components_from_same_run"])
        self.assertTrue(all(
            component["etl_run_id"] == "run-123"
            for component in manifest["components"].values()
        ))

    def test_platform_root_rejects_a_mixed_child_release(self):
        children = self._platform_children()
        children["public_manifest"] = {
            "release_id": "public-intelligence-old",
            "etl_run_id": "older-run",
        }
        with self.assertRaisesRegex(PlatformReleaseValidationError, "older-run"):
            build_platform_manifest(etl_run_id="run-123", **children)

    def test_platform_candidate_never_updates_current_pointer(self):
        manifest = build_platform_manifest(
            etl_run_id="run-123",
            **self._platform_children(),
        )
        s3 = FakeS3({})
        publish_platform_candidate(s3, "bucket", manifest)
        self.assertEqual(len(s3.puts), 2)
        self.assertNotIn(PLATFORM_CURRENT_KEY, {item["Key"] for item in s3.puts})

    def test_platform_promotion_is_one_pointer_write(self):
        manifest = build_platform_manifest(
            etl_run_id="run-123",
            **self._platform_children(),
        )
        s3 = FakeS3({})
        result = promote_platform_manifest(s3, "bucket", manifest)
        self.assertEqual(len(s3.puts), 1)
        self.assertEqual(s3.puts[0]["Key"], PLATFORM_CURRENT_KEY)
        self.assertEqual(result["s3_version_id"], "put-version-1")

    def test_platform_rollback_republishes_exact_immutable_root(self):
        old_manifest = build_platform_manifest(
            etl_run_id="run-old",
            **self._platform_children("run-old"),
        )
        immutable_key = "mimir/platform/releases/mimir-platform-run-old/manifest.json"
        original_body = (json.dumps(old_manifest, sort_keys=True) + "\n").encode()
        s3 = FakeS3({immutable_key: {"Body": original_body}})
        result = rollback_platform_manifest(s3, "bucket", immutable_key)
        self.assertEqual(len(s3.puts), 1)
        self.assertEqual(s3.puts[0]["Key"], PLATFORM_CURRENT_KEY)
        self.assertEqual(s3.puts[0]["Body"], original_body)
        self.assertEqual(result["release_id"], "mimir-platform-run-old")

    def test_athena_retry_classification_is_bounded_to_transient_failures(self):
        self.assertTrue(
            is_retryable_athena_reason(
                "HIVE_S3_THROTTLING: S3 returned Status Code: 503"
            )
        )
        self.assertTrue(is_retryable_athena_reason("GENERIC_INTERNAL_ERROR"))
        self.assertFalse(is_retryable_athena_reason("COLUMN_NOT_FOUND: bad_name"))

    def test_scheduler_uses_candidate_only_mode(self):
        template = (
            Path(__file__).parent / "infrastructure" / "automated_etl_refresh.yaml"
        ).read_text()
        self.assertIn("IsPresent: true", template)
        self.assertIn("StringEquals: candidate", template)
        self.assertIn("Next: RefreshCoreCandidate", template)
        self.assertIn("Input: '{\"mode\":\"candidate\"}'", template)
        self.assertIn("Next: SelectPostValidation", template)

        post_validation = template.split("SelectPostValidation:", 1)[1].split(
            "CompareReleases:", 1
        )[0]
        self.assertIn("StringEquals: candidate", post_validation)
        self.assertIn("Next: PublishAskCandidate", post_validation)
        self.assertIn("Default: CompareReleases", post_validation)

    def test_mutable_product_sources_receive_deep_comparison(self):
        self.assertIn("products.parquet", DEEP_COMPARE_FILES)
        self.assertIn("nsn_cage_reference.parquet", DEEP_COMPARE_FILES)

    def test_merged_schema_must_preserve_both_feature_branches(self):
        assert_preserves_schemas(
            [("id", "varchar"), ("unit_price", "double"), ("status", "varchar(64)")],
            [
                [("id", "varchar"), ("unit_price", "double")],
                [("id", "varchar"), ("status", "varchar(24)")],
            ],
            {"status": "varchar(64)"},
        )

    def test_view_repair_changes_only_outer_status_projection(self):
        sql = "SELECT\n  , unit_price_status\n  , p.unit_price_status\nFROM source"
        repaired = repair_source_sql(sql)
        self.assertIn(
            ", CAST(p.unit_price_status AS VARCHAR(64)) AS unit_price_status",
            repaired,
        )
        self.assertIn(", unit_price_status", repaired)

    def test_view_repair_replaces_exact_qualified_relation(self):
        sql = 'SELECT * FROM "market_intel_gold"."dashboard_master_view" t'
        replaced = replace_relation(
            sql,
            "dashboard_master_view",
            "dashboard_master_view_repaircheck",
        )
        self.assertEqual(replaced.count("dashboard_master_view_repaircheck"), 1)

    def test_view_repair_reports_only_changed_schema_fields(self):
        delta = schema_delta(
            [("id", "varchar"), ("status", "varchar(50)")],
            [("id", "varchar"), ("status", "varchar(64)")],
        )
        self.assertEqual(
            delta,
            [
                {
                    "column": "status",
                    "before": "varchar(50)",
                    "after": "varchar(64)",
                }
            ],
        )

    def test_preflight_analyzes_primary_athena_view(self):
        athena = FakeAthena("SUCCEEDED")
        result = validate_athena_serving_views(
            athena,
            "bucket",
            poll_seconds=0,
        )
        self.assertEqual(result["state"], "SUCCEEDED")
        self.assertIn("dashboard_summary_v2", athena.start["QueryString"])
        self.assertEqual(athena.start["WorkGroup"], "primary")

    def test_preflight_rejects_invalid_athena_view(self):
        with self.assertRaisesRegex(UpstreamValidationError, "INVALID_VIEW"):
            validate_athena_serving_views(
                FakeAthena("FAILED", "INVALID_VIEW: stored view is stale"),
                "bucket",
                poll_seconds=0,
            )

    def test_manual_control_is_isolated_and_reproduces_full_force_mode(self):
        environment = manual_environment("compare-123")
        self.assertEqual(environment["FORCE_REBUILD"], "1")
        self.assertEqual(environment["ONLY_FORCE_REBUILD_FILES"], "0")
        self.assertEqual(environment["ETL_AUTOMATION_RUN_ID"], "compare-123-manual")
        self.assertEqual(
            environment["CACHE_PREFIX"],
            "mimir/comparisons/compare-123/manual/app_cache/",
        )

    def test_manual_control_seeds_exact_live_versions(self):
        modified = datetime(2026, 9, 23, 5, tzinfo=timezone.utc)
        s3 = FakeS3(
            {
                "app_cache/summary.parquet": remote_object("old", modified),
                "app_cache/network.parquet": remote_object("old", modified),
            }
        )
        copied = seed_manual_control(s3, "bucket", "compare-123")
        self.assertEqual(len(copied), 2)
        self.assertEqual(len(s3.copies), 2)
        self.assertEqual(s3.copies[0]["source"]["VersionId"], "version-1")
        self.assertTrue(
            all(
                item["key"].startswith(
                    "mimir/comparisons/compare-123/manual/app_cache/"
                )
                for item in s3.copies
            )
        )

    def test_deep_comparison_detects_aggregate_change(self):
        with tempfile.TemporaryDirectory() as directory:
            control_path = Path(directory) / "control.parquet"
            shadow_path = Path(directory) / "shadow.parquet"
            pd.DataFrame(
                {
                    "cage_code": ["A", "B"],
                    "year": [2025, 2026],
                    "total_spend": [10.0, 20.0],
                }
            ).to_parquet(control_path)
            pd.DataFrame(
                {
                    "cage_code": ["A", "B"],
                    "year": [2025, 2026],
                    "total_spend": [10.0, 25.0],
                }
            ).to_parquet(shadow_path)
            control = _deep_metrics(control_path, "summary.parquet")
            shadow = _deep_metrics(shadow_path, "summary.parquet")
        deltas = _metric_deltas(control["metrics"], shadow["metrics"])
        changed = {item["metric"] for item in deltas}
        self.assertIn("numeric.total_spend.sum", changed)
        self.assertNotIn("row_count", changed)

    def test_preflight_accepts_recent_successful_upstreams(self):
        now = datetime(2026, 9, 23, 5, tzinfo=timezone.utc)
        results = validate_glue_jobs(
            FakeGlue(
                {
                    "prime": [
                        {
                            "JobRunState": "SUCCEEDED",
                            "CompletedOn": now - timedelta(hours=2),
                        }
                    ]
                }
            ),
            now,
            gates=(GlueGate("prime", max_age_hours=4),),
        )
        state_machine = validate_state_machine(
            FakeStepFunctions(
                [
                    {
                        "status": "SUCCEEDED",
                        "stopDate": now - timedelta(hours=1),
                    }
                ]
            ),
            "arn:aws:states:us-east-1:123:stateMachine:sam-daily",
            now,
            max_age_hours=4,
        )
        self.assertEqual(results[0]["state"], "SUCCEEDED")
        self.assertEqual(state_machine["state"], "SUCCEEDED")

    def test_preflight_rejects_latest_failed_job(self):
        now = datetime(2026, 9, 23, 5, tzinfo=timezone.utc)
        with self.assertRaisesRegex(UpstreamValidationError, "FAILED"):
            validate_glue_jobs(
                FakeGlue(
                    {
                        "prime": [
                            {
                                "JobRunState": "FAILED",
                                "CompletedOn": now - timedelta(hours=2),
                            }
                        ]
                    }
                ),
                now,
                gates=(GlueGate("prime"),),
            )

    def test_preflight_rejects_stale_success(self):
        now = datetime(2026, 9, 23, 5, tzinfo=timezone.utc)
        with self.assertRaisesRegex(UpstreamValidationError, "stale"):
            validate_glue_jobs(
                FakeGlue(
                    {
                        "prime": [
                            {
                                "JobRunState": "SUCCEEDED",
                                "CompletedOn": now - timedelta(hours=40),
                            }
                        ]
                    }
                ),
                now,
                gates=(GlueGate("prime", max_age_hours=36),),
            )

    def test_phase_environment_is_targeted_and_stamps_run(self):
        environment = phase_environment(
            "run-123",
            ["summary.parquet", "network.parquet"],
            base={"KEEP_ME": "yes", "FORCE_REBUILD": "1"},
        )
        self.assertEqual(environment["ETL_AUTOMATION_RUN_ID"], "run-123")
        self.assertEqual(environment["ONLY_FORCE_REBUILD_FILES"], "1")
        self.assertEqual(environment["FORCE_REBUILD"], "0")
        self.assertEqual(
            environment["FORCE_REBUILD_FILES"],
            "summary.parquet,network.parquet",
        )
        self.assertEqual(environment["KEEP_ME"], "yes")
        self.assertEqual(environment["CACHE_PREFIX"], staging_cache_prefix("run-123"))

    def test_ask_candidate_publisher_stamps_the_shared_etl_run(self):
        calls = []

        def fake_runner(command, **kwargs):
            calls.append((command, kwargs))

        publish_ask_candidate("bucket", "run-123", runner=fake_runner)

        self.assertEqual(len(calls), 1)
        command, kwargs = calls[0]
        self.assertIn("--serving-source-prefix", command)
        self.assertEqual(
            command[command.index("--serving-source-prefix") + 1],
            staging_cache_prefix("run-123"),
        )
        self.assertEqual(kwargs["env"]["ETL_AUTOMATION_RUN_ID"], "run-123")

    def test_operational_refresh_reuses_pinned_foia_release_and_advances_as_of_date(self):
        calls = []

        def fake_runner(command, **kwargs):
            calls.append((command, kwargs))

        s3 = FakeS3({
            "candidate.json": {
                "Body": json.dumps({
                    "source_release": "2026-09-16-1038",
                    "retrieval_date": "2026-09-24",
                })
            }
        })
        result = refresh_operational_candidate(
            s3=s3,
            bucket="bucket",
            region="us-east-1",
            run_id="run-123",
            run_started_at=datetime(2026, 9, 25, 1, tzinfo=timezone.utc),
            manifest_key="candidate.json",
            runner=fake_runner,
        )
        command, kwargs = calls[0]
        self.assertEqual(command[command.index("--source-release") + 1], "2026-09-16-1038")
        self.assertEqual(command[command.index("--retrieval-date") + 1], "2026-09-24")
        self.assertEqual(command[command.index("--as-of-date") + 1], "2026-09-25")
        self.assertIn("run-123/app_cache", result["serving_candidate_prefix"])
        self.assertTrue(kwargs["check"])

    def test_operational_sidecars_are_pinned_into_the_same_candidate_run(self):
        bucket = "bucket"
        manifest_key = "mimir/nsn-enrichment-candidates/candidate_manifest.json"
        filenames = (
            "nsn_supply_state_lookup.parquet",
            "nsn_price_summary_lookup.parquet",
            "nsn_opportunity_summary_lookup.parquet",
            "nsn_opportunity_detail.parquet",
        )
        artifacts = {
            filename: {
                "sha256": character * 64,
                "row_count": index * 10,
                "schema_sha256": character.upper() * 64,
            }
            for index, (filename, character) in enumerate(
                zip(filenames, ("a", "b", "c", "d")), start=1
            )
        }
        uploads = [
            {
                "s3_uri": f"s3://{bucket}/release/app_cache/{filename}",
                "version_id": f"source-version-{index}",
            }
            for index, filename in enumerate(filenames, start=1)
        ]
        manifest = {
            "source_release": "2026-09-16-1038",
            "artifacts": artifacts,
            "uploaded_objects": uploads,
        }
        objects = {manifest_key: {"Body": json.dumps(manifest)}}
        for filename, upload in zip(filenames, uploads):
            source_key = upload["s3_uri"].split(f"s3://{bucket}/", 1)[1]
            objects[source_key] = {
                "Metadata": {"sha256": artifacts[filename]["sha256"]}
            }
        s3 = FakeS3(objects)

        result = stage_operational_sidecars(
            s3,
            bucket,
            "run-123",
            manifest_key=manifest_key,
        )

        self.assertEqual(result["source_release"], "2026-09-16-1038")
        self.assertEqual(len(s3.copies), 4)
        self.assertTrue(all(
            item["Key"].startswith(staging_cache_prefix("run-123"))
            for item in s3.copies
        ))
        self.assertTrue(all(
            item["Metadata"]["etl-run-id"] == "run-123"
            for item in s3.copies
        ))

    def test_parquet_metadata_captures_rows_schema_and_run(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "data.parquet"
            pd.DataFrame({"cage": ["A", "B"], "spend": [1.0, 2.0]}).to_parquet(path)
            original = os.environ.get("ETL_AUTOMATION_RUN_ID")
            os.environ["ETL_AUTOMATION_RUN_ID"] = "run-abc"
            try:
                metadata = parquet_object_metadata(path)
            finally:
                if original is None:
                    os.environ.pop("ETL_AUTOMATION_RUN_ID", None)
                else:
                    os.environ["ETL_AUTOMATION_RUN_ID"] = original
        self.assertEqual(metadata["row-count"], "2")
        self.assertEqual(metadata["etl-run-id"], "run-abc")
        self.assertEqual(len(metadata["schema-sha256"]), 64)

    def test_candidate_pins_versions_and_never_writes_current_pointer(self):
        run_id = "run-123"
        started = datetime(2026, 9, 23, 1, tzinfo=timezone.utc)
        staging_prefix = staging_cache_prefix(run_id)
        objects = {
            f"{staging_prefix}summary.parquet": remote_object(
                run_id, started + timedelta(minutes=1)
            ),
            f"{staging_prefix}network.parquet": remote_object(
                run_id, started + timedelta(minutes=2)
            ),
            f"{staging_prefix}profiles.parquet": remote_object(
                run_id, started + timedelta(minutes=3)
            ),
            "app_cache/dod_contract_announcements.parquet": {
                **remote_object("other-pipeline", started - timedelta(days=1)),
                "Metadata": {},
            },
        }
        s3 = FakeS3(objects)
        artifacts = (
            Artifact("summary.parquet"),
            Artifact("network.parquet"),
            Artifact("profiles.parquet"),
            Artifact(
                "dod_contract_announcements.parquet",
                generated_by_refresh=False,
            ),
        )
        manifest = build_candidate_manifest(
            s3,
            "bucket",
            run_id,
            started,
            artifacts=artifacts,
            cache_prefix=staging_prefix,
        )
        locations = publish_candidate_manifest(s3, "bucket", manifest)

        self.assertEqual(manifest["status"], "candidate")
        self.assertTrue(manifest["validation"]["all_generated_objects_from_same_run"])
        self.assertEqual(manifest["cache_prefix"], staging_prefix)
        generated_keys = {
            entry["s3_key"]
            for entry in manifest["files"]
            if entry["generated_by_refresh"]
        }
        self.assertTrue(all(key.startswith(staging_prefix) for key in generated_keys))
        self.assertEqual(len(s3.puts), 2)
        written_keys = {item["Key"] for item in s3.puts}
        self.assertNotIn(MAIN_CURRENT_KEY, written_keys)
        self.assertIn(locations["candidate_manifest_key"], written_keys)
        candidate_body = next(
            item["Body"]
            for item in s3.puts
            if item["Key"] == locations["candidate_manifest_key"]
        )
        self.assertEqual(json.loads(candidate_body)["etl_run_id"], run_id)

    def test_candidate_rejects_mixed_run_outputs(self):
        started = datetime(2026, 9, 23, 1, tzinfo=timezone.utc)
        s3 = FakeS3(
            {
                "app_cache/summary.parquet": remote_object(
                    "older-run", started + timedelta(minutes=1)
                )
            }
        )
        with self.assertRaisesRegex(CandidateValidationError, "older-run"):
            build_candidate_manifest(
                s3,
                "bucket",
                "current-run",
                started,
                artifacts=(Artifact("summary.parquet"),),
            )

    def test_profiles_must_follow_summary_and_network(self):
        run_id = "run-123"
        started = datetime(2026, 9, 23, 1, tzinfo=timezone.utc)
        s3 = FakeS3(
            {
                "app_cache/summary.parquet": remote_object(
                    run_id, started + timedelta(minutes=2)
                ),
                "app_cache/network.parquet": remote_object(
                    run_id, started + timedelta(minutes=3)
                ),
                "app_cache/profiles.parquet": remote_object(
                    run_id, started + timedelta(minutes=1)
                ),
            }
        )
        with self.assertRaisesRegex(CandidateValidationError, "generated before"):
            build_candidate_manifest(
                s3,
                "bucket",
                run_id,
                started,
                artifacts=(
                    Artifact("summary.parquet"),
                    Artifact("network.parquet"),
                    Artifact("profiles.parquet"),
                ),
            )


if __name__ == "__main__":
    unittest.main()
