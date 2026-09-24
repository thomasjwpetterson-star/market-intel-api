"""Run dependency-ordered Mimir ETL stages locally or in ECS/Fargate."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping, Sequence

import boto3

from .baseline import run_manual_control, seed_manual_control
from .comparison import compare_releases
from .paths import staging_cache_prefix
from .preflight import validate_daily_upstreams
from .release import (
    CORE_FILENAMES,
    DEPENDENT_FILENAMES,
    OPERATIONAL_SIDECAR_FILENAMES,
    validate_and_publish_candidate,
)
from source_automation.runner import acquire_usaspending_archive


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BUCKET = "a-and-d-intel-lake-newaccount"
DEFAULT_SAM_STATE_MACHINE_ARN = (
    "arn:aws:states:us-east-1:868631722720:stateMachine:sam-daily-pipeline"
)
DEFAULT_OPERATIONAL_CANDIDATE_MANIFEST_KEY = (
    "mimir/nsn-enrichment-candidates/candidate_manifest.json"
)
STAGES = (
    "preflight",
    "seed-manual",
    "manual-control",
    "core",
    "dependents",
    "validate-main",
    "compare",
    "publish-ask",
    "source-usaspending-archive",
    "all",
    "comparison-all",
)


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("run timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def phase_environment(
    run_id: str,
    filenames: Sequence[str],
    base: Mapping[str, str] | None = None,
) -> dict[str, str]:
    environment = dict(os.environ if base is None else base)
    environment.update(
        {
            "ETL_AUTOMATION_RUN_ID": run_id,
            "ONLY_FORCE_REBUILD_FILES": "1",
            "FORCE_REBUILD": "0",
            "FORCE_REBUILD_FILES": ",".join(filenames),
            "CACHE_PREFIX": staging_cache_prefix(run_id),
            "PYTHONUNBUFFERED": "1",
        }
    )
    return environment


def run_etl_phase(
    run_id: str,
    filenames: Sequence[str],
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> None:
    runner(
        [sys.executable, str(ROOT / "run_etl.py")],
        cwd=ROOT,
        env=phase_environment(run_id, filenames),
        check=True,
    )


def refresh_operational_candidate(
    s3: object,
    bucket: str,
    region: str,
    run_id: str,
    run_started_at: datetime,
    manifest_key: str = DEFAULT_OPERATIONAL_CANDIDATE_MANIFEST_KEY,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> dict[str, object]:
    """Refresh daily solicitation fields while retaining the pinned FOIA release."""
    response = s3.get_object(Bucket=bucket, Key=manifest_key)
    previous = json.loads(response["Body"].read())
    source_release = str(previous.get("source_release") or "").strip()
    retrieval_date = str(previous.get("retrieval_date") or "").strip()
    if not source_release or not retrieval_date:
        raise RuntimeError("Operational candidate pointer has no pinned source release")

    output_prefix = (
        f"s3://{bucket}/gold/dla/operational_metrics_candidate/etl_run_id={run_id}"
    )
    serving_prefix = (
        f"s3://{bucket}/mimir/nsn-enrichment-candidates/{run_id}/app_cache"
    )
    runner(
        [
            sys.executable,
            str(ROOT / "dla_operational_v2" / "materialize_operational_metrics.py"),
            "--source-release", source_release,
            "--retrieval-date", retrieval_date,
            "--as-of-date", run_started_at.date().isoformat(),
            "--output-prefix", output_prefix,
            "--serving-candidate-prefix", serving_prefix,
            "--candidate-pointer-key", manifest_key,
        ],
        cwd=ROOT,
        env={**os.environ, "AWS_REGION": region, "PYTHONUNBUFFERED": "1"},
        check=True,
    )
    return {
        "source_release": source_release,
        "retrieval_date": retrieval_date,
        "solicitation_as_of_date": run_started_at.date().isoformat(),
        "candidate_manifest_key": manifest_key,
        "serving_candidate_prefix": serving_prefix,
    }


def stage_operational_sidecars(
    s3: object,
    bucket: str,
    run_id: str,
    manifest_key: str = DEFAULT_OPERATIONAL_CANDIDATE_MANIFEST_KEY,
) -> dict[str, object]:
    """Pin validated monthly NIIN sidecars into one main-platform ETL run."""
    response = s3.get_object(Bucket=bucket, Key=manifest_key)
    manifest_body = response["Body"].read()
    manifest = json.loads(manifest_body)
    source_release = str(manifest.get("source_release") or "").strip()
    artifacts = manifest.get("artifacts") or {}
    uploaded = manifest.get("uploaded_objects") or []
    destination_prefix = staging_cache_prefix(run_id).strip().strip("/") + "/"
    staged = []

    for filename in OPERATIONAL_SIDECAR_FILENAMES:
        artifact = artifacts.get(filename) or {}
        expected_sha256 = str(artifact.get("sha256") or "").strip()
        expected_rows = int(artifact.get("row_count") or -1)
        expected_schema = str(artifact.get("schema_sha256") or "").strip()
        if not expected_sha256 or expected_rows <= 0 or not expected_schema:
            raise RuntimeError(
                f"Operational candidate manifest is incomplete for {filename}"
            )
        suffix = f"/app_cache/{filename}"
        candidates = [
            entry for entry in uploaded
            if str(entry.get("s3_uri") or "").endswith(suffix)
        ]
        if len(candidates) != 1:
            raise RuntimeError(
                f"Operational candidate manifest must pin exactly one {filename}"
            )
        source = candidates[0]
        source_uri = str(source["s3_uri"])
        source_key = source_uri.split(f"s3://{bucket}/", 1)[-1]
        version_id = str(source.get("version_id") or "").strip()
        if not version_id:
            raise RuntimeError(f"Operational candidate has no S3 version for {filename}")
        head = s3.head_object(Bucket=bucket, Key=source_key, VersionId=version_id)
        remote_metadata = {
            str(key).lower(): str(value)
            for key, value in (head.get("Metadata") or {}).items()
        }
        if remote_metadata.get("sha256") != expected_sha256:
            raise RuntimeError(f"Operational candidate hash mismatch for {filename}")

        destination_key = f"{destination_prefix}{filename}"
        copy_result = s3.copy_object(
            Bucket=bucket,
            Key=destination_key,
            CopySource={"Bucket": bucket, "Key": source_key, "VersionId": version_id},
            MetadataDirective="REPLACE",
            ServerSideEncryption="AES256",
            Metadata={
                "sha256": expected_sha256,
                "row-count": str(expected_rows),
                "schema-sha256": expected_schema,
                "etl-run-id": run_id,
                "generated-at": datetime.now(timezone.utc).isoformat(),
                "source-release": source_release,
                "source-version-id": version_id,
                "source-manifest-sha256": hashlib.sha256(manifest_body).hexdigest(),
            },
        )
        staged.append(
            {
                "filename": filename,
                "destination_key": destination_key,
                "destination_version_id": copy_result.get("VersionId"),
                "source_key": source_key,
                "source_version_id": version_id,
                "sha256": expected_sha256,
            }
        )
    return {
        "source_release": source_release,
        "source_manifest_key": manifest_key,
        "artifacts": staged,
    }


def publish_ask_candidate(
    bucket: str,
    run_id: str,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> None:
    runner(
        [
            sys.executable,
            str(ROOT / "ask_mimir_beta" / "publish_runtime_release.py"),
            "--bucket",
            bucket,
            "--only",
            "serving-data",
            "--skip-local-input-verification",
            "--serving-source-prefix",
            staging_cache_prefix(run_id),
        ],
        cwd=ROOT / "ask_mimir_beta",
        env={
            **os.environ,
            "ETL_AUTOMATION_RUN_ID": run_id,
            "PYTHONUNBUFFERED": "1",
        },
        check=True,
    )


def execute_stage(
    stage: str,
    bucket: str,
    region: str,
    run_id: str,
    run_started_at: datetime,
    sam_state_machine_arn: str,
) -> dict[str, object]:
    if stage not in STAGES:
        raise ValueError(f"Unknown stage: {stage}")

    result: dict[str, object] = {
        "stage": stage,
        "run_id": run_id,
        "run_started_at": run_started_at.isoformat(),
    }
    if stage in {"preflight", "all", "comparison-all"}:
        session = boto3.Session(region_name=region)
        result["upstreams"] = validate_daily_upstreams(
            glue=session.client("glue"),
            stepfunctions=session.client("stepfunctions"),
            athena=session.client("athena"),
            bucket=bucket,
            sam_state_machine_arn=sam_state_machine_arn,
        )
    if stage == "source-usaspending-archive":
        result["source_candidate"] = acquire_usaspending_archive(
            bucket=bucket,
            region=region,
            run_id=run_id,
        )
    if stage in {"seed-manual", "comparison-all"}:
        s3 = boto3.Session(region_name=region).client("s3")
        result["manual_seed"] = seed_manual_control(s3, bucket, run_id)
    if stage in {"manual-control", "comparison-all"}:
        run_manual_control(run_id)
        result["manual_control"] = True
    if stage in {"core", "all", "comparison-all"}:
        run_etl_phase(run_id, CORE_FILENAMES)
        result["core_artifacts"] = list(CORE_FILENAMES)
    if stage in {"dependents", "all", "comparison-all"}:
        s3 = boto3.Session(region_name=region).client("s3")
        operational_manifest_key = os.getenv(
            "DLA_OPERATIONAL_CANDIDATE_MANIFEST_KEY",
            DEFAULT_OPERATIONAL_CANDIDATE_MANIFEST_KEY,
        )
        result["operational_candidate_refresh"] = refresh_operational_candidate(
            s3=s3,
            bucket=bucket,
            region=region,
            run_id=run_id,
            run_started_at=run_started_at,
            manifest_key=operational_manifest_key,
        )
        run_etl_phase(run_id, DEPENDENT_FILENAMES)
        result["dependent_artifacts"] = list(DEPENDENT_FILENAMES)
        result["operational_sidecars"] = stage_operational_sidecars(
            s3=s3,
            bucket=bucket,
            run_id=run_id,
            manifest_key=operational_manifest_key,
        )
    if stage in {"validate-main", "all", "comparison-all"}:
        s3 = boto3.Session(region_name=region).client("s3")
        result["main_candidate"] = validate_and_publish_candidate(
            s3=s3,
            bucket=bucket,
            run_id=run_id,
            run_started_at=run_started_at,
            cache_prefix=staging_cache_prefix(run_id),
        )
    if stage in {"compare", "comparison-all"}:
        s3 = boto3.Session(region_name=region).client("s3")
        result["comparison"] = compare_releases(s3, bucket, run_id)
    if stage in {"publish-ask", "all", "comparison-all"}:
        publish_ask_candidate(bucket, run_id)
        result["ask_mimir_candidate"] = True
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dependency-ordered, candidate-only Mimir ETL refresh"
    )
    parser.add_argument("--stage", choices=STAGES, default="all")
    parser.add_argument(
        "--bucket",
        default=os.getenv("ATHENA_OUTPUT_BUCKET", DEFAULT_BUCKET)
        .replace("s3://", "")
        .split("/")[0],
    )
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"))
    parser.add_argument(
        "--sam-state-machine-arn",
        default=os.getenv(
            "SAM_DAILY_STATE_MACHINE_ARN",
            DEFAULT_SAM_STATE_MACHINE_ARN,
        ),
    )
    parser.add_argument(
        "--run-id",
        default=f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:10]}",
    )
    parser.add_argument(
        "--run-started-at",
        default=datetime.now(timezone.utc).isoformat(),
    )
    arguments = parser.parse_args()
    result = execute_stage(
        stage=arguments.stage,
        bucket=arguments.bucket,
        region=arguments.region,
        run_id=arguments.run_id,
        run_started_at=_parse_timestamp(arguments.run_started_at),
        sam_state_machine_arn=arguments.sam_state_machine_arn,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
