"""Promote one fully verified atomic platform candidate with a durable audit snapshot.

This is deliberately stricter than the generic promotion helper.  The first
production cutover must still point at the exact Main candidate that passed the
shadow suite, must contain all three immutable children, and must retain the
legacy pointer versions needed for a feature-flag rollback.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

from etl_automation.platform_release import (
    PLATFORM_CURRENT_KEY,
    promote_platform_manifest,
    validate_platform_manifest,
)


DEFAULT_BUCKET = "a-and-d-intel-lake-newaccount"
PLATFORM_CANDIDATE_KEY = "mimir/platform/candidate_manifest.json"
MAIN_CANDIDATE_KEY = "mimir/runtime/candidate_manifest.json"
LEGACY_POINTER_KEYS = (
    "mimir/runtime/current_manifest.json",
    "app_cache/public_intelligence/current.json",
    "ask_mimir/runtime/current_manifest.json",
)


def load_json(
    s3: Any,
    bucket: str,
    key: str,
    version_id: str | None = None,
) -> dict[str, Any]:
    request = {"Bucket": bucket, "Key": key}
    if version_id:
        request["VersionId"] = version_id
    response = s3.get_object(**request)
    value = json.loads(response["Body"].read())
    if not isinstance(value, dict):
        raise RuntimeError(f"{key} is not a JSON object")
    return value


def object_state(s3: Any, bucket: str, key: str) -> dict[str, Any]:
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code") or "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return {"key": key, "exists": False}
        raise
    return {
        "key": key,
        "exists": True,
        "version_id": str(head.get("VersionId") or ""),
        "etag": str(head.get("ETag") or "").strip('"'),
        "last_modified": head["LastModified"].isoformat(),
        "content_length": int(head.get("ContentLength") or 0),
    }


def require_current_version(
    s3: Any,
    bucket: str,
    key: str,
    expected_version_id: str,
) -> dict[str, Any]:
    state = object_state(s3, bucket, key)
    if not state.get("exists"):
        raise RuntimeError(f"Required object is absent: {key}")
    if state.get("version_id") != expected_version_id:
        raise RuntimeError(
            f"{key} advanced from {expected_version_id} to "
            f"{state.get('version_id') or '<unversioned>'}"
        )
    return state


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--candidate-version-id", required=True)
    parser.add_argument("--main-candidate-version-id", required=True)
    parser.add_argument("--observed-nsn-candidate-version-id", required=True)
    parser.add_argument("--expected-run-id", required=True)
    parser.add_argument("--change-control-prefix", required=True)
    parser.add_argument("--git-commit", required=True)
    parser.add_argument("--execute", action="store_true")
    arguments = parser.parse_args()

    change_prefix = arguments.change_control_prefix.strip().strip("/")
    if not change_prefix.startswith("mimir/change-control/"):
        raise RuntimeError("Change-control prefix is outside mimir/change-control")

    s3 = boto3.client("s3", region_name=arguments.region)
    platform_state = require_current_version(
        s3,
        arguments.bucket,
        PLATFORM_CANDIDATE_KEY,
        arguments.candidate_version_id,
    )
    main_candidate_state = require_current_version(
        s3,
        arguments.bucket,
        MAIN_CANDIDATE_KEY,
        arguments.main_candidate_version_id,
    )
    nsn_candidate_state = require_current_version(
        s3,
        arguments.bucket,
        "mimir/nsn-enrichment-candidates/candidate_manifest.json",
        arguments.observed_nsn_candidate_version_id,
    )

    platform = load_json(
        s3,
        arguments.bucket,
        PLATFORM_CANDIDATE_KEY,
        arguments.candidate_version_id,
    )
    validate_platform_manifest(platform)
    run_id = str(platform.get("etl_run_id") or "")
    if run_id != arguments.expected_run_id:
        raise RuntimeError(f"Platform run {run_id} is not {arguments.expected_run_id}")
    if platform.get("status") != "production-candidate":
        raise RuntimeError("Platform root is not marked production-candidate")

    children: dict[str, dict[str, Any]] = {}
    for name, reference in platform["components"].items():
        child = load_json(
            s3,
            arguments.bucket,
            reference["immutable_manifest_key"],
        )
        if child.get("release_id") != reference.get("release_id"):
            raise RuntimeError(f"{name} immutable child release ID changed")
        if child.get("etl_run_id") != run_id:
            raise RuntimeError(f"{name} immutable child is from a mixed ETL run")
        children[name] = child

    main_candidate = load_json(
        s3,
        arguments.bucket,
        MAIN_CANDIDATE_KEY,
        arguments.main_candidate_version_id,
    )
    if main_candidate.get("release_id") != children["main"].get("release_id"):
        raise RuntimeError("Frozen Main candidate does not match the platform child")
    if len(children["public"].get("artifacts") or []) != 10:
        raise RuntimeError("Public child does not contain all 10 atomic artifacts")
    if not children["ask_mimir"].get("files"):
        raise RuntimeError("Ask Mimir child has no files")

    pre_cutover = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "git_commit": arguments.git_commit,
        "execute_requested": bool(arguments.execute),
        "platform_candidate": platform_state,
        "main_candidate": main_candidate_state,
        "concurrent_nsn_candidate": nsn_candidate_state,
        "legacy_pointers": {
            key: object_state(s3, arguments.bucket, key)
            for key in (PLATFORM_CURRENT_KEY, *LEGACY_POINTER_KEYS)
        },
        "rollback": {
            "consumer_action": "Set MIMIR_USE_PLATFORM_MANIFEST=0 and redeploy both consumers",
            "code_action": "Roll back Render services to git commit cac59e9d85e5b83f06b32d2c6fa49fd02a71a45e",
            "legacy_pointers_preserved": True,
            "platform_pointer_can_remain_inert": True,
        },
        "platform": platform,
    }
    snapshot_key = f"{change_prefix}/pre-cutover-snapshot.json"
    snapshot = s3.put_object(
        Bucket=arguments.bucket,
        Key=snapshot_key,
        Body=(json.dumps(pre_cutover, indent=2, sort_keys=True) + "\n").encode(),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )

    result: dict[str, Any] = {
        "validated": True,
        "executed": False,
        "platform_release_id": platform["release_id"],
        "etl_run_id": run_id,
        "snapshot_key": snapshot_key,
        "snapshot_version_id": str(snapshot.get("VersionId") or ""),
        "public_artifact_count": len(children["public"]["artifacts"]),
        "ask_file_count": len(children["ask_mimir"]["files"]),
    }
    if arguments.execute:
        result.update(
            promote_platform_manifest(s3, arguments.bucket, platform)
        )
        result["executed"] = True
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
