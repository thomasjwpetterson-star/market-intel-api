"""Materialize a durable, candidate-only platform root from a tested rehearsal."""

from __future__ import annotations

import argparse
import copy
import json
from typing import Any

import boto3
from botocore.exceptions import ClientError

from etl_automation.platform_release import (
    build_platform_manifest,
    publish_platform_candidate,
    validate_platform_manifest,
)


DEFAULT_BUCKET = "a-and-d-intel-lake-newaccount"


def load_json(
    s3: Any,
    bucket: str,
    key: str,
    version_id: str | None = None,
) -> dict[str, Any]:
    request = {"Bucket": bucket, "Key": key}
    if version_id:
        request["VersionId"] = version_id
    return json.loads(s3.get_object(**request)["Body"].read())


def json_body(value: dict[str, Any]) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def copy_public_release(
    s3: Any,
    bucket: str,
    source_manifest: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    """Copy exact public artifact versions into a durable immutable release."""
    release_id = str(source_manifest.get("release_id") or "").strip()
    run_id = str(source_manifest.get("etl_run_id") or "").strip()
    artifacts = source_manifest.get("artifacts") or []
    if not release_id or not run_id or len(artifacts) != 10:
        raise RuntimeError("Public source manifest is incomplete")

    durable = copy.deepcopy(source_manifest)
    durable_artifacts = []
    release_prefix = f"mimir/public/releases/{release_id}"
    for artifact in artifacts:
        filename = str(artifact.get("filename") or "").strip()
        source_key = str(artifact.get("s3_key") or "").strip()
        source_version = str(artifact.get("s3_version_id") or "").strip()
        expected_size = int(artifact.get("size") or 0)
        expected_sha = str(artifact.get("sha256") or "").strip()
        if not filename or not source_key or not source_version:
            raise RuntimeError("Public artifact is not version pinned")
        if not expected_size or not expected_sha:
            raise RuntimeError(f"Public artifact lacks integrity metadata: {filename}")
        destination_key = f"{release_prefix}/{filename}"
        try:
            existing = s3.head_object(Bucket=bucket, Key=destination_key)
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code") or "")
            if code not in {"404", "NoSuchKey", "NotFound"}:
                raise
            existing = None
        if existing is not None:
            metadata = {
                str(key).lower(): str(value)
                for key, value in (existing.get("Metadata") or {}).items()
            }
            if (
                int(existing.get("ContentLength") or 0) != expected_size
                or metadata.get("sha256") != expected_sha
            ):
                raise RuntimeError(
                    f"Durable public artifact already exists with different content: "
                    f"{destination_key}"
                )
            destination_version = str(existing.get("VersionId") or "")
        else:
            copied = s3.copy_object(
                Bucket=bucket,
                Key=destination_key,
                CopySource={
                    "Bucket": bucket,
                    "Key": source_key,
                    "VersionId": source_version,
                },
                MetadataDirective="COPY",
                ServerSideEncryption="AES256",
            )
            destination_version = str(copied.get("VersionId") or "")
        if not destination_version:
            raise RuntimeError(f"S3 did not version durable artifact {destination_key}")
        durable_artifacts.append(
            {
                **artifact,
                "s3_key": destination_key,
                "s3_version_id": destination_version,
            }
        )

    durable["artifacts"] = durable_artifacts
    immutable_key = f"{release_prefix}/manifest.json"
    manifest_body = json_body(durable)
    try:
        existing_manifest = s3.get_object(Bucket=bucket, Key=immutable_key)[
            "Body"
        ].read()
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code") or "")
        if code not in {"404", "NoSuchKey", "NotFound"}:
            raise
        existing_manifest = None
    if existing_manifest is not None and existing_manifest != manifest_body:
        raise RuntimeError(
            f"Durable public manifest already exists with different content: "
            f"{immutable_key}"
        )
    if existing_manifest is None:
        s3.put_object(
            Bucket=bucket,
            Key=immutable_key,
            Body=manifest_body,
            ContentType="application/json",
            ServerSideEncryption="AES256",
        )
    s3.put_object(
        Bucket=bucket,
        Key="mimir/public/candidate_manifest.json",
        Body=manifest_body,
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )
    return durable, immutable_key


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--source-platform-key", required=True)
    parser.add_argument("--source-platform-version-id", required=True)
    parser.add_argument("--durable-ask-manifest-key", required=True)
    parser.add_argument("--expected-run-id", required=True)
    parser.add_argument("--platform-release-id", required=True)
    parser.add_argument("--change-control-prefix", required=True)
    arguments = parser.parse_args()

    if not arguments.source_platform_key.startswith("mimir/rehearsals/"):
        raise RuntimeError("Source platform root must be the tested rehearsal root")
    change_prefix = arguments.change_control_prefix.strip().strip("/")
    if not change_prefix.startswith("mimir/change-control/"):
        raise RuntimeError("Change-control writes must stay under mimir/change-control/")

    s3 = boto3.client("s3", region_name=arguments.region)
    source_root = load_json(
        s3,
        arguments.bucket,
        arguments.source_platform_key,
        arguments.source_platform_version_id,
    )
    validate_platform_manifest(source_root)
    run_id = str(source_root["etl_run_id"])
    if run_id != arguments.expected_run_id:
        raise RuntimeError(
            f"Tested root belongs to {run_id}, expected {arguments.expected_run_id}"
        )

    main_key = source_root["components"]["main"]["immutable_manifest_key"]
    public_source_key = source_root["components"]["public"][
        "immutable_manifest_key"
    ]
    main_manifest = load_json(s3, arguments.bucket, main_key)
    public_source = load_json(s3, arguments.bucket, public_source_key)
    ask_manifest = load_json(
        s3,
        arguments.bucket,
        arguments.durable_ask_manifest_key,
    )
    public_manifest, public_key = copy_public_release(
        s3,
        arguments.bucket,
        public_source,
    )

    platform = build_platform_manifest(
        etl_run_id=run_id,
        main_manifest=main_manifest,
        main_manifest_key=main_key,
        public_manifest=public_manifest,
        public_manifest_key=public_key,
        ask_mimir_manifest=ask_manifest,
        ask_mimir_manifest_key=arguments.durable_ask_manifest_key,
        platform_release_id=arguments.platform_release_id,
    )
    platform["status"] = "production-candidate"
    immutable_platform_key = (
        f"mimir/platform/releases/{platform['release_id']}/manifest.json"
    )
    try:
        s3.head_object(Bucket=arguments.bucket, Key=immutable_platform_key)
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code") or "")
        if code not in {"404", "NoSuchKey", "NotFound"}:
            raise
    else:
        raise RuntimeError(
            "Refusing to overwrite existing immutable platform manifest: "
            f"{immutable_platform_key}"
        )
    platform_locations = publish_platform_candidate(
        s3,
        arguments.bucket,
        platform,
    )
    snapshot_key = f"{change_prefix}/production-candidate.json"
    snapshot_body = json_body(
        {
            "source_rehearsal": {
                "key": arguments.source_platform_key,
                "version_id": arguments.source_platform_version_id,
            },
            "platform": platform,
            "platform_locations": platform_locations,
            "durable_public_manifest_key": public_key,
            "durable_ask_manifest_key": arguments.durable_ask_manifest_key,
        }
    )
    snapshot_response = s3.put_object(
        Bucket=arguments.bucket,
        Key=snapshot_key,
        Body=snapshot_body,
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )
    print(
        json.dumps(
            {
                "etl_run_id": run_id,
                "platform_release_id": platform["release_id"],
                "platform_candidate_key": platform_locations[
                    "candidate_manifest_key"
                ],
                "platform_immutable_key": platform_locations[
                    "immutable_manifest_key"
                ],
                "public_immutable_key": public_key,
                "ask_immutable_key": arguments.durable_ask_manifest_key,
                "change_control_key": snapshot_key,
                "change_control_version_id": str(
                    snapshot_response.get("VersionId") or ""
                ),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
