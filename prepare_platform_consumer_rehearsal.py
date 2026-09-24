"""Create a rehearsal-only atomic root suitable for consumer cold-start tests."""

from __future__ import annotations

import argparse
import json

import boto3

from etl_automation.platform_release import (
    build_platform_manifest,
    promote_platform_manifest,
    publish_platform_candidate,
)


def load_json(s3, bucket: str, key: str) -> dict:
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--rehearsal-prefix", required=True)
    parser.add_argument("--main-manifest-key", required=True)
    parser.add_argument("--public-manifest-key", required=True)
    parser.add_argument("--ask-manifest-key", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    prefix = args.rehearsal_prefix.strip().strip("/")
    if not prefix.startswith("mimir/rehearsals/"):
        raise RuntimeError("Consumer rehearsal writes must stay under mimir/rehearsals/")
    s3 = boto3.client("s3", region_name="us-east-1")
    main_manifest = load_json(s3, args.bucket, args.main_manifest_key)
    public_manifest = load_json(s3, args.bucket, args.public_manifest_key)
    ask_manifest = load_json(s3, args.bucket, args.ask_manifest_key)
    run_id = str(main_manifest["etl_run_id"])
    if str(public_manifest.get("etl_run_id") or "") != run_id:
        raise RuntimeError("Public rehearsal does not match the main ETL run")

    # Older Ask releases predate etl_run_id in their JSON body.  Preserve their
    # exact file entries but create a rehearsal-only immutable child that makes
    # the same-run binding explicit.  Future pipeline publications stamp this
    # field directly.
    ask_manifest = dict(ask_manifest)
    existing_ask_run = str(ask_manifest.get("etl_run_id") or "")
    if existing_ask_run and existing_ask_run != run_id:
        raise RuntimeError("Ask Mimir rehearsal belongs to a different ETL run")
    ask_manifest["etl_run_id"] = run_id
    ask_manifest_key = (
        f"{prefix}/ask_mimir/releases/{ask_manifest['release_id']}/runtime_manifest.json"
    )
    s3.put_object(
        Bucket=args.bucket,
        Key=ask_manifest_key,
        Body=(json.dumps(ask_manifest, indent=2, sort_keys=True) + "\n").encode(),
        ContentType="application/json",
    )

    platform = build_platform_manifest(
        etl_run_id=run_id,
        main_manifest=main_manifest,
        main_manifest_key=args.main_manifest_key,
        public_manifest=public_manifest,
        public_manifest_key=args.public_manifest_key,
        ask_mimir_manifest=ask_manifest,
        ask_mimir_manifest_key=ask_manifest_key,
    )
    platform["status"] = "nonproduction-consumer-rehearsal"
    platform_prefix = f"{prefix}/consumer-platform"
    locations = publish_platform_candidate(
        s3,
        args.bucket,
        platform,
        candidate_key=f"{platform_prefix}/candidate_manifest.json",
        immutable_prefix=f"{platform_prefix}/releases",
    )
    promoted = promote_platform_manifest(
        s3,
        args.bucket,
        platform,
        current_key=f"{platform_prefix}/current_manifest.json",
    )
    print(
        json.dumps(
            {
                "rehearsal_prefix": prefix,
                "etl_run_id": run_id,
                "platform_release_id": platform["release_id"],
                "platform_current_key": promoted["current_manifest_key"],
                "platform_current_version_id": promoted["s3_version_id"],
                "platform_immutable_key": locations["immutable_manifest_key"],
                "ask_rehearsal_manifest_key": ask_manifest_key,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
