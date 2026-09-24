"""Register an isolated exact-comparison and rollback verifier task."""

from __future__ import annotations

import argparse
import json

import boto3

from register_candidate_task import ALLOWED_CONTAINER_FIELDS, immutable_image


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True, type=immutable_image)
    parser.add_argument("--rehearsal-prefix", required=True)
    parser.add_argument("--public-release-id", required=True)
    parser.add_argument("--main-manifest-key", required=True)
    parser.add_argument("--ask-manifest-key", required=True)
    parser.add_argument(
        "--baseline-prefix",
        required=True,
        help="Immutable directory for the exact last accepted public release.",
    )
    parser.add_argument("--bucket", default="a-and-d-intel-lake-newaccount")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--source-task", default="mimir-etl-refresh:6")
    parser.add_argument("--family", default="mimir-etl-nsn-public-verifier")
    arguments = parser.parse_args()

    ecs = boto3.client("ecs", region_name=arguments.region)
    source = ecs.describe_task_definition(taskDefinition=arguments.source_task)[
        "taskDefinition"
    ]
    original = source["containerDefinitions"][0]
    container = {
        key: value
        for key, value in original.items()
        if key in ALLOWED_CONTAINER_FIELDS
    }
    public_manifest_key = (
        f"{arguments.rehearsal_prefix.strip().strip('/')}/public/releases/"
        f"{arguments.public_release_id}/manifest.json"
    )
    container.update(
        {
            "image": arguments.image,
            "entryPoint": ["python", "/app/verify_public_rehearsal.py"],
            "command": [
                "--bucket",
                arguments.bucket,
                "--rehearsal-prefix",
                arguments.rehearsal_prefix,
                "--public-manifest-key",
                public_manifest_key,
                "--baseline-public-prefix",
                arguments.baseline_prefix,
                "--main-manifest-key",
                arguments.main_manifest_key,
                "--ask-manifest-key",
                arguments.ask_manifest_key,
                "--work-dir",
                "/tmp/mimir-public-verification",
            ],
        }
    )

    response = ecs.register_task_definition(
        family=arguments.family,
        taskRoleArn=source["taskRoleArn"],
        executionRoleArn=source["executionRoleArn"],
        networkMode=source["networkMode"],
        containerDefinitions=[container],
        volumes=source.get("volumes", []),
        placementConstraints=source.get("placementConstraints", []),
        requiresCompatibilities=source["requiresCompatibilities"],
        cpu=source["cpu"],
        memory=source["memory"],
        ephemeralStorage=source["ephemeralStorage"],
        runtimePlatform=source.get("runtimePlatform", {}),
        tags=[
            {"key": "Purpose", "value": "nsn-enrichment-public-verifier"},
            {"key": "LiveServing", "value": "false"},
            {"key": "ImageDigestPinned", "value": "true"},
        ],
    )
    task = response["taskDefinition"]
    print(
        json.dumps(
            {
                "task_definition_arn": task["taskDefinitionArn"],
                "image": arguments.image,
                "public_manifest_key": public_manifest_key,
                "main_manifest_key": arguments.main_manifest_key,
                "ask_manifest_key": arguments.ask_manifest_key,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
