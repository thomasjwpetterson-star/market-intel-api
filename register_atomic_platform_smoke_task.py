"""Register a standalone Fargate task for the atomic consumer rehearsal."""

from __future__ import annotations

import argparse
import json

import boto3

from register_candidate_task import ALLOWED_CONTAINER_FIELDS, immutable_image


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True, type=immutable_image)
    parser.add_argument("--rehearsal-prefix", required=True)
    parser.add_argument("--platform-version-id", required=True)
    parser.add_argument("--source-task", default="mimir-etl-refresh:9")
    parser.add_argument("--family", default="mimir-atomic-platform-consumer-smoke")
    return parser.parse_args()


def main():
    args = parse_args()
    rehearsal_prefix = args.rehearsal_prefix.strip().strip("/")
    if not rehearsal_prefix.startswith("mimir/rehearsals/"):
        raise RuntimeError("Atomic smoke writes must stay under mimir/rehearsals/")
    ecs = boto3.client("ecs", region_name="us-east-1")
    source = ecs.describe_task_definition(taskDefinition=args.source_task)[
        "taskDefinition"
    ]
    original = source["containerDefinitions"][0]
    container = {
        key: value
        for key, value in original.items()
        if key in ALLOWED_CONTAINER_FIELDS
    }
    platform_key = f"{rehearsal_prefix}/consumer-platform/current_manifest.json"
    report_key = f"{rehearsal_prefix}/reports/atomic-consumer-smoke-report.json"
    container.update(
        {
            "image": args.image,
            "entryPoint": ["python", "/app/smoke_atomic_platform.py"],
            "command": [
                "--bucket", "a-and-d-intel-lake-newaccount",
                "--platform-manifest-key", platform_key,
                "--platform-manifest-version-id", args.platform_version_id,
                "--work-dir", "/tmp/mimir-atomic-consumer-smoke",
                "--report-key", report_key,
            ],
        }
    )
    response = ecs.register_task_definition(
        family=args.family,
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
            {"key": "Purpose", "value": "atomic-platform-consumer-smoke"},
            {"key": "LiveServing", "value": "false"},
        ],
    )
    task = response["taskDefinition"]
    print(
        json.dumps(
            {
                "task_definition_arn": task["taskDefinitionArn"],
                "image": args.image,
                "rehearsal_prefix": rehearsal_prefix,
                "platform_key": platform_key,
                "platform_version_id": args.platform_version_id,
                "report_key": report_key,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
