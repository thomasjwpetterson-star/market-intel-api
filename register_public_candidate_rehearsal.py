"""Register an isolated public-release rehearsal for an exact main candidate."""

from __future__ import annotations

import argparse
import json

import boto3

from register_candidate_task import ALLOWED_CONTAINER_FIELDS, immutable_image


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True, type=immutable_image)
    parser.add_argument("--main-manifest-version", required=True)
    parser.add_argument("--rehearsal-prefix", required=True)
    parser.add_argument(
        "--baseline-prefix",
        required=True,
        help=(
            "Immutable directory containing the latest accepted public artifacts. "
            "This is intentionally required so an older baseline cannot be reused "
            "silently."
        ),
    )
    parser.add_argument("--bucket", default="a-and-d-intel-lake-newaccount")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--source-task", default="mimir-etl-refresh:6")
    parser.add_argument("--family", default="mimir-etl-nsn-public-rehearsal")
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
    container.update(
        {
            "image": arguments.image,
            "entryPoint": ["python", "/app/rehearse_public_release.py"],
            "command": [
                "--bucket",
                arguments.bucket,
                "--main-manifest-s3-key",
                "mimir/runtime/candidate_manifest.json",
                "--main-manifest-s3-version-id",
                arguments.main_manifest_version,
                "--previous-public-dir",
                "/tmp/mimir-rehearsal/previous-public",
                "--previous-public-s3-prefix",
                arguments.baseline_prefix,
                "--output-dir",
                "/tmp/mimir-rehearsal/output",
                "--report",
                "/tmp/mimir-rehearsal/report.json",
                "--extension-directory",
                "/tmp/mimir-rehearsal/extensions",
                "--download-inputs",
                "--publish-rehearsal-prefix",
                arguments.rehearsal_prefix,
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
            {"key": "Purpose", "value": "nsn-enrichment-public-rehearsal"},
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
                "main_manifest_version": arguments.main_manifest_version,
                "rehearsal_prefix": arguments.rehearsal_prefix,
                "baseline_prefix": arguments.baseline_prefix,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
