"""Register a standalone ECS task for an isolated cold-start verifier."""

import argparse
import json

import boto3

from register_candidate_task import ALLOWED_CONTAINER_FIELDS, immutable_image

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True, type=immutable_image)
    parser.add_argument("--rehearsal-prefix", required=True)
    parser.add_argument("--public-manifest-key", required=True)
    parser.add_argument("--baseline-public-prefix", required=True)
    parser.add_argument("--baseline-profile-prefix", required=True)
    parser.add_argument("--main-manifest-key", required=True)
    parser.add_argument("--ask-manifest-key", required=True)
    parser.add_argument("--bucket", default="a-and-d-intel-lake-newaccount")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--source-task", default="mimir-etl-refresh:6")
    parser.add_argument("--family", default="mimir-etl-public-rehearsal-verifier")
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
            "entryPoint": ["python", "/app/verify_public_rehearsal.py"],
            "command": [
                "--bucket",
                arguments.bucket,
                "--rehearsal-prefix",
                arguments.rehearsal_prefix,
                "--public-manifest-key",
                arguments.public_manifest_key,
                "--baseline-public-prefix",
                arguments.baseline_public_prefix,
                "--baseline-profile-prefix",
                arguments.baseline_profile_prefix,
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
            {"key": "Purpose", "value": "isolated-public-verification"},
            {"key": "LiveServing", "value": "false"},
        ],
    )
    task = response["taskDefinition"]
    print(
        json.dumps(
            {
                "task_definition_arn": task["taskDefinitionArn"],
                "image": arguments.image,
                "rehearsal_prefix": arguments.rehearsal_prefix,
                "public_manifest_key": arguments.public_manifest_key,
            }
        )
    )


if __name__ == "__main__":
    main()
