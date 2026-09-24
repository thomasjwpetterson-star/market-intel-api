"""Register a standalone ECS task for the isolated cold-start verifier."""

import json

import boto3


IMAGE_TAG = "rehearsal-cold-start-20260924-129c489dbe-v5"
REPOSITORY_URI = "868631722720.dkr.ecr.us-east-1.amazonaws.com/mimir-etl-refresh"
REHEARSAL_PREFIX = "mimir/rehearsals/cold-start-20260924-129c489dbe"
PUBLIC_RELEASE_ID = "public-intelligence-20260924T005335Z"


def main():
    ecs = boto3.client("ecs", region_name="us-east-1")
    ecr = boto3.client("ecr", region_name="us-east-1")
    source = ecs.describe_task_definition(taskDefinition="mimir-etl-refresh:6")[
        "taskDefinition"
    ]
    image = ecr.describe_images(
        repositoryName="mimir-etl-refresh",
        imageIds=[{"imageTag": IMAGE_TAG}],
    )["imageDetails"][0]
    image_uri = f"{REPOSITORY_URI}@{image['imageDigest']}"
    original = source["containerDefinitions"][0]
    container = {
        key: value
        for key, value in original.items()
        if key
        in {
            "name",
            "cpu",
            "memory",
            "memoryReservation",
            "portMappings",
            "essential",
            "environment",
            "mountPoints",
            "volumesFrom",
            "linuxParameters",
            "secrets",
            "dependsOn",
            "startTimeout",
            "stopTimeout",
            "workingDirectory",
            "readonlyRootFilesystem",
            "logConfiguration",
            "ulimits",
            "systemControls",
            "resourceRequirements",
        }
    }
    container.update(
        {
            "image": image_uri,
            "entryPoint": ["python", "/app/verify_public_rehearsal.py"],
            "command": [
                "--bucket",
                "a-and-d-intel-lake-newaccount",
                "--rehearsal-prefix",
                REHEARSAL_PREFIX,
                "--public-manifest-key",
                f"{REHEARSAL_PREFIX}/public/releases/{PUBLIC_RELEASE_ID}/manifest.json",
                "--baseline-public-prefix",
                f"{REHEARSAL_PREFIX}/baseline-public",
                "--main-manifest-key",
                "mimir/releases/4aa34dd9-f060-4945-981c-8679769b0de1/manifest.json",
                "--ask-manifest-key",
                "ask_mimir/releases/ask-mimir-beta-20260923T113507Z-5c57d5c878fc/runtime_manifest.json",
                "--work-dir",
                "/tmp/mimir-public-verification",
            ],
        }
    )
    response = ecs.register_task_definition(
        family="mimir-etl-public-rehearsal-verifier",
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
                "image": image_uri,
            }
        )
    )


if __name__ == "__main__":
    main()
