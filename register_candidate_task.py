"""Register a one-off candidate-only ECS task with an immutable image.

This intentionally copies the deployed ETL task's roles, resource limits,
network mode and logging configuration. It does not update CloudFormation,
the scheduled task definition, a service, or any production release pointer.
"""

from __future__ import annotations

import argparse
import json

import boto3


ALLOWED_CONTAINER_FIELDS = {
    "name",
    "cpu",
    "memory",
    "memoryReservation",
    "links",
    "portMappings",
    "essential",
    "environment",
    "environmentFiles",
    "mountPoints",
    "volumesFrom",
    "linuxParameters",
    "secrets",
    "dependsOn",
    "startTimeout",
    "stopTimeout",
    "hostname",
    "user",
    "workingDirectory",
    "disableNetworking",
    "privileged",
    "readonlyRootFilesystem",
    "dnsServers",
    "dnsSearchDomains",
    "extraHosts",
    "dockerSecurityOptions",
    "interactive",
    "pseudoTerminal",
    "dockerLabels",
    "ulimits",
    "logConfiguration",
    "healthCheck",
    "systemControls",
    "resourceRequirements",
    "firelensConfiguration",
    "credentialSpecs",
}


def immutable_image(value: str) -> str:
    if "@sha256:" not in value or value.endswith("@sha256:"):
        raise argparse.ArgumentTypeError("image must be pinned by @sha256 digest")
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True, type=immutable_image)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--source-task", default="mimir-etl-refresh:6")
    parser.add_argument("--family", default="mimir-etl-nsn-enrichment-candidate")
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
    container["image"] = arguments.image

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
            {"key": "Purpose", "value": "candidate-only-nsn-enrichment"},
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
                "cpu": task["cpu"],
                "memory": task["memory"],
                "ephemeral_storage_gib": task["ephemeralStorage"]["sizeInGiB"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
