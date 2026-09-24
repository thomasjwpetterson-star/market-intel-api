"""Register a standalone ECS task revision for the isolated public rehearsal."""

import json

import boto3


IMAGE_TAG = "rehearsal-cold-start-20260924-129c489dbe-v4"
REPOSITORY_URI = "868631722720.dkr.ecr.us-east-1.amazonaws.com/mimir-etl-refresh"
REHEARSAL_PREFIX = "mimir/rehearsals/cold-start-20260924-129c489dbe"
MAIN_MANIFEST_VERSION = "SHErYbuD_LdeMXIUsVbbmAz_wC8JdfWI"


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
    container = copy_container(source["containerDefinitions"][0], image_uri)
    response = ecs.register_task_definition(
        family="mimir-etl-public-rehearsal",
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
            {"key": "Purpose", "value": "isolated-public-cold-start-rehearsal"},
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


def copy_container(source, image_uri):
    allowed = {
        key: value
        for key, value in source.items()
        if key
        in {
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
    }
    allowed.update(
        {
            "image": image_uri,
            "entryPoint": ["python", "/app/rehearse_public_release.py"],
            "command": [
                "--bucket",
                "a-and-d-intel-lake-newaccount",
                "--main-manifest-s3-key",
                "mimir/runtime/candidate_manifest.json",
                "--main-manifest-s3-version-id",
                MAIN_MANIFEST_VERSION,
                "--previous-public-dir",
                "/tmp/mimir-rehearsal/previous-public",
                "--previous-public-s3-prefix",
                f"{REHEARSAL_PREFIX}/baseline-public",
                "--output-dir",
                "/tmp/mimir-rehearsal/output",
                "--report",
                "/tmp/mimir-rehearsal/report.json",
                "--extension-directory",
                "/tmp/mimir-rehearsal/extensions",
                "--download-inputs",
                "--publish-rehearsal-prefix",
                REHEARSAL_PREFIX,
            ],
        }
    )
    return allowed


if __name__ == "__main__":
    main()
