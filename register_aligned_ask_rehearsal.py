"""Register an isolated Ask Mimir candidate publisher for one ETL run."""

from __future__ import annotations

import argparse
import json

import boto3

from register_candidate_task import ALLOWED_CONTAINER_FIELDS, immutable_image


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True, type=immutable_image)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--rehearsal-prefix", required=True)
    parser.add_argument("--bucket", default="a-and-d-intel-lake-newaccount")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--source-task", default="mimir-etl-refresh:9")
    parser.add_argument("--family", default="mimir-etl-aligned-ask-rehearsal")
    arguments = parser.parse_args()

    prefix = arguments.rehearsal_prefix.strip().strip("/")
    if not prefix.startswith("mimir/rehearsals/"):
        raise RuntimeError("Ask rehearsal writes must stay under mimir/rehearsals/")
    run_id = arguments.run_id.strip()
    if not run_id or "/" in run_id:
        raise RuntimeError("run-id must be a non-empty path-safe identifier")

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
    candidate_key = f"{prefix}/ask_mimir/candidate_manifest.json"
    serving_prefix = f"mimir/staging/{run_id}/app_cache/"
    environment = [
        entry
        for entry in container.get("environment", [])
        if entry.get("name") != "ETL_AUTOMATION_RUN_ID"
    ]
    environment.append({"name": "ETL_AUTOMATION_RUN_ID", "value": run_id})
    container.update(
        {
            "image": arguments.image,
            "entryPoint": ["python", "/app/ask_mimir_beta/publish_runtime_release.py"],
            "command": [
                "--bucket",
                arguments.bucket,
                "--only",
                "serving-data",
                "--skip-local-input-verification",
                "--serving-source-prefix",
                serving_prefix,
                "--candidate-manifest-key",
                candidate_key,
            ],
            "environment": environment,
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
            {"key": "Purpose", "value": "aligned-ask-candidate-rehearsal"},
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
                "run_id": run_id,
                "candidate_manifest_key": candidate_key,
                "serving_source_prefix": serving_prefix,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
