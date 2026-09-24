"""Start CodeBuild from a short-lived rehearsal source without changing its project."""

import argparse
import json

import boto3


BUCKET = "a-and-d-intel-lake-newaccount"
SOURCE_KEY = (
    "mimir/rehearsals/cold-start-20260924-129c489dbe/"
    "build-source/mimir-etl-refresh-v8.zip"
)
IMAGE_TAG = "rehearsal-cold-start-20260924-129c489dbe-v8"

BUILDSPEC = """version: 0.2
phases:
  pre_build:
    commands:
      - curl -fsSL "$REHEARSAL_SOURCE_URL" -o /tmp/rehearsal-source.zip
      - mkdir -p /tmp/rehearsal-source
      - cd /tmp/rehearsal-source
      - unzip -q /tmp/rehearsal-source.zip
      - aws ecr get-login-password --region "$AWS_DEFAULT_REGION" | docker login --username AWS --password-stdin "$REPOSITORY_URI"
  build:
    commands:
      - IMAGE_URI="${REPOSITORY_URI}:${REHEARSAL_IMAGE_TAG}"
      - docker build --file Dockerfile.etl --tag "$IMAGE_URI" .
  post_build:
    commands:
      - IMAGE_URI="${REPOSITORY_URI}:${REHEARSAL_IMAGE_TAG}"
      - docker push "$IMAGE_URI"
      - IMAGE_DIGEST="$(aws ecr describe-images --repository-name "$REPOSITORY_NAME" --image-ids imageTag="$REHEARSAL_IMAGE_TAG" --query 'imageDetails[0].imageDigest' --output text)"
      - printf 'ETL_IMAGE_URI=%s@%s\\n' "$REPOSITORY_URI" "$IMAGE_DIGEST"
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-key", default=SOURCE_KEY)
    parser.add_argument("--image-tag", default=IMAGE_TAG)
    arguments = parser.parse_args()
    s3 = boto3.client("s3", region_name="us-east-1")
    codebuild = boto3.client("codebuild", region_name="us-east-1")
    source_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": BUCKET, "Key": arguments.source_key},
        ExpiresIn=3600,
    )
    response = codebuild.start_build(
        projectName="mimir-etl-refresh",
        sourceTypeOverride="NO_SOURCE",
        buildspecOverride=BUILDSPEC,
        environmentVariablesOverride=[
            {
                "name": "REHEARSAL_SOURCE_URL",
                "value": source_url,
                "type": "PLAINTEXT",
            },
            {
                "name": "REHEARSAL_IMAGE_TAG",
                "value": arguments.image_tag,
                "type": "PLAINTEXT",
            },
        ],
    )
    build = response["build"]
    print(
        json.dumps(
            {
                "id": build["id"],
                "status": build["buildStatus"],
                "source_key": arguments.source_key,
                "image_tag": arguments.image_tag,
            }
        )
    )


if __name__ == "__main__":
    main()
