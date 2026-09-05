"""Publish Ask Mimir artifacts and a last-written runtime manifest to S3."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import boto3
import duckdb

from bootstrap_data import DEFAULT_BUCKET, file_sha256


ROOT = Path(__file__).resolve().parent
CURRENT_MANIFEST_KEY = "ask_mimir/runtime/current_manifest.json"
DATA_ROOT = Path(
    os.getenv("ASK_MIMIR_SERVING_DATA_ROOT", str(ROOT.parent / "local_data"))
).resolve()
DATA_FILES = (
    "transactions.parquet",
    "network.parquet",
    "nsn_supplier_lookup.parquet",
    "nsn_profile_lookup.parquet",
    "nsn_cage_reference.parquet",
    "platform_bom.parquet",
    "cage_locations.parquet",
    "geo.parquet",
    "opportunities.parquet",
    "contracts_rolled.parquet",
    "profiles.parquet",
)
CLASSIFICATION_REFERENCE = ROOT / "validation-output" / "classification_reference.parquet"
ARTIFACT_DIRS: Tuple[Tuple[Path, str], ...] = (
    (ROOT / "validation-output", "metric-release"),
    (ROOT / "validation-output" / "company-context", "company-context"),
    (ROOT / "validation-output" / "company-opportunities", "company-opportunities"),
    (ROOT / "validation-output" / "platform-supply-chains", "platform-supply-chains"),
    (ROOT / "validation-output" / "program-momentum", "program-momentum"),
)


def artifact_files() -> Iterable[Tuple[Path, str]]:
    included = set()
    for source_dir, destination_dir in ARTIFACT_DIRS:
        for path in sorted(source_dir.rglob("*")):
            if not path.is_file() or "duckdb_tmp" in path.parts:
                continue
            if source_dir == ROOT / "validation-output" and path.parent != source_dir:
                continue
            destination = f"artifacts/{destination_dir}/{path.relative_to(source_dir)}"
            if destination not in included:
                included.add(destination)
                yield path, destination


def manifest_entry(path: Path, local_path: str, s3_key: str) -> Dict[str, Any]:
    return {
        "local_path": local_path,
        "s3_key": s3_key,
        "size": path.stat().st_size,
        "sha256": file_sha256(path),
    }


def serving_manifest_entry(
    s3: Any,
    bucket: str,
    source_key: str,
    local_path: str,
    local_file: Path,
) -> Dict[str, Any]:
    remote = s3.head_object(Bucket=bucket, Key=source_key)
    remote_size = int(remote["ContentLength"])
    if local_file.exists() and local_file.stat().st_size == remote_size:
        digest = file_sha256(local_file)
    else:
        with tempfile.NamedTemporaryFile() as temporary_file:
            s3.download_fileobj(bucket, source_key, temporary_file)
            temporary_file.flush()
            digest = file_sha256(Path(temporary_file.name))
    entry = {
        "local_path": local_path,
        "s3_key": source_key,
        "size": remote_size,
        "sha256": digest,
    }
    version_id = str(remote.get("VersionId") or "").strip()
    if not version_id or version_id == "null":
        raise RuntimeError(
            f"S3 versioning is required for atomic release input: {source_key}"
        )
    entry["s3_version_id"] = version_id
    return entry


def trigger_render_deploy(deploy_hook_url: str) -> int:
    request = urllib.request.Request(deploy_hook_url, method="POST")
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status not in {200, 202}:
            raise RuntimeError(f"Render deploy hook returned HTTP {response.status}")
        return response.status


def build_classification_reference() -> Path:
    summary = DATA_ROOT / "summary.parquet"
    if not summary.exists():
        raise FileNotFoundError(f"classification source was not found: {summary}")
    CLASSIFICATION_REFERENCE.parent.mkdir(parents=True, exist_ok=True)
    summary_sql = str(summary).replace("'", "''")
    output_sql = str(CLASSIFICATION_REFERENCE).replace("'", "''")
    with duckdb.connect() as connection:
        connection.execute(
            f"""
            COPY (
                SELECT
                    'PSC' AS classification_type,
                    TRIM(psc_code) AS code,
                    MODE(psc_description) AS description
                FROM read_parquet('{summary_sql}')
                WHERE COALESCE(TRIM(psc_code), '') <> ''
                  AND COALESCE(TRIM(psc_description), '') <> ''
                GROUP BY 2
                UNION ALL
                SELECT
                    'NAICS' AS classification_type,
                    TRIM(naics_code) AS code,
                    MODE(naics_description) AS description
                FROM read_parquet('{summary_sql}')
                WHERE COALESCE(TRIM(naics_code), '') <> ''
                  AND COALESCE(TRIM(naics_description), '') <> ''
                GROUP BY 2
            ) TO '{output_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    return CLASSIFICATION_REFERENCE


def publish(
    bucket: str,
    profile: str | None,
    deploy_hook_url: str | None = None,
) -> Dict[str, Any]:
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client("s3", region_name="us-east-1")
    generated_at = datetime.now(timezone.utc).isoformat()
    identity = hashlib.sha256(generated_at.encode()).hexdigest()[:12]
    release_id = f"ask-mimir-beta-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{identity}"
    prefix = f"ask_mimir/releases/{release_id}"
    entries: List[Dict[str, Any]] = []

    for path, local_path in artifact_files():
        print(f"Publishing artifact: {local_path}", file=sys.stderr)
        key = f"{prefix}/{local_path}"
        s3.upload_file(str(path), bucket, key)
        entries.append(manifest_entry(path, local_path, key))

    for filename in DATA_FILES:
        print(f"Pinning serving data: {filename}", file=sys.stderr)
        path = DATA_ROOT / filename
        source_key = f"app_cache/{filename}"
        entries.append(
            serving_manifest_entry(
                s3,
                bucket,
                source_key,
                f"data/{filename}",
                path,
            )
        )

    classification_path = build_classification_reference()
    classification_key = f"{prefix}/data/{classification_path.name}"
    s3.upload_file(str(classification_path), bucket, classification_key)
    entries.append(
        manifest_entry(
            classification_path,
            f"data/{classification_path.name}",
            classification_key,
        )
    )

    manifest = {
        "release_id": release_id,
        "generated_at": generated_at,
        "metric_release_id": json.loads(
            (ROOT / "validation-output" / "manifest.json").read_text()
        )["release_id"],
        "files": entries,
    }
    manifest_key = f"{prefix}/runtime_manifest.json"
    manifest_body = json.dumps(manifest, indent=2).encode()
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=manifest_body,
        ContentType="application/json",
    )
    # This is the only mutable object in the release path and is written last.
    s3.put_object(
        Bucket=bucket,
        Key=CURRENT_MANIFEST_KEY,
        Body=manifest_body,
        ContentType="application/json",
    )

    deploy_status = None
    if deploy_hook_url:
        deploy_status = trigger_render_deploy(deploy_hook_url)
    return {
        "release_id": release_id,
        "immutable_manifest_key": manifest_key,
        "current_manifest_key": CURRENT_MANIFEST_KEY,
        "render_deploy_status": deploy_status,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--profile")
    parser.add_argument(
        "--deploy-hook-url",
        default=os.getenv("ASK_MIMIR_RENDER_DEPLOY_HOOK_URL"),
    )
    arguments = parser.parse_args()
    print(
        json.dumps(
            publish(
                arguments.bucket,
                arguments.profile,
                arguments.deploy_hook_url,
            ),
            indent=2,
        )
    )
