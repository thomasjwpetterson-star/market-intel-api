"""AWS Glue entry point for the daily DoD contract-announcement pipeline."""

from __future__ import annotations

import copy
import hashlib
import json
import sys
import tempfile
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

from ingest_dod_contract_announcements import ingest


SILVER_KEY = (
    "silver/dod/ref_contract_announcements/dod_contract_announcements.parquet"
)
APP_CACHE_KEY = "app_cache/dod_contract_announcements.parquet"
CURRENT_MANIFEST_KEY = "ask_mimir/runtime/current_manifest.json"
CANDIDATE_MANIFEST_KEY = "ask_mimir/runtime/candidate_manifest.json"
ANNOUNCEMENT_LOCAL_PATH = "data/dod_contract_announcements.parquet"


def _load_json_object(s3: Any, bucket: str, key: str) -> Dict[str, Any]:
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read())


def _download_existing(s3: Any, bucket: str, destination: Path) -> bool:
    try:
        s3.download_file(bucket, SILVER_KEY, str(destination))
        return True
    except ClientError as exc:
        code = str((exc.response.get("Error") or {}).get("Code") or "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def _announcement_manifest_entry(s3: Any, bucket: str) -> Dict[str, Any]:
    remote = s3.head_object(
        Bucket=bucket,
        Key=APP_CACHE_KEY,
        ChecksumMode="ENABLED",
    )
    entry: Dict[str, Any] = {
        "local_path": ANNOUNCEMENT_LOCAL_PATH,
        "s3_key": APP_CACHE_KEY,
        "size": int(remote["ContentLength"]),
    }
    version_id = str(remote.get("VersionId") or "").strip()
    if not version_id:
        raise RuntimeError("Announcement cache object has no S3 version ID")
    entry["s3_version_id"] = version_id
    etag = str(remote.get("ETag") or "").strip('"')
    if etag:
        entry["s3_etag"] = etag
    for field in ("ChecksumCRC32", "ChecksumCRC32C", "ChecksumSHA1", "ChecksumSHA256"):
        value = remote.get(field)
        if value:
            entry[f"s3_{field.lower()}"] = value
    checksum_type = remote.get("ChecksumType")
    if checksum_type:
        entry["s3_checksum_type"] = checksum_type
    return entry


def _validate_manifest(s3: Any, bucket: str, manifest: Dict[str, Any]) -> None:
    if not manifest.get("release_id"):
        raise RuntimeError("Runtime manifest has no release ID")
    paths = set()
    for entry in manifest.get("files") or []:
        local_path = str(entry.get("local_path") or "")
        if not local_path or local_path in paths:
            raise RuntimeError(f"Invalid or duplicate runtime path: {local_path}")
        paths.add(local_path)
        request = {"Bucket": bucket, "Key": entry["s3_key"]}
        if entry.get("s3_version_id"):
            request["VersionId"] = entry["s3_version_id"]
        remote = s3.head_object(**request)
        if int(remote["ContentLength"]) != int(entry["size"]):
            raise RuntimeError(f"Published object size mismatch: {local_path}")
        expected_etag = str(entry.get("s3_etag") or "").strip('"')
        actual_etag = str(remote.get("ETag") or "").strip('"')
        if expected_etag and actual_etag != expected_etag:
            raise RuntimeError(f"Published object ETag mismatch: {local_path}")
    if ANNOUNCEMENT_LOCAL_PATH not in paths:
        raise RuntimeError("Runtime manifest is missing the announcement dataset")


def publish_runtime_manifest(s3: Any, bucket: str) -> Dict[str, Any]:
    base = _load_json_object(s3, bucket, CURRENT_MANIFEST_KEY)
    entry = _announcement_manifest_entry(s3, bucket)
    files_by_path = {
        item["local_path"]: copy.deepcopy(item) for item in base.get("files") or []
    }
    files_by_path[ANNOUNCEMENT_LOCAL_PATH] = entry
    generated_at = datetime.now(timezone.utc).isoformat()
    identity = hashlib.sha256(
        f"{generated_at}|{entry['s3_version_id']}".encode()
    ).hexdigest()[:12]
    release_id = f"ask-mimir-beta-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{identity}"
    manifest = {
        "release_id": release_id,
        "generated_at": generated_at,
        "base_release_id": base.get("release_id"),
        "updated_domains": ["announcements"],
        "metric_release_id": base.get("metric_release_id"),
        "derived_release": copy.deepcopy(base.get("derived_release") or {}),
        "files": [files_by_path[path] for path in sorted(files_by_path)],
    }
    body = json.dumps(manifest, indent=2).encode()
    immutable_key = f"ask_mimir/releases/{release_id}/runtime_manifest.json"
    s3.put_object(Bucket=bucket, Key=immutable_key, Body=body, ContentType="application/json")
    _validate_manifest(s3, bucket, manifest)
    s3.put_object(
        Bucket=bucket,
        Key=CANDIDATE_MANIFEST_KEY,
        Body=body,
        ContentType="application/json",
    )
    s3.put_object(
        Bucket=bucket,
        Key=CURRENT_MANIFEST_KEY,
        Body=body,
        ContentType="application/json",
    )
    return {
        "release_id": release_id,
        "base_release_id": base.get("release_id"),
        "immutable_manifest_key": immutable_key,
        "current_manifest_key": CURRENT_MANIFEST_KEY,
    }


def _secret_value(secrets: Any, secret_id: str) -> str:
    response = secrets.get_secret_value(SecretId=secret_id)
    value = str(response.get("SecretString") or "").strip()
    if not value:
        raise RuntimeError(f"Secret {secret_id} has no string value")
    return value


def _trigger_render_deploy(url: str) -> int:
    request = urllib.request.Request(url, method="POST")
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status not in {200, 202}:
            raise RuntimeError(f"Render deploy hook returned HTTP {response.status}")
        return response.status


def run(
    *,
    bucket: str,
    ask_mimir_hook_secret: str,
    main_api_hook_secret: str,
    since_days: int = 7,
    max_articles: int = 20,
) -> Dict[str, Any]:
    session = boto3.Session(region_name="us-east-1")
    s3 = session.client("s3")
    secrets = session.client("secretsmanager")
    with tempfile.TemporaryDirectory(prefix="mimir-dod-announcements-") as directory:
        output_dir = Path(directory)
        parquet_path = output_dir / "dod_contract_announcements.parquet"
        history_loaded = _download_existing(s3, bucket, parquet_path)
        ingestion = ingest(
            output_dir,
            since_days=since_days,
            max_articles=max_articles,
            bucket=bucket,
            fail_on_fetch_error=True,
        )
        if not parquet_path.exists() or not ingestion.get("total_entries"):
            raise RuntimeError("DoD announcement refresh produced no serving records")
        s3.upload_file(str(parquet_path), bucket, APP_CACHE_KEY)
        publication = publish_runtime_manifest(s3, bucket)

    ask_status = _trigger_render_deploy(
        _secret_value(secrets, ask_mimir_hook_secret)
    )
    api_status = _trigger_render_deploy(
        _secret_value(secrets, main_api_hook_secret)
    )
    return {
        "history_loaded": history_loaded,
        "ingestion": ingestion,
        "publication": publication,
        "render_deploy_status": {
            "ask_mimir": ask_status,
            "main_api": api_status,
        },
    }


def main() -> None:
    from awsglue.utils import getResolvedOptions

    arguments = getResolvedOptions(
        sys.argv,
        [
            "BUCKET",
            "ASK_MIMIR_HOOK_SECRET",
            "MAIN_API_HOOK_SECRET",
            "SINCE_DAYS",
            "MAX_ARTICLES",
        ],
    )
    result = run(
        bucket=arguments["BUCKET"],
        ask_mimir_hook_secret=arguments["ASK_MIMIR_HOOK_SECRET"],
        main_api_hook_secret=arguments["MAIN_API_HOOK_SECRET"],
        since_days=int(arguments["SINCE_DAYS"]),
        max_articles=int(arguments["MAX_ARTICLES"]),
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
