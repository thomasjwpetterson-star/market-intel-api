"""Refresh DoD announcements and activate the small serving artifact."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

from bootstrap_data import DEFAULT_BUCKET
from ingest_dod_contract_announcements import ingest
from publish_runtime_release import publish, trigger_render_deploy


SILVER_KEY = (
    "silver/dod/ref_contract_announcements/dod_contract_announcements.parquet"
)
APP_CACHE_KEY = "app_cache/dod_contract_announcements.parquet"


def _session(profile: str | None) -> boto3.Session:
    return boto3.Session(profile_name=profile) if profile else boto3.Session()


def _download_existing(s3: Any, bucket: str, destination: Path) -> bool:
    try:
        s3.download_file(bucket, SILVER_KEY, str(destination))
        return True
    except ClientError as exc:
        code = str((exc.response.get("Error") or {}).get("Code") or "")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def refresh(
    output_dir: Path,
    *,
    bucket: str = DEFAULT_BUCKET,
    profile: str | None = None,
    since_days: int = 7,
    max_articles: int = 20,
    promote: bool = False,
    allow_partial: bool = False,
    ask_mimir_deploy_hook_url: str | None = None,
    main_api_deploy_hook_url: str | None = None,
) -> Dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / "dod_contract_announcements.parquet"
    session = _session(profile)
    s3 = session.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
    history_loaded = _download_existing(s3, bucket, parquet_path)

    ingestion = ingest(
        output_dir,
        since_days=since_days,
        max_articles=max_articles,
        bucket=bucket,
        profile=profile,
        fail_on_fetch_error=not allow_partial,
    )
    if not parquet_path.exists() or not ingestion.get("total_entries"):
        raise RuntimeError("DoD announcement refresh produced no serving records")

    s3.upload_file(str(parquet_path), bucket, APP_CACHE_KEY)
    publication = publish(
        bucket,
        profile,
        deploy_hook_url=ask_mimir_deploy_hook_url,
        domains={"announcements"},
        promote=promote,
        verify_local_inputs=False,
    )

    main_api_deploy_status = None
    if promote and main_api_deploy_hook_url:
        main_api_deploy_status = trigger_render_deploy(main_api_deploy_hook_url)

    return {
        "history_loaded": history_loaded,
        "ingestion": ingestion,
        "app_cache_key": APP_CACHE_KEY,
        "publication": publication,
        "main_api_deploy_status": main_api_deploy_status,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--bucket", default=os.getenv("ASK_MIMIR_BUCKET", DEFAULT_BUCKET))
    parser.add_argument("--profile")
    parser.add_argument("--since-days", type=int, default=7)
    parser.add_argument("--max-articles", type=int, default=20)
    parser.add_argument("--promote", action="store_true")
    parser.add_argument("--allow-partial", action="store_true")
    args = parser.parse_args()

    common = {
        "bucket": args.bucket,
        "profile": args.profile,
        "since_days": args.since_days,
        "max_articles": args.max_articles,
        "promote": args.promote,
        "allow_partial": args.allow_partial,
        "ask_mimir_deploy_hook_url": os.getenv(
            "ASK_MIMIR_RENDER_DEPLOY_HOOK_URL"
        ),
        "main_api_deploy_hook_url": os.getenv("MIMIR_API_RENDER_DEPLOY_HOOK_URL"),
    }
    if args.output_dir:
        result = refresh(args.output_dir.resolve(), **common)
    else:
        with tempfile.TemporaryDirectory(prefix="mimir-dod-announcements-") as directory:
            result = refresh(Path(directory), **common)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
