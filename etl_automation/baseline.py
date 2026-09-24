"""Create and run an isolated control that reproduces the current manual ETL."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

from .paths import manual_cache_prefix


ROOT = Path(__file__).resolve().parent.parent
CONTROL_SEED_FILES = ("summary.parquet", "network.parquet")


def seed_manual_control(s3: Any, bucket: str, run_id: str) -> list[dict[str, str]]:
    """Copy the two prior-generation inputs read early by the monolithic ETL."""
    copied = []
    prefix = manual_cache_prefix(run_id)
    for filename in CONTROL_SEED_FILES:
        source_key = f"app_cache/{filename}"
        remote = s3.head_object(
            Bucket=bucket,
            Key=source_key,
            ChecksumMode="ENABLED",
        )
        version_id = str(remote.get("VersionId") or "").strip()
        if not version_id or version_id == "null":
            raise RuntimeError(f"Live control input is not versioned: {source_key}")
        destination_key = f"{prefix}{filename}"
        s3.copy(
            {"Bucket": bucket, "Key": source_key, "VersionId": version_id},
            bucket,
            destination_key,
            ExtraArgs={"MetadataDirective": "COPY"},
        )
        copied.append(
            {
                "filename": filename,
                "source_key": source_key,
                "source_version_id": version_id,
                "destination_key": destination_key,
            }
        )
    return copied


def manual_environment(run_id: str) -> dict[str, str]:
    environment = dict(os.environ)
    environment.update(
        {
            "ETL_AUTOMATION_RUN_ID": f"{run_id}-manual",
            "CACHE_PREFIX": manual_cache_prefix(run_id),
            "ONLY_FORCE_REBUILD_FILES": "0",
            "FORCE_REBUILD": "1",
            "FORCE_REBUILD_FILES": "",
            "PYTHONUNBUFFERED": "1",
        }
    )
    return environment


def run_manual_control(
    run_id: str,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> None:
    """Run the current all-in-one sequence against the isolated seeded prefix."""
    runner(
        [sys.executable, str(ROOT / "run_etl.py")],
        cwd=ROOT,
        env=manual_environment(run_id),
        check=True,
    )
