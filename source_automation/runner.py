"""Entry points used by the existing Fargate task definition."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import boto3

from .usaspending_archive import acquire_candidate, reconciliation_fiscal_years


def acquire_usaspending_archive(
    bucket: str,
    region: str,
    run_id: str,
    *,
    s3: Any | None = None,
) -> dict[str, object]:
    client = s3 or boto3.Session(region_name=region).client("s3")
    years = reconciliation_fiscal_years(datetime.now(timezone.utc).date())
    return acquire_candidate(client, bucket, run_id, years)
