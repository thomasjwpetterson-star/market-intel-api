"""Fail-closed action-month coverage audit against USAspending's published feed."""

from __future__ import annotations

from datetime import date, timedelta
import os
import time
from typing import Callable

import requests


COUNT_URL = "https://api.usaspending.gov/api/v2/download/count/"
AWARD_TYPES = ("A", "B", "C", "D", "IDV_A", "IDV_B", "IDV_B_A",
               "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E")


class CoverageError(RuntimeError):
    """Published action-month coverage is unavailable or materially incomplete."""


def recent_closed_months(today: date, count: int | None = None) -> list[tuple[str, str, str]]:
    if count is None:
        current_fy = today.year + (today.month >= 10)
        first_prior_fy_month = date(current_fy - 2, 10, 1)
        count = (today.year - first_prior_fy_month.year) * 12 + today.month - first_prior_fy_month.month
    year, month = today.year, today.month
    months = []
    for _ in range(count):
        month -= 1
        if month == 0:
            year, month = year - 1, 12
        start = date(year, month, 1)
        end = date(year + (month == 12), month % 12 + 1, 1) - timedelta(days=1)
        months.append((start.strftime("%Y-%m"), start.isoformat(), end.isoformat()))
    return list(reversed(months))


def official_count(start: str, end: str, post: Callable = requests.post) -> int:
    payload = {"filters": {
        "agencies": [{"type": "awarding", "tier": "toptier", "name": "Department of Defense"}],
        "award_type_codes": list(AWARD_TYPES),
        "time_period": [{"start_date": start, "end_date": end}],
    }}
    for attempt in range(3):
        try:
            response = post(COUNT_URL, json=payload, timeout=60)
            response.raise_for_status()
            body = response.json()
            count = body.get("calculated_transaction_count")
            if count is None or body.get("transaction_rows_gt_limit"):
                raise CoverageError(f"Official count is incomplete for {start} to {end}")
            return int(count)
        except (requests.RequestException, ValueError) as exc:
            if attempt == 2:
                raise CoverageError(f"Official count failed for {start} to {end}: {exc}") from exc
            time.sleep(2 ** attempt)
    raise AssertionError("unreachable")


def silver_counts(athena, bucket: str, start: str, end: str,
                  timeout_seconds: float = 180.0) -> dict[str, int]:
    sql = (
        "SELECT substr(CAST(action_date AS VARCHAR), 1, 7) AS action_month, COUNT(*) AS rows "
        "FROM market_intel_silver.dataset_prime_contracts "
        f"WHERE TRY_CAST(action_date AS DATE) BETWEEN DATE '{start}' AND DATE '{end}' "
        "GROUP BY 1"
    )
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": "market_intel_silver"},
        ResultConfiguration={"OutputLocation": f"s3://{bucket}/mimir/coverage-audit/athena/"},
        WorkGroup=os.getenv("ATHENA_WORKGROUP", "primary") or "primary",
    )
    query_id = response["QueryExecutionId"]
    deadline = time.monotonic() + timeout_seconds
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        if status["State"] == "SUCCEEDED":
            break
        if status["State"] in {"FAILED", "CANCELLED"}:
            raise CoverageError(f"Silver coverage query failed: {status.get('StateChangeReason')}")
        if time.monotonic() >= deadline:
            athena.stop_query_execution(QueryExecutionId=query_id)
            raise CoverageError("Silver coverage query timed out")
        time.sleep(2)
    rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
    return {row["Data"][0]["VarCharValue"]: int(row["Data"][1]["VarCharValue"])
            for row in rows[1:]}


def audit_recent_months(athena, bucket: str, today: date,
                        count_fn: Callable[[str, str], int] = official_count) -> list[dict]:
    months = recent_closed_months(today)
    observed = silver_counts(athena, bucket, months[0][1], months[-1][2])
    results = []
    for month, start, end in months:
        published = count_fn(start, end)
        silver = observed.get(month, 0)
        gap = published - silver
        tolerance = max(100, int(published * 0.005))
        results.append({"month": month, "official_published_rows": published,
                        "silver_rows": silver, "gap": gap, "tolerance": tolerance})
    bad = [row for row in results if abs(row["gap"]) > row["tolerance"]]
    if bad:
        raise CoverageError(f"Published DoD source coverage differs materially: {bad}")
    return results
