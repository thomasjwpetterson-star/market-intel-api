"""Fail-closed freshness checks for the daily ETL dependencies."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os
import time
from typing import Any, Dict, Sequence


@dataclass(frozen=True)
class GlueGate:
    job_name: str
    max_age_hours: float = 36.0


DAILY_GLUE_GATES: tuple[GlueGate, ...] = (
    GlueGate("usaspending_prime_api_to_bronze"),
    GlueGate("usaspending_bronze_to_silver"),
    GlueGate("dla_process_solicitations"),
    GlueGate("dod_contract_announcements_daily"),
)


class UpstreamValidationError(RuntimeError):
    """Raised when a required upstream is missing, failed, running, or stale."""


def validate_athena_serving_views(
    athena: Any,
    bucket: str,
    database: str = "market_intel_gold",
    timeout_seconds: float = 120.0,
    poll_seconds: float = 1.0,
) -> Dict[str, object]:
    """Analyze the primary serving-view chain before launching expensive tasks."""
    response = athena.start_query_execution(
        QueryString='SELECT 1 FROM "dashboard_summary_v2" LIMIT 1',
        QueryExecutionContext={"Database": database},
        ResultConfiguration={
            "OutputLocation": f"s3://{bucket}/temp_etl/preflight/"
        },
        WorkGroup=os.getenv("ATHENA_WORKGROUP", "primary").strip() or "primary",
    )
    query_id = response["QueryExecutionId"]
    deadline = time.monotonic() + timeout_seconds
    while True:
        execution = athena.get_query_execution(QueryExecutionId=query_id)[
            "QueryExecution"
        ]
        status = execution["Status"]
        state = str(status["State"])
        if state == "SUCCEEDED":
            return {
                "type": "athena",
                "name": f"{database}.dashboard_summary_v2",
                "state": state,
                "query_execution_id": query_id,
            }
        if state in {"FAILED", "CANCELLED"}:
            reason = status.get("StateChangeReason", "unknown Athena failure")
            raise UpstreamValidationError(
                f"Athena serving-view health check failed: {reason}"
            )
        if time.monotonic() >= deadline:
            athena.stop_query_execution(QueryExecutionId=query_id)
            raise UpstreamValidationError(
                f"Athena serving-view health check timed out: {query_id}"
            )
        time.sleep(poll_seconds)


def _age_hours(completed_at: datetime, now: datetime) -> float:
    if completed_at.tzinfo is None:
        completed_at = completed_at.replace(tzinfo=timezone.utc)
    return (now - completed_at.astimezone(timezone.utc)).total_seconds() / 3600.0


def validate_glue_jobs(
    glue: Any,
    now: datetime,
    gates: Sequence[GlueGate] = DAILY_GLUE_GATES,
) -> list[Dict[str, object]]:
    results = []
    for gate in gates:
        runs = glue.get_job_runs(JobName=gate.job_name, MaxResults=1).get("JobRuns", [])
        if not runs:
            raise UpstreamValidationError(
                f"Required Glue job has no run history: {gate.job_name}"
            )
        latest = runs[0]
        state = str(latest.get("JobRunState") or "UNKNOWN")
        if state != "SUCCEEDED":
            raise UpstreamValidationError(
                f"Latest Glue run is not successful: {gate.job_name}={state}"
            )
        completed_at = latest.get("CompletedOn")
        if not isinstance(completed_at, datetime):
            raise UpstreamValidationError(
                f"Successful Glue run has no completion time: {gate.job_name}"
            )
        age = _age_hours(completed_at, now)
        if age > gate.max_age_hours:
            raise UpstreamValidationError(
                f"Glue input is stale: {gate.job_name} is {age:.1f} hours old"
            )
        results.append(
            {
                "type": "glue",
                "name": gate.job_name,
                "state": state,
                "completed_at": completed_at.astimezone(timezone.utc).isoformat(),
                "age_hours": round(age, 2),
            }
        )
    return results


def validate_state_machine(
    stepfunctions: Any,
    state_machine_arn: str,
    now: datetime,
    max_age_hours: float = 36.0,
) -> Dict[str, object]:
    executions = stepfunctions.list_executions(
        stateMachineArn=state_machine_arn,
        maxResults=1,
    ).get("executions", [])
    if not executions:
        raise UpstreamValidationError(
            f"Required state machine has no execution history: {state_machine_arn}"
        )
    latest = executions[0]
    status = str(latest.get("status") or "UNKNOWN")
    if status != "SUCCEEDED":
        raise UpstreamValidationError(
            f"Latest state-machine execution is not successful: {status}"
        )
    completed_at = latest.get("stopDate")
    if not isinstance(completed_at, datetime):
        raise UpstreamValidationError(
            f"Successful state-machine execution has no stop time: {state_machine_arn}"
        )
    age = _age_hours(completed_at, now)
    if age > max_age_hours:
        raise UpstreamValidationError(
            f"State-machine input is stale: {state_machine_arn} is {age:.1f} hours old"
        )
    return {
        "type": "stepfunctions",
        "name": state_machine_arn,
        "state": status,
        "completed_at": completed_at.astimezone(timezone.utc).isoformat(),
        "age_hours": round(age, 2),
    }


def validate_daily_upstreams(
    glue: Any,
    stepfunctions: Any,
    athena: Any,
    bucket: str,
    sam_state_machine_arn: str,
    now: datetime | None = None,
) -> list[Dict[str, object]]:
    if not sam_state_machine_arn.strip():
        raise ValueError("SAM daily state-machine ARN is required")
    checked_at = now or datetime.now(timezone.utc)
    if checked_at.tzinfo is None:
        raise ValueError("preflight timestamp must be timezone-aware")
    results = validate_glue_jobs(glue, checked_at)
    results.append(
        validate_state_machine(
            stepfunctions,
            sam_state_machine_arn,
            checked_at,
        )
    )
    results.append(validate_athena_serving_views(athena, bucket))
    return results
