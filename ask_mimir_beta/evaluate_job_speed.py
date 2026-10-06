"""Measure the customer job path for a small cross-workflow Ask sample.

This keeps answer text out of the saved report. Run against an isolated,
approved real-model environment before a speed release; mock answers cannot
establish the latency or factual-quality target.
"""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from .evaluate_lab import score_result
except ImportError:  # Direct script invocation from ask_mimir_beta/.
    from evaluate_lab import score_result


ROOT = Path(__file__).resolve().parent


def request_json(url: str, *, method: str, payload: dict | None, headers: dict,
                 timeout: float = 35) -> dict:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")[:500]
        raise RuntimeError(f"Ask job API returned HTTP {error.code}: {detail}") from error


def first_visible_text(job: dict[str, Any]) -> str | None:
    for field in ("provisional_answer", "evidence_preview"):
        value = job.get(field)
        if isinstance(value, dict) and str(value.get("text") or "").strip():
            return field
    if job.get("status") == "completed" and str((job.get("result") or {}).get("answer") or "").strip():
        return "completed_answer"
    return None


def evaluate_job_case(base_url: str, case: dict, *, tier: str, subject: str,
                      proxy_secret: str | None = None, poll_seconds: float = 0.5,
                      deadline_seconds: float = 360) -> dict:
    request_id = str(uuid.uuid4())
    headers = {
        "Content-Type": "application/json",
        "X-Ask-Mimir-Tier": tier,
        "X-Ask-Mimir-Subject": subject,
    }
    if proxy_secret:
        headers["X-Ask-Mimir-Proxy-Secret"] = proxy_secret
    payload = {
        "messages": case.get("messages") or [{"role": "user", "content": case["question"]}],
        "client_request_id": request_id,
    }
    if case.get("active_scope"):
        payload["active_scope"] = case["active_scope"]
    if case.get("conversation_id"):
        payload["conversation_id"] = case["conversation_id"]
    started = time.perf_counter()
    job = request_json(
        f"{base_url.rstrip('/')}/api/ask/jobs",
        method="POST", payload=payload, headers=headers,
    )
    accepted_ms = round((time.perf_counter() - started) * 1000)
    if job.get("request_id") != request_id:
        raise RuntimeError("Ask job returned an unexpected request identifier")
    first_text_ms = None
    first_text_mode = None
    first_model_draft_ms = None
    first_evidence_preview_ms = None
    while True:
        mode = first_visible_text(job)
        elapsed_ms = round((time.perf_counter() - started) * 1000)
        if mode and first_text_ms is None:
            first_text_ms = elapsed_ms
            first_text_mode = mode
        if job.get("provisional_answer") and first_model_draft_ms is None:
            first_model_draft_ms = elapsed_ms
        if job.get("evidence_preview") and first_evidence_preview_ms is None:
            first_evidence_preview_ms = elapsed_ms
        if job.get("status") in {"completed", "failed"}:
            break
        if time.perf_counter() - started >= deadline_seconds:
            raise TimeoutError(f"Ask job exceeded {deadline_seconds}s: {case['case_id']}")
        time.sleep(poll_seconds)
        job = request_json(
            f"{base_url.rstrip('/')}/api/ask/jobs/{request_id}",
            method="GET", payload=None, headers=headers,
        )
    completed_ms = round((time.perf_counter() - started) * 1000)
    result = job.get("result") or {}
    quality = score_result(case, result) if job.get("status") == "completed" else None
    return {
        "case_id": case["case_id"],
        "workflow": job.get("workflow"),
        "expected_workflow": case.get("expected_workflow"),
        "status": job.get("status"),
        "accepted_ms": accepted_ms,
        "first_visible_text_ms": first_text_ms,
        "first_text_mode": first_text_mode,
        "first_model_draft_ms": first_model_draft_ms,
        "first_evidence_preview_ms": first_evidence_preview_ms,
        "server_first_model_text_ms": (job.get("timings") or {}).get("first_model_text_ms"),
        "completed_ms": completed_ms,
        "first_text_within_30s": first_text_ms is not None and first_text_ms <= 30_000,
        "workflow_correct": (
            not case.get("expected_workflow")
            or job.get("workflow") == case["expected_workflow"]
        ),
        "basic_answer_checks_passed": quality["passed"] if quality else False,
        "basic_answer_checks": quality["checks"] if quality else [],
        "failure_stage": job.get("failure_stage") if job.get("status") == "failed" else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:10100")
    parser.add_argument("--cases-file", type=Path, default=ROOT / "speed_eval_cases.json")
    parser.add_argument("--case-id", action="append")
    parser.add_argument("--tier", default="enterprise")
    parser.add_argument("--subject", default="ask-speed-evaluation")
    parser.add_argument("--proxy-secret", default=os.getenv("ASK_MIMIR_TRUSTED_PROXY_SECRET"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--enforce-target", action="store_true")
    args = parser.parse_args()
    cases = json.loads(args.cases_file.read_text())
    cases = [case for case in cases if not args.case_id or case["case_id"] in args.case_id]
    if not cases:
        raise SystemExit("No speed-evaluation cases matched")
    results = []
    for case in cases:
        try:
            result = evaluate_job_case(
                args.base_url, case, tier=args.tier,
                subject=f"{args.subject}:{case['case_id']}", proxy_secret=args.proxy_secret,
            )
        except Exception as error:
            result = {
                "case_id": case["case_id"], "expected_workflow": case.get("expected_workflow"),
                "status": "benchmark_error", "error_class": type(error).__name__,
                "first_text_within_30s": False, "workflow_correct": False,
                "basic_answer_checks_passed": False,
            }
        results.append(result)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "base_url": args.base_url,
        "method": "asynchronous Ask job API; first visible poll containing provisional or completed text",
        "target": "first useful text by 30 seconds for each sample, with basic answer checks",
        "quality_review_note": "Term and route checks are a smoke test; factual and citation quality still require reviewed answers against the incumbent release.",
        "cases": results,
    }
    report["all_cases_passed"] = all(
        row["status"] == "completed"
        and row["workflow_correct"]
        and row["first_text_within_30s"]
        and row["basic_answer_checks_passed"]
        for row in results
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"all_cases_passed": report["all_cases_passed"], "output": str(args.output)}))
    if args.enforce_target and not report["all_cases_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
