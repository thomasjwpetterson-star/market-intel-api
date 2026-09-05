"""Run repeatable Ask Mimir lab checks against mock or approved real-model mode."""

from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parent


def post_json(
    url: str, payload: Dict[str, Any], *, tier: str, subject: str
) -> Dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "X-Ask-Mimir-Tier": tier,
            "X-Ask-Mimir-Subject": subject,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=360) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"lab returned HTTP {exc.code}: {detail}") from exc


def score_result(case: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
    answer = str(result.get("answer") or "")
    tools = [entry.get("tool") for entry in result.get("tool_trace", []) if entry.get("tool")]
    checks: List[Dict[str, Any]] = []
    for tool in case.get("required_tools", []):
        checks.append({"check": f"required tool: {tool}", "passed": tool in tools})
    for term in case.get("required_answer_terms", []):
        checks.append({"check": f"required answer term: {term}", "passed": term.lower() in answer.lower()})
    forbidden_terms = [
        "pack_id",
        "ranking_universe",
        "current_year_treatment",
        "release `",
        "the dossier",
        "platform supply-chain pack",
        *case.get("forbidden_answer_terms", []),
    ]
    for term in forbidden_terms:
        checks.append({"check": f"forbidden answer term: {term}", "passed": term.lower() not in answer.lower()})
    evidence_calls = [
        entry for entry in result.get("tool_trace", []) if entry.get("tool") == "get_metric_evidence"
    ]
    checks.append(
        {
            "check": "model received at most five supporting records",
            "passed": all(len(entry.get("result", {}).get("records", [])) <= 5 for entry in evidence_calls),
        }
    )
    return {
        "case_id": case["case_id"],
        "passed": all(check["passed"] for check in checks),
        "checks": checks,
        "response": result,
    }


def evaluate_case(
    base_url: str, case: Dict[str, Any], *, tier: str, subject: str
) -> Dict[str, Any]:
    messages = case.get("messages") or [
        {"role": "user", "content": case["question"]}
    ]
    payload: Dict[str, Any] = {"messages": messages}
    if case.get("active_scope"):
        payload["active_scope"] = case["active_scope"]
    result = post_json(
        f"{base_url.rstrip('/')}/api/ask",
        payload,
        tier=tier,
        subject=subject,
    )
    return score_result(case, result)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:10100")
    parser.add_argument("--case-id", action="append")
    parser.add_argument("--cases-file", type=Path, default=ROOT / "eval_cases.json")
    parser.add_argument("--tier", default="enterprise")
    parser.add_argument("--subject", default="launch-evaluation")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    cases = json.loads(args.cases_file.read_text())
    selected = [case for case in cases if not args.case_id or case["case_id"] in args.case_id]
    if not selected:
        raise SystemExit("no evaluation cases matched")
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "base_url": args.base_url,
        "cases": [
            evaluate_case(
                args.base_url,
                case,
                tier=args.tier,
                subject=f"{args.subject}:{case['case_id']}",
            )
            for case in selected
        ],
    }
    report["passed"] = all(case["passed"] for case in report["cases"])
    rendered = json.dumps(report, indent=2, default=str) + "\n"
    if args.output:
        args.output.write_text(rendered)
    print(rendered)
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
