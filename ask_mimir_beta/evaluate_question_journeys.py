"""Evaluate multi-turn Ask Mimir journeys without calling OpenAI."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict, List

from evaluate_evidence_offline import (
    ROOT,
    _build_evidence,
    _path_value,
    _preview,
)
from lab_api import ActiveScope, AskRequest, ChatMessage, runtime, workflow_for_request


def _scope_from_evidence(
    evidence: Dict[str, Any], expected: Dict[str, Any]
) -> ActiveScope | None:
    expected_type = expected.get("scope_type")
    expected_id = str(expected.get("scope_id") or "").upper()
    scope = evidence.get("scope", {})
    if expected_type == "platform" and scope.get("platform_id"):
        return ActiveScope(
            scope_type="platform",
            scope_id=str(scope["platform_id"]),
            scope_name=scope.get("display_name"),
            group_kind="platform_or_program",
            platform_focus=(scope.get("requested_focus") or {}).get("focus_id"),
        )
    if expected_type == "state_market" and scope.get("state_code"):
        return ActiveScope(
            scope_type="state_market",
            scope_id=str(scope["state_code"]),
            scope_name=f"{scope.get('state_name')} defense industrial base",
            group_kind="state_market",
        )
    if expected_type == "capability_market" and scope.get("capability_id"):
        return ActiveScope(
            scope_type="capability_market",
            scope_id=str(scope["capability_id"]),
            scope_name=scope.get("display_name"),
            group_kind="capability_market",
        )
    if expected_type == "market_segment" and scope.get("segment_id"):
        return ActiveScope(
            scope_type="market_segment",
            scope_id=str(scope["segment_id"]),
            scope_name=scope.get("display_name"),
            group_kind="market_segment",
        )
    if expected_type == "record_search" and scope.get("scope_id"):
        return ActiveScope(
            scope_type="record_search",
            scope_id=str(scope["scope_id"]),
            scope_name=f"{scope.get('record_type')} search: {scope.get('subject')}",
            group_kind="record_search",
        )
    if expected_type == "item":
        resolved_niin = evidence.get("resolved_niin")
        matches = evidence.get("matches", [])
        match = matches[0] if len(matches) == 1 else None
        if resolved_niin and match:
            return ActiveScope(
                scope_type="item",
                scope_id=str(resolved_niin),
                scope_name=str(match.get("nsn") or resolved_niin),
                group_kind="item",
            )
    if expected_type in {"contract", "opportunity"}:
        match = evidence.get("resolved")
        if not match and len(evidence.get("matches", [])) == 1:
            match = evidence["matches"][0]
        if match and match.get("record_type") == expected_type:
            return ActiveScope(
                scope_type=expected_type,
                scope_id=str(match.get("record_id") or match.get("public_identifier")),
                scope_name=str(match.get("public_identifier") or match.get("record_id")),
                group_kind=expected_type,
            )
    if expected_type in {"company_site", "company_parent"}:
        name_fragment = str(expected.get("scope_name_contains") or "").upper()
        match = next(
            (
                row
                for row in evidence.get("matches", [])
                if row.get("scope_type") == expected_type
                and (not expected_id or str(row.get("scope_id") or "").upper() == expected_id)
                and (
                    not name_fragment
                    or name_fragment in str(row.get("scope_name") or "").upper()
                )
            ),
            None,
        )
        if match:
            return ActiveScope(
                scope_type=expected_type,
                scope_id=str(match["scope_id"]),
                scope_name=match.get("scope_name"),
                resolved_cages=match.get("resolved_cages", []),
                group_kind=str(
                    match.get("group_kind")
                    or ("observed_company_group" if expected_type == "company_parent" else "cage_site")
                ),
            )
    return None


def _run_journey(journey: Dict[str, Any]) -> Dict[str, Any]:
    messages: List[ChatMessage] = []
    active_scope: ActiveScope | None = None
    expected_scope = journey["expected_scope"]
    results = []
    for index, turn in enumerate(journey["turns"], start=1):
        turn_started = time.perf_counter()
        messages.append(ChatMessage(role="user", content=turn["question"]))
        request = AskRequest(messages=messages, active_scope=active_scope)
        workflow = workflow_for_request(request)
        evidence = _build_evidence(request, workflow)
        preview = _preview(workflow, evidence)
        checks = [
            {
                "check": f"workflow is {turn['expected_workflow']}",
                "passed": workflow == turn["expected_workflow"],
                "actual": workflow,
            }
        ]
        if active_scope is None:
            active_scope = _scope_from_evidence(evidence, expected_scope)
            checks.append(
                {
                    "check": f"scope resolves to {expected_scope['scope_type']}",
                    "passed": active_scope is not None,
                    "actual": active_scope.model_dump() if active_scope else None,
                }
            )
            if active_scope and expected_scope.get("scope_id"):
                checks.append(
                    {
                        "check": f"scope id is {expected_scope['scope_id']}",
                        "passed": active_scope.scope_id.upper()
                        == str(expected_scope["scope_id"]).upper(),
                        "actual": active_scope.scope_id,
                    }
                )
        for path, minimum in turn.get("minimum_values", {}).items():
            actual = _path_value(evidence, path)
            checks.append(
                {
                    "check": f"{path} >= {minimum}",
                    "passed": isinstance(actual, (int, float)) and actual >= minimum,
                    "actual": actual,
                }
            )
        for path, expected_value in turn.get("required_values", {}).items():
            actual = _path_value(evidence, path)
            checks.append(
                {
                    "check": f"{path} == {expected_value}",
                    "passed": actual == expected_value,
                    "actual": actual,
                }
            )
        for path, minimum in turn.get("minimum_list_lengths", {}).items():
            actual = _path_value(evidence, path)
            checks.append(
                {
                    "check": f"{path} contains at least {minimum} records",
                    "passed": isinstance(actual, list) and len(actual) >= minimum,
                    "actual": len(actual) if isinstance(actual, list) else None,
                }
            )
        results.append(
            {
                "turn": index,
                "question": turn["question"],
                "workflow": workflow,
                "passed": all(check["passed"] for check in checks),
                "checks": checks,
                "deterministic_answer_skeleton": preview,
                "elapsed_ms": round((time.perf_counter() - turn_started) * 1000, 1),
            }
        )
        messages.append(ChatMessage(role="assistant", content=preview or "Evidence assembled."))
    entry_variant_checks = []
    expected_entry_workflow = journey["turns"][0]["expected_workflow"]
    for variant in journey.get("entry_variants", []):
        variant_request = AskRequest(
            messages=[ChatMessage(role="user", content=variant)], active_scope=None
        )
        actual_workflow = workflow_for_request(variant_request)
        entry_variant_checks.append(
            {
                "question": variant,
                "expected_workflow": expected_entry_workflow,
                "actual_workflow": actual_workflow,
                "passed": actual_workflow == expected_entry_workflow,
            }
        )
    return {
        "journey_id": journey["journey_id"],
        "label": journey["label"],
        "passed": all(result["passed"] for result in results)
        and all(check["passed"] for check in entry_variant_checks),
        "active_scope": active_scope.model_dump() if active_scope else None,
        "turns": results,
        "entry_variant_checks": entry_variant_checks,
    }


def _markdown_summary(report: Dict[str, Any]) -> str:
    passed_count = sum(1 for journey in report["journeys"] if journey["passed"])
    lines = [
        "# Ask Mimir deterministic journey regression",
        "",
        f"- OpenAI calls: **{report['openai_calls']}**",
        f"- Journeys passed: **{passed_count}/{report['journey_count']}**",
        f"- Turns tested: **{report['turn_count']}**",
        f"- Alternative entry phrasings tested: **{report['entry_variant_count']}**",
        "",
        "| Journey | Result | Failed checks |",
        "| --- | --- | --- |",
    ]
    for journey in report["journeys"]:
        failures = []
        for turn in journey["turns"]:
            for check in turn["checks"]:
                if not check["passed"]:
                    failures.append(
                        f"T{turn['turn']}: {check['check']} (actual: {check.get('actual')})"
                    )
        for check in journey.get("entry_variant_checks", []):
            if not check["passed"]:
                failures.append(
                    "Variant: "
                    f"workflow is {check['expected_workflow']} "
                    f"(actual: {check['actual_workflow']})"
                )
        lines.append(
            f"| {journey['label']} | {'PASS' if journey['passed'] else 'FAIL'} | "
            f"{'<br>'.join(failures) if failures else '-'} |"
        )
    lines.extend(
        [
            "",
            "This suite validates routing, entity scope, follow-up retention and required evidence. "
            "It does not score final prose because no language-model call is made.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--journeys-file",
        type=Path,
        default=ROOT / "expanded_workflow_question_journeys.json",
    )
    parser.add_argument("--journey-id", action="append")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--summary-output", type=Path)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()
    journeys = json.loads(args.journeys_file.read_text())
    selected = [
        journey
        for journey in journeys
        if not args.journey_id or journey["journey_id"] in args.journey_id
    ]
    results = [_run_journey(journey) for journey in selected]
    report = {
        "mode": "deterministic_multi_turn_evidence_only",
        "openai_calls": 0,
        "passed": all(result["passed"] for result in results),
        "journey_count": len(results),
        "turn_count": sum(len(result["turns"]) for result in results),
        "entry_variant_count": sum(
            len(result.get("entry_variant_checks", [])) for result in results
        ),
        "journeys": results,
    }
    rendered = json.dumps(report, indent=2, default=str) + "\n"
    if args.output:
        args.output.write_text(rendered)
    if args.summary_output:
        args.summary_output.write_text(_markdown_summary(report))
    if args.quiet:
        passed_count = sum(1 for result in results if result["passed"])
        destination = str(args.summary_output or args.output or "stdout")
        print(
            f"Ask Mimir regression: {passed_count}/{len(results)} journeys passed; "
            f"{report['turn_count']} turns and {report['entry_variant_count']} alternative "
            f"entry phrasings; 0 OpenAI calls. Report: {destination}"
        )
    else:
        print(rendered)
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
