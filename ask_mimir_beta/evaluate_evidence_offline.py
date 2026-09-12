"""Run Ask Mimir route and evidence regressions without calling OpenAI."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parent
os.environ.setdefault("ASK_MIMIR_MOCK", "1")
os.environ.setdefault("ASK_MIMIR_CACHE_DIR", "/tmp/ask-mimir-offline-cache")
os.environ.setdefault("ASK_MIMIR_BETA_STATE", "/tmp/ask-mimir-offline-state.sqlite3")

runtime_root = ROOT / ".runtime-data"
if runtime_root.exists():
    os.environ.setdefault(
        "ASK_MIMIR_RELEASE_DIR",
        str(runtime_root / "artifacts" / "metric-release"),
    )
    os.environ.setdefault(
        "ASK_MIMIR_TRANSACTIONS",
        str(runtime_root / "data" / "transactions.parquet"),
    )
    os.environ.setdefault("ASK_MIMIR_DATA_ROOT", str(runtime_root / "data"))
    os.environ.setdefault(
        "ASK_MIMIR_COMPANY_CONTEXT_DIR",
        str(runtime_root / "artifacts" / "company-context"),
    )
    os.environ.setdefault(
        "ASK_MIMIR_COMPANY_OPPORTUNITY_DIR",
        str(runtime_root / "artifacts" / "company-opportunities"),
    )
    os.environ.setdefault(
        "ASK_MIMIR_PLATFORM_SUPPLY_CHAIN_DIR",
        str(runtime_root / "artifacts" / "platform-supply-chains"),
    )
    os.environ.setdefault(
        "ASK_MIMIR_PROGRAM_MOMENTUM_PACK",
        str(runtime_root / "artifacts" / "program-momentum" / "missile-program-momentum.json"),
    )

from capability_discovery import resolve_capability  # noqa: E402
from geographic_market import resolve_state  # noqa: E402
from market_segment import resolve_market_segment  # noqa: E402
from market_record_search import (  # noqa: E402
    record_search_from_scope_id,
    resolve_market_record_search,
)
from lab_api import (  # noqa: E402
    ActiveScope,
    AskRequest,
    ChatMessage,
    explicit_award_or_opportunity_query,
    explicit_item_query,
    explicit_platform_comparison,
    explicit_platform_query,
    explicit_company_name_query,
    runtime,
    workflow_for_request,
)
from platform_context import requested_platform_focus  # noqa: E402
from program_outlook_store import is_program_outlook_language  # noqa: E402


def _path_value(value: Any, path: str) -> Any:
    current = value
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _money(value: Any) -> str:
    amount = float(value or 0)
    if abs(amount) >= 1_000_000_000:
        return f"${amount / 1_000_000_000:.1f}B"
    if abs(amount) >= 1_000_000:
        return f"${amount / 1_000_000:.1f}M"
    if abs(amount) >= 1_000:
        return f"${amount / 1_000:.1f}K"
    return f"${amount:,.0f}"


def _build_evidence(request: AskRequest, workflow: str) -> Dict[str, Any]:
    latest = request.messages[-1].content
    if workflow == "market_record_search":
        spec = resolve_market_record_search(latest)
        if not spec and request.active_scope:
            spec = record_search_from_scope_id(request.active_scope.scope_id)
        return runtime.call_tool(
            "get_market_record_search", {**(spec or {}), "limit": 75}
        )
    if workflow == "market_segment_intelligence":
        segment_id = resolve_market_segment(latest)
        if not segment_id and request.active_scope:
            segment_id = request.active_scope.scope_id
        return runtime.call_tool(
            "get_market_segment", {"segment_id": segment_id, "limit": 40}
        )
    if workflow == "state_industrial_base":
        state_code = resolve_state(latest)
        if not state_code and request.active_scope:
            state_code = request.active_scope.scope_id
        return runtime.call_tool(
            "get_state_industrial_base", {"state_code": state_code, "limit": 40}
        )
    if workflow == "capability_discovery":
        capability_id = resolve_capability(latest)
        if not capability_id and request.active_scope:
            capability_id = request.active_scope.scope_id
        if not capability_id:
            return {"unsupported_capability": latest}
        return runtime.call_tool(
            "get_capability_market", {"capability_id": capability_id, "limit": 30}
        )
    if workflow == "platform_comparison":
        platform_ids = explicit_platform_comparison(request.messages, runtime.platform_contexts)
        if not platform_ids and request.active_scope:
            platform_ids = request.active_scope.compared_platform_ids
        return runtime.call_tool(
            "compare_platform_contexts", {"platform_ids": platform_ids}
        )
    if workflow == "platform_intelligence":
        platform_id = explicit_platform_query(request.messages, runtime.platform_contexts)
        focus_id = requested_platform_focus(latest)
        if not platform_id and request.active_scope:
            platform_id = request.active_scope.scope_id
            focus_id = focus_id or request.active_scope.platform_focus
        resolution = runtime.call_tool(
            "search_platform_contexts", {"query": platform_id or latest, "limit": 15}
        )
        resolved = resolution.get("resolved_platform_id")
        if not resolved:
            return {"resolution": resolution}
        arguments = {"platform_id": resolved, "supplier_limit": 180}
        if focus_id:
            arguments["focus_id"] = focus_id
        evidence = runtime.call_tool("get_platform_context", arguments)
        if (
            is_program_outlook_language(latest)
            and runtime.program_outlook.supports(resolved)
        ):
            evidence["structured_program_outlook"] = runtime.call_tool(
                "get_program_outlook", {"platform_id": resolved}
            )
        return evidence
    if workflow == "company_site_intelligence":
        if request.active_scope and request.active_scope.scope_type in {
            "company_parent",
            "company_site",
        }:
            return runtime.call_tool(
                "get_company_context",
                {
                    "scope_type": request.active_scope.scope_type,
                    "scope_id": request.active_scope.scope_id,
                    "focus": "full_dossier",
                },
            )
        company_query = explicit_company_name_query(request.messages) or latest
        return runtime.call_tool(
            "search_company_contexts",
            {"query": company_query, "scope_type": None, "limit": 20},
        )
    if workflow == "item_intelligence":
        query = explicit_item_query(request.messages)
        if not query and request.active_scope:
            query = request.active_scope.scope_id
        return runtime.call_tool("search_item_contexts", {"query": query, "limit": 20})
    if workflow == "contract_or_opportunity":
        query = explicit_award_or_opportunity_query(request.messages)
        if not query and request.active_scope:
            query = request.active_scope.scope_id
        return runtime.call_tool(
            "search_award_opportunity_contexts", {"query": query, "limit": 20}
        )
    if workflow == "program_momentum":
        return runtime.call_tool("get_program_momentum", {"market": "missiles", "limit": 12})
    return {"workflow_only": True}


def _preview(workflow: str, evidence: Dict[str, Any]) -> str:
    if workflow == "market_record_search" and evidence.get("scope"):
        scope = evidence["scope"]
        rows = evidence.get("records", [])[:5]
        labels = ", ".join(
            str(row.get("title") or row.get("record_id")) for row in rows
        )
        return (
            f"{scope.get('record_type')} search for {scope.get('subject')} | "
            f"{scope.get('observation_window')} | "
            f"{evidence.get('coverage', {}).get('matching_records', 0)} matching records. "
            f"Leading results: {labels}."
        )
    if workflow == "platform_intelligence" and evidence.get("scope"):
        scope = evidence["scope"]
        coverage = evidence.get("coverage", {})
        totals = evidence.get("financial_totals", {})
        status = "partial" if coverage.get("reported_supplier_lane_is_sparse") else "broad"
        suppliers = ", ".join(
            f"{row.get('supplier_name')} (CAGE {row.get('cage')})"
            for row in evidence.get("reported_supplier_sites", [])[:5]
        )
        return (
            f"{scope.get('display_name')} | {scope.get('observation_window')} | "
            f"reported first-tier coverage: {status}; "
            f"{coverage.get('reported_supplier_sites', 0)} reported supplier sites, "
            f"{coverage.get('observed_dla_recipient_sites', 0)} observed DLA recipient sites and "
            f"{coverage.get('associated_niins', 0):,} associated NIINs. "
            f"Net prime obligations: {_money(totals.get('net_prime_obligations_usd'))}. "
            f"Leading reported sites: {suppliers}."
        )
    if workflow == "capability_discovery" and evidence.get("scope"):
        rows = evidence.get("supplier_sites", [])[:6]
        sites = "; ".join(
            f"{row.get('supplier_name')} (CAGE {row.get('cage')}), "
            f"{row.get('city')}, {row.get('state')}"
            for row in rows
        )
        return (
            f"{evidence['scope']['display_name']} | {evidence['scope']['observation_window']}. "
            f"Leading evidence-supported sites: {sites}."
        )
    if workflow == "state_industrial_base" and evidence.get("scope"):
        rows = evidence.get("ranked_registered_facilities", [])[:6]
        sites = "; ".join(
            f"{row.get('vendor_name')} (CAGE {row.get('cage')}), {row.get('city')}, "
            f"prime {_money(row.get('net_prime_obligations_usd'))}"
            for row in rows
        )
        return (
            f"{evidence['scope']['state_name']} | {evidence['scope']['observation_window']}. "
            f"Leading facilities: {sites}."
        )
    if workflow == "market_segment_intelligence" and evidence.get("scope"):
        programs = ", ".join(
            str(row.get("platform_family"))
            for row in evidence.get("platform_activity", [])[:6]
        )
        suppliers = "; ".join(
            f"{row.get('supplier_name')} (CAGE {row.get('cage')})"
            for row in evidence.get("leading_reported_supplier_sites", [])[:5]
        )
        return (
            f"{evidence['scope']['display_name']} | "
            f"{evidence['scope']['observation_window']}. "
            f"Leading mapped programs: {programs}. Leading reported supplier sites: {suppliers}."
        )
    if workflow == "company_site_intelligence" and evidence.get("scope"):
        scope = evidence["scope"]
        identity = evidence.get("identity", {})
        platforms = evidence.get("platform_exposure", [])
        performance_records = evidence.get("place_of_performance_activity", {}).get(
            "records", []
        )
        performance_platforms = sorted(
            {
                str(row.get("platform_family"))
                for row in performance_records
                if row.get("platform_family")
            }
        )
        awards = evidence.get("top_awards", [])
        return (
            f"{scope.get('scope_name')} | {scope.get('observation_window')} | "
            f"{identity.get('site_count', 0)} resolved sites, "
            f"{len(evidence.get('observed_financials', []))} financial observations, "
            f"{len(platforms)} contracting-CAGE platform records, "
            f"{len(performance_records)} same-company place-of-performance records "
            f"({', '.join(performance_platforms) or 'no mapped platform'}), and "
            f"{len(awards)} leading contracting-CAGE awards."
        )
    if workflow == "platform_comparison":
        platforms = evidence.get("platform_ids", [])
        return (
            f"{' vs '.join(platforms)} | {evidence.get('observation_window')}; "
            f"{evidence.get('overlap_counts', {}).get('exact_cage_site_overlap', 0)} shared CAGE sites."
        )
    return json.dumps(evidence, default=str)[:1500]


def _evaluate_case(case: Dict[str, Any]) -> Dict[str, Any]:
    source_messages = case.get("messages")
    if source_messages is None:
        source_messages = [{"role": "user", "content": case["question"]}]
    messages = [
        ChatMessage(**message)
        for message in source_messages
    ]
    active_scope = ActiveScope(**case["active_scope"]) if case.get("active_scope") else None
    request = AskRequest(messages=messages, active_scope=active_scope)
    workflow = workflow_for_request(request)
    evidence = _build_evidence(request, workflow)
    preview = _preview(workflow, evidence)
    checks: List[Dict[str, Any]] = [
        {
            "check": f"workflow is {case['expected_workflow']}",
            "passed": workflow == case["expected_workflow"],
            "actual": workflow,
        }
    ]
    for path, minimum in case.get("minimum_values", {}).items():
        actual = _path_value(evidence, path)
        checks.append(
            {
                "check": f"{path} >= {minimum}",
                "passed": isinstance(actual, (int, float)) and actual >= minimum,
                "actual": actual,
            }
        )
    for path, expected in case.get("required_values", {}).items():
        actual = _path_value(evidence, path)
        checks.append(
            {
                "check": f"{path} == {expected}",
                "passed": actual == expected,
                "actual": actual,
            }
        )
    for path, minimum in case.get("minimum_list_lengths", {}).items():
        actual = _path_value(evidence, path)
        checks.append(
            {
                "check": f"{path} contains at least {minimum} records",
                "passed": isinstance(actual, list) and len(actual) >= minimum,
                "actual": len(actual) if isinstance(actual, list) else None,
            }
        )
    for path in case.get("required_non_empty_paths", []):
        actual = _path_value(evidence, path)
        checks.append(
            {
                "check": f"{path} is populated",
                "passed": actual not in (None, "", [], {}),
                "actual": actual,
            }
        )
    for term in case.get("required_preview_terms", []):
        checks.append(
            {
                "check": f"preview contains {term}",
                "passed": term.lower() in preview.lower(),
            }
        )
    for term in (
        "source-reported parent data",
        "company revenue",
        "non-additive",
        "the dossier",
        "release `",
    ):
        checks.append(
            {
                "check": f"preview excludes {term}",
                "passed": term.lower() not in preview.lower(),
            }
        )
    return {
        "case_id": case["case_id"],
        "question": messages[-1].content,
        "workflow": workflow,
        "passed": all(check["passed"] for check in checks),
        "checks": checks,
        "preview": preview,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cases-file", type=Path, default=ROOT / "offline_regression_cases.json"
    )
    parser.add_argument("--case-id", action="append")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    cases = json.loads(args.cases_file.read_text())
    selected = [case for case in cases if not args.case_id or case["case_id"] in args.case_id]
    results = [_evaluate_case(case) for case in selected]
    report = {
        "mode": "deterministic_evidence_only",
        "openai_calls": 0,
        "passed": all(result["passed"] for result in results),
        "cases": results,
    }
    rendered = json.dumps(report, indent=2, default=str) + "\n"
    if args.output:
        args.output.write_text(rendered)
    print(rendered)
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
