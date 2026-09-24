"""HTTP smoke test for a locally booted candidate API."""

from __future__ import annotations

import argparse
import json
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional


def request_json(
    base_url: str,
    path: str,
    payload: Optional[Dict[str, Any]] = None,
) -> tuple[Any, float]:
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(
        base_url.rstrip("/") + path,
        data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST" if payload is not None else "GET",
    )
    started = time.perf_counter()
    with urllib.request.urlopen(request, timeout=30) as response:
        body = json.loads(response.read())
        if response.status != 200:
            raise AssertionError(f"{path} returned HTTP {response.status}: {body}")
    return body, time.perf_counter() - started


def run(base_url: str, niin: str, absent_niin: str, release: str) -> dict[str, Any]:
    ready, ready_seconds = request_json(base_url, "/ready")
    if not ready.get("ready"):
        raise AssertionError(f"API is not ready: {ready}")

    profile, profile_seconds = request_json(
        base_url, f"/api/nsn/profile?nsn={niin}"
    )
    supply = profile.get("supply_state") or {}
    price = profile.get("price_intelligence") or {}
    opportunity = profile.get("opportunity_summary") or {}
    if supply.get("source_release") != release or price.get("source_release") != release:
        raise AssertionError("Profile is not serving the pinned operational release")

    explorer_payload = {
        "table": "v_nsn_cage_reference",
        "columns": [
            "niin", "cage_code", "part_number", "supplier_status",
            "supply_signal", "total_stock", "reorder_point_gap",
            "forecast_12m_qty", "latest_net_price",
            "trailing_12m_median_price", "operational_source_release",
            "active_solicitation_count", "next_response_deadline",
            "next_solicitation_number", "next_quantity",
        ],
        "filters": {"niin": niin},
    }
    explorer, explorer_seconds = request_json(
        base_url, "/api/explorer/preview", explorer_payload
    )
    if not explorer:
        raise AssertionError("Explorer returned no relationship rows")
    operational_values = {
        (
            row.get("supply_signal"), row.get("total_stock"),
            row.get("reorder_point_gap"), row.get("forecast_12m_qty"),
            row.get("latest_net_price"), row.get("trailing_12m_median_price"),
            row.get("operational_source_release"),
            row.get("active_solicitation_count"), row.get("next_response_deadline"),
            row.get("next_solicitation_number"), row.get("next_quantity"),
        )
        for row in explorer
    }
    if len(operational_values) != 1:
        raise AssertionError("NIIN metrics vary across Explorer relationship rows")
    expected = (
        supply.get("supply_signal"), supply.get("total_stock"),
        supply.get("reorder_point_gap"), supply.get("forecast_12m_qty"),
        price.get("latest_net_price"), price.get("trailing_12m_median_price"),
        release,
        opportunity.get("active_solicitation_count"),
        opportunity.get("next_response_deadline"),
        opportunity.get("next_solicitation_number"),
        opportunity.get("next_quantity"),
    )
    if next(iter(operational_values)) != expected:
        raise AssertionError("Explorer and profile operational metrics differ")

    count, count_seconds = request_json(
        base_url,
        "/api/explorer/count",
        {"table": "v_nsn_cage_reference", "columns": ["niin"], "filters": {"niin": niin}},
    )
    if int(count.get("count") or 0) < len(explorer):
        raise AssertionError("Explorer count is lower than its preview")

    public, public_seconds = request_json(
        base_url, f"/api/public/intelligence/nsn/{niin}"
    )
    teaser = public.get("demand_supply_teaser") or {}
    if teaser.get("source_release") != release:
        raise AssertionError("Public teaser is not serving the pinned release")
    if "total_stock" in teaser or "latest_net_price" in teaser:
        raise AssertionError("Public teaser leaked paid exact stock or price fields")
    public_opportunity = public.get("opportunity_summary") or {}
    if opportunity and (
        public_opportunity.get("next_solicitation_number")
        != opportunity.get("next_solicitation_number")
    ):
        raise AssertionError("Public opportunity summary differs from direct DLA sidecar")

    opportunity_rows, opportunity_seconds = request_json(
        base_url, f"/api/nsn/opportunities?nsn={niin}&limit=50"
    )
    if opportunity and not opportunity_rows:
        raise AssertionError("Opportunity detail endpoint returned no active rows")

    absent_profile, absent_profile_seconds = request_json(
        base_url, f"/api/nsn/profile?nsn={absent_niin}"
    )
    if absent_profile.get("supply_state") or absent_profile.get("price_intelligence"):
        raise AssertionError("Absent sidecar NIIN received operational metrics")
    absent_explorer, absent_explorer_seconds = request_json(
        base_url,
        "/api/explorer/preview",
        {
            "table": "v_nsn_cage_reference",
            "columns": ["niin", "cage_code", "part_number", "supply_signal", "latest_net_price"],
            "filters": {"niin": absent_niin},
        },
    )
    if not absent_explorer or any(
        row.get("supply_signal") is not None or row.get("latest_net_price") is not None
        for row in absent_explorer
    ):
        raise AssertionError("Absent sidecar fallback did not return null additive columns")

    return {
        "status": "PASS",
        "base_url": base_url,
        "source_release": release,
        "tested_niin": niin,
        "absent_sidecar_niin": absent_niin,
        "relationship_count": int(count["count"]),
        "preview_rows": len(explorer),
        "checks": {
            "readiness": True,
            "profile_supply_and_price": True,
            "direct_opportunity_summary_and_detail": True,
            "explorer_additive_columns": True,
            "explorer_relationship_count": True,
            "public_teaser_without_paid_exact_values": True,
            "absent_sidecar_backward_compatibility": True,
        },
        "latency_seconds": {
            "ready": round(ready_seconds, 4),
            "profile": round(profile_seconds, 4),
            "explorer_preview": round(explorer_seconds, 4),
            "explorer_count": round(count_seconds, 4),
            "public_snapshot": round(public_seconds, 4),
            "opportunity_detail": round(opportunity_seconds, 4),
            "absent_profile": round(absent_profile_seconds, 4),
            "absent_explorer": round(absent_explorer_seconds, 4),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:10100")
    parser.add_argument("--niin", default="001860967")
    parser.add_argument("--absent-niin", default="000119952")
    parser.add_argument("--release", default="2026-09-16-1038")
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    report = run(
        arguments.base_url,
        arguments.niin,
        arguments.absent_niin,
        arguments.release,
    )
    rendered = json.dumps(report, indent=2)
    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
