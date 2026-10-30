"""Read-only cost and latency summary for Ask's proposed work-class credits.

The ledger already records a quote and estimated job cost, while customer
allowances still count queries. This report exposes only aggregates; it never
prints questions, account IDs, prompts or tool evidence.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
from collections import defaultdict
from contextlib import closing
from pathlib import Path
from typing import Any


def _percentile(values: list[float], proportion: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[max(math.ceil(proportion * len(ordered)) - 1, 0)], 4)


def _summary(values: list[float]) -> dict[str, float | None]:
    return {
        "p50": round(statistics.median(values), 4) if values else None,
        "p90": _percentile(values, 0.9),
        "max": round(max(values), 4) if values else None,
    }


def summarize_rows(
    rows: list[tuple[Any, ...]], *, min_samples: int = 20,
) -> dict[str, Any]:
    """Summarize completed ledger rows without returning customer-level data."""
    groups: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {"costs": [], "unit_costs": [], "latencies": [], "units": set(), "web_calls": []}
    )
    missing_cost = 0
    for mode, work_class, units, cost, latency_ms, performance_json in rows:
        if isinstance(cost, bool) or not isinstance(cost, (int, float)) or not math.isfinite(cost) or cost < 0:
            missing_cost += 1
            continue
        group = groups[(str(mode), str(work_class))]
        group["costs"].append(float(cost))
        if isinstance(units, int) and not isinstance(units, bool) and units >= 0:
            group["units"].add(units)
            if units:
                group["unit_costs"].append(float(cost) / units)
        if isinstance(latency_ms, (int, float)) and not isinstance(latency_ms, bool):
            if math.isfinite(latency_ms) and latency_ms >= 0:
                group["latencies"].append(float(latency_ms) / 1000)
        try:
            performance = json.loads(performance_json) if performance_json else {}
        except (TypeError, ValueError):
            performance = {}
        web_calls = performance.get("model_web_search_call_count") if isinstance(performance, dict) else None
        if isinstance(web_calls, int) and not isinstance(web_calls, bool) and web_calls >= 0:
            group["web_calls"].append(web_calls)

    classes = []
    for (mode, work_class), group in sorted(groups.items()):
        costs = group["costs"]
        classes.append({
            "answer_mode": mode,
            "work_class": work_class,
            "jobs": len(costs),
            "proposed_units_observed": sorted(group["units"]),
            "estimated_total_usd": round(sum(costs), 4),
            "estimated_cost_usd": _summary(costs),
            "estimated_cost_per_proposed_unit_usd": _summary(group["unit_costs"]),
            "latency_seconds": _summary(group["latencies"]),
            "web_search_calls_p90": _percentile(group["web_calls"], 0.9),
            "enough_samples_for_review": len(costs) >= min_samples,
        })
    return {
        "completed_rows": len(rows),
        "rows_without_valid_cost": missing_cost,
        "minimum_samples_per_class": min_samples,
        "classes": classes,
    }


def report_from_ledger(
    db_path: Path, *, since: str | None = None, min_samples: int = 20,
) -> dict[str, Any]:
    """Open the beta SQLite ledger read-only and use completed, billable jobs."""
    if min_samples < 1:
        raise ValueError("min_samples must be positive")
    uri = db_path.resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        query = """
            SELECT answer_mode, proposed_work_class, proposed_credit_units,
                   estimated_cost_usd, latency_ms, performance_json
            FROM query_events WHERE status = 'completed'
        """
        parameters: list[str] = []
        if since:
            query += " AND completed_at >= ?"
            parameters.append(since)
        rows = connection.execute(query, parameters).fetchall()
    return {"since": since, **summarize_rows(rows, min_samples=min_samples)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("db", type=Path, help="Path to a copy of the Ask beta state SQLite database")
    parser.add_argument("--since", help="Earliest completed_at UTC ISO timestamp")
    parser.add_argument("--min-samples", type=int, default=20)
    args = parser.parse_args()
    print(json.dumps(report_from_ledger(args.db, since=args.since, min_samples=args.min_samples), indent=2))


if __name__ == "__main__":
    main()
