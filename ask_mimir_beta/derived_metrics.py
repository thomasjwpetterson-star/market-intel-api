"""Deterministic formulas used by the first Ask Mimir analytical layer."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from math import pow
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from metric_registry import CALCULATION_VERSION


def last_completed_us_fiscal_year(today: Optional[date] = None) -> int:
    current = today or date.today()
    return current.year if current.month >= 10 else current.year - 1


def percent_change(current: float, previous: float) -> Optional[float]:
    if previous <= 0:
        return None
    return ((current - previous) / previous) * 100.0


def exact_cagr(current: float, baseline: float, periods: int) -> Optional[float]:
    if current < 0 or baseline <= 0 or periods <= 0:
        return None
    return (pow(current / baseline, 1.0 / periods) - 1.0) * 100.0


def stable_id(prefix: str, payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return f"{prefix}_{hashlib.sha256(canonical.encode('utf-8')).hexdigest()[:24]}"


def calculate_concentration(
    components: Sequence[Mapping[str, Any]],
    value_field: str = "positive_value_usd",
    top_limit: int = 10,
) -> Dict[str, Any]:
    clean: List[Tuple[str, str, float]] = []
    for component in components:
        value = float(component.get(value_field) or 0)
        if value <= 0:
            continue
        component_id = str(component.get("component_id") or "UNKNOWN")
        component_name = str(component.get("component_name") or "Unknown")
        clean.append((component_id, component_name, value))

    clean.sort(key=lambda item: (-item[2], item[0]))
    total = sum(value for _, _, value in clean)
    if total <= 0:
        return {
            "status": "insufficient_data",
            "component_count": 0,
            "positive_value_usd": 0.0,
            "hhi": None,
            "top_1_share_pct": None,
            "top_3_share_pct": None,
            "effective_component_count": None,
            "top_components": [],
        }

    weighted = [(component_id, name, value, value / total) for component_id, name, value in clean]
    hhi = sum((share * 100.0) ** 2 for _, _, _, share in weighted)
    effective_count = 1.0 / sum(share**2 for _, _, _, share in weighted)
    return {
        "status": "available",
        "component_count": len(weighted),
        "positive_value_usd": round(total, 2),
        "hhi": round(hhi, 1),
        "top_1_share_pct": round(weighted[0][3] * 100.0, 2),
        "top_3_share_pct": round(sum(row[3] for row in weighted[:3]) * 100.0, 2),
        "effective_component_count": round(effective_count, 2),
        "top_components": [
            {
                "component_id": component_id,
                "component_name": name,
                "positive_value_usd": round(value, 2),
                "share_pct": round(share * 100.0, 2),
            }
            for component_id, name, value, share in weighted[: max(1, top_limit)]
        ],
    }


def calculate_series_metrics(
    rows: Sequence[Mapping[str, Any]],
    universe_rows: Sequence[Mapping[str, Any]],
    analysis_fy: int,
    as_of_date: Optional[date] = None,
) -> Dict[str, Any]:
    by_year = {int(row["fiscal_year"]): row for row in rows}
    universe_by_year = {int(row["fiscal_year"]): row for row in universe_rows}
    current = by_year.get(analysis_fy, {})
    prior = by_year.get(analysis_fy - 1, {})
    prior_two = by_year.get(analysis_fy - 2, {})
    baseline_three = by_year.get(analysis_fy - 3, {})
    current_universe = universe_by_year.get(analysis_fy, {})
    prior_universe = universe_by_year.get(analysis_fy - 1, {})

    net = float(current.get("net_value_usd") or 0)
    prior_net = float(prior.get("net_value_usd") or 0)
    prior_two_net = float(prior_two.get("net_value_usd") or 0)
    baseline_three_net = float(baseline_three.get("net_value_usd") or 0)
    positive = float(current.get("positive_value_usd") or 0)
    prior_positive = float(prior.get("positive_value_usd") or 0)

    yoy = percent_change(net, prior_net)
    prior_yoy = percent_change(prior_net, prior_two_net)
    current_universe_net = float(current_universe.get("net_value_usd") or 0)
    prior_universe_net = float(prior_universe.get("net_value_usd") or 0)
    current_universe_positive = float(current_universe.get("positive_value_usd") or 0)
    prior_universe_positive = float(prior_universe.get("positive_value_usd") or 0)

    share = (net / current_universe_net * 100.0) if current_universe_net > 0 else None
    prior_share = (prior_net / prior_universe_net * 100.0) if prior_universe_net > 0 else None
    positive_share = (
        positive / current_universe_positive * 100.0 if current_universe_positive > 0 else None
    )
    prior_positive_share = (
        prior_positive / prior_universe_positive * 100.0 if prior_universe_positive > 0 else None
    )
    latest_action_date = _parse_date(current.get("latest_action_date"))
    recency_days = (
        ((as_of_date or date.today()) - latest_action_date).days
        if latest_action_date is not None
        else None
    )
    absolute_value = float(current.get("absolute_value_usd") or 0)

    def coverage(field: str) -> Optional[float]:
        if absolute_value <= 0:
            return None
        return round(float(current.get(field) or 0) / absolute_value * 100.0, 2)

    return {
        "analysis_fy": analysis_fy,
        "comparison_fy": analysis_fy - 1,
        "net_value_usd": round(net, 2),
        "comparison_net_value_usd": round(prior_net, 2),
        "three_year_baseline_value_usd": round(baseline_three_net, 2),
        "positive_value_usd": round(positive, 2),
        "deobligation_value_usd": round(float(current.get("deobligation_value_usd") or 0), 2),
        "absolute_change_usd": round(net - prior_net, 2),
        "growth_yoy_pct": None if yoy is None else round(yoy, 2),
        "growth_acceleration_pp": (
            None if yoy is None or prior_yoy is None else round(yoy - prior_yoy, 2)
        ),
        "three_year_cagr_pct": (
            None
            if analysis_fy - 3 not in by_year
            else _round_optional(exact_cagr(net, baseline_three_net, 3))
        ),
        "distinct_awards": int(current.get("distinct_awards") or 0),
        "distinct_award_change": int(current.get("distinct_awards") or 0)
        - int(prior.get("distinct_awards") or 0),
        "award_activity_growth_pct": _round_optional(
            percent_change(
                float(current.get("distinct_awards") or 0),
                float(prior.get("distinct_awards") or 0),
            )
        ),
        "distinct_actions": int(current.get("distinct_actions") or 0),
        "distinct_action_change": int(current.get("distinct_actions") or 0)
        - int(prior.get("distinct_actions") or 0),
        "action_activity_growth_pct": _round_optional(
            percent_change(
                float(current.get("distinct_actions") or 0),
                float(prior.get("distinct_actions") or 0),
            )
        ),
        "latest_action_date": latest_action_date.isoformat() if latest_action_date else None,
        "activity_recency_days": recency_days,
        "observed_share_pct": _round_optional(share),
        "share_change_pp": (
            None if share is None or prior_share is None else round(share - prior_share, 2)
        ),
        "positive_value_share_pct": _round_optional(positive_share),
        "positive_value_share_change_pp": (
            None
            if positive_share is None or prior_positive_share is None
            else round(positive_share - prior_positive_share, 2)
        ),
        "platform_mapping_coverage": {
            "attributed_absolute_value_pct": coverage("attributed_absolute_value_usd"),
            "shared_use_absolute_value_pct": coverage("shared_use_absolute_value_usd"),
            "unmapped_absolute_value_pct": coverage("unmapped_absolute_value_usd"),
            "unmapped_record_count": int(current.get("unmapped_record_count") or 0),
        },
        "entity_resolution_coverage": {
            "unresolved_absolute_value_pct": coverage("unresolved_entity_absolute_value_usd"),
            "unresolved_record_count": int(current.get("unresolved_entity_record_count") or 0),
        },
    }


def make_observation_contract(
    *,
    release_id: str,
    scope_type: str,
    scope_id: str,
    scope_name: str,
    measure_type: str,
    analysis_fy: int,
    source_snapshot_sha256: str,
    metrics: Mapping[str, Any],
    evidence_filter: Mapping[str, Any],
    generated_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    identity = {
        "release_id": release_id,
        "calculation_version": CALCULATION_VERSION,
        "scope_type": scope_type,
        "scope_id": scope_id,
        "measure_type": measure_type,
        "analysis_fy": analysis_fy,
        "evidence_filter": evidence_filter,
    }
    return {
        "observation_id": stable_id("dmo", identity),
        "release_id": release_id,
        "calculation_version": CALCULATION_VERSION,
        "generated_at": (generated_at or datetime.utcnow()).isoformat(timespec="seconds") + "Z",
        "scope_type": scope_type,
        "scope_id": scope_id,
        "scope_name": scope_name,
        "measure_type": measure_type,
        "analysis_fy": analysis_fy,
        "source_snapshot_sha256": source_snapshot_sha256,
        "metrics": dict(metrics),
        "evidence_filter": dict(evidence_filter),
    }


def _round_optional(value: Optional[float]) -> Optional[float]:
    return None if value is None else round(value, 2)


def _parse_date(value: Any) -> Optional[date]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None
