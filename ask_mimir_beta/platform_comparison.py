"""Evidence assembly for comparisons between mapped platforms or programs."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List

from platform_context import PlatformContextStore


def _name_key(value: Any) -> str:
    text = " ".join(re.findall(r"[A-Z0-9]+", str(value or "").upper()))
    suffixes = (" CORPORATION", " CORP", " INCORPORATED", " INC", " LLC", " LTD")
    for suffix in suffixes:
        if text.endswith(suffix):
            text = text[: -len(suffix)].strip()
    return text


def _supplier_map(rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {
        str(row.get("cage") or "").strip().upper(): row
        for row in rows
        if str(row.get("cage") or "").strip()
    }


def _supplier_projection(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "cage": row.get("cage"),
        "supplier_name": row.get("supplier_name"),
        "city": row.get("city"),
        "state": row.get("state"),
        "country": row.get("country"),
        "mimir_modelled_reported_subcontract_value_usd": row.get(
            "mimir_modelled_reported_subcontract_value_usd"
        ),
        "share_of_reported_subcontract_value_pct": row.get(
            "share_of_reported_subcontract_value_pct"
        ),
        "reported_descriptions": list(row.get("reported_descriptions") or [])[:5],
        "reported_prime_names": list(row.get("reported_prime_names") or [])[:4],
        "reported_prime_cages": list(row.get("reported_prime_cages") or [])[:4],
        "sample_prime_contract_ids": list(row.get("sample_prime_contract_ids") or [])[:5],
        "mapped_platforms": list(row.get("mapped_platforms") or [])[:20],
        "first_reported_date": row.get("first_reported_date"),
        "latest_reported_date": row.get("latest_reported_date"),
    }


def _platform_projection(context: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "scope": context.get("scope"),
        "reported_supplier_summary": context.get("reported_supplier_summary"),
        "reported_supplier_sites": [
            _supplier_projection(row)
            for row in list(context.get("reported_supplier_sites") or [])[:60]
        ],
        "reported_component_categories": list(
            context.get("reported_component_categories") or []
        )[:50],
    }


def build_platform_comparison(
    store: PlatformContextStore,
    platform_ids: List[str],
) -> Dict[str, Any]:
    """Build a balanced comparison without treating prime share as supplier concentration."""
    resolved = list(dict.fromkeys(platform_ids))
    if len(resolved) < 2:
        raise ValueError("at least two distinct platforms are required")
    contexts = [
        store.comparison_projection(platform_id) for platform_id in resolved[:4]
    ]
    supplier_maps = [
        _supplier_map(context.get("reported_supplier_sites") or [])
        for context in contexts
    ]
    shared_cages = sorted(set.intersection(*(set(values) for values in supplier_maps)))
    exact_site_overlap = []
    for cage in shared_cages:
        per_platform = {
            context["scope"]["platform_id"]: _supplier_projection(supplier_maps[index][cage])
            for index, context in enumerate(contexts)
        }
        combined_value = sum(
            abs(
                float(
                    row.get("mimir_modelled_reported_subcontract_value_usd") or 0
                )
            )
            for row in per_platform.values()
        )
        exact_site_overlap.append(
            {
                "cage": cage,
                "combined_observed_value_usd": combined_value,
                "platform_evidence": per_platform,
            }
        )
    exact_site_overlap.sort(
        key=lambda row: row["combined_observed_value_usd"], reverse=True
    )

    organization_members: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for context in contexts:
        platform_id = context["scope"]["platform_id"]
        for supplier in context.get("reported_supplier_sites") or []:
            key = _name_key(supplier.get("supplier_name"))
            if not key:
                continue
            organization_members.setdefault(key, {}).setdefault(platform_id, []).append(
                _supplier_projection(supplier)
            )
    organization_overlap = []
    for key, platform_sites in organization_members.items():
        if len(platform_sites) != len(contexts):
            continue
        platform_cages = [
            {str(row.get("cage") or "").upper() for row in rows}
            for rows in platform_sites.values()
        ]
        if set.intersection(*platform_cages):
            continue
        combined_value = sum(
            abs(
                float(
                    row.get("mimir_modelled_reported_subcontract_value_usd") or 0
                )
            )
            for rows in platform_sites.values()
            for row in rows
        )
        organization_overlap.append(
            {
                "reported_organization_name": key,
                "combined_observed_value_usd": combined_value,
                "platform_sites": platform_sites,
            }
        )
    organization_overlap.sort(
        key=lambda row: row["combined_observed_value_usd"], reverse=True
    )

    comparison_ids = {
        context["scope"]["platform_id"].upper() for context in contexts
    }
    cross_program_sites = []
    all_cages = sorted(set().union(*(set(values) for values in supplier_maps)))
    for cage in all_cages:
        rows = {
            context["scope"]["platform_id"]: _supplier_projection(
                supplier_maps[index][cage]
            )
            for index, context in enumerate(contexts)
            if cage in supplier_maps[index]
        }
        other_platforms = sorted(
            {
                str(platform)
                for row in rows.values()
                for platform in row.get("mapped_platforms") or []
                if str(platform).upper() not in comparison_ids
            }
        )
        if not other_platforms:
            continue
        combined_value = sum(
            abs(
                float(
                    row.get("mimir_modelled_reported_subcontract_value_usd") or 0
                )
            )
            for row in rows.values()
        )
        cross_program_sites.append(
            {
                "cage": cage,
                "combined_observed_value_usd": combined_value,
                "compared_platform_evidence": rows,
                "other_mapped_platforms": other_platforms[:20],
            }
        )
    cross_program_sites.sort(
        key=lambda row: row["combined_observed_value_usd"], reverse=True
    )

    return {
        "context_type": "platform_supplier_base_comparison",
        "platform_ids": [context["scope"]["platform_id"] for context in contexts],
        "observation_window": "FY2021-FY2026 observed records",
        "platforms": [_platform_projection(context) for context in contexts],
        "overlap_counts": {
            "exact_cage_site_overlap": len(exact_site_overlap),
            "reported_organization_overlap_at_different_sites": len(
                organization_overlap
            ),
        },
        "material_exact_cage_site_overlap": exact_site_overlap[:50],
        "material_reported_organization_overlap_at_different_sites": (
            organization_overlap[:30]
        ),
        "material_cross_program_supplier_sites": cross_program_sites[:70],
        "comparison_method": {
            "primary_unit": "reported supplier CAGE site",
            "prime_recipient_rule": (
                "Prime-recipient concentration is not used as supplier-base concentration."
            ),
            "overlap_rule": (
                "Exact CAGE overlap is strongest. Same-name organizations at different CAGE sites "
                "are retained as separate site records."
            ),
            "role_rule": (
                "Specific roles require a reported description or aligned authoritative source."
            ),
            "insufficient_evidence_rule": (
                "Do not rank breadth, concentration or technical emphasis when coverage is not comparable."
            ),
        },
    }


def comparison_answer_projection(
    pack: Dict[str, Any], answer_mode: str
) -> Dict[str, Any]:
    """Remove evidence lanes that are not needed for the requested comparison mode."""
    projected = {
        **pack,
        "platforms": [dict(platform) for platform in pack.get("platforms", [])],
    }
    if answer_mode == "comparison_cross_program_exposure":
        for platform in projected["platforms"]:
            platform["reported_supplier_sites"] = []
            platform["reported_component_categories"] = []
        projected["material_exact_cage_site_overlap"] = []
        projected["material_reported_organization_overlap_at_different_sites"] = []
        projected["material_cross_program_supplier_sites"] = list(
            pack.get("material_cross_program_supplier_sites") or []
        )[:70]
    elif answer_mode == "comparison_overlap":
        for platform in projected["platforms"]:
            platform["reported_supplier_sites"] = []
            platform["reported_component_categories"] = []
        projected["material_exact_cage_site_overlap"] = list(
            pack.get("material_exact_cage_site_overlap") or []
        )[:50]
        projected["material_reported_organization_overlap_at_different_sites"] = list(
            pack.get("material_reported_organization_overlap_at_different_sites") or []
        )[:30]
        projected["material_cross_program_supplier_sites"] = []
    else:
        supplier_limit = 50 if answer_mode == "supplier_base_comparison" else 40
        category_limit = 30
        for platform in projected["platforms"]:
            platform["reported_supplier_sites"] = list(
                platform.get("reported_supplier_sites") or []
            )[:supplier_limit]
            platform["reported_component_categories"] = list(
                platform.get("reported_component_categories") or []
            )[:category_limit]
        projected["material_exact_cage_site_overlap"] = list(
            pack.get("material_exact_cage_site_overlap") or []
        )[:30]
        projected["material_reported_organization_overlap_at_different_sites"] = list(
            pack.get("material_reported_organization_overlap_at_different_sites") or []
        )[:20]
        projected["material_cross_program_supplier_sites"] = (
            list(pack.get("material_cross_program_supplier_sites") or [])[:30]
            if answer_mode == "comparison_conclusions"
            else []
        )
    projected["requested_answer_mode"] = answer_mode
    return projected
