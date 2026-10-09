"""Lossless, model-only representation of repeated platform supplier years."""

from __future__ import annotations

from typing import Any


ANNUAL_COLUMNS = (
    "fiscal_year",
    "mimir_modelled_reported_subcontract_value_usd",
    "source_reported_value_usd",
    "selected_report_count",
    "prime_award_count",
    "observation_status",
)
REQUIRED_ANNUAL_KEYS = frozenset(("cage", *ANNUAL_COLUMNS[:-1]))
ALLOWED_ANNUAL_KEYS = REQUIRED_ANNUAL_KEYS | {ANNUAL_COLUMNS[-1]}


def compact_platform_model_evidence(pack: dict[str, Any]) -> dict[str, Any]:
    """Keep all supplier rows and values while removing repeated annual keys.

    This is only for the model prompt. Callers retain the original dossier for
    answer artifacts, citations and customer evidence exports.
    """
    suppliers = pack.get("reported_supplier_sites")
    if not isinstance(suppliers, list) or not suppliers:
        return pack

    compact_suppliers = []
    converted = 0
    for supplier in suppliers:
        if not isinstance(supplier, dict):
            compact_suppliers.append(supplier)
            continue
        annual = supplier.get("annual_reported_subcontract_activity")
        if not isinstance(annual, list) or not annual:
            compact_suppliers.append(supplier)
            continue
        cage = supplier.get("cage")
        if not cage or any(
            not isinstance(row, dict)
            or not REQUIRED_ANNUAL_KEYS <= row.keys()
            or not row.keys() <= ALLOWED_ANNUAL_KEYS
            or row["cage"] != cage
            or ("observation_status" in row and row["observation_status"] is None)
            for row in annual
        ):
            # An unfamiliar source shape is passed through unchanged.
            compact_suppliers.append(supplier)
            continue
        projected = dict(supplier)
        projected["annual_reported_subcontract_activity"] = [
            [row.get(column) for column in ANNUAL_COLUMNS]
            for row in annual
        ]
        compact_suppliers.append(projected)
        converted += 1

    if not converted:
        return pack
    projected_pack = dict(pack)
    projected_pack["reported_supplier_sites"] = compact_suppliers
    projected_pack["supplier_annual_activity_table"] = {
        "columns": list(ANNUAL_COLUMNS),
        "cage": "Use the parent supplier row's cage for each annual observation.",
        "null_observation_status": "The source observation_status field was absent.",
        "rows_converted": converted,
        "rows_total": len(suppliers),
    }
    return projected_pack


def expand_platform_model_evidence(projected_pack: dict[str, Any]) -> dict[str, Any]:
    """Reconstruct source-shaped annual rows for a round-trip integrity check."""
    table = projected_pack.get("supplier_annual_activity_table")
    if not isinstance(table, dict):
        return projected_pack
    if table.get("columns") != list(ANNUAL_COLUMNS):
        raise ValueError("Unknown supplier annual-activity table schema")
    result = dict(projected_pack)
    result.pop("supplier_annual_activity_table")
    suppliers = []
    for supplier in projected_pack.get("reported_supplier_sites", []):
        if not isinstance(supplier, dict):
            suppliers.append(supplier)
            continue
        annual = supplier.get("annual_reported_subcontract_activity")
        if not isinstance(annual, list) or not annual or not isinstance(annual[0], list):
            suppliers.append(supplier)
            continue
        restored = dict(supplier)
        restored_annual = []
        for values in annual:
            if not isinstance(values, list) or len(values) != len(ANNUAL_COLUMNS):
                raise ValueError("Invalid supplier annual-activity row")
            row = dict(zip(ANNUAL_COLUMNS, values))
            if row["observation_status"] is None:
                row.pop("observation_status")
            row["cage"] = supplier["cage"]
            restored_annual.append(row)
        restored["annual_reported_subcontract_activity"] = restored_annual
        suppliers.append(restored)
    result["reported_supplier_sites"] = suppliers
    return result
