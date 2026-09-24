"""Shared exposure policy for public NSN representations.

Both the request-time fallback and the daily precomputed projection import this
module.  Keeping the limits and supplier relationship vocabulary here prevents
the two public paths from silently drifting apart.
"""

from __future__ import annotations

from typing import Any, Mapping


PUBLIC_NSN_PART_NUMBER_LIMIT = 10
PUBLIC_NSN_SUPPLIER_SITE_LIMIT = 5
PUBLIC_NSN_CONNECTED_PLATFORM_LIMIT = 6
PUBLIC_NSN_RECENT_CONTRACT_LIMIT = 5
PUBLIC_NSN_ACTIVE_SOLICITATION_LIMIT = 3

# Increment whenever the public NSN payload contract changes.  This value is
# emitted by the API and included in release metadata/fingerprints so caches can
# distinguish the corrected dossier from an older daily-release payload.
PUBLIC_NSN_SCHEMA_VERSION = 3
PUBLIC_NSN_CACHE_EPOCH = "public-nsn-v3-20260925"


def _codes(value: Any) -> set[str]:
    return {
        code.strip().upper()
        for code in str(value or "").split(",")
        if code.strip()
    }


def public_supplier_relationship(supplier: Mapping[str, Any]) -> tuple[str, int]:
    """Return the governed public label and deterministic display priority."""

    rncc = _codes(supplier.get("rncc_codes"))
    rnvc = _codes(supplier.get("rnvc_codes"))
    rnsc = _codes(supplier.get("rnsc_codes"))
    relationship_source = str(supplier.get("source") or "").upper()
    if bool(supplier.get("is_active_authorized_source")):
        return "DLA-authorised source", 0
    if bool(supplier.get("is_procurement_authorized")):
        return "DLA-authorised source · inactive CAGE", 1
    if "3" in rncc and "2" in rnvc:
        return "Item-identifying manufacturer", 2
    if "1" in rncc:
        return "Source-control reference", 3
    if "7" in rncc:
        return "Vendor item-control reference", 4
    if "OBSERVED_DLA_SALE" in relationship_source:
        return "DLA award recipient", 5
    if "F" in rnsc:
        return "Qualified-source requirement", 6
    if "5" in rncc:
        return "Secondary part reference", 7
    if "9" in rnvc:
        return "Obsolete part reference", 8
    return "Part/CAGE reference", 7


def public_supplier_relationship_sql(
    *,
    active: str,
    procurement: str,
    rncc: str,
    rnvc: str,
    rnsc: str,
    source: str,
) -> tuple[str, str]:
    """Return SQL CASE expressions matching :func:`public_supplier_relationship`."""

    def has_code(expression: str, code: str) -> str:
        return (
            "LIST_CONTAINS(STR_SPLIT(REPLACE(UPPER(COALESCE(" + expression
            + ", '')), ' ', ''), ','), '" + code + "')"
        )

    rncc_3 = has_code(rncc, "3")
    rncc_1 = has_code(rncc, "1")
    rncc_7 = has_code(rncc, "7")
    rncc_5 = has_code(rncc, "5")
    rnvc_2 = has_code(rnvc, "2")
    rnvc_9 = has_code(rnvc, "9")
    rnsc_f = has_code(rnsc, "F")
    predicates = (
        (active, "DLA-authorised source", 0),
        (procurement, "DLA-authorised source · inactive CAGE", 1),
        (f"{rncc_3} AND {rnvc_2}", "Item-identifying manufacturer", 2),
        (rncc_1, "Source-control reference", 3),
        (rncc_7, "Vendor item-control reference", 4),
        (f"POSITION('OBSERVED_DLA_SALE' IN UPPER(COALESCE({source}, ''))) > 0", "DLA award recipient", 5),
        (rnsc_f, "Qualified-source requirement", 6),
        (rncc_5, "Secondary part reference", 7),
        (rnvc_9, "Obsolete part reference", 8),
    )
    label = "CASE " + " ".join(
        f"WHEN {predicate} THEN '{text}'" for predicate, text, _ in predicates
    ) + " ELSE 'Part/CAGE reference' END"
    rank = "CASE " + " ".join(
        f"WHEN {predicate} THEN {priority}" for predicate, _, priority in predicates
    ) + " ELSE 7 END"
    return label, rank
