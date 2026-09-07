"""Deterministic intent modes for platform and program questions."""

from __future__ import annotations


def platform_answer_mode(text: str) -> str:
    """Classify the latest platform question without inheriting prior answer language."""
    lowered = str(text or "").lower()
    has_supplier = any(
        term in lowered
        for term in ("supplier", "supply chain", "subcontractor", "sub-contractor")
    )
    if any(
        phrase in lowered
        for phrase in (
            "most important conclusions",
            "key conclusions",
            "main conclusions",
            "three conclusions",
            "top conclusions",
            "main takeaways",
            "key takeaways",
            "what matters most",
        )
    ):
        return "platform_conclusions"
    if has_supplier and any(
        phrase in lowered
        for phrase in (
            "other programs",
            "other platforms",
            "cross-program",
            "cross program",
            "shared across",
            "dependency",
            "dependencies",
        )
    ):
        return "supplier_cross_program"
    if has_supplier and any(
        phrase in lowered
        for phrase in (
            "fewest alternative sources",
            "fewest alternatives",
            "sole source",
            "single source",
            "source depth",
            "alternative source",
        )
    ):
        return "supplier_source_depth"
    if has_supplier and any(
        term in lowered
        for term in (
            "rank",
            "largest",
            "top supplier",
            "strongest position",
            "by value",
            "mapped activity",
            "visible activity",
            "significant position",
            "most significant",
        )
    ):
        return "supplier_value_ranking"
    if any(
        phrase in lowered
        for phrase in (
            "what does each",
            "what do they provide",
            "what each supplier provides",
            "what suppliers provide",
            "what do the suppliers provide",
        )
    ):
        return "supplier_roles"
    if has_supplier and any(
        term in lowered
        for term in (
            "concentrat",
            "source depth",
            "dependent on a small number",
            "depend on a small number",
            "few suppliers",
            "few supplier sites",
        )
    ):
        return "supplier_concentration"
    if has_supplier and any(term in lowered for term in ("which facilities", "important facilities")):
        return "supplier_facilities"
    if has_supplier or "who supplies" in lowered:
        return "supplier_overview"
    return "platform_overview"


def platform_follow_up_intent(text: str) -> bool:
    """Return whether a short follow-up should retain the active platform scope."""
    lowered = str(text or "").lower()
    return platform_answer_mode(lowered) in {
        "platform_conclusions",
        "supplier_overview",
        "supplier_roles",
        "supplier_value_ranking",
        "supplier_concentration",
        "supplier_facilities",
        "supplier_cross_program",
        "supplier_source_depth",
    } or any(
        phrase in lowered
        for phrase in (
            "supporting evidence",
            "underlying contracts",
            "underlying records",
            "export the companies",
            "export the suppliers",
            "conclusions",
            "takeaways",
            "what matters most",
        )
    )


def platform_follow_up_retains_scope(text: str) -> bool:
    """Keep an anaphoric comparison anchored to the active platform."""
    lowered = str(text or "").lower()
    if not platform_follow_up_intent(lowered):
        return False
    if platform_answer_mode(lowered) in {
        "platform_conclusions",
        "supplier_cross_program",
    }:
        return True
    return any(
        phrase in lowered
        for phrase in (
            "those suppliers",
            "these suppliers",
            "of those",
            "which of them",
            "the suppliers",
            "the supplier base",
            "this platform",
            "that platform",
            "the platform",
            "this program",
            "that program",
            "the program",
            "the supply chain",
        )
    )
