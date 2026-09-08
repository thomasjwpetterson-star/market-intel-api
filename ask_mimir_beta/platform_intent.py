"""Deterministic intent modes for platform and program questions."""

from __future__ import annotations

import re


def is_platform_centered_request(text: str, *, has_platform_mention: bool) -> bool:
    """Keep named-platform questions out of the company-name resolver."""
    if not has_platform_mention:
        return False
    lowered = str(text or "").lower().strip()
    supplier_request = any(
        phrase in lowered
        for phrase in (
            "who supplies",
            "who builds",
            "who makes",
            "who manufactures",
            "supplier base",
            "supply chain",
            "suppliers for",
            "suppliers of",
            "suppliers to",
            "platform suppliers",
            "program suppliers",
            "programme suppliers",
        )
    ) or bool(
        re.search(
            r"^(?:show|list|find|identify|name|map|give)(?:\s+me)?\b.*\b"
            r"(?:suppliers?|vendors?|manufacturers?|subcontractors?|contractors?)\b",
            lowered,
        )
    ) or bool(
        re.search(
            r"^(?:who|which|what)\b.*\b"
            r"(?:suppliers?|vendors?|manufacturers?|subcontractors?|contractors?)\b",
            lowered,
        )
    ) or bool(
        re.search(
            r"^(?:which|what|show(?:\s+me)?|list|find)\s+"
            r"(?:companies|firms)\b.*\b(?:supply|supplies|supplying|support|"
            r"supports|supporting|build|builds|building|work|works|involved)\b",
            lowered,
        )
    ) or bool(
        re.search(
            r"^(?:which|what)\s+(?:facilities|sites|locations)\b.*\b"
            r"(?:support|supports|serve|serves|work|works)\b",
            lowered,
        )
    ) or bool(
        re.search(
            r"^what\s+does\s+.+?\s+provide\s+(?:on|for|to)\b|"
            r"^(?:what\s+is|describe)\s+.+?(?:'s|’s)?\s+role\s+(?:on|in)\b|"
            r"^how\s+is\s+.+?\s+involved\s+in\b",
            lowered,
        )
    ) or bool(
        re.fullmatch(
            r"(?:the\s+)?\S+(?:\s+\S+){0,3}\s+"
            r"(?:suppliers?|vendors?|manufacturers?|subcontractors?|contractors?)\??",
            lowered,
        )
    )
    direct_platform_request = bool(
        re.search(
            r"^(?:tell\s+me\s+about|give\s+me\s+an?\s+overview\s+of|"
            r"overview\s+of|explain|map)\b",
            lowered,
        )
    )
    compact_platform_request = len(lowered.split()) <= 7 and any(
        term in lowered
        for term in (
            "supplier",
            "vendor",
            "manufacturer",
            "subcontractor",
            "contractor",
            "supply chain",
            "industrial base",
            "production",
            "procurement",
            "outlook",
            "modernization",
            "modernisation",
            "progress",
            "trajectory",
        )
    )
    return supplier_request or direct_platform_request or compact_platform_request


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
    if has_supplier and (
        (
            "other" in lowered
            and any(
                term in lowered
                for term in ("program", "programme", "platform", "aviation")
            )
        )
        or any(
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
            "show me the evidence",
            "underlying contracts",
            "underlying records",
            "directly evidenced",
            "which parts are inferred",
            "which parts of that answer",
            "export the companies",
            "export the suppliers",
            "conclusions",
            "takeaways",
            "what matters most",
            "best positioned to benefit",
            "positioned to benefit",
            "increasing production mean",
            "production increase mean",
            "risks or uncertainties",
            "main risks",
            "in that outlook",
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


def platform_comparison_answer_mode(text: str) -> str:
    """Classify a question comparing two or more platform supplier bases."""
    lowered = str(text or "").lower()
    if any(
        phrase in lowered
        for phrase in (
            "commercially interesting",
            "commercial conclusion",
            "most important conclusion",
            "key conclusion",
            "main conclusion",
            "three conclusions",
            "key takeaways",
        )
    ):
        return "comparison_conclusions"
    if (
        "supplier" in lowered
        and "other" in lowered
        and any(
            term in lowered
            for term in ("program", "programme", "platform", "aviation")
        )
    ) or any(
        phrase in lowered
        for phrase in (
            "particularly exposed",
            "wider rotorcraft",
            "other army aviation",
            "other aviation programs",
            "other aviation programmes",
            "other programs",
            "other platforms",
        )
    ):
        return "comparison_cross_program_exposure"
    if any(
        phrase in lowered
        for phrase in (
            "share important suppliers",
            "shared suppliers",
            "supplier overlap",
            "overlap between",
            "where do the two",
        )
    ):
        return "comparison_overlap"
    return "supplier_base_comparison"


def platform_comparison_follow_up_intent(text: str) -> bool:
    """Recognize anaphoric follow-ups to an active multi-platform comparison."""
    lowered = str(text or "").lower()
    return any(
        phrase in lowered
        for phrase in (
            "the two platforms",
            "both platforms",
            "both programs",
            "both programmes",
            "between the two",
            "between them",
            "those suppliers",
            "these suppliers",
            "which of those",
            "the overlap",
            "three conclusions",
            "key conclusions",
            "most important conclusion",
            "commercially interesting",
        )
    )


def is_open_capability_discovery_request(text: str) -> bool:
    """Identify market-wide supplier searches that need a dedicated capability index."""
    lowered = str(text or "").lower()
    has_discovery = any(
        phrase in lowered
        for phrase in (
            "find manufacturers",
            "find us manufacturers",
            "find suppliers",
            "which manufacturers",
            "which suppliers",
            "which us manufacturers",
            "which us suppliers",
            "companies supplying",
            "manufacturers with",
            "suppliers with",
        )
    )
    has_capability = any(
        term in lowered
        for term in (
            "capability",
            "equipment",
            "electronics",
            "electrical",
            "power",
            "avionics",
            "propulsion",
            "antenna",
            "radar",
            "actuation",
            "machining",
            "composites",
            "energetics",
            "brake",
            "brakes",
            "braking",
        )
    )
    return has_discovery and has_capability
