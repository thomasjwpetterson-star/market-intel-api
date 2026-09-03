"""Conservative award-description to platform candidate matching.

Description-only matches are deliberately candidates, not financial attribution.
Promotion requires either a structured program field or independent item evidence.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Sequence


def _normalized_text(value: Any) -> str:
    return " ".join(re.findall(r"[A-Z0-9]+", str(value or "").upper()))


def _contains_phrase(text: str, phrase: str) -> bool:
    normalized_phrase = _normalized_text(phrase)
    if not normalized_phrase:
        return False
    return f" {normalized_phrase} " in f" {text} "


@dataclass(frozen=True)
class PlatformAliasRule:
    platform_id: str
    display_name: str
    aliases: Sequence[str]
    context_terms: Sequence[str] = ()
    ambiguous_aliases: Sequence[str] = ()


DEFAULT_RULES = (
    PlatformAliasRule("PAC3_MSE", "PAC-3 MSE", ("PAC-3 MSE", "PAC3 MSE", "PATRIOT PAC-3")),
    PlatformAliasRule("PRSM", "Precision Strike Missile (PrSM)", ("PRECISION STRIKE MISSILE", "PRSM")),
    PlatformAliasRule(
        "JASSM_LRASM",
        "JASSM / LRASM family",
        ("JASSM", "LRASM", "JOINT AIR SURFACE STANDOFF MISSILE", "LONG RANGE ANTI SHIP MISSILE"),
    ),
    PlatformAliasRule("GMLRS", "GMLRS", ("GMLRS", "GUIDED MULTIPLE LAUNCH ROCKET"), ("ROCKET",)),
    PlatformAliasRule("AMRAAM", "AMRAAM", ("AMRAAM", "ADVANCED MEDIUM RANGE AIR TO AIR MISSILE")),
    PlatformAliasRule("AIM9X", "AIM-9X", ("AIM-9X", "AIM 9X", "SIDEWINDER")),
    PlatformAliasRule("SM6", "Standard Missile 6", ("SM-6", "SM 6", "STANDARD MISSILE 6")),
    PlatformAliasRule("STINGER", "Stinger", ("STINGER MISSILE",), ("STINGER",)),
    PlatformAliasRule("TOMAHAWK", "Tomahawk", ("TOMAHAWK", "TACTOM")),
    PlatformAliasRule("THAAD", "THAAD", ("THAAD", "TERMINAL HIGH ALTITUDE AREA DEFENSE")),
    PlatformAliasRule("TRIDENT_II", "Trident II", ("TRIDENT II", "TRIDENT D5")),
    PlatformAliasRule("CH53K", "CH-53K", ("CH-53K", "CH 53K", "KING STALLION")),
    PlatformAliasRule("F35", "F-35", ("F-35", "F 35", "JOINT STRIKE FIGHTER")),
)


def map_platform_candidates(
    description: str | None,
    *,
    structured_program_fields: Iterable[str] = (),
    item_platforms: Iterable[str] = (),
    rules: Sequence[PlatformAliasRule] = DEFAULT_RULES,
) -> List[Dict[str, Any]]:
    """Return evidence-ranked candidates without forcing ambiguous attribution."""

    description_text = _normalized_text(description)
    structured_texts = [_normalized_text(value) for value in structured_program_fields if value]
    normalized_item_platforms = {_normalized_text(value) for value in item_platforms if value}
    candidates: List[Dict[str, Any]] = []

    for rule in rules:
        matched_aliases = [alias for alias in rule.aliases if _contains_phrase(description_text, alias)]
        structured_aliases = [
            alias
            for alias in rule.aliases
            if any(_contains_phrase(value, alias) for value in structured_texts)
        ]
        item_match = any(
            _normalized_text(alias) == item_platform
            or _normalized_text(rule.display_name) == item_platform
            for alias in rule.aliases
            for item_platform in normalized_item_platforms
        )
        if not matched_aliases and not structured_aliases:
            continue

        ambiguous_only = bool(matched_aliases) and all(
            _normalized_text(alias) in {_normalized_text(value) for value in rule.ambiguous_aliases}
            for alias in matched_aliases
        )
        context_present = not rule.context_terms or any(
            _contains_phrase(description_text, term) for term in rule.context_terms
        )

        if structured_aliases:
            status = "ATTRIBUTABLE_STRUCTURED_IDENTIFIER"
            may_attribute_financials = True
            rationale = "An explicit platform alias appears in a structured program field."
        elif item_match and not ambiguous_only:
            status = "ATTRIBUTABLE_CORROBORATED"
            may_attribute_financials = True
            rationale = "The description match is corroborated by a deterministic item-platform association."
        elif ambiguous_only or not context_present:
            status = "REJECTED_AMBIGUOUS"
            may_attribute_financials = False
            rationale = "The matching term is too broad to identify a platform safely."
        else:
            status = "DESCRIPTION_CANDIDATE_ONLY"
            may_attribute_financials = False
            rationale = "The description supports review, but not automatic financial attribution."

        candidates.append(
            {
                "platform_id": rule.platform_id,
                "platform_name": rule.display_name,
                "status": status,
                "may_attribute_financials": may_attribute_financials,
                "matched_description_aliases": sorted(set(matched_aliases)),
                "matched_structured_aliases": sorted(set(structured_aliases)),
                "item_platform_corroboration": item_match,
                "rationale": rationale,
            }
        )

    return candidates
