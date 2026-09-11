"""Deterministic intent helpers for company and facility follow-ups."""

from __future__ import annotations

import re


def company_follow_up_intent(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(
        term in lowered
        for term in (
            "them",
            "their",
            "this company",
            "this site",
            "what do they",
            "how has",
            "how have",
            "changed most",
            "changed significantly",
            "change since",
            "changes since",
            "over the last",
            "which parts of",
            "relative importance",
            "largest customers",
            "prime contractors buy",
            "platforms",
            "platform",
            "programs",
            "capabilities",
            "awards",
            "contracts",
            "how important",
            "wider us defence footprint",
            "wider us defense footprint",
            "wider defence footprint",
            "wider defense footprint",
            "investigate next",
            "due diligence",
            "this facility",
            "the facility",
            "commercial importance",
            "most important conclusions",
            "key conclusions",
            "main conclusions",
        )
    )


def company_wide_intent(text: str) -> bool:
    lowered = str(text or "").lower()
    if lowered.strip(" .?") in {"all", "all of them", "both"}:
        return True
    if re.search(
        r"\bwhat\s+does\s+.+?\s+(?:actually\s+)?do\s+(?:in|for)\s+"
        r"(?:the\s+)?(?:us\s+)?(?:defense|defence|military)(?:\s+market)?\b",
        lowered,
    ):
        return True
    return any(
        phrase in lowered
        for phrase in (
            "parent-wide",
            "parent wide",
            "company-wide",
            "company wide",
            "corporation-wide",
            "corporation wide",
            "whole company",
            "entire company",
            "all sites",
            "all facilities",
            "us defence business",
            "us defense business",
            "into the us defence market",
            "into the us defense market",
            "largest defence customers",
            "largest defense customers",
            "largest visible defence positions",
            "largest visible defense positions",
            "which cage codes",
            "which facilities belong",
            "work is carried out at",
            "concise defence-market profile",
            "concise defense-market profile",
        )
    )
