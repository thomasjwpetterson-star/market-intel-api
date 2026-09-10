"""Deterministic intent helpers for company and facility follow-ups."""

from __future__ import annotations


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
