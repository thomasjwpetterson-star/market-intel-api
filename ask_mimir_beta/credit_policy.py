"""Deterministic, pre-job Ask credit quotes for the two-mode pilot.

The quote is a work-class estimate, never a charge calculated from tokens after
the answer. Keep enforcement disabled until the classes have been calibrated
against real service cost and shown to customers before submission.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CreditQuote:
    work_class: str
    units: int


DEEP_BASE_UNITS = 3
COMPARISON_WORKFLOWS = frozenset({
    "platform_comparison", "platform_comparison_dossier",
    "market_segment_intelligence", "program_momentum",
    "defined_market_competitive_position", "general_defense_research",
})
FOCUSED_PLATFORM_SEGMENTS = frozenset({
    "prime", "awards", "parts", "supply_chain",
})


def quote_ask_work(
    *,
    answer_mode: str,
    workflow: str,
    conversation_segment: str | None = None,
    live_check: bool = False,
    menu_only: bool = False,
) -> CreditQuote:
    """Quote from the resolved work plan before reserving the job.

    The caller must derive ``conversation_segment`` and ``live_check`` from
    server-side routing; client-supplied values must not determine the quote.
    A clarification is handled separately by the existing refund path.
    """
    if answer_mode not in {"conversation", "deep_research"}:
        raise ValueError("Unsupported Ask answer mode")
    if menu_only:
        if answer_mode != "conversation":
            raise ValueError("A topic menu is only available in Conversation")
        return CreditQuote("conversation_menu", 0)
    if answer_mode == "conversation":
        if live_check:
            return CreditQuote("conversation_live", 3)
        if (workflow == "platform_intelligence"
                and conversation_segment in FOCUSED_PLATFORM_SEGMENTS):
            return CreditQuote("conversation_focused", 1)
        return CreditQuote("conversation_overview", 2)
    if live_check:
        return CreditQuote("deep_live", 5)
    if workflow in COMPARISON_WORKFLOWS:
        return CreditQuote("deep_extended", 5)
    return CreditQuote("deep_standard", DEEP_BASE_UNITS)
