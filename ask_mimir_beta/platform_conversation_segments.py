"""Small, source-bound platform evidence views for conversational questions."""

from __future__ import annotations

import re
from typing import Any


TOPICS = (
    ("prime", "Prime contractor", "Who is the prime contractor for {name}?"),
    ("awards", "Awards and spending", "Summarize historical awards and obligations for {name}."),
    ("parts", "NSNs and parts", "What related NSNs and parts are evidenced for {name}?"),
    ("supply_chain", "Supply chain", "Describe the evidenced supply chain for {name}."),
    ("all", "All of the above · Deep research", "Research the prime, historical awards and obligations, related NSNs and parts, and supply chain for {name}."),
)


def conversation_platform_topic(question: str, *, has_active_scope: bool = False) -> str:
    """Choose a topic from the latest question; never infer it from old prose."""
    text = re.sub(r"\s+", " ", str(question or "").lower()).strip()
    if re.search(r"\b(?:show|give|list)\s+(?:me\s+)?(?:the\s+)?(?:topic\s+)?(?:options|choices)\b", text):
        return "menu"
    if re.search(r"\b(?:all of the above|everything|comprehensive|complete picture)\b", text):
        return "overview"
    if re.search(r"\b(?:all|every|full list|exhaustive)\s+(?:of\s+the\s+)?(?:suppliers?|subcontractors?|nsns?|parts|awards|contracts)\b", text):
        return "overview"
    matches = [
        name for name, pattern in (
            ("supply_chain", r"\b(?:supply chain|suppliers?|subcontractors?|sub[- ]?awards?|manufacturers?)\b"),
            ("awards", r"\b(?:awards?|contracts?|obligations?|spending|spent|procurement history)\b"),
            ("parts", r"\b(?:nsns?|niins?|parts|components?|bill of materials|bom)\b"),
            ("prime", r"\b(?:prime contractor|prime recipient|system integrator|who builds|who makes)\b"),
        ) if re.search(pattern, text)
    ]
    if len(matches) == 1:
        return matches[0]
    if not matches and re.search(r"\bfy\s*20\d{2}\b", text) and re.search(
        r"\b(?:figure|demand|activity|higher|lower|fell|declin(?:e|ed)|increas(?:e|ed))\b",
        text,
    ):
        return "awards"
    return "overview"


def platform_topic_options(platform_id: str) -> list[dict[str, str]]:
    """UI prompts are fixed templates with a resolved, canonical platform ID."""
    name = str(platform_id)
    return [
        {"topic": topic, "label": label, "prompt": prompt.format(name=name),
         "answer_mode": "deep_research" if topic == "all" else "conversation"}
        for topic, label, prompt in TOPICS
    ]


def platform_segment_projection(pack: dict[str, Any], topic: str) -> dict[str, Any]:
    """Keep a bounded selected view and mark every unprovided lane as unexamined."""
    if topic not in {"prime", "awards", "parts", "supply_chain", "overview"}:
        raise ValueError(f"unsupported platform segment: {topic}")
    shared = {
        key: pack[key]
        for key in ("context_type", "scope", "coverage", "methodology", "evidence_index")
        if key in pack
    }
    included = {
        "prime": ("direct_award_recipients", "top_prime_awards", "financial_totals"),
        "awards": ("annual_activity", "financial_totals", "top_prime_awards", "direct_award_recipients"),
        "parts": ("item_and_component_evidence", "reported_component_categories"),
        "supply_chain": ("direct_award_recipients", "reported_supplier_sites",
                         "reported_supplier_concentration", "reported_component_categories",
                         "item_and_component_evidence", "financial_totals"),
        "overview": ("annual_activity", "financial_totals", "direct_award_recipients",
                     "reported_supplier_sites", "top_prime_awards",
                     "item_and_component_evidence", "reported_component_categories"),
    }[topic]
    result = {**shared, "requested_segment": topic}
    for key in included:
        if key in pack:
            result[key] = pack[key]
    caps = {
        "direct_award_recipients": 5,
        "top_prime_awards": 8,
        "reported_supplier_sites": 14,
        "reported_component_categories": 8,
    }
    for key, limit in caps.items():
        if isinstance(result.get(key), list):
            result[key] = result[key][:limit]
    if isinstance(result.get("item_and_component_evidence"), dict):
        items = dict(result["item_and_component_evidence"])
        for key, limit in (("top_items", 8), ("top_item_supplier_sites", 8)):
            if isinstance(items.get(key), list):
                items[key] = items[key][:limit]
        result["item_and_component_evidence"] = items
    result["evidence_scope"] = {
        "depth": "selected_platform_topic",
        "topic": topic,
        "omitted_lanes": sorted(set(pack) - set(result)),
        "interpretation": (
            "Only the selected topic and bounded examples were supplied. "
            "Omitted lanes were not checked and must not be described as absent or zero."
        ),
    }
    return result
