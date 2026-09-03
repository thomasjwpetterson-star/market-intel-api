"""Versioned web-source hierarchy used by Ask Mimir prompts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


DEFAULT_POLICY_PATH = Path(__file__).with_name("web_source_policy.json")


def load_web_source_policy(path: Path = DEFAULT_POLICY_PATH) -> Dict[str, Any]:
    policy = json.loads(path.read_text())
    if not policy.get("allow_general_web_fallback"):
        raise ValueError("Ask Mimir web research must retain a general-web fallback.")
    if not policy.get("tiers"):
        raise ValueError("Ask Mimir web source policy has no priority tiers.")
    return policy


def render_web_source_policy(policy: Dict[str, Any]) -> str:
    lines = [
        f"Web source policy: {policy['policy_id']}.",
        "The domains below are a priority hierarchy, not a hard allowlist.",
    ]
    for tier in policy["tiers"]:
        domains = ", ".join(tier["domains"])
        lines.append(
            f"Tier {tier['tier']} - {tier['name']}: {tier['guidance']} "
            f"Priority domains: {domains}."
        )
    lines.append("General-web fallback rules:")
    lines.extend(f"- {rule}" for rule in policy["general_web_rules"])
    return "\n".join(lines)
