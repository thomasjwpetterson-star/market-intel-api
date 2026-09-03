"""Read deterministic program-momentum packs for Ask Mimir."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


ROOT = Path(__file__).resolve().parent
DEFAULT_PACK = ROOT / "validation-output" / "program-momentum" / "missile-program-momentum.json"


class ProgramMomentumStore:
    def __init__(self, pack_path: Path = DEFAULT_PACK) -> None:
        self.pack_path = pack_path.resolve()
        self.pack = json.loads(self.pack_path.read_text())

    def get(self, *, market: str, limit: int) -> Dict[str, Any]:
        if market.strip().lower() not in {"missile", "missiles", "us missile programs"}:
            raise KeyError(f"unsupported momentum market: {market}")
        bounded_limit = min(max(int(limit), 1), 12)
        programs = [
            self._answer_projection(program)
            for program in self.pack["programs"][:bounded_limit]
        ]
        return {
            key: value
            for key, value in self.pack.items()
            if key not in {"programs", "evidence_fingerprints"}
        } | {"programs": programs}

    def full_programs(self, *, limit: int) -> list[Dict[str, Any]]:
        """Return complete released records for drawers and exports, not model context."""
        bounded_limit = min(max(int(limit), 1), 12)
        return self.pack["programs"][:bounded_limit]

    @staticmethod
    def _answer_projection(program: Dict[str, Any]) -> Dict[str, Any]:
        """Keep the synthesis prompt compact without discarding released evidence."""
        lanes = program["signal_lanes"]
        production = lanes["production_events"]
        approved_events = [
            event
            for event in production.get("events", [])
            if event.get("registry_status") == "APPROVED"
        ]
        projected_lanes = {
            **lanes,
            "production_events": {
                **production,
                "events": approved_events,
                "provisional_score": None,
                "governance_note": None,
            },
        }
        return {
            "program_id": program["program_id"],
            "display_name": program["display_name"],
            "rank": program["rank"],
            "composite": program["composite"],
            "signal_lanes": projected_lanes,
            "top_prime_awards": program.get("top_prime_awards", [])[:3],
            "top_reported_supplier_sites": program.get(
                "top_reported_supplier_sites", []
            )[:4],
            "open_opportunities": program.get("open_opportunities", [])[:3],
        }

    def explain(self, *, program_id: str) -> Dict[str, Any]:
        wanted = program_id.strip().upper()
        for index, program in enumerate(self.pack["programs"]):
            if program["program_id"].upper() == wanted:
                return {
                    "calculation_version": self.pack["calculation_version"],
                    "completed_fiscal_year_window": self.pack["completed_fiscal_year_window"],
                    "weights": self.pack["weights"],
                    "program": program,
                    "immediately_above": self.pack["programs"][index - 1] if index else None,
                    "immediately_below": self.pack["programs"][index + 1]
                    if index + 1 < len(self.pack["programs"])
                    else None,
                    "interpretation_rules": self.pack["interpretation_rules"],
                }
        raise KeyError(f"program momentum record was not found: {program_id}")
