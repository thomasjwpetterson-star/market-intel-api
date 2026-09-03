"""Read compact company opportunity packs for the isolated Ask Mimir lab."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parent
DEFAULT_OPPORTUNITY_DIR = ROOT / "validation-output" / "company-opportunities"


class CompanyOpportunityStore:
    def __init__(self, opportunity_dir: Path = DEFAULT_OPPORTUNITY_DIR) -> None:
        self.opportunity_dir = opportunity_dir.resolve()
        manifest_path = self.opportunity_dir / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(
                f"company opportunity manifest was not found: {manifest_path}"
            )
        self.manifest = json.loads(manifest_path.read_text())
        self.packs: List[Dict[str, Any]] = []
        for entry in self.manifest.get("packs", []):
            path = self.opportunity_dir / entry["path"]
            self.packs.append(json.loads(path.read_text()))

    def get(self, scope_type: str, scope_id: str) -> Dict[str, Any]:
        clean_id = str(scope_id).strip().upper()
        pack = next(
            (
                row
                for row in self.packs
                if row["scope"]["scope_type"] == scope_type
                and str(row["scope"]["scope_id"]).upper() == clean_id
            ),
            None,
        )
        if pack is None:
            raise KeyError(f"company opportunity pack was not found: {scope_type}/{scope_id}")
        return {
            "opportunity_pack_id": pack["opportunity_pack_id"],
            "calculation_version": pack["calculation_version"],
            "definition_version": pack["definition_version"],
            "generated_at": pack["generated_at"],
            "scope": pack["scope"],
            "identity": {
                "site_count": pack["identity"].get("site_count"),
                "resolved_cages": pack["identity"].get("resolved_cages", []),
                "sites": [
                    {
                        "cage": row.get("cage"),
                        "vendor_name": row.get("vendor_name"),
                        "city": row.get("city"),
                        "state": row.get("state"),
                        "official_site_label": row.get("official_site_label"),
                        "official_capability_summary": row.get(
                            "official_capability_summary"
                        ),
                    }
                    for row in pack["identity"].get("sites", [])[:5]
                ],
            },
            "capability_profile": pack["capability_profile"],
            "evidence_coverage": pack["evidence_coverage"],
            "current_exposure": {
                "observation_window": pack["current_exposure"].get(
                    "observation_window"
                ),
                "platforms": pack["current_exposure"].get("platforms", [])[:10],
            },
            "decision_horizons": {
                category: [self._compact_candidate(row) for row in rows]
                for category, rows in pack["decision_horizons"].items()
            },
            "assessment_framework": pack.get("assessment_framework", {}),
            "interpretation_rules": pack["interpretation_rules"],
            "accessible_value_rule": pack["accessible_value_rule"],
            "evidence_chain": pack["evidence_chain"],
        }

    @staticmethod
    def _compact_candidate(candidate: Dict[str, Any]) -> Dict[str, Any]:
        retained_keys = [
            "candidate_id",
            "rank",
            "title",
            "program",
            "current_exposure_status",
            "priority_band",
            "fit_band",
            "commercial_utility",
            "route_to_market",
            "next_actions",
            "disqualifiers",
            "candidate_status",
            "observed_position_check",
            "scoring_primitives",
        ]
        result = {
            key: candidate[key] for key in retained_keys if key in candidate
        }
        result["prime_award_evidence"] = [
            {
                "contract_id": row.get("contract_id"),
                "recipient_cage": row.get("recipient_cage"),
                "recipient_name": row.get("recipient_name"),
                "base_award_description": row.get("base_award_description"),
                "net_prime_obligations_usd": row.get("net_prime_obligations_usd"),
                "latest_action_date": row.get("latest_action_date"),
            }
            for row in candidate.get("prime_award_evidence", [])[:5]
        ]
        result["event_evidence"] = [
            {
                "event_id": row.get("event_id"),
                "event_type": row.get("event_type"),
                "event_date": row.get("event_date"),
                "effective_period": row.get("effective_period"),
                "status": row.get("status"),
                "fact": row.get("fact"),
                "source": {
                    "publisher": row.get("source", {}).get("publisher"),
                    "title": row.get("source", {}).get("title"),
                    "publication_date": row.get("source", {}).get(
                        "publication_date"
                    ),
                    "canonical_url": row.get("source", {}).get("canonical_url"),
                },
            }
            for row in candidate.get("event_evidence", [])[:3]
        ]
        capability_slice = candidate.get("analogous_subcontract_capability_slice", {})
        result["analogous_subcontract_capability_slice"] = {
            "observation_window": capability_slice.get("observation_window"),
            "visible_slice_value_usd": capability_slice.get(
                "visible_slice_value_usd"
            ),
            "interpretation": capability_slice.get("interpretation"),
            "analyst_reviewed_exclusions": capability_slice.get(
                "analyst_reviewed_exclusions", []
            ),
            "records": [
                {
                    "incumbent_cage": row.get("incumbent_cage"),
                    "incumbent_name": row.get("incumbent_name"),
                    "prime_names": row.get("prime_names"),
                    "mimir_modelled_subcontract_value_usd": row.get(
                        "mimir_modelled_subcontract_value_usd"
                    ),
                    "selected_action_count": row.get("selected_action_count"),
                    "latest_observed_date": row.get("latest_observed_date"),
                    "source_report_ids": row.get("source_report_ids", []),
                    "source_report_last_modified_dates": row.get(
                        "source_report_last_modified_dates", []
                    ),
                    "matching_descriptions": [
                        str(description)[:140]
                        for description in row.get("matching_descriptions", [])[:1]
                    ],
                }
                for row in capability_slice.get("records", [])[:4]
            ],
        }
        result["budget_evidence"] = [
            {
                "fiscal_year": row.get("fiscal_year"),
                "funding_status": row.get("funding_status"),
                "measure_type": row.get("measure_type"),
                "amount_usd": row.get("amount_usd"),
                "quantity": row.get("quantity"),
                "budget_line_item_title": row.get("budget_line_item_title"),
                "source_locator": (
                    f"{row.get('source_file')} / {row.get('source_sheet')} / "
                    f"row {row.get('source_row_number')}"
                ),
            }
            for row in candidate.get("budget_evidence", [])[:8]
        ]
        return result
