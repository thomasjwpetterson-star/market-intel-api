"""Read compact, versioned company contexts for the isolated Ask Mimir lab."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parent
DEFAULT_CONTEXT_DIR = ROOT / "validation-output" / "company-context"


FOCUS_SECTIONS = {
    "profile": [
        "identity",
        "observed_financials",
        "annual_activity",
        "site_financials",
        "location_footprint",
        "capability_evidence",
        "product_and_part_evidence",
        "platform_exposure",
        "missile_program_trajectory",
        "future_demand_context",
        "customer_context",
        "top_awards",
        "evidence_index",
        "quality",
    ],
    "full_dossier": [
        "identity",
        "observed_financials",
        "annual_activity",
        "site_financials",
        "location_footprint",
        "capability_evidence",
        "product_and_part_evidence",
        "platform_exposure",
        "missile_program_trajectory",
        "future_demand_context",
        "customer_context",
        "reported_subcontract_relationships",
        "top_awards",
        "open_solicitation_candidates",
        "evidence_index",
        "quality",
    ],
    "supply_chain": [
        "identity",
        "annual_activity",
        "capability_evidence",
        "platform_exposure",
        "missile_program_trajectory",
        "reported_subcontract_relationships",
        "quality",
    ],
    "opportunity_discovery": [
        "identity",
        "observed_financials",
        "annual_activity",
        "capability_evidence",
        "platform_exposure",
        "missile_program_trajectory",
        "customer_context",
        "reported_subcontract_relationships",
        "top_awards",
        "open_solicitation_candidates",
        "quality",
    ],
}


class CompanyContextStore:
    def __init__(self, context_dir: Path = DEFAULT_CONTEXT_DIR) -> None:
        self.context_dir = context_dir.resolve()
        manifest_path = self.context_dir / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(f"company context manifest was not found: {manifest_path}")
        self.manifest = json.loads(manifest_path.read_text())
        self.contexts: List[Dict[str, Any]] = []
        for entry in self.manifest.get("contexts", []):
            path = self.context_dir / entry["path"]
            context = json.loads(path.read_text())
            context["_artifact_path"] = str(path)
            self.contexts.append(context)

    def search(
        self, query: str, scope_type: str | None = None, limit: int = 10
    ) -> Dict[str, Any]:
        clean_query = str(query or "").strip().upper()
        if not clean_query:
            return {"matches": []}
        matches = []
        for context in self.contexts:
            scope = context["scope"]
            if scope_type and scope["scope_type"] != scope_type:
                continue
            searchable = [scope["scope_id"], scope["scope_name"]]
            searchable.extend(
                context.get("identity", {})
                .get("parent_resolution", {})
                .get("aliases", [])
            )
            for site in context.get("identity", {}).get("sites", []):
                searchable.extend(
                    [
                        site.get("cage"),
                        site.get("vendor_name"),
                        site.get("official_site_label"),
                        site.get("city"),
                        site.get("state"),
                    ]
                )
            if not any(clean_query in str(value or "").upper() for value in searchable):
                continue
            sites = context.get("identity", {}).get("sites", [])
            representative_site = sites[0] if sites else {}
            if scope["scope_type"] == "company_site":
                location = ", ".join(
                    value
                    for value in (
                        representative_site.get("city"),
                        representative_site.get("state"),
                    )
                    if value
                )
                option_label = scope["scope_name"]
                if location:
                    option_label += f" - {location}"
                option_label += f" (CAGE {scope['scope_id']})"
            else:
                site_count = context.get("identity", {}).get("site_count", 0)
                option_label = f"{scope['scope_name']} - parent view ({site_count} sites)"
            matches.append(
                {
                    "context_id": context["context_id"],
                    "scope_type": scope["scope_type"],
                    "scope_id": scope["scope_id"],
                    "scope_name": scope["scope_name"],
                    "observation_window": scope["observation_window"],
                    "site_count": context.get("identity", {}).get("site_count", 0),
                    "resolved_cages": context.get("identity", {}).get("resolved_cages", []),
                    "city": representative_site.get("city"),
                    "state": representative_site.get("state"),
                    "option_label": option_label,
                }
            )
        matches.sort(key=lambda row: (row["scope_type"] != "company_site", row["scope_name"]))
        selected = matches[: max(1, min(int(limit), 20))]
        distinct_site_ids = {
            row["scope_id"] for row in selected if row["scope_type"] == "company_site"
        }
        requires_disambiguation = len(distinct_site_ids) > 1
        site_options = [
            row["option_label"]
            for row in selected
            if row["scope_type"] == "company_site"
        ]
        multi_site_parent_options = [
            row["option_label"]
            for row in selected
            if row["scope_type"] == "company_parent" and row["site_count"] > 1
        ]
        return {
            "query": query,
            "matches": selected,
            "requires_disambiguation": requires_disambiguation,
            "disambiguation_options": (
                (
                    site_options[:5] + multi_site_parent_options[:1]
                    if multi_site_parent_options
                    else site_options[:6]
                )
                if requires_disambiguation
                else []
            ),
        }

    def get(self, scope_type: str, scope_id: str, focus: str) -> Dict[str, Any]:
        if focus not in FOCUS_SECTIONS:
            raise ValueError(f"unsupported company context focus: {focus}")
        clean_id = str(scope_id).strip().upper()
        context = next(
            (
                row
                for row in self.contexts
                if row["scope"]["scope_type"] == scope_type
                and str(row["scope"]["scope_id"]).upper() == clean_id
            ),
            None,
        )
        if context is None:
            raise KeyError(f"company context was not found: {scope_type}/{scope_id}")

        result = {
            "context_id": context["context_id"],
            "evidence_fingerprint": context["evidence_fingerprint"],
            "calculation_version": context["calculation_version"],
            "generated_at": context["generated_at"],
            "scope": context["scope"],
            "focus": focus,
        }
        for section in FOCUS_SECTIONS[focus]:
            result[section] = self._compact_section(section, context.get(section))
        result["evidence_chain"] = {
            "identity_definition_version": context.get("source_manifest", {})
            .get("identity_definition", {})
            .get("version"),
            "source_file_hashes": {
                name: details.get("sha256")
                for name, details in context.get("source_manifest", {}).get("files", {}).items()
            },
        }
        return result

    @staticmethod
    def _compact_section(section: str, value: Any) -> Any:
        if section == "identity" and isinstance(value, dict):
            return {**value, "sites": value.get("sites", [])[:20]}
        if section == "site_financials" and isinstance(value, list):
            return value[:15]
        if section == "capability_evidence" and isinstance(value, dict):
            return {
                **value,
                "psc": value.get("psc", [])[:6],
                "naics": value.get("naics", [])[:4],
                "dla_items": value.get("dla_items", [])[:5],
                "prime_award_descriptions": value.get("prime_award_descriptions", [])[:6],
                "reported_subaward_descriptions": value.get(
                    "reported_subaward_descriptions", []
                )[:5],
            }
        if section == "location_footprint" and isinstance(value, dict):
            return {
                **value,
                "registered_or_contracting_sites": value.get(
                    "registered_or_contracting_sites", []
                )[:20],
                "prime_award_places_of_performance": value.get(
                    "prime_award_places_of_performance", []
                )[:8],
                "reported_subaward_locations": value.get(
                    "reported_subaward_locations", []
                )[:8],
            }
        if section == "product_and_part_evidence" and isinstance(value, dict):
            financial_rows = []
            for row in value.get("niin_financial_observations", [])[:10]:
                financial_rows.append(
                    {**row, "contract_ids": row.get("contract_ids", [])[:6]}
                )
            qualified = value.get("qualified_source_context", {})
            qualified_items = list(qualified.get("items", []))
            selected_qualified = qualified_items[:12]
            selected_niins = {row.get("niin") for row in selected_qualified}
            financial_niins = {row.get("niin") for row in financial_rows}
            selected_qualified.extend(
                row
                for row in qualified_items
                if row.get("niin") in financial_niins
                and row.get("niin") not in selected_niins
            )
            return {
                **value,
                "niin_financial_observations": financial_rows,
                "part_number_references": value.get("part_number_references", [])[:20],
                "qualified_source_context": {
                    **qualified,
                    "items": selected_qualified,
                },
            }
        if section in {"platform_exposure", "customer_context", "top_awards"}:
            return (value or [])[:8]
        if section == "missile_program_trajectory" and isinstance(value, dict):
            return {**value, "programs": value.get("programs", [])[:8]}
        if section == "future_demand_context" and isinstance(value, dict):
            programs = []
            for program in value.get("programs", [])[:5]:
                budget_lines: Dict[tuple, Dict[str, Any]] = {}
                for row in program.get("budget_projection_rows", []):
                    key = (
                        row.get("component"),
                        row.get("p1_line_number"),
                        row.get("budget_line_item"),
                        row.get("budget_line_item_title"),
                        row.get("is_advance_procurement_exhibit"),
                    )
                    budget_line = budget_lines.setdefault(
                        key,
                        {
                            "component": row.get("component"),
                            "p1_line_number": row.get("p1_line_number"),
                            "budget_line_item": row.get("budget_line_item"),
                            "budget_line_item_title": row.get("budget_line_item_title"),
                            "is_advance_procurement_exhibit": row.get(
                                "is_advance_procurement_exhibit"
                            ),
                            "observations": {},
                            "source": {
                                "source_document_title": row.get(
                                    "source_document_title"
                                ),
                                "source_page_number": row.get("source_page_number"),
                                "source_landing_page": row.get("source_landing_page"),
                                "source_download_url": row.get("source_download_url"),
                                "source_locator": row.get("source_locator"),
                            },
                        },
                    )
                    fiscal_year = str(row.get("fiscal_year"))
                    observation = budget_line["observations"].setdefault(
                        fiscal_year,
                        {
                            "fiscal_year": row.get("fiscal_year"),
                            "funding_status": row.get("funding_status"),
                        },
                    )
                    if row.get("measure_type") == "net_procurement_p1":
                        observation["net_procurement_usd"] = row.get("amount_usd")
                    elif row.get("measure_type") == "procurement_quantity":
                        observation["procurement_quantity"] = row.get("quantity")
                compact_lines = []
                for budget_line in budget_lines.values():
                    budget_line["observations"] = sorted(
                        budget_line["observations"].values(),
                        key=lambda row: row.get("fiscal_year") or 0,
                    )
                    compact_lines.append(budget_line)
                programs.append(
                    {
                        "program_id": program.get("program_id"),
                        "program_name": program.get("program_name"),
                        "observed_site_reported_subcontract_value_usd": program.get(
                            "observed_site_reported_subcontract_value_usd"
                        ),
                        "budget_lines": compact_lines[:8],
                    }
                )
            return {**value, "programs": programs}
        if section == "reported_subcontract_relationships" and isinstance(value, dict):
            return {
                **value,
                "as_subcontractor_to": value.get("as_subcontractor_to", [])[:6],
                "reported_subcontractors": value.get("reported_subcontractors", [])[:6],
            }
        if section == "open_solicitation_candidates" and isinstance(value, dict):
            return {**value, "candidates": value.get("candidates", [])[:5]}
        if section == "evidence_index" and isinstance(value, dict):
            records = value.get("records", [])
            balanced = []
            limits = {
                "official_company_site_source": 3,
                "usaspending_prime_award": 5,
                "dla_procurement_history": 5,
                "usaspending_reported_subaward": 5,
            }
            for evidence_type, limit in limits.items():
                balanced.extend(
                    [
                        row
                        for row in records
                        if row.get("evidence_type") == evidence_type
                    ][:limit]
                )
            return {**value, "records": balanced}
        return value
