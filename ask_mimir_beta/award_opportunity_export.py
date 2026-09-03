"""Customer-safe evidence exports for contracts and opportunities."""

from __future__ import annotations

import csv
import io
import re
import zipfile
from typing import Any, Dict, Iterable, List


def _text(value: Any) -> Any:
    if isinstance(value, list):
        return " | ".join(str(item) for item in value if item not in (None, ""))
    if isinstance(value, dict):
        return " | ".join(f"{key}: {item}" for key, item in value.items() if item not in (None, ""))
    return value


def _write_csv(archive: zipfile.ZipFile, name: str, rows: Iterable[Dict[str, Any]], fields: List[str]) -> None:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: _text(row.get(field)) for field in fields})
    archive.writestr(name, stream.getvalue().encode("utf-8-sig"))


def build_award_opportunity_evidence_zip(context: Dict[str, Any]) -> bytes:
    output = io.BytesIO()
    record_type = context["identity"]["record_type"]
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        _write_csv(archive, "01_record_summary.csv", [context["identity"]], list(context["identity"].keys()))
        if record_type == "contract":
            _write_csv(
                archive, "02_annual_prime_obligations.csv",
                context["financial_summary"]["annual_obligations"],
                ["fiscal_year", "net_prime_obligations_usd", "action_count", "earliest_action_date", "latest_action_date", "observation_status"],
            )
            _write_csv(
                archive, "03_action_history.csv", context.get("action_history", []),
                ["contract_id", "modification_number", "action_date", "fiscal_year", "prime_obligation_usd", "action_description", "base_award_description", "recipient_name", "recipient_cage", "psc", "naics_code", "platform", "place_of_performance_city", "place_of_performance_state", "place_of_performance_country"],
            )
            _write_csv(
                archive, "04_reported_subaward_suppliers.csv", context.get("reported_subaward_suppliers", []),
                ["supplier_cage", "supplier_name", "city", "state", "country", "mimir_modelled_reported_subcontract_value_usd", "source_reported_value_usd", "selected_report_count", "reported_action_count", "first_reported_date", "latest_reported_date", "reported_descriptions"],
            )
            _write_csv(
                archive, "05_comparable_suppliers.csv", context.get("comparable_suppliers", []),
                ["cage", "supplier_name", "city", "state", "completed_year_prime_obligations_usd", "award_count", "matching_psc_awards", "matching_naics_awards", "relevance_band", "relevance_reasons", "sample_contract_ids", "sample_award_descriptions", "interpretation"],
            )
            _write_csv(
                archive, "06_related_opportunities.csv", context.get("related_opportunities", []),
                ["id", "sol_num", "title", "agency", "sub_agency", "deadline", "set_aside_type", "naics_code", "psc", "state", "url"],
            )
        else:
            _write_csv(
                archive, "02_historically_relevant_suppliers.csv", context.get("likely_competitors", []),
                ["cage", "supplier_name", "city", "state", "completed_year_prime_obligations_usd", "award_count", "matching_psc_awards", "matching_naics_awards", "program_match_awards", "program_match_obligations_usd", "relevance_band", "relevance_reasons", "program_match_contract_ids", "program_match_descriptions", "sample_contract_ids", "sample_award_descriptions", "interpretation"],
            )
            _write_csv(
                archive, "03_direct_program_award_recipients.csv", context.get("direct_program_award_recipients", []),
                ["cage", "supplier_name", "city", "state", "program_match_awards", "program_match_obligations_usd", "program_match_contract_ids", "program_match_descriptions"],
            )
            _write_csv(
                archive, "04_related_historical_awards.csv", context.get("related_historical_awards", []),
                ["contract_id", "recipient_name", "recipient_cage", "base_award_description", "psc", "naics_code", "completed_year_prime_obligations_usd", "action_count", "start_date", "last_action_date", "parent_agency", "sub_agency", "place_of_performance_city", "place_of_performance_state", "same_buyer_family"],
            )
            _write_csv(
                archive, "05_related_current_opportunities.csv", context.get("related_current_opportunities", []),
                ["id", "sol_num", "title", "agency", "sub_agency", "deadline", "set_aside_type", "naics_code", "psc", "state", "url"],
            )
            _write_csv(
                archive, "06_description_platform_candidates.csv", context.get("description_platform_candidates", []),
                ["platform_family", "status", "evidence", "may_attribute_financials"],
            )
        _write_csv(
            archive, "09_source_guide.csv", context.get("evidence_index", []),
            ["source", "public_record_id", "supports", "public_url"],
        )
        archive.writestr(
            "README.txt",
            (
                "Mimir contract and opportunity evidence pack\n\n"
                "Prime obligations and Mimir-modelled reported subcontract value are separate measures.\n"
                "Historically relevant suppliers are evidence-based potential competitors or teaming candidates, not confirmed bidders.\n"
                "The source guide identifies the public records supporting the dossier.\n"
            ),
        )
    return output.getvalue()


def award_opportunity_evidence_filename(context: Dict[str, Any]) -> str:
    identity = context["identity"]
    identifier = identity.get("contract_id") or identity.get("solicitation_number") or identity.get("opportunity_id")
    clean = re.sub(r"[^A-Za-z0-9]+", "-", str(identifier)).strip("-").lower()
    return f"mimir-{identity['record_type']}-{clean}-evidence.zip"
