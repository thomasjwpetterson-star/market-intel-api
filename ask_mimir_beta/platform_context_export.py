"""Customer evidence export for universal platform dossiers."""

from __future__ import annotations

import csv
import io
import re
import zipfile
from typing import Any, Dict, Iterable, List


def _value(value: Any) -> Any:
    if isinstance(value, list):
        return " | ".join(str(item) for item in value if item not in (None, ""))
    return value


def _write(archive: zipfile.ZipFile, name: str, rows: Iterable[Dict[str, Any]], fields: List[str]) -> None:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: _value(row.get(field)) for field in fields})
    archive.writestr(name, stream.getvalue().encode("utf-8-sig"))


def build_platform_context_zip(context: Dict[str, Any]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        _write(archive, "01_annual_activity.csv", context["annual_activity"]["records"], [
            "fiscal_year", "source_system", "net_prime_obligations_usd",
            "positive_prime_obligations_usd", "prime_deobligations_usd",
            "attributed_dla_procurement_value_usd", "shared_use_niin_exposure_usd",
            "action_or_line_count", "award_count", "niin_count",
        ])
        _write(archive, "02_direct_award_recipients.csv", context["direct_award_recipients"], [
            "cage", "recipient_name", "contracting_city", "contracting_state", "location_quality",
            "net_prime_obligations_usd", "positive_prime_obligations_usd", "deobligations_usd",
            "action_count", "award_count", "first_action_date", "latest_action_date",
            "sample_contract_ids", "sample_award_descriptions", "observed_places_of_performance",
        ])
        _write(archive, "03_reported_supplier_sites.csv", context["reported_supplier_sites"], [
            "cage", "supplier_name", "city", "state", "country", "location_quality",
            "mimir_modelled_reported_subcontract_value_usd", "source_reported_value_usd",
            "selected_report_count", "prime_award_count", "first_reported_date", "latest_reported_date",
            "reported_prime_names", "reported_prime_cages", "sample_prime_contract_ids", "reported_descriptions",
        ])
        _write(archive, "04_reported_component_categories.csv", context["reported_component_categories"], [
            "reported_description", "mimir_modelled_reported_subcontract_value_usd",
            "selected_report_count", "supplier_site_count", "suppliers", "sample_prime_contract_ids",
        ])
        _write(archive, "05_item_relationships.csv", context["item_and_component_evidence"]["top_items"], [
            "nsn", "niin", "description", "fsc_code", "attributed_dla_procurement_value_usd",
            "shared_use_niin_exposure_usd", "latest_observed_date", "wsdc_codes", "association_sources",
        ])
        _write(archive, "06_item_supplier_sites.csv", context["item_and_component_evidence"]["top_item_supplier_sites"], [
            "niin", "cage", "supplier_name", "attributed_dla_procurement_value_usd",
            "shared_use_niin_exposure_usd", "observed_units",
            "latest_observed_date", "has_multiple_platforms", "platform_families", "contract_count",
            "city", "state", "location_quality",
        ])
        _write(archive, "07_prime_awards.csv", context["top_prime_awards"], [
            "contract_id", "recipient_name", "recipient_cage", "base_award_description",
            "net_prime_obligations_usd", "action_count", "first_action_date", "latest_action_date",
            "place_of_performance_city", "place_of_performance_state",
        ])
        _write(archive, "08_opportunities.csv", context["current_opportunities"], [
            "id", "sol_num", "title", "agency", "sub_agency", "deadline", "response_status",
            "set_aside_type", "naics_code", "psc", "state", "url",
        ])
        _write(archive, "09_source_guide.csv", context["evidence_index"], [
            "source", "supports", "public_record_ids",
        ])
        archive.writestr(
            "README.txt",
            (
                f"Mimir platform evidence pack: {context['scope']['display_name']}\n\n"
                "Prime obligations, reported subcontract value, attributed DLA procurement and shared-use NIIN exposure are separate evidence lanes.\n"
                "Reported descriptions support bounded capability language. Exact component claims require platform-specific source evidence.\n"
            ),
        )
    return output.getvalue()


def platform_context_filename(context: Dict[str, Any]) -> str:
    clean = re.sub(r"[^A-Za-z0-9]+", "-", context["scope"]["platform_id"]).strip("-").lower()
    return f"mimir-platform-{clean}-evidence.zip"
