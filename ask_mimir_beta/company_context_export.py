"""Create customer-safe CSV evidence packs for Ask Mimir company dossiers."""

from __future__ import annotations

import csv
import io
import json
import re
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parent
DEFAULT_CONTEXT_DIR = ROOT / "validation-output" / "company-context"


def _text(value: Any) -> Any:
    if isinstance(value, list):
        return " | ".join(str(item) for item in value if item not in (None, ""))
    return value


def _write_csv(
    archive: zipfile.ZipFile,
    name: str,
    rows: Iterable[Dict[str, Any]],
    fieldnames: List[str],
) -> None:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: _text(row.get(key)) for key in fieldnames})
    archive.writestr(name, stream.getvalue().encode("utf-8-sig"))


def _load_context(scope_type: str, scope_id: str, context_dir: Path) -> Dict[str, Any]:
    clean_type = str(scope_type).strip().lower()
    clean_id = str(scope_id).strip().upper()
    manifest = json.loads((context_dir / "manifest.json").read_text())
    for entry in manifest.get("contexts", []):
        context = json.loads((context_dir / entry["path"]).read_text())
        scope = context.get("scope", {})
        if (
            str(scope.get("scope_type", "")).lower() == clean_type
            and str(scope.get("scope_id", "")).upper() == clean_id
        ):
            return context
    raise KeyError(f"company context was not found: {scope_type}/{scope_id}")


def build_company_evidence_zip(
    scope_type: str,
    scope_id: str,
    context_dir: Path = DEFAULT_CONTEXT_DIR,
) -> bytes:
    context = _load_context(scope_type, scope_id, context_dir)
    product = context.get("product_and_part_evidence", {})
    relationships = context.get("reported_subcontract_relationships", {})
    locations = context.get("location_footprint", {})

    location_rows = []
    for location_type, key in (
        ("Registered or contracting site", "registered_or_contracting_sites"),
        ("Prime award place of performance", "prime_award_places_of_performance"),
        ("Reported subcontract location", "reported_subaward_locations"),
    ):
        for row in locations.get(key, []):
            location_rows.append({"location_type": location_type, **row})

    budget_rows = []
    for program in context.get("future_demand_context", {}).get("programs", []):
        for row in program.get("budget_projection_rows", []):
            budget_rows.append(
                {
                    "program": program.get("program_name"),
                    "observed_site_reported_subcontract_value_usd": program.get(
                        "observed_site_reported_subcontract_value_usd"
                    ),
                    **row,
                }
            )

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        _write_csv(
            archive,
            "01_financial_summary.csv",
            context.get("observed_financials", []),
            [
                "measure_type", "net_value_usd", "positive_value_usd",
                "deobligation_value_usd", "distinct_awards", "distinct_actions",
                "first_action_date", "latest_action_date",
            ],
        )
        _write_csv(
            archive,
            "02_prime_awards.csv",
            context.get("top_awards", []),
            [
                "contract_id", "recipient_cage", "recipient_name", "base_award_description",
                "net_prime_obligations_usd", "action_count", "first_action_date",
                "latest_action_date", "platform_family", "psc", "public_record_url",
            ],
        )
        _write_csv(
            archive,
            "03_prime_customer_routes.csv",
            relationships.get("as_subcontractor_to", []),
            [
                "prime_cage", "prime_name", "reported_prime_parent", "registered_city",
                "registered_state", "mimir_modelled_subcontract_value_usd",
                "source_reported_value_usd", "selected_relationship_actions",
                "first_observed_date", "latest_observed_date",
            ],
        )
        _write_csv(
            archive,
            "04_platform_exposure.csv",
            context.get("platform_exposure", []),
            [
                "evidence_layer", "platform_family", "observed_value_usd",
                "distinct_awards", "contributing_sites", "latest_date",
            ],
        )
        _write_csv(
            archive,
            "05_niin_procurement.csv",
            product.get("niin_financial_observations", []),
            [
                "niin", "nsn", "description", "dla_procurement_value_usd",
                "distinct_awards", "distinct_actions", "contract_ids",
                "first_observed_date", "latest_observed_date",
            ],
        )
        _write_csv(
            archive,
            "06_part_number_references.csv",
            product.get("part_number_references", []),
            [
                "niin", "nsn", "cage", "part_number", "description", "supplier_status",
                "is_procurement_authorized", "is_active_authorized_source", "rncc_codes",
                "rnvc_codes", "rnsc_codes", "cage_status_codes", "platform_families",
                "platform_count", "has_multiple_platforms",
            ],
        )
        _write_csv(
            archive,
            "07_authorized_source_context.csv",
            product.get("qualified_source_context", {}).get("items", []),
            [
                "niin", "nsn", "description", "acquisition_advice_code",
                "target_is_procurement_authorized", "target_is_active_authorized_source",
                "active_authorized_source_count", "other_active_authorized_source_count",
                "active_authorized_sources", "target_reference_status",
            ],
        )
        _write_csv(
            archive,
            "08_locations.csv",
            location_rows,
            [
                "location_type", "cage", "vendor_name", "city", "state", "country",
                "postal_code", "location_quality", "net_prime_obligations_usd",
                "mimir_modelled_reported_subcontract_value_usd", "distinct_awards",
                "distinct_actions", "selected_relationship_actions",
            ],
        )
        _write_csv(
            archive,
            "09_forward_program_funding.csv",
            budget_rows,
            [
                "program", "component", "budget_line_item", "budget_line_item_title",
                "fiscal_year", "funding_status", "measure_type", "amount_usd", "quantity",
                "source_document_title", "source_page_number", "source_landing_page",
                "source_download_url", "source_locator",
                "observed_site_reported_subcontract_value_usd",
            ],
        )
        _write_csv(
            archive,
            "10_public_evidence_links.csv",
            context.get("evidence_index", {}).get("records", []),
            ["evidence_type", "record_id", "title", "public_url", "supports"],
        )
        scope = context["scope"]
        archive.writestr(
            "README.txt",
            (
                "Mimir company evidence pack\n\n"
                f"Scope: {scope['scope_name']} ({scope['scope_type']} {scope['scope_id']})\n"
                f"Observation window: {scope['observation_window']}\n"
                f"Calculation version: {context['calculation_version']}\n\n"
                "Prime obligations, DLA procurement value and Mimir-modelled reported "
                "subcontract value are presented as separate measures. DLA financial value is "
                "reported at NIIN/CAGE level; part-number records describe reference relationships.\n"
            ),
        )
    return output.getvalue()


def evidence_pack_filename(scope_type: str, scope_id: str) -> str:
    safe_id = re.sub(r"[^a-z0-9]+", "-", str(scope_id).lower()).strip("-")
    return f"mimir-{scope_type.lower().replace('_', '-')}-{safe_id}-evidence.zip"
