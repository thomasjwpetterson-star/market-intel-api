"""Customer-safe CSV evidence packs for NSN, NIIN and part-number dossiers."""

from __future__ import annotations

import csv
import io
import re
import zipfile
from typing import Any, Dict, Iterable, List


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


def build_item_evidence_zip(context: Dict[str, Any]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        _write_csv(
            archive,
            "01_item_identity.csv",
            [context["identity"]],
            [
                "nsn", "niin", "fsc_code", "description", "unit_of_issue",
                "acquisition_advice_code", "government_estimated_price_usd",
                "source_of_supply", "demil_code", "shelf_life_code",
            ],
        )
        _write_csv(
            archive,
            "02_annual_procurement_activity.csv",
            context.get("procurement_activity", {}).get("annual_activity", []),
            [
                "fiscal_year", "net_dla_procurement_value_usd", "distinct_awards",
                "distinct_transaction_lines", "observed_supplier_sites",
            ],
        )
        _write_csv(
            archive,
            "03_linked_prime_obligations_by_year.csv",
            context.get("linked_prime_activity", {}).get("annual_activity", []),
            [
                "fiscal_year", "net_linked_prime_obligations_usd", "distinct_awards",
                "distinct_actions", "recipient_sites", "observation_status",
            ],
        )
        _write_csv(
            archive,
            "04_supplier_sites.csv",
            context.get("supplier_summary", []),
            [
                "cage", "vendor_name", "city", "state", "location_quality",
                "has_observed_dla_procurement", "net_dla_procurement_value_usd",
                "distinct_dla_awards", "dla_transaction_line_count",
                "has_linked_prime_obligations", "net_linked_prime_obligations_usd",
                "distinct_linked_prime_awards", "latest_observed_date",
                "is_procurement_authorized", "is_active_authorized_source",
                "part_numbers", "relationship_statuses",
            ],
        )
        _write_csv(
            archive,
            "05_part_number_relationships.csv",
            context.get("reference_relationships", []),
            [
                "niin", "nsn", "cage", "vendor_name", "part_number",
                "supplier_status", "supplier_status_detail",
                "is_procurement_authorized", "is_active_authorized_source",
                "rnsc_codes", "rnsc_meanings", "rncc_codes", "rncc_meanings",
                "rnvc_codes", "rnvc_meanings", "cage_status_codes",
            ],
        )
        _write_csv(
            archive,
            "06_platform_associations.csv",
            context.get("platform_associations", {}).get("platforms", []),
            ["platform_family", "wsdc_codes", "association_sources"],
        )
        _write_csv(
            archive,
            "07_dla_contract_history.csv",
            context.get("contracts", []),
            [
                "contract_id", "vendor_cage", "vendor_name", "description",
                "net_dla_procurement_value_usd", "transaction_line_count",
                "first_observed_date", "latest_observed_date", "observed_part_numbers",
                "purchase_order_numbers",
            ],
        )
        _write_csv(
            archive,
            "08_linked_prime_awards.csv",
            context.get("linked_prime_awards", []),
            [
                "contract_id", "vendor_cage", "recipient_name", "base_award_description",
                "net_linked_prime_obligations_usd", "action_count", "first_action_date",
                "latest_action_date", "place_of_performance_city",
                "place_of_performance_state", "place_of_performance_country",
                "item_link_method",
            ],
        )
        _write_csv(
            archive,
            "09_source_guide.csv",
            context.get("evidence_index", []),
            ["source", "record_locator", "supports", "public_url"],
        )
        identity = context["identity"]
        window = context["observation_window"]
        archive.writestr(
            "README.txt",
            (
                "Mimir item evidence pack\n\n"
                f"NSN: {identity.get('nsn') or 'not available'}\n"
                f"NIIN: {identity['niin']}\n"
                f"Description: {identity.get('description') or 'not available'}\n"
                f"Completed-year observation window: {window['label']}\n\n"
                "DLA item procurement value is the primary item-specific financial measure. "
                "Federal obligations on linked awards provide broader award-level context; "
                "DLA item procurement remains the item-specific measure. "
                "Part-number relationships and platform associations are provided separately.\n"
            ),
        )
    return output.getvalue()


def item_evidence_filename(context: Dict[str, Any]) -> str:
    identifier = context.get("identity", {}).get("nsn") or context["identity"]["niin"]
    clean = re.sub(r"[^0-9A-Za-z]+", "-", str(identifier)).strip("-").lower()
    return f"mimir-item-{clean}-evidence.zip"
