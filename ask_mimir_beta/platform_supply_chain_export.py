"""Create customer-safe CSV evidence packs for platform supply-chain answers."""

from __future__ import annotations

import csv
import io
import json
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parent
DEFAULT_PACK_DIR = ROOT / "validation-output" / "platform-supply-chains"


def _text(value: Any) -> Any:
    if isinstance(value, list):
        return " | ".join(str(item) for item in value if item not in (None, ""))
    return value


def _csv_bytes(rows: Iterable[Dict[str, Any]], fieldnames: List[str]) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: _text(row.get(key)) for key in fieldnames})
    return stream.getvalue().encode("utf-8-sig")


def _write_csv(
    archive: zipfile.ZipFile,
    name: str,
    rows: Iterable[Dict[str, Any]],
    fieldnames: List[str],
) -> None:
    archive.writestr(name, _csv_bytes(rows, fieldnames))


def build_customer_evidence_zip(
    platform_id: str,
    pack_dir: Path = DEFAULT_PACK_DIR,
) -> bytes:
    clean_id = str(platform_id).strip().upper()
    manifest = json.loads((pack_dir / "manifest.json").read_text())
    entry = next(
        (row for row in manifest.get("packs", []) if row["platform_id"].upper() == clean_id),
        None,
    )
    if entry is None:
        raise KeyError(f"platform supply-chain pack was not found: {platform_id}")
    pack = json.loads((pack_dir / entry["path"]).read_text())

    verified_rows = pack.get("supplier_site_summary", [])

    first_tier_rows = []
    for supplier in pack["reported_first_tier_supplier_sites"]:
        first_tier_rows.append(
            {
                "supplier": supplier.get("supplier_name"),
                "cage": supplier.get("cage"),
                "city": supplier.get("city"),
                "state": supplier.get("state"),
                "country": supplier.get("country"),
                "location_quality": supplier.get("location_quality"),
                "observed_place_of_performance_locations": supplier.get(
                    "observed_place_of_performance_locations"
                ),
                "reported_prime_contractors": supplier.get("reported_prime_names"),
                "mimir_modelled_reported_subcontract_value_usd": supplier.get(
                    "mimir_modelled_subcontract_value_usd"
                ),
                "selected_report_count": supplier.get("selected_report_count"),
                "first_reported_date": supplier.get("first_reported_date"),
                "latest_reported_date": supplier.get("latest_reported_date"),
                "reported_descriptions": supplier.get("reported_descriptions"),
                "capability_evidence": [
                    item.get("capability_description")
                    for item in supplier.get("capability_evidence", [])
                ],
                "sample_prime_contract_ids": supplier.get(
                    "sample_prime_contract_ids"
                ),
            }
        )

    capability_rows = []
    for row in pack["capability_supported_first_tier_suppliers"]:
        clean_row = {
            key: value
            for key, value in row.items()
            if key not in {"internal_source_report_ids", "supplier_capability_profile"}
        }
        profile = row.get("supplier_capability_profile", {})
        clean_row["supplier_capability_summary"] = profile.get("capability_summary")
        clean_row["capability_attribution_limit"] = profile.get("attribution_limit")
        clean_row["supplier_capability_sources"] = [
            source.get("url") for source in profile.get("sources", [])
        ]
        capability_rows.append(clean_row)

    prime_rows = []
    for relationship, rows in (
        ("Platform prime contractor", pack["platform_prime_contractors"]),
        ("Other direct government award recipient", pack["other_direct_prime_recipients"]),
    ):
        for row in rows:
            prime_rows.append({"relationship": relationship, **row})

    exclusions = [
        {key: value for key, value in row.items() if key != "internal_source_report_ids"}
        for row in pack["known_configuration_exclusions"]
    ]

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        _write_csv(
            archive,
            "01_verified_ch53k_content.csv",
            verified_rows,
            [
                "supplier",
                "cage",
                "contracting_site_city",
                "contracting_site_state",
                "contracting_site_country",
                "contracting_location_quality",
                "observed_place_of_performance_locations",
                "platform",
                "supplier_tier",
                "capability",
                "capability_scope",
                "evidence_examples",
                "source_urls",
                "notes",
            ],
        )
        _write_csv(
            archive,
            "02_reported_first_tier_suppliers.csv",
            first_tier_rows,
            [
                "supplier",
                "cage",
                "city",
                "state",
                "country",
                "location_quality",
                "observed_place_of_performance_locations",
                "reported_prime_contractors",
                "mimir_modelled_reported_subcontract_value_usd",
                "selected_report_count",
                "first_reported_date",
                "latest_reported_date",
                "reported_descriptions",
                "capability_evidence",
                "sample_prime_contract_ids",
            ],
        )
        _write_csv(
            archive,
            "03_reported_capability_evidence.csv",
            capability_rows,
            [
                "supplier_name",
                "cage",
                "reported_prime_names",
                "capability_description",
                "supplier_capability_summary",
                "capability_attribution_limit",
                "supplier_capability_sources",
                "evidence_precision",
                "mimir_modelled_subcontract_value_usd",
                "selected_report_count",
                "first_reported_date",
                "latest_reported_date",
                "source_descriptions",
                "prime_award_context",
                "sample_prime_contract_ids",
            ],
        )
        _write_csv(
            archive,
            "04_prime_and_direct_award_recipients.csv",
            prime_rows,
            [
                "relationship",
                "supplier_name",
                "cage",
                "city",
                "state",
                "country",
                "net_prime_obligations_usd",
                "positive_prime_obligations_usd",
                "deobligations_usd",
                "action_count",
                "award_count",
                "first_action_date",
                "latest_action_date",
                "sample_contract_ids",
                "sample_award_descriptions",
            ],
        )
        _write_csv(
            archive,
            "05_broader_ch53_family_items.csv",
            pack["broader_ch53_family"]["top_items"],
            [
                "platform_family",
                "niin",
                "nsn",
                "description",
                "observed_dla_value_usd",
                "observed_contract_count",
                "first_observed_year",
                "last_observed_year",
                "latest_observed_date",
                "referenced_cage_count",
                "referenced_part_number_count",
                "sample_part_numbers",
            ],
        )
        _write_csv(
            archive,
            "06_broader_ch53_family_supplier_references.csv",
            pack["broader_ch53_family"]["supplier_part_relationships"],
            [
                "platform_family",
                "niin",
                "nsn",
                "description",
                "cage",
                "vendor_name",
                "city",
                "state",
                "location_quality",
                "part_number",
                "supplier_status",
                "is_active_authorized_source",
                "rncc_codes",
                "rnvc_codes",
            ],
        )
        _write_csv(
            archive,
            "07_known_configuration_exclusions.csv",
            exclusions,
            [
                "exclusion_id",
                "excluded_content",
                "report_count",
                "supplier_site_count",
                "mimir_modelled_subcontract_value_usd",
                "supplier_names",
                "sample_prime_contract_ids",
                "reason",
            ],
        )
        archive.writestr(
            "README.txt",
            (
                "Mimir CH-53K supply-chain evidence pack\n\n"
                "Confirmed CH-53K content, reported first-tier subcontract relationships, "
                "direct government award recipients and broader CH-53 family references are "
                "separate evidence layers and should not be combined. Broader family records do "
                "not confirm CH-53K applicability. DLA procurement value is stored once at the "
                "NIIN level and is intentionally absent from supplier-reference rows. Prime "
                "obligations and reported subcontract value are non-additive.\n"
            ),
        )
    return output.getvalue()
