"""Build customer-safe structured artifacts returned alongside Ask Mimir prose."""

from __future__ import annotations

from typing import Any, Dict


def platform_answer_artifacts(pack: Dict[str, Any]) -> Dict[str, Any]:
    customer_fields = {
        "supplier",
        "cage",
        "contracting_site_city",
        "contracting_site_state",
        "contracting_site_country",
        "observed_place_of_performance_locations",
        "platform",
        "supplier_tier",
        "capability",
        "evidence_examples",
        "source_urls",
        "notes",
    }
    return {
        "supplier_site_index": [
            {key: value for key, value in row.items() if key in customer_fields}
            for row in pack.get("supplier_site_summary", [])
        ],
        "evidence_pack": {
            "format": "zip",
            "download_url": (
                "/api/evidence/platform-supply-chain/"
                f"{pack.get('scope', {}).get('platform_id', 'CH-53K')}.zip"
            ),
        },
    }
