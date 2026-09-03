"""Customer-safe CSV evidence pack for named-market competitive position."""

from __future__ import annotations

import csv
import io
import json
import zipfile
from typing import Any, Dict, Iterable


def _csv(rows: Iterable[Dict[str, Any]]) -> bytes:
    materialized = list(rows)
    if not materialized:
        return b""
    fields = sorted({key for row in materialized for key in row})
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    for row in materialized:
        writer.writerow(
            {
                key: " | ".join(map(str, value)) if isinstance(value, list)
                else json.dumps(value, sort_keys=True) if isinstance(value, dict)
                else value
                for key, value in row.items()
            }
        )
    return stream.getvalue().encode()


def build_competitive_position_zip(pack: Dict[str, Any]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("reported_supply_chain_position.csv", _csv(pack["reported_supply_chain_position"]))
        archive.writestr("dla_item_procurement_position.csv", _csv(pack["dla_item_procurement_position"]))
        archive.writestr("direct_award_position.csv", _csv(pack["direct_award_position"]))
        archive.writestr(
            "methodology.json",
            json.dumps(
                {
                    "scope": pack["scope"],
                    "coverage": pack["coverage"],
                    "methodology": pack["methodology"],
                    "calculation_version": pack["calculation_version"],
                    "definition_version": pack["definition_version"],
                },
                indent=2,
            ),
        )
    return output.getvalue()
