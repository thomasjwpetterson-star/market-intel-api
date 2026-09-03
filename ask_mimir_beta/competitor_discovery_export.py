"""Customer-safe evidence export for observed competitor discovery."""

from __future__ import annotations

import csv
import io
import json
import zipfile
from typing import Any, Dict, Iterable


def _csv(rows: Iterable[Dict[str, Any]]) -> bytes:
    records = list(rows)
    if not records:
        return b""
    fields = sorted({key for row in records for key in row})
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=fields)
    writer.writeheader()
    for row in records:
        writer.writerow(
            {
                key: " | ".join(map(str, value)) if isinstance(value, list)
                else json.dumps(value, sort_keys=True) if isinstance(value, dict)
                else value
                for key, value in row.items()
            }
        )
    return stream.getvalue().encode()


def build_competitor_discovery_zip(pack: Dict[str, Any]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("target_sites.csv", _csv(pack["target_sites"]))
        archive.writestr("observed_peer_sites.csv", _csv(pack["observed_peers"]))
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
