"""Fail a FYDP release when governed sources or explicit program links disappear."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LINKAGES = ROOT / "fydp_platform_linkages.json"
DEFAULT_MANIFEST = Path(__file__).resolve().parent / "fydp_source_manifest.json"

AIRCRAFT_PROGRAM_IDS = {
    "AH64",
    "B1B",
    "B2",
    "B52",
    "C130",
    "C17A",
    "C5",
    "CH47",
    "CH53K",
    "E2D",
    "E3",
    "F15",
    "F16",
    "F22A",
    "F35",
    "FA18",
    "KC46A",
    "MQ4C",
    "MQ9",
    "P8A",
    "T7A",
    "UH60",
    "V22",
}


def _normalize(value: Any) -> str:
    return " ".join(re.findall(r"[A-Z0-9]+", str(value or "").upper()))


def _ordered(values: Iterable[str]) -> List[str]:
    return sorted({str(value) for value in values})


def validate(
    facts_path: Path,
    linkages_path: Path = DEFAULT_LINKAGES,
    manifest_path: Path = DEFAULT_MANIFEST,
) -> Dict[str, Any]:
    catalogue = json.loads(linkages_path.read_text())
    manifest = json.loads(manifest_path.read_text())
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT
                source_id,
                budget_line_item_title,
                fiscal_year,
                availability_status,
                amount_usd,
                quantity
            FROM read_parquet(?)
            WHERE fiscal_year BETWEEN 2028 AND 2031
            """,
            [str(facts_path.resolve())],
        ).fetchall()
    finally:
        connection.close()

    source_ids = {str(row[0]) for row in rows}
    expected_source_ids = {
        str(source["source_id"]) for source in manifest.get("sources", [])
    }
    title_rows: Dict[str, list[tuple[Any, ...]]] = {}
    for row in rows:
        title_rows.setdefault(_normalize(row[1]), []).append(row)

    linked_with_published_outyears = []
    linked_without_published_outyears = []
    unlinked_programs = []
    program_matches: Dict[str, List[str]] = {}
    for definition in catalogue.get("linkages", []):
        program_id = str(definition["program_id"])
        aliases = {
            _normalize(alias)
            for alias in definition.get("budget_title_aliases", [])
            if _normalize(alias)
        }
        matched_titles = sorted(aliases.intersection(title_rows))
        program_matches[program_id] = matched_titles
        if not matched_titles:
            unlinked_programs.append(program_id)
            continue
        matching_rows = [
            row for title in matched_titles for row in title_rows.get(title, [])
        ]
        has_published_value = any(
            str(row[3]) == "PUBLISHED" and (row[4] is not None or row[5] is not None)
            for row in matching_rows
        )
        if has_published_value:
            linked_with_published_outyears.append(program_id)
        else:
            linked_without_published_outyears.append(program_id)

    aircraft_without_published_outyears = sorted(
        AIRCRAFT_PROGRAM_IDS.difference(linked_with_published_outyears)
    )
    return {
        "facts_path": str(facts_path.resolve()),
        "definition_version": catalogue.get("definition_version"),
        "source_count": len(source_ids),
        "expected_source_count": len(expected_source_ids),
        "missing_source_ids": _ordered(expected_source_ids.difference(source_ids)),
        "program_count": len(catalogue.get("linkages", [])),
        "linked_with_published_outyears": _ordered(linked_with_published_outyears),
        "linked_without_published_outyears": _ordered(
            linked_without_published_outyears
        ),
        "unlinked_programs": _ordered(unlinked_programs),
        "aircraft_program_count": len(AIRCRAFT_PROGRAM_IDS),
        "aircraft_without_published_outyears": aircraft_without_published_outyears,
        "program_matches": program_matches,
        "passed": not (
            expected_source_ids.difference(source_ids)
            or unlinked_programs
            or aircraft_without_published_outyears
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--facts", type=Path, required=True)
    parser.add_argument("--linkages", type=Path, default=DEFAULT_LINKAGES)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    args = parser.parse_args()
    result = validate(args.facts, args.linkages, args.manifest)
    print(json.dumps(result, indent=2))
    if not result["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
