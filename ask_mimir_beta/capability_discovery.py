"""Evidence-led market-wide supplier discovery for supported capabilities."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List

import duckdb


DEFAULT_DATA_ROOT = Path(
    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data"
)

CAPABILITY_DEFINITIONS = {
    "aircraft_braking": {
        "display_name": "Military-aircraft braking systems and components",
        "request_pattern": r"\b(?:aircraft|aviation|military aircraft)\b.*\bbrak(?:e|es|ing)\b|\bbrak(?:e|es|ing)\b.*\b(?:aircraft|aviation)\b",
        "fsc_codes": ["1630"],
        "item_pattern": r"BRAKE|BRAKING|ANTI[- ]?SKID",
        "scope_note": "Aircraft wheel and brake equipment identified through FSC 1630 and matching item descriptions.",
    },
}

NON_COMMERCIAL_NAME_PATTERN = re.compile(
    r"MILITARY (?:STANDARDS|SPECIFICATIONS)|NAVAL INVENTORY|NAVAIR|NAVSEA|"
    r"UNITED STATES DEPARTMENT|U\.?\s*S\.?\s*(?:ARMY|AIR FORCE|NAVY)|"
    r"DEFENSE LOGISTICS AGENCY|AIR LOGISTICS CENTER",
    re.IGNORECASE,
)


def _rows(cursor: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def resolve_capability(text: str) -> str | None:
    clean = str(text or "")
    for capability_id, definition in CAPABILITY_DEFINITIONS.items():
        if re.search(definition["request_pattern"], clean, re.IGNORECASE):
            return capability_id
    return None


class CapabilityDiscoveryStore:
    """Build a CAGE-site supplier universe from exact item and source evidence."""

    def __init__(self, data_root: Path = DEFAULT_DATA_ROOT) -> None:
        self.data_root = data_root.resolve()
        self.paths = {
            "references": self.data_root / "nsn_cage_reference.parquet",
            "locations": self.data_root / "cage_locations.parquet",
        }
        missing = [str(path) for path in self.paths.values() if not path.exists()]
        if missing:
            raise FileNotFoundError(f"capability-discovery sources are missing: {missing}")
        self.connection = duckdb.connect()
        self.connection.execute("SET preserve_insertion_order=false")
        self.connection.execute("SET threads=2")
        self.connection.execute("SET memory_limit='1GB'")
        self._cache: Dict[str, Dict[str, Any]] = {}

    def search(self, query: str) -> Dict[str, Any]:
        capability_id = resolve_capability(query)
        return {
            "query": str(query or "").strip(),
            "resolved_capability_id": capability_id,
            "resolved_capability_name": (
                CAPABILITY_DEFINITIONS[capability_id]["display_name"]
                if capability_id
                else None
            ),
        }

    def get(self, capability_id: str, limit: int = 25) -> Dict[str, Any]:
        clean_id = str(capability_id or "").strip().lower()
        definition = CAPABILITY_DEFINITIONS.get(clean_id)
        if not definition:
            raise KeyError(f"capability definition was not found: {capability_id}")
        if clean_id not in self._cache:
            self._cache[clean_id] = self._build(clean_id, definition)
        pack = self._cache[clean_id]
        bounded = min(max(int(limit), 1), 50)
        return {**pack, "supplier_sites": pack["supplier_sites"][:bounded]}

    def _build(self, capability_id: str, definition: Dict[str, Any]) -> Dict[str, Any]:
        fsc_codes = definition["fsc_codes"]
        rows = _rows(
            self.connection.execute(
                """
                WITH item_relationships AS (
                    SELECT
                        LPAD(TRIM(niin), 9, '0') AS niin,
                        MAX(nsn) AS nsn,
                        UPPER(TRIM(cage)) AS cage,
                        MAX(vendor_name) AS vendor_name,
                        MAX(description) AS description,
                        LIST_SLICE(
                            LIST_DISTINCT(LIST(part_number) FILTER (
                                WHERE part_number IS NOT NULL AND TRIM(part_number) <> ''
                            )), 1, 8
                        ) AS sample_part_numbers,
                        BOOL_OR(COALESCE(is_active_authorized_source, false))
                            AS is_active_authorized_source,
                        BOOL_OR(COALESCE(has_observed_revenue, false))
                            AS has_observed_procurement,
                        MAX(CASE WHEN COALESCE(has_observed_revenue, false)
                            THEN COALESCE(observed_spend, 0) ELSE 0 END)
                            AS observed_dla_procurement_value_usd,
                        MAX(observed_contract_count) AS observed_contract_count,
                        MIN(first_observed_year) AS first_observed_fiscal_year,
                        MAX(last_observed_year) AS last_observed_fiscal_year,
                        LIST_SLICE(
                            LIST_DISTINCT(LIST(platform_families) FILTER (
                                WHERE platform_families IS NOT NULL
                                  AND TRIM(platform_families) <> ''
                            )), 1, 12
                        ) AS platform_groups
                    FROM read_parquet(?)
                    WHERE fsc_code IN (SELECT UNNEST(?))
                      AND REGEXP_MATCHES(UPPER(COALESCE(description, '')), ?)
                      AND cage IS NOT NULL AND TRIM(cage) <> ''
                    GROUP BY 1, 3
                ), locations AS (
                    SELECT UPPER(TRIM(cage_code)) AS cage,
                           MAX(vendor_name) AS location_vendor_name,
                           MAX(city) AS city,
                           MAX(state) AS state,
                           MAX(location_quality) AS location_quality
                    FROM read_parquet(?)
                    GROUP BY 1
                )
                SELECT
                    r.cage,
                    COALESCE(MAX(r.vendor_name), MAX(l.location_vendor_name)) AS supplier_name,
                    MAX(l.city) AS city,
                    MAX(l.state) AS state,
                    MAX(l.location_quality) AS location_quality,
                    COUNT(DISTINCT r.niin) AS matching_niin_count,
                    COUNT(DISTINCT r.niin) FILTER (
                        WHERE r.is_active_authorized_source
                    ) AS active_authorized_niin_count,
                    COUNT(DISTINCT r.niin) FILTER (
                        WHERE r.has_observed_procurement
                    ) AS observed_procurement_niin_count,
                    SUM(r.observed_dla_procurement_value_usd) AS observed_dla_procurement_value_usd,
                    SUM(COALESCE(r.observed_contract_count, 0)) AS observed_contract_count,
                    MIN(r.first_observed_fiscal_year) AS first_observed_fiscal_year,
                    MAX(r.last_observed_fiscal_year) AS last_observed_fiscal_year,
                    LIST_SLICE(LIST(STRUCT_PACK(
                        niin := r.niin,
                        nsn := r.nsn,
                        description := r.description,
                        part_numbers := r.sample_part_numbers,
                        active_authorized_source := r.is_active_authorized_source,
                        observed_procurement := r.has_observed_procurement,
                        platform_groups := r.platform_groups
                    ) ORDER BY r.is_active_authorized_source DESC,
                               r.has_observed_procurement DESC,
                               r.observed_dla_procurement_value_usd DESC), 1, 10)
                        AS item_evidence,
                    COUNT(*) OVER () AS total_available
                FROM item_relationships r
                LEFT JOIN locations l USING (cage)
                WHERE (r.is_active_authorized_source OR r.has_observed_procurement)
                  AND l.state IS NOT NULL AND TRIM(l.state) <> ''
                GROUP BY r.cage
                ORDER BY active_authorized_niin_count DESC,
                         observed_procurement_niin_count DESC,
                         matching_niin_count DESC
                """,
                [
                    str(self.paths["references"]),
                    fsc_codes,
                    definition["item_pattern"],
                    str(self.paths["locations"]),
                ],
            )
        )
        commercial_rows = [
            row
            for row in rows
            if not NON_COMMERCIAL_NAME_PATTERN.search(str(row.get("supplier_name") or ""))
        ]
        matching_niins = int(
            self.connection.execute(
                """
                SELECT COUNT(DISTINCT LPAD(TRIM(niin), 9, '0'))
                FROM read_parquet(?)
                WHERE fsc_code IN (SELECT UNNEST(?))
                  AND REGEXP_MATCHES(UPPER(COALESCE(description, '')), ?)
                  AND cage IS NOT NULL AND TRIM(cage) <> ''
                """,
                [str(self.paths["references"]), fsc_codes, definition["item_pattern"]],
            ).fetchone()[0]
            or 0
        )
        for index, row in enumerate(commercial_rows, start=1):
            row["rank"] = index
            row["evidence_basis"] = [
                label
                for present, label in (
                    (row["active_authorized_niin_count"] > 0, "DLA authorized-source relationships"),
                    (row["observed_procurement_niin_count"] > 0, "observed DLA procurement"),
                    (bool(row.get("item_evidence")), "NIIN and part-number references"),
                )
                if present
            ]
        return {
            "context_type": "capability_supplier_market",
            "scope": {
                "capability_id": capability_id,
                "display_name": definition["display_name"],
                "observation_window": "FY2021-FY2026 observed procurement; current DLA source references",
                "definition": definition["scope_note"],
            },
            "supplier_sites": commercial_rows,
            "coverage": {
                "commercial_supplier_sites": len(commercial_rows),
                "supplier_sites_with_active_authorized_items": sum(
                    row["active_authorized_niin_count"] > 0 for row in commercial_rows
                ),
                "supplier_sites_with_observed_procurement": sum(
                    row["observed_procurement_niin_count"] > 0 for row in commercial_rows
                ),
                "matching_niins": matching_niins,
            },
            "ranking_basis": (
                "CAGE sites are ordered by active authorized-source NIIN count, then observed "
                "procurement NIIN count and total matching NIIN count. Financial values do not "
                "determine the ranking."
            ),
        }
