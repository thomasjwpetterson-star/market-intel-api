"""State-level defense-industrial-base evidence for Ask Mimir."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List

import duckdb


DEFAULT_DATA_ROOT = Path(
    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data"
)

US_STATES = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR",
    "CALIFORNIA": "CA", "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE",
    "FLORIDA": "FL", "GEORGIA": "GA", "HAWAII": "HI", "IDAHO": "ID",
    "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA", "KANSAS": "KS",
    "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD",
    "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN",
    "MISSISSIPPI": "MS", "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE",
    "NEVADA": "NV", "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM", "NEW YORK": "NY", "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK", "OREGON": "OR",
    "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT",
    "VERMONT": "VT", "VIRGINIA": "VA", "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV", "WISCONSIN": "WI", "WYOMING": "WY",
    "DISTRICT OF COLUMBIA": "DC",
}
STATE_NAMES = {code: name.title() for name, code in US_STATES.items()}


def _rows(cursor: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def resolve_state(text: str) -> str | None:
    normalized = re.sub(r"[^A-Z ]", " ", str(text or "").upper())
    normalized = " ".join(normalized.split())
    for state_name, state_code in sorted(US_STATES.items(), key=lambda item: -len(item[0])):
        if re.search(rf"\b{re.escape(state_name)}\b", normalized):
            return state_code
    match = re.search(r"\b(?:IN|WITHIN|ACROSS)\s+([A-Z]{2})\b", normalized)
    if match and match.group(1) in STATE_NAMES:
        return match.group(1)
    return None


def is_geographic_market_request(text: str) -> bool:
    lowered = str(text or "").lower()
    company_site_request = bool(
        re.search(
            r"\bassociated\s+with\b.+?(?:['’]s|s)\s+.+?\b(?:operations|site|facility)\b",
            lowered,
        )
    )
    if company_site_request:
        return False
    explicit_phrasing = any(
        phrase in lowered
        for phrase in (
            "industrial base", "defence companies", "defense companies",
            "defence facilities", "defense facilities", "companies and facilities",
            "defence contractors", "defense contractors", "military contractors",
            "defence suppliers", "defense suppliers", "military suppliers",
            "defence firms", "defense firms", "military firms",
            "defence footprint", "defense footprint",
            "defence activity", "defense activity",
            "important in the state", "state's defense", "state's defence",
            "military industry", "defence industry", "defense industry",
        )
    )
    entity_phrasing = (
        any(term in lowered for term in ("defence", "defense", "military"))
        and any(term in lowered for term in ("companies", "contractors", "firms", "facilities", "platforms", "programs", "programmes"))
        and any(term in lowered for term in ("important", "largest", "leading", "activity", "map", "matter most"))
    )
    return resolve_state(text) is not None and (explicit_phrasing or entity_phrasing)


def state_market_follow_up_intent(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(
        phrase in lowered
        for phrase in (
            "which defence companies", "which defense companies",
            "which facilities", "most important in the state",
            "largest companies", "largest facilities", "leading companies",
            "leading facilities", "what are the main programs",
            "what programmes", "what programs", "show me the figures",
            "show the evidence", "why are they important",
            "which platforms and capability areas",
            "which programmes and capability areas",
            "most visible activity",
        )
    )


class StateIndustrialBaseStore:
    """Rank registered facilities and in-state work using separate value lanes."""

    def __init__(self, data_root: Path = DEFAULT_DATA_ROOT) -> None:
        self.data_root = data_root.resolve()
        self.paths = {
            "transactions": self.data_root / "transactions.parquet",
            "network": self.data_root / "network.parquet",
            "locations": self.data_root / "cage_locations.parquet",
        }
        missing = [str(path) for path in self.paths.values() if not path.exists()]
        if missing:
            raise FileNotFoundError(f"state-market sources are missing: {missing}")
        self.connection = duckdb.connect()
        self.connection.execute("SET preserve_insertion_order=false")
        self.connection.execute("SET threads=2")
        self.connection.execute("SET memory_limit='1GB'")
        self._cache: Dict[str, Dict[str, Any]] = {}

    def get(self, state_code: str, limit: int = 30) -> Dict[str, Any]:
        code = str(state_code or "").strip().upper()
        if code not in STATE_NAMES:
            raise KeyError(f"US state code was not recognized: {state_code}")
        if code not in self._cache:
            self._cache[code] = self._build(code)
        pack = self._cache[code]
        bounded = min(max(int(limit), 1), 75)
        return {
            **pack,
            "ranked_registered_facilities": pack["ranked_registered_facilities"][:bounded],
            "leading_places_of_performance": pack["leading_places_of_performance"][:bounded],
        }

    def _build(self, state_code: str) -> Dict[str, Any]:
        state_name = STATE_NAMES[state_code]
        facilities = _rows(
            self.connection.execute(
                """
                WITH locations AS (
                    SELECT UPPER(TRIM(cage_code)) AS cage,
                           MAX(vendor_name) AS vendor_name,
                           MAX(city) AS city,
                           MAX(state) AS state,
                           MAX(location_quality) AS location_quality
                    FROM read_parquet(?)
                    WHERE UPPER(TRIM(state)) IN (?, ?)
                    GROUP BY 1
                ), prime AS (
                    SELECT UPPER(TRIM(vendor_cage)) AS cage,
                           SUM(spend_amount) AS net_prime_obligations_usd,
                           SUM(CASE WHEN spend_amount > 0 THEN spend_amount ELSE 0 END)
                               AS positive_prime_obligations_usd,
                           COUNT(DISTINCT contract_id) AS prime_award_count,
                           LIST_SLICE(LIST_DISTINCT(LIST(platform_family) FILTER (
                               WHERE platform_family IS NOT NULL
                                 AND UPPER(TRIM(platform_family)) NOT IN ('', 'UNMAPPED', 'REVIEW NEEDED')
                           )), 1, 15) AS prime_platforms
                    FROM read_parquet(?)
                    WHERE source_system = 'USA_SPENDING' AND year BETWEEN 2021 AND 2026
                      AND UPPER(TRIM(vendor_cage)) IN (SELECT cage FROM locations)
                    GROUP BY 1
                ), dla AS (
                    SELECT UPPER(TRIM(vendor_cage)) AS cage,
                           SUM(spend_amount) AS dla_procurement_value_usd,
                           COUNT(DISTINCT niin) AS dla_niin_count,
                           COUNT(DISTINCT contract_id) AS dla_contract_count,
                           LIST_SLICE(LIST_DISTINCT(LIST(platform_family) FILTER (
                               WHERE platform_family IS NOT NULL
                                 AND UPPER(TRIM(platform_family)) NOT IN ('', 'UNMAPPED', 'REVIEW NEEDED')
                           )), 1, 15) AS dla_platforms
                    FROM read_parquet(?)
                    WHERE source_system = 'DLA' AND year BETWEEN 2021 AND 2026
                      AND UPPER(TRIM(vendor_cage)) IN (SELECT cage FROM locations)
                    GROUP BY 1
                ), subcontracts AS (
                    SELECT UPPER(TRIM(sub_cage)) AS cage,
                           SUM(subaward_value) AS modelled_reported_subcontract_value_usd,
                           COUNT(DISTINCT contract_id) AS prime_customer_award_count,
                           LIST_SLICE(LIST_DISTINCT(LIST(prime_name) FILTER (
                               WHERE prime_name IS NOT NULL AND TRIM(prime_name) <> ''
                           )), 1, 12) AS reported_prime_customers,
                           LIST_SLICE(LIST_DISTINCT(LIST(platform_family) FILTER (
                               WHERE platform_family IS NOT NULL
                                 AND UPPER(TRIM(platform_family)) NOT IN ('', 'UNMAPPED', 'REVIEW NEEDED')
                           )), 1, 15) AS subcontract_platforms
                    FROM read_parquet(?)
                    WHERE year BETWEEN 2021 AND 2026
                      AND UPPER(TRIM(sub_cage)) IN (SELECT cage FROM locations)
                    GROUP BY 1
                )
                SELECT
                    l.cage, l.vendor_name, l.city, l.state, l.location_quality,
                    COALESCE(p.net_prime_obligations_usd, 0) AS net_prime_obligations_usd,
                    COALESCE(p.positive_prime_obligations_usd, 0) AS positive_prime_obligations_usd,
                    COALESCE(p.prime_award_count, 0) AS prime_award_count,
                    COALESCE(d.dla_procurement_value_usd, 0) AS dla_procurement_value_usd,
                    COALESCE(d.dla_niin_count, 0) AS dla_niin_count,
                    COALESCE(d.dla_contract_count, 0) AS dla_contract_count,
                    COALESCE(s.modelled_reported_subcontract_value_usd, 0)
                        AS modelled_reported_subcontract_value_usd,
                    COALESCE(s.prime_customer_award_count, 0) AS prime_customer_award_count,
                    p.prime_platforms, d.dla_platforms,
                    s.reported_prime_customers, s.subcontract_platforms
                FROM locations l
                LEFT JOIN prime p USING (cage)
                LEFT JOIN dla d USING (cage)
                LEFT JOIN subcontracts s USING (cage)
                WHERE COALESCE(p.prime_award_count, 0) > 0
                   OR COALESCE(d.dla_contract_count, 0) > 0
                   OR COALESCE(s.prime_customer_award_count, 0) > 0
                """,
                [
                    str(self.paths["locations"]), state_code, state_name.upper(),
                    str(self.paths["transactions"]),
                    str(self.paths["transactions"]),
                    str(self.paths["network"]),
                ],
            )
        )
        for row in facilities:
            row["largest_observed_lane_usd"] = max(
                float(row.get("positive_prime_obligations_usd") or 0),
                float(row.get("dla_procurement_value_usd") or 0),
                max(float(row.get("modelled_reported_subcontract_value_usd") or 0), 0),
            )
        facilities.sort(
            key=lambda row: (-row["largest_observed_lane_usd"], row["vendor_name"], row["cage"])
        )
        for index, row in enumerate(facilities, start=1):
            row["rank"] = index

        performance = _rows(
            self.connection.execute(
                """
                SELECT UPPER(TRIM(vendor_cage)) AS cage,
                       MAX(vendor_name) AS recipient_name,
                       MAX(place_of_performance_city) AS place_of_performance_city,
                       SUM(spend_amount) AS net_prime_obligations_usd,
                       COUNT(DISTINCT contract_id) AS prime_award_count,
                       LIST_SLICE(LIST_DISTINCT(LIST(platform_family) FILTER (
                           WHERE platform_family IS NOT NULL
                             AND UPPER(TRIM(platform_family)) NOT IN ('', 'UNMAPPED', 'REVIEW NEEDED')
                       )), 1, 15) AS mapped_platforms
                FROM read_parquet(?)
                WHERE source_system = 'USA_SPENDING' AND year BETWEEN 2021 AND 2026
                  AND UPPER(TRIM(place_of_performance_state)) IN (?, ?)
                GROUP BY 1
                ORDER BY net_prime_obligations_usd DESC
                LIMIT 75
                """,
                [str(self.paths["transactions"]), state_code, state_name.upper()],
            )
        )
        return {
            "context_type": "state_defense_industrial_base",
            "scope": {
                "state_code": state_code,
                "state_name": state_name,
                "observation_window": "FY2021-FY2026 observed records",
            },
            "ranked_registered_facilities": facilities,
            "leading_places_of_performance": performance,
            "coverage": {
                "registered_facilities_with_observed_activity": len(facilities),
                "prime_recipient_facilities": sum(row["prime_award_count"] > 0 for row in facilities),
                "dla_recipient_facilities": sum(row["dla_contract_count"] > 0 for row in facilities),
                "reported_subcontractor_facilities": sum(
                    row["prime_customer_award_count"] > 0 for row in facilities
                ),
                "place_of_performance_recipients": len(performance),
            },
            "state_totals": {
                "net_prime_obligations_at_registered_facilities_usd": sum(
                    float(row["net_prime_obligations_usd"]) for row in facilities
                ),
                "dla_procurement_value_at_registered_facilities_usd": sum(
                    float(row["dla_procurement_value_usd"]) for row in facilities
                ),
                "modelled_reported_subcontract_value_at_registered_facilities_usd": sum(
                    float(row["modelled_reported_subcontract_value_usd"]) for row in facilities
                ),
                "net_prime_obligations_performed_in_state_usd": sum(
                    float(row["net_prime_obligations_usd"]) for row in performance
                ),
            },
            "ranking_basis": (
                "Facilities are ordered by their largest observed financial lane. Prime obligations, "
                "DLA procurement and reported subcontract value remain separately labelled."
            ),
        }
