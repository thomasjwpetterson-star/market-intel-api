"""Deterministic multi-platform market-segment evidence for Ask Mimir."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List

import duckdb


DEFAULT_DATA_ROOT = Path(
    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data"
)
OBSERVATION_WINDOW = "FY2021-FY2026 observed records"

MARKET_SEGMENTS = {
    "US_MILITARY_ROTORCRAFT": {
        "display_name": "US military rotorcraft market",
        "request_pattern": (
            r"\b(?:us|u\.?s\.?)?\s*military\s+(?:rotorcraft|helicopters?)\b"
            r"|\bmilitary\s+helicopter\s+market\b"
            r"|\b(?:army|marine corps)\s+(?:rotorcraft|helicopters?)\b"
        ),
        "mapped_market_segments": ["Air"],
        "platform_pattern": r"(?:^|\b)(?:AH|CH|HH|MH|SH|UH)-|V-22|ROTORCRAFT|HELICOPTER|TILTROTOR",
        "record_pattern": r"ROTORCRAFT|HELICOPTER|TILTROTOR|BLACK HAWK|CHINOOK|APACHE|KING STALLION",
    },
    "US_AIR_AND_MISSILE_DEFENSE": {
        "display_name": "US air and missile defense market",
        "request_pattern": r"\b(?:us|u\.?s\.?)?\s*air\s+and\s+missile\s+defen[cs]e(?:\s+market)?\b|\bmissile\s+defen[cs]e\s+market\b",
        "mapped_market_segments": ["Missiles & Munitions", "Cross-Domain / Support"],
        "platform_pattern": r"PATRIOT|PAC-3|THAAD|SM-2|SM-3|SM-6|STANDARD MISSILE|AEGIS|LTAMDS|NASAMS|AIR AND MISSILE DEFENSE",
        "record_pattern": r"AIR AND MISSILE DEFEN[CS]E|MISSILE DEFEN[CS]E|PATRIOT|PAC-3|THAAD|STANDARD MISSILE|LTAMDS|NASAMS|AEGIS",
    },
    "US_C_UAS": {
        "display_name": "US counter-uncrewed-aircraft-systems market",
        "request_pattern": r"\b(?:us|u\.?s\.?)?\s*(?:c[- ]?uas|counter[- ]?(?:uas|drone)|counter\s+(?:unmanned|uncrewed)\s+(?:aircraft|aerial)\s+systems?)(?:\s+(?:market|segment))?\b",
        "mapped_market_segments": ["Air", "Ground", "Missiles & Munitions", "Cross-Domain / Support"],
        "platform_pattern": r"C[- ]?SUAS|C[- ]?UAS|COUNTER.{0,16}(?:UAS|UNMANNED|UNCREWED|DRONE)",
        "record_pattern": r"C[- ]?SUAS|C[- ]?UAS|COUNTER.{0,16}(?:UAS|UNMANNED|UNCREWED|DRONE)|LOW,? SLOW,? SMALL UNMANNED",
    },
}


def _rows(cursor: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def resolve_market_segment(text: str) -> str | None:
    clean = str(text or "")
    for segment_id, definition in MARKET_SEGMENTS.items():
        if re.search(definition["request_pattern"], clean, re.IGNORECASE):
            return segment_id
    return None


def market_segment_follow_up_intent(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(
        phrase in lowered
        for phrase in (
            "which programs",
            "which programmes",
            "driving the most activity",
            "which companies",
            "across the market",
            "most important",
            "supplier base",
            "show me the evidence",
            "supporting evidence",
            "underlying contracts",
        )
    )


class MarketSegmentStore:
    """Aggregate named mapped programs without merging unlike financial measures."""

    def __init__(self, data_root: Path = DEFAULT_DATA_ROOT) -> None:
        self.data_root = data_root.resolve()
        self.paths = {
            "transactions": self.data_root / "transactions.parquet",
            "contracts": self.data_root / "contracts_rolled.parquet",
            "network": self.data_root / "network.parquet",
            "locations": self.data_root / "cage_locations.parquet",
            "opportunities": self.data_root / "opportunities.parquet",
        }
        missing = [str(path) for path in self.paths.values() if not path.exists()]
        if missing:
            raise FileNotFoundError(f"market-segment sources are missing: {missing}")
        self.connection = duckdb.connect()
        self.connection.execute("SET preserve_insertion_order=false")
        self.connection.execute("SET threads=2")
        self.connection.execute("SET memory_limit='1GB'")
        self._cache: Dict[str, Dict[str, Any]] = {}

    def get(self, segment_id: str, limit: int = 30) -> Dict[str, Any]:
        clean_id = str(segment_id or "").strip().upper()
        definition = MARKET_SEGMENTS.get(clean_id)
        if not definition:
            raise KeyError(f"market segment was not found: {segment_id}")
        if clean_id not in self._cache:
            self._cache[clean_id] = self._build(clean_id, definition)
        pack = self._cache[clean_id]
        bounded = min(max(int(limit), 1), 75)
        return {
            **pack,
            "leading_prime_recipient_sites": pack["leading_prime_recipient_sites"][:bounded],
            "leading_reported_supplier_sites": pack["leading_reported_supplier_sites"][:bounded],
        }

    def _build(self, segment_id: str, definition: Dict[str, Any]) -> Dict[str, Any]:
        mapped_segments = definition.get("mapped_market_segments", [])
        platform_pattern = definition.get("platform_pattern", r"a^")
        record_pattern = definition.get("record_pattern", platform_pattern)
        platforms = [
            row[0]
            for row in self.connection.execute(
                """
                SELECT DISTINCT platform_family
                FROM read_parquet(?)
                WHERE platform_family IS NOT NULL
                  AND market_segment IN (SELECT UNNEST(?))
                  AND REGEXP_MATCHES(UPPER(platform_family), ?)
                ORDER BY 1
                """,
                [str(self.paths["transactions"]), mapped_segments, platform_pattern],
            ).fetchall()
        ]
        has_platforms = bool(platforms)
        platform_activity = _rows(
            self.connection.execute(
                """
                WITH prime AS (
                    SELECT CASE WHEN ? AND platform_family IN (SELECT UNNEST(?))
                                THEN platform_family ELSE ? END AS platform_family,
                           SUM(spend_amount) AS net_prime_obligations_usd,
                           SUM(CASE WHEN spend_amount > 0 THEN spend_amount ELSE 0 END)
                               AS positive_prime_obligations_usd,
                           COUNT(DISTINCT award_key) AS prime_award_count,
                           COUNT(DISTINCT vendor_cage) AS prime_recipient_site_count
                    FROM read_parquet(?)
                    WHERE source_system = 'USA_SPENDING' AND year BETWEEN 2021 AND 2026
                      AND ((? AND platform_family IN (SELECT UNNEST(?)))
                           OR REGEXP_MATCHES(UPPER(COALESCE(description, '')), ?))
                    GROUP BY 1
                ), dla AS (
                    SELECT CASE WHEN ? AND platform_family IN (SELECT UNNEST(?))
                                THEN platform_family ELSE ? END AS platform_family,
                           SUM(COALESCE(platform_attributed_spend_amount, 0))
                               AS attributed_dla_procurement_value_usd,
                           SUM(COALESCE(shared_use_exposure_amount, 0))
                               AS shared_use_niin_exposure_usd,
                           COUNT(DISTINCT niin) AS observed_dla_niin_count
                    FROM read_parquet(?)
                    WHERE source_system = 'DLA' AND year BETWEEN 2021 AND 2026
                      AND ((? AND platform_family IN (SELECT UNNEST(?)))
                           OR REGEXP_MATCHES(UPPER(COALESCE(description, '')), ?))
                    GROUP BY 1
                ), subs AS (
                    SELECT CASE WHEN ? AND platform_family IN (SELECT UNNEST(?))
                                THEN platform_family ELSE ? END AS platform_family,
                           SUM(COALESCE(subaward_value, 0))
                               AS mimir_modelled_reported_subcontract_value_usd,
                           COUNT(DISTINCT sub_cage) AS reported_supplier_site_count
                    FROM read_parquet(?)
                    WHERE year BETWEEN 2021 AND 2026
                      AND ((? AND platform_family IN (SELECT UNNEST(?)))
                           OR REGEXP_MATCHES(UPPER(COALESCE(prime_award_description, '') || ' ' || COALESCE(description, '')), ?))
                    GROUP BY 1
                )
                SELECT p.platform_family,
                       COALESCE(p.net_prime_obligations_usd, 0) AS net_prime_obligations_usd,
                       COALESCE(p.positive_prime_obligations_usd, 0) AS positive_prime_obligations_usd,
                       COALESCE(p.prime_award_count, 0) AS prime_award_count,
                       COALESCE(p.prime_recipient_site_count, 0) AS prime_recipient_site_count,
                       COALESCE(s.mimir_modelled_reported_subcontract_value_usd, 0)
                           AS mimir_modelled_reported_subcontract_value_usd,
                       COALESCE(s.reported_supplier_site_count, 0) AS reported_supplier_site_count,
                       COALESCE(d.attributed_dla_procurement_value_usd, 0)
                           AS attributed_dla_procurement_value_usd,
                       COALESCE(d.shared_use_niin_exposure_usd, 0) AS shared_use_niin_exposure_usd,
                       COALESCE(d.observed_dla_niin_count, 0) AS observed_dla_niin_count
                FROM prime p
                LEFT JOIN dla d USING (platform_family)
                LEFT JOIN subs s USING (platform_family)
                ORDER BY p.positive_prime_obligations_usd DESC
                """,
                [
                    has_platforms, platforms, f"Other directly matched {definition['display_name']} activity",
                    str(self.paths["transactions"]), has_platforms, platforms, record_pattern,
                    has_platforms, platforms, f"Other directly matched {definition['display_name']} activity",
                    str(self.paths["transactions"]), has_platforms, platforms, record_pattern,
                    has_platforms, platforms, f"Other directly matched {definition['display_name']} activity",
                    str(self.paths["network"]), has_platforms, platforms, record_pattern,
                ],
            )
        )
        prime_sites = _rows(
            self.connection.execute(
                """
                WITH locations AS (
                    SELECT UPPER(TRIM(cage_code)) AS cage, MAX(vendor_name) AS location_name,
                           MAX(city) AS city, MAX(state) AS state
                    FROM read_parquet(?) GROUP BY 1
                )
                SELECT t.vendor_cage AS cage,
                       COALESCE(MAX(t.vendor_name), MAX(l.location_name)) AS recipient_name,
                       MAX(l.city) AS city, MAX(l.state) AS state,
                       SUM(t.spend_amount) AS net_prime_obligations_usd,
                       SUM(CASE WHEN t.spend_amount > 0 THEN t.spend_amount ELSE 0 END)
                           AS positive_prime_obligations_usd,
                       COUNT(DISTINCT t.award_key) AS prime_award_count,
                       LIST_SLICE(LIST_DISTINCT(LIST(t.platform_family)), 1, 20) AS platforms,
                       COUNT(*) OVER () AS total_available
                FROM read_parquet(?) t
                LEFT JOIN locations l ON UPPER(TRIM(t.vendor_cage)) = l.cage
                WHERE t.source_system = 'USA_SPENDING' AND t.year BETWEEN 2021 AND 2026
                  AND ((? AND t.platform_family IN (SELECT UNNEST(?)))
                       OR REGEXP_MATCHES(UPPER(COALESCE(t.description, '')), ?))
                GROUP BY t.vendor_cage
                ORDER BY positive_prime_obligations_usd DESC
                LIMIT 150
                """,
                [str(self.paths["locations"]), str(self.paths["transactions"]), has_platforms, platforms, record_pattern],
            )
        )
        supplier_sites = _rows(
            self.connection.execute(
                """
                WITH locations AS (
                    SELECT UPPER(TRIM(cage_code)) AS cage, MAX(vendor_name) AS location_name,
                           MAX(city) AS city, MAX(state) AS state
                    FROM read_parquet(?) GROUP BY 1
                )
                SELECT n.sub_cage AS cage,
                       COALESCE(MAX(n.sub_name), MAX(l.location_name)) AS supplier_name,
                       MAX(COALESCE(n.sub_city, l.city)) AS city,
                       MAX(COALESCE(n.sub_state, l.state)) AS state,
                       SUM(COALESCE(n.subaward_value, 0))
                           AS mimir_modelled_reported_subcontract_value_usd,
                       COUNT(DISTINCT n.contract_id) AS prime_award_count,
                       LIST_SLICE(LIST_DISTINCT(LIST(n.platform_family)), 1, 20) AS platforms,
                       LIST_SLICE(LIST_DISTINCT(LIST(n.description) FILTER (
                           WHERE n.description IS NOT NULL AND TRIM(n.description) <> ''
                       )), 1, 8) AS reported_descriptions,
                       COUNT(*) OVER () AS total_available
                FROM read_parquet(?) n
                LEFT JOIN locations l ON UPPER(TRIM(n.sub_cage)) = l.cage
                WHERE n.year BETWEEN 2021 AND 2026
                  AND ((? AND n.platform_family IN (SELECT UNNEST(?)))
                       OR REGEXP_MATCHES(UPPER(COALESCE(n.prime_award_description, '') || ' ' || COALESCE(n.description, '')), ?))
                  AND n.sub_cage IS NOT NULL
                  AND UPPER(TRIM(n.sub_cage)) NOT IN ('', 'UNKNOWN', 'UNKNO')
                GROUP BY n.sub_cage
                HAVING SUM(COALESCE(n.subaward_value, 0)) <> 0
                ORDER BY ABS(mimir_modelled_reported_subcontract_value_usd) DESC
                LIMIT 250
                """,
                [str(self.paths["locations"]), str(self.paths["network"]), has_platforms, platforms, record_pattern],
            )
        )
        leading_awards = _rows(
            self.connection.execute(
                """
                SELECT contract_id, vendor_name, vendor_cage,
                       COALESCE(base_award_description, description) AS description,
                       platform_family, total_spend AS net_prime_obligations_usd,
                       last_action_date
                FROM read_parquet(?)
                WHERE source_system = 'USA_SPENDING' AND year BETWEEN 2021 AND 2026
                  AND ((? AND platform_family IN (SELECT UNNEST(?)))
                       OR REGEXP_MATCHES(UPPER(COALESCE(base_award_description, description, '')), ?))
                ORDER BY ABS(total_spend) DESC
                LIMIT 75
                """,
                [str(self.paths["contracts"]), has_platforms, platforms, record_pattern],
            )
        )
        current_opportunities = _rows(
            self.connection.execute(
                """
                SELECT id, sol_num, title, agency, sub_agency, deadline, psc, naics, url
                FROM read_parquet(?)
                WHERE SUBSTR(COALESCE(deadline, ''), 1, 10) >= CAST(CURRENT_DATE AS VARCHAR)
                  AND REGEXP_MATCHES(UPPER(COALESCE(search_text, title, '')), ?)
                ORDER BY SUBSTR(deadline, 1, 10), title
                LIMIT 50
                """,
                [str(self.paths["opportunities"]), record_pattern],
            )
        )
        return {
            "context_type": "market_segment_dossier",
            "scope": {
                "segment_id": segment_id,
                "display_name": definition["display_name"],
                "included_platforms": platforms,
                "mapped_market_segments": mapped_segments,
                "scope_basis": "Existing mapped market segments plus directly matched platform, award, subcontract and opportunity evidence.",
                "observation_window": OBSERVATION_WINDOW,
            },
            "platform_activity": platform_activity,
            "leading_prime_recipient_sites": prime_sites,
            "leading_reported_supplier_sites": supplier_sites,
            "leading_awards": leading_awards,
            "current_opportunities": current_opportunities,
            "coverage": {
                "defined_platform_count": len(platforms),
                "platforms_with_prime_activity": len(platform_activity),
                "prime_recipient_sites": int(prime_sites[0].get("total_available") or 0)
                    if prime_sites else 0,
                "reported_supplier_sites": int(supplier_sites[0].get("total_available") or 0)
                    if supplier_sites else 0,
                "directly_matched_awards": len(leading_awards),
                "current_opportunities": len(current_opportunities),
            },
            "methodology": {
                "scope_rule": "Programs are selected from Mimir's mapped taxonomy and supplemented by directly matching public records.",
                "financial_rule": "Prime obligations, reported subcontract value and DLA procurement remain separate measures.",
            },
        }
