"""Deterministic named-market competitive-position evidence packs."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import duckdb


ROOT = Path(__file__).resolve().parent
DEFAULT_DATA_ROOT = Path(
    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data"
)


def _rows(cursor: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _clean_list(value: Any) -> List[str]:
    return sorted({str(item) for item in (value or []) if item not in (None, "")})


def rank_records(
    records: List[Dict[str, Any]],
    *,
    value_field: str,
    platform_field: str = "platforms",
) -> List[Dict[str, Any]]:
    """Rank within one evidence lane without calling the result market share."""
    if not records:
        return []
    max_value = max(abs(float(row.get(value_field) or 0)) for row in records) or 1.0
    max_platforms = max(len(row.get(platform_field) or []) for row in records) or 1
    max_awards = max(int(row.get("award_count") or row.get("contract_count") or 0) for row in records) or 1
    for row in records:
        platforms = _clean_list(row.get(platform_field))
        years = _clean_list(row.get("fiscal_years"))
        awards = int(row.get("award_count") or row.get("contract_count") or 0)
        value = abs(float(row.get(value_field) or 0))
        value_component = math.log1p(value) / math.log1p(max_value)
        score = (
            35 * len(platforms) / max_platforms
            + 20 * len(years) / 5
            + 15 * awards / max_awards
            + 30 * value_component
        )
        row["platforms"] = platforms
        row["fiscal_years"] = [int(year) for year in years]
        row["observed_position_score"] = round(score, 2)
        row["score_components"] = {
            "platform_breadth_points": round(35 * len(platforms) / max_platforms, 2),
            "completed_year_persistence_points": round(20 * len(years) / 5, 2),
            "award_breadth_points": round(15 * awards / max_awards, 2),
            "within_lane_value_points": round(30 * value_component, 2),
        }
    records.sort(
        key=lambda row: (
            -float(row["observed_position_score"]),
            -abs(float(row.get(value_field) or 0)),
            str(row.get("supplier_name") or row.get("cage") or ""),
        )
    )
    for index, row in enumerate(records, start=1):
        row["rank"] = index
    return records


class CompetitivePositionStore:
    def __init__(
        self,
        data_root: Path = DEFAULT_DATA_ROOT,
        definitions_path: Path = ROOT / "competitive_position_definitions.json",
    ) -> None:
        self.data_root = data_root.resolve()
        self.definitions_path = definitions_path.resolve()
        self.definitions = json.loads(self.definitions_path.read_text())
        self.paths = {
            "transactions": self.data_root / "transactions.parquet",
            "network": self.data_root / "network.parquet",
            "item_suppliers": self.data_root / "nsn_supplier_lookup.parquet",
            "item_profiles": self.data_root / "nsn_profile_lookup.parquet",
            "locations": self.data_root / "cage_locations.parquet",
        }
        missing = [str(path) for path in self.paths.values() if not path.exists()]
        if missing:
            raise FileNotFoundError(f"competitive-position sources are missing: {missing}")
        self.connection = duckdb.connect()
        self.connection.execute("SET preserve_insertion_order=false")
        self.connection.execute("SET threads=2")
        self.connection.execute("SET memory_limit='1GB'")
        self._cache: Dict[str, Dict[str, Any]] = {}

    def get(self, market_id: str = "army_ground_vehicle_power", limit: int = 15) -> Dict[str, Any]:
        clean_id = str(market_id).strip().lower()
        definition = self.definitions["markets"].get(clean_id)
        if not definition:
            raise KeyError(f"defined market was not found: {market_id}")
        if clean_id not in self._cache:
            self._cache[clean_id] = self._build(clean_id, definition)
        pack = self._cache[clean_id]
        bounded = min(max(int(limit), 1), 30)
        return {
            **pack,
            "reported_supply_chain_position": pack["reported_supply_chain_position"][:bounded],
            "dla_item_procurement_position": pack["dla_item_procurement_position"][:bounded],
            "direct_award_position": pack["direct_award_position"][:bounded],
        }

    def _platform_sql(self, definition: Dict[str, Any]) -> str:
        return ",".join("?" for _ in definition["platforms"])

    def _locations_cte(self) -> str:
        return """
            locations AS (
                SELECT UPPER(TRIM(cage_code)) cage,
                       MAX(vendor_name) location_vendor_name,
                       MAX(city) city, MAX(state) state,
                       MAX(location_quality) location_quality
                FROM read_parquet(?)
                GROUP BY 1
            )
        """

    def _reported_supply_chain(self, definition: Dict[str, Any]) -> List[Dict[str, Any]]:
        placeholders = self._platform_sql(definition)
        rows = _rows(
            self.connection.execute(
                f"""
                WITH {self._locations_cte()}, matched AS (
                    SELECT n.*,
                           CASE
                             WHEN REGEXP_MATCHES(UPPER(COALESCE(n.description,'')), ?) THEN 'SPECIFIC_POWER_TERM'
                             ELSE 'BROADER_ELECTRICAL_CATEGORY'
                           END capability_match_precision
                    FROM read_parquet(?) n
                    WHERE n.year BETWEEN 2021 AND 2025
                      AND n.platform_family IN ({placeholders})
                      AND n.sub_cage IS NOT NULL
                      AND UPPER(TRIM(n.sub_cage)) NOT IN ('', 'UNKNOWN', 'UNKNO')
                      AND (
                        REGEXP_MATCHES(UPPER(COALESCE(n.description,'')), ?)
                        OR REGEXP_MATCHES(UPPER(COALESCE(n.description,'')), ?)
                      )
                )
                SELECT
                    m.sub_cage cage,
                    COALESCE(MAX(m.sub_name), MAX(l.location_vendor_name)) supplier_name,
                    MAX(COALESCE(m.sub_city,l.city)) city,
                    MAX(COALESCE(m.sub_state,l.state)) state,
                    MAX(l.location_quality) location_quality,
                    LIST_DISTINCT(LIST(m.platform_family)) platforms,
                    LIST_DISTINCT(LIST(m.year)) fiscal_years,
                    LIST_DISTINCT(LIST(m.capability_match_precision)) match_precision,
                    LIST_SLICE(LIST_DISTINCT(LIST(m.description)),1,8) evidence_descriptions,
                    LIST_SLICE(LIST_DISTINCT(LIST(m.prime_name)),1,8) reported_prime_customers,
                    LIST_SLICE(LIST_DISTINCT(LIST(m.contract_id)),1,8) sample_contract_ids,
                    COUNT(DISTINCT m.contract_id) award_count,
                    COUNT(*) selected_report_count,
                    SUM(COALESCE(m.subaward_value,0)) mimir_modelled_reported_subcontract_value_usd,
                    SUM(COALESCE(m.subaward_value_raw,0)) source_reported_value_usd
                FROM matched m
                LEFT JOIN locations l ON UPPER(TRIM(m.sub_cage))=l.cage
                GROUP BY m.sub_cage
                """,
                [
                    str(self.paths["locations"]),
                    definition["strict_capability_pattern"],
                    str(self.paths["network"]),
                    *definition["platforms"],
                    definition["strict_capability_pattern"],
                    definition["broader_reported_pattern"],
                ],
            )
        )
        return rank_records(
            rows, value_field="mimir_modelled_reported_subcontract_value_usd"
        )

    def _reported_platform_coverage(self, definition: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Describe source coverage before the capability-description filter is applied."""
        placeholders = self._platform_sql(definition)
        return _rows(
            self.connection.execute(
                f"""
                SELECT
                    platform_family,
                    COUNT(*) selected_report_count,
                    COUNT(DISTINCT sub_cage) resolved_supplier_site_count,
                    COUNT(DISTINCT contract_id) prime_award_count,
                    SUM(COALESCE(subaward_value, 0))
                        mimir_modelled_reported_subcontract_value_usd
                FROM read_parquet(?)
                WHERE year BETWEEN 2021 AND 2025
                  AND platform_family IN ({placeholders})
                  AND sub_cage IS NOT NULL
                  AND UPPER(TRIM(sub_cage)) NOT IN ('', 'UNKNOWN', 'UNKNO')
                GROUP BY platform_family
                ORDER BY platform_family
                """,
                [str(self.paths["network"]), *definition["platforms"]],
            )
        )

    def _dla_item_procurement(self, definition: Dict[str, Any]) -> List[Dict[str, Any]]:
        placeholders = self._platform_sql(definition)
        rows = _rows(
            self.connection.execute(
                f"""
                WITH {self._locations_cte()}, profiles AS (
                    SELECT niin, MAX(item_name) description, MAX(nsn) nsn
                    FROM read_parquet(?) GROUP BY 1
                ), matched AS (
                    SELECT s.*, p.description, p.nsn,
                           CASE WHEN COALESCE(s.platform_count,1) > 1
                                THEN COALESCE(s.total_revenue,0) ELSE 0 END shared_use_value,
                           CASE WHEN COALESCE(s.platform_count,1) = 1
                                THEN COALESCE(s.total_revenue,0) ELSE 0 END attributed_value
                    FROM read_parquet(?) s
                    JOIN profiles p USING(niin)
                    WHERE s.year BETWEEN 2021 AND 2025
                      AND s.platform_family IN ({placeholders})
                      AND REGEXP_MATCHES(UPPER(COALESCE(p.description,'')), ?)
                )
                SELECT
                    m.cage,
                    COALESCE(MAX(m.vendor), MAX(l.location_vendor_name)) supplier_name,
                    MAX(l.city) city, MAX(l.state) state,
                    MAX(l.location_quality) location_quality,
                    LIST_DISTINCT(LIST(m.platform_family)) platforms,
                    LIST_DISTINCT(LIST(m.year)) fiscal_years,
                    LIST_SLICE(LIST_DISTINCT(LIST(m.description)),1,8) item_descriptions,
                    LIST_SLICE(LIST_DISTINCT(LIST(m.nsn)),1,8) sample_nsns,
                    LIST_SLICE(LIST_DISTINCT(LIST(m.contract_id)),1,8) sample_contract_ids,
                    COUNT(DISTINCT m.contract_id) contract_count,
                    COUNT(DISTINCT m.niin) niin_count,
                    SUM(m.attributed_value) attributed_dla_procurement_value_usd,
                    SUM(m.shared_use_value) shared_use_niin_exposure_usd
                FROM matched m
                LEFT JOIN locations l ON UPPER(TRIM(m.cage))=l.cage
                WHERE m.cage IS NOT NULL AND TRIM(m.cage) <> ''
                GROUP BY m.cage
                """,
                [
                    str(self.paths["locations"]),
                    str(self.paths["item_profiles"]),
                    str(self.paths["item_suppliers"]),
                    *definition["platforms"],
                    definition["strict_capability_pattern"],
                ],
            )
        )
        return rank_records(rows, value_field="attributed_dla_procurement_value_usd")

    def _direct_awards(self, definition: Dict[str, Any]) -> List[Dict[str, Any]]:
        placeholders = self._platform_sql(definition)
        rows = _rows(
            self.connection.execute(
                f"""
                WITH {self._locations_cte()}, matched AS (
                    SELECT * FROM read_parquet(?)
                    WHERE source_system='USA_SPENDING' AND year BETWEEN 2021 AND 2025
                      AND platform_family IN ({placeholders})
                      AND REGEXP_MATCHES(
                        UPPER(CONCAT_WS(' ',base_award_description,action_description,description)), ?
                      )
                )
                SELECT
                    m.vendor_cage cage,
                    COALESCE(MAX(m.vendor_name), MAX(l.location_vendor_name)) supplier_name,
                    MAX(l.city) city, MAX(l.state) state,
                    MAX(l.location_quality) location_quality,
                    LIST_DISTINCT(LIST(m.platform_family)) platforms,
                    LIST_DISTINCT(LIST(m.year)) fiscal_years,
                    LIST_SLICE(LIST_DISTINCT(LIST(m.base_award_description)),1,8) evidence_descriptions,
                    LIST_SLICE(LIST_DISTINCT(LIST(m.contract_id)),1,8) sample_contract_ids,
                    COUNT(DISTINCT m.award_key) award_count,
                    COUNT(DISTINCT m.transaction_key) action_count,
                    SUM(m.spend_amount) net_prime_obligations_usd
                FROM matched m
                LEFT JOIN locations l ON UPPER(TRIM(m.vendor_cage))=l.cage
                WHERE m.vendor_cage IS NOT NULL AND TRIM(m.vendor_cage) <> ''
                GROUP BY m.vendor_cage
                """,
                [
                    str(self.paths["locations"]),
                    str(self.paths["transactions"]),
                    *definition["platforms"],
                    definition["strict_capability_pattern"],
                ],
            )
        )
        return rank_records(rows, value_field="net_prime_obligations_usd")

    def _build(self, market_id: str, definition: Dict[str, Any]) -> Dict[str, Any]:
        reported_platform_coverage = self._reported_platform_coverage(definition)
        reported = self._reported_supply_chain(definition)
        dla = self._dla_item_procurement(definition)
        direct = self._direct_awards(definition)
        represented = {
            "reported_supply_chain": sorted(
                {platform for row in reported for platform in row.get("platforms", [])}
            ),
            "dla_item_procurement": sorted(
                {platform for row in dla for platform in row.get("platforms", [])}
            ),
            "direct_awards": sorted(
                {platform for row in direct for platform in row.get("platforms", [])}
            ),
        }
        return {
            "context_type": "defined_market_competitive_position",
            "calculation_version": "mimir-competitive-position-2026-09-v1",
            "definition_version": self.definitions["definition_version"],
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "scope": {
                "market_id": market_id,
                "display_name": definition["display_name"],
                "platform_universe": definition["platforms"],
                "completed_fiscal_years": definition["completed_fiscal_years"],
                "scope_note": definition["scope_note"],
            },
            "reported_supply_chain_position": reported,
            "dla_item_procurement_position": dla,
            "direct_award_position": direct,
            "coverage": {
                "platforms_represented_by_lane": represented,
                "reported_relationship_platforms": [
                    row["platform_family"] for row in reported_platform_coverage
                ],
                "reported_relationships_by_platform": reported_platform_coverage,
                "reported_power_description_platforms": represented[
                    "reported_supply_chain"
                ],
                "reported_supplier_sites": len(reported),
                "dla_recipient_sites": len(dla),
                "direct_recipient_sites": len(direct),
            },
            "methodology": {
                "ranking_rule": "Ranked separately within each evidence lane using platform breadth, completed-year persistence, award breadth and log-scaled observed value.",
                "market_share_rule": "The result is an observed-position ranking inside the declared evidence universe, not a market-share estimate.",
                "lane_rule": "Reported subcontract value, DLA item procurement and prime obligations are not added together.",
                "capability_rule": "Platform relationship coverage is measured before the description filter. Strict power-system terms are then distinguished from broader source-reported electrical categories; a platform absent from the filtered lane is not absent from Mimir.",
                "site_rule": "Rows are CAGE-site specific; no unreviewed corporate-parent roll-up is applied.",
            },
        }
