"""Universal platform and program evidence dossiers for Ask Mimir."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import duckdb


DEFAULT_DATA_ROOT = Path(
    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data"
)
COMPLETED_FISCAL_YEARS = tuple(range(2021, 2026))
OBSERVATION_WINDOW = "FY2021-FY2026 year to date"
EVIDENCE_EXPORT_ROW_LIMIT = 5000


def _rows(cursor: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _normalize(value: Any) -> str:
    return " ".join(re.findall(r"[A-Z0-9]+", str(value or "").upper()))


def _date(value: Any) -> str | None:
    text = str(value or "").strip()
    return text[:10] if text else None


class PlatformContextStore:
    """Build a common evidence baseline for any mapped platform or program."""

    def __init__(self, data_root: Path = DEFAULT_DATA_ROOT) -> None:
        self.data_root = data_root.resolve()
        self.paths = {
            name: self.data_root / filename
            for name, filename in {
                "transactions": "transactions.parquet",
                "network": "network.parquet",
                "platform_bom": "platform_bom.parquet",
                "item_profiles": "nsn_profile_lookup.parquet",
                "item_suppliers": "nsn_supplier_lookup.parquet",
                "locations": "cage_locations.parquet",
                "opportunities": "opportunities.parquet",
            }.items()
        }
        missing = [str(path) for path in self.paths.values() if not path.exists()]
        if missing:
            raise FileNotFoundError(f"platform context sources are missing: {missing}")
        self.connection = duckdb.connect()
        self.connection.execute("SET preserve_insertion_order=false")
        self.connection.execute("SET threads=2")
        self.connection.execute("SET memory_limit='1GB'")
        self.platforms = self._load_platform_catalog()
        self._cache: Dict[str, Dict[str, Any]] = {}

    def _load_platform_catalog(self) -> List[str]:
        rows = self.connection.execute(
            """
            SELECT DISTINCT TRIM(platform_family) AS platform_family
            FROM read_parquet(?)
            WHERE platform_family IS NOT NULL AND TRIM(platform_family) <> ''
              AND UPPER(TRIM(platform_family)) NOT IN ('UNMAPPED', 'REVIEW NEEDED')
            UNION
            SELECT DISTINCT TRIM(platform_family)
            FROM read_parquet(?)
            WHERE platform_family IS NOT NULL AND TRIM(platform_family) <> ''
              AND UPPER(TRIM(platform_family)) NOT IN ('UNMAPPED', 'REVIEW NEEDED')
            UNION
            SELECT DISTINCT TRIM(platform_family)
            FROM read_parquet(?)
            WHERE platform_family IS NOT NULL AND TRIM(platform_family) <> ''
              AND UPPER(TRIM(platform_family)) NOT IN ('UNMAPPED', 'REVIEW NEEDED')
            ORDER BY 1
            """,
            [
                str(self.paths["platform_bom"]),
                str(self.paths["transactions"]),
                str(self.paths["network"]),
            ],
        ).fetchall()
        catalog: Dict[str, str] = {}
        for row in rows:
            platform = row[0]
            normalized = _normalize(platform)
            current = catalog.get(normalized)
            if current is None or (platform == platform.upper() and current != current.upper()):
                catalog[normalized] = platform
        return sorted(catalog.values(), key=_normalize)

    def search(self, query: str, limit: int = 15) -> Dict[str, Any]:
        clean = str(query or "").strip()
        normalized = _normalize(clean)
        if not normalized:
            return {"query": clean, "matches": [], "requires_disambiguation": False}
        exact = [platform for platform in self.platforms if _normalize(platform) == normalized]
        aliases = {
            "GMLRS": "GMLRS/GMLRS AW",
            "MLRS": "GMLRS/GMLRS AW",
            "TRIDENT II": "TRIDENT II MISSILE",
            "TOMAHAWK": "TACTOM (TACTICAL TOMAHAWK)",
            "ABRAMS": "M1 ABRAMS",
            "VIRGINIA CLASS": "VIRGINIA CLASS (SSN 774)",
            "DDG 51": "DDG-51 ARLEIGH BURKE",
            "FORD CLASS": "FORD CLASS CARRIER",
            "COLUMBIA": "COLUMBIA CLASS SSBN",
            "COLUMBIA CLASS": "COLUMBIA CLASS SSBN",
            "COLOMBIA": "COLUMBIA CLASS SSBN",
            "COLOMBIA CLASS": "COLUMBIA CLASS SSBN",
        }
        if not exact and normalized in aliases and aliases[normalized] in self.platforms:
            exact = [aliases[normalized]]
        if exact:
            matches = exact
            match_type = "EXACT"
        else:
            matches = [
                platform
                for platform in self.platforms
                if normalized in _normalize(platform) or _normalize(platform) in normalized
            ][: min(max(int(limit), 1), 20)]
            match_type = "TEXT"
        return {
            "query": clean,
            "match_type": match_type,
            "matches": [
                {
                    "platform_id": platform,
                    "display_name": platform,
                    "option_label": platform,
                }
                for platform in matches
            ],
            "requires_disambiguation": len(matches) > 1,
            "resolved_platform_id": matches[0] if len(matches) == 1 else None,
        }

    def mentions(self, text: str) -> List[str]:
        normalized = f" {_normalize(text)} "
        matches = []
        for platform in self.platforms:
            candidate = _normalize(platform)
            if len(candidate) >= 3 and f" {candidate} " in normalized:
                matches.append(platform)
        aliases = {
            "GMLRS": "GMLRS/GMLRS AW",
            "TRIDENT II": "TRIDENT II MISSILE",
            "TOMAHAWK": "TACTOM (TACTICAL TOMAHAWK)",
            "ABRAMS": "M1 ABRAMS",
            "VIRGINIA CLASS": "VIRGINIA CLASS (SSN 774)",
            "DDG 51": "DDG-51 ARLEIGH BURKE",
            "FORD CLASS": "FORD CLASS CARRIER",
            "COLUMBIA": "COLUMBIA CLASS SSBN",
            "COLUMBIA CLASS": "COLUMBIA CLASS SSBN",
            "COLOMBIA": "COLUMBIA CLASS SSBN",
            "COLOMBIA CLASS": "COLUMBIA CLASS SSBN",
        }
        for alias, platform in aliases.items():
            if f" {alias} " in normalized and platform in self.platforms:
                matches.append(platform)
        return sorted(set(matches), key=len, reverse=True)

    def get(self, platform_id: str) -> Dict[str, Any]:
        resolution = self.search(platform_id)
        resolved = resolution.get("resolved_platform_id")
        if not resolved:
            if resolution.get("requires_disambiguation"):
                raise ValueError(f"platform identifier is ambiguous: {platform_id}")
            raise KeyError(f"platform was not found: {platform_id}")
        if resolved in self._cache:
            return self._cache[resolved]

        annual = self._annual_activity(resolved)
        direct_recipients = self._direct_award_recipients(resolved)
        reported_suppliers = self._reported_supplier_sites(resolved)
        items = self._item_evidence(resolved)
        opportunities = self._opportunities(resolved)
        top_awards = self._top_awards(resolved)
        component_categories = self._component_categories(resolved)
        financial_totals = self._financial_totals(resolved, annual)
        prime_total = abs(float(financial_totals["net_prime_obligations_usd"] or 0))
        subcontract_total = abs(float(financial_totals["mimir_modelled_reported_subcontract_value_usd"] or 0))
        for row in direct_recipients:
            row["share_of_platform_prime_obligations_pct"] = (
                abs(float(row.get("net_prime_obligations_usd") or 0)) / prime_total * 100
                if prime_total else 0
            )
        for row in reported_suppliers:
            row["share_of_reported_subcontract_value_pct"] = (
                abs(float(row.get("mimir_modelled_reported_subcontract_value_usd") or 0))
                / subcontract_total * 100 if subcontract_total else 0
            )
        fingerprint_input = {
            "platform": resolved,
            "annual": annual,
            "direct_recipients": direct_recipients,
            "reported_suppliers": reported_suppliers,
            "items": items,
            "opportunities": opportunities,
            "top_awards": top_awards,
        }
        context = {
            "context_type": "universal_platform_dossier",
            "calculation_version": "mimir-platform-context-2026-09-v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "scope": {
                "platform_id": resolved,
                "display_name": resolved,
                "completed_fiscal_years": list(COMPLETED_FISCAL_YEARS),
                "partial_fiscal_year": 2026,
                "observation_window": OBSERVATION_WINDOW,
            },
            "annual_activity": annual,
            "direct_award_recipients": direct_recipients,
            "reported_supplier_sites": reported_suppliers,
            "reported_component_categories": component_categories,
            "item_and_component_evidence": items,
            "top_prime_awards": top_awards,
            "current_opportunities": opportunities,
            "coverage": {
                "direct_award_recipient_sites": self._available_count(direct_recipients),
                "direct_award_recipient_sites_loaded": len(direct_recipients),
                "reported_supplier_sites": self._available_count(reported_suppliers),
                "reported_supplier_sites_loaded": len(reported_suppliers),
                "associated_niins": items["associated_niin_count"],
                "item_relationships_loaded": len(items["top_items"]),
                "prime_awards": self._available_count(top_awards),
                "prime_awards_loaded": len(top_awards),
                "open_or_loaded_opportunities": len(opportunities),
                "component_proof_status": "CURATED_WHEN_AVAILABLE_OTHERWISE_REPORTED_DESCRIPTION_OR_ITEM_REFERENCE",
            },
            "financial_totals": financial_totals,
            "methodology": {
                "direct_award_lane": "Prime obligations on awards mapped directly to this platform or program.",
                "reported_supplier_lane": "Mimir-modelled reported subcontract value on mapped prime awards; kept separate from prime obligations.",
                "item_lane": "NIIN relationships mapped through the WSDC/platform bridge. Attributed procurement and shared-use exposure are reported separately.",
                "component_rule": "Reported descriptions support bounded capability language. Exact component claims require a platform-specific government or first-party source.",
                "opportunity_rule": "Opportunity matches are research leads based on the platform or program name in the loaded notice text.",
            },
            "evidence_index": self._source_index(resolved, top_awards, opportunities),
            "evidence_fingerprint": hashlib.sha256(
                json.dumps(fingerprint_input, default=str, sort_keys=True).encode()
            ).hexdigest(),
        }
        self._cache[resolved] = context
        return context

    @staticmethod
    def _available_count(rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        return int(rows[0].get("total_available") or len(rows))

    def get_export_context(
        self, platform_id: str, limit: int = EVIDENCE_EXPORT_ROW_LIMIT
    ) -> Dict[str, Any]:
        """Build expanded evidence only when a customer requests the download."""
        base = self.get(platform_id)
        resolved = base["scope"]["platform_id"]
        row_limit = min(max(int(limit), 1), EVIDENCE_EXPORT_ROW_LIMIT)
        expanded = {
            **base,
            "direct_award_recipients": self._direct_award_recipients(resolved, row_limit),
            "reported_supplier_sites": self._reported_supplier_sites(resolved, row_limit),
            "reported_component_categories": self._component_categories(resolved, row_limit),
            "item_and_component_evidence": self._item_evidence(resolved, row_limit),
            "top_prime_awards": self._top_awards(resolved, row_limit),
            "export_row_limit_per_table": row_limit,
        }
        return expanded

    def answer_projection(self, platform_id: str) -> Dict[str, Any]:
        context = self.get(platform_id)
        direct = []
        for row in context["direct_award_recipients"][:8]:
            direct.append({
                **row,
                "sample_contract_ids": (row.get("sample_contract_ids") or [])[:4],
                "sample_award_descriptions": (row.get("sample_award_descriptions") or [])[:3],
                "observed_places_of_performance": (row.get("observed_places_of_performance") or [])[:4],
            })
        suppliers = []
        for row in context["reported_supplier_sites"][:14]:
            suppliers.append({
                **row,
                "reported_prime_names": (row.get("reported_prime_names") or [])[:4],
                "reported_prime_cages": (row.get("reported_prime_cages") or [])[:4],
                "sample_prime_contract_ids": (row.get("sample_prime_contract_ids") or [])[:4],
                "reported_descriptions": (row.get("reported_descriptions") or [])[:4],
            })
        customer_context = {
            key: value
            for key, value in context.items()
            if key not in {"calculation_version", "generated_at", "evidence_fingerprint"}
        }
        customer_context["coverage"] = {
            key: value
            for key, value in context["coverage"].items()
            if key != "component_proof_status"
        }
        return {
            **customer_context,
            "direct_award_recipients": direct,
            "reported_supplier_sites": suppliers,
            "reported_component_categories": context["reported_component_categories"][:12],
            "top_prime_awards": context["top_prime_awards"][:8],
            "current_opportunities": context["current_opportunities"][:6],
            "item_and_component_evidence": {
                **context["item_and_component_evidence"],
                "top_items": context["item_and_component_evidence"]["top_items"][:10],
                "top_item_supplier_sites": context["item_and_component_evidence"]["top_item_supplier_sites"][:12],
            },
        }

    @staticmethod
    def _platform_condition(alias: str = "t") -> str:
        return (
            f"({alias}.platform_family = ? OR "
            f"LIST_CONTAINS(STR_SPLIT(COALESCE({alias}.platform_families, ''), ' | '), ?))"
        )

    def _annual_activity(self, platform: str) -> Dict[str, Any]:
        condition = self._platform_condition("t")
        rows = _rows(
            self.connection.execute(
                f"""
                SELECT
                    year AS fiscal_year,
                    source_system,
                    SUM(CASE WHEN source_system='USA_SPENDING' THEN spend_amount ELSE 0 END)
                        AS net_prime_obligations_usd,
                    SUM(CASE WHEN source_system='USA_SPENDING' AND spend_amount > 0 THEN spend_amount ELSE 0 END)
                        AS positive_prime_obligations_usd,
                    SUM(CASE WHEN source_system='USA_SPENDING' AND spend_amount < 0 THEN spend_amount ELSE 0 END)
                        AS prime_deobligations_usd,
                    SUM(CASE WHEN source_system='DLA' THEN COALESCE(platform_attributed_spend_amount, 0) ELSE 0 END)
                        AS attributed_dla_procurement_value_usd,
                    SUM(CASE WHEN source_system='DLA' THEN COALESCE(shared_use_exposure_amount, 0) ELSE 0 END)
                        AS shared_use_niin_exposure_usd,
                    COUNT(*) AS action_or_line_count,
                    COUNT(DISTINCT award_key) AS award_count,
                    COUNT(DISTINCT niin) FILTER (WHERE niin IS NOT NULL) AS niin_count
                FROM read_parquet(?) t
                WHERE year BETWEEN 2021 AND 2026 AND {condition}
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
                [str(self.paths["transactions"]), platform, platform],
            )
        )
        return {
            "records": rows,
            "completed_fiscal_years": list(COMPLETED_FISCAL_YEARS),
            "partial_fiscal_year": 2026,
            "measure_labels": {
                "USA_SPENDING": "Net prime obligations",
                "DLA": "Attributed DLA procurement value and shared-use NIIN exposure",
            },
        }

    def _direct_award_recipients(self, platform: str, limit: int = 100) -> List[Dict[str, Any]]:
        return _rows(
            self.connection.execute(
                """
                WITH locations AS (
                    SELECT UPPER(TRIM(cage_code)) AS cage, MAX(vendor_name) AS location_name,
                           MAX(city) AS city, MAX(state) AS state, MAX(location_quality) AS location_quality
                    FROM read_parquet(?) GROUP BY 1
                )
                SELECT
                    t.vendor_cage AS cage,
                    COALESCE(MAX(t.vendor_name), MAX(l.location_name)) AS recipient_name,
                    MAX(l.city) AS contracting_city,
                    MAX(l.state) AS contracting_state,
                    MAX(l.location_quality) AS location_quality,
                    SUM(t.spend_amount) AS net_prime_obligations_usd,
                    SUM(CASE WHEN t.spend_amount > 0 THEN t.spend_amount ELSE 0 END) AS positive_prime_obligations_usd,
                    SUM(CASE WHEN t.spend_amount < 0 THEN t.spend_amount ELSE 0 END) AS deobligations_usd,
                    COUNT(*) AS action_count,
                    COUNT(DISTINCT t.award_key) AS award_count,
                    MIN(SUBSTR(t.action_date,1,10)) AS first_action_date,
                    MAX(SUBSTR(t.action_date,1,10)) AS latest_action_date,
                    LIST_SLICE(LIST_DISTINCT(LIST(t.contract_id) FILTER (WHERE t.contract_id IS NOT NULL)),1,8) AS sample_contract_ids,
                    LIST_SLICE(LIST_DISTINCT(LIST(t.base_award_description) FILTER (WHERE t.base_award_description IS NOT NULL)),1,6) AS sample_award_descriptions,
                    LIST_SLICE(LIST_DISTINCT(LIST(CONCAT_WS(', ', NULLIF(t.place_of_performance_city,''), NULLIF(t.place_of_performance_state,''), NULLIF(t.place_of_performance_country,''))) FILTER (WHERE NULLIF(t.place_of_performance_city,'') IS NOT NULL)),1,8) AS observed_places_of_performance,
                    COUNT(*) OVER () AS total_available
                FROM read_parquet(?) t
                LEFT JOIN locations l ON UPPER(TRIM(t.vendor_cage)) = l.cage
                WHERE t.source_system = 'USA_SPENDING'
                  AND t.year BETWEEN 2021 AND 2026
                  AND t.platform_family = ?
                GROUP BY t.vendor_cage
                ORDER BY ABS(net_prime_obligations_usd) DESC
                LIMIT ?
                """,
                [str(self.paths["locations"]), str(self.paths["transactions"]), platform, limit],
            )
        )

    def _reported_supplier_sites(self, platform: str, limit: int = 250) -> List[Dict[str, Any]]:
        return _rows(
            self.connection.execute(
                """
                WITH locations AS (
                    SELECT UPPER(TRIM(cage_code)) AS cage, MAX(vendor_name) AS location_name,
                           MAX(city) AS city, MAX(state) AS state, MAX(location_quality) AS location_quality
                    FROM read_parquet(?) GROUP BY 1
                )
                SELECT
                    n.sub_cage AS cage,
                    COALESCE(MAX(n.sub_name), MAX(l.location_name)) AS supplier_name,
                    MAX(COALESCE(n.sub_city,l.city)) AS city,
                    MAX(COALESCE(n.sub_state,l.state)) AS state,
                    MAX(n.sub_country) AS country,
                    MAX(l.location_quality) AS location_quality,
                    SUM(COALESCE(n.subaward_value,0)) AS mimir_modelled_reported_subcontract_value_usd,
                    SUM(COALESCE(n.subaward_value_raw,0)) AS source_reported_value_usd,
                    COUNT(*) AS selected_report_count,
                    COUNT(DISTINCT n.contract_id) AS prime_award_count,
                    MIN(SUBSTR(n.action_date,1,10)) AS first_reported_date,
                    MAX(SUBSTR(n.action_date,1,10)) AS latest_reported_date,
                    LIST_SLICE(LIST_DISTINCT(LIST(n.prime_name) FILTER (WHERE n.prime_name IS NOT NULL)),1,8) AS reported_prime_names,
                    LIST_SLICE(LIST_DISTINCT(LIST(n.prime_cage) FILTER (WHERE n.prime_cage IS NOT NULL)),1,8) AS reported_prime_cages,
                    LIST_SLICE(LIST_DISTINCT(LIST(n.contract_id) FILTER (WHERE n.contract_id IS NOT NULL)),1,8) AS sample_prime_contract_ids,
                    LIST_SLICE(LIST_DISTINCT(LIST(n.description) FILTER (WHERE n.description IS NOT NULL)),1,8) AS reported_descriptions,
                    COUNT(*) OVER () AS total_available
                FROM read_parquet(?) n
                LEFT JOIN locations l ON UPPER(TRIM(n.sub_cage)) = l.cage
                WHERE n.platform_family = ?
                  AND n.year BETWEEN 2021 AND 2026
                  AND n.sub_cage IS NOT NULL
                  AND UPPER(TRIM(n.sub_cage)) NOT IN ('','UNKNOWN','UNKNO')
                GROUP BY n.sub_cage
                HAVING SUM(COALESCE(n.subaward_value,0)) <> 0
                ORDER BY ABS(mimir_modelled_reported_subcontract_value_usd) DESC
                LIMIT ?
                """,
                [str(self.paths["locations"]), str(self.paths["network"]), platform, limit],
            )
        )

    def _component_categories(self, platform: str, limit: int = 100) -> List[Dict[str, Any]]:
        return _rows(
            self.connection.execute(
                """
                SELECT
                    description AS reported_description,
                    SUM(COALESCE(subaward_value,0)) AS mimir_modelled_reported_subcontract_value_usd,
                    COUNT(*) AS selected_report_count,
                    COUNT(DISTINCT sub_cage) AS supplier_site_count,
                    LIST_SLICE(LIST_DISTINCT(LIST(sub_name) FILTER (WHERE sub_name IS NOT NULL)),1,8) AS suppliers,
                    LIST_SLICE(LIST_DISTINCT(LIST(contract_id) FILTER (WHERE contract_id IS NOT NULL)),1,6) AS sample_prime_contract_ids,
                    COUNT(*) OVER () AS total_available
                FROM read_parquet(?)
                WHERE platform_family = ? AND year BETWEEN 2021 AND 2026
                  AND description IS NOT NULL AND TRIM(description) <> ''
                GROUP BY description
                HAVING SUM(COALESCE(subaward_value,0)) <> 0
                ORDER BY ABS(mimir_modelled_reported_subcontract_value_usd) DESC
                LIMIT ?
                """,
                [str(self.paths["network"]), platform, limit],
            )
        )

    def _item_evidence(self, platform: str, limit: int = 100) -> Dict[str, Any]:
        associated_count = self.connection.execute(
            "SELECT COUNT(DISTINCT LPAD(TRIM(niin),9,'0')) FROM read_parquet(?) WHERE platform_family = ?",
            [str(self.paths["platform_bom"]), platform],
        ).fetchone()[0]
        top_items = _rows(
            self.connection.execute(
                """
                WITH bridge AS (
                    SELECT LPAD(TRIM(niin),9,'0') AS niin,
                           LIST_DISTINCT(LIST(wsdc_code)) AS wsdc_codes,
                           LIST_DISTINCT(LIST(association_source)) AS association_sources
                    FROM read_parquet(?) WHERE platform_family = ? GROUP BY 1
                ), platform_value AS (
                    SELECT LPAD(TRIM(niin),9,'0') AS niin,
                           SUM(COALESCE(platform_attributed_spend_amount,0)) AS attributed_dla_procurement_value_usd,
                           SUM(COALESCE(shared_use_exposure_amount,0)) AS shared_use_niin_exposure_usd,
                           MAX(SUBSTR(action_date,1,10)) AS latest_observed_date
                    FROM read_parquet(?)
                    WHERE source_system='DLA' AND year BETWEEN 2021 AND 2026
                      AND (platform_family=? OR LIST_CONTAINS(STR_SPLIT(COALESCE(platform_families,''),' | '),?))
                    GROUP BY 1
                )
                SELECT p.nsn, b.niin, p.item_name AS description, p.fsc_code,
                       COALESCE(v.attributed_dla_procurement_value_usd,0) AS attributed_dla_procurement_value_usd,
                       COALESCE(v.shared_use_niin_exposure_usd,0) AS shared_use_niin_exposure_usd,
                       v.latest_observed_date,
                       b.wsdc_codes, b.association_sources
                FROM bridge b
                LEFT JOIN read_parquet(?) p ON LPAD(TRIM(p.niin),9,'0') = b.niin
                LEFT JOIN platform_value v ON b.niin=v.niin
                ORDER BY ABS(COALESCE(v.attributed_dla_procurement_value_usd,0))
                       + ABS(COALESCE(v.shared_use_niin_exposure_usd,0)) DESC
                LIMIT ?
                """,
                [
                    str(self.paths["platform_bom"]), platform,
                    str(self.paths["transactions"]), platform, platform,
                    str(self.paths["item_profiles"]), limit,
                ],
            )
        )
        suppliers = _rows(
            self.connection.execute(
                """
                WITH supplier_values AS (
                SELECT niin, cage, MAX(vendor) AS supplier_name,
                       SUM(CASE WHEN COALESCE(has_multiple_platforms,FALSE)=FALSE THEN total_revenue ELSE 0 END)
                           AS attributed_dla_procurement_value_usd,
                       SUM(CASE WHEN COALESCE(has_multiple_platforms,FALSE)=TRUE THEN total_revenue ELSE 0 END)
                           AS shared_use_niin_exposure_usd,
                       SUM(total_units_sold) AS observed_units,
                       MAX(last_sold) AS latest_observed_date,
                       MAX(has_multiple_platforms) AS has_multiple_platforms,
                       MAX(platform_families) AS platform_families,
                       COUNT(DISTINCT contract_id) AS contract_count
                FROM read_parquet(?)
                WHERE year BETWEEN 2021 AND 2026
                  AND (platform_family = ? OR LIST_CONTAINS(STR_SPLIT(COALESCE(platform_families,''),' | '),?))
                GROUP BY niin,cage
                HAVING SUM(total_revenue) <> 0
                ), locations AS (
                    SELECT UPPER(TRIM(cage_code)) AS cage, MAX(city) AS city, MAX(state) AS state,
                           MAX(location_quality) AS location_quality
                    FROM read_parquet(?) GROUP BY 1
                )
                SELECT s.*, l.city, l.state, l.location_quality
                FROM supplier_values s
                LEFT JOIN locations l ON UPPER(TRIM(s.cage))=l.cage
                ORDER BY ABS(attributed_dla_procurement_value_usd) + ABS(shared_use_niin_exposure_usd) DESC
                LIMIT ?
                """,
                [
                    str(self.paths["item_suppliers"]), platform, platform,
                    str(self.paths["locations"]), limit,
                ],
            )
        )
        return {
            "associated_niin_count": associated_count,
            "top_items": top_items,
            "top_item_supplier_sites": suppliers,
            "financial_treatment": "Single-platform attributed value and shared-use NIIN exposure remain separate.",
        }

    def _top_awards(self, platform: str, limit: int = 100) -> List[Dict[str, Any]]:
        return _rows(
            self.connection.execute(
                """
                SELECT contract_id, vendor_name AS recipient_name, vendor_cage AS recipient_cage,
                       base_award_description, SUM(spend_amount) AS net_prime_obligations_usd,
                       COUNT(*) AS action_count, MIN(SUBSTR(action_date,1,10)) AS first_action_date,
                       MAX(SUBSTR(action_date,1,10)) AS latest_action_date,
                       MAX(place_of_performance_city) AS place_of_performance_city,
                       MAX(place_of_performance_state) AS place_of_performance_state,
                       COUNT(*) OVER () AS total_available
                FROM read_parquet(?)
                WHERE source_system='USA_SPENDING' AND year BETWEEN 2021 AND 2026
                  AND platform_family = ?
                GROUP BY contract_id,vendor_name,vendor_cage,base_award_description
                ORDER BY ABS(net_prime_obligations_usd) DESC
                LIMIT ?
                """,
                [str(self.paths["transactions"]), platform, limit],
            )
        )

    def _financial_totals(self, platform: str, annual: Dict[str, Any]) -> Dict[str, Any]:
        totals = {
            "observation_window": OBSERVATION_WINDOW,
            "net_prime_obligations_usd": 0.0,
            "attributed_dla_procurement_value_usd": 0.0,
            "shared_use_niin_exposure_usd": 0.0,
        }
        for row in annual.get("records", []):
            totals["net_prime_obligations_usd"] += float(row.get("net_prime_obligations_usd") or 0)
            totals["attributed_dla_procurement_value_usd"] += float(row.get("attributed_dla_procurement_value_usd") or 0)
            totals["shared_use_niin_exposure_usd"] += float(row.get("shared_use_niin_exposure_usd") or 0)
        subcontract_total = self.connection.execute(
            """
            SELECT SUM(COALESCE(n.subaward_value, 0))
            FROM read_parquet(?) n
            WHERE n.year BETWEEN 2021 AND 2026 AND n.platform_family = ?
            """,
            [str(self.paths["network"]), platform],
        ).fetchone()[0]
        totals["mimir_modelled_reported_subcontract_value_usd"] = float(subcontract_total or 0)
        return totals

    def _opportunities(self, platform: str) -> List[Dict[str, Any]]:
        pattern = f"%{platform}%"
        rows = _rows(
            self.connection.execute(
                """
                SELECT id, sol_num, title, agency, sub_agency, SUBSTR(deadline,1,10) AS deadline,
                       set_aside_type, CAST(naics AS BIGINT)::VARCHAR AS naics_code, psc, state, url
                FROM read_parquet(?)
                WHERE UPPER(COALESCE(search_text,title,'')) LIKE UPPER(?)
                ORDER BY deadline DESC
                LIMIT 30
                """,
                [str(self.paths["opportunities"]), pattern],
            )
        )
        today = datetime.now(timezone.utc).date().isoformat()
        for row in rows:
            row["response_status"] = "OPEN" if row.get("deadline") and row["deadline"] >= today else "CLOSED"
        return rows

    @staticmethod
    def _source_index(platform: str, awards: List[Dict[str, Any]], opportunities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        sources = [
            {
                "source": "USAspending award and transaction records",
                "supports": "prime-award recipients, obligations, descriptions and places of performance",
                "public_record_ids": [row.get("contract_id") for row in awards[:12]],
            },
            {
                "source": "Reported federal subaward records",
                "supports": "reported prime-to-supplier relationships and descriptions",
                "public_record_ids": [row.get("contract_id") for row in awards[:12]],
            },
            {
                "source": "DLA contract history and FLIS/WSDC references",
                "supports": "NIIN procurement, supplier and item-platform relationships",
                "public_record_ids": [],
            },
        ]
        if opportunities:
            sources.append(
                {
                    "source": "SAM.gov opportunity notices",
                    "supports": "loaded current and recent opportunity records naming the platform",
                    "public_record_ids": [row.get("sol_num") or row.get("id") for row in opportunities[:8]],
                }
            )
        return sources
