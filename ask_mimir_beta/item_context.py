"""Build deterministic NSN, NIIN and part-number dossiers for Ask Mimir."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence

import duckdb


DEFAULT_DATA_ROOT = Path(
    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data"
)
DEFAULT_COMPLETED_FISCAL_YEARS = tuple(range(2021, 2026))

RNSC_DEFINITIONS = {
    "A": "Manufacturer and reference number are authorized for procurement from the identified CAGE.",
    "B": "Manufacturer and/or reference number are not authorized for procurement.",
    "C": "Unrestricted procurement document; the listed CAGE is the document originator, not the supply source.",
    "D": "Procurement authority has not yet been checked.",
    "E": "Restricted procurement document; the listed CAGE is not the supply source.",
    "F": "The item is subject to qualification and may be procured only from qualified manufacturers.",
    "G": "Manufacturer and reference are not authorized; the manufacturer uses other organizations for distribution.",
    "H": "Technical document is restricted to one manufacturer; the listed CAGE is not itself the supply source.",
}

RNCC_DEFINITIONS = {
    "1": "Source-control reference",
    "2": "Definitive government specification or standard",
    "3": "Manufacturer design-control reference",
    "4": "Non-definitive government specification or standard",
    "5": "Secondary reference",
    "6": "Informative reference",
    "7": "Vendor item-control drawing reference",
    "8": "Reproduced-item reference",
    "A": "Packaging or logistics-data reference",
    "C": "Reference establishing a special item relationship",
    "D": "Other drawing-number reference",
}

RNVC_DEFINITIONS = {
    "1": "Non-item-identifying reference",
    "2": "Item-identifying reference",
    "9": "Obsolete or superseded reference retained for traceability",
}


def _rows(cursor: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _split_codes(value: Any) -> List[str]:
    return sorted({part.strip().upper() for part in str(value or "").split(",") if part.strip()})


def normalize_niin(value: Any) -> str | None:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) == 13:
        return digits[-9:]
    if 1 <= len(digits) <= 9:
        return digits.zfill(9)
    return None


def normalize_part_number(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


class ItemContextStore:
    """Resolve and read item evidence directly from the frozen serving files."""

    def __init__(self, data_root: Path = DEFAULT_DATA_ROOT) -> None:
        self.data_root = data_root.resolve()
        self.paths = {
            name: self.data_root / filename
            for name, filename in {
                "profile": "nsn_profile_lookup.parquet",
                "reference": "nsn_cage_reference.parquet",
                "transactions": "transactions.parquet",
                "platform_bom": "platform_bom.parquet",
                "cage_locations": "cage_locations.parquet",
                "geo": "geo.parquet",
            }.items()
        }
        missing = [str(path) for path in self.paths.values() if not path.exists()]
        if missing:
            raise FileNotFoundError(f"item context sources are missing: {missing}")
        self.connection = duckdb.connect()
        self.connection.execute("SET preserve_insertion_order=false")
        self.connection.execute("SET threads=2")
        self.connection.execute("SET memory_limit='1GB'")
        self._cache: Dict[tuple[str, tuple[int, ...]], Dict[str, Any]] = {}

    def search(self, query: str, limit: int = 20) -> Dict[str, Any]:
        clean_query = str(query or "").strip()
        if not clean_query:
            return {"query": clean_query, "matches": [], "requires_disambiguation": False}

        digits = re.sub(r"\D", "", clean_query)
        direct_niin = normalize_niin(clean_query) if len(digits) in {8, 9, 13} else None
        if direct_niin and self._niin_exists(direct_niin):
            matches = [self._resolution_summary(direct_niin, clean_query)]
            return {
                "query": clean_query,
                "query_type": "NSN" if len(digits) == 13 else "NIIN",
                "matches": matches,
                "requires_disambiguation": False,
                "resolved_niin": direct_niin,
            }

        normalized_part = normalize_part_number(clean_query)
        if not normalized_part:
            return {"query": clean_query, "matches": [], "requires_disambiguation": False}
        cursor = self.connection.execute(
            """
            SELECT
                LPAD(TRIM(niin), 9, '0') AS niin,
                MAX(nsn) AS nsn,
                MAX(description) AS description,
                MAX(part_number) AS matched_part_number,
                COUNT(DISTINCT cage) AS referenced_cage_count,
                COUNT(DISTINCT cage) FILTER (WHERE is_active_authorized_source)
                    AS active_authorized_source_count
            FROM read_parquet(?)
            WHERE REGEXP_REPLACE(UPPER(TRIM(part_number)), '[^A-Z0-9]', '', 'g') = ?
            GROUP BY 1
            ORDER BY active_authorized_source_count DESC, referenced_cage_count DESC, niin
            LIMIT ?
            """,
            [str(self.paths["reference"]), normalized_part, min(max(int(limit), 1), 20)],
        )
        matches = _rows(cursor)
        for row in matches:
            row["option_label"] = (
                f"{row.get('matched_part_number') or clean_query} - "
                f"{row.get('description') or 'Item description unavailable'} - "
                f"NSN {row.get('nsn') or row['niin']}"
            )
        return {
            "query": clean_query,
            "query_type": "PART_NUMBER",
            "matches": matches,
            "requires_disambiguation": len(matches) > 1,
            "resolved_niin": matches[0]["niin"] if len(matches) == 1 else None,
        }

    def get(
        self,
        niin: str,
        fiscal_years: Sequence[int] = DEFAULT_COMPLETED_FISCAL_YEARS,
    ) -> Dict[str, Any]:
        clean_niin = normalize_niin(niin)
        if not clean_niin:
            raise ValueError("A valid 9-digit NIIN or 13-digit NSN is required")
        years = tuple(sorted({int(year) for year in fiscal_years}))
        cache_key = (clean_niin, years)
        if cache_key in self._cache:
            return self._cache[cache_key]
        if not self._niin_exists(clean_niin):
            raise KeyError(f"item was not found: {niin}")

        profile = self._profile(clean_niin)
        references = self._references(clean_niin)
        financials = self._financials(clean_niin, years)
        contracts = self._contracts(clean_niin, years)
        linked_prime_activity = self._linked_prime_financials(clean_niin, years)
        linked_prime_awards = self._linked_prime_awards(clean_niin, years)
        current_partial = self._financials(clean_niin, [2026])
        platforms = self._platforms(clean_niin)
        suppliers = self._suppliers(clean_niin, years, references)
        part_numbers = self._part_numbers(references, contracts)
        source_index = self._source_index(
            clean_niin, contracts, linked_prime_awards, platforms
        )

        identity = {
            "niin": clean_niin,
            "nsn": profile.get("nsn") or next((row.get("nsn") for row in references if row.get("nsn")), None),
            "fsc_code": profile.get("fsc_code") or next((row.get("fsc_code") for row in references if row.get("fsc_code")), None),
            "description": profile.get("item_name") or next((row.get("description") for row in references if row.get("description")), None),
            "unit_of_issue": profile.get("unit_of_issue"),
            "acquisition_advice_code": profile.get("acquisition_advice_code"),
            "government_estimated_price_usd": profile.get("govt_estimated_price"),
            "source_of_supply": profile.get("source_of_supply"),
            "demil_code": profile.get("demil_code"),
            "shelf_life_code": profile.get("shelf_life_code"),
        }
        fingerprint_basis = json.dumps(
            {
                "identity": identity,
                "financials": financials,
                "platforms": platforms,
                "contracts": contracts,
                "references": references,
            },
            default=str,
            sort_keys=True,
        )
        context = {
            "context_type": "item_dossier",
            "calculation_version": "mimir-item-context-2026-09-v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "identity": identity,
            "observation_window": {
                "completed_fiscal_years": list(years),
                "label": f"FY{min(years)}-FY{max(years)}" if years else "No completed years selected",
                "partial_fiscal_year": 2026,
            },
            "procurement_activity": financials,
            "linked_prime_activity": linked_prime_activity,
            "current_partial_activity": current_partial,
            "supplier_summary": suppliers,
            "reference_relationships": references,
            "part_number_summary": part_numbers,
            "platform_associations": {
                "platforms": platforms,
                "platform_count": len(platforms),
                "has_multiple_platforms": len(platforms) > 1,
                "financial_treatment": (
                    "The item-platform bridge is a reference association. NIIN-level procurement "
                    "value is not repeated across platform rows when an item maps to multiple platforms."
                ),
            },
            "contracts": contracts,
            "linked_prime_awards": linked_prime_awards,
            "code_definitions": {
                "rnsc": RNSC_DEFINITIONS,
                "rncc": RNCC_DEFINITIONS,
                "rnvc": RNVC_DEFINITIONS,
            },
            "evidence_index": source_index,
            "evidence_fingerprint": hashlib.sha256(fingerprint_basis.encode()).hexdigest(),
            "methodology": {
                "financial_grain": "DLA procurement value is calculated from DLA transaction lines at NIIN and CAGE grain.",
                "linked_prime_rule": "Federal obligations on linked awards provide award-level context for the same procurement activity. They are shown alongside, rather than added to, DLA item procurement value.",
                "part_reference_rule": "Part-number reference rows do not inherit or duplicate NIIN financial value.",
                "authorized_source_rule": "Current active-authorized-source status requires both an authorized FLIS relationship and an active CAGE status in the loaded reference snapshot.",
                "platform_rule": "All mapped item-platform associations are retained; ambiguous multi-platform value remains shared-use exposure rather than being assigned arbitrarily.",
            },
        }
        self._cache[cache_key] = context
        return context

    def answer_projection(self, niin: str) -> Dict[str, Any]:
        """Return a compact model payload while preserving the full export context."""

        context = self.get(niin)
        material_suppliers = [
            row
            for row in context["supplier_summary"]
            if row["is_active_authorized_source"]
            or row["has_observed_dla_procurement"]
            or row["has_linked_prime_obligations"]
        ]
        material_cages = {row["cage"] for row in material_suppliers}
        reference_rows = [
            row
            for row in context["reference_relationships"]
            if row["cage"] in material_cages
        ]
        projected = {
            key: value
            for key, value in context.items()
            if key
            not in {
                "supplier_summary",
                "reference_relationships",
                "part_number_summary",
                "contracts",
                "linked_prime_awards",
                "evidence_index",
                "evidence_fingerprint",
            }
        }
        projected.update(
            {
                "supplier_summary": material_suppliers,
                "reference_relationships": reference_rows[:30],
                "part_number_summary": context["part_number_summary"][:30],
                "contracts": context["contracts"][:15],
                "linked_prime_awards": context["linked_prime_awards"][:15],
                "evidence_index": context["evidence_index"][:40],
                "coverage": {
                    "supplier_or_reference_sites": len(context["supplier_summary"]),
                    "material_supplier_sites_in_answer": len(material_suppliers),
                    "part_number_relationships": len(context["reference_relationships"]),
                    "distinct_part_numbers": len(context["part_number_summary"]),
                    "dla_contract_records": len(context["contracts"]),
                    "linked_prime_award_records": len(context["linked_prime_awards"]),
                    "platform_associations": context["platform_associations"]["platform_count"],
                },
            }
        )
        return projected

    def _niin_exists(self, niin: str) -> bool:
        return bool(
            self.connection.execute(
                "SELECT 1 FROM read_parquet(?) WHERE niin=? LIMIT 1",
                [str(self.paths["reference"]), niin],
            ).fetchone()
            or self.connection.execute(
                "SELECT 1 FROM read_parquet(?) WHERE niin=? LIMIT 1",
                [str(self.paths["profile"]), niin],
            ).fetchone()
        )

    def _resolution_summary(self, niin: str, query: str) -> Dict[str, Any]:
        profile = self._profile(niin)
        row = self.connection.execute(
            """
            SELECT MAX(nsn), MAX(description), COUNT(DISTINCT cage),
                   COUNT(DISTINCT part_number),
                   COUNT(DISTINCT cage) FILTER (WHERE is_active_authorized_source)
            FROM read_parquet(?) WHERE niin=?
            """,
            [str(self.paths["reference"]), niin],
        ).fetchone()
        nsn = profile.get("nsn") or row[0]
        description = profile.get("item_name") or row[1]
        return {
            "niin": niin,
            "nsn": nsn,
            "description": description,
            "referenced_cage_count": int(row[2] or 0),
            "referenced_part_number_count": int(row[3] or 0),
            "active_authorized_source_count": int(row[4] or 0),
            "option_label": f"{description or 'Item'} - NSN {nsn or niin}",
            "matched_query": query,
        }

    def _profile(self, niin: str) -> Dict[str, Any]:
        cursor = self.connection.execute(
            "SELECT * FROM read_parquet(?) WHERE niin=? LIMIT 1",
            [str(self.paths["profile"]), niin],
        )
        rows = _rows(cursor)
        return rows[0] if rows else {}

    def _references(self, niin: str) -> List[Dict[str, Any]]:
        cursor = self.connection.execute(
            """
            SELECT
                niin, MAX(nsn) AS nsn, MAX(fsc_code) AS fsc_code, cage,
                MAX(vendor_name) AS vendor_name, MAX(description) AS description,
                part_number, STRING_AGG(DISTINCT rncc_codes, ',' ORDER BY rncc_codes)
                    FILTER (WHERE COALESCE(rncc_codes, '') <> '') AS rncc_codes,
                STRING_AGG(DISTINCT rnvc_codes, ',' ORDER BY rnvc_codes)
                    FILTER (WHERE COALESCE(rnvc_codes, '') <> '') AS rnvc_codes,
                STRING_AGG(DISTINCT rnsc_codes, ',' ORDER BY rnsc_codes)
                    FILTER (WHERE COALESCE(rnsc_codes, '') <> '') AS rnsc_codes,
                STRING_AGG(DISTINCT cage_status_codes, ',' ORDER BY cage_status_codes)
                    FILTER (WHERE COALESCE(cage_status_codes, '') <> '') AS cage_status_codes,
                BOOL_OR(COALESCE(is_procurement_authorized, false)) AS is_procurement_authorized,
                BOOL_OR(COALESCE(is_active_authorized_source, false)) AS is_active_authorized_source,
                MAX(supplier_status) AS supplier_status,
                MAX(supplier_status_detail) AS supplier_status_detail
            FROM read_parquet(?)
            WHERE niin=?
            GROUP BY niin, cage, part_number
            ORDER BY is_active_authorized_source DESC, is_procurement_authorized DESC,
                     vendor_name, part_number
            """,
            [str(self.paths["reference"]), niin],
        )
        rows = _rows(cursor)
        for row in rows:
            row["rnsc_meanings"] = [RNSC_DEFINITIONS.get(code, "Definition not loaded") for code in _split_codes(row.get("rnsc_codes"))]
            row["rncc_meanings"] = [RNCC_DEFINITIONS.get(code, "Definition not loaded") for code in _split_codes(row.get("rncc_codes"))]
            row["rnvc_meanings"] = [RNVC_DEFINITIONS.get(code, "Definition not loaded") for code in _split_codes(row.get("rnvc_codes"))]
        return rows

    def _financials(self, niin: str, years: Sequence[int]) -> Dict[str, Any]:
        if not years:
            return {}
        marks = ",".join("?" for _ in years)
        params = [str(self.paths["transactions"]), niin, *years]
        summary_cursor = self.connection.execute(
            f"""
            SELECT SUM(spend_amount) AS net_dla_procurement_value_usd,
                   SUM(CASE WHEN spend_amount > 0 THEN spend_amount ELSE 0 END)
                       AS positive_dla_procurement_value_usd,
                   SUM(CASE WHEN spend_amount < 0 THEN spend_amount ELSE 0 END)
                       AS negative_adjustment_value_usd,
                   COUNT(DISTINCT award_key) AS distinct_awards,
                   COUNT(DISTINCT transaction_key) AS distinct_transaction_lines,
                   COUNT(DISTINCT vendor_cage) AS observed_supplier_sites,
                   MIN(TRY_CAST(action_date AS DATE)) AS first_observed_date,
                   MAX(TRY_CAST(action_date AS DATE)) AS latest_observed_date
            FROM read_parquet(?)
            WHERE source_system='DLA' AND niin=? AND year IN ({marks})
            """,
            params,
        )
        summary = _rows(summary_cursor)[0]
        annual_cursor = self.connection.execute(
            f"""
            SELECT year AS fiscal_year, SUM(spend_amount) AS net_dla_procurement_value_usd,
                   COUNT(DISTINCT award_key) AS distinct_awards,
                   COUNT(DISTINCT transaction_key) AS distinct_transaction_lines,
                   COUNT(DISTINCT vendor_cage) AS observed_supplier_sites
            FROM read_parquet(?)
            WHERE source_system='DLA' AND niin=? AND year IN ({marks})
            GROUP BY year ORDER BY year
            """,
            params,
        )
        observed_by_year = {
            int(row["fiscal_year"]): row for row in _rows(annual_cursor)
        }
        summary["annual_activity"] = [
            observed_by_year.get(
                int(year),
                {
                    "fiscal_year": int(year),
                    "net_dla_procurement_value_usd": None,
                    "distinct_awards": 0,
                    "distinct_transaction_lines": 0,
                    "observed_supplier_sites": 0,
                    "observation_status": "NOT_OBSERVED",
                },
            )
            for year in years
        ]
        for row in summary["annual_activity"]:
            row.setdefault("observation_status", "OBSERVED")
        return summary

    def _contracts(self, niin: str, years: Sequence[int]) -> List[Dict[str, Any]]:
        marks = ",".join("?" for _ in years)
        cursor = self.connection.execute(
            f"""
            SELECT award_key, contract_id, vendor_cage,
                   MAX_BY(vendor_name, TRY_CAST(action_date AS DATE)) AS vendor_name,
                   MAX_BY(description, TRY_CAST(action_date AS DATE)) AS description,
                   SUM(spend_amount) AS net_dla_procurement_value_usd,
                   COUNT(DISTINCT transaction_key) AS transaction_line_count,
                   MIN(TRY_CAST(action_date AS DATE)) AS first_observed_date,
                   MAX(TRY_CAST(action_date AS DATE)) AS latest_observed_date,
                   STRING_AGG(DISTINCT part_number, ' | ' ORDER BY part_number)
                       FILTER (WHERE COALESCE(TRIM(part_number), '') <> '') AS observed_part_numbers,
                   STRING_AGG(DISTINCT po_number, ' | ' ORDER BY po_number)
                       FILTER (WHERE COALESCE(TRIM(po_number), '') <> '') AS purchase_order_numbers
            FROM read_parquet(?)
            WHERE source_system='DLA' AND niin=? AND year IN ({marks})
            GROUP BY award_key, contract_id, vendor_cage
            ORDER BY ABS(net_dla_procurement_value_usd) DESC, latest_observed_date DESC
            """,
            [str(self.paths["transactions"]), niin, *years],
        )
        return _rows(cursor)

    def _linked_prime_financials(
        self, niin: str, years: Sequence[int]
    ) -> Dict[str, Any]:
        if not years:
            return {}
        marks = ",".join("?" for _ in years)
        params = [str(self.paths["transactions"]), niin, *years]
        cursor = self.connection.execute(
            f"""
            SELECT SUM(spend_amount) AS net_linked_prime_obligations_usd,
                   SUM(CASE WHEN spend_amount > 0 THEN spend_amount ELSE 0 END)
                       AS positive_linked_prime_obligations_usd,
                   SUM(CASE WHEN spend_amount < 0 THEN spend_amount ELSE 0 END)
                       AS deobligations_usd,
                   COUNT(DISTINCT award_key) AS distinct_awards,
                   COUNT(DISTINCT transaction_key) AS distinct_actions,
                   COUNT(DISTINCT vendor_cage) AS recipient_sites,
                   MIN(TRY_CAST(action_date AS DATE)) AS first_observed_date,
                   MAX(TRY_CAST(action_date AS DATE)) AS latest_observed_date
            FROM read_parquet(?)
            WHERE source_system='USA_SPENDING' AND niin=? AND year IN ({marks})
              AND nsn_resolution_status='RESOLVED'
            """,
            params,
        )
        summary = _rows(cursor)[0]
        annual_cursor = self.connection.execute(
            f"""
            SELECT year AS fiscal_year,
                   SUM(spend_amount) AS net_linked_prime_obligations_usd,
                   COUNT(DISTINCT award_key) AS distinct_awards,
                   COUNT(DISTINCT transaction_key) AS distinct_actions,
                   COUNT(DISTINCT vendor_cage) AS recipient_sites
            FROM read_parquet(?)
            WHERE source_system='USA_SPENDING' AND niin=? AND year IN ({marks})
              AND nsn_resolution_status='RESOLVED'
            GROUP BY year ORDER BY year
            """,
            params,
        )
        observed_by_year = {
            int(row["fiscal_year"]): row for row in _rows(annual_cursor)
        }
        summary["annual_activity"] = [
            observed_by_year.get(
                int(year),
                {
                    "fiscal_year": int(year),
                    "net_linked_prime_obligations_usd": None,
                    "distinct_awards": 0,
                    "distinct_actions": 0,
                    "recipient_sites": 0,
                    "observation_status": "NOT_OBSERVED",
                },
            )
            for year in years
        ]
        for row in summary["annual_activity"]:
            row.setdefault("observation_status", "OBSERVED")
        return summary

    def _linked_prime_awards(
        self, niin: str, years: Sequence[int]
    ) -> List[Dict[str, Any]]:
        marks = ",".join("?" for _ in years)
        cursor = self.connection.execute(
            f"""
            SELECT award_key, contract_id, vendor_cage,
                   MAX_BY(vendor_name, TRY_CAST(action_date AS DATE)) AS recipient_name,
                   MAX_BY(base_award_description, TRY_CAST(action_date AS DATE))
                       AS base_award_description,
                   SUM(spend_amount) AS net_linked_prime_obligations_usd,
                   COUNT(DISTINCT transaction_key) AS action_count,
                   MIN(TRY_CAST(action_date AS DATE)) AS first_action_date,
                   MAX(TRY_CAST(action_date AS DATE)) AS latest_action_date,
                   MAX(place_of_performance_city) AS place_of_performance_city,
                   MAX(place_of_performance_state) AS place_of_performance_state,
                   MAX(place_of_performance_country) AS place_of_performance_country,
                   STRING_AGG(DISTINCT nsn_derivation_method, ' | ' ORDER BY nsn_derivation_method)
                       AS item_link_method
            FROM read_parquet(?)
            WHERE source_system='USA_SPENDING' AND niin=? AND year IN ({marks})
              AND nsn_resolution_status='RESOLVED'
            GROUP BY award_key, contract_id, vendor_cage
            ORDER BY ABS(net_linked_prime_obligations_usd) DESC, latest_action_date DESC
            """,
            [str(self.paths["transactions"]), niin, *years],
        )
        return _rows(cursor)

    def _platforms(self, niin: str) -> List[Dict[str, Any]]:
        cursor = self.connection.execute(
            """
            SELECT platform_family,
                   STRING_AGG(DISTINCT wsdc_code, ' | ' ORDER BY wsdc_code)
                       FILTER (WHERE COALESCE(TRIM(wsdc_code), '') <> '') AS wsdc_codes,
                   STRING_AGG(DISTINCT association_source, ' | ' ORDER BY association_source)
                       FILTER (WHERE COALESCE(TRIM(association_source), '') <> '') AS association_sources
            FROM read_parquet(?)
            WHERE niin=? AND COALESCE(TRIM(platform_family), '') <> ''
            GROUP BY platform_family ORDER BY platform_family
            """,
            [str(self.paths["platform_bom"]), niin],
        )
        return _rows(cursor)

    def _suppliers(
        self, niin: str, years: Sequence[int], references: Sequence[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        marks = ",".join("?" for _ in years)
        cursor = self.connection.execute(
            f"""
            SELECT vendor_cage AS cage,
                   MAX_BY(vendor_name, TRY_CAST(action_date AS DATE)) AS observed_vendor_name,
                   SUM(spend_amount) FILTER (WHERE source_system='DLA')
                       AS net_dla_procurement_value_usd,
                   COUNT(DISTINCT award_key) FILTER (WHERE source_system='DLA')
                       AS distinct_dla_awards,
                   COUNT(DISTINCT transaction_key) FILTER (WHERE source_system='DLA')
                       AS dla_transaction_line_count,
                   SUM(spend_amount) FILTER (
                       WHERE source_system='USA_SPENDING' AND nsn_resolution_status='RESOLVED'
                   ) AS net_linked_prime_obligations_usd,
                   COUNT(DISTINCT award_key) FILTER (
                       WHERE source_system='USA_SPENDING' AND nsn_resolution_status='RESOLVED'
                   ) AS distinct_linked_prime_awards,
                   MAX(TRY_CAST(action_date AS DATE)) AS latest_observed_date
            FROM read_parquet(?)
            WHERE source_system IN ('DLA', 'USA_SPENDING')
              AND niin=? AND year IN ({marks})
            GROUP BY vendor_cage
            """,
            [str(self.paths["transactions"]), niin, *years],
        )
        activity = {row["cage"]: row for row in _rows(cursor)}
        reference_by_cage: Dict[str, List[Dict[str, Any]]] = {}
        for row in references:
            reference_by_cage.setdefault(row["cage"], []).append(row)
        cages = sorted(set(activity) | set(reference_by_cage))
        geo_by_cage: Dict[str, Dict[str, Any]] = {}
        if cages:
            geo_marks = ",".join("?" for _ in cages)
            geo_cursor = self.connection.execute(
                f"""
                WITH candidates AS (
                    SELECT cage_code AS cage, vendor_name AS geo_vendor_name,
                           city, state, location_quality, 1 AS source_rank
                    FROM read_parquet(?)
                    WHERE cage_code IN ({geo_marks})
                    UNION ALL
                    SELECT cage_code AS cage, vendor_name AS geo_vendor_name,
                           city, state, location_quality, 2 AS source_rank
                    FROM read_parquet(?)
                    WHERE cage_code IN ({geo_marks})
                )
                SELECT cage,
                       MAX_BY(geo_vendor_name, -source_rank) AS geo_vendor_name,
                       MAX_BY(city, -source_rank) FILTER (WHERE city IS NOT NULL) AS city,
                       MAX_BY(state, -source_rank) FILTER (WHERE state IS NOT NULL) AS state,
                       MAX_BY(location_quality, -source_rank)
                           FILTER (WHERE city IS NOT NULL OR state IS NOT NULL)
                           AS location_quality
                FROM candidates
                GROUP BY cage
                """,
                [
                    str(self.paths["cage_locations"]), *cages,
                    str(self.paths["geo"]), *cages,
                ],
            )
            geo_by_cage = {row["cage"]: row for row in _rows(geo_cursor)}
        rows: List[Dict[str, Any]] = []
        for cage in cages:
            observed = activity.get(cage, {})
            geo = geo_by_cage.get(cage, {})
            refs = reference_by_cage.get(cage, [])
            vendor_name = (
                observed.get("observed_vendor_name")
                or next((row.get("vendor_name") for row in refs if row.get("vendor_name")), None)
                or geo.get("geo_vendor_name")
                or f"CAGE {cage}"
            )
            rows.append(
                {
                    "cage": cage,
                    "vendor_name": vendor_name,
                    "city": geo.get("city"),
                    "state": geo.get("state"),
                    "location_quality": geo.get("location_quality"),
                    "has_observed_dla_procurement": observed.get("net_dla_procurement_value_usd") is not None,
                    "net_dla_procurement_value_usd": observed.get("net_dla_procurement_value_usd"),
                    "distinct_dla_awards": int(observed.get("distinct_dla_awards") or 0),
                    "dla_transaction_line_count": int(observed.get("dla_transaction_line_count") or 0),
                    "has_linked_prime_obligations": observed.get("net_linked_prime_obligations_usd") is not None,
                    "net_linked_prime_obligations_usd": observed.get("net_linked_prime_obligations_usd"),
                    "distinct_linked_prime_awards": int(observed.get("distinct_linked_prime_awards") or 0),
                    "latest_observed_date": observed.get("latest_observed_date"),
                    "is_procurement_authorized": any(bool(row.get("is_procurement_authorized")) for row in refs),
                    "is_active_authorized_source": any(bool(row.get("is_active_authorized_source")) for row in refs),
                    "part_numbers": sorted({row["part_number"] for row in refs if row.get("part_number")}),
                    "relationship_statuses": sorted({row["supplier_status"] for row in refs if row.get("supplier_status")}),
                }
            )
        return sorted(
            rows,
            key=lambda row: (
                not (row["has_observed_dla_procurement"] or row["has_linked_prime_obligations"]),
                -max(
                    abs(float(row.get("net_dla_procurement_value_usd") or 0)),
                    abs(float(row.get("net_linked_prime_obligations_usd") or 0)),
                ),
                not row["is_active_authorized_source"],
                row["vendor_name"],
            ),
        )

    @staticmethod
    def _part_numbers(
        references: Sequence[Dict[str, Any]], contracts: Sequence[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        observed_contracts: Dict[str, set[str]] = {}
        for contract in contracts:
            for part in str(contract.get("observed_part_numbers") or "").split(" | "):
                if part:
                    observed_contracts.setdefault(normalize_part_number(part), set()).add(contract["contract_id"])
        grouped: Dict[str, Dict[str, Any]] = {}
        for row in references:
            key = normalize_part_number(row.get("part_number"))
            entry = grouped.setdefault(
                key,
                {
                    "part_number": row.get("part_number"),
                    "referenced_cages": set(),
                    "active_authorized_source_cages": set(),
                    "observed_contract_ids": observed_contracts.get(key, set()),
                },
            )
            entry["referenced_cages"].add(row["cage"])
            if row.get("is_active_authorized_source"):
                entry["active_authorized_source_cages"].add(row["cage"])
        return [
            {
                "part_number": row["part_number"],
                "referenced_cages": sorted(row["referenced_cages"]),
                "active_authorized_source_cages": sorted(row["active_authorized_source_cages"]),
                "observed_on_dla_transaction_line": bool(row["observed_contract_ids"]),
                "sample_observed_contract_ids": sorted(row["observed_contract_ids"])[:5],
            }
            for row in sorted(grouped.values(), key=lambda item: str(item["part_number"] or ""))
        ]

    @staticmethod
    def _source_index(
        niin: str,
        contracts: Sequence[Dict[str, Any]],
        linked_prime_awards: Sequence[Dict[str, Any]],
        platforms: Sequence[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        records: Dict[tuple[str, str], Dict[str, Any]] = {}
        records[("DLA_FLIS", niin)] = {
            "source": "DLA Federal Logistics Information System reference data",
            "record_locator": f"NIIN {niin}",
            "supports": "Item identity, part-number relationships and authorized-source status",
            "public_url": "https://www.dla.mil/Information-Operations/FLIS-Data-Electronic-Reading-Room/",
        }
        for contract in contracts:
            record_id = str(contract.get("contract_id") or "")
            if record_id:
                records[("DLA_CONTRACT_HISTORY", record_id)] = {
                    "source": "DLA contract history",
                    "record_locator": f"Contract {record_id}",
                    "supports": "Observed DLA procurement activity for the NIIN",
                    "public_url": "https://www.dibbs.bsm.dla.mil/",
                }
        for award in linked_prime_awards:
            record_id = str(award.get("contract_id") or "")
            if record_id:
                records[("USASPENDING_AWARD", record_id)] = {
                    "source": "USAspending federal award records",
                    "record_locator": f"Award {record_id}",
                    "supports": "Prime obligations on an award deterministically linked to the NIIN",
                    "public_url": (
                        "https://www.usaspending.gov/award/"
                        f"{award.get('award_key')}/"
                    ),
                }
        for platform in platforms:
            name = str(platform.get("platform_family") or "")
            records[("MIMIR_ITEM_PLATFORM", name)] = {
                "source": "Mimir item-platform reference model",
                "record_locator": f"NIIN {niin}; {name}",
                "supports": "Reference association between the NIIN and platform family",
                "public_url": None,
            }
        return list(records.values())
