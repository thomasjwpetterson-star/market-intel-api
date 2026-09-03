"""Deterministic contract and opportunity dossiers for Ask Mimir."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import duckdb

from platform_description_mapping import map_platform_candidates


DEFAULT_DATA_ROOT = Path(
    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data"
)
COMPLETED_FISCAL_YEARS = tuple(range(2021, 2026))


def _rows(cursor: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _date(value: Any) -> str | None:
    text = str(value or "").strip()
    return text[:10] if text else None


def _clean_code(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if re.fullmatch(r"\d+\.0", text):
        text = text[:-2]
    return text.upper()


def _buyer_family(*values: Any) -> str:
    text = " ".join(str(value or "").upper() for value in values)
    if "MISSILE DEFENSE" in text:
        return "MISSILE_DEFENSE_AGENCY"
    if "AIR FORCE" in text:
        return "AIR_FORCE"
    if "ARMY" in text:
        return "ARMY"
    if "NAVY" in text or "MARINE CORPS" in text:
        return "NAVY"
    if "DEFENSE LOGISTICS" in text or re.search(r"\bDLA\b", text):
        return "DLA"
    return re.sub(r"[^A-Z0-9]+", "_", text).strip("_")[:80]


def _requirement_accessibility(*values: Any) -> Dict[str, Any]:
    text = " ".join(str(value or "") for value in values).upper()
    signals = {
        "qualification_or_certification": ("QUALIF", "CERTIF", "APPROVED SOURCE"),
        "integration_or_system_test": ("INTEGRAT", "SYSTEM TEST", "FLIGHT TEST", "LIVE FIRE"),
        "source_control_or_technical_data": ("SOURCE CONTROL", "TECHNICAL DATA", "TDP", "DRAWING"),
        "special_facility_or_security": ("FACILITY CLEARANCE", "SECURITY CLEARANCE", "SPECIAL ACCESS", "FACILIT"),
        "production_system_complexity": ("MISSILE SYSTEM", "WEAPON SYSTEM", "INTERCEPTOR", "GUIDED MISSILE"),
    }
    observed = [name for name, terms in signals.items() if any(term in text for term in terms)]
    complex_requirement = len(observed) >= 2 or "SOURCES SOUGHT" in text and bool(observed)
    return {
        "classification": "COMPLEX_QUALIFIED_REQUIREMENT" if complex_requirement else "GENERAL_REQUIREMENT",
        "observed_complexity_signals": observed,
        "competitive_inference_rule": (
            "Only direct requirement or named-program history supports a plausible prime-level competitor. "
            "PSC, NAICS or buyer overlap alone supports broader industrial-base comparison or teaming context."
            if complex_requirement
            else "Direct requirement evidence is strongest; category and buyer overlap provide secondary context."
        ),
    }


class AwardOpportunityContextStore:
    """Resolve public identifiers and build bounded evidence packs."""

    def __init__(self, data_root: Path = DEFAULT_DATA_ROOT) -> None:
        self.data_root = data_root.resolve()
        self.paths = {
            name: self.data_root / filename
            for name, filename in {
                "contracts": "contracts_rolled.parquet",
                "transactions": "transactions.parquet",
                "opportunities": "opportunities.parquet",
                "network": "network.parquet",
                "locations": "cage_locations.parquet",
            }.items()
        }
        missing = [str(path) for path in self.paths.values() if not path.exists()]
        if missing:
            raise FileNotFoundError(f"award and opportunity sources are missing: {missing}")
        self.connection = duckdb.connect()
        self.connection.execute("SET preserve_insertion_order=false")
        self.connection.execute("SET threads=2")
        self.connection.execute("SET memory_limit='1GB'")
        self._cache: Dict[tuple[str, str], Dict[str, Any]] = {}

    def search(self, query: str, limit: int = 12) -> Dict[str, Any]:
        clean = str(query or "").strip()
        if not clean:
            return {"query": clean, "matches": [], "requires_disambiguation": False}
        cap = min(max(int(limit), 1), 20)
        exact = self._search_exact(clean, cap)
        matches = exact if exact else self._search_fuzzy(clean, cap)
        return {
            "query": clean,
            "match_type": "EXACT_IDENTIFIER" if exact else "TEXT_SEARCH",
            "matches": matches,
            "requires_disambiguation": len(matches) > 1,
            "resolved": matches[0] if len(matches) == 1 else None,
        }

    def get(self, record_type: str, record_id: str) -> Dict[str, Any]:
        clean_type = str(record_type).strip().lower()
        if clean_type not in {"contract", "opportunity"}:
            raise ValueError("record_type must be contract or opportunity")
        cache_key = (clean_type, str(record_id).upper())
        if cache_key in self._cache:
            return self._cache[cache_key]
        context = (
            self._contract_context(record_id)
            if clean_type == "contract"
            else self._opportunity_context(record_id)
        )
        self._cache[cache_key] = context
        return context

    def answer_projection(self, record_type: str, record_id: str) -> Dict[str, Any]:
        context = self.get(record_type, record_id)
        projected = dict(context)
        if record_type == "contract":
            projected["action_history"] = context["action_history"][:20]
            projected["reported_subaward_suppliers"] = context["reported_subaward_suppliers"][:20]
            projected["comparable_suppliers"] = context["comparable_suppliers"][:12]
        else:
            projected["likely_competitors"] = context["likely_competitors"][:12]
            projected["related_historical_awards"] = context["related_historical_awards"][:15]
            projected["related_current_opportunities"] = context["related_current_opportunities"][:10]
        return projected

    def _search_exact(self, query: str, limit: int) -> List[Dict[str, Any]]:
        contract_rows = _rows(
            self.connection.execute(
                """
                SELECT DISTINCT
                    'contract' AS record_type,
                    contract_id AS record_id,
                    contract_id AS public_identifier,
                    COALESCE(base_award_description, description) AS title,
                    vendor_name AS organization,
                    sub_agency AS agency,
                    last_action_date AS relevant_date
                FROM read_parquet(?)
                WHERE UPPER(TRIM(contract_id)) = UPPER(TRIM(?))
                LIMIT ?
                """,
                [str(self.paths["contracts"]), query, limit],
            )
        )
        opportunity_rows = _rows(
            self.connection.execute(
                """
                SELECT DISTINCT
                    'opportunity' AS record_type,
                    COALESCE(NULLIF(sol_num, ''), id) AS record_id,
                    COALESCE(NULLIF(sol_num, ''), id) AS public_identifier,
                    title,
                    agency AS organization,
                    sub_agency AS agency,
                    deadline AS relevant_date
                FROM read_parquet(?)
                WHERE UPPER(TRIM(id)) = UPPER(TRIM(?))
                   OR UPPER(TRIM(sol_num)) = UPPER(TRIM(?))
                LIMIT ?
                """,
                [str(self.paths["opportunities"]), query, query, limit],
            )
        )
        return self._label_matches(contract_rows + opportunity_rows)

    def _search_fuzzy(self, query: str, limit: int) -> List[Dict[str, Any]]:
        pattern = f"%{query}%"
        contracts = _rows(
            self.connection.execute(
                """
                SELECT
                    'contract' AS record_type,
                    contract_id AS record_id,
                    contract_id AS public_identifier,
                    COALESCE(base_award_description, description) AS title,
                    vendor_name AS organization,
                    sub_agency AS agency,
                    last_action_date AS relevant_date,
                    ABS(total_spend) AS sort_value
                FROM read_parquet(?)
                WHERE UPPER(contract_id) LIKE UPPER(?)
                   OR UPPER(COALESCE(base_award_description, description, '')) LIKE UPPER(?)
                ORDER BY sort_value DESC
                LIMIT ?
                """,
                [str(self.paths["contracts"]), pattern, pattern, limit],
            )
        )
        opportunities = _rows(
            self.connection.execute(
                """
                SELECT
                    'opportunity' AS record_type,
                    COALESCE(NULLIF(sol_num, ''), id) AS record_id,
                    COALESCE(NULLIF(sol_num, ''), id) AS public_identifier,
                    title,
                    agency AS organization,
                    sub_agency AS agency,
                    deadline AS relevant_date,
                    0::DOUBLE AS sort_value
                FROM read_parquet(?)
                WHERE UPPER(COALESCE(search_text, title, '')) LIKE UPPER(?)
                   OR UPPER(COALESCE(sol_num, id, '')) LIKE UPPER(?)
                ORDER BY deadline DESC
                LIMIT ?
                """,
                [str(self.paths["opportunities"]), pattern, pattern, limit],
            )
        )
        combined = contracts + opportunities
        combined.sort(key=lambda row: (row["record_type"] != "opportunity", -(row.get("sort_value") or 0)))
        return self._label_matches(combined[:limit])

    @staticmethod
    def _label_matches(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        for row in rows:
            row.pop("sort_value", None)
            row["relevant_date"] = _date(row.get("relevant_date"))
            row["option_label"] = " - ".join(
                value
                for value in (
                    row.get("public_identifier"),
                    row.get("title"),
                    row.get("organization"),
                )
                if value
            )
        return rows

    def _contract_row(self, record_id: str) -> Dict[str, Any]:
        rows = _rows(
            self.connection.execute(
                """
                SELECT *
                FROM read_parquet(?)
                WHERE UPPER(TRIM(contract_id)) = UPPER(TRIM(?))
                ORDER BY ABS(total_spend) DESC
                LIMIT 2
                """,
                [str(self.paths["contracts"]), record_id],
            )
        )
        if not rows:
            raise KeyError(f"contract was not found: {record_id}")
        if len(rows) > 1 and rows[0].get("award_key") != rows[1].get("award_key"):
            raise ValueError(f"contract identifier is ambiguous: {record_id}")
        return rows[0]

    def _opportunity_row(self, record_id: str) -> Dict[str, Any]:
        rows = _rows(
            self.connection.execute(
                """
                SELECT *
                FROM read_parquet(?)
                WHERE UPPER(TRIM(id)) = UPPER(TRIM(?))
                   OR UPPER(TRIM(sol_num)) = UPPER(TRIM(?))
                ORDER BY deadline DESC
                LIMIT 2
                """,
                [str(self.paths["opportunities"]), record_id, record_id],
            )
        )
        if not rows:
            raise KeyError(f"opportunity was not found: {record_id}")
        if len(rows) > 1 and rows[0].get("id") != rows[1].get("id"):
            raise ValueError(f"opportunity identifier is ambiguous: {record_id}")
        return rows[0]

    def _contract_context(self, record_id: str) -> Dict[str, Any]:
        row = self._contract_row(record_id)
        award_key = row.get("award_key")
        annual = [
            {
                "fiscal_year": year,
                "net_prime_obligations_usd": row.get(f"obligations_fy{year}") or 0.0,
                "action_count": row.get(f"action_count_fy{year}") or 0,
                "earliest_action_date": _date(row.get(f"earliest_action_date_fy{year}")),
                "latest_action_date": _date(row.get(f"latest_action_date_fy{year}")),
                "observation_status": "OBSERVED" if row.get(f"action_count_fy{year}") else "NOT_OBSERVED",
            }
            for year in range(2019, 2027)
        ]
        actions = self._contract_actions(row["contract_id"], award_key)
        subawards = self._contract_subawards(row["contract_id"], award_key)
        related_opportunities = self._related_opportunities(
            row.get("psc"), row.get("naics_code"), row.get("solicitation_id"), limit=10
        )
        comparable = self._comparable_suppliers(
            psc=row.get("psc"),
            naics=row.get("naics_code"),
            agency=row.get("parent_agency"),
            sub_agency=row.get("sub_agency"),
            exclude_cage=row.get("vendor_cage"),
            limit=15,
        )
        identity = {
            "record_type": "contract",
            "contract_id": row.get("contract_id"),
            "recipient_name": row.get("vendor_name"),
            "recipient_cage": row.get("vendor_cage"),
            "base_award_description": row.get("base_award_description") or row.get("description"),
            "latest_action_description": row.get("latest_action_description"),
            "awarding_agency": row.get("parent_agency"),
            "awarding_sub_agency": row.get("sub_agency"),
            "start_date": _date(row.get("start_date")),
            "latest_action_date": _date(row.get("last_action_date")),
            "psc": row.get("psc"),
            "naics_code": row.get("naics_code"),
            "naics_description": row.get("naics_description"),
            "competition_type": row.get("competition_type"),
            "offers_received": row.get("offers_count"),
            "pricing_type": row.get("pricing_type"),
            "set_aside_type": row.get("set_aside_type"),
            "solicitation_identifier": row.get("solicitation_id"),
            "platform": row.get("platform_family"),
            "platforms": row.get("platform_families"),
            "contracting_location": {
                "city": row.get("city"), "state": row.get("state"), "country": row.get("country")
            },
            "place_of_performance": {
                "city": row.get("place_of_performance_city"),
                "state": row.get("place_of_performance_state"),
                "country": row.get("place_of_performance_country"),
                "postal_code": row.get("place_of_performance_zip"),
            },
        }
        context = {
            "context_type": "contract_dossier",
            "calculation_version": "mimir-contract-opportunity-2026-09-v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "identity": identity,
            "financial_summary": {
                "net_prime_obligations_all_loaded_years_usd": row.get("total_spend") or 0.0,
                "annual_obligations": annual,
                "loaded_fiscal_years": list(range(2019, 2027)),
                "partial_fiscal_year": 2026,
            },
            "action_history": actions,
            "reported_subaward_suppliers": subawards,
            "comparable_suppliers": comparable,
            "related_opportunities": related_opportunities,
            "evidence_index": self._contract_sources(row, actions, related_opportunities),
            "methodology": {
                "award_value": "Net prime obligations are summed from reported contract actions in the loaded fiscal years.",
                "subaward_value": "Mimir-modelled reported subcontract value uses the current curated subaward methodology and remains separate from prime obligations.",
                "comparables": "Comparable suppliers are ranked from completed-year awards using shared PSC, NAICS and buying-organization evidence; they are not identified bidders.",
            },
        }
        context["evidence_fingerprint"] = self._fingerprint(context)
        return context

    def _opportunity_context(self, record_id: str) -> Dict[str, Any]:
        row = self._opportunity_row(record_id)
        psc = _clean_code(row.get("psc"))
        naics = _clean_code(row.get("naics"))
        platform_candidates = map_platform_candidates(row.get("description") or row.get("title") or "")
        accessibility = _requirement_accessibility(
            row.get("title"), row.get("description"), row.get("psc"), row.get("naics")
        )
        competitors = self._comparable_suppliers(
            psc=psc,
            naics=naics,
            agency=row.get("agency"),
            sub_agency=row.get("sub_agency"),
            exclude_cage=None,
            limit=20,
        )
        program_terms = sorted(
            {
                alias
                for candidate in platform_candidates
                if candidate.get("status") != "REJECTED_AMBIGUOUS"
                for alias in (
                    candidate.get("matched_description_aliases") or []
                )
                if len(alias) >= 4
            }
        )
        program_incumbents = self._program_incumbents(program_terms)
        incumbent_by_cage = {row["cage"]: row for row in program_incumbents}
        for competitor in competitors:
            incumbent = incumbent_by_cage.get(competitor.get("cage"))
            if incumbent:
                competitor["program_match_awards"] = incumbent["program_match_awards"]
                competitor["program_match_obligations_usd"] = incumbent["program_match_obligations_usd"]
                competitor["program_match_contract_ids"] = incumbent["program_match_contract_ids"]
                competitor["program_match_descriptions"] = incumbent["program_match_descriptions"]
                competitor["relevance_score"] += 5
                competitor["relevance_band"] = "DIRECT_PROGRAM_HISTORY"
                competitor["relevance_reasons"] = [
                    "direct historical awards naming the program",
                    *competitor["relevance_reasons"],
                ]
                competitor["candidate_role"] = "DIRECT_PROGRAM_INCUMBENT_EVIDENCE"
                competitor["interpretation"] = (
                    "Direct named-program award history; relevant incumbent evidence, not a confirmed bidder."
                )
            else:
                competitor["candidate_role"] = "INDUSTRIAL_BASE_COMPARABLE"
                if accessibility["classification"] == "COMPLEX_QUALIFIED_REQUIREMENT":
                    competitor["interpretation"] = (
                        "Broader industrial-base comparable or teaming context; not evidence of qualification "
                        "for this requirement and not a confirmed bidder."
                    )
        competitors.sort(
            key=lambda item: (
                item["relevance_score"],
                abs(item.get("program_match_obligations_usd") or 0),
                abs(item.get("completed_year_prime_obligations_usd") or 0),
            ),
            reverse=True,
        )
        related_awards = self._related_awards(
            psc, naics, row.get("agency"), row.get("sub_agency"), limit=20
        )
        related_opportunities = self._related_opportunities(
            psc, naics, exclude_identifier=row.get("id"), limit=12
        )
        deadline = _date(row.get("deadline"))
        today = datetime.now(timezone.utc).date().isoformat()
        identity = {
            "record_type": "opportunity",
            "opportunity_id": row.get("id"),
            "solicitation_number": row.get("sol_num"),
            "title": row.get("title"),
            "agency": row.get("agency"),
            "sub_agency": row.get("sub_agency"),
            "response_deadline": deadline,
            "response_status": "OPEN" if deadline and deadline >= today else "CLOSED",
            "response_deadline_source_value": row.get("deadline"),
            "set_aside_type": row.get("set_aside_type"),
            "naics_code": naics,
            "psc": psc,
            "state": row.get("state"),
            "description": row.get("description"),
            "public_notice_url": row.get("url"),
            "point_of_contact_email": row.get("poc_email"),
        }
        context = {
            "context_type": "opportunity_dossier",
            "calculation_version": "mimir-contract-opportunity-2026-09-v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "identity": identity,
            "requirement_accessibility": accessibility,
            "likely_competitors": competitors,
            "direct_program_award_recipients": program_incumbents,
            "related_historical_awards": related_awards,
            "related_current_opportunities": related_opportunities,
            "description_platform_candidates": platform_candidates,
            "evidence_index": self._opportunity_sources(row, related_awards),
            "methodology": {
                "competitor_definition": "Direct named-program history is incumbent evidence. Other firms are broader industrial-base comparables or possible teaming context unless requirement-level evidence supports qualification.",
                "platform_treatment": "Description matches are research candidates only and do not alter financial platform attribution.",
                "financial_window": "Comparable award values use completed FY2021-FY2025 net prime obligations.",
            },
        }
        context["evidence_fingerprint"] = self._fingerprint(context)
        return context

    def _program_incumbents(self, aliases: List[str]) -> List[Dict[str, Any]]:
        if not aliases:
            return []
        conditions = " OR ".join(
            "UPPER(COALESCE(base_award_description, description, '')) LIKE ?"
            for _ in aliases
        )
        params = [f"%{alias.upper()}%" for alias in aliases]
        return _rows(
            self.connection.execute(
                f"""
                SELECT
                    vendor_cage AS cage,
                    MAX(vendor_name) AS supplier_name,
                    MAX(city) AS city,
                    MAX(state) AS state,
                    COUNT(DISTINCT award_key) AS program_match_awards,
                    SUM(COALESCE(obligations_fy2021,0)+COALESCE(obligations_fy2022,0)+
                        COALESCE(obligations_fy2023,0)+COALESCE(obligations_fy2024,0)+
                        COALESCE(obligations_fy2025,0)) AS program_match_obligations_usd,
                    LIST_SLICE(LIST_DISTINCT(LIST(contract_id) FILTER (WHERE contract_id IS NOT NULL)), 1, 8) AS program_match_contract_ids,
                    LIST_SLICE(LIST_DISTINCT(LIST(COALESCE(base_award_description, description)) FILTER (WHERE COALESCE(base_award_description, description) IS NOT NULL)), 1, 8) AS program_match_descriptions
                FROM read_parquet(?)
                WHERE year BETWEEN 2021 AND 2025 AND ({conditions})
                GROUP BY vendor_cage
                ORDER BY ABS(program_match_obligations_usd) DESC
                LIMIT 25
                """,
                [str(self.paths["contracts"]), *params],
            )
        )

    def _contract_actions(self, contract_id: str, award_key: str | None) -> List[Dict[str, Any]]:
        return _rows(
            self.connection.execute(
                """
                SELECT
                    contract_id,
                    modification_number,
                    SUBSTR(action_date, 1, 10) AS action_date,
                    year AS fiscal_year,
                    spend_amount AS prime_obligation_usd,
                    action_description,
                    base_award_description,
                    vendor_name AS recipient_name,
                    vendor_cage AS recipient_cage,
                    psc,
                    naics_code,
                    platform_family AS platform,
                    place_of_performance_city,
                    place_of_performance_state,
                    place_of_performance_country
                FROM read_parquet(?)
                WHERE (? IS NOT NULL AND award_key = ?)
                   OR (? IS NULL AND UPPER(contract_id) = UPPER(?))
                ORDER BY action_date DESC, ABS(spend_amount) DESC
                """,
                [str(self.paths["transactions"]), award_key, award_key, award_key, contract_id],
            )
        )

    def _contract_subawards(self, contract_id: str, award_key: str | None) -> List[Dict[str, Any]]:
        return _rows(
            self.connection.execute(
                """
                WITH locations AS (
                    SELECT UPPER(TRIM(cage_code)) AS cage, MAX(vendor_name) AS location_vendor_name,
                           MAX(city) AS city, MAX(state) AS state
                    FROM read_parquet(?) GROUP BY 1
                )
                SELECT
                    n.sub_cage AS supplier_cage,
                    COALESCE(MAX(n.sub_name), MAX(l.location_vendor_name)) AS supplier_name,
                    MAX(COALESCE(n.sub_city, l.city)) AS city,
                    MAX(COALESCE(n.sub_state, l.state)) AS state,
                    MAX(n.sub_country) AS country,
                    SUM(COALESCE(n.subaward_value, 0)) AS mimir_modelled_reported_subcontract_value_usd,
                    SUM(COALESCE(n.subaward_value_raw, 0)) AS source_reported_value_usd,
                    COUNT(*) AS selected_report_count,
                    COUNT(DISTINCT n.invoice_id) AS reported_action_count,
                    MIN(SUBSTR(n.action_date, 1, 10)) AS first_reported_date,
                    MAX(SUBSTR(n.action_date, 1, 10)) AS latest_reported_date,
                    LIST_SLICE(LIST_DISTINCT(LIST(n.description) FILTER (WHERE n.description IS NOT NULL)), 1, 8) AS reported_descriptions
                FROM read_parquet(?) n
                LEFT JOIN locations l ON UPPER(TRIM(n.sub_cage)) = l.cage
                WHERE ((? IS NOT NULL AND n.award_key = ?)
                    OR (? IS NULL AND UPPER(n.contract_id) = UPPER(?)))
                  AND n.sub_cage IS NOT NULL
                GROUP BY n.sub_cage
                ORDER BY ABS(mimir_modelled_reported_subcontract_value_usd) DESC
                """,
                [str(self.paths["locations"]), str(self.paths["network"]), award_key, award_key, award_key, contract_id],
            )
        )

    def _comparable_suppliers(
        self,
        psc: Any,
        naics: Any,
        agency: Any,
        sub_agency: Any,
        exclude_cage: Any,
        limit: int,
    ) -> List[Dict[str, Any]]:
        clean_psc, clean_naics = _clean_code(psc), _clean_code(naics)
        if not clean_psc and not clean_naics:
            return []
        rows = _rows(
            self.connection.execute(
                """
                SELECT
                    vendor_cage AS cage,
                    MAX(vendor_name) AS supplier_name,
                    MAX(city) AS city,
                    MAX(state) AS state,
                    SUM(COALESCE(obligations_fy2021,0)+COALESCE(obligations_fy2022,0)+
                        COALESCE(obligations_fy2023,0)+COALESCE(obligations_fy2024,0)+
                        COALESCE(obligations_fy2025,0)) AS completed_year_prime_obligations_usd,
                    COUNT(DISTINCT award_key) AS award_count,
                    COUNT(DISTINCT award_key) FILTER (WHERE UPPER(TRIM(psc)) = ?) AS matching_psc_awards,
                    COUNT(DISTINCT award_key) FILTER (WHERE UPPER(TRIM(naics_code)) = ?) AS matching_naics_awards,
                    LIST_SLICE(LIST_DISTINCT(LIST(parent_agency) FILTER (WHERE parent_agency IS NOT NULL)), 1, 6) AS parent_agencies,
                    LIST_SLICE(LIST_DISTINCT(LIST(sub_agency) FILTER (WHERE sub_agency IS NOT NULL)), 1, 8) AS sub_agencies,
                    LIST_SLICE(LIST_DISTINCT(LIST(contract_id) FILTER (WHERE contract_id IS NOT NULL)), 1, 5) AS sample_contract_ids,
                    LIST_SLICE(LIST_DISTINCT(LIST(COALESCE(base_award_description, description)) FILTER (WHERE COALESCE(base_award_description, description) IS NOT NULL)), 1, 5) AS sample_award_descriptions
                FROM read_parquet(?)
                WHERE year BETWEEN 2021 AND 2025
                  AND (UPPER(TRIM(psc)) = ? OR UPPER(TRIM(naics_code)) = ?)
                  AND (? IS NULL OR UPPER(TRIM(vendor_cage)) <> UPPER(TRIM(?)))
                GROUP BY vendor_cage
                ORDER BY ABS(completed_year_prime_obligations_usd) DESC
                LIMIT 250
                """,
                [clean_psc or "", clean_naics or "", str(self.paths["contracts"]), clean_psc or "", clean_naics or "", exclude_cage, exclude_cage],
            )
        )
        target_buyer = _buyer_family(agency, sub_agency)
        ranked = []
        for row in rows:
            buyer_match = any(
                _buyer_family(value) == target_buyer
                for value in (row.get("parent_agencies") or []) + (row.get("sub_agencies") or [])
            )
            psc_match = bool(row.get("matching_psc_awards"))
            naics_match = bool(row.get("matching_naics_awards"))
            score = (2 if psc_match else 0) + (1 if naics_match else 0) + (2 if buyer_match else 0)
            if psc_match and naics_match and buyer_match:
                band = "HIGH_RELEVANCE"
            elif score >= 3:
                band = "MODERATE_RELEVANCE"
            else:
                band = "ADJACENT_RELEVANCE"
            reasons = []
            if psc_match:
                reasons.append(f"awards in PSC {clean_psc}")
            if naics_match:
                reasons.append(f"awards in NAICS {clean_naics}")
            if buyer_match:
                reasons.append("history with the same service or buying organization")
            row.update(
                {
                    "relevance_band": band,
                    "relevance_score": score,
                    "relevance_reasons": reasons,
                    "interpretation": "Historically relevant supplier; not a confirmed bidder.",
                }
            )
            ranked.append(row)
        ranked.sort(
            key=lambda row: (row["relevance_score"], abs(row.get("completed_year_prime_obligations_usd") or 0)),
            reverse=True,
        )
        return ranked[:limit]

    def _related_awards(self, psc: Any, naics: Any, agency: Any, sub_agency: Any, limit: int) -> List[Dict[str, Any]]:
        clean_psc, clean_naics = _clean_code(psc), _clean_code(naics)
        rows = _rows(
            self.connection.execute(
                """
                SELECT contract_id, vendor_name AS recipient_name, vendor_cage AS recipient_cage,
                       COALESCE(base_award_description, description) AS base_award_description,
                       psc, naics_code,
                       COALESCE(obligations_fy2021,0)+COALESCE(obligations_fy2022,0)+
                       COALESCE(obligations_fy2023,0)+COALESCE(obligations_fy2024,0)+
                       COALESCE(obligations_fy2025,0) AS completed_year_prime_obligations_usd,
                       action_count, start_date, last_action_date, parent_agency, sub_agency,
                       place_of_performance_city, place_of_performance_state
                FROM read_parquet(?)
                WHERE year BETWEEN 2021 AND 2025
                  AND (UPPER(TRIM(psc)) = ? OR UPPER(TRIM(naics_code)) = ?)
                ORDER BY ABS(completed_year_prime_obligations_usd) DESC
                LIMIT ?
                """,
                [str(self.paths["contracts"]), clean_psc or "", clean_naics or "", limit * 4],
            )
        )
        target_buyer = _buyer_family(agency, sub_agency)
        for row in rows:
            row["same_buyer_family"] = _buyer_family(row.get("parent_agency"), row.get("sub_agency")) == target_buyer
            row["start_date"] = _date(row.get("start_date"))
            row["last_action_date"] = _date(row.get("last_action_date"))
        rows.sort(key=lambda row: (row["same_buyer_family"], abs(row.get("completed_year_prime_obligations_usd") or 0)), reverse=True)
        return rows[:limit]

    def _related_opportunities(self, psc: Any, naics: Any, exclude_identifier: Any, limit: int) -> List[Dict[str, Any]]:
        clean_psc, clean_naics = _clean_code(psc), _clean_code(naics)
        rows = _rows(
            self.connection.execute(
                """
                SELECT id, sol_num, title, agency, sub_agency, deadline,
                       set_aside_type, CAST(naics AS BIGINT)::VARCHAR AS naics_code,
                       psc, state, url
                FROM read_parquet(?)
                WHERE (UPPER(TRIM(psc)) = ? OR CAST(CAST(naics AS BIGINT) AS VARCHAR) = ?)
                  AND (? IS NULL OR (UPPER(COALESCE(id,'')) <> UPPER(?) AND UPPER(COALESCE(sol_num,'')) <> UPPER(?)))
                ORDER BY deadline ASC
                LIMIT ?
                """,
                [str(self.paths["opportunities"]), clean_psc or "", clean_naics or "", exclude_identifier, exclude_identifier, exclude_identifier, limit],
            )
        )
        for row in rows:
            row["deadline"] = _date(row.get("deadline"))
        return rows

    @staticmethod
    def _contract_sources(row: Dict[str, Any], actions: List[Dict[str, Any]], opportunities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        sources = [{
            "source": "USAspending award and transaction records",
            "public_record_id": row.get("contract_id"),
            "supports": "award identity, recipient, descriptions, obligations and action history",
            "public_url": f"https://www.usaspending.gov/award/{row.get('award_key')}" if row.get("award_key") else None,
        }]
        for opportunity in opportunities:
            if opportunity.get("url"):
                sources.append({
                    "source": "SAM.gov opportunity notice",
                    "public_record_id": opportunity.get("sol_num") or opportunity.get("id"),
                    "supports": "related current opportunity",
                    "public_url": opportunity.get("url"),
                })
        return sources

    @staticmethod
    def _opportunity_sources(row: Dict[str, Any], awards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        sources = [{
            "source": "SAM.gov opportunity notice",
            "public_record_id": row.get("sol_num") or row.get("id"),
            "supports": "notice scope, response date, classification and point of contact",
            "public_url": row.get("url"),
        }]
        for award in awards[:10]:
            sources.append({
                "source": "USAspending award record",
                "public_record_id": award.get("contract_id"),
                "supports": "historical comparable award and recipient evidence",
                "public_url": None,
            })
        return sources

    @staticmethod
    def _fingerprint(context: Dict[str, Any]) -> str:
        material = {key: value for key, value in context.items() if key not in {"generated_at", "evidence_fingerprint"}}
        return hashlib.sha256(json.dumps(material, default=str, sort_keys=True).encode()).hexdigest()
