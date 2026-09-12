"""Reusable, release-bound platform and program forward views for Ask Mimir."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

import duckdb


ROOT = Path(__file__).resolve().parent
DEFAULT_DEFINITIONS = ROOT / "fydp_platform_linkages.json"
COMPLETED_FISCAL_YEARS = tuple(range(2021, 2026))
PARTIAL_FISCAL_YEAR = 2026


def _normalize(value: Any) -> str:
    return " ".join(re.findall(r"[A-Z0-9]+", str(value or "").upper()))


def _rows(cursor: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _placeholders(values: Iterable[Any]) -> str:
    materialized = list(values)
    return ", ".join("?" for _ in materialized) or "NULL"


def is_program_outlook_language(text: str) -> bool:
    """Recognize questions that need a named program's structured forward view."""
    lowered = str(text or "").lower()
    if any(
        phrase in lowered
        for phrase in (
            "outlook",
            "forward view",
            "future demand",
            "future funding",
            "funding trajectory",
            "budget trajectory",
            "production trajectory",
            "production forecast",
            "five-year",
            "five year",
            "next five years",
            "next 5 years",
            "fydp",
            "outyear",
            "what lies ahead",
        )
    ):
        return True
    if "production" in lowered and any(
        term in lowered
        for term in ("current", "currently", "happening", "ramp", "increase", "future", "next", "through")
    ):
        return True
    return "budget" in lowered and any(
        term in lowered
        for term in ("current", "future", "next", "request", "enacted", "projection")
    )


class ProgramOutlookStore:
    """Join current release evidence into one non-additive program outlook."""

    def __init__(
        self,
        data_root: Path,
        platform_contexts: Any,
        *,
        definitions_path: Path = DEFAULT_DEFINITIONS,
        dod_budget_path: Path | None = None,
        fydp_budget_path: Path | None = None,
        announcement_path: Path | None = None,
    ) -> None:
        self.data_root = data_root.resolve()
        self.platform_contexts = platform_contexts
        self.definitions_path = definitions_path.resolve()
        self.catalogue = json.loads(self.definitions_path.read_text())
        self.definitions = list(self.catalogue.get("linkages", []))
        self.dod_budget_path = (
            dod_budget_path
            or Path(
                os.getenv(
                    "ASK_MIMIR_DOD_BUDGET_FILE",
                    str(self.data_root / "dod_budget_facts.parquet"),
                )
            )
        ).resolve()
        self.fydp_budget_path = (
            fydp_budget_path
            or Path(
                os.getenv(
                    "ASK_MIMIR_FYDP_BUDGET_FILE",
                    str(self.data_root / "dod_fydp_budget_facts.parquet"),
                )
            )
        ).resolve()
        self.announcement_path = (
            announcement_path
            or Path(
                os.getenv(
                    "ASK_MIMIR_DOD_ANNOUNCEMENT_FILE",
                    str(self.data_root / "dod_contract_announcements.parquet"),
                )
            )
        ).resolve()
        self.transactions_path = self.data_root / "transactions.parquet"
        self.connection = duckdb.connect()
        self.connection.execute("SET preserve_insertion_order=false")
        self.connection.execute("SET threads=2")
        self.connection.execute("SET memory_limit='768MB'")
        duckdb_temp = os.getenv("ASK_MIMIR_DUCKDB_TEMP", "/tmp/ask-mimir-duckdb")
        self.connection.execute("SET temp_directory = ?", [duckdb_temp])

    @property
    def source_paths(self) -> List[Path]:
        return [
            path
            for path in (
                self.definitions_path,
                self.dod_budget_path,
                self.fydp_budget_path,
                self.announcement_path,
            )
            if path.exists()
        ]

    def _definition(self, platform_id: str) -> Dict[str, Any] | None:
        requested = _normalize(platform_id)
        if not requested:
            return None
        for definition in self.definitions:
            identifiers = {
                _normalize(definition.get("program_id")),
                _normalize(definition.get("display_name")),
                *{
                    _normalize(value)
                    for value in definition.get("platform_aliases", [])
                },
            }
            if requested in identifiers:
                return definition
        resolution = self.platform_contexts.search(platform_id)
        resolved = _normalize(resolution.get("resolved_platform_id"))
        if resolved and resolved != requested:
            return self._definition(resolution["resolved_platform_id"])
        return None

    def supports(self, platform_id: str) -> bool:
        return self._definition(platform_id) is not None

    def _budget_rows(self, definition: Dict[str, Any]) -> List[Dict[str, Any]]:
        aliases = sorted(
            {
                str(value).strip().upper()
                for value in definition.get("budget_title_aliases", [])
                if str(value or "").strip()
            }
        )
        if not aliases:
            return []
        rows: List[Dict[str, Any]] = []
        if self.dod_budget_path.exists():
            query = f"""
                WITH selected_rows AS (
                    SELECT
                        exhibit_type,
                        organization AS component,
                        CAST(line_number AS VARCHAR) AS budget_line_number,
                        budget_line_item,
                        budget_line_item_title,
                        fiscal_year,
                        CASE fiscal_year
                            WHEN 2025 THEN 'actual'
                            WHEN 2026 THEN 'enacted_and_spend_plan_total'
                            WHEN 2027 THEN 'total_request'
                        END AS funding_status,
                        measure_type,
                        amount_usd,
                        quantity,
                        source_file,
                        source_row_number
                    FROM read_parquet(?)
                    WHERE exhibit_type IN ('P-1', 'R-1')
                      AND funding_status = 'Total'
                      AND fiscal_year BETWEEN 2025 AND 2027
                      AND COALESCE(is_additive, true)
                      AND UPPER(TRIM(budget_line_item_title)) IN (
                          {_placeholders(aliases)}
                      )
                )
                SELECT
                    exhibit_type,
                    component,
                    budget_line_number,
                    budget_line_item,
                    budget_line_item_title,
                    fiscal_year,
                    funding_status,
                    CASE
                        WHEN exhibit_type = 'P-1' AND measure_type = 'amount'
                            THEN 'net_procurement_p1'
                        WHEN exhibit_type = 'P-1' AND measure_type = 'quantity'
                            THEN 'procurement_quantity'
                        WHEN exhibit_type = 'R-1' AND measure_type = 'amount'
                            THEN 'rdte_r1'
                    END AS measure_type,
                    SUM(amount_usd) AS amount_usd,
                    SUM(quantity) AS quantity,
                    'PUBLISHED' AS availability_status,
                    MAX(source_file) AS source_id,
                    CONCAT('Department of Defense FY2027 ', exhibit_type, ' display table')
                        AS source_document_title,
                    NULL::BIGINT AS source_page_number,
                    'https://comptroller.defense.gov/Budget-Materials/'
                        AS source_landing_page,
                    NULL::VARCHAR AS source_download_url,
                    STRING_AGG(
                        DISTINCT CONCAT(source_file, ' row ', source_row_number),
                        ' | ' ORDER BY CONCAT(source_file, ' row ', source_row_number)
                    ) AS source_locator
                FROM selected_rows
                WHERE measure_type IN ('amount', 'quantity')
                  AND NOT (exhibit_type = 'R-1' AND measure_type = 'quantity')
                GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
                ORDER BY fiscal_year, exhibit_type, component, budget_line_number, measure_type
            """
            rows.extend(
                _rows(
                    self.connection.execute(
                        query,
                        [str(self.dod_budget_path), *aliases],
                    )
                )
            )

        if self.fydp_budget_path.exists():
            first_year = 2028 if self.dod_budget_path.exists() else 2025
            query = f"""
                SELECT
                    'P-1' AS exhibit_type,
                    component,
                    CAST(p1_line_number AS VARCHAR) AS budget_line_number,
                    budget_line_item,
                    budget_line_item_title,
                    fiscal_year,
                    funding_status,
                    measure_type,
                    amount_usd,
                    quantity,
                    availability_status,
                    source_id,
                    source_document_title,
                    source_page_number,
                    source_landing_page,
                    source_download_url,
                    source_locator
                FROM read_parquet(?)
                WHERE UPPER(TRIM(budget_line_item_title)) IN (
                    {_placeholders(aliases)}
                )
                  AND measure_type IN ('net_procurement_p1', 'procurement_quantity')
                  AND availability_status = 'PUBLISHED'
                  AND fiscal_year BETWEEN ? AND 2031
                  AND (
                      (fiscal_year = 2025 AND funding_status = 'actual')
                      OR (fiscal_year = 2026 AND funding_status = 'enacted')
                      OR (fiscal_year = 2027 AND funding_status = 'total_request')
                      OR (fiscal_year BETWEEN 2028 AND 2031 AND funding_status = 'projected')
                  )
                ORDER BY fiscal_year, component, budget_line_number, measure_type
            """
            rows.extend(
                _rows(
                    self.connection.execute(
                        query,
                        [str(self.fydp_budget_path), *aliases, first_year],
                    )
                )
            )

        for row in rows:
            year = int(row["fiscal_year"])
            row["planning_phase"] = (
                "actual"
                if year <= 2025
                else "enacted"
                if year == 2026
                else "request"
                if year == 2027
                else "projection"
            )
        return rows

    @staticmethod
    def _announcement_pattern(definition: Dict[str, Any]) -> str:
        aliases = {
            str(definition.get("display_name") or ""),
            *{str(value) for value in definition.get("platform_aliases", [])},
        }
        expressions = []
        for alias in sorted(aliases, key=len, reverse=True):
            tokens = re.findall(r"[A-Z0-9]+", alias.upper())
            if not tokens:
                continue
            expressions.append(r"[^A-Z0-9]+".join(re.escape(token) for token in tokens))
        if not expressions:
            return r"a^"
        return r"(^|[^A-Z0-9])(?:" + "|".join(expressions) + r")([^A-Z0-9]|$)"

    def _announcements(
        self,
        definition: Dict[str, Any],
        platform_members: List[str],
        *,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        if not self.announcement_path.exists() or not self.transactions_path.exists():
            return []
        pattern = self._announcement_pattern(definition)
        cursor = self.connection.execute(
            """
            WITH scope_contracts AS (
                SELECT DISTINCT REGEXP_REPLACE(
                    UPPER(COALESCE(CAST(contract_id AS VARCHAR), '')),
                    '[^A-Z0-9]', '', 'g'
                ) AS normalized_contract_id
                FROM read_parquet(?)
                WHERE platform_family IN (SELECT UNNEST(?))
                  AND COALESCE(CAST(contract_id AS VARCHAR), '') <> ''
            ), candidates AS (
                SELECT
                    a.*,
                    CASE WHEN
                        REGEXP_REPLACE(
                            UPPER(COALESCE(a.primary_contract_id, '')),
                            '[^A-Z0-9]', '', 'g'
                        ) IN (SELECT normalized_contract_id FROM scope_contracts)
                        OR EXISTS (
                            SELECT 1
                            FROM UNNEST(a.contract_ids) ids(contract_id)
                            WHERE REGEXP_REPLACE(
                                UPPER(COALESCE(ids.contract_id, '')),
                                '[^A-Z0-9]', '', 'g'
                            ) IN (SELECT normalized_contract_id FROM scope_contracts)
                        )
                    THEN 'LINKED_CONTRACT_ID'
                    ELSE 'PROGRAM_NAME_IN_ANNOUNCEMENT'
                    END AS match_basis
                FROM read_parquet(?) a
                WHERE
                    REGEXP_REPLACE(
                        UPPER(COALESCE(a.primary_contract_id, '')),
                        '[^A-Z0-9]', '', 'g'
                    ) IN (SELECT normalized_contract_id FROM scope_contracts)
                    OR EXISTS (
                        SELECT 1
                        FROM UNNEST(a.contract_ids) ids(contract_id)
                        WHERE REGEXP_REPLACE(
                            UPPER(COALESCE(ids.contract_id, '')),
                            '[^A-Z0-9]', '', 'g'
                        ) IN (SELECT normalized_contract_id FROM scope_contracts)
                    )
                    OR REGEXP_MATCHES(
                        UPPER(CONCAT_WS(' ', a.search_text, a.description, a.source_title)),
                        ?
                    )
            )
            SELECT
                announcement_id,
                CAST(announcement_date AS VARCHAR) AS announcement_date,
                service,
                entry_index,
                entry_type,
                recipient_text,
                primary_contract_id,
                contract_ids,
                announced_value_usd,
                obligated_at_announcement_usd,
                work_locations,
                completion_text AS period_of_performance_text,
                competition_text,
                contracting_activity,
                description,
                source_title,
                source_url,
                CAST(source_published_at AS VARCHAR) AS source_published_at,
                CAST(retrieved_at AS VARCHAR) AS retrieved_at,
                match_basis
            FROM candidates
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY announcement_id, entry_index
                ORDER BY CASE match_basis WHEN 'LINKED_CONTRACT_ID' THEN 0 ELSE 1 END
            ) = 1
            ORDER BY announcement_date DESC, entry_index DESC
            LIMIT ?
            """,
            [
                str(self.transactions_path),
                platform_members,
                str(self.announcement_path),
                pattern,
                min(max(int(limit), 1), 40),
            ],
        )
        return _rows(cursor)

    @staticmethod
    def _historical_obligations(platform: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = []
        for row in platform.get("annual_activity", {}).get("records", []):
            if str(row.get("source_system") or "").upper() != "USA_SPENDING":
                continue
            year = int(row["fiscal_year"])
            rows.append(
                {
                    "fiscal_year": year,
                    "period_status": (
                        "completed" if year in COMPLETED_FISCAL_YEARS else "partial"
                    ),
                    "net_prime_obligations_usd": row.get("net_prime_obligations_usd"),
                    "positive_prime_obligations_usd": row.get(
                        "positive_prime_obligations_usd"
                    ),
                    "prime_deobligations_usd": row.get("prime_deobligations_usd"),
                    "award_count": row.get("award_count"),
                    "action_count": row.get("action_or_line_count"),
                }
            )
        return sorted(rows, key=lambda row: row["fiscal_year"])

    def get(self, *, platform_id: str) -> Dict[str, Any]:
        definition = self._definition(platform_id)
        if definition is None:
            raise KeyError(f"structured program outlook is unavailable: {platform_id}")
        resolution = self.platform_contexts.search(platform_id)
        resolved = resolution.get("resolved_platform_id") or platform_id
        platform = self.platform_contexts.get(resolved)
        platform_members = list(self.platform_contexts._platform_members(resolved))
        budget_rows = self._budget_rows(definition)
        announcements = self._announcements(definition, platform_members)
        opportunities = [
            row
            for row in platform.get("current_opportunities", [])
            if row.get("response_status") == "OPEN"
        ]
        obligations = self._historical_obligations(platform)
        quantity_rows = [
            row
            for row in budget_rows
            if row.get("measure_type") == "procurement_quantity"
            and row.get("quantity") is not None
        ]
        available_lanes = [
            lane
            for lane, values in (
                ("historical_prime_obligations", obligations),
                ("official_contract_announcements", announcements),
                ("budget_and_fydp", budget_rows),
                ("explicit_procurement_quantities", quantity_rows),
                ("open_solicitations", opportunities),
            )
            if values
        ]
        return {
            "context_type": "structured_program_outlook",
            "schema_version": "mimir-program-outlook-2026-09-v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "definition_version": self.catalogue.get("definition_version"),
            "scope": {
                "program_id": definition.get("program_id"),
                "display_name": definition.get("display_name"),
                "resolved_platform_id": resolved,
                "included_platform_records": platform_members,
                "historical_observation_window": "FY2021-FY2025 completed; FY2026 partial",
                "planning_horizon": "FY2025-FY2031 where published",
            },
            "evidence_lanes": {
                "historical_prime_obligations": obligations,
                "official_contract_announcements": announcements,
                "budget_and_fydp": budget_rows,
                "explicit_procurement_quantities": quantity_rows,
                "open_solicitations": opportunities,
            },
            "leading_historical_awards": platform.get("top_prime_awards", [])[:10],
            "coverage": {
                "available_lanes": available_lanes,
                "budget_line_aliases": definition.get("budget_title_aliases", []),
                "announcement_count": len(announcements),
                "open_solicitation_count": len(opportunities),
                "published_budget_fact_count": len(budget_rows),
            },
            "interpretation_rules": {
                "non_additive_measures": (
                    "Prime obligations, announced contract values or ceilings, amounts obligated "
                    "at announcement, procurement or RDT&E budget values, and solicitation values "
                    "are separate measures and must not be added."
                ),
                "supplier_allocation": (
                    "Program budgets and announcements are program-level demand evidence and are "
                    "not allocated to suppliers without a separate supported relationship."
                ),
                "planning_status": (
                    "Actual, enacted, requested and projected values retain their published status; "
                    "an outyear projection is not an obligation or an award."
                ),
                "announcement_deduplication": (
                    "Official announcements enrich linked awards. USAspending remains the source for "
                    "prime obligations when the corresponding award record becomes available."
                ),
            },
            "source_index": {
                "historical_prime_obligations": "USAspending award actions mapped to the platform",
                "official_contract_announcements": (
                    "Official U.S. Department of Defense daily contract announcements"
                ),
                "budget_and_fydp": (
                    "Published Department of Defense P-1/R-1 tables and P-40 resource summaries"
                ),
                "open_solicitations": "SAM.gov opportunity notices naming the platform or program",
            },
        }

    def answer_projection(self, *, platform_id: str) -> Dict[str, Any]:
        outlook = self.get(platform_id=platform_id)
        lanes = outlook["evidence_lanes"]
        return {
            **outlook,
            "evidence_lanes": {
                "historical_prime_obligations": lanes[
                    "historical_prime_obligations"
                ],
                "official_contract_announcements": lanes[
                    "official_contract_announcements"
                ][:10],
                "budget_and_fydp": lanes["budget_and_fydp"][:80],
                "explicit_procurement_quantities": lanes[
                    "explicit_procurement_quantities"
                ][:30],
                "open_solicitations": lanes["open_solicitations"][:10],
            },
            "leading_historical_awards": outlook["leading_historical_awards"][:6],
        }
