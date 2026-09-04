"""Build evidence-led parent and CAGE-site context packs for Ask Mimir."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import duckdb


ROOT = Path(__file__).resolve().parent
DEFAULT_DATA_ROOT = Path(
    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data"
)
DEFAULT_IDENTITY_FILE = ROOT / "company_identity_overrides.json"
DEFAULT_PROGRAM_DEFINITIONS = ROOT / "program_momentum_definitions.json"
DEFAULT_FYDP_BUDGET_FILE = (
    ROOT
    / "budget_pipeline"
    / "validation-output"
    / "fydp"
    / "dod_fydp_budget_facts.parquet"
)
DEFAULT_OUTPUT_DIR = ROOT / "validation-output" / "company-context"
DEFAULT_FISCAL_YEARS = [2021, 2022, 2023, 2024, 2025, 2026]


SOURCE_FILES = {
    "transactions": "transactions.parquet",
    "profiles": "profiles.parquet",
    "geo": "geo.parquet",
    "network": "network.parquet",
    "summary": "summary.parquet",
    "opportunities": "opportunities.parquet",
    "nsn_reference": "nsn_cage_reference.parquet",
}
OPTIONAL_SOURCE_FILES = {"summary"}


def file_sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def stable_id(prefix: str, *parts: Any) -> str:
    value = "|".join(str(part or "").strip().upper() for part in parts)
    return f"{prefix}_{hashlib.sha256(value.encode()).hexdigest()[:20]}"


def clean_cage(value: Any) -> str:
    clean = re.sub(r"[^A-Z0-9]", "", str(value or "").upper())
    return clean.zfill(5) if clean and len(clean) < 5 else clean


def slug(value: Any) -> str:
    clean = re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_")
    return clean or "unnamed"


def rows_as_dicts(cursor: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def placeholders(values: Sequence[Any]) -> str:
    if not values:
        raise ValueError("at least one value is required")
    return ",".join("?" for _ in values)


class CompanyContextBuilder:
    def __init__(
        self,
        data_root: Path = DEFAULT_DATA_ROOT,
        identity_file: Path = DEFAULT_IDENTITY_FILE,
    ) -> None:
        self.data_root = data_root.resolve()
        self.identity_file = identity_file.resolve()
        self.identity = json.loads(self.identity_file.read_text())
        self.program_definitions = json.loads(DEFAULT_PROGRAM_DEFINITIONS.read_text())
        self.fydp_budget_path = Path(
            os.getenv("ASK_MIMIR_FYDP_BUDGET_FILE", str(DEFAULT_FYDP_BUDGET_FILE))
        ).resolve()
        self.paths = {
            key: (self.data_root / filename).resolve()
            for key, filename in SOURCE_FILES.items()
        }
        missing = [
            str(path)
            for name, path in self.paths.items()
            if name not in OPTIONAL_SOURCE_FILES and not path.exists()
        ]
        if missing:
            raise FileNotFoundError(f"company context sources are missing: {missing}")
        self._source_manifest_cache: Dict[str, Any] | None = None
        self.connection = duckdb.connect()
        self.connection.execute("SET preserve_insertion_order = false")
        self.connection.execute("SET threads = 2")
        self.connection.execute("SET memory_limit = '1GB'")

    def parent_definition(self, parent_id: str) -> Dict[str, Any]:
        clean = str(parent_id).strip().upper()
        for parent in self.identity.get("parents", []):
            aliases = {str(alias).strip().upper() for alias in parent.get("aliases", [])}
            if clean in {str(parent.get("parent_id", "")).upper(), *aliases}:
                return parent
        raise KeyError(f"parent identity is not configured: {parent_id}")

    def build_parent(
        self,
        parent_id: str,
        fiscal_years: Sequence[int] = DEFAULT_FISCAL_YEARS,
    ) -> Dict[str, Any]:
        parent = self.parent_definition(parent_id)
        site_definitions = parent.get("sites", [])
        cages = sorted({clean_cage(site.get("cage")) for site in site_definitions})
        context = self._build_context(
            scope_type="company_parent",
            scope_id=str(parent["parent_id"]),
            scope_name=str(parent["parent_name"]),
            cages=cages,
            fiscal_years=fiscal_years,
            site_definitions=site_definitions,
        )
        context["identity"]["parent_resolution"] = {
            "parent_id": parent.get("parent_id"),
            "parent_name": parent.get("parent_name"),
            "aliases": parent.get("aliases", []),
            "definition_version": self.identity["version"],
            "review_status": parent.get("review_status"),
            "as_of_date": parent.get("as_of_date"),
            "sources": parent.get("sources", []),
            "method": "analyst-reviewed current facility bridge",
        }
        return context

    def build_site(
        self,
        cage: str,
        fiscal_years: Sequence[int] = DEFAULT_FISCAL_YEARS,
    ) -> Dict[str, Any]:
        clean = clean_cage(cage)
        parent_match = None
        site_definition = None
        for parent in self.identity.get("parents", []):
            for site in parent.get("sites", []):
                if clean_cage(site.get("cage")) == clean:
                    parent_match = parent
                    site_definition = site
                    break
            if parent_match:
                break
        identity_rows = self._site_identities([clean], [site_definition] if site_definition else [])
        if not identity_rows:
            raise KeyError(f"CAGE was not found in the local company identity data: {clean}")
        scope_name = str(identity_rows[0].get("vendor_name") or f"CAGE {clean}")
        context = self._build_context(
            scope_type="company_site",
            scope_id=clean,
            scope_name=scope_name,
            cages=[clean],
            fiscal_years=fiscal_years,
            site_definitions=[site_definition] if site_definition else [],
        )
        if parent_match:
            context["identity"]["parent_resolution"] = {
                "parent_id": parent_match["parent_id"],
                "parent_name": parent_match["parent_name"],
                "aliases": parent_match.get("aliases", []),
                "definition_version": self.identity["version"],
                "review_status": parent_match.get("review_status"),
                "as_of_date": parent_match.get("as_of_date"),
                "sources": parent_match.get("sources", []),
                "method": "analyst-reviewed current facility bridge",
            }
        return context

    def _build_context(
        self,
        *,
        scope_type: str,
        scope_id: str,
        scope_name: str,
        cages: Sequence[str],
        fiscal_years: Sequence[int],
        site_definitions: Sequence[Dict[str, Any] | None],
    ) -> Dict[str, Any]:
        years = sorted({int(year) for year in fiscal_years})
        if not years:
            raise ValueError("at least one fiscal year is required")
        sites = self._site_identities(cages, site_definitions)
        source_manifest = self._source_manifest()
        evidence_fingerprint = hashlib.sha256(
            json.dumps(source_manifest, sort_keys=True).encode()
        ).hexdigest()
        scope_fingerprint = hashlib.sha256(
            "|".join([scope_type, scope_id, *[str(year) for year in years]]).encode()
        ).hexdigest()[:12]
        context_id = (
            f"company_context_{evidence_fingerprint[:12]}_{scope_fingerprint}"
        )
        product_evidence = self._product_and_part_evidence(cages, years)
        capability_evidence = self._capability_evidence(cages, years)
        top_awards = self._top_awards(cages, years)
        network_context = self._network_context(cages, years)
        platform_exposure = self._platform_exposure(cages, years)
        missile_program_trajectory = self._missile_program_trajectory(cages, years)
        return {
            "context_id": context_id,
            "evidence_fingerprint": evidence_fingerprint,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "calculation_version": "mimir-company-context-2026-09-v5",
            "scope": {
                "scope_type": scope_type,
                "scope_id": scope_id,
                "scope_name": scope_name,
                "fiscal_years": years,
                "observation_window": (
                    f"FY{years[0]}-FY{years[-1]} year to date"
                    if years[-1] == 2026
                    else f"FY{years[0]}-FY{years[-1]}"
                ),
            },
            "identity": {
                "sites": sites,
                "site_count": len(sites),
                "requested_cages": list(cages),
                "resolved_cages": [site["cage"] for site in sites],
                "location_semantics": (
                    "Registered or contracting-site location from Mimir's CAGE location layer; "
                    "place of performance remains a separate award attribute."
                ),
            },
            "observed_financials": self._financial_summary(cages, years),
            "annual_activity": self._annual_activity(cages, years),
            "site_financials": self._site_financials(cages, years),
            "location_footprint": self._location_footprint(cages, years, sites),
            "capability_evidence": capability_evidence,
            "product_and_part_evidence": product_evidence,
            "platform_exposure": platform_exposure,
            "missile_program_trajectory": missile_program_trajectory,
            "future_demand_context": self._future_demand_context(
                missile_program_trajectory
            ),
            "customer_context": self._customer_context(cages, years),
            "reported_subcontract_relationships": network_context,
            "top_awards": top_awards,
            "open_solicitation_candidates": self._opportunity_candidates(cages, years),
            "evidence_index": self._evidence_index(
                sites,
                top_awards,
                product_evidence,
                capability_evidence,
            ),
            "quality": self._quality_summary(cages, years, sites),
            "source_manifest": source_manifest,
        }

    def _future_demand_context(
        self, missile_program_trajectory: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not self.fydp_budget_path.exists():
            return {
                "programs": [],
                "method": "FYDP budget projection artifact is not available",
            }
        definitions = {
            row["program_id"]: row
            for row in self.program_definitions.get("programs", [])
        }
        observed_programs = []
        for program in missile_program_trajectory.get("programs", []):
            observed_value = sum(
                float(row.get("mimir_modelled_reported_subcontract_value_usd") or 0)
                for row in program.get("annual_observations", [])
            )
            definition = definitions.get(program.get("program_id"))
            if observed_value and definition and definition.get("budget_title_aliases"):
                observed_programs.append((observed_value, program, definition))
        observed_programs.sort(key=lambda row: row[0], reverse=True)

        programs = []
        for observed_value, program, definition in observed_programs:
            aliases = [str(value).strip().upper() for value in definition["budget_title_aliases"]]
            query = f"""
                SELECT
                    component,
                    p1_line_number,
                    budget_line_item,
                    budget_line_item_title,
                    is_advance_procurement_exhibit,
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
                WHERE UPPER(TRIM(budget_line_item_title)) IN ({placeholders(aliases)})
                  AND measure_type IN ('net_procurement_p1', 'procurement_quantity')
                  AND availability_status = 'PUBLISHED'
                  AND (
                      (fiscal_year = 2025 AND funding_status = 'actual')
                      OR (fiscal_year = 2026 AND funding_status = 'enacted')
                      OR (fiscal_year = 2027 AND funding_status = 'total_request')
                      OR (fiscal_year BETWEEN 2028 AND 2031 AND funding_status = 'projected')
                  )
                ORDER BY component, p1_line_number, fiscal_year, measure_type
            """
            rows = rows_as_dicts(
                self.connection.execute(query, [str(self.fydp_budget_path), *aliases])
            )
            if not rows:
                continue
            programs.append(
                {
                    "program_id": program["program_id"],
                    "program_name": program["display_name"],
                    "observed_site_reported_subcontract_value_usd": observed_value,
                    "budget_projection_rows": rows,
                }
            )
        return {
            "programs": programs,
            "method": (
                "Public budget projections are attached only to named programs already observed "
                "in the site's Mimir program evidence. They are contextual program demand signals, "
                "not a forecast of supplier revenue or accessible value."
            ),
            "financial_rule": (
                "FY2027 uses total request only; FY2028-FY2031 use projected values. Net procurement "
                "and procurement quantity remain separate, and advance-procurement exhibits retain "
                "their own P-1 line identity."
            ),
        }

    def _location_footprint(
        self,
        cages: Sequence[str],
        years: Sequence[int],
        sites: Sequence[Dict[str, Any]],
    ) -> Dict[str, Any]:
        prime_query = f"""
            SELECT
                place_of_performance_city AS city,
                place_of_performance_state AS state,
                place_of_performance_country AS country,
                place_of_performance_zip AS postal_code,
                SUM(spend_amount) AS net_prime_obligations_usd,
                COUNT(DISTINCT award_key) AS distinct_awards,
                COUNT(DISTINCT transaction_key) AS distinct_actions
            FROM read_parquet(?)
            WHERE source_system = 'USA_SPENDING'
              AND vendor_cage IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
              AND COALESCE(TRIM(place_of_performance_city), '') <> ''
            GROUP BY 1, 2, 3, 4
            ORDER BY ABS(net_prime_obligations_usd) DESC
            LIMIT 25
        """
        subaward_query = f"""
            SELECT
                sub_city AS city,
                sub_state AS state,
                sub_country AS country,
                sub_zip AS postal_code,
                SUM(subaward_value) AS mimir_modelled_reported_subcontract_value_usd,
                COUNT(DISTINCT source_dedup_key) AS selected_relationship_actions
            FROM read_parquet(?)
            WHERE sub_cage IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
              AND COALESCE(TRIM(sub_city), '') <> ''
            GROUP BY 1, 2, 3, 4
            ORDER BY ABS(mimir_modelled_reported_subcontract_value_usd) DESC
            LIMIT 25
        """
        return {
            "registered_or_contracting_sites": list(sites),
            "prime_award_places_of_performance": rows_as_dicts(
                self.connection.execute(
                    prime_query, [str(self.paths["transactions"]), *cages, *years]
                )
            ),
            "reported_subaward_locations": rows_as_dicts(
                self.connection.execute(
                    subaward_query, [str(self.paths["network"]), *cages, *years]
                )
            ),
            "location_rules": [
                "Registered or contracting-site locations identify the CAGE site.",
                "Prime place of performance is the location reported on the prime-contract action and may differ from the CAGE site.",
                "Reported subaward locations are retained as reported and are not silently substituted for the CAGE site.",
            ],
        }

    def _product_and_part_evidence(
        self, cages: Sequence[str], years: Sequence[int]
    ) -> Dict[str, Any]:
        financial_query = f"""
            SELECT
                LPAD(NULLIF(TRIM(niin), ''), 9, '0') AS niin,
                MAX(nsn) AS nsn,
                MODE(description) AS description,
                SUM(spend_amount) AS dla_procurement_value_usd,
                COUNT(DISTINCT award_key) AS distinct_awards,
                COUNT(DISTINCT transaction_key) AS distinct_actions,
                ARRAY_AGG(DISTINCT contract_id ORDER BY contract_id) AS contract_ids,
                MIN(TRY_CAST(action_date AS DATE)) AS first_observed_date,
                MAX(TRY_CAST(action_date AS DATE)) AS latest_observed_date
            FROM read_parquet(?)
            WHERE source_system = 'DLA'
              AND vendor_cage IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
              AND niin IS NOT NULL AND TRIM(niin) <> ''
            GROUP BY 1
            ORDER BY ABS(dla_procurement_value_usd) DESC
        """
        reference_query = f"""
            SELECT
                LPAD(NULLIF(TRIM(niin), ''), 9, '0') AS niin,
                MAX(nsn) AS nsn,
                cage,
                part_number,
                MAX(description) AS description,
                MAX(supplier_status) AS supplier_status,
                BOOL_OR(COALESCE(is_procurement_authorized, false))
                    AS is_procurement_authorized,
                BOOL_OR(COALESCE(is_active_authorized_source, false))
                    AS is_active_authorized_source,
                MAX(rncc_codes) AS rncc_codes,
                MAX(rnvc_codes) AS rnvc_codes,
                MAX(rnsc_codes) AS rnsc_codes,
                MAX(cage_status_codes) AS cage_status_codes,
                MAX(platform_families) AS platform_families,
                MAX(platform_count) AS platform_count,
                BOOL_OR(COALESCE(platform_count, 0) > 1)
                    AS has_multiple_platforms
            FROM read_parquet(?)
            WHERE cage IN ({placeholders(cages)})
              AND niin IS NOT NULL AND TRIM(niin) <> ''
              AND part_number IS NOT NULL AND TRIM(part_number) <> ''
            GROUP BY 1, 3, 4
            ORDER BY 1, 4
        """
        financial = rows_as_dicts(
            self.connection.execute(
                financial_query, [str(self.paths["transactions"]), *cages, *years]
            )
        )
        references = rows_as_dicts(
            self.connection.execute(
                reference_query, [str(self.paths["nsn_reference"]), *cages]
            )
        )
        competition_query = f"""
            WITH target_niins AS (
                SELECT
                    LPAD(NULLIF(TRIM(niin), ''), 9, '0') AS niin,
                    BOOL_OR(COALESCE(is_procurement_authorized, false))
                        AS target_is_procurement_authorized,
                    BOOL_OR(COALESCE(is_active_authorized_source, false))
                        AS target_is_active_authorized_source,
                    STRING_AGG(DISTINCT supplier_status, ' | ' ORDER BY supplier_status)
                        FILTER (WHERE COALESCE(TRIM(supplier_status), '') <> '')
                        AS target_reference_status
                FROM read_parquet(?)
                WHERE cage IN ({placeholders(cages)})
                  AND niin IS NOT NULL AND TRIM(niin) <> ''
                GROUP BY 1
            ), source_counts AS (
                SELECT
                    LPAD(NULLIF(TRIM(r.niin), ''), 9, '0') AS niin,
                    MAX(r.nsn) AS nsn,
                    MAX(r.description) AS description,
                    MAX(r.acquisition_advice_code) AS acquisition_advice_code,
                    COUNT(DISTINCT r.cage) FILTER (
                        WHERE COALESCE(r.is_active_authorized_source, false)
                    ) AS active_authorized_source_count,
                    COUNT(DISTINCT r.cage) FILTER (
                        WHERE COALESCE(r.is_active_authorized_source, false)
                          AND r.cage NOT IN ({placeholders(cages)})
                    ) AS other_active_authorized_source_count,
                    STRING_AGG(
                        DISTINCT CONCAT(
                            r.cage,
                            CASE WHEN COALESCE(TRIM(r.vendor_name), '') <> ''
                                 THEN CONCAT(' (', r.vendor_name, ')') ELSE '' END
                        ),
                        ' | ' ORDER BY CONCAT(
                            r.cage,
                            CASE WHEN COALESCE(TRIM(r.vendor_name), '') <> ''
                                 THEN CONCAT(' (', r.vendor_name, ')') ELSE '' END
                        )
                    ) FILTER (
                        WHERE COALESCE(r.is_active_authorized_source, false)
                    ) AS active_authorized_sources
                FROM read_parquet(?) r
                INNER JOIN target_niins t
                    ON LPAD(NULLIF(TRIM(r.niin), ''), 9, '0') = t.niin
                GROUP BY 1
            )
            SELECT
                s.*,
                t.target_is_procurement_authorized,
                t.target_is_active_authorized_source,
                t.target_reference_status
            FROM source_counts s
            INNER JOIN target_niins t USING (niin)
            ORDER BY s.other_active_authorized_source_count DESC, s.niin
        """
        competition_rows = rows_as_dicts(
            self.connection.execute(
                competition_query,
                [
                    str(self.paths["nsn_reference"]),
                    *cages,
                    *cages,
                    str(self.paths["nsn_reference"]),
                ],
            )
        )
        financial_value_by_niin = {
            row["niin"]: abs(float(row.get("dla_procurement_value_usd") or 0))
            for row in financial
        }
        references.sort(
            key=lambda row: (
                row["niin"] not in financial_value_by_niin,
                -financial_value_by_niin.get(row["niin"], 0),
                not bool(row.get("is_active_authorized_source")),
                row["niin"],
                row["part_number"],
            )
        )
        return {
            "niin_financial_observations": financial,
            "part_number_references": references,
            "qualified_source_context": {
                "summary": {
                    "target_sole_active_source_niin_count": sum(
                        1
                        for row in competition_rows
                        if bool(row.get("target_is_active_authorized_source"))
                        and int(row.get("other_active_authorized_source_count") or 0) == 0
                    ),
                    "target_multi_source_niin_count": sum(
                        1
                        for row in competition_rows
                        if bool(row.get("target_is_active_authorized_source"))
                        and int(row.get("other_active_authorized_source_count") or 0) > 0
                    ),
                    "target_not_active_authorized_source_niin_count": sum(
                        1
                        for row in competition_rows
                        if not bool(row.get("target_is_active_authorized_source"))
                    ),
                },
                "items": competition_rows,
                "interpretation": (
                    "Counts compare active authorized CAGE relationships in the current loaded FLIS "
                    "reference snapshot. Acquisition Advice Code describes how an item is acquired; "
                    "it is not a substitute for AMC/AMSC technical breakout evidence."
                ),
            },
            "summary": {
                "observed_financial_niin_count": len(financial),
                "referenced_niin_count": len({row["niin"] for row in references}),
                "part_number_reference_count": len(references),
                "active_authorized_reference_count": sum(
                    1 for row in references if row["is_active_authorized_source"]
                ),
                "multi_platform_reference_count": sum(
                    1 for row in references if row["has_multiple_platforms"]
                ),
            },
            "financial_grain_rule": (
                "DLA procurement value is calculated once at CAGE/NIIN over the selected fiscal years; "
                "part-number relationships are non-financial references and are never summed."
            ),
            "reference_time_rule": (
                "Part-number and authorized-source fields describe the current loaded FLIS reference "
                "snapshot and are not treated as historical status for the selected fiscal years."
            ),
        }

    def _annual_activity(
        self, cages: Sequence[str], years: Sequence[int]
    ) -> List[Dict[str, Any]]:
        transaction_query = f"""
            SELECT
                year AS fiscal_year,
                CASE
                    WHEN source_system = 'USA_SPENDING' THEN 'prime_obligations'
                    WHEN source_system = 'DLA' THEN 'dla_procurement_value'
                    ELSE lower(source_system)
                END AS measure_type,
                SUM(spend_amount) AS net_value_usd,
                COUNT(DISTINCT award_key) AS distinct_awards,
                COUNT(DISTINCT transaction_key) AS distinct_actions
            FROM read_parquet(?)
            WHERE vendor_cage IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
            GROUP BY 1, 2
        """
        network_query = f"""
            SELECT
                year AS fiscal_year,
                'mimir_modelled_reported_subcontract_value' AS measure_type,
                SUM(subaward_value) AS net_value_usd,
                COUNT(DISTINCT contract_id) AS distinct_awards,
                COUNT(DISTINCT source_dedup_key) AS distinct_actions
            FROM read_parquet(?)
            WHERE sub_cage IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
            GROUP BY 1
        """
        rows = rows_as_dicts(
            self.connection.execute(
                transaction_query,
                [str(self.paths["transactions"]), *cages, *years],
            )
        )
        rows.extend(
            rows_as_dicts(
                self.connection.execute(
                    network_query, [str(self.paths["network"]), *cages, *years]
                )
            )
        )
        return sorted(
            rows,
            key=lambda row: (int(row["fiscal_year"]), str(row["measure_type"])),
        )

    def _missile_program_trajectory(
        self, cages: Sequence[str], years: Sequence[int]
    ) -> Dict[str, Any]:
        alias_to_program: Dict[str, Dict[str, str]] = {}
        for program in self.program_definitions.get("programs", []):
            for alias in program.get("platform_aliases", []):
                alias_to_program[str(alias).strip().upper()] = {
                    "program_id": str(program["program_id"]),
                    "display_name": str(program["display_name"]),
                }

        network_query = f"""
            SELECT
                year AS fiscal_year,
                UPPER(TRIM(platform_family)) AS platform_family,
                prime_cage,
                MAX(prime_name) AS prime_name,
                SUM(subaward_value) AS observed_value_usd,
                SUM(subaward_value_raw) AS source_reported_value_usd,
                COUNT(DISTINCT source_dedup_key) AS selected_report_count,
                COUNT(DISTINCT contract_id) AS distinct_prime_awards,
                ARRAY_AGG(DISTINCT description) FILTER (
                    WHERE description IS NOT NULL AND TRIM(description) <> ''
                ) AS reported_descriptions
            FROM read_parquet(?)
            WHERE sub_cage IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
              AND platform_family IS NOT NULL
              AND TRIM(platform_family) <> ''
              AND UPPER(TRIM(platform_family)) <> 'UNMAPPED'
            GROUP BY 1, 2, 3
        """
        rows = rows_as_dicts(
            self.connection.execute(
                network_query, [str(self.paths["network"]), *cages, *years]
            )
        )

        grouped: Dict[str, Dict[str, Any]] = {}
        missile_name_pattern = re.compile(
            r"AMRAAM|JASSM|LRASM|PATRIOT|THAAD|ATACMS|PRSM|MISSILE|"
            r"AIM-|AGM-|SM-|GMLRS|TOMAHAWK|MRIC|MSE|NASAMS|STINGER|TRIDENT"
        )
        for row in rows:
            platform_name = str(row["platform_family"]).strip().upper()
            definition = alias_to_program.get(platform_name)
            if not definition and missile_name_pattern.search(platform_name):
                definition = {
                    "program_id": re.sub(r"[^A-Z0-9]+", "_", platform_name).strip("_"),
                    "display_name": platform_name,
                }
            if not definition:
                continue
            program = grouped.setdefault(
                definition["program_id"],
                {
                    **definition,
                    "annual_observations": {},
                    "prime_customer_sites": {},
                    "reported_descriptions": set(),
                    "total_observed_value_usd": 0.0,
                    "selected_report_count": 0,
                    "distinct_prime_awards": 0,
                },
            )
            fiscal_year = int(row["fiscal_year"])
            value = float(row["observed_value_usd"] or 0)
            annual = program["annual_observations"].setdefault(
                fiscal_year,
                {
                    "fiscal_year": fiscal_year,
                    "mimir_modelled_reported_subcontract_value_usd": 0.0,
                    "source_reported_value_usd": 0.0,
                    "selected_report_count": 0,
                    "distinct_prime_awards": 0,
                },
            )
            annual["mimir_modelled_reported_subcontract_value_usd"] += value
            annual["source_reported_value_usd"] += float(
                row["source_reported_value_usd"] or 0
            )
            annual["selected_report_count"] += int(row["selected_report_count"] or 0)
            annual["distinct_prime_awards"] += int(row["distinct_prime_awards"] or 0)
            program["total_observed_value_usd"] += value
            program["selected_report_count"] += int(row["selected_report_count"] or 0)
            program["distinct_prime_awards"] += int(row["distinct_prime_awards"] or 0)
            customer_key = str(row["prime_cage"] or "UNKNOWN")
            customer = program["prime_customer_sites"].setdefault(
                customer_key,
                {
                    "prime_cage": row["prime_cage"],
                    "prime_name": row["prime_name"],
                    "observed_value_usd": 0.0,
                },
            )
            customer["observed_value_usd"] += value
            program["reported_descriptions"].update(
                str(item).strip()
                for item in (row["reported_descriptions"] or [])
                if str(item).strip()
            )

        total_by_year = {
            int(row[0]): float(row[1] or 0)
            for row in self.connection.execute(
                f"""
                SELECT year, SUM(subaward_value)
                FROM read_parquet(?)
                WHERE sub_cage IN ({placeholders(cages)})
                  AND year IN ({placeholders(years)})
                GROUP BY 1
                """,
                [str(self.paths["network"]), *cages, *years],
            ).fetchall()
        }
        programs: List[Dict[str, Any]] = []
        missile_by_year = {int(year): 0.0 for year in years}
        for program in grouped.values():
            annual_rows = []
            for year in years:
                observation = program["annual_observations"].get(int(year))
                if observation:
                    missile_by_year[int(year)] += float(
                        observation["mimir_modelled_reported_subcontract_value_usd"]
                    )
                    annual_rows.append(observation)
                else:
                    annual_rows.append(
                        {
                            "fiscal_year": int(year),
                            "mimir_modelled_reported_subcontract_value_usd": None,
                            "source_reported_value_usd": None,
                            "selected_report_count": 0,
                            "distinct_prime_awards": 0,
                            "observation_status": "NOT_OBSERVED",
                        }
                    )
            program["annual_observations"] = annual_rows
            program["prime_customer_sites"] = sorted(
                program["prime_customer_sites"].values(),
                key=lambda row: abs(float(row["observed_value_usd"])),
                reverse=True,
            )
            program["reported_descriptions"] = sorted(
                program["reported_descriptions"]
            )[:12]
            programs.append(program)
        programs.sort(
            key=lambda row: abs(float(row["total_observed_value_usd"])), reverse=True
        )

        annual_summary = []
        for year in years:
            total = total_by_year.get(int(year), 0.0)
            missile_value = missile_by_year[int(year)]
            annual_summary.append(
                {
                    "fiscal_year": int(year),
                    "all_reported_subcontract_value_usd": total,
                    "missile_program_reported_subcontract_value_usd": missile_value,
                    "missile_share_of_reported_subcontract_value_pct": (
                        round(missile_value / total * 100, 2) if total else None
                    ),
                    "observed_missile_program_count": sum(
                        1
                        for program in programs
                        if any(
                            row["fiscal_year"] == int(year)
                            and row.get("mimir_modelled_reported_subcontract_value_usd")
                            is not None
                            for row in program["annual_observations"]
                        )
                    ),
                }
            )
        return {
            "market": "US missile programs",
            "program_definition_version": self.program_definitions.get(
                "definition_version"
            ),
            "annual_summary": annual_summary,
            "programs": programs,
            "interpretation_rules": [
                "Values are Mimir-modelled reported subcontract value for reports dated in each fiscal year.",
                "A missing annual observation means no mapped report was observed; it does not prove that the supplier exited the program.",
                "Prime obligations, DLA procurement value and reported subcontract value are separate and non-additive.",
                "Changes in reported subcontract value are not equivalent to changes in company revenue or production volume.",
            ],
        }

    def _site_identities(
        self,
        cages: Sequence[str],
        site_definitions: Sequence[Dict[str, Any] | None],
    ) -> List[Dict[str, Any]]:
        site_by_cage = {
            clean_cage(site.get("cage")): site for site in site_definitions if site
        }
        requested_values = ", ".join("(?)" for _ in cages)
        query = f"""
            WITH requested(cage_code) AS (
                VALUES {requested_values}
            )
            SELECT
                r.cage_code AS cage,
                COALESCE(p.vendor_name, g.vendor_name, r.cage_code) AS vendor_name,
                g.city,
                g.state,
                g.location_quality,
                p.profile_source,
                p.last_active_year,
                p.network_last_active_year
            FROM requested r
            LEFT JOIN read_parquet(?) p USING (cage_code)
            LEFT JOIN read_parquet(?) g USING (cage_code)
            ORDER BY r.cage_code
        """
        rows = rows_as_dicts(
            self.connection.execute(
                query,
                [*cages, str(self.paths["profiles"]), str(self.paths["geo"])],
            )
        )
        for row in rows:
            definition = site_by_cage.get(clean_cage(row["cage"])) or {}
            row["official_site_label"] = definition.get("official_site_label")
            row["official_capability_summary"] = definition.get(
                "official_capability_summary"
            )
            row["official_source_url"] = definition.get("official_source_url")
            row["parent_membership_method"] = (
                "analyst-reviewed company facility bridge" if definition else "not assigned"
            )
        return rows

    def _financial_summary(
        self, cages: Sequence[str], years: Sequence[int]
    ) -> List[Dict[str, Any]]:
        query = f"""
            SELECT
                CASE
                    WHEN source_system = 'USA_SPENDING' THEN 'prime_obligations'
                    WHEN source_system = 'DLA' THEN 'dla_procurement_value'
                    ELSE lower(source_system)
                END AS measure_type,
                SUM(spend_amount) AS net_value_usd,
                SUM(CASE WHEN spend_amount > 0 THEN spend_amount ELSE 0 END)
                    AS positive_value_usd,
                SUM(CASE WHEN spend_amount < 0 THEN spend_amount ELSE 0 END)
                    AS deobligation_value_usd,
                COUNT(DISTINCT award_key) AS distinct_awards,
                COUNT(DISTINCT transaction_key) AS distinct_actions,
                MIN(TRY_CAST(action_date AS DATE)) AS first_action_date,
                MAX(TRY_CAST(action_date AS DATE)) AS latest_action_date
            FROM read_parquet(?)
            WHERE vendor_cage IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
            GROUP BY 1
            ORDER BY 1
        """
        return rows_as_dicts(
            self.connection.execute(
                query, [str(self.paths["transactions"]), *cages, *years]
            )
        )

    def _site_financials(
        self, cages: Sequence[str], years: Sequence[int]
    ) -> List[Dict[str, Any]]:
        query = f"""
            SELECT
                vendor_cage AS cage,
                MODE(vendor_name) AS vendor_name,
                CASE
                    WHEN source_system = 'USA_SPENDING' THEN 'prime_obligations'
                    WHEN source_system = 'DLA' THEN 'dla_procurement_value'
                    ELSE lower(source_system)
                END AS measure_type,
                SUM(spend_amount) AS net_value_usd,
                COUNT(DISTINCT award_key) AS distinct_awards,
                MAX(TRY_CAST(action_date AS DATE)) AS latest_action_date
            FROM read_parquet(?)
            WHERE vendor_cage IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
            GROUP BY 1, 3
            ORDER BY ABS(net_value_usd) DESC
        """
        return rows_as_dicts(
            self.connection.execute(
                query, [str(self.paths["transactions"]), *cages, *years]
            )
        )

    def _capability_evidence(
        self, cages: Sequence[str], years: Sequence[int]
    ) -> Dict[str, Any]:
        if self.paths["summary"].exists():
            psc_query = f"""
                SELECT
                    t.source_system,
                    t.psc,
                    MAX(s.psc_description) AS psc_description,
                    SUM(t.spend_amount) AS net_value_usd,
                    COUNT(DISTINCT t.award_key) AS distinct_awards,
                    MIN(t.year) AS first_fiscal_year,
                    MAX(t.year) AS latest_fiscal_year,
                    MIN(TRY_CAST(t.action_date AS DATE)) AS first_action_date,
                    MAX(TRY_CAST(t.action_date AS DATE)) AS latest_action_date
                FROM read_parquet(?) t
                LEFT JOIN (
                    SELECT psc_code, MAX(psc_description) AS psc_description
                    FROM read_parquet(?)
                    GROUP BY 1
                ) s ON t.psc = s.psc_code
                WHERE t.vendor_cage IN ({placeholders(cages)})
                  AND t.year IN ({placeholders(years)})
                  AND t.psc IS NOT NULL AND TRIM(t.psc) <> ''
                GROUP BY 1, 2
                ORDER BY ABS(net_value_usd) DESC
                LIMIT 20
            """
            psc_rows = rows_as_dicts(
                self.connection.execute(
                    psc_query,
                    [
                        str(self.paths["transactions"]),
                        str(self.paths["summary"]),
                        *cages,
                        *years,
                    ],
                )
            )
            naics_query = f"""
                SELECT
                    naics_code,
                    MAX(naics_description) AS naics_description,
                    SUM(total_spend) AS net_value_usd,
                    SUM(contract_count) AS action_count,
                    MIN(year) AS first_fiscal_year,
                    MAX(year) AS latest_fiscal_year
                FROM read_parquet(?)
                WHERE cage_code IN ({placeholders(cages)})
                  AND year IN ({placeholders(years)})
                  AND naics_code IS NOT NULL AND TRIM(naics_code) <> ''
                GROUP BY 1
                ORDER BY ABS(net_value_usd) DESC
                LIMIT 15
            """
            naics_rows = rows_as_dicts(
                self.connection.execute(
                    naics_query, [str(self.paths["summary"]), *cages, *years]
                )
            )
        else:
            psc_query = f"""
                SELECT
                    source_system,
                    psc,
                    CAST(NULL AS VARCHAR) AS psc_description,
                    SUM(spend_amount) AS net_value_usd,
                    COUNT(DISTINCT award_key) AS distinct_awards,
                    MIN(year) AS first_fiscal_year,
                    MAX(year) AS latest_fiscal_year,
                    MIN(TRY_CAST(action_date AS DATE)) AS first_action_date,
                    MAX(TRY_CAST(action_date AS DATE)) AS latest_action_date
                FROM read_parquet(?)
                WHERE vendor_cage IN ({placeholders(cages)})
                  AND year IN ({placeholders(years)})
                  AND psc IS NOT NULL AND TRIM(psc) <> ''
                GROUP BY 1, 2
                ORDER BY ABS(net_value_usd) DESC
                LIMIT 20
            """
            psc_rows = rows_as_dicts(
                self.connection.execute(
                    psc_query, [str(self.paths["transactions"]), *cages, *years]
                )
            )
            naics_query = f"""
                SELECT
                    naics_code,
                    CAST(NULL AS VARCHAR) AS naics_description,
                    SUM(spend_amount) AS net_value_usd,
                    COUNT(DISTINCT transaction_key) AS action_count,
                    MIN(year) AS first_fiscal_year,
                    MAX(year) AS latest_fiscal_year
                FROM read_parquet(?)
                WHERE vendor_cage IN ({placeholders(cages)})
                  AND year IN ({placeholders(years)})
                  AND naics_code IS NOT NULL AND TRIM(naics_code) <> ''
                GROUP BY 1
                ORDER BY ABS(net_value_usd) DESC
                LIMIT 15
            """
            naics_rows = rows_as_dicts(
                self.connection.execute(
                    naics_query, [str(self.paths["transactions"]), *cages, *years]
                )
            )
        product_query = f"""
            SELECT
                LPAD(NULLIF(TRIM(niin), ''), 9, '0') AS niin,
                MAX(nsn) AS nsn,
                MODE(description) AS description,
                SUM(spend_amount) AS dla_procurement_value_usd,
                COUNT(DISTINCT award_key) AS distinct_awards,
                COUNT(DISTINCT vendor_cage) AS contributing_sites,
                MAX(TRY_CAST(action_date AS DATE)) AS latest_action_date
            FROM read_parquet(?)
            WHERE source_system = 'DLA'
              AND vendor_cage IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
              AND niin IS NOT NULL AND TRIM(niin) <> ''
            GROUP BY 1
            ORDER BY ABS(dla_procurement_value_usd) DESC
            LIMIT 15
        """
        award_description_query = f"""
            SELECT
                award_key,
                MAX(contract_id) AS contract_id,
                MAX(base_award_description) AS base_award_description,
                SUM(spend_amount) AS net_prime_obligations_usd,
                MAX(TRY_CAST(action_date AS DATE)) AS latest_action_date
            FROM read_parquet(?)
            WHERE source_system = 'USA_SPENDING'
              AND vendor_cage IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
            GROUP BY 1
            HAVING ABS(SUM(spend_amount)) > 0
            ORDER BY ABS(net_prime_obligations_usd) DESC
            LIMIT 12
        """
        subaward_description_query = f"""
            SELECT
                sub_cage AS cage,
                MAX(sub_name) AS sub_name,
                MAX(prime_name) AS prime_name,
                MAX(contract_id) AS prime_contract_id,
                MAX(award_key) AS prime_award_key,
                MAX(description) AS reported_description,
                SUM(subaward_value) AS mimir_modelled_subcontract_value_usd,
                COUNT(*) AS selected_report_count,
                MAX(TRY_CAST(action_date AS DATE)) AS latest_report_date,
                CONCAT('https://www.usaspending.gov/award/', MAX(award_key), '/')
                    AS prime_award_public_url
            FROM read_parquet(?)
            WHERE sub_cage IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
            GROUP BY sub_cage, prime_cage, source_dedup_key
            ORDER BY ABS(mimir_modelled_subcontract_value_usd) DESC
            LIMIT 12
        """
        return {
            "psc": psc_rows,
            "naics": naics_rows,
            "dla_items": rows_as_dicts(
                self.connection.execute(
                    product_query, [str(self.paths["transactions"]), *cages, *years]
                )
            ),
            "prime_award_descriptions": rows_as_dicts(
                self.connection.execute(
                    award_description_query,
                    [str(self.paths["transactions"]), *cages, *years],
                )
            ),
            "reported_subaward_descriptions": rows_as_dicts(
                self.connection.execute(
                    subaward_description_query,
                    [str(self.paths["network"]), *cages, *years],
                )
            ),
            "interpretation_rule": (
                "These are demonstrated procurement and relationship signals, not a declaration "
                "that the company is technically qualified for every adjacent requirement."
            ),
        }

    def _platform_exposure(
        self, cages: Sequence[str], years: Sequence[int]
    ) -> List[Dict[str, Any]]:
        transaction_query = f"""
            SELECT
                'prime_or_dla_action' AS evidence_layer,
                source_system,
                platform_family,
                SUM(spend_amount) AS observed_value_usd,
                COUNT(DISTINCT award_key) AS distinct_awards,
                COUNT(DISTINCT vendor_cage) AS contributing_sites,
                MAX(TRY_CAST(action_date AS DATE)) AS latest_date
            FROM read_parquet(?)
            WHERE vendor_cage IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
              AND platform_family IS NOT NULL AND TRIM(platform_family) <> ''
            GROUP BY 2, 3
        """
        network_query = f"""
            SELECT
                'reported_subaward' AS evidence_layer,
                'USA_SPENDING_SUBAWARD' AS source_system,
                platform_family,
                SUM(subaward_value) AS observed_value_usd,
                COUNT(DISTINCT source_dedup_key) AS distinct_awards,
                COUNT(DISTINCT sub_cage) AS contributing_sites,
                MAX(TRY_CAST(action_date AS DATE)) AS latest_date
            FROM read_parquet(?)
            WHERE sub_cage IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
              AND platform_family IS NOT NULL AND TRIM(platform_family) <> ''
              AND UPPER(platform_family) <> 'UNMAPPED'
            GROUP BY 3
        """
        rows = rows_as_dicts(
            self.connection.execute(
                transaction_query,
                [str(self.paths["transactions"]), *cages, *years],
            )
        )
        rows.extend(
            rows_as_dicts(
                self.connection.execute(
                    network_query, [str(self.paths["network"]), *cages, *years]
                )
            )
        )
        return sorted(rows, key=lambda row: abs(float(row["observed_value_usd"] or 0)), reverse=True)[:30]

    def _customer_context(
        self, cages: Sequence[str], years: Sequence[int]
    ) -> List[Dict[str, Any]]:
        query = f"""
            SELECT
                sub_agency,
                parent_agency,
                SUM(spend_amount) AS net_prime_obligations_usd,
                COUNT(DISTINCT award_key) AS distinct_awards,
                MAX(TRY_CAST(action_date AS DATE)) AS latest_action_date
            FROM read_parquet(?)
            WHERE source_system = 'USA_SPENDING'
              AND vendor_cage IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
            GROUP BY 1, 2
            ORDER BY ABS(net_prime_obligations_usd) DESC
            LIMIT 15
        """
        return rows_as_dicts(
            self.connection.execute(
                query, [str(self.paths["transactions"]), *cages, *years]
            )
        )

    def _network_context(
        self, cages: Sequence[str], years: Sequence[int]
    ) -> Dict[str, Any]:
        upstream_query = f"""
            WITH relationships AS (
                SELECT
                    prime_cage,
                    MAX(prime_name) AS prime_name,
                    MAX(prime_gold_parent) AS reported_prime_parent,
                    SUM(subaward_value) AS mimir_modelled_subcontract_value_usd,
                    SUM(subaward_value_raw) AS source_reported_value_usd,
                    COUNT(DISTINCT source_dedup_key) AS selected_relationship_actions,
                    COUNT(*) AS selected_report_count,
                    MIN(TRY_CAST(action_date AS DATE)) AS first_observed_date,
                    MAX(TRY_CAST(action_date AS DATE)) AS latest_observed_date
                FROM read_parquet(?)
                WHERE sub_cage IN ({placeholders(cages)})
                  AND year IN ({placeholders(years)})
                GROUP BY prime_cage
            ), prime_sites AS (
                SELECT
                    cage_code,
                    MAX(city) AS registered_city,
                    MAX(state) AS registered_state,
                    MAX(location_quality) AS location_quality
                FROM read_parquet(?)
                GROUP BY 1
            )
            SELECT r.*, p.registered_city, p.registered_state, p.location_quality
            FROM relationships r
            LEFT JOIN prime_sites p ON r.prime_cage = p.cage_code
            ORDER BY ABS(r.mimir_modelled_subcontract_value_usd) DESC
            LIMIT 15
        """
        downstream_query = f"""
            SELECT
                sub_cage,
                MAX(sub_name) AS sub_name,
                MAX(sub_cage_resolution) AS cage_resolution,
                SUM(subaward_value) AS mimir_modelled_subcontract_value_usd,
                SUM(subaward_value_raw) AS source_reported_value_usd,
                COUNT(DISTINCT source_dedup_key) AS selected_relationship_actions,
                COUNT(*) AS selected_report_count,
                MIN(TRY_CAST(action_date AS DATE)) AS first_observed_date,
                MAX(TRY_CAST(action_date AS DATE)) AS latest_observed_date
            FROM read_parquet(?)
            WHERE prime_cage IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
            GROUP BY sub_cage
            ORDER BY ABS(mimir_modelled_subcontract_value_usd) DESC
            LIMIT 15
        """
        return {
            "as_subcontractor_to": rows_as_dicts(
                self.connection.execute(
                    upstream_query,
                    [
                        str(self.paths["network"]),
                        *cages,
                        *years,
                        str(self.paths["geo"]),
                    ],
                )
            ),
            "reported_subcontractors": rows_as_dicts(
                self.connection.execute(
                    downstream_query, [str(self.paths["network"]), *cages, *years]
                )
            ),
            "financial_rule": (
                "Mimir-modelled reported subcontract value is separate from prime obligations "
                "and DLA procurement value and must not be added to either measure."
            ),
        }

    def _top_awards(
        self, cages: Sequence[str], years: Sequence[int]
    ) -> List[Dict[str, Any]]:
        query = f"""
            SELECT
                award_key,
                MAX(contract_id) AS contract_id,
                MODE(vendor_cage) AS recipient_cage,
                MODE(vendor_name) AS recipient_name,
                MAX(base_award_description) AS base_award_description,
                SUM(spend_amount) AS net_prime_obligations_usd,
                COUNT(DISTINCT transaction_key) AS action_count,
                MIN(TRY_CAST(action_date AS DATE)) AS first_action_date,
                MAX(TRY_CAST(action_date AS DATE)) AS latest_action_date,
                MAX(platform_family) AS platform_family,
                MAX(psc) AS psc,
                CONCAT('https://www.usaspending.gov/award/', award_key, '/')
                    AS public_record_url
            FROM read_parquet(?)
            WHERE source_system = 'USA_SPENDING'
              AND vendor_cage IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
            GROUP BY 1
            ORDER BY ABS(net_prime_obligations_usd) DESC
            LIMIT 15
        """
        return rows_as_dicts(
            self.connection.execute(
                query, [str(self.paths["transactions"]), *cages, *years]
            )
        )

    @staticmethod
    def _evidence_index(
        sites: Sequence[Dict[str, Any]],
        top_awards: Sequence[Dict[str, Any]],
        product_evidence: Dict[str, Any],
        capability_evidence: Dict[str, Any],
    ) -> Dict[str, Any]:
        records: List[Dict[str, Any]] = []
        for site in sites:
            if site.get("official_source_url"):
                records.append(
                    {
                        "evidence_type": "official_company_site_source",
                        "record_id": site.get("cage"),
                        "title": site.get("official_site_label") or site.get("vendor_name"),
                        "public_url": site.get("official_source_url"),
                        "supports": "current site identity and stated capability context",
                    }
                )
        for award in top_awards:
            records.append(
                {
                    "evidence_type": "usaspending_prime_award",
                    "record_id": award.get("award_key"),
                    "display_id": award.get("contract_id"),
                    "title": award.get("base_award_description"),
                    "public_url": award.get("public_record_url"),
                    "supports": "prime award purpose, recipient, actions and prime obligations",
                }
            )
        for item in product_evidence.get("niin_financial_observations", []):
            records.append(
                {
                    "evidence_type": "dla_procurement_history",
                    "record_id": item.get("niin"),
                    "display_id": item.get("nsn") or item.get("niin"),
                    "title": item.get("description"),
                    "contract_ids": item.get("contract_ids", [])[:20],
                    "supports": "CAGE/NIIN procurement value and contract history",
                }
            )
        for relationship in capability_evidence.get("reported_subaward_descriptions", []):
            records.append(
                {
                    "evidence_type": "usaspending_reported_subaward",
                    "record_id": relationship.get("prime_award_key"),
                    "display_id": relationship.get("prime_contract_id"),
                    "title": relationship.get("reported_description"),
                    "public_url": relationship.get("prime_award_public_url"),
                    "supports": "reported subcontract relationship, description and modelled value",
                }
            )
        return {
            "records": records,
            "record_count": len(records),
            "provenance_rule": (
                "Public record identifiers and URLs are retained where the source supports a stable "
                "locator. Internal ingestion and deduplication keys remain in the governed data layer "
                "and are not customer-facing evidence labels."
            ),
        }

    def _opportunity_candidates(
        self, cages: Sequence[str], years: Sequence[int]
    ) -> Dict[str, Any]:
        if self.paths["summary"].exists():
            naics_source = self.paths["summary"]
            cage_column = "cage_code"
            value_column = "total_spend"
        else:
            naics_source = self.paths["transactions"]
            cage_column = "vendor_cage"
            value_column = "spend_amount"
        naics_rows = self.connection.execute(
            f"""
            SELECT naics_code
            FROM read_parquet(?)
            WHERE {cage_column} IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
              AND naics_code IS NOT NULL AND LENGTH(TRIM(naics_code)) >= 4
            GROUP BY 1
            ORDER BY ABS(SUM({value_column})) DESC
            LIMIT 8
            """,
            [str(naics_source), *cages, *years],
        ).fetchall()
        naics_codes = [str(row[0]).strip() for row in naics_rows]
        if not naics_codes:
            return {"candidates": [], "method": "no NAICS evidence available"}
        conditions = " OR ".join("CAST(TRY_CAST(naics AS BIGINT) AS VARCHAR) LIKE ?" for _ in naics_codes)
        query = f"""
            SELECT
                id,
                sol_num,
                title,
                agency,
                sub_agency,
                CAST(TRY_CAST(naics AS BIGINT) AS VARCHAR) AS naics,
                psc,
                deadline,
                state,
                source_system,
                url
            FROM read_parquet(?)
            WHERE ({conditions})
              AND TRY_CAST(SUBSTR(deadline, 1, 10) AS DATE) >= CURRENT_DATE
            ORDER BY TRY_CAST(SUBSTR(deadline, 1, 10) AS DATE), title
            LIMIT 12
        """
        candidates = rows_as_dicts(
            self.connection.execute(
                query,
                [str(self.paths["opportunities"]), *[f"{code}%" for code in naics_codes]],
            )
        )
        return {
            "candidates": candidates,
            "method": "broad historical NAICS overlap only",
            "warning": (
                "These are discovery candidates, not ranked company-program fits. Technical "
                "requirements, incumbents and capability evidence must be evaluated before recommendation."
            ),
        }

    def _quality_summary(
        self, cages: Sequence[str], years: Sequence[int], sites: Sequence[Dict[str, Any]]
    ) -> Dict[str, Any]:
        transaction_rows = rows_as_dicts(self.connection.execute(
            f"""
            SELECT
                CASE WHEN source_system = 'USA_SPENDING'
                     THEN 'prime_obligations' ELSE 'dla_procurement_value' END
                    AS financial_lane,
                COUNT(*) AS record_count,
                SUM(ABS(spend_amount)) AS absolute_value_usd,
                SUM(ABS(spend_amount)) FILTER (
                    WHERE platform_family IS NOT NULL
                      AND TRIM(platform_family) <> ''
                      AND UPPER(platform_family) <> 'UNMAPPED'
                ) AS platform_mapped_absolute_value_usd,
                SUM(spend_amount) AS net_value_usd
            FROM read_parquet(?)
            WHERE vendor_cage IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
              AND source_system IN ('USA_SPENDING', 'DLA')
            GROUP BY 1
            """,
            [str(self.paths["transactions"]), *cages, *years],
        ))
        network_row = rows_as_dicts(self.connection.execute(
            f"""
            SELECT
                'reported_subcontract_value' AS financial_lane,
                COUNT(*) AS record_count,
                SUM(ABS(subaward_value)) AS absolute_value_usd,
                SUM(ABS(subaward_value)) FILTER (
                    WHERE platform_family IS NOT NULL
                      AND TRIM(platform_family) <> ''
                      AND UPPER(platform_family) <> 'UNMAPPED'
                ) AS platform_mapped_absolute_value_usd,
                SUM(subaward_value) AS net_value_usd
            FROM read_parquet(?)
            WHERE sub_cage IN ({placeholders(cages)})
              AND year IN ({placeholders(years)})
            """,
            [str(self.paths["network"]), *cages, *years],
        ))
        platform_mapping = transaction_rows + network_row
        for row in platform_mapping:
            absolute_value = float(row.get("absolute_value_usd") or 0)
            mapped_value = float(row.get("platform_mapped_absolute_value_usd") or 0)
            row["platform_mapped_absolute_value_pct"] = (
                round(mapped_value / absolute_value * 100, 2)
                if absolute_value
                else None
            )
        return {
            "platform_mapping_by_financial_lane": platform_mapping,
            "requested_site_count": len(cages),
            "resolved_site_count": len(sites),
            "missing_site_cages": sorted(set(cages) - {site["cage"] for site in sites}),
            "location_quality_counts": self._count_values(
                site.get("location_quality") or "MISSING" for site in sites
            ),
            "similar_prime_award_signature_review": {
                "candidates": self._similar_prime_award_signatures(cages, years),
                "rule": (
                    "Separate prime award IDs sharing recipient CAGE, initial financial-action "
                    "date and net observed value. These are review candidates, not automatically duplicates."
                ),
            },
            "known_limitations": [
                "The financial lanes describe observed U.S. defense contracting activity, not total company or site revenue.",
                "Platform coverage differs by financial lane; shared-use NIIN relationships are not allocated repeatedly across platforms.",
                "Reported subcontract value follows Mimir's published treatment of source reports and remains separate from prime obligations.",
                "Current opportunity candidates use broad historical NAICS overlap and require further fit analysis.",
            ],
        }

    def _similar_prime_award_signatures(
        self, cages: Sequence[str], years: Sequence[int]
    ) -> List[Dict[str, Any]]:
        query = f"""
            WITH awards AS (
                SELECT
                    vendor_cage,
                    award_key,
                    MAX(contract_id) AS contract_id,
                    ROUND(SUM(spend_amount), 2) AS net_value_usd,
                    MIN(TRY_CAST(action_date AS DATE)) FILTER (WHERE spend_amount <> 0)
                        AS first_financial_action_date,
                    MAX(base_award_description) AS base_award_description
                FROM read_parquet(?)
                WHERE source_system = 'USA_SPENDING'
                  AND vendor_cage IN ({placeholders(cages)})
                  AND year IN ({placeholders(years)})
                GROUP BY 1, 2
            )
            SELECT
                vendor_cage AS cage,
                net_value_usd,
                first_financial_action_date,
                COUNT(*) AS award_count,
                ARRAY_AGG(contract_id ORDER BY contract_id) AS contract_ids,
                ARRAY_AGG(base_award_description ORDER BY contract_id) AS descriptions
            FROM awards
            WHERE net_value_usd <> 0
            GROUP BY 1, 2, 3
            HAVING COUNT(*) > 1
            ORDER BY ABS(net_value_usd) DESC
            LIMIT 10
        """
        return rows_as_dicts(
            self.connection.execute(
                query, [str(self.paths["transactions"]), *cages, *years]
            )
        )

    @staticmethod
    def _count_values(values: Iterable[str]) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for value in values:
            counts[value] = counts.get(value, 0) + 1
        return counts

    def _source_manifest(self) -> Dict[str, Any]:
        if self._source_manifest_cache is not None:
            return self._source_manifest_cache
        self._source_manifest_cache = {
            "generated_on": str(date.today()),
            "identity_definition": {
                "path": str(self.identity_file),
                "sha256": file_sha256(self.identity_file),
                "version": self.identity["version"],
            },
            "files": {
                name: {
                    "path": str(path),
                    "sha256": file_sha256(path),
                    "size_bytes": path.stat().st_size,
                }
                for name, path in self.paths.items()
                if path.exists()
            },
        }
        if self.fydp_budget_path.exists():
            self._source_manifest_cache["fydp_budget"] = {
                "path": str(self.fydp_budget_path),
                "sha256": file_sha256(self.fydp_budget_path),
                "size_bytes": self.fydp_budget_path.stat().st_size,
            }
        return self._source_manifest_cache


def write_context(context: Dict[str, Any], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(context, indent=2, default=str) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument("--identity-file", type=Path, default=DEFAULT_IDENTITY_FILE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--parent", action="append")
    parser.add_argument("--site-cage", action="append")
    parser.add_argument("--fiscal-year", type=int, action="append")
    args = parser.parse_args()
    years = args.fiscal_year or DEFAULT_FISCAL_YEARS
    builder = CompanyContextBuilder(args.data_root, args.identity_file)
    output_dir = args.output_dir.resolve()
    parent_ids = args.parent or [
        str(parent["parent_id"]) for parent in builder.identity.get("parents", [])
    ]
    site_cages = args.site_cage or sorted(
        {
            clean_cage(site.get("cage"))
            for parent in builder.identity.get("parents", [])
            for site in parent.get("sites", [])
        }
    )
    contexts: List[Dict[str, Any]] = []
    entries: List[Dict[str, Any]] = []
    for parent_id in parent_ids:
        context = builder.build_parent(parent_id, years)
        path = f"parent_{slug(parent_id)}.json"
        write_context(context, output_dir / path)
        contexts.append(context)
        entries.append(
            {"context_id": context["context_id"], "scope": context["scope"], "path": path}
        )
    for cage in site_cages:
        context = builder.build_site(cage, years)
        path = f"site_{clean_cage(cage)}.json"
        write_context(context, output_dir / path)
        contexts.append(context)
        entries.append(
            {"context_id": context["context_id"], "scope": context["scope"], "path": path}
        )
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "calculation_version": contexts[0]["calculation_version"] if contexts else None,
        "contexts": entries,
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
