"""Universal platform and program evidence dossiers for Ask Mimir."""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence

import duckdb


DEFAULT_DATA_ROOT = Path(
    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data"
)
COMPLETED_FISCAL_YEARS = tuple(range(2021, 2026))
OBSERVATION_WINDOW = "FY2021-FY2026 observed records"
EVIDENCE_EXPORT_ROW_LIMIT = 5000

PLATFORM_GROUPS = {
    "TOMAHAWK": (
        "TOMAHAWK",
        "TACTOM (TACTICAL TOMAHAWK)",
        "BGM-109 TOMAHAWK",
    ),
    "PATRIOT AIR DEFENSE SYSTEM": (
        "PATRIOT",
        "PAC-3",
        "PAC-3 MSE",
        "LTAMDS",
    ),
}

PLATFORM_DISPLAY_NAMES = {
    "TOMAHAWK": "Tomahawk missile family",
    "PATRIOT AIR DEFENSE SYSTEM": "Patriot air defense system",
}

PLATFORM_ALIASES = {
    "HIGH MOBILITY ARTILLERY ROCKET SYSTEM": "HIMARS",
    "GMLRS": "GMLRS/GMLRS AW",
    "MLRS": "GMLRS/GMLRS AW",
    "TRIDENT II": "TRIDENT II MISSILE",
    "TOMAHAWK": "TOMAHAWK",
    "TOMAHAWK MISSILE": "TOMAHAWK",
    "TOMAHAWK MISSILE FAMILY": "TOMAHAWK",
    "TACTOM": "TOMAHAWK",
    "TACTICAL TOMAHAWK": "TOMAHAWK",
    "BGM 109 TOMAHAWK": "TOMAHAWK",
    "PATRIOT": "PATRIOT AIR DEFENSE SYSTEM",
    "PATRIOT AIR DEFENSE": "PATRIOT AIR DEFENSE SYSTEM",
    "PATRIOT AIR DEFENSE SYSTEM": "PATRIOT AIR DEFENSE SYSTEM",
    "PATRIOT AIR DEFENCE": "PATRIOT AIR DEFENSE SYSTEM",
    "PATRIOT AIR DEFENCE SYSTEM": "PATRIOT AIR DEFENSE SYSTEM",
    "ABRAMS": "M1 ABRAMS",
    "APACHE": "AH-64",
    "BLACK HAWK": "UH-60",
    "CHINOOK": "CH-47",
    "VIRGINIA CLASS": "VIRGINIA CLASS (SSN 774)",
    "DDG 51": "DDG-51 ARLEIGH BURKE",
    "FORD CLASS": "FORD CLASS CARRIER",
    "COLUMBIA": "COLUMBIA CLASS SSBN",
    "COLUMBIA CLASS": "COLUMBIA CLASS SSBN",
    "COLOMBIA": "COLUMBIA CLASS SSBN",
    "COLOMBIA CLASS": "COLUMBIA CLASS SSBN",
    "F 15EX": "F-15",
    "F15EX": "F-15",
    "EAGLE II": "F-15",
    "KING STALLION": "CH-53K",
    "CH 53K KING STALLION": "CH-53K",
    "E 7A": "E-7",
    "E7A": "E-7",
    "WEDGETAIL": "E-7",
    "LRASM": "JASSM",
    "LONG RANGE ANTI SHIP MISSILE": "JASSM",
    "STANDARD MISSILE 6": "SM-6",
    "STANDARD MISSILE SIX": "SM-6",
}

PLATFORM_FOCUSES = {
    "F-15EX": {
        "display_name": "F-15EX",
        "base_platform": "F-15",
        "match_pattern": r"F-?15EX|EAGLE II",
        "related_mapped_platforms": ["F-15"],
        "relationship": "variant focus within the F-15 platform family",
    },
    "LRASM": {
        "display_name": "LRASM",
        "base_platform": "JASSM",
        "match_pattern": r"LRASM|LONG RANGE ANTI.SHIP MISSILE",
        "related_mapped_platforms": ["JASSM", "P-8A"],
        "relationship": "program focus within the JASSM/LRASM industrial family",
    },
    "E-7A": {
        "display_name": "E-7A Wedgetail",
        "base_platform": "E-7",
        "match_pattern": r"E-?7A|WEDGETAIL",
        "related_mapped_platforms": ["E-7", "E-7A RP"],
        "relationship": "US variant focus within the E-7 platform family",
    },
}

# Tomahawk aliases are intentionally collapsed into one family. Patriot retains
# its component program labels so a PAC-3-specific query can stay PAC-3-specific.
COLLAPSED_PLATFORM_GROUPS = {"TOMAHAWK"}


def _rows(cursor: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _canonical_fingerprint_value(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 2)
    if isinstance(value, dict):
        return {
            key: _canonical_fingerprint_value(item)
            for key, item in sorted(value.items())
        }
    if isinstance(value, list):
        normalized = [_canonical_fingerprint_value(item) for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(item, default=str, sort_keys=True),
        )
    return value


def _normalize(value: Any) -> str:
    return " ".join(re.findall(r"[A-Z0-9]+", str(value or "").upper()))


def _date(value: Any) -> str | None:
    text = str(value or "").strip()
    return text[:10] if text else None


def requested_platform_focus(text: str) -> str | None:
    normalized = f" {_normalize(text)} "
    if (
        " F 15EX " in normalized
        or " F15EX " in normalized
        or " EAGLE II " in normalized
    ):
        return "F-15EX"
    if " LRASM " in normalized or " LONG RANGE ANTI SHIP MISSILE " in normalized:
        return "LRASM"
    if " E 7A " in normalized or " E7A " in normalized or " WEDGETAIL " in normalized:
        return "E-7A"
    return None


class PlatformContextStore:
    """Build a common evidence baseline for any mapped platform or program."""

    def __init__(
        self,
        data_root: Path = DEFAULT_DATA_ROOT,
        precomputed_dir: Path | None = None,
        *,
        load_precomputed: bool = True,
    ) -> None:
        self.data_root = data_root.resolve()
        self.paths = {
            name: self.data_root / filename
            for name, filename in {
                "transactions": "transactions.parquet",
                "network": "network.parquet",
                "platform_bom": "platform_bom.parquet",
                "niin_source_depth": "niin_source_depth.parquet",
                "platform_source_depth": "platform_source_depth.parquet",
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
        duckdb_temp = os.getenv("ASK_MIMIR_DUCKDB_TEMP", "/tmp/ask-mimir-duckdb")
        self.connection.execute("SET temp_directory = ?", [duckdb_temp])
        self.platforms = self._load_platform_catalog()
        self._cache: Dict[str, Dict[str, Any]] = {}
        configured_dir = os.getenv("ASK_MIMIR_PLATFORM_CONTEXT_DIR", "").strip()
        self.precomputed_dir = (
            precomputed_dir.resolve()
            if precomputed_dir is not None
            else Path(configured_dir).resolve()
            if configured_dir
            else None
        )
        self._precomputed_paths: Dict[str, Path] = {}
        if load_precomputed and self.precomputed_dir is not None:
            manifest_path = self.precomputed_dir / "manifest.json"
            if manifest_path.exists():
                manifest = json.loads(manifest_path.read_text())
                self._precomputed_paths = {
                    str(entry["platform_id"]).upper(): self.precomputed_dir
                    / str(entry["path"])
                    for entry in manifest.get("platforms", [])
                }

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
        grouped_members = {
            _normalize(member)
            for group, members in PLATFORM_GROUPS.items()
            if group in COLLAPSED_PLATFORM_GROUPS
            for member in members
        }
        platforms = [
            platform
            for normalized, platform in catalog.items()
            if normalized not in grouped_members
        ]
        platforms.extend(PLATFORM_GROUPS)
        return sorted(set(platforms), key=_normalize)

    def search(self, query: str, limit: int = 15) -> Dict[str, Any]:
        clean = str(query or "").strip()
        normalized = _normalize(clean)
        if not normalized:
            return {"query": clean, "matches": [], "requires_disambiguation": False}
        alias_target = PLATFORM_ALIASES.get(normalized)
        exact = [alias_target] if alias_target in self.platforms else []
        if not exact:
            exact = [platform for platform in self.platforms if _normalize(platform) == normalized]
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
                    "display_name": PLATFORM_DISPLAY_NAMES.get(platform, platform),
                    "option_label": PLATFORM_DISPLAY_NAMES.get(platform, platform),
                }
                for platform in matches
            ],
            "requires_disambiguation": len(matches) > 1,
            "resolved_platform_id": matches[0] if len(matches) == 1 else None,
        }

    def mentions(self, text: str) -> List[str]:
        normalized = f" {_normalize(text)} "
        alias_matches = [
            target
            for alias, target in sorted(
                PLATFORM_ALIASES.items(), key=lambda item: len(item[0]), reverse=True
            )
            if f" {alias} " in normalized and target in self.platforms
        ]
        grouped_alias_matches = [
            target for target in alias_matches if target in PLATFORM_GROUPS
        ]
        if grouped_alias_matches:
            return list(dict.fromkeys(grouped_alias_matches))

        matches = []
        for platform in self.platforms:
            candidate = _normalize(platform)
            compact_candidate = candidate.replace(" ", "")
            compact_match = (
                compact_candidate != candidate
                and len(compact_candidate) >= 3
                and any(character.isdigit() for character in compact_candidate)
                and f" {compact_candidate} " in normalized
            )
            if len(candidate) >= 3 and (
                f" {candidate} " in normalized or compact_match
            ):
                matches.append(platform)
        matches.extend(alias_matches)
        ordered = sorted(set(matches), key=lambda value: len(_normalize(value)), reverse=True)
        return [
            platform
            for platform in ordered
            if not any(
                _normalize(platform) != _normalize(other)
                and f" {_normalize(platform)} " in f" {_normalize(other)} "
                for other in ordered
            )
        ]

    def get(self, platform_id: str) -> Dict[str, Any]:
        resolution = self.search(platform_id)
        resolved = resolution.get("resolved_platform_id")
        if not resolved:
            if resolution.get("requires_disambiguation"):
                raise ValueError(f"platform identifier is ambiguous: {platform_id}")
            raise KeyError(f"platform was not found: {platform_id}")
        if resolved in self._cache:
            return self._cache[resolved]
        precomputed_path = self._precomputed_paths.get(resolved.upper())
        if precomputed_path is not None and precomputed_path.exists():
            context = json.loads(precomputed_path.read_text())
            if context.get("scope", {}).get("platform_id") == resolved:
                self._cache[resolved] = context
                return context

        members = self._platform_members(resolved)
        annual = self._annual_activity(resolved)
        direct_recipients = self._direct_award_recipients(resolved)
        reported_suppliers = self._reported_supplier_sites(resolved)
        self._attach_supplier_annual_activity(resolved, reported_suppliers)
        items = self._item_evidence(resolved)
        opportunities = self._opportunities(resolved)
        top_awards = self._top_awards(resolved)
        component_categories = self._component_categories(resolved)
        financial_totals = self._financial_totals(resolved, annual)
        prime_total = float(financial_totals["positive_prime_obligations_usd"] or 0)
        positive_supplier_values = [
            max(float(row.get("mimir_modelled_reported_subcontract_value_usd") or 0), 0)
            for row in reported_suppliers
        ]
        subcontract_total = sum(positive_supplier_values)
        for row in direct_recipients:
            row["share_of_platform_prime_obligations_pct"] = (
                float(row.get("positive_prime_obligations_usd") or 0) / prime_total * 100
                if prime_total else 0
            )
        for row in reported_suppliers:
            row["share_of_reported_subcontract_value_pct"] = (
                max(float(row.get("mimir_modelled_reported_subcontract_value_usd") or 0), 0)
                / subcontract_total * 100 if subcontract_total else 0
            )
        positive_supplier_values = sorted(positive_supplier_values, reverse=True)
        positive_supplier_total = sum(positive_supplier_values)
        observed_dla_recipient_sites = int(items.get("observed_dla_recipient_site_count") or 0)
        reported_supplier_lane_is_sparse = (
            len(reported_suppliers) < 10
            and (
                int(items.get("associated_niin_count") or 0) >= 100
                or observed_dla_recipient_sites >= 25
            )
        )
        supplier_concentration = {
            "supplier_site_count": len(reported_suppliers),
            "positive_reported_subcontract_value_usd": positive_supplier_total,
            "top_supplier_share_pct": (
                positive_supplier_values[0] / positive_supplier_total * 100
                if positive_supplier_total and positive_supplier_values else 0
            ),
            "top_five_supplier_share_pct": (
                sum(positive_supplier_values[:5]) / positive_supplier_total * 100
                if positive_supplier_total else 0
            ),
            "top_ten_supplier_share_pct": (
                sum(positive_supplier_values[:10]) / positive_supplier_total * 100
                if positive_supplier_total else 0
            ),
            "interpretation_note": (
                "Value concentration describes the distribution of observed reported subcontract "
                "value across supplier sites. Component source depth is a separate question and "
                "depends on item-level source evidence."
            ),
        }
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
                "display_name": PLATFORM_DISPLAY_NAMES.get(resolved, resolved),
                "included_platform_records": members,
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
                "observed_dla_recipient_sites": observed_dla_recipient_sites,
                "associated_niins": items["associated_niin_count"],
                "item_relationships_loaded": len(items["top_items"]),
                "prime_awards": self._available_count(top_awards),
                "prime_awards_loaded": len(top_awards),
                "open_or_loaded_opportunities": len(opportunities),
                "component_proof_status": "CURATED_WHEN_AVAILABLE_OTHERWISE_REPORTED_DESCRIPTION_OR_ITEM_REFERENCE",
                "reported_supplier_lane_is_sparse": reported_supplier_lane_is_sparse,
                "reported_supplier_coverage_note": (
                    "Reported first-tier coverage is partial for this platform. Use the reported "
                    "supplier lane together with the wider NIIN and DLA procurement evidence."
                    if reported_supplier_lane_is_sparse
                    else "Reported first-tier coverage is broad enough for an observed supplier-base summary."
                ),
            },
            "financial_totals": financial_totals,
            "reported_supplier_concentration": supplier_concentration,
            "methodology": {
                "direct_award_lane": "Prime obligations on awards mapped directly to this platform or program.",
                "reported_supplier_lane": "Mimir-modelled reported first-tier subcontract value on mapped prime awards; kept separate from prime obligations.",
                "item_lane": "NIIN relationships mapped through the WSDC/platform bridge. Attributed procurement and shared-use exposure are reported separately.",
                "component_rule": "Reported descriptions support bounded capability language. Exact component claims require a platform-specific government or first-party source.",
                "opportunity_rule": "Opportunity matches are research leads based on the platform or program name in the loaded notice text.",
            },
            "evidence_index": self._source_index(resolved, top_awards, opportunities),
            "evidence_fingerprint": hashlib.sha256(
                json.dumps(
                    _canonical_fingerprint_value(fingerprint_input),
                    default=str,
                    sort_keys=True,
                ).encode()
            ).hexdigest(),
        }
        self._cache[resolved] = context
        return context

    def comparison_projection(self, platform_id: str) -> Dict[str, Any]:
        """Return the supplier evidence needed for a multi-platform comparison."""
        resolution = self.search(platform_id)
        resolved = resolution.get("resolved_platform_id")
        if not resolved:
            raise KeyError(f"platform was not found: {platform_id}")
        suppliers = self._reported_supplier_sites(resolved, limit=500)
        self._attach_supplier_annual_activity(resolved, suppliers)
        categories = self._component_categories(resolved, limit=150)
        positive_values = [
            max(
                float(
                    row.get("mimir_modelled_reported_subcontract_value_usd") or 0
                ),
                0,
            )
            for row in suppliers
        ]
        positive_total = sum(positive_values)
        return {
            "scope": {
                "platform_id": resolved,
                "display_name": PLATFORM_DISPLAY_NAMES.get(resolved, resolved),
                "included_platform_records": self._platform_members(resolved),
                "completed_fiscal_years": list(COMPLETED_FISCAL_YEARS),
                "partial_fiscal_year": 2026,
                "observation_window": OBSERVATION_WINDOW,
            },
            "reported_supplier_sites": suppliers,
            "reported_component_categories": categories,
            "reported_supplier_summary": {
                "supplier_site_count": len(suppliers),
                "available_supplier_site_count": self._available_count(suppliers),
                "positive_reported_subcontract_value_usd": positive_total,
                "top_supplier_share_pct": (
                    max(positive_values) / positive_total * 100
                    if positive_total and positive_values
                    else 0
                ),
                "top_five_supplier_share_pct": (
                    sum(sorted(positive_values, reverse=True)[:5])
                    / positive_total
                    * 100
                    if positive_total
                    else 0
                ),
            },
        }

    @staticmethod
    def _available_count(rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        return int(rows[0].get("total_available") or len(rows))

    @staticmethod
    def _platform_members(platform: str) -> List[str]:
        return list(PLATFORM_GROUPS.get(platform, (platform,)))

    @classmethod
    def _single_platform_condition(cls, alias: str = "t") -> str:
        return f"{alias}.platform_family IN (SELECT UNNEST(?))"

    @classmethod
    def _multi_platform_condition(cls, alias: str = "t") -> str:
        return (
            f"({alias}.platform_family IN (SELECT UNNEST(?)) OR "
            f"LIST_HAS_ANY(STR_SPLIT(COALESCE({alias}.platform_families, ''), ' | '), ?))"
        )

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
        self._attach_supplier_annual_activity(resolved, expanded["reported_supplier_sites"])
        return expanded

    def answer_projection(
        self, platform_id: str, supplier_limit: int = 14, focus_id: str | None = None
    ) -> Dict[str, Any]:
        if focus_id:
            return self._focused_program_projection(platform_id, focus_id, supplier_limit)
        context = self.get(platform_id)
        supplier_limit = min(max(int(supplier_limit), 1), 250)
        direct = []
        for row in context["direct_award_recipients"][:8]:
            direct.append({
                **row,
                "sample_contract_ids": (row.get("sample_contract_ids") or [])[:4],
                "sample_award_descriptions": (row.get("sample_award_descriptions") or [])[:3],
                "observed_places_of_performance": (row.get("observed_places_of_performance") or [])[:4],
            })
        suppliers = []
        for row in context["reported_supplier_sites"][:supplier_limit]:
            suppliers.append({
                **row,
                "reported_prime_names": (row.get("reported_prime_names") or [])[:4],
                "reported_prime_cages": (row.get("reported_prime_cages") or [])[:4],
                "sample_prime_contract_ids": (row.get("sample_prime_contract_ids") or [])[:4],
                "reported_descriptions": (row.get("reported_descriptions") or [])[:4],
            })
        self._attach_supplier_annual_activity(resolved, suppliers)
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
        projected = {
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
        return projected

    def _focused_program_projection(
        self, platform_id: str, focus_id: str, supplier_limit: int
    ) -> Dict[str, Any]:
        resolution = self.search(platform_id)
        resolved = resolution.get("resolved_platform_id")
        if not resolved:
            raise KeyError(f"base platform was not found: {platform_id}")
        focus = self._focus_evidence(focus_id)
        if focus["base_platform"] != resolved:
            raise ValueError(
                f"{focus_id} is configured against {focus['base_platform']}, not {resolved}"
            )
        base_activity = _rows(
            self.connection.execute(
                """
                SELECT year AS fiscal_year, source_system,
                       SUM(CASE WHEN source_system = 'USA_SPENDING' THEN spend_amount ELSE 0 END)
                           AS net_prime_obligations_usd,
                       SUM(CASE WHEN source_system = 'DLA'
                           THEN COALESCE(platform_attributed_spend_amount, 0) ELSE 0 END)
                           AS attributed_dla_procurement_value_usd,
                       SUM(CASE WHEN source_system = 'DLA'
                           THEN COALESCE(shared_use_exposure_amount, 0) ELSE 0 END)
                           AS shared_use_niin_exposure_usd,
                       COUNT(DISTINCT award_key) AS award_count
                FROM read_parquet(?)
                WHERE year BETWEEN 2021 AND 2026 AND platform_family = ?
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
                [str(self.paths["transactions"]), resolved],
            )
        )
        focus_prime = sum(
            float(row.get("net_value_usd") or 0)
            for row in focus["explicit_named_record_summary"]
            if row.get("source_system") == "USA_SPENDING"
        )
        focus_supplier_value = sum(
            float(row.get("mimir_modelled_reported_subcontract_value_usd") or 0)
            for row in focus["reported_supplier_sites_on_explicit_records"]
        )
        supplier_limit = min(max(int(supplier_limit), 1), 250)
        return {
            "context_type": "focused_platform_or_program_dossier",
            "scope": {
                "platform_id": resolved,
                "display_name": focus["display_name"],
                "included_platform_records": [resolved],
                "completed_fiscal_years": list(COMPLETED_FISCAL_YEARS),
                "partial_fiscal_year": 2026,
                "observation_window": OBSERVATION_WINDOW,
                "requested_focus": focus,
            },
            "annual_activity": {
                "records": base_activity,
                "completed_fiscal_years": list(COMPLETED_FISCAL_YEARS),
                "partial_fiscal_year": 2026,
            },
            "direct_award_recipients": [],
            "reported_supplier_sites": focus[
                "reported_supplier_sites_on_explicit_records"
            ][:supplier_limit],
            "reported_component_categories": [],
            "item_and_component_evidence": {
                "associated_niin_count": 0,
                "top_items": [],
                "top_item_supplier_sites": [],
            },
            "top_prime_awards": focus["top_explicit_prime_awards"],
            "current_opportunities": [],
            "coverage": {
                "reported_supplier_sites": len(
                    focus["reported_supplier_sites_on_explicit_records"]
                ),
                "reported_supplier_sites_loaded": min(
                    len(focus["reported_supplier_sites_on_explicit_records"]),
                    supplier_limit,
                ),
                "prime_awards": len(focus["top_explicit_prime_awards"]),
                "prime_awards_loaded": len(focus["top_explicit_prime_awards"]),
                "focus_uses_explicit_named_records": True,
                "reported_supplier_lane_is_sparse": len(
                    focus["reported_supplier_sites_on_explicit_records"]
                ) < 10,
            },
            "financial_totals": {
                "observation_window": OBSERVATION_WINDOW,
                "net_prime_obligations_usd": focus_prime,
                "mimir_modelled_reported_subcontract_value_usd": focus_supplier_value,
            },
            "reported_supplier_concentration": {},
            "methodology": {
                "focus_rule": "Explicit named records define the requested variant or related program.",
                "base_rule": "The mapped base platform provides wider industrial context without relabelling all base activity as the requested focus.",
            },
            "evidence_index": [],
        }

    def _focus_evidence(self, focus_id: str) -> Dict[str, Any]:
        clean_focus = str(focus_id or "").strip().upper()
        definition = PLATFORM_FOCUSES.get(clean_focus)
        if not definition:
            raise KeyError(f"platform focus was not found: {focus_id}")
        pattern = definition["match_pattern"]
        transaction_expression = (
            "UPPER(COALESCE(base_award_description, '') || ' ' || "
            "COALESCE(action_description, '') || ' ' || COALESCE(description, ''))"
        )
        network_expression = (
            "UPPER(COALESCE(prime_award_description, '') || ' ' || "
            "COALESCE(description, ''))"
        )
        mapped_records = _rows(
            self.connection.execute(
                f"""
                SELECT source_system, COALESCE(platform_family, 'UNMAPPED') AS platform_family,
                       COUNT(*) AS record_count, COUNT(DISTINCT contract_id) AS award_count,
                       SUM(spend_amount) AS net_value_usd
                FROM read_parquet(?)
                WHERE year BETWEEN 2021 AND 2026
                  AND REGEXP_MATCHES({transaction_expression}, ?)
                GROUP BY 1, 2
                ORDER BY ABS(net_value_usd) DESC
                """,
                [str(self.paths["transactions"]), pattern],
            )
        )
        prime_awards = _rows(
            self.connection.execute(
                f"""
                SELECT contract_id, MAX(vendor_name) AS recipient_name,
                       MAX(vendor_cage) AS recipient_cage,
                       MAX(base_award_description) AS base_award_description,
                       SUM(spend_amount) AS net_prime_obligations_usd,
                       MIN(SUBSTR(action_date, 1, 10)) AS first_action_date,
                       MAX(SUBSTR(action_date, 1, 10)) AS latest_action_date
                FROM read_parquet(?)
                WHERE source_system = 'USA_SPENDING' AND year BETWEEN 2021 AND 2026
                  AND REGEXP_MATCHES({transaction_expression}, ?)
                GROUP BY contract_id
                ORDER BY ABS(net_prime_obligations_usd) DESC
                LIMIT 20
                """,
                [str(self.paths["transactions"]), pattern],
            )
        )
        supplier_sites = _rows(
            self.connection.execute(
                f"""
                SELECT sub_cage AS cage, MAX(sub_name) AS supplier_name,
                       MAX(sub_city) AS city, MAX(sub_state) AS state,
                       SUM(COALESCE(subaward_value, 0))
                           AS mimir_modelled_reported_subcontract_value_usd,
                       LIST_SLICE(LIST_SORT(LIST_DISTINCT(LIST(description) FILTER (
                           WHERE description IS NOT NULL AND TRIM(description) <> ''
                       ))), 1, 8) AS reported_descriptions,
                       LIST_SLICE(LIST_SORT(LIST_DISTINCT(LIST(platform_family))), 1, 8)
                           AS current_platform_mappings
                FROM read_parquet(?)
                WHERE year BETWEEN 2021 AND 2026
                  AND REGEXP_MATCHES({network_expression}, ?)
                  AND sub_cage IS NOT NULL
                  AND UPPER(TRIM(sub_cage)) NOT IN ('', 'UNKNOWN', 'UNKNO')
                GROUP BY sub_cage
                HAVING SUM(COALESCE(subaward_value, 0)) <> 0
                ORDER BY ABS(mimir_modelled_reported_subcontract_value_usd) DESC
                LIMIT 40
                """,
                [str(self.paths["network"]), pattern],
            )
        )
        return {
            "focus_id": clean_focus,
            "display_name": definition["display_name"],
            "base_platform": definition["base_platform"],
            "relationship": definition["relationship"],
            "related_mapped_platforms": definition["related_mapped_platforms"],
            "observation_window": OBSERVATION_WINDOW,
            "explicit_named_record_summary": mapped_records,
            "top_explicit_prime_awards": prime_awards,
            "reported_supplier_sites_on_explicit_records": supplier_sites,
        }

    def _annual_activity(self, platform: str) -> Dict[str, Any]:
        condition = self._multi_platform_condition("t")
        members = self._platform_members(platform)
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
                [str(self.paths["transactions"]), members, members],
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
        members = self._platform_members(platform)
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
                    LIST_SLICE(LIST_SORT(LIST_DISTINCT(LIST(t.contract_id) FILTER (WHERE t.contract_id IS NOT NULL))),1,8) AS sample_contract_ids,
                    LIST_SLICE(LIST_SORT(LIST_DISTINCT(LIST(t.base_award_description) FILTER (WHERE t.base_award_description IS NOT NULL))),1,6) AS sample_award_descriptions,
                    LIST_SLICE(LIST_SORT(LIST_DISTINCT(LIST(CONCAT_WS(', ', NULLIF(t.place_of_performance_city,''), NULLIF(t.place_of_performance_state,''), NULLIF(t.place_of_performance_country,''))) FILTER (WHERE NULLIF(t.place_of_performance_city,'') IS NOT NULL))),1,8) AS observed_places_of_performance,
                    COUNT(*) OVER () AS total_available
                FROM read_parquet(?) t
                LEFT JOIN locations l ON UPPER(TRIM(t.vendor_cage)) = l.cage
                WHERE t.source_system = 'USA_SPENDING'
                  AND t.year BETWEEN 2021 AND 2026
                  AND t.platform_family IN (SELECT UNNEST(?))
                GROUP BY t.vendor_cage
                ORDER BY positive_prime_obligations_usd DESC,
                         net_prime_obligations_usd DESC,
                         t.vendor_cage
                LIMIT ?
                """,
                [str(self.paths["locations"]), str(self.paths["transactions"]), members, limit],
            )
        )

    def _reported_supplier_sites(self, platform: str, limit: int = 250) -> List[Dict[str, Any]]:
        members = self._platform_members(platform)
        return _rows(
            self.connection.execute(
                """
                WITH locations AS (
                    SELECT UPPER(TRIM(cage_code)) AS cage, MAX(vendor_name) AS location_name,
                           MAX(city) AS city, MAX(state) AS state, MAX(location_quality) AS location_quality
                    FROM read_parquet(?) GROUP BY 1
                ), supplier_platforms AS (
                    SELECT
                        UPPER(TRIM(sub_cage)) AS cage,
                        LIST_SLICE(
                            LIST_SORT(LIST_DISTINCT(
                                LIST(platform_family) FILTER (
                                    WHERE platform_family IS NOT NULL AND TRIM(platform_family) <> ''
                                )
                            )),
                            1,
                            30
                        ) AS mapped_platforms
                    FROM read_parquet(?)
                    WHERE year BETWEEN 2021 AND 2026
                      AND UPPER(TRIM(COALESCE(platform_family, ''))) NOT IN (
                          '', 'UNMAPPED', 'REVIEW NEEDED'
                      )
                      AND sub_cage IS NOT NULL
                      AND UPPER(TRIM(sub_cage)) NOT IN ('','UNKNOWN','UNKNO')
                    GROUP BY 1
                )
                SELECT
                    n.sub_cage AS cage,
                    COALESCE(MAX(n.sub_name), MAX(l.location_name)) AS supplier_name,
                    MAX(COALESCE(n.sub_city,l.city)) AS city,
                    MAX(COALESCE(n.sub_state,l.state)) AS state,
                    MAX(n.sub_country) AS country,
                    MAX(l.location_quality) AS location_quality,
                    SUM(COALESCE(n.subaward_value,0)) AS mimir_modelled_reported_subcontract_value_usd,
                    SUM(CASE WHEN COALESCE(n.subaward_value,0) > 0 THEN n.subaward_value ELSE 0 END)
                        AS positive_mimir_modelled_reported_subcontract_value_usd,
                    SUM(CASE WHEN COALESCE(n.subaward_value,0) < 0 THEN n.subaward_value ELSE 0 END)
                        AS negative_mimir_modelled_reported_subcontract_value_usd,
                    SUM(COALESCE(n.subaward_value_raw,0)) AS source_reported_value_usd,
                    COUNT(*) AS selected_report_count,
                    COUNT(DISTINCT n.contract_id) AS prime_award_count,
                    MIN(SUBSTR(n.action_date,1,10)) AS first_reported_date,
                    MAX(SUBSTR(n.action_date,1,10)) AS latest_reported_date,
                    LIST_SLICE(LIST_SORT(LIST_DISTINCT(LIST(n.prime_name) FILTER (WHERE n.prime_name IS NOT NULL))),1,8) AS reported_prime_names,
                    LIST_SLICE(LIST_SORT(LIST_DISTINCT(LIST(n.prime_cage) FILTER (WHERE n.prime_cage IS NOT NULL))),1,8) AS reported_prime_cages,
                    LIST_SLICE(LIST_SORT(LIST_DISTINCT(LIST(n.contract_id) FILTER (WHERE n.contract_id IS NOT NULL))),1,8) AS sample_prime_contract_ids,
                    LIST_SLICE(LIST_SORT(LIST_DISTINCT(LIST(n.description) FILTER (WHERE n.description IS NOT NULL))),1,8) AS reported_descriptions,
                    ANY_VALUE(sp.mapped_platforms) AS mapped_platforms,
                    COUNT(*) OVER () AS total_available
                FROM read_parquet(?) n
                LEFT JOIN locations l ON UPPER(TRIM(n.sub_cage)) = l.cage
                LEFT JOIN supplier_platforms sp ON UPPER(TRIM(n.sub_cage)) = sp.cage
                WHERE n.platform_family IN (SELECT UNNEST(?))
                  AND n.year BETWEEN 2021 AND 2026
                  AND n.sub_cage IS NOT NULL
                  AND UPPER(TRIM(n.sub_cage)) NOT IN ('','UNKNOWN','UNKNO')
                GROUP BY n.sub_cage
                HAVING SUM(COALESCE(n.subaward_value,0)) <> 0
                ORDER BY mimir_modelled_reported_subcontract_value_usd DESC, n.sub_cage
                LIMIT ?
                """,
                [
                    str(self.paths["locations"]),
                    str(self.paths["network"]),
                    str(self.paths["network"]),
                    members,
                    limit,
                ],
            )
        )

    def _attach_supplier_annual_activity(
        self,
        platform: str,
        suppliers: List[Dict[str, Any]],
    ) -> None:
        cages = sorted({
            str(row.get("cage") or "").strip().upper()
            for row in suppliers
            if str(row.get("cage") or "").strip()
            and "annual_reported_subcontract_activity" not in row
        })
        if not cages:
            return
        members = self._platform_members(platform)
        rows = _rows(
            self.connection.execute(
                """
                SELECT
                    UPPER(TRIM(sub_cage)) AS cage,
                    year AS fiscal_year,
                    SUM(COALESCE(subaward_value, 0))
                        AS mimir_modelled_reported_subcontract_value_usd,
                    SUM(COALESCE(subaward_value_raw, 0)) AS source_reported_value_usd,
                    COUNT(DISTINCT source_dedup_key) AS selected_report_count,
                    COUNT(DISTINCT contract_id) AS prime_award_count
                FROM read_parquet(?)
                WHERE platform_family IN (SELECT UNNEST(?))
                  AND year BETWEEN 2021 AND 2026
                  AND UPPER(TRIM(sub_cage)) IN (SELECT UNNEST(?))
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
                [str(self.paths["network"]), members, cages],
            )
        )
        by_cage: Dict[str, Dict[int, Dict[str, Any]]] = {}
        for row in rows:
            by_cage.setdefault(str(row["cage"]), {})[int(row["fiscal_year"])] = row
        for supplier in suppliers:
            observations = by_cage.get(str(supplier.get("cage") or "").upper(), {})
            supplier["annual_reported_subcontract_activity"] = [
                observations.get(year) or {
                    "cage": str(supplier.get("cage") or "").upper(),
                    "fiscal_year": year,
                    "mimir_modelled_reported_subcontract_value_usd": None,
                    "source_reported_value_usd": None,
                    "selected_report_count": 0,
                    "prime_award_count": 0,
                    "observation_status": "NOT_OBSERVED",
                }
                for year in range(2021, 2027)
            ]

    def _component_categories(self, platform: str, limit: int = 100) -> List[Dict[str, Any]]:
        members = self._platform_members(platform)
        return _rows(
            self.connection.execute(
                """
                SELECT
                    description AS reported_description,
                    SUM(COALESCE(subaward_value,0)) AS mimir_modelled_reported_subcontract_value_usd,
                    COUNT(*) AS selected_report_count,
                    COUNT(DISTINCT sub_cage) AS supplier_site_count,
                    LIST_SLICE(LIST_SORT(LIST_DISTINCT(LIST(sub_name) FILTER (WHERE sub_name IS NOT NULL))),1,8) AS suppliers,
                    LIST_SLICE(LIST_SORT(LIST_DISTINCT(LIST(contract_id) FILTER (WHERE contract_id IS NOT NULL))),1,6) AS sample_prime_contract_ids,
                    COUNT(*) OVER () AS total_available
                FROM read_parquet(?)
                WHERE platform_family IN (SELECT UNNEST(?)) AND year BETWEEN 2021 AND 2026
                  AND description IS NOT NULL AND TRIM(description) <> ''
                GROUP BY description
                HAVING SUM(COALESCE(subaward_value,0)) <> 0
                ORDER BY ABS(mimir_modelled_reported_subcontract_value_usd) DESC,
                         description
                LIMIT ?
                """,
                [str(self.paths["network"]), members, limit],
            )
        )

    def _item_evidence(self, platform: str, limit: int = 100) -> Dict[str, Any]:
        members = self._platform_members(platform)
        associated_count = self.connection.execute(
            "SELECT COUNT(DISTINCT LPAD(TRIM(niin),9,'0')) FROM read_parquet(?) WHERE platform_family IN (SELECT UNNEST(?))",
            [str(self.paths["platform_bom"]), members],
        ).fetchone()[0]
        top_items = _rows(
            self.connection.execute(
                """
                WITH bridge AS (
                    SELECT LPAD(TRIM(niin),9,'0') AS niin,
                           LIST_SORT(LIST_DISTINCT(LIST(wsdc_code))) AS wsdc_codes,
                           LIST_SORT(LIST_DISTINCT(LIST(association_source))) AS association_sources
                    FROM read_parquet(?) WHERE platform_family IN (SELECT UNNEST(?)) GROUP BY 1
                ), platform_value AS (
                    SELECT LPAD(TRIM(niin),9,'0') AS niin,
                           SUM(COALESCE(platform_attributed_spend_amount,0)) AS attributed_dla_procurement_value_usd,
                           SUM(COALESCE(shared_use_exposure_amount,0)) AS shared_use_niin_exposure_usd,
                           MAX(SUBSTR(action_date,1,10)) AS latest_observed_date
                    FROM read_parquet(?)
                    WHERE source_system='DLA' AND year BETWEEN 2021 AND 2026
                      AND (platform_family IN (SELECT UNNEST(?)) OR EXISTS (
                          SELECT 1 FROM UNNEST(STR_SPLIT(COALESCE(platform_families,''),' | ')) member(value)
                          WHERE value IN (SELECT UNNEST(?))))
                    GROUP BY 1
                )
                SELECT p.nsn, b.niin, p.item_name AS description, p.fsc_code,
                       COALESCE(v.attributed_dla_procurement_value_usd,0) AS attributed_dla_procurement_value_usd,
                       COALESCE(v.shared_use_niin_exposure_usd,0) AS shared_use_niin_exposure_usd,
                       v.latest_observed_date,
                       b.wsdc_codes, b.association_sources,
                       COALESCE(s.active_authorized_source_count, 0)
                           AS active_authorized_source_count,
                       s.active_authorized_source_cages,
                       s.active_authorized_source_names,
                       s.source_depth
                FROM bridge b
                LEFT JOIN read_parquet(?) p ON LPAD(TRIM(p.niin),9,'0') = b.niin
                LEFT JOIN platform_value v ON b.niin=v.niin
                LEFT JOIN read_parquet(?) s ON b.niin=s.niin
                ORDER BY ABS(COALESCE(v.attributed_dla_procurement_value_usd,0))
                       + ABS(COALESCE(v.shared_use_niin_exposure_usd,0)) DESC,
                         b.niin
                LIMIT ?
                """,
                [
                    str(self.paths["platform_bom"]), members,
                    str(self.paths["transactions"]), members, members,
                    str(self.paths["item_profiles"]),
                    str(self.paths["niin_source_depth"]),
                    limit,
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
                  AND (platform_family IN (SELECT UNNEST(?)) OR EXISTS (
                      SELECT 1 FROM UNNEST(STR_SPLIT(COALESCE(platform_families,''),' | ')) member(value)
                      WHERE value IN (SELECT UNNEST(?))))
                GROUP BY niin,cage
                HAVING SUM(total_revenue) <> 0
                ), locations AS (
                    SELECT UPPER(TRIM(cage_code)) AS cage, MAX(city) AS city, MAX(state) AS state,
                           MAX(location_quality) AS location_quality
                    FROM read_parquet(?) GROUP BY 1
                )
                SELECT s.*, l.city, l.state, l.location_quality,
                       COUNT(*) OVER () AS total_available,
                       COUNT(DISTINCT s.cage) OVER () AS total_supplier_sites
                FROM supplier_values s
                LEFT JOIN locations l ON UPPER(TRIM(s.cage))=l.cage
                ORDER BY ABS(attributed_dla_procurement_value_usd)
                       + ABS(shared_use_niin_exposure_usd) DESC,
                         s.niin,
                         s.cage
                LIMIT ?
                """,
                [
                    str(self.paths["item_suppliers"]), members, members,
                    str(self.paths["locations"]), limit,
                ],
            )
        )
        source_depth = self._source_depth_summary(members)
        return {
            "associated_niin_count": associated_count,
            "authorized_source_depth": source_depth,
            "observed_dla_recipient_site_count": int(
                suppliers[0].get("total_supplier_sites") or 0
            ) if suppliers else 0,
            "top_items": top_items,
            "top_item_supplier_sites": suppliers,
            "financial_treatment": "Single-platform attributed value and shared-use NIIN exposure remain separate.",
        }

    def _source_depth_summary(self, members: List[str]) -> Dict[str, Any]:
        if len(members) == 1:
            rows = _rows(
                self.connection.execute(
                    """
                    SELECT associated_niin_count,
                           niin_count_without_active_authorized_source,
                           niin_count_with_one_active_authorized_source,
                           niin_count_with_multiple_active_authorized_sources,
                           active_authorized_source_relationship_count
                    FROM read_parquet(?)
                    WHERE platform_family = ?
                    """,
                    [str(self.paths["platform_source_depth"]), members[0]],
                )
            )
        else:
            rows = _rows(
                self.connection.execute(
                    """
                    WITH member_items AS (
                        SELECT DISTINCT LPAD(TRIM(niin), 9, '0') AS niin
                        FROM read_parquet(?)
                        WHERE platform_family IN (SELECT UNNEST(?))
                    )
                    SELECT
                        COUNT(*) AS associated_niin_count,
                        COUNT(*) FILTER (WHERE s.active_authorized_source_count = 0)
                            AS niin_count_without_active_authorized_source,
                        COUNT(*) FILTER (WHERE s.active_authorized_source_count = 1)
                            AS niin_count_with_one_active_authorized_source,
                        COUNT(*) FILTER (WHERE s.active_authorized_source_count > 1)
                            AS niin_count_with_multiple_active_authorized_sources,
                        SUM(s.active_authorized_source_count)
                            AS active_authorized_source_relationship_count
                    FROM member_items m
                    JOIN read_parquet(?) s USING (niin)
                    """,
                    [
                        str(self.paths["platform_bom"]),
                        members,
                        str(self.paths["niin_source_depth"]),
                    ],
                )
            )
        return rows[0] if rows else {
            "associated_niin_count": 0,
            "niin_count_without_active_authorized_source": 0,
            "niin_count_with_one_active_authorized_source": 0,
            "niin_count_with_multiple_active_authorized_sources": 0,
            "active_authorized_source_relationship_count": 0,
        }

    def _top_awards(self, platform: str, limit: int = 100) -> List[Dict[str, Any]]:
        members = self._platform_members(platform)
        return _rows(
            self.connection.execute(
                """
                SELECT contract_id, vendor_name AS recipient_name, vendor_cage AS recipient_cage,
                       base_award_description, SUM(spend_amount) AS net_prime_obligations_usd,
                       SUM(CASE WHEN spend_amount > 0 THEN spend_amount ELSE 0 END)
                           AS positive_prime_obligations_usd,
                       COUNT(*) AS action_count, MIN(SUBSTR(action_date,1,10)) AS first_action_date,
                       MAX(SUBSTR(action_date,1,10)) AS latest_action_date,
                       MAX(place_of_performance_city) AS place_of_performance_city,
                       MAX(place_of_performance_state) AS place_of_performance_state,
                       COUNT(*) OVER () AS total_available
                FROM read_parquet(?)
                WHERE source_system='USA_SPENDING' AND year BETWEEN 2021 AND 2026
                  AND platform_family IN (SELECT UNNEST(?))
                GROUP BY contract_id,vendor_name,vendor_cage,base_award_description
                ORDER BY positive_prime_obligations_usd DESC,
                         net_prime_obligations_usd DESC,
                         contract_id,
                         recipient_cage
                LIMIT ?
                """,
                [str(self.paths["transactions"]), members, limit],
            )
        )

    def _financial_totals(self, platform: str, annual: Dict[str, Any]) -> Dict[str, Any]:
        totals = {
            "observation_window": OBSERVATION_WINDOW,
            "net_prime_obligations_usd": 0.0,
            "positive_prime_obligations_usd": 0.0,
            "prime_deobligations_usd": 0.0,
            "attributed_dla_procurement_value_usd": 0.0,
            "shared_use_niin_exposure_usd": 0.0,
        }
        for row in annual.get("records", []):
            totals["net_prime_obligations_usd"] += float(row.get("net_prime_obligations_usd") or 0)
            totals["positive_prime_obligations_usd"] += float(row.get("positive_prime_obligations_usd") or 0)
            totals["prime_deobligations_usd"] += float(row.get("prime_deobligations_usd") or 0)
            totals["attributed_dla_procurement_value_usd"] += float(row.get("attributed_dla_procurement_value_usd") or 0)
            totals["shared_use_niin_exposure_usd"] += float(row.get("shared_use_niin_exposure_usd") or 0)
        members = self._platform_members(platform)
        subcontract_total = self.connection.execute(
            """
            SELECT SUM(COALESCE(n.subaward_value, 0))
            FROM read_parquet(?) n
            WHERE n.year BETWEEN 2021 AND 2026
              AND n.platform_family IN (SELECT UNNEST(?))
            """,
            [str(self.paths["network"]), members],
        ).fetchone()[0]
        totals["mimir_modelled_reported_subcontract_value_usd"] = float(subcontract_total or 0)
        return totals

    def _opportunities(self, platform: str) -> List[Dict[str, Any]]:
        patterns = [f"%{member}%" for member in self._platform_members(platform)]
        rows = _rows(
            self.connection.execute(
                """
                SELECT id, sol_num, title, agency, sub_agency, SUBSTR(deadline,1,10) AS deadline,
                       set_aside_type, CAST(naics AS BIGINT)::VARCHAR AS naics_code, psc, state, url
                FROM read_parquet(?)
                WHERE EXISTS (
                    SELECT 1 FROM UNNEST(?) pattern(value)
                    WHERE UPPER(COALESCE(search_text,title,'')) LIKE UPPER(value)
                )
                ORDER BY deadline DESC, COALESCE(sol_num, id), id
                LIMIT 30
                """,
                [str(self.paths["opportunities"]), patterns],
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


def build_precomputed_platform_contexts(
    data_root: Path,
    output_dir: Path,
    platform_ids: Sequence[str],
    *,
    release_id: str | None = None,
) -> Dict[str, Any]:
    """Materialize selected high-use platform dossiers for one runtime release."""
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    entries = []
    for requested_id in platform_ids:
        store = PlatformContextStore(data_root, load_precomputed=False)
        resolution = store.search(requested_id)
        resolved = resolution.get("resolved_platform_id")
        if not resolved:
            raise RuntimeError(f"key platform did not resolve uniquely: {requested_id}")
        print(f"Building platform context: {resolved}", flush=True)
        context = store.get(resolved)
        filename = (
            hashlib.sha256(resolved.encode()).hexdigest()[:16]
            + "-platform-context.json"
        )
        destination = output_dir / filename
        temporary = destination.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(context, default=str))
        temporary.replace(destination)
        entries.append(
            {
                "platform_id": resolved,
                "display_name": context["scope"]["display_name"],
                "path": filename,
                "evidence_fingerprint": context["evidence_fingerprint"],
            }
        )
        store.connection.close()
    manifest = {
        "schema_version": "platform-context-precompute-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runtime_release_id": release_id,
        "platform_count": len(entries),
        "platforms": entries,
    }
    manifest_path = output_dir / "manifest.json"
    temporary = manifest_path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(manifest, indent=2))
    temporary.replace(manifest_path)
    return manifest
