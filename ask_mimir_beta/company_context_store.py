"""Read compact, versioned company contexts for the isolated Ask Mimir lab."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence

import duckdb


ROOT = Path(__file__).resolve().parent
DEFAULT_CONTEXT_DIR = ROOT / "validation-output" / "company-context"
DEFAULT_DATA_ROOT = Path(
    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data"
)
DYNAMIC_CONTEXT_SCHEMA_VERSION = "company-context-v9"

CANONICAL_CONSOLIDATED_PARENT_NAMES = {
    "CURTISS WRIGHT": "CURTISS-WRIGHT CORPORATION",
    "TRANSDIGM": "TRANSDIGM GROUP INCORPORATED",
}


# Reviewed trading-name aliases prevent common surnames from sweeping unrelated
# legal entities into an automatically generated company-wide scope.
REVIEWED_COMPANY_ALIASES = {
    "ONTIC": (
        "ONTIC ENGINEERING",
        "ONTIC ENGINEERING MANUFACTURING",
        "ONTIC ENGINEERING AND MANUFACTURING",
        "ONTIC ENGINEERING MFG",
    ),
    "WOODWARD": ("WOODWARD", "WOODWARD HRT", "WOODWARD FST"),
    "COLLINS AEROSPACE": (
        "COLLINS AEROSPACE",
        "ROCKWELL COLLINS",
        "COLLINS ELBIT VISION SYSTEMS",
    ),
    "ROCKWELL COLLINS": (
        "COLLINS AEROSPACE",
        "ROCKWELL COLLINS",
        "COLLINS ELBIT VISION SYSTEMS",
    ),
    "EATON": (
        "EATON",
        "EATON AEROSPACE",
        "EATON AEROQUIP",
        "EATON CORPORATION",
        "EATON FILTRATION",
        "EATON INDUSTRIES",
        "EATON LIMITED",
    ),
    "EATON AEROSPACE": (
        "EATON",
        "EATON AEROSPACE",
        "EATON AEROQUIP",
        "EATON CORPORATION",
        "EATON FILTRATION",
        "EATON INDUSTRIES",
        "EATON LIMITED",
    ),
}


FOCUS_SECTIONS = {
    "article_implications": [
        "identity",
        "observed_financials",
        "annual_activity",
        "capability_evidence",
        "reported_subcontract_relationships",
        "top_awards",
        "evidence_index",
    ],
    "profile": [
        "identity",
        "observed_financials",
        "annual_activity",
        "site_financials",
        "site_capability_evidence",
        "location_footprint",
        "place_of_performance_activity",
        "capability_evidence",
        "product_and_part_evidence",
        "platform_exposure",
        "missile_program_trajectory",
        "future_demand_context",
        "customer_context",
        "top_awards",
        "evidence_index",
        "quality",
    ],
    "full_dossier": [
        "identity",
        "observed_financials",
        "annual_activity",
        "site_financials",
        "site_capability_evidence",
        "location_footprint",
        "place_of_performance_activity",
        "capability_evidence",
        "product_and_part_evidence",
        "platform_exposure",
        "missile_program_trajectory",
        "future_demand_context",
        "customer_context",
        "reported_subcontract_relationships",
        "top_awards",
        "open_solicitation_candidates",
        "evidence_index",
        "quality",
    ],
    "supply_chain": [
        "identity",
        "annual_activity",
        "site_capability_evidence",
        "place_of_performance_activity",
        "capability_evidence",
        "platform_exposure",
        "missile_program_trajectory",
        "reported_subcontract_relationships",
        "quality",
    ],
    "opportunity_discovery": [
        "identity",
        "observed_financials",
        "annual_activity",
        "site_capability_evidence",
        "place_of_performance_activity",
        "capability_evidence",
        "platform_exposure",
        "missile_program_trajectory",
        "customer_context",
        "reported_subcontract_relationships",
        "top_awards",
        "open_solicitation_candidates",
        "quality",
    ],
}


def _normalize_cage(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def _legal_name_key(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def _name_pattern(value: Any) -> str:
    tokens = re.findall(r"[A-Z0-9]+", str(value or "").upper())
    if not tokens:
        return ""
    phrase = r"[^A-Z0-9]+".join(re.escape(token) for token in tokens)
    return rf"(^|[^A-Z0-9]){phrase}([^A-Z0-9]|$)"


def _name_patterns(value: Any) -> str:
    """Match a company name even when a city or legal suffix follows it."""
    tokens = re.findall(r"[A-Z0-9]+", str(value or "").upper())
    while tokens and tokens[0] in {"THE", "A", "AN"}:
        tokens.pop(0)
    if not tokens:
        return ""
    generic_single_tokens = {
        "ADVANCED", "AMERICAN", "COMPANY", "CORPORATION", "DATA", "DEFENSE",
        "GENERAL", "GLOBAL", "GROUP", "INTERNATIONAL", "SYSTEMS", "TECHNOLOGIES",
    }
    variants = []
    for end in range(len(tokens), 0, -1):
        candidate = tokens[:end]
        if len(candidate) == 1 and candidate[0] in generic_single_tokens:
            continue
        variants.append(_name_pattern(" ".join(candidate)))
    return "|".join(pattern for pattern in variants if pattern)


def _group_id(label: str, cages: List[str]) -> str:
    digest = hashlib.sha256("|".join([label.upper(), *sorted(cages)]).encode()).hexdigest()
    return f"OBSERVED_{digest[:20].upper()}"


def _reported_parent_id(parent_name: str) -> str:
    digest = hashlib.sha256(parent_name.strip().upper().encode()).hexdigest()
    return f"PARENT_{digest[:20].upper()}"


def _parent_display_name(parent_name: Any) -> str:
    clean = re.sub(r"\s+", " ", str(parent_name or "").strip()).upper()
    return {
        "THE BOEING": "THE BOEING COMPANY",
    }.get(clean, clean)


def _company_name_core(value: Any) -> str:
    tokens = re.findall(r"[A-Z0-9]+", str(value or "").upper())
    while tokens and tokens[0] == "THE":
        tokens.pop(0)
    legal_suffixes = {
        "CO",
        "COMPANY",
        "CORP",
        "CORPORATION",
        "INC",
        "INCORPORATED",
        "LLC",
        "LP",
        "LTD",
        "PLC",
    }
    while tokens and tokens[-1] in legal_suffixes:
        tokens.pop()
    return " ".join(tokens)


def _reviewed_company_aliases(value: Any) -> tuple[str, ...]:
    return REVIEWED_COMPANY_ALIASES.get(_company_name_core(value), ())


def _matches_reviewed_company_alias(value: Any, aliases: Sequence[str]) -> bool:
    core = _company_name_core(value)
    return any(
        core == alias
        or (len(alias.split()) > 1 and core.startswith(f"{alias} "))
        for alias in aliases
    )


def _bounded_precomputed_context(
    context: Dict[str, Any], row_limit: int = 5000
) -> Dict[str, Any]:
    """Bound persisted evidence rows without changing aggregate observations."""
    limit = min(max(int(row_limit), 1), 5000)
    bounded = dict(context)

    identity = dict(bounded.get("identity", {}))
    identity["sites"] = list(identity.get("sites", []))[:limit]
    bounded["identity"] = identity
    bounded["site_financials"] = list(bounded.get("site_financials", []))[:limit]
    bounded["site_capability_evidence"] = list(
        bounded.get("site_capability_evidence", [])
    )[:limit]

    location = dict(bounded.get("location_footprint", {}))
    for key in (
        "registered_or_contracting_sites",
        "prime_award_places_of_performance",
        "reported_subaward_locations",
    ):
        location[key] = list(location.get(key, []))[:limit]
    bounded["location_footprint"] = location

    performance = dict(bounded.get("place_of_performance_activity", {}))
    performance["records"] = list(performance.get("records", []))[:limit]
    bounded["place_of_performance_activity"] = performance

    capability = dict(bounded.get("capability_evidence", {}))
    for key in (
        "psc",
        "naics",
        "dla_items",
        "prime_award_descriptions",
        "reported_subaward_descriptions",
    ):
        capability[key] = list(capability.get(key, []))[:limit]
    bounded["capability_evidence"] = capability

    product = dict(bounded.get("product_and_part_evidence", {}))
    financial_rows = []
    for row in list(product.get("niin_financial_observations", []))[:limit]:
        financial_rows.append(
            {**row, "contract_ids": list(row.get("contract_ids", []))[:20]}
        )
    product["niin_financial_observations"] = financial_rows
    product["part_number_references"] = list(
        product.get("part_number_references", [])
    )[:limit]
    qualified = dict(product.get("qualified_source_context", {}))
    qualified["items"] = list(qualified.get("items", []))[:limit]
    product["qualified_source_context"] = qualified
    bounded["product_and_part_evidence"] = product

    relationships = dict(bounded.get("reported_subcontract_relationships", {}))
    relationships["as_subcontractor_to"] = list(
        relationships.get("as_subcontractor_to", [])
    )[:limit]
    relationships["reported_subcontractors"] = list(
        relationships.get("reported_subcontractors", [])
    )[:limit]
    bounded["reported_subcontract_relationships"] = relationships

    bounded["platform_exposure"] = list(bounded.get("platform_exposure", []))[:limit]
    bounded["customer_context"] = list(bounded.get("customer_context", []))[:limit]
    bounded["top_awards"] = list(bounded.get("top_awards", []))[:limit]
    solicitations = dict(bounded.get("open_solicitation_candidates", {}))
    solicitations["candidates"] = list(solicitations.get("candidates", []))[:limit]
    bounded["open_solicitation_candidates"] = solicitations
    evidence_index = dict(bounded.get("evidence_index", {}))
    evidence_index["records"] = list(evidence_index.get("records", []))[:limit]
    bounded["evidence_index"] = evidence_index
    bounded["precomputed_row_limit_per_table"] = limit
    return bounded


class CompanyContextStore:
    def __init__(
        self,
        context_dir: Path = DEFAULT_CONTEXT_DIR,
        data_root: Path | None = None,
    ) -> None:
        self.context_dir = context_dir.resolve()
        manifest_path = self.context_dir / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(f"company context manifest was not found: {manifest_path}")
        self.manifest = json.loads(manifest_path.read_text())
        self.contexts: List[Dict[str, Any]] = []
        self._context_paths: Dict[tuple[str, str], Path] = {}
        lazy_search_contexts: List[Dict[str, Any]] = []
        for entry in self.manifest.get("contexts", []):
            path = self.context_dir / entry["path"]
            scope = entry.get("scope", {})
            key = (
                str(scope.get("scope_type") or ""),
                str(scope.get("scope_id") or "").upper(),
            )
            if entry.get("lazy") and all(key):
                self._context_paths[key] = path
                lazy_search_contexts.append(
                    {
                        "context_id": entry.get("context_id"),
                        "scope": scope,
                        "identity": entry.get("search_identity", {}),
                        "_artifact_path": str(path),
                    }
                )
                continue
            context = json.loads(path.read_text())
            context["_artifact_path"] = str(path)
            self.contexts.append(context)
        self._search_contexts = [*self.contexts, *lazy_search_contexts]
        configured_data_root = data_root or Path(
            os.getenv("ASK_MIMIR_DATA_ROOT", str(DEFAULT_DATA_ROOT))
        )
        self.data_root = configured_data_root.resolve()
        self.directory_paths = {
            "profiles": self.data_root / "profiles.parquet",
            "locations": self.data_root / "cage_locations.parquet",
        }
        self.directory_sources = [
            path for path in self.directory_paths.values() if path.exists()
        ]
        self._directory_lock = threading.Lock()
        self._parent_bridge_lock = threading.Lock()
        self._parent_bridge: Dict[str, tuple[str, str | None]] | None = None
        self._dynamic_lock = threading.Lock()
        self._dynamic_builder = None
        self._dynamic_contexts: Dict[tuple[str, str], Dict[str, Any]] = {}
        self._dynamic_groups: Dict[str, Dict[str, Any]] = {}
        release_identity = os.getenv("ASK_MIMIR_RELEASE_ID", "").strip()
        if not release_identity:
            release_identity = os.getenv("ASK_MIMIR_PINNED_MANIFEST_KEY", "").strip()
        if not release_identity:
            release_identity = os.getenv("ASK_MIMIR_MANIFEST_KEY", "").strip()
        if not release_identity:
            release_identity = "|".join(
                f"{path.name}:{path.stat().st_size}:{path.stat().st_mtime_ns}"
                for path in self.directory_sources
            )
        release_namespace = hashlib.sha256(
            f"{DYNAMIC_CONTEXT_SCHEMA_VERSION}|{release_identity}".encode()
        ).hexdigest()[:16]
        cache_root = Path(
            os.getenv("ASK_MIMIR_CACHE_DIR", str(self.context_dir / ".dynamic-cache"))
        ).resolve()
        self.dynamic_cache_dir = cache_root / "company-context" / release_namespace

    def _exact_name_parent_bridge(self) -> Dict[str, tuple[str, str | None]]:
        """Resolve blank parents only from unambiguous exact legal-name matches."""
        if self._parent_bridge is not None:
            return self._parent_bridge
        profiles_path = self.directory_paths["profiles"]
        if not profiles_path.exists():
            self._parent_bridge = {}
            return self._parent_bridge
        with self._parent_bridge_lock:
            if self._parent_bridge is not None:
                return self._parent_bridge
            with duckdb.connect() as connection:
                columns = {
                    column[0]
                    for column in connection.execute(
                        "SELECT * FROM read_parquet(?) LIMIT 0",
                        [str(profiles_path)],
                    ).description
                }
                if not {"ultimate_parent_name", "ultimate_parent_uei"}.issubset(columns):
                    self._parent_bridge = {}
                    return self._parent_bridge
                bridge_rows = connection.execute(
                    """
                    WITH normalized AS (
                        SELECT
                            UPPER(REGEXP_REPLACE(TRIM(COALESCE(vendor_name, '')), '[^A-Za-z0-9]', '', 'g')) AS legal_name_key,
                            NULLIF(TRIM(ultimate_parent_name), '') AS parent_name,
                            NULLIF(TRIM(ultimate_parent_uei), '') AS parent_uei
                        FROM read_parquet(?)
                        WHERE NULLIF(TRIM(vendor_name), '') IS NOT NULL
                    )
                    SELECT
                        legal_name_key,
                        MIN(parent_name) AS parent_name,
                        MODE(parent_uei) AS parent_uei
                    FROM normalized
                    WHERE parent_name IS NOT NULL
                    GROUP BY legal_name_key
                    HAVING COUNT(DISTINCT UPPER(parent_name)) = 1
                    """,
                    [str(profiles_path)],
                ).fetchall()
            self._parent_bridge = {
                str(name_key): (str(parent_name), parent_uei)
                for name_key, parent_name, parent_uei in bridge_rows
                if name_key and parent_name
            }
        return self._parent_bridge

    def search(
        self, query: str, scope_type: str | None = None, limit: int = 10
    ) -> Dict[str, Any]:
        clean_query = str(query or "").strip().upper()
        if not clean_query:
            return {"matches": []}
        reviewed_aliases = _reviewed_company_aliases(query)
        matches = []
        for context in self._search_contexts:
            scope = context["scope"]
            if scope_type and scope["scope_type"] != scope_type:
                continue
            searchable = [scope["scope_id"], scope["scope_name"]]
            searchable.extend(
                context.get("identity", {})
                .get("parent_resolution", {})
                .get("aliases", [])
            )
            for site in context.get("identity", {}).get("sites", []):
                searchable.extend(
                    [
                        site.get("cage"),
                        site.get("vendor_name"),
                        site.get("official_site_label"),
                        site.get("city"),
                        site.get("state"),
                    ]
                )
            if not any(clean_query in str(value or "").upper() for value in searchable):
                continue
            sites = context.get("identity", {}).get("sites", [])
            representative_site = sites[0] if sites else {}
            if scope["scope_type"] == "company_site":
                location = ", ".join(
                    value
                    for value in (
                        representative_site.get("city"),
                        representative_site.get("state"),
                    )
                    if value
                )
                option_label = scope["scope_name"]
                if location:
                    option_label += f" - {location}"
                option_label += f" (CAGE {scope['scope_id']})"
            else:
                site_count = context.get("identity", {}).get("site_count", 0)
                option_label = f"{scope['scope_name']} - parent view ({site_count} sites)"
            matches.append(
                {
                    "context_id": context["context_id"],
                    "scope_type": scope["scope_type"],
                    "scope_id": scope["scope_id"],
                    "scope_name": scope["scope_name"],
                    "observation_window": scope["observation_window"],
                    "site_count": context.get("identity", {}).get("site_count", 0),
                    "resolved_cages": context.get("identity", {}).get("resolved_cages", []),
                    "city": representative_site.get("city"),
                    "state": representative_site.get("state"),
                    "option_label": option_label,
                    "context_available": True,
                }
            )
        if reviewed_aliases:
            matches = [
                row
                for row in matches
                if row.get("scope_type") == "company_site"
                and _matches_reviewed_company_alias(
                    row.get("scope_name"), reviewed_aliases
                )
            ]
        if not scope_type or scope_type in {"company_site", "company_parent"}:
            if reviewed_aliases:
                directory_matches_by_cage: Dict[str, Dict[str, Any]] = {}
                for alias in reviewed_aliases:
                    for row in self._directory_search(alias, max(limit, 500)):
                        if _matches_reviewed_company_alias(
                            row.get("scope_name"), reviewed_aliases
                        ):
                            directory_matches_by_cage[str(row["scope_id"])] = row
                directory_matches = list(directory_matches_by_cage.values())
            else:
                directory_matches = self._directory_search(query, max(limit, 500))
            matches = self._merge_directory_matches(
                matches,
                directory_matches if scope_type != "company_parent" else [],
            )
            if reviewed_aliases and scope_type != "company_site":
                group = self._reviewed_group_match(
                    query, directory_matches, reviewed_aliases
                )
                if group:
                    matches = [
                        row for row in matches if row["scope_type"] != "company_parent"
                    ]
                    matches = self._merge_directory_matches(matches, [group])
            reported_parent_matches: List[Dict[str, Any]] = []
            if not reviewed_aliases and scope_type != "company_site":
                reported_parent_matches = self._reported_parent_matches(query)
                if not any(row["scope_type"] == "company_parent" for row in matches):
                    matches = self._merge_directory_matches(
                        matches,
                        reported_parent_matches,
                    )
                consolidated_parent = self._consolidated_reported_parent_match(
                    query,
                    reported_parent_matches,
                    directory_matches,
                )
                if consolidated_parent:
                    matches = [
                        row for row in matches if row["scope_type"] != "company_parent"
                    ]
                    matches = self._merge_directory_matches(
                        matches, [consolidated_parent]
                    )
            if scope_type != "company_site" and not any(
                row["scope_type"] == "company_parent" for row in matches
            ):
                group = self._observed_group_match(query, directory_matches)
                if group:
                    matches = self._merge_directory_matches(matches, [group])
            if scope_type != "company_site":
                parent_candidates = sorted(
                    (
                        row
                        for row in matches
                        if row.get("scope_type") == "company_parent"
                        and row.get("resolved_cages")
                    ),
                    key=lambda row: int(row.get("site_count") or 0),
                    reverse=True,
                )
                if parent_candidates:
                    location_group = self.resolve_site_reference(
                        parent_candidates[0]["resolved_cages"],
                        query,
                        parent_name=str(parent_candidates[0].get("scope_name") or query),
                    )
                    if (
                        location_group
                        and location_group.get("group_kind") == "co_located_facility"
                    ):
                        matches = self._merge_directory_matches(
                            matches, [location_group]
                        )
        matches.sort(
            key=lambda row: (
                not row.get("context_available", False),
                row["scope_type"] != "company_site",
                row.get("_directory_rank", 999),
                row["scope_name"],
            )
        )
        total_site_matches = len(
            {row["scope_id"] for row in matches if row["scope_type"] == "company_site"}
        )
        selected = matches[: max(1, min(int(limit), 100))]
        if not any(row["scope_type"] == "company_parent" for row in selected):
            parent_match = next(
                (
                    row
                    for row in matches
                    if row["scope_type"] == "company_parent"
                    and row.get("group_kind") == "co_located_facility"
                ),
                None,
            ) or next(
                (row for row in matches if row["scope_type"] == "company_parent"), None
            )
            if parent_match:
                if len(selected) >= max(1, min(int(limit), 100)):
                    selected[-1] = parent_match
                else:
                    selected.append(parent_match)
        for row in selected:
            row.pop("_directory_rank", None)
            row.pop("context_available", None)
            row.pop("has_observed_profile", None)
        distinct_site_ids = {
            row["scope_id"] for row in selected if row["scope_type"] == "company_site"
        }
        requires_disambiguation = len(distinct_site_ids) > 1
        site_options = [
            row["option_label"]
            for row in selected
            if row["scope_type"] == "company_site"
        ]
        multi_site_parent_options = [
            row["option_label"]
            for row in selected
            if row["scope_type"] == "company_parent" and row["site_count"] > 1
        ]
        return {
            "query": query,
            "matches": selected,
            "total_site_matches": total_site_matches,
            "options_shown": min(len(site_options), 6),
            "has_more_site_matches": total_site_matches > 6,
            "requires_disambiguation": requires_disambiguation,
            "disambiguation_options": (
                (
                    site_options[:5] + multi_site_parent_options[:1]
                    if multi_site_parent_options
                    else site_options[:6]
                )
                if requires_disambiguation
                else []
            ),
        }

    def _consolidated_reported_parent_match(
        self,
        query: str,
        parent_matches: List[Dict[str, Any]],
        directory_matches: List[Dict[str, Any]],
    ) -> Dict[str, Any] | None:
        """Combine equivalent reported parent-name variants into one company scope."""
        if len(parent_matches) < 2:
            return None
        query_core = _company_name_core(query)
        compatible = [
            row
            for row in parent_matches
            if (
                _company_name_core(row.get("scope_name")) == query_core
                or _company_name_core(row.get("scope_name", "")).startswith(
                    f"{query_core} "
                )
            )
        ]
        if len(compatible) < 2:
            return None
        cages = {
            _normalize_cage(cage)
            for row in compatible
            for cage in row.get("resolved_cages", [])
            if _normalize_cage(cage)
        }
        for row in directory_matches:
            name_core = _company_name_core(row.get("scope_name"))
            if name_core == query_core or name_core.startswith(f"{query_core} "):
                cage = _normalize_cage(row.get("scope_id"))
                if cage:
                    cages.add(cage)
        if len(cages) < 2:
            return None
        preferred = max(compatible, key=lambda row: int(row.get("site_count") or 0))
        scope_name = CANONICAL_CONSOLIDATED_PARENT_NAMES.get(
            query_core,
            str(preferred.get("scope_name") or query).strip(),
        )
        scope_id = _reported_parent_id(scope_name)
        cage_list = sorted(cages)
        site_by_cage = {
            _normalize_cage(row.get("scope_id")): dict(row)
            for row in directory_matches
            if _normalize_cage(row.get("scope_id")) in cages
        }
        self._dynamic_groups[scope_id] = {
            "scope_name": scope_name,
            "cages": cage_list,
            "identity_sites": list(site_by_cage.values()),
            "group_kind": "reported_ultimate_parent",
            "parent_name": scope_name,
        }
        return {
            "context_id": None,
            "scope_type": "company_parent",
            "scope_id": scope_id,
            "scope_name": scope_name,
            "observation_window": None,
            "site_count": len(cage_list),
            "resolved_cages": cage_list,
            "city": None,
            "state": None,
            "option_label": f"{scope_name} - company-wide ({len(cage_list)} CAGE sites)",
            "context_available": False,
            "group_kind": "reported_ultimate_parent",
        }

    def resolve_site_reference(
        self,
        cages: Sequence[str],
        text: str,
        *,
        parent_name: str | None = None,
    ) -> Dict[str, Any] | None:
        """Resolve a location phrase only within the active company scope."""
        locations_path = self.directory_paths["locations"]
        clean_cages = sorted(
            {_normalize_cage(cage) for cage in cages if _normalize_cage(cage)}
        )
        if not locations_path.exists() or not clean_cages:
            return None

        placeholders = ",".join("?" for _ in clean_cages)
        with self._directory_lock, duckdb.connect() as connection:
            rows = connection.execute(
                f"""
                SELECT
                    UPPER(REGEXP_REPLACE(COALESCE(cage_code, ''), '[^A-Za-z0-9]', '', 'g')) AS cage,
                    MODE(vendor_name) AS vendor_name,
                    MODE(city) AS city,
                    MODE(state) AS state
                FROM read_parquet(?)
                WHERE UPPER(REGEXP_REPLACE(COALESCE(cage_code, ''), '[^A-Za-z0-9]', '', 'g'))
                      IN ({placeholders})
                GROUP BY 1
                """,
                [str(locations_path), *clean_cages],
            ).fetchall()

        question = str(text or "").upper()
        matches = []
        for cage, vendor_name, city, state in rows:
            city_text = str(city or "").strip().upper()
            if len(city_text) < 3 or not re.search(
                rf"\b{re.escape(city_text)}\b", question
            ):
                continue
            matches.append(
                {
                    "context_id": None,
                    "scope_type": "company_site",
                    "scope_id": cage,
                    "scope_name": str(vendor_name or f"CAGE {cage}").strip(),
                    "observation_window": None,
                    "site_count": 1,
                    "resolved_cages": [cage],
                    "city": city,
                    "state": state,
                    "option_label": (
                        f"{vendor_name or cage} - "
                        f"{', '.join(value for value in (city, state) if value)} "
                        f"(CAGE {cage})"
                    ),
                }
            )

        if len(matches) == 1:
            return matches[0]
        if len(matches) < 2:
            return None

        matched_cages = sorted({row["scope_id"] for row in matches})
        city = matches[0].get("city")
        state = matches[0].get("state")
        location = ", ".join(value for value in (city, state) if value)
        scope_name = f"{parent_name or matches[0]['scope_name']} - {location}"
        scope_id = _group_id(scope_name, matched_cages)
        self._dynamic_groups[scope_id] = {
            "scope_name": scope_name,
            "cages": matched_cages,
            "identity_sites": matches,
            "group_kind": "co_located_facility",
        }
        return {
            "context_id": None,
            "scope_type": "company_parent",
            "scope_id": scope_id,
            "scope_name": scope_name,
            "observation_window": None,
            "site_count": len(matched_cages),
            "resolved_cages": matched_cages,
            "city": city,
            "state": state,
            "option_label": f"{scope_name} facility ({len(matched_cages)} CAGE codes)",
            "group_kind": "co_located_facility",
        }

    def _directory_search(self, query: str, limit: int) -> List[Dict[str, Any]]:
        paths = self.directory_paths
        if not paths["profiles"].exists() and not paths["locations"].exists():
            return []
        clean_query = str(query or "").strip()
        cage_query = _normalize_cage(clean_query)
        cage_is_exact = bool(re.fullmatch(r"[A-Z0-9]{5}", cage_query))
        name_pattern = _name_patterns(clean_query)
        full_name_pattern = _name_pattern(clean_query)
        if not name_pattern and not cage_is_exact:
            return []

        parent_bridge = self._exact_name_parent_bridge()
        rows: Dict[str, Dict[str, Any]] = {}
        with self._directory_lock, duckdb.connect() as connection:
            connection.execute("SET preserve_insertion_order=false")
            connection.execute("SET threads=1")
            if paths["profiles"].exists():
                profile_columns = {
                    column[0]
                    for column in connection.execute(
                        "SELECT * FROM read_parquet(?) LIMIT 0",
                        [str(paths["profiles"])],
                    ).description
                }
                parent_name_select = (
                    "ultimate_parent_name"
                    if "ultimate_parent_name" in profile_columns
                    else "CAST(NULL AS VARCHAR)"
                )
                parent_uei_select = (
                    "ultimate_parent_uei"
                    if "ultimate_parent_uei" in profile_columns
                    else "CAST(NULL AS VARCHAR)"
                )
                parent_name_filter = (
                    "OR REGEXP_MATCHES(UPPER(COALESCE(ultimate_parent_name, '')), ?)"
                    if "ultimate_parent_name" in profile_columns
                    else ""
                )
                profile_rows = connection.execute(
                    f"""
                    SELECT
                        UPPER(REGEXP_REPLACE(COALESCE(cage_code, ''), '[^A-Za-z0-9]', '', 'g')) AS cage,
                        vendor_name,
                        COALESCE(total_lifetime_spend, 0) AS prime_value,
                        COALESCE(network_flow_total, 0) AS subcontract_value,
                        {parent_name_select} AS ultimate_parent_name,
                        {parent_uei_select} AS ultimate_parent_uei
                    FROM read_parquet(?)
                    WHERE
                        (? AND UPPER(REGEXP_REPLACE(COALESCE(cage_code, ''), '[^A-Za-z0-9]', '', 'g')) = ?)
                        OR REGEXP_MATCHES(UPPER(COALESCE(vendor_name, '')), ?)
                        {parent_name_filter}
                    ORDER BY ABS(COALESCE(total_lifetime_spend, 0))
                           + ABS(COALESCE(network_flow_total, 0)) DESC
                    LIMIT ?
                    """,
                    [
                        str(paths["profiles"]),
                        cage_is_exact,
                        cage_query,
                        name_pattern or r"a^",
                        *([name_pattern or r"a^"] if parent_name_filter else []),
                        min(max(int(limit), 1), 500),
                    ],
                ).fetchall()
                for (
                    cage,
                    vendor_name,
                    prime_value,
                    subcontract_value,
                    ultimate_parent_name,
                    ultimate_parent_uei,
                ) in profile_rows:
                    if not cage or cage in {"UNKNOWN", "UNKNO", "00000"}:
                        continue
                    inferred_parent = parent_bridge.get(_legal_name_key(vendor_name))
                    if not str(ultimate_parent_name or "").strip() and inferred_parent:
                        ultimate_parent_name, ultimate_parent_uei = inferred_parent
                    rows[cage] = {
                        "cage": cage,
                        "vendor_name": vendor_name,
                        "has_observed_profile": True,
                        "observed_value": abs(float(prime_value or 0))
                        + abs(float(subcontract_value or 0)),
                        "ultimate_parent_name": ultimate_parent_name,
                        "ultimate_parent_uei": ultimate_parent_uei,
                    }

            if paths["locations"].exists():
                location_rows = connection.execute(
                    """
                    SELECT
                        UPPER(REGEXP_REPLACE(COALESCE(cage_code, ''), '[^A-Za-z0-9]', '', 'g')) AS cage,
                        vendor_name,
                        city,
                        state,
                        cage_status,
                        UPPER(REGEXP_REPLACE(COALESCE(CAST(replacement_cage AS VARCHAR), ''), '[^A-Za-z0-9]', '', 'g')) AS replacement_cage
                    FROM read_parquet(?)
                    WHERE
                        (? AND UPPER(REGEXP_REPLACE(COALESCE(cage_code, ''), '[^A-Za-z0-9]', '', 'g')) = ?)
                        OR REGEXP_MATCHES(UPPER(COALESCE(vendor_name, '')), ?)
                    LIMIT ?
                    """,
                    [
                        str(paths["locations"]),
                        cage_is_exact,
                        cage_query,
                        name_pattern or r"a^",
                        min(max(int(limit) * 2, 1), 1000),
                    ],
                ).fetchall()
                for cage, vendor_name, city, state, cage_status, replacement_cage in location_rows:
                    if not cage or cage in {"UNKNOWN", "UNKNO", "00000"}:
                        continue
                    row = rows.setdefault(
                        cage,
                        {
                            "cage": cage,
                            "vendor_name": vendor_name,
                            "has_observed_profile": False,
                            "observed_value": 0.0,
                        },
                    )
                    row["vendor_name"] = row.get("vendor_name") or vendor_name
                    row["city"] = city
                    row["state"] = state
                    row["cage_status"] = cage_status
                    row["replacement_cage"] = replacement_cage or None

        # A retired CAGE with a known replacement should not be a default user choice.
        candidates = [
            row
            for row in rows.values()
            if not (
                str(row.get("cage_status") or "").upper() == "R"
                and row.get("replacement_cage")
            )
        ]
        if not cage_is_exact:
            query_name_tokens = re.findall(r"[A-Z0-9]+", clean_query.upper())
            full_company_matches = [
                row
                for row in candidates
                if full_name_pattern
                and any(
                    re.search(full_name_pattern, str(value or "").upper())
                    for value in (
                        row.get("vendor_name"),
                        row.get("ultimate_parent_name"),
                    )
                )
            ]
            if len(query_name_tokens) >= 2 and full_company_matches:
                candidates = full_company_matches
            exact_company_name_match = bool(full_name_pattern) and any(
                re.search(
                    full_name_pattern,
                    str(row.get("vendor_name") or "").upper(),
                )
                for row in candidates
            )
            if not exact_company_name_match:
                location_candidates = [
                    row
                    for row in candidates
                    if any(
                        len(str(value or "").strip()) >= 4
                        and re.search(
                            rf"\b{re.escape(str(value).strip().upper())}\b",
                            clean_query.upper(),
                        )
                        for value in (row.get("city"), row.get("state"))
                    )
                ]
                if location_candidates:
                    candidates = location_candidates
        candidates.sort(
            key=lambda row: (
                not row.get("has_observed_profile", False),
                -float(row.get("observed_value") or 0),
                str(row.get("vendor_name") or ""),
                row["cage"],
            )
        )
        results = []
        for directory_rank, row in enumerate(
            candidates[: min(max(int(limit), 1), 500)]
        ):
            name = str(row.get("vendor_name") or f"CAGE {row['cage']}").strip()
            location = ", ".join(
                str(value).strip()
                for value in (row.get("city"), row.get("state"))
                if str(value or "").strip()
            )
            option_label = name
            if location:
                option_label += f" - {location}"
            option_label += f" (CAGE {row['cage']})"
            results.append(
                {
                    "context_id": None,
                    "scope_type": "company_site",
                    "scope_id": row["cage"],
                    "scope_name": name,
                    "observation_window": None,
                    "site_count": 1,
                    "resolved_cages": [row["cage"]],
                    "city": row.get("city"),
                    "state": row.get("state"),
                    "option_label": option_label,
                    "context_available": False,
                    "has_observed_profile": row.get("has_observed_profile", False),
                    "ultimate_parent_name": row.get("ultimate_parent_name"),
                    "ultimate_parent_uei": row.get("ultimate_parent_uei"),
                    "_directory_rank": directory_rank,
                }
            )
        return results

    def _reviewed_group_match(
        self,
        query: str,
        directory_matches: List[Dict[str, Any]],
        aliases: Sequence[str],
    ) -> Dict[str, Any] | None:
        """Build a reviewed company scope that also retains reference-only CAGE sites."""
        sites = [
            row
            for row in directory_matches
            if row["scope_type"] == "company_site"
            and _matches_reviewed_company_alias(row.get("scope_name"), aliases)
        ]
        cages = sorted({str(row["scope_id"]).upper() for row in sites})
        if len(cages) < 2:
            return None
        scope_name = re.sub(r"\s+", " ", str(query or "").strip()).rstrip(".?")
        scope_id = _group_id(scope_name, cages)
        self._dynamic_groups[scope_id] = {
            "scope_name": scope_name,
            "cages": cages,
            "identity_sites": [dict(row) for row in sites],
            "group_kind": "reviewed_company_group",
        }
        return {
            "context_id": None,
            "scope_type": "company_parent",
            "scope_id": scope_id,
            "scope_name": scope_name,
            "observation_window": None,
            "site_count": len(cages),
            "resolved_cages": cages,
            "city": None,
            "state": None,
            "option_label": f"{scope_name} - company-wide ({len(cages)} CAGE sites)",
            "context_available": False,
            "group_kind": "reviewed_company_group",
        }

    def _reported_parent_matches(self, query: str) -> List[Dict[str, Any]]:
        profiles_path = self.directory_paths["profiles"]
        if not profiles_path.exists():
            return []
        clean_query = str(query or "").strip()
        parent_pattern = _name_patterns(clean_query)
        exact_parent_uei = re.sub(r"[^A-Z0-9]", "", clean_query.upper())
        if not parent_pattern and not exact_parent_uei:
            return []

        parent_bridge = self._exact_name_parent_bridge()
        with self._directory_lock, duckdb.connect() as connection:
            connection.execute("SET preserve_insertion_order=false")
            connection.execute("SET threads=1")
            profile_columns = {
                column[0]
                for column in connection.execute(
                    "SELECT * FROM read_parquet(?) LIMIT 0",
                    [str(profiles_path)],
                ).description
            }
            required = {"ultimate_parent_name", "ultimate_parent_uei"}
            if not required.issubset(profile_columns):
                return []

            parent_rows = connection.execute(
                """
                WITH matched_parents AS (
                    SELECT
                        ultimate_parent_name,
                        SUM(
                            ABS(COALESCE(total_lifetime_spend, 0))
                            + ABS(COALESCE(network_flow_total, 0))
                        ) AS observed_value
                    FROM read_parquet(?)
                    WHERE NULLIF(TRIM(ultimate_parent_name), '') IS NOT NULL
                      AND (
                          REGEXP_MATCHES(UPPER(ultimate_parent_name), ?)
                          OR UPPER(REGEXP_REPLACE(COALESCE(ultimate_parent_uei, ''), '[^A-Za-z0-9]', '', 'g')) = ?
                      )
                    GROUP BY ultimate_parent_name
                    ORDER BY observed_value DESC, ultimate_parent_name
                    LIMIT 10
                )
                SELECT ultimate_parent_name, observed_value
                FROM matched_parents
                ORDER BY observed_value DESC, ultimate_parent_name
                """,
                [str(profiles_path), parent_pattern or r"a^", exact_parent_uei],
            ).fetchall()

            query_core = _company_name_core(clean_query)
            compatible_name_rows = [
                row
                for row in parent_rows
                if (
                    _company_name_core(_parent_display_name(row[0])) == query_core
                    or _company_name_core(_parent_display_name(row[0])).startswith(
                        f"{query_core} "
                    )
                )
            ]
            if compatible_name_rows:
                parent_rows = compatible_name_rows

            results: List[Dict[str, Any]] = []
            for parent_rank, (parent_name, _observed_value) in enumerate(parent_rows):
                parent_filter = (
                    "UPPER(TRIM(COALESCE(p.ultimate_parent_name, ''))) = UPPER(TRIM(?))"
                )
                parent_value = str(parent_name)

                location_join = ""
                location_select = "CAST(NULL AS VARCHAR) AS city, CAST(NULL AS VARCHAR) AS state"
                parameters: List[Any] = [str(profiles_path)]
                if self.directory_paths["locations"].exists():
                    location_join = """
                        LEFT JOIN read_parquet(?) l
                          ON UPPER(REGEXP_REPLACE(COALESCE(p.cage_code, ''), '[^A-Za-z0-9]', '', 'g'))
                           = UPPER(REGEXP_REPLACE(COALESCE(l.cage_code, ''), '[^A-Za-z0-9]', '', 'g'))
                    """
                    location_select = "l.city, l.state"
                    parameters.append(str(self.directory_paths["locations"]))
                parameters.append(parent_value)
                site_rows = connection.execute(
                    f"""
                    SELECT
                        UPPER(REGEXP_REPLACE(COALESCE(p.cage_code, ''), '[^A-Za-z0-9]', '', 'g')) AS cage,
                        p.vendor_name,
                        {location_select},
                        COALESCE(p.total_lifetime_spend, 0) AS prime_value,
                        COALESCE(p.network_flow_total, 0) AS subcontract_value
                    FROM read_parquet(?) p
                    {location_join}
                    WHERE {parent_filter}
                    ORDER BY ABS(COALESCE(p.total_lifetime_spend, 0))
                           + ABS(COALESCE(p.network_flow_total, 0)) DESC,
                             p.vendor_name,
                             p.cage_code
                    """,
                    parameters,
                ).fetchall()

                inferred_name_keys = sorted(
                    name_key
                    for name_key, (inferred_parent_name, _parent_uei) in parent_bridge.items()
                    if _company_name_core(_parent_display_name(inferred_parent_name))
                    == _company_name_core(_parent_display_name(parent_name))
                )
                if inferred_name_keys:
                    placeholders = ",".join("?" for _ in inferred_name_keys)
                    inferred_location_join = ""
                    inferred_location_select = (
                        "CAST(NULL AS VARCHAR) AS city, CAST(NULL AS VARCHAR) AS state"
                    )
                    inferred_parameters: List[Any] = [str(profiles_path)]
                    if self.directory_paths["locations"].exists():
                        inferred_location_join = """
                            LEFT JOIN read_parquet(?) l
                              ON UPPER(REGEXP_REPLACE(COALESCE(p.cage_code, ''), '[^A-Za-z0-9]', '', 'g'))
                               = UPPER(REGEXP_REPLACE(COALESCE(l.cage_code, ''), '[^A-Za-z0-9]', '', 'g'))
                        """
                        inferred_location_select = "l.city, l.state"
                        inferred_parameters.append(str(self.directory_paths["locations"]))
                    inferred_parameters.extend(inferred_name_keys)
                    site_rows.extend(
                        connection.execute(
                            f"""
                            SELECT
                                UPPER(REGEXP_REPLACE(COALESCE(p.cage_code, ''), '[^A-Za-z0-9]', '', 'g')) AS cage,
                                p.vendor_name,
                                {inferred_location_select},
                                COALESCE(p.total_lifetime_spend, 0) AS prime_value,
                                COALESCE(p.network_flow_total, 0) AS subcontract_value
                            FROM read_parquet(?) p
                            {inferred_location_join}
                            WHERE NULLIF(TRIM(p.ultimate_parent_name), '') IS NULL
                              AND UPPER(REGEXP_REPLACE(TRIM(COALESCE(p.vendor_name, '')), '[^A-Za-z0-9]', '', 'g'))
                                  IN ({placeholders})
                            """,
                            inferred_parameters,
                        ).fetchall()
                    )

                sites_by_cage: Dict[str, Dict[str, Any]] = {}
                for cage, vendor_name, city, state, prime_value, subcontract_value in site_rows:
                    if not cage or cage in {"UNKNOWN", "UNKNO", "00000"}:
                        continue
                    sites_by_cage[cage] = {
                        "cage": cage,
                        "scope_id": cage,
                        "scope_type": "company_site",
                        "scope_name": vendor_name or f"CAGE {cage}",
                        "vendor_name": vendor_name,
                        "city": city,
                        "state": state,
                        "has_observed_profile": True,
                        "observed_value": abs(float(prime_value or 0))
                        + abs(float(subcontract_value or 0)),
                        "ultimate_parent_name": parent_name,
                    }
                sites = list(sites_by_cage.values())
                cages = sorted({site["cage"] for site in sites})
                if not cages:
                    continue
                scope_name = _parent_display_name(parent_name)
                scope_id = _reported_parent_id(scope_name)
                self._dynamic_groups[scope_id] = {
                    "scope_name": scope_name,
                    "cages": cages,
                    "identity_sites": sites,
                    "group_kind": "reported_ultimate_parent",
                    "parent_name": scope_name,
                }
                results.append(
                    {
                        "context_id": None,
                        "scope_type": "company_parent",
                        "scope_id": scope_id,
                        "scope_name": scope_name,
                        "observation_window": None,
                        "site_count": len(cages),
                        "resolved_cages": cages,
                        "city": None,
                        "state": None,
                        "option_label": f"{scope_name} - company-wide ({len(cages)} CAGE sites)",
                        "context_available": False,
                        "group_kind": "reported_ultimate_parent",
                        "_directory_rank": parent_rank,
                    }
                )
            return results

    def _observed_group_match(
        self, query: str, directory_matches: List[Dict[str, Any]]
    ) -> Dict[str, Any] | None:
        sites = [row for row in directory_matches if row["scope_type"] == "company_site"]
        if len(sites) < 2:
            return None
        observed_cages = sorted(
            {
                str(row["scope_id"]).upper()
                for row in sites
                if row.get("has_observed_profile")
            }
        )
        if not observed_cages:
            return None
        cage_set = set(observed_cages)
        scoped_sites = [
            row for row in sites if str(row["scope_id"]).upper() in cage_set
        ]
        all_location_keys = {
            (str(row.get("city") or "").upper(), str(row.get("state") or "").upper())
            for row in sites
        }
        all_at_one_location = (
            len(all_location_keys) == 1 and next(iter(all_location_keys))[0]
        )
        if all_at_one_location:
            cages = sorted({str(row["scope_id"]).upper() for row in sites})
            cage_set = set(cages)
            scoped_sites = [
                row for row in sites if str(row["scope_id"]).upper() in cage_set
            ]
        else:
            cages = observed_cages
        location_keys = {
            (str(row.get("city") or "").upper(), str(row.get("state") or "").upper())
            for row in scoped_sites
        }
        one_location = len(location_keys) == 1 and next(iter(location_keys))[0]
        query_label = re.sub(r"\s+", " ", str(query or "").strip()).rstrip(".?")
        if one_location:
            city, state = next(iter(location_keys))
            query_label = re.sub(
                rf"\b{re.escape(city)}\b.*$",
                "",
                query_label,
                flags=re.IGNORECASE,
            ).strip(" ,- ")
            scope_name = f"{query_label} - {city.title()}, {state}"
            option_label = f"{scope_name} facility ({len(cages)} CAGE codes)"
            group_kind = "co_located_facility"
        else:
            scope_name = query_label
            option_label = f"{scope_name} - company-wide ({len(cages)} CAGE sites)"
            group_kind = "observed_company_group"
        scope_id = _group_id(scope_name, cages)
        self._dynamic_groups[scope_id] = {
            "scope_name": scope_name,
            "cages": cages,
            "identity_sites": [dict(row) for row in scoped_sites],
            "group_kind": group_kind,
        }
        return {
            "context_id": None,
            "scope_type": "company_parent",
            "scope_id": scope_id,
            "scope_name": scope_name,
            "observation_window": None,
            "site_count": len(cages),
            "resolved_cages": cages,
            "city": sites[0].get("city") if one_location else None,
            "state": sites[0].get("state") if one_location else None,
            "option_label": option_label,
            "context_available": False,
            "group_kind": group_kind,
        }

    def register_group(
        self,
        *,
        scope_id: str,
        scope_name: str,
        cages: List[str],
        group_kind: str = "observed_company_group",
    ) -> None:
        clean_id = str(scope_id or "").strip().upper()
        clean_cages = sorted({_normalize_cage(cage) for cage in cages if _normalize_cage(cage)})
        if not clean_id or not clean_cages:
            return
        existing = self._dynamic_groups.get(clean_id, {})
        identity_sites = list(existing.get("identity_sites", []))
        if not identity_sites:
            allowed_cages = set(clean_cages)
            identity_sites = [
                row
                for row in self._directory_search(scope_name, 500)
                if _normalize_cage(row.get("scope_id")) in allowed_cages
            ]
        self._dynamic_groups[clean_id] = {
            "scope_name": str(scope_name or clean_id).strip(),
            "cages": clean_cages,
            "identity_sites": identity_sites,
            "group_kind": group_kind,
        }

    @staticmethod
    def _merge_directory_matches(
        context_matches: List[Dict[str, Any]],
        directory_matches: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        merged = {
            (row["scope_type"], str(row["scope_id"]).upper()): row
            for row in directory_matches
        }
        for row in context_matches:
            key = (row["scope_type"], str(row["scope_id"]).upper())
            existing = merged.get(key, {})
            merged[key] = {
                **existing,
                **row,
                "city": row.get("city") or existing.get("city"),
                "state": row.get("state") or existing.get("state"),
                "context_available": True,
            }
        return list(merged.values())

    def get(self, scope_type: str, scope_id: str, focus: str) -> Dict[str, Any]:
        if focus not in FOCUS_SECTIONS:
            raise ValueError(f"unsupported company context focus: {focus}")
        clean_id = str(scope_id).strip().upper()
        context = self.get_raw(scope_type, clean_id)
        if (
            scope_type == "company_site"
            and "place_of_performance_activity" not in context
        ):
            from company_context import CompanyContextBuilder

            with self._dynamic_lock:
                if self._dynamic_builder is None:
                    self._dynamic_builder = CompanyContextBuilder(
                        data_root=self.data_root
                    )
                context["place_of_performance_activity"] = (
                    self._dynamic_builder._place_of_performance_activity(
                        context.get("identity", {}).get("sites", []),
                        context.get("scope", {}).get("fiscal_years", []),
                    )
                )

        result = {
            "context_id": context["context_id"],
            "evidence_fingerprint": context["evidence_fingerprint"],
            "calculation_version": context["calculation_version"],
            "generated_at": context["generated_at"],
            "scope": context["scope"],
            "focus": focus,
        }
        for section in FOCUS_SECTIONS[focus]:
            result[section] = self._compact_section(section, context.get(section))
        result["evidence_chain"] = {
            "identity_definition_version": context.get("source_manifest", {})
            .get("identity_definition", {})
            .get("version"),
            "source_file_hashes": {
                name: details.get("sha256")
                for name, details in context.get("source_manifest", {}).get("files", {}).items()
            },
        }
        return result

    def get_raw(self, scope_type: str, scope_id: str) -> Dict[str, Any]:
        clean_id = str(scope_id).strip().upper()
        context = next(
            (
                row
                for row in self.contexts
                if row["scope"]["scope_type"] == scope_type
                and str(row["scope"]["scope_id"]).upper() == clean_id
            ),
            None,
        )
        if context is not None:
            return context
        key = (scope_type, clean_id)
        if key in self._dynamic_contexts:
            return self._dynamic_contexts[key]
        precomputed_path = self._context_paths.get(key)
        if precomputed_path is not None:
            context = json.loads(precomputed_path.read_text())
            context["_artifact_path"] = str(precomputed_path)
            self._dynamic_contexts[key] = context
            return context
        group = self._dynamic_groups.get(clean_id)
        if scope_type == "company_parent" and group:
            pass
        elif scope_type != "company_site" or not self._directory_search(clean_id, 1):
            raise KeyError(f"company context was not found: {scope_type}/{scope_id}")

        with self._dynamic_lock:
            if key in self._dynamic_contexts:
                return self._dynamic_contexts[key]
            cached_context = self._read_dynamic_cache(scope_type, clean_id)
            if cached_context is not None:
                self._dynamic_contexts[key] = cached_context
                return cached_context
            from company_context import CompanyContextBuilder

            if self._dynamic_builder is None:
                self._dynamic_builder = CompanyContextBuilder(data_root=self.data_root)
            if scope_type == "company_parent" and group:
                context = self._dynamic_builder.build_group(
                    scope_id=clean_id,
                    scope_name=group["scope_name"],
                    cages=group["cages"],
                    group_kind=group["group_kind"],
                    identity_sites=group.get("identity_sites", []),
                )
                if group.get("parent_name"):
                    context["identity"]["parent_resolution"] = {
                        "parent_name": group["parent_name"],
                        "method": "reported ultimate-parent relationship",
                    }
            else:
                context = self._dynamic_builder.build_site(clean_id)
            self._write_dynamic_cache(scope_type, clean_id, context)
            self._dynamic_contexts[key] = context
            return context

    def get_export_context(
        self, scope_type: str, scope_id: str, limit: int = 5000
    ) -> Dict[str, Any]:
        """Return expanded download evidence while preserving compact answer contexts."""
        context = self.get_raw(scope_type, scope_id)
        from company_context import CompanyContextBuilder

        with self._dynamic_lock:
            if self._dynamic_builder is None:
                self._dynamic_builder = CompanyContextBuilder(data_root=self.data_root)
            return self._dynamic_builder.build_export_context(context, limit)

    def _read_dynamic_cache(self, scope_type: str, scope_id: str) -> Dict[str, Any] | None:
        path = self.dynamic_cache_dir / f"{scope_type}-{scope_id}.json"
        if not path.exists():
            return None
        try:
            context = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            return None
        scope = context.get("scope", {})
        if (
            scope.get("scope_type") != scope_type
            or str(scope.get("scope_id", "")).upper() != scope_id
        ):
            return None
        return context

    def _write_dynamic_cache(
        self, scope_type: str, scope_id: str, context: Dict[str, Any]
    ) -> None:
        self.dynamic_cache_dir.mkdir(parents=True, exist_ok=True)
        destination = self.dynamic_cache_dir / f"{scope_type}-{scope_id}.json"
        temporary = destination.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(context, default=str))
        temporary.replace(destination)

    @staticmethod
    def _compact_section(section: str, value: Any) -> Any:
        if section == "identity" and isinstance(value, dict):
            return {**value, "sites": value.get("sites", [])[:500]}
        if section == "site_financials" and isinstance(value, list):
            return value[:15]
        if section == "site_capability_evidence" and isinstance(value, list):
            return value[:40]
        if section == "capability_evidence" and isinstance(value, dict):
            psc_rows = [row for row in value.get("psc", []) if row.get("psc_description")]
            naics_rows = [
                row for row in value.get("naics", []) if row.get("naics_description")
            ]
            return {
                **value,
                "psc": psc_rows[:10],
                "naics": naics_rows[:8],
                "dla_items": value.get("dla_items", [])[:10],
                "prime_award_descriptions": value.get("prime_award_descriptions", [])[:10],
                "reported_subaward_descriptions": value.get(
                    "reported_subaward_descriptions", []
                )[:10],
            }
        if section == "location_footprint" and isinstance(value, dict):
            return {
                **value,
                "registered_or_contracting_sites": value.get(
                    "registered_or_contracting_sites", []
                )[:500],
                "prime_award_places_of_performance": value.get(
                    "prime_award_places_of_performance", []
                )[:8],
                "reported_subaward_locations": value.get(
                    "reported_subaward_locations", []
                )[:8],
            }
        if section == "place_of_performance_activity" and isinstance(value, dict):
            return {**value, "records": value.get("records", [])[:15]}
        if section == "product_and_part_evidence" and isinstance(value, dict):
            financial_rows = []
            for row in value.get("niin_financial_observations", [])[:10]:
                financial_rows.append(
                    {**row, "contract_ids": row.get("contract_ids", [])[:6]}
                )
            qualified = value.get("qualified_source_context", {})
            qualified_items = list(qualified.get("items", []))
            selected_qualified = qualified_items[:12]
            selected_niins = {row.get("niin") for row in selected_qualified}
            financial_niins = {row.get("niin") for row in financial_rows}
            selected_qualified.extend(
                row
                for row in qualified_items
                if row.get("niin") in financial_niins
                and row.get("niin") not in selected_niins
            )
            return {
                **value,
                "niin_financial_observations": financial_rows,
                "part_number_references": value.get("part_number_references", [])[:20],
                "qualified_source_context": {
                    **qualified,
                    "items": selected_qualified,
                },
            }
        if section in {"platform_exposure", "customer_context", "top_awards"}:
            return (value or [])[:8]
        if section == "missile_program_trajectory" and isinstance(value, dict):
            return {**value, "programs": value.get("programs", [])[:8]}
        if section == "future_demand_context" and isinstance(value, dict):
            programs = []
            for program in value.get("programs", [])[:5]:
                budget_lines: Dict[tuple, Dict[str, Any]] = {}
                for row in program.get("budget_projection_rows", []):
                    key = (
                        row.get("component"),
                        row.get("p1_line_number"),
                        row.get("budget_line_item"),
                        row.get("budget_line_item_title"),
                        row.get("is_advance_procurement_exhibit"),
                    )
                    budget_line = budget_lines.setdefault(
                        key,
                        {
                            "component": row.get("component"),
                            "p1_line_number": row.get("p1_line_number"),
                            "budget_line_item": row.get("budget_line_item"),
                            "budget_line_item_title": row.get("budget_line_item_title"),
                            "is_advance_procurement_exhibit": row.get(
                                "is_advance_procurement_exhibit"
                            ),
                            "observations": {},
                            "source": {
                                "source_document_title": row.get(
                                    "source_document_title"
                                ),
                                "source_page_number": row.get("source_page_number"),
                                "source_landing_page": row.get("source_landing_page"),
                                "source_download_url": row.get("source_download_url"),
                                "source_locator": row.get("source_locator"),
                            },
                        },
                    )
                    fiscal_year = str(row.get("fiscal_year"))
                    observation = budget_line["observations"].setdefault(
                        fiscal_year,
                        {
                            "fiscal_year": row.get("fiscal_year"),
                            "funding_status": row.get("funding_status"),
                        },
                    )
                    if row.get("measure_type") == "net_procurement_p1":
                        observation["net_procurement_usd"] = row.get("amount_usd")
                    elif row.get("measure_type") == "procurement_quantity":
                        observation["procurement_quantity"] = row.get("quantity")
                compact_lines = []
                for budget_line in budget_lines.values():
                    budget_line["observations"] = sorted(
                        budget_line["observations"].values(),
                        key=lambda row: row.get("fiscal_year") or 0,
                    )
                    compact_lines.append(budget_line)
                programs.append(
                    {
                        "program_id": program.get("program_id"),
                        "program_name": program.get("program_name"),
                        "observed_site_reported_subcontract_value_usd": program.get(
                            "observed_site_reported_subcontract_value_usd"
                        ),
                        "budget_lines": compact_lines[:8],
                    }
                )
            return {**value, "programs": programs}
        if section == "reported_subcontract_relationships" and isinstance(value, dict):
            return {
                **value,
                "as_subcontractor_to": value.get("as_subcontractor_to", [])[:6],
                "reported_subcontractors": value.get("reported_subcontractors", [])[:6],
            }
        if section == "open_solicitation_candidates" and isinstance(value, dict):
            return {**value, "candidates": value.get("candidates", [])[:5]}
        if section == "evidence_index" and isinstance(value, dict):
            records = value.get("records", [])
            balanced = []
            limits = {
                "official_company_site_source": 3,
                "usaspending_prime_award": 5,
                "dla_procurement_history": 5,
                "usaspending_reported_subaward": 5,
            }
            for evidence_type, limit in limits.items():
                balanced.extend(
                    [
                        row
                        for row in records
                        if row.get("evidence_type") == evidence_type
                    ][:limit]
                )
            return {**value, "records": balanced}
        return value


def _matching_parent_scope_ids(
    candidates: Sequence[Dict[str, Any]],
    selected_scope_id: str,
    entries: Sequence[Dict[str, Any]] = (),
    selected_scope_name: str = "",
) -> set[str]:
    scope_ids = {
        str(candidate.get("scope_id") or "").upper()
        for candidate in candidates
        if candidate.get("scope_id")
    }
    scope_ids.add(str(selected_scope_id).upper())
    selected_name_core = _company_name_core(selected_scope_name)
    if selected_name_core:
        scope_ids.update(
            str(entry.get("scope", {}).get("scope_id") or "").upper()
            for entry in entries
            if entry.get("scope", {}).get("scope_type") == "company_parent"
            and _company_name_core(entry.get("scope", {}).get("scope_name"))
            == selected_name_core
            and entry.get("scope", {}).get("scope_id")
        )
    return scope_ids


def build_precomputed_parent_contexts(
    data_root: Path,
    source_context_dir: Path,
    output_dir: Path,
    parent_queries: Sequence[str],
    *,
    release_id: str,
) -> Dict[str, Any]:
    """Add selected parent-wide contexts to a release-bound context directory."""
    source_context_dir = source_context_dir.resolve()
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    for source in source_context_dir.glob("*.json"):
        shutil.copy2(source, output_dir / source.name)

    source_manifest = json.loads((source_context_dir / "manifest.json").read_text())
    entries = list(source_manifest.get("contexts", []))
    previous_cache = os.environ.get("ASK_MIMIR_CACHE_DIR")
    previous_release = os.environ.get("ASK_MIMIR_RELEASE_ID")
    previous_memory_limit = os.environ.get("ASK_MIMIR_DUCKDB_MEMORY_LIMIT")
    try:
        with tempfile.TemporaryDirectory(prefix="ask-mimir-parent-precompute-") as cache:
            os.environ["ASK_MIMIR_CACHE_DIR"] = cache
            os.environ["ASK_MIMIR_RELEASE_ID"] = release_id
            os.environ["ASK_MIMIR_DUCKDB_MEMORY_LIMIT"] = "2GB"
            store = CompanyContextStore(source_context_dir, data_root)
            for query in parent_queries:
                resolution = store.search(query, scope_type="company_parent", limit=100)
                candidates = [
                    row
                    for row in resolution.get("matches", [])
                    if row.get("scope_type") == "company_parent"
                    and int(row.get("site_count") or 0) > 1
                ]
                if not candidates:
                    raise RuntimeError(f"parent scope did not resolve: {query}")
                match = max(candidates, key=lambda row: int(row.get("site_count") or 0))
                print(
                    f"Building parent context: {match['scope_name']} "
                    f"({match['site_count']} CAGE sites)",
                    flush=True,
                )
                clean_scope_id = str(match["scope_id"]).upper()
                replacement_scope_ids = _matching_parent_scope_ids(
                    candidates,
                    clean_scope_id,
                    entries,
                    str(match.get("scope_name") or ""),
                )
                store.contexts = [
                    context
                    for context in store.contexts
                    if not (
                        context.get("scope", {}).get("scope_type") == "company_parent"
                        and str(context.get("scope", {}).get("scope_id") or "").upper()
                        in replacement_scope_ids
                    )
                ]
                for scope_id in replacement_scope_ids:
                    store._dynamic_contexts.pop(("company_parent", scope_id), None)
                    store._context_paths.pop(("company_parent", scope_id), None)
                context = _bounded_precomputed_context(
                    dict(store.get_raw("company_parent", match["scope_id"]))
                )
                if store._dynamic_builder is not None:
                    store._dynamic_builder.connection.close()
                    store._dynamic_builder = None
                context.pop("_artifact_path", None)
                filename = (
                    hashlib.sha256(str(match["scope_id"]).encode()).hexdigest()[:16]
                    + "-parent-context.json"
                )
                destination = output_dir / filename
                temporary = destination.with_suffix(".json.tmp")
                temporary.write_text(json.dumps(context, default=str))
                temporary.replace(destination)
                replaced_entries = [
                    entry
                    for entry in entries
                    if (
                        entry.get("scope", {}).get("scope_type") == "company_parent"
                        and str(entry.get("scope", {}).get("scope_id") or "").upper()
                        in replacement_scope_ids
                    )
                ]
                entries = [
                    entry
                    for entry in entries
                    if not (
                        entry.get("scope", {}).get("scope_type") == "company_parent"
                        and str(entry.get("scope", {}).get("scope_id") or "").upper()
                        in replacement_scope_ids
                    )
                ]
                for replaced_entry in replaced_entries:
                    replaced_path = output_dir / str(replaced_entry.get("path") or "")
                    if replaced_path != destination and replaced_path.is_file():
                        replaced_path.unlink()
                entries.append(
                    {
                        "context_id": context["context_id"],
                        "scope": context["scope"],
                        "path": filename,
                    }
                )
    finally:
        dynamic_builder = locals().get("store") and store._dynamic_builder
        if dynamic_builder is not None:
            dynamic_builder.connection.close()
        if previous_cache is None:
            os.environ.pop("ASK_MIMIR_CACHE_DIR", None)
        else:
            os.environ["ASK_MIMIR_CACHE_DIR"] = previous_cache
        if previous_release is None:
            os.environ.pop("ASK_MIMIR_RELEASE_ID", None)
        else:
            os.environ["ASK_MIMIR_RELEASE_ID"] = previous_release
        if previous_memory_limit is None:
            os.environ.pop("ASK_MIMIR_DUCKDB_MEMORY_LIMIT", None)
        else:
            os.environ["ASK_MIMIR_DUCKDB_MEMORY_LIMIT"] = previous_memory_limit

    lazy_entries = []
    for entry in entries:
        context = json.loads((output_dir / entry["path"]).read_text())
        identity = context.get("identity", {})
        search_sites = [
            {
                key: site.get(key)
                for key in (
                    "cage",
                    "vendor_name",
                    "official_site_label",
                    "city",
                    "state",
                )
            }
            for site in identity.get("sites", [])
        ]
        lazy_entries.append(
            {
                "context_id": context.get("context_id"),
                "scope": context.get("scope", {}),
                "path": entry["path"],
                "lazy": True,
                "search_identity": {
                    "sites": search_sites,
                    "site_count": identity.get("site_count", len(search_sites)),
                    "resolved_cages": identity.get("resolved_cages", []),
                    "parent_resolution": identity.get("parent_resolution", {}),
                },
            }
        )

    referenced_paths = {str(entry["path"]) for entry in lazy_entries}
    for context_path in output_dir.glob("*.json"):
        if context_path.name != "manifest.json" and context_path.name not in referenced_paths:
            context_path.unlink()

    manifest = {
        **source_manifest,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runtime_release_id": release_id,
        "precomputed_parent_queries": list(parent_queries),
        "contexts": lazy_entries,
    }
    manifest_path = output_dir / "manifest.json"
    temporary = manifest_path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(manifest, indent=2))
    temporary.replace(manifest_path)
    return manifest
