"""Evidence-led market-wide supplier discovery for supported capabilities."""

from __future__ import annotations

import json
import hashlib
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import quote, unquote

import duckdb


DEFAULT_DATA_ROOT = Path(
    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data"
)
ROOT = Path(__file__).resolve().parent
DEFAULT_PRECOMPUTED_DIR = ROOT / "validation-output" / "capability-markets"
DEFAULT_ONTOLOGY_PATH = Path(
    os.getenv("ASK_MIMIR_CAPABILITY_ONTOLOGY", str(ROOT / "capability_ontology.json"))
)
EVIDENCE_MODES = {
    "classification_complete",
    "classification_complete_sparse_items",
    "description_bounded",
    "hybrid",
}


def load_capability_ontology(path: Path = DEFAULT_ONTOLOGY_PATH) -> Dict[str, Any]:
    ontology = json.loads(path.read_text())
    if ontology.get("schema_version") != 1:
        raise ValueError("unsupported capability ontology schema version")
    definitions = ontology.get("capabilities")
    if not isinstance(definitions, dict) or not definitions:
        raise ValueError("capability ontology must contain capability definitions")
    required = {"display_name", "evidence_mode", "request_pattern", "fsc_codes", "item_pattern", "scope_note"}
    for capability_id, definition in definitions.items():
        if not re.fullmatch(r"[a-z0-9_]+", capability_id):
            raise ValueError(f"invalid capability id: {capability_id}")
        missing = required.difference(definition)
        if missing:
            raise ValueError(f"{capability_id} is missing fields: {sorted(missing)}")
        if definition["evidence_mode"] not in EVIDENCE_MODES:
            raise ValueError(f"{capability_id} has an invalid evidence mode")
        complete_codes = set(definition.get("complete_fsc_codes", []))
        if not complete_codes.issubset(set(definition["fsc_codes"])):
            raise ValueError(f"{capability_id} complete FSC codes must be in fsc_codes")
        re.compile(definition["request_pattern"], re.IGNORECASE)
        re.compile(definition["item_pattern"], re.IGNORECASE)
    aliases = ontology.get("id_aliases", {})
    unknown_aliases = sorted(set(aliases.values()).difference(definitions))
    if unknown_aliases:
        raise ValueError(f"capability aliases reference unknown ids: {unknown_aliases}")
    return ontology


CAPABILITY_ONTOLOGY = load_capability_ontology()
CAPABILITY_DEFINITIONS = CAPABILITY_ONTOLOGY["capabilities"]
CAPABILITY_ID_ALIASES = CAPABILITY_ONTOLOGY.get("id_aliases", {})

DYNAMIC_CAPABILITY_PREFIX = "capability:"
CAPABILITY_QUERY_PATTERNS = (
    r"(?:tell\s+(?:me|be)\s+about)\s+(?:the\s+)?(.+?)\s+market(?:[?.]|$)",
    r"what\s+is\s+(?:the\s+)?market\s+for\s+(.+?)(?:[?.]|$)",
    r"what\s+is\s+(?:the\s+)?(.+?)\s+market(?:[?.]|$)",
    r"(?:market,\s*)?capability area(?:\s+or\s+industrial base)?\s*:\s*(.+?)(?:\?|$)",
    r"overview of (?:the\s+)?(?:us\s+)?(?:defen[cs]e\s+)?(?:market|capability area|industrial base)\s*:\s*(.+?)(?:\?|$)",
    r"overview of (?:the\s+)?(.+?)\s+in\s+(?:the\s+)?(?:us\s+)?defen[cs]e market(?:[?.]|$)",
    r"(?:what is happening in|describe|analyse|analyze)\s+(?:the\s+)?(?:us\s+)?(.+?)\s+(?:defen[cs]e\s+)?market(?:[?.]|$)",
    r"(?:give me an?\s+)?overview of\s+(?:the\s+)?(.+?)\s+market(?:[?.]|$)",
    r"(?:find|identify|show)\s+(?:us\s+)?(?:manufacturers|suppliers|companies)\s+(?:that\s+)?(?:supply|provide|make|manufacture)\s+(.+?)(?:\s+to|\s+for)\s+(?:the\s+)?(?:us\s+)?military",
    r"(?:which|what)\s+(?:us\s+)?(?:manufacturers|suppliers|companies)\s+(?:supply|provide|make|manufacture)\s+(.+?)(?:\?|$)",
    r"(?:companies|manufacturers|suppliers)\s+(?:supplying|providing|manufacturing|with)\s+(.+?)(?:\s+to|\s+for)\s+(?:military|defen[cs]e)",
    r"who\s+(?:makes|manufactures)\s+(.+?)(?:\?|$)",
    r"(?:who\s+makes|find|identify|show)\s+(?:us\s+)?(?:manufacturers|suppliers|companies|firms)?\s*(?:that\s+)?(?:supply|supplying|provide|providing|make|manufacture|manufacturing|of)\s+(.+?)(?:\?|$)",
    r"(?:find|identify|show)\s+(?:us\s+)?(?:manufacturers|suppliers|companies|firms)\s+(?:with|capable\s+of\s+(?:making|manufacturing|producing))\s+(.+?)(?:\?|$)",
)
CAPABILITY_STOPWORDS = {
    "and", "or", "the", "a", "an", "for", "to", "of", "into", "with",
    "military", "defense", "defence", "platform", "platforms", "systems",
    "system", "equipment", "components", "component", "products", "product",
    "aerospace", "aircraft", "aviation", "airborne", "naval", "maritime",
    "shipboard", "ground", "vehicle", "vehicles", "land", "space",
}
CAPABILITY_DOMAIN_RULES = (
    (r"\b(?:aerospace|aircraft|aviation|airborne)\b", ["AIR"], ["aircraft", "aviation", "airborne", "aerospace"]),
    (r"\b(?:naval|maritime|shipboard|warship|submarine)\b", ["NAVAL"], ["naval", "ship", "marine", "submarine"]),
    (r"\b(?:ground|land)[- ]?(?:vehicle|vehicles|system|systems)?\b", ["GROUND"], ["ground", "vehicular", "vehicle"]),
    (r"\b(?:missile|missiles|munition|munitions)\b", ["MISSILES & MUNITIONS"], ["missile", "munition"]),
    (r"\bspace\b", ["SPACE"], ["space", "satellite"]),
)
US_STATE_CODES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI",
    "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN",
    "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH",
    "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA",
    "WV", "WI", "WY", "PR",
]

NON_COMMERCIAL_NAME_PATTERN = re.compile(
    r"MILITARY (?:STANDARDS|SPECIFICATIONS)|NAVAL INVENTORY|NAVAIR|NAVSEA|"
    r"^DEPARTMENT OF|UNITED STATES DEPARTMENT|U\.?\s*S\.?\s*(?:ARMY|AIR FORCE|NAVY)|"
    r"DEFENSE LOGISTICS AGENCY|AIR LOGISTICS CENTER|"
    r"NAVAL (?:AIR|SEA) SYSTEMS COMMAND|COMBAT CAPABILITIES DEVELOPMENT|"
    r"JOINT ELECTRONICS TYPE DESIGNATION(?: SYSTEM)?",
    re.IGNORECASE,
)


def _rows(cursor: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def summarize_platform_breadth(
    platform_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Describe platform breadth without treating NIIN associations as financial share."""
    total_associations = sum(
        int(row.get("matching_niin_count") or 0) for row in platform_rows
    )
    leading = platform_rows[0] if platform_rows else {}
    leading_count = int(leading.get("matching_niin_count") or 0)
    leading_share = (
        leading_count / total_associations if total_associations > 0 else 0.0
    )
    return {
        "platforms_shown": len(platform_rows),
        "total_niin_platform_associations_shown": total_associations,
        "leading_platform": leading.get("platform"),
        "leading_platform_niin_count": leading_count,
        "leading_platform_share_of_associations_shown": leading_share,
        "single_platform_dominates_associations_shown": leading_share >= 0.5,
        "interpretation": (
            "Counts describe item-to-platform associations, not procurement value or "
            "market share. A market should not be characterized as centered on the "
            "leading platform unless single_platform_dominates_associations_shown is true."
        ),
    }


def resolve_capability(text: str) -> str | None:
    clean = str(text or "")
    if re.search(r"\bwho\s+competes\s+with\b|\bwhere\s+should\b", clean, re.IGNORECASE):
        return None
    for capability_id, definition in CAPABILITY_DEFINITIONS.items():
        if re.search(definition["request_pattern"], clean, re.IGNORECASE):
            return capability_id
    for pattern in CAPABILITY_QUERY_PATTERNS:
        match = re.search(pattern, clean, re.IGNORECASE)
        if not match:
            continue
        phrase = re.sub(r"\s+", " ", match.group(1)).strip(" .?")
        if phrase:
            return DYNAMIC_CAPABILITY_PREFIX + quote(phrase.lower(), safe="-_ ")
    return None


def _capability_terms(phrase: str) -> List[str]:
    normalized = re.sub(r"[^a-z0-9]+", " ", phrase.lower())
    terms = []
    for token in normalized.split():
        singular = token[:-1] if token.endswith("s") and len(token) > 4 else token
        if singular not in CAPABILITY_STOPWORDS and len(singular) >= 4:
            for suffix in ("ations", "ation", "ications", "ication", "ing"):
                if singular.endswith(suffix) and len(singular) - len(suffix) >= 4:
                    singular = singular[: -len(suffix)]
                    break
            terms.append(singular)
    return list(dict.fromkeys(terms))[:8]


def _capability_domains(phrase: str) -> tuple[List[str], List[str]]:
    segments: List[str] = []
    classification_terms: List[str] = []
    for pattern, matched_segments, matched_terms in CAPABILITY_DOMAIN_RULES:
        if re.search(pattern, phrase, re.IGNORECASE):
            segments.extend(matched_segments)
            classification_terms.extend(matched_terms)
    return list(dict.fromkeys(segments)), list(dict.fromkeys(classification_terms))


def capability_market_follow_up_intent(text: str) -> bool:
    """Recognize follow-ups that should retain the selected capability market."""
    lowered = str(text or "").lower()
    normalized = re.sub(r"[^a-z0-9]+", " ", lowered).strip()
    if normalized in {
        "us military",
        "u s military",
        "us defense",
        "u s defense",
        "us defence",
        "u s defence",
        "military",
        "defense",
        "defence",
        "the broader ecosystem",
        "broader ecosystem",
        "complete units",
        "complete systems",
        "both",
        "global",
        "global defense",
        "global defence",
        "commercial and defense",
        "commercial and defence",
    }:
        return True
    return any(
        phrase in lowered
        for phrase in (
            "these suppliers",
            "those suppliers",
            "which of these",
            "which of those",
            "five suppliers",
            "top suppliers",
            "most relevant",
            "multiple military aircraft",
            "multiple aircraft platforms",
            "support multiple platforms",
            "multiple defence programs",
            "multiple defense programs",
            "evidence supporting",
            "directly evidenced",
            "which are inferred",
        )
    )


class CapabilityDiscoveryStore:
    """Build a CAGE-site supplier universe from exact item and source evidence."""

    def __init__(
        self,
        data_root: Path = DEFAULT_DATA_ROOT,
        precomputed_dir: Path | None = None,
        *,
        load_precomputed: bool = True,
    ) -> None:
        self.data_root = data_root.resolve()
        self.ontology_path = DEFAULT_ONTOLOGY_PATH.resolve()
        self.paths = {
            "references": self.data_root / "nsn_cage_reference.parquet",
            "suppliers": self.data_root / "nsn_supplier_lookup.parquet",
            "locations": self.data_root / "cage_locations.parquet",
            "contracts": self.data_root / "contracts_rolled.parquet",
            "classifications": self.data_root / "classification_reference.parquet",
        }
        missing = [str(path) for path in self.paths.values() if not path.exists()]
        if missing:
            raise FileNotFoundError(f"capability-discovery sources are missing: {missing}")
        self.connection = duckdb.connect()
        self.connection.execute("SET preserve_insertion_order=false")
        self.connection.execute("SET threads=2")
        self.connection.execute("SET memory_limit='1GB'")
        duckdb_temp = os.getenv("ASK_MIMIR_DUCKDB_TEMP", "/tmp/ask-mimir-duckdb")
        self.connection.execute("SET temp_directory = ?", [duckdb_temp])
        self._cache: Dict[str, Dict[str, Any]] = {}
        configured_dir = precomputed_dir or Path(
            os.getenv("ASK_MIMIR_CAPABILITY_DIR", str(DEFAULT_PRECOMPUTED_DIR))
        )
        self.precomputed_dir = configured_dir.resolve() if load_precomputed else None

    def search(self, query: str) -> Dict[str, Any]:
        capability_id = resolve_capability(query)
        if capability_id and capability_id.startswith(DYNAMIC_CAPABILITY_PREFIX):
            capability_name = unquote(
                capability_id[len(DYNAMIC_CAPABILITY_PREFIX):]
            ).strip().title()
        else:
            capability_name = (
                CAPABILITY_DEFINITIONS[capability_id]["display_name"]
                if capability_id
                else None
            )
        return {
            "query": str(query or "").strip(),
            "resolved_capability_id": capability_id,
            "resolved_capability_name": capability_name,
        }

    def get(self, capability_id: str, limit: int = 25) -> Dict[str, Any]:
        clean_id = str(capability_id or "").strip().lower()
        clean_id = CAPABILITY_ID_ALIASES.get(clean_id, clean_id)
        definition = CAPABILITY_DEFINITIONS.get(clean_id)
        if not definition and clean_id.startswith(DYNAMIC_CAPABILITY_PREFIX):
            definition = self._dynamic_definition(clean_id)
        if not definition:
            raise KeyError(f"capability definition was not found: {capability_id}")
        if clean_id not in self._cache:
            precomputed_path = (
                self.precomputed_dir / f"{clean_id}.json"
                if self.precomputed_dir is not None
                else None
            )
            if precomputed_path is not None and precomputed_path.exists():
                self._cache[clean_id] = json.loads(precomputed_path.read_text())
            else:
                self._cache[clean_id] = self._build(clean_id, definition)
        pack = self._cache[clean_id]
        bounded = min(max(int(limit), 1), 50)
        return {**pack, "supplier_sites": pack["supplier_sites"][:bounded]}

    def _dynamic_definition(self, capability_id: str) -> Dict[str, Any] | None:
        phrase = unquote(capability_id[len(DYNAMIC_CAPABILITY_PREFIX):]).strip()
        terms = _capability_terms(phrase)
        if not terms:
            return None
        market_segments, domain_terms = _capability_domains(phrase)
        minimum_matches = min(2, len(terms))
        classification_candidates = _rows(
            self.connection.execute(
                """
                SELECT code, description
                FROM read_parquet(?)
                WHERE classification_type = 'PSC'
                  AND REGEXP_MATCHES(code, '^[A-Z0-9]{4}$')
                """,
                [str(self.paths["classifications"])],
            )
        )
        scored_codes = []
        for row in classification_candidates:
            description = str(row.get("description") or "").lower()
            subject_matches = sum(term in description for term in terms)
            domain_matches = sum(
                bool(re.search(rf"\b{re.escape(term)}\b", description))
                for term in domain_terms
            )
            if subject_matches < minimum_matches:
                continue
            scored_codes.append(
                {
                    **row,
                    "term_matches": subject_matches,
                    "domain_matches": domain_matches,
                }
            )
        contextual_codes = [row for row in scored_codes if row["domain_matches"] > 0]
        if domain_terms:
            scored_codes = contextual_codes
        code_rows = sorted(
            scored_codes,
            key=lambda row: (-row["domain_matches"], -row["term_matches"], row["code"]),
        )[:20]
        item_terms = list(terms)
        if any(term.startswith("comput") for term in terms):
            item_terms.extend(["processor", "data processing", "single board computer"])
        item_pattern = "|".join(re.escape(term).replace(r"\ ", r"\s+") for term in item_terms)
        return {
            "display_name": phrase.title(),
            "evidence_mode": "description_bounded",
            "fsc_codes": [str(row["code"]) for row in code_rows],
            "item_pattern": item_pattern.upper(),
            "scope_note": f"Supplier evidence matched to the requested capability using product classifications and item descriptions for {phrase}.",
            "classification_matches": code_rows,
            "dynamic": True,
            "term_stems": terms,
            "minimum_term_matches": minimum_matches,
            "market_segments": market_segments,
        }

    def _build(self, capability_id: str, definition: Dict[str, Any]) -> Dict[str, Any]:
        fsc_codes = definition["fsc_codes"]
        complete_fsc_codes = definition.get("complete_fsc_codes", [])
        classification_matches = definition.get("classification_matches", [])
        market_segments = definition.get("market_segments", [])
        if fsc_codes and not classification_matches:
            classification_matches = _rows(
                self.connection.execute(
                    """
                    SELECT code, description
                    FROM read_parquet(?)
                    WHERE classification_type = 'PSC'
                      AND code IN (SELECT UNNEST(?))
                    ORDER BY code
                    """,
                    [str(self.paths["classifications"]), fsc_codes],
                )
            )
        use_scope = bool(fsc_codes or market_segments)
        is_dynamic = bool(definition.get("dynamic"))
        term_stems = definition.get("term_stems", [])
        minimum_term_matches = (
            definition.get("minimum_term_matches", 1) if is_dynamic else 1
        )
        rows = _rows(
            self.connection.execute(
                """
                WITH observed_procurement AS (
                    SELECT
                        LPAD(TRIM(niin), 9, '0') AS niin,
                        UPPER(TRIM(cage)) AS cage,
                        MAX(vendor) AS vendor_name,
                        SUM(COALESCE(total_revenue, 0)) AS observed_value,
                        COUNT(DISTINCT contract_id) AS observed_contract_count,
                        MIN(year) AS first_observed_fiscal_year,
                        MAX(year) AS last_observed_fiscal_year
                    FROM read_parquet(?)
                    WHERE year BETWEEN 2021 AND 2026
                      AND cage IS NOT NULL AND TRIM(cage) <> ''
                    GROUP BY 1, 2
                ), item_relationships AS (
                    SELECT
                        LPAD(TRIM(r.niin), 9, '0') AS niin,
                        MAX(r.nsn) AS nsn,
                        UPPER(TRIM(r.cage)) AS cage,
                        COALESCE(MAX(o.vendor_name), MAX(r.vendor_name)) AS vendor_name,
                        MAX(r.description) AS description,
                        LIST_SLICE(
                            LIST_DISTINCT(LIST(r.part_number) FILTER (
                                WHERE r.part_number IS NOT NULL AND TRIM(r.part_number) <> ''
                            )), 1, 8
                        ) AS sample_part_numbers,
                        BOOL_OR(COALESCE(r.is_active_authorized_source, false))
                            AS is_active_authorized_source,
                        BOOL_OR(o.cage IS NOT NULL) AS has_observed_procurement,
                        COALESCE(MAX(o.observed_value), 0)
                            AS observed_dla_procurement_value_usd,
                        COALESCE(MAX(o.observed_contract_count), 0)
                            AS observed_contract_count,
                        MIN(o.first_observed_fiscal_year) AS first_observed_fiscal_year,
                        MAX(o.last_observed_fiscal_year) AS last_observed_fiscal_year,
                        LIST_SLICE(
                            LIST_DISTINCT(LIST(r.platform_families) FILTER (
                                WHERE r.platform_families IS NOT NULL
                                  AND TRIM(r.platform_families) <> ''
                            )), 1, 12
                        ) AS platform_groups
                    FROM read_parquet(?) r
                    LEFT JOIN observed_procurement o
                      ON LPAD(TRIM(r.niin), 9, '0') = o.niin
                     AND UPPER(TRIM(r.cage)) = o.cage
                    WHERE (NOT ?
                           OR r.fsc_code IN (SELECT UNNEST(?))
                           OR UPPER(TRIM(COALESCE(r.market_segment, ''))) IN (SELECT UNNEST(?)))
                      AND (r.fsc_code IN (SELECT UNNEST(?))
                           OR (NOT ? AND REGEXP_MATCHES(UPPER(COALESCE(r.description, '')), ?))
                           OR (? AND LIST_SUM(LIST_TRANSFORM(?, term ->
                               CASE WHEN CONTAINS(LOWER(COALESCE(r.description, '')), term)
                                    THEN 1 ELSE 0 END
                           )) >= ?))
                      AND r.cage IS NOT NULL AND TRIM(r.cage) <> ''
                    GROUP BY 1, 3
                ), locations AS (
                    SELECT UPPER(TRIM(cage_code)) AS cage,
                           MAX(vendor_name) AS location_vendor_name,
                           MAX(city) AS city,
                           MAX(state) AS state,
                           MAX(latitude) AS latitude,
                           MAX(longitude) AS longitude,
                           MAX(location_quality) AS location_quality
                    FROM read_parquet(?)
                    GROUP BY 1
                )
                SELECT
                    r.cage,
                    COALESCE(MAX(r.vendor_name), MAX(l.location_vendor_name)) AS supplier_name,
                    MAX(l.city) AS city,
                    MAX(l.state) AS state,
                    MAX(l.location_quality) AS location_quality,
                    COUNT(DISTINCT r.niin) AS matching_niin_count,
                    COUNT(DISTINCT r.niin) FILTER (
                        WHERE r.is_active_authorized_source
                    ) AS active_authorized_niin_count,
                    COUNT(DISTINCT r.niin) FILTER (
                        WHERE r.has_observed_procurement
                    ) AS observed_procurement_niin_count,
                    COUNT(DISTINCT r.niin) FILTER (
                        WHERE LEN(r.platform_groups) > 0
                    ) AS mapped_platform_niin_count,
                    SUM(r.observed_dla_procurement_value_usd) AS observed_dla_procurement_value_usd,
                    SUM(COALESCE(r.observed_contract_count, 0)) AS observed_contract_count,
                    MIN(r.first_observed_fiscal_year) AS first_observed_fiscal_year,
                    MAX(r.last_observed_fiscal_year) AS last_observed_fiscal_year,
                    LIST_SLICE(LIST(STRUCT_PACK(
                        niin := r.niin,
                        nsn := r.nsn,
                        description := r.description,
                        part_numbers := r.sample_part_numbers,
                        active_authorized_source := r.is_active_authorized_source,
                        observed_procurement := r.has_observed_procurement,
                        platform_groups := r.platform_groups
                    ) ORDER BY r.is_active_authorized_source DESC,
                               r.has_observed_procurement DESC,
                               r.observed_dla_procurement_value_usd DESC), 1, 10)
                        AS item_evidence,
                    COUNT(*) OVER () AS total_available
                FROM item_relationships r
                LEFT JOIN locations l USING (cage)
                WHERE (r.is_active_authorized_source OR r.has_observed_procurement)
                  AND UPPER(TRIM(l.state)) IN (SELECT UNNEST(?))
                  AND (l.latitude IS NULL OR (
                      l.latitude BETWEEN 18 AND 72
                      AND l.longitude BETWEEN -180 AND -60
                  ))
                GROUP BY r.cage
                ORDER BY active_authorized_niin_count DESC,
                         observed_procurement_niin_count DESC,
                         matching_niin_count DESC,
                         mapped_platform_niin_count DESC
                """,
                [
                    str(self.paths["suppliers"]),
                    str(self.paths["references"]),
                    use_scope,
                    fsc_codes,
                    market_segments,
                    complete_fsc_codes,
                    is_dynamic,
                    definition["item_pattern"],
                    is_dynamic,
                    term_stems,
                    minimum_term_matches,
                    str(self.paths["locations"]),
                    US_STATE_CODES,
                ],
            )
        )
        commercial_rows = [
            row
            for row in rows
            if not NON_COMMERCIAL_NAME_PATTERN.search(str(row.get("supplier_name") or ""))
        ]
        matching_niins = int(
            self.connection.execute(
                """
                SELECT COUNT(DISTINCT LPAD(TRIM(niin), 9, '0'))
                FROM read_parquet(?)
                WHERE (NOT ?
                       OR fsc_code IN (SELECT UNNEST(?))
                       OR UPPER(TRIM(COALESCE(market_segment, ''))) IN (SELECT UNNEST(?)))
                  AND (fsc_code IN (SELECT UNNEST(?))
                       OR (NOT ? AND REGEXP_MATCHES(UPPER(COALESCE(description, '')), ?))
                       OR (? AND LIST_SUM(LIST_TRANSFORM(?, term ->
                           CASE WHEN CONTAINS(LOWER(COALESCE(description, '')), term)
                                THEN 1 ELSE 0 END
                       )) >= ?))
                  AND cage IS NOT NULL AND TRIM(cage) <> ''
                """,
                [str(self.paths["references"]), use_scope, fsc_codes, market_segments,
                 complete_fsc_codes, is_dynamic,
                 definition["item_pattern"], is_dynamic, term_stems, minimum_term_matches],
            ).fetchone()[0]
            or 0
        )
        prime_award_sites = _rows(
            self.connection.execute(
                """
                WITH locations AS (
                    SELECT UPPER(TRIM(cage_code)) AS cage,
                           MAX(vendor_name) AS location_name,
                           MAX(city) AS city, MAX(state) AS state,
                           MAX(latitude) AS latitude, MAX(longitude) AS longitude
                    FROM read_parquet(?) GROUP BY 1
                )
                SELECT UPPER(TRIM(c.vendor_cage)) AS cage,
                       COALESCE(MAX(c.vendor_name), MAX(l.location_name)) AS supplier_name,
                       MAX(l.city) AS city, MAX(l.state) AS state,
                       SUM(
                           COALESCE(c.obligations_fy2021, 0) +
                           COALESCE(c.obligations_fy2022, 0) +
                           COALESCE(c.obligations_fy2023, 0) +
                           COALESCE(c.obligations_fy2024, 0) +
                           COALESCE(c.obligations_fy2025, 0) +
                           COALESCE(c.obligations_fy2026, 0)
                       ) AS broader_related_award_obligations_usd,
                       SUM(CASE WHEN c.psc IN (SELECT UNNEST(?)) THEN
                           COALESCE(c.obligations_fy2021, 0) +
                           COALESCE(c.obligations_fy2022, 0) +
                           COALESCE(c.obligations_fy2023, 0) +
                           COALESCE(c.obligations_fy2024, 0) +
                           COALESCE(c.obligations_fy2025, 0) +
                           COALESCE(c.obligations_fy2026, 0)
                       ELSE 0 END) AS directly_classified_prime_obligations_usd,
                       COUNT(DISTINCT c.award_key) FILTER (WHERE
                           COALESCE(c.action_count_fy2021, 0) +
                           COALESCE(c.action_count_fy2022, 0) +
                           COALESCE(c.action_count_fy2023, 0) +
                           COALESCE(c.action_count_fy2024, 0) +
                           COALESCE(c.action_count_fy2025, 0) +
                           COALESCE(c.action_count_fy2026, 0) > 0
                       ) AS prime_award_count,
                       LIST_SLICE(LIST_DISTINCT(LIST(c.psc)), 1, 12) AS matching_psc_codes,
                       LIST_SLICE(LIST_DISTINCT(LIST(c.platform_family) FILTER (
                           WHERE c.platform_family IS NOT NULL
                       )), 1, 20) AS platforms,
                       LIST_SLICE(LIST_DISTINCT(LIST(COALESCE(
                           c.base_award_description, c.description
                       ))), 1, 8) AS award_descriptions
                FROM read_parquet(?) c
                LEFT JOIN locations l ON UPPER(TRIM(c.vendor_cage)) = l.cage
                WHERE c.source_system = 'USA_SPENDING'
                  AND c.vendor_cage IS NOT NULL
                  AND UPPER(TRIM(l.state)) IN (SELECT UNNEST(?))
                  AND (l.latitude IS NULL OR (
                      l.latitude BETWEEN 18 AND 72
                      AND l.longitude BETWEEN -180 AND -60
                  ))
                  AND (NOT ?
                       OR c.psc IN (SELECT UNNEST(?))
                       OR UPPER(TRIM(COALESCE(c.market_segment, ''))) IN (SELECT UNNEST(?)))
                  AND (c.psc IN (SELECT UNNEST(?))
                       OR (NOT ? AND REGEXP_MATCHES(UPPER(COALESCE(
                           c.base_award_description, c.description, ''
                       )), ?)) OR (? AND LIST_SUM(LIST_TRANSFORM(?, term ->
                           CASE WHEN CONTAINS(LOWER(COALESCE(
                               c.base_award_description, c.description, ''
                           )), term) THEN 1 ELSE 0 END
                       )) >= ?))
                GROUP BY 1
                ORDER BY ABS(directly_classified_prime_obligations_usd) DESC,
                         ABS(broader_related_award_obligations_usd) DESC
                LIMIT 100
                """,
                [
                    str(self.paths["locations"]), fsc_codes,
                    str(self.paths["contracts"]),
                    US_STATE_CODES, use_scope, fsc_codes, market_segments,
                    complete_fsc_codes, is_dynamic,
                    definition["item_pattern"], is_dynamic, term_stems,
                    minimum_term_matches,
                ],
            )
        )
        prime_by_cage = {
            str(row.get("cage") or "").upper(): row for row in prime_award_sites
        }
        for index, row in enumerate(commercial_rows, start=1):
            row["rank"] = index
            prime = prime_by_cage.get(str(row.get("cage") or "").upper(), {})
            row["direct_prime_award_count"] = int(prime.get("prime_award_count") or 0)
            row["evidence_summary"] = [
                label
                for count, label in (
                    (
                        int(row["active_authorized_niin_count"] or 0),
                        f"Authorized by DLA for {int(row['active_authorized_niin_count'] or 0):,} matched item(s)",
                    ),
                    (
                        int(row["observed_procurement_niin_count"] or 0),
                        f"Received DLA procurement awards for {int(row['observed_procurement_niin_count'] or 0):,} matched item(s)",
                    ),
                    (
                        int(prime.get("prime_award_count") or 0),
                        f"Recipient of {int(prime.get('prime_award_count') or 0):,} directly relevant prime award(s)",
                    ),
                )
                if count > 0
            ]
        annual_prime_activity = _rows(
            self.connection.execute(
                """
                WITH matched AS (
                    SELECT *, psc IN (SELECT UNNEST(?)) AS is_direct_classification
                    FROM read_parquet(?)
                    WHERE source_system = 'USA_SPENDING'
                      AND (NOT ?
                           OR psc IN (SELECT UNNEST(?))
                           OR UPPER(TRIM(COALESCE(market_segment, ''))) IN (SELECT UNNEST(?)))
                      AND (psc IN (SELECT UNNEST(?))
                           OR (NOT ? AND REGEXP_MATCHES(UPPER(COALESCE(
                               base_award_description, description, ''
                           )), ?)) OR (? AND LIST_SUM(LIST_TRANSFORM(?, term ->
                               CASE WHEN CONTAINS(LOWER(COALESCE(
                                   base_award_description, description, ''
                               )), term) THEN 1 ELSE 0 END
                           )) >= ?))
                ), annualized AS (
                    SELECT award_key, vendor_cage, is_direct_classification,
                           activity.fiscal_year,
                           activity.net_obligations_usd,
                           activity.action_count
                    FROM matched,
                    UNNEST([
                        STRUCT_PACK(fiscal_year := 2021, net_obligations_usd := COALESCE(obligations_fy2021, 0), action_count := COALESCE(action_count_fy2021, 0)),
                        STRUCT_PACK(fiscal_year := 2022, net_obligations_usd := COALESCE(obligations_fy2022, 0), action_count := COALESCE(action_count_fy2022, 0)),
                        STRUCT_PACK(fiscal_year := 2023, net_obligations_usd := COALESCE(obligations_fy2023, 0), action_count := COALESCE(action_count_fy2023, 0)),
                        STRUCT_PACK(fiscal_year := 2024, net_obligations_usd := COALESCE(obligations_fy2024, 0), action_count := COALESCE(action_count_fy2024, 0)),
                        STRUCT_PACK(fiscal_year := 2025, net_obligations_usd := COALESCE(obligations_fy2025, 0), action_count := COALESCE(action_count_fy2025, 0)),
                        STRUCT_PACK(fiscal_year := 2026, net_obligations_usd := COALESCE(obligations_fy2026, 0), action_count := COALESCE(action_count_fy2026, 0))
                    ]) AS u(activity)
                )
                SELECT fiscal_year,
                       SUM(net_obligations_usd) FILTER (WHERE is_direct_classification)
                           AS directly_classified_prime_obligations_usd,
                       SUM(net_obligations_usd)
                           AS broader_related_award_obligations_usd,
                       COUNT(DISTINCT award_key) FILTER (WHERE action_count > 0)
                           AS prime_award_count,
                       COUNT(DISTINCT vendor_cage) FILTER (WHERE action_count > 0)
                           AS recipient_site_count
                FROM annualized
                GROUP BY 1
                ORDER BY 1
                """,
                [
                    fsc_codes, str(self.paths["contracts"]), use_scope, fsc_codes,
                    market_segments, complete_fsc_codes, is_dynamic,
                    definition["item_pattern"],
                    is_dynamic, term_stems, minimum_term_matches,
                ],
            )
        )
        top_platform_activity = _rows(
            self.connection.execute(
                """
                WITH matched AS (
                    SELECT DISTINCT
                           LPAD(TRIM(niin), 9, '0') AS niin,
                           UPPER(TRIM(cage)) AS cage,
                           TRIM(platform) AS platform
                    FROM read_parquet(?),
                         UNNEST(STRING_SPLIT(COALESCE(platform_families, ''), ' | ')) AS p(platform)
                    WHERE (NOT ?
                           OR fsc_code IN (SELECT UNNEST(?))
                           OR UPPER(TRIM(COALESCE(market_segment, ''))) IN (SELECT UNNEST(?)))
                      AND (fsc_code IN (SELECT UNNEST(?))
                           OR (NOT ? AND REGEXP_MATCHES(UPPER(COALESCE(description, '')), ?))
                           OR (? AND LIST_SUM(LIST_TRANSFORM(?, term ->
                               CASE WHEN CONTAINS(LOWER(COALESCE(description, '')), term)
                                    THEN 1 ELSE 0 END
                           )) >= ?))
                      AND COALESCE(TRIM(platform), '') <> ''
                )
                SELECT platform,
                       COUNT(DISTINCT niin) AS matching_niin_count,
                       COUNT(DISTINCT cage) AS supplier_site_count
                FROM matched
                GROUP BY 1
                ORDER BY matching_niin_count DESC, supplier_site_count DESC, platform
                LIMIT 30
                """,
                [
                    str(self.paths["references"]), use_scope, fsc_codes,
                    market_segments, complete_fsc_codes, is_dynamic,
                    definition["item_pattern"],
                    is_dynamic, term_stems, minimum_term_matches,
                ],
            )
        )
        ecosystem_fsc_codes = definition.get("ecosystem_fsc_codes", [])
        broader_ecosystem = None
        if ecosystem_fsc_codes:
            classified_ecosystem_niins = int(
                self.connection.execute(
                    """
                    SELECT COUNT(DISTINCT LPAD(TRIM(niin), 9, '0'))
                    FROM read_parquet(?)
                    WHERE fsc_code IN (SELECT UNNEST(?))
                      AND niin IS NOT NULL AND TRIM(niin) <> ''
                    """,
                    [str(self.paths["references"]), ecosystem_fsc_codes],
                ).fetchone()[0]
                or 0
            )
            ecosystem_rows = _rows(
                self.connection.execute(
                    """
                    WITH observed_procurement AS (
                        SELECT LPAD(TRIM(niin), 9, '0') AS niin,
                               UPPER(TRIM(cage)) AS cage,
                               MAX(vendor) AS supplier_name,
                               SUM(COALESCE(total_revenue, 0)) AS observed_value
                        FROM read_parquet(?)
                        WHERE year BETWEEN 2021 AND 2026
                          AND cage IS NOT NULL AND TRIM(cage) <> ''
                        GROUP BY 1, 2
                    ), relationships AS (
                        SELECT LPAD(TRIM(r.niin), 9, '0') AS niin,
                               UPPER(TRIM(r.cage)) AS cage,
                               COALESCE(MAX(o.supplier_name), MAX(r.vendor_name)) AS supplier_name,
                               BOOL_OR(COALESCE(r.is_active_authorized_source, false)) AS is_active,
                               BOOL_OR(o.cage IS NOT NULL) AS is_observed,
                               COALESCE(MAX(o.observed_value), 0) AS observed_value
                        FROM read_parquet(?) r
                        LEFT JOIN observed_procurement o
                          ON LPAD(TRIM(r.niin), 9, '0') = o.niin
                         AND UPPER(TRIM(r.cage)) = o.cage
                        WHERE r.fsc_code IN (SELECT UNNEST(?))
                          AND r.cage IS NOT NULL AND TRIM(r.cage) <> ''
                        GROUP BY 1, 2
                    ), locations AS (
                        SELECT UPPER(TRIM(cage_code)) AS cage,
                               MAX(vendor_name) AS location_name,
                               MAX(state) AS state,
                               MAX(latitude) AS latitude,
                               MAX(longitude) AS longitude
                        FROM read_parquet(?) GROUP BY 1
                    )
                    SELECT r.cage,
                           COALESCE(MAX(r.supplier_name), MAX(l.location_name)) AS supplier_name,
                           LIST(DISTINCT r.niin) AS niins,
                           COUNT(DISTINCT r.niin) AS matching_niin_count,
                           COUNT(DISTINCT r.niin) FILTER (WHERE r.is_active) AS active_niin_count,
                           COUNT(DISTINCT r.niin) FILTER (WHERE r.is_observed) AS observed_niin_count,
                           SUM(r.observed_value) AS observed_dla_procurement_value_usd
                    FROM relationships r
                    LEFT JOIN locations l USING (cage)
                    WHERE (r.is_active OR r.is_observed)
                      AND UPPER(TRIM(l.state)) IN (SELECT UNNEST(?))
                      AND (l.latitude IS NULL OR (
                          l.latitude BETWEEN 18 AND 72
                          AND l.longitude BETWEEN -180 AND -60
                      ))
                    GROUP BY r.cage
                    """,
                    [
                        str(self.paths["suppliers"]),
                        str(self.paths["references"]),
                        ecosystem_fsc_codes,
                        str(self.paths["locations"]),
                        US_STATE_CODES,
                    ],
                )
            )
            commercial_ecosystem_rows = [
                row
                for row in ecosystem_rows
                if not NON_COMMERCIAL_NAME_PATTERN.search(
                    str(row.get("supplier_name") or "")
                )
            ]
            broader_ecosystem = {
                "definition": (
                    "All active-authorized or observed US supplier-site relationships in "
                    "the stated broader FSC classes. This is an adjacent ecosystem, not "
                    "a pure estimate of the narrower capability market."
                ),
                "fsc_codes": ecosystem_fsc_codes,
                "supplier_sites": len(commercial_ecosystem_rows),
                "classified_niins": classified_ecosystem_niins,
                "active_or_observed_relationship_niins": len(
                    {
                        niin
                        for row in commercial_ecosystem_rows
                        for niin in row.get("niins", [])
                    }
                ),
                "sites_with_active_authorized_items": sum(
                    int(row.get("active_niin_count") or 0) > 0
                    for row in commercial_ecosystem_rows
                ),
                "sites_with_observed_procurement": sum(
                    int(row.get("observed_niin_count") or 0) > 0
                    for row in commercial_ecosystem_rows
                ),
                "observed_dla_procurement_value_usd": sum(
                    float(row.get("observed_dla_procurement_value_usd") or 0)
                    for row in commercial_ecosystem_rows
                ),
            }
        return {
            "context_type": "capability_supplier_market",
            "scope": {
                "capability_id": capability_id,
                "display_name": definition["display_name"],
                "evidence_boundary": {
                    "classification_complete": "All records in the stated product classifications",
                    "classification_complete_sparse_items": "All records in the stated product classifications, with limited item-procurement coverage",
                    "description_bounded": "Description-confirmed records within the stated product classifications",
                    "hybrid": "Complete core classifications with description-confirmed adjacent records",
                }[definition["evidence_mode"]],
                "observation_window": "FY2021-FY2026 observed procurement; current DLA source references",
                "definition": definition["scope_note"],
                "matched_product_classifications": classification_matches,
                "matched_market_segments": market_segments,
                "included_lanes": definition.get("included_lanes", []),
            },
            "supplier_sites": commercial_rows,
            "prime_award_sites": prime_award_sites,
            "annual_prime_activity": annual_prime_activity,
            "top_platform_activity": top_platform_activity,
            "platform_breadth": summarize_platform_breadth(top_platform_activity),
            "broader_ecosystem": broader_ecosystem,
            "coverage": {
                "commercial_supplier_sites": len(commercial_rows),
                "supplier_sites_with_active_authorized_items": sum(
                    row["active_authorized_niin_count"] > 0 for row in commercial_rows
                ),
                "supplier_sites_with_observed_procurement": sum(
                    row["observed_procurement_niin_count"] > 0 for row in commercial_rows
                ),
                "matching_niins": matching_niins,
                "prime_award_sites": len(prime_award_sites),
                "observed_dla_procurement_value_usd": sum(
                    float(row.get("observed_dla_procurement_value_usd") or 0)
                    for row in commercial_rows
                ),
                "directly_classified_prime_obligations_usd": sum(
                    float(row.get("directly_classified_prime_obligations_usd") or 0)
                    for row in annual_prime_activity
                ),
                "broader_related_award_obligations_usd": sum(
                    float(row.get("broader_related_award_obligations_usd") or 0)
                    for row in annual_prime_activity
                ),
            },
            "ranking_basis": (
                "CAGE sites are ordered by active authorized-source NIIN count, then observed "
                "procurement NIIN count and total matching NIIN count. Financial values do not "
                "determine the ranking."
            ),
        }


def build_precomputed_capabilities(data_root: Path, output_dir: Path) -> Dict[str, Any]:
    """Materialize governed capability evidence for fast, release-bound retrieval."""
    output_dir = output_dir.resolve()
    staging_dir = output_dir.parent / f".{output_dir.name}.building"
    backup_dir = output_dir.parent / f".{output_dir.name}.previous"
    shutil.rmtree(staging_dir, ignore_errors=True)
    staging_dir.mkdir(parents=True, exist_ok=True)
    ontology_body = DEFAULT_ONTOLOGY_PATH.read_bytes()
    (staging_dir / "ontology.json").write_bytes(ontology_body)
    store = CapabilityDiscoveryStore(data_root, load_precomputed=False)
    entries = []
    for capability_id in sorted(CAPABILITY_DEFINITIONS):
        pack = store.get(capability_id, limit=50)
        path = staging_dir / f"{capability_id}.json"
        path.write_text(json.dumps(pack, indent=2, default=str))
        entries.append(
            {
                "capability_id": capability_id,
                "display_name": CAPABILITY_DEFINITIONS[capability_id]["display_name"],
                "evidence_mode": CAPABILITY_DEFINITIONS[capability_id]["evidence_mode"],
                "path": path.name,
                "commercial_supplier_sites": pack["coverage"]["commercial_supplier_sites"],
                "matching_niins": pack["coverage"]["matching_niins"],
            }
        )
    store.connection.close()
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ontology_schema_version": CAPABILITY_ONTOLOGY["schema_version"],
        "ontology_sha256": hashlib.sha256(ontology_body).hexdigest(),
        "capabilities": entries,
    }
    (staging_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    shutil.rmtree(backup_dir, ignore_errors=True)
    if output_dir.exists():
        output_dir.rename(backup_dir)
    try:
        staging_dir.rename(output_dir)
    except Exception:
        if backup_dir.exists() and not output_dir.exists():
            backup_dir.rename(output_dir)
        raise
    shutil.rmtree(backup_dir, ignore_errors=True)
    return manifest
