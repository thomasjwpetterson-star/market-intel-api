"""Evidence-led market-wide supplier discovery for supported capabilities."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import quote, unquote

import duckdb


DEFAULT_DATA_ROOT = Path(
    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data"
)
DEFAULT_PRECOMPUTED_DIR = Path(__file__).resolve().parent / "validation-output" / "capability-markets"

CAPABILITY_DEFINITIONS = {
    "aircraft_actuation": {
        "display_name": "Military-aircraft actuation and flight-control equipment",
        "request_pattern": r"\b(?:aircraft|aviation|flight[- ]?control)\b.*\bactuat(?:or|ors|ion)\b|\bactuat(?:or|ors|ion)\b.*\b(?:aircraft|aviation|flight[- ]?control)\b",
        "fsc_codes": ["1650", "1680"],
        "item_pattern": r"ACTUAT(?:OR|ORS|ION)|SERVOACTUAT(?:OR|ORS)|FLIGHT[- ,]?CONTROL|CONTROL ASSEMBLY",
        "scope_note": "Aircraft actuation and flight-control equipment identified through FSC 1650 and 1680 with actuation-specific item and award descriptions.",
    },
    "aircraft_braking": {
        "display_name": "Military-aircraft braking systems and components",
        "request_pattern": r"\b(?:aircraft|aviation|military aircraft)\b.*\bbrak(?:e|es|ing)\b|\bbrak(?:e|es|ing)\b.*\b(?:aircraft|aviation)\b",
        "fsc_codes": ["1630"],
        "item_pattern": r"BRAKE|BRAKING|ANTI[- ]?SKID",
        "scope_note": "Aircraft wheel and brake equipment identified through FSC 1630 and matching item descriptions.",
    },
    "military_radar": {
        "display_name": "Military radar systems and components",
        "request_pattern": r"\bradar\s+(?:systems?|equipment|components?|suppliers?|manufacturers?)\b|\b(?:systems?|equipment|components?|suppliers?|manufacturers?)\b.*\bradar\b",
        "fsc_codes": ["5840", "5841"],
        "item_pattern": r"RADAR|AZIMUTH|WAVEGUIDE|ANTENNA|DISPLAY|INDICATOR",
        "scope_note": "Ground, shipboard and airborne radar equipment identified through FSC 5840 and 5841 with radar-related item descriptions.",
    },
    "mission_computing": {
        "display_name": "Military mission computing and rugged computing equipment",
        "request_pattern": r"\bmission\s+comput(?:er|ers|ing)\b|\brugged(?:ized|ised)?\s+(?:mission\s+)?comput(?:er|ers|ing|ing equipment)\b",
        "fsc_codes": ["7010", "7021", "7025", "7042"],
        "item_pattern": r"MISSION COMPUTER|RUGGED(?:IZED|ISED)? COMPUTER|SINGLE[- ]BOARD COMPUTER|COMPUTER,?(?: DIGITAL| FLIGHT| MISSION| NAVIGATION| FIRE CONTROL)|COMPUTER SYSTEM,DIGITAL|COMPUTER SUBASSEMBLY|PROCESSOR,GATEWAY|DATA ACQUISITION UNIT",
        "scope_note": "Mission-computing and rugged-computing equipment identified through relevant product classifications and item descriptions.",
    },
}

DYNAMIC_CAPABILITY_PREFIX = "capability:"
CAPABILITY_QUERY_PATTERNS = (
    r"(?:market,\s*)?capability area(?:\s+or\s+industrial base)?\s*:\s*(.+?)(?:\?|$)",
    r"overview of (?:the\s+)?(?:us\s+)?(?:defen[cs]e\s+)?(?:market|capability area|industrial base)\s*:\s*(.+?)(?:\?|$)",
    r"overview of (?:the\s+)?(.+?)\s+in\s+(?:the\s+)?(?:us\s+)?defen[cs]e market(?:[?.]|$)",
    r"(?:what is happening in|describe|analyse|analyze)\s+(?:the\s+)?(?:us\s+)?(.+?)\s+(?:defen[cs]e\s+)?market(?:[?.]|$)",
    r"(?:find|identify|show)\s+(?:us\s+)?(?:manufacturers|suppliers|companies)\s+(?:that\s+)?(?:supply|provide|make|manufacture)\s+(.+?)(?:\s+to|\s+for)\s+(?:the\s+)?(?:us\s+)?military",
    r"(?:which|what)\s+(?:us\s+)?(?:manufacturers|suppliers|companies)\s+(?:supply|provide|make|manufacture)\s+(.+?)(?:\?|$)",
    r"(?:companies|manufacturers|suppliers)\s+(?:supplying|providing|manufacturing|with)\s+(.+?)(?:\s+to|\s+for)\s+(?:military|defen[cs]e)",
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
    r"UNITED STATES DEPARTMENT|U\.?\s*S\.?\s*(?:ARMY|AIR FORCE|NAVY)|"
    r"DEFENSE LOGISTICS AGENCY|AIR LOGISTICS CENTER|"
    r"JOINT ELECTRONICS TYPE DESIGNATION(?: SYSTEM)?",
    re.IGNORECASE,
)


def _rows(cursor: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def resolve_capability(text: str) -> str | None:
    clean = str(text or "")
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
        self.paths = {
            "references": self.data_root / "nsn_cage_reference.parquet",
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
            if subject_matches < 1:
                continue
            scored_codes.append(
                {
                    **row,
                    "term_matches": subject_matches,
                    "domain_matches": domain_matches,
                }
            )
        contextual_codes = [row for row in scored_codes if row["domain_matches"] > 0]
        if domain_terms and contextual_codes:
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
        minimum_term_matches = 1 if use_scope else definition.get("minimum_term_matches", 1)
        rows = _rows(
            self.connection.execute(
                """
                WITH item_relationships AS (
                    SELECT
                        LPAD(TRIM(niin), 9, '0') AS niin,
                        MAX(nsn) AS nsn,
                        UPPER(TRIM(cage)) AS cage,
                        MAX(vendor_name) AS vendor_name,
                        MAX(description) AS description,
                        LIST_SLICE(
                            LIST_DISTINCT(LIST(part_number) FILTER (
                                WHERE part_number IS NOT NULL AND TRIM(part_number) <> ''
                            )), 1, 8
                        ) AS sample_part_numbers,
                        BOOL_OR(COALESCE(is_active_authorized_source, false))
                            AS is_active_authorized_source,
                        BOOL_OR(COALESCE(has_observed_revenue, false))
                            AS has_observed_procurement,
                        MAX(CASE WHEN COALESCE(has_observed_revenue, false)
                            THEN COALESCE(observed_spend, 0) ELSE 0 END)
                            AS observed_dla_procurement_value_usd,
                        MAX(observed_contract_count) AS observed_contract_count,
                        MIN(first_observed_year) AS first_observed_fiscal_year,
                        MAX(last_observed_year) AS last_observed_fiscal_year,
                        LIST_SLICE(
                            LIST_DISTINCT(LIST(platform_families) FILTER (
                                WHERE platform_families IS NOT NULL
                                  AND TRIM(platform_families) <> ''
                            )), 1, 12
                        ) AS platform_groups
                    FROM read_parquet(?)
                    WHERE (NOT ?
                           OR fsc_code IN (SELECT UNNEST(?))
                           OR UPPER(TRIM(COALESCE(market_segment, ''))) IN (SELECT UNNEST(?)))
                      AND ((NOT ? AND REGEXP_MATCHES(UPPER(COALESCE(description, '')), ?))
                           OR (? AND LIST_SUM(LIST_TRANSFORM(?, term ->
                               CASE WHEN CONTAINS(LOWER(COALESCE(description, '')), term)
                                    THEN 1 ELSE 0 END
                           )) >= ?))
                      AND cage IS NOT NULL AND TRIM(cage) <> ''
                    GROUP BY 1, 3
                ), locations AS (
                    SELECT UPPER(TRIM(cage_code)) AS cage,
                           MAX(vendor_name) AS location_vendor_name,
                           MAX(city) AS city,
                           MAX(state) AS state,
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
                GROUP BY r.cage
                ORDER BY active_authorized_niin_count DESC,
                         observed_procurement_niin_count DESC,
                         matching_niin_count DESC,
                         mapped_platform_niin_count DESC
                """,
                [
                    str(self.paths["references"]),
                    use_scope,
                    fsc_codes,
                    market_segments,
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
                  AND ((NOT ? AND REGEXP_MATCHES(UPPER(COALESCE(description, '')), ?))
                       OR (? AND LIST_SUM(LIST_TRANSFORM(?, term ->
                           CASE WHEN CONTAINS(LOWER(COALESCE(description, '')), term)
                                THEN 1 ELSE 0 END
                       )) >= ?))
                  AND cage IS NOT NULL AND TRIM(cage) <> ''
                """,
                [str(self.paths["references"]), use_scope, fsc_codes, market_segments, is_dynamic,
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
                           MAX(city) AS city, MAX(state) AS state
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
                  AND (NOT ?
                       OR c.psc IN (SELECT UNNEST(?))
                       OR UPPER(TRIM(COALESCE(c.market_segment, ''))) IN (SELECT UNNEST(?)))
                  AND ((NOT ? AND REGEXP_MATCHES(UPPER(COALESCE(
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
                    US_STATE_CODES, use_scope, fsc_codes, market_segments, is_dynamic,
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
                      AND ((NOT ? AND REGEXP_MATCHES(UPPER(COALESCE(
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
                    market_segments, is_dynamic, definition["item_pattern"],
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
                      AND ((NOT ? AND REGEXP_MATCHES(UPPER(COALESCE(description, '')), ?))
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
                    market_segments, is_dynamic, definition["item_pattern"],
                    is_dynamic, term_stems, minimum_term_matches,
                ],
            )
        )
        return {
            "context_type": "capability_supplier_market",
            "scope": {
                "capability_id": capability_id,
                "display_name": definition["display_name"],
                "observation_window": "FY2021-FY2026 observed procurement; current DLA source references",
                "definition": definition["scope_note"],
                "matched_product_classifications": classification_matches,
                "matched_market_segments": market_segments,
            },
            "supplier_sites": commercial_rows,
            "prime_award_sites": prime_award_sites,
            "annual_prime_activity": annual_prime_activity,
            "top_platform_activity": top_platform_activity,
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
    output_dir.mkdir(parents=True, exist_ok=True)
    store = CapabilityDiscoveryStore(data_root, load_precomputed=False)
    entries = []
    for capability_id in sorted(CAPABILITY_DEFINITIONS):
        pack = store.get(capability_id, limit=50)
        path = output_dir / f"{capability_id}.json"
        path.write_text(json.dumps(pack, indent=2, default=str))
        entries.append(
            {
                "capability_id": capability_id,
                "display_name": CAPABILITY_DEFINITIONS[capability_id]["display_name"],
                "path": path.name,
                "commercial_supplier_sites": pack["coverage"]["commercial_supplier_sites"],
                "matching_niins": pack["coverage"]["matching_niins"],
            }
        )
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "capabilities": entries,
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest
