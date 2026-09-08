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
    r"(?:find|identify|show)\s+(?:us\s+)?(?:manufacturers|suppliers|companies)\s+(?:that\s+)?(?:supply|provide|make|manufacture)\s+(.+?)(?:\s+to|\s+for)\s+(?:the\s+)?(?:us\s+)?military",
    r"(?:which|what)\s+(?:us\s+)?(?:manufacturers|suppliers|companies)\s+(?:supply|provide|make|manufacture)\s+(.+?)(?:\?|$)",
    r"(?:companies|manufacturers|suppliers)\s+(?:supplying|providing|manufacturing|with)\s+(.+?)(?:\s+to|\s+for)\s+(?:military|defen[cs]e)",
)
CAPABILITY_STOPWORDS = {
    "and", "or", "the", "a", "an", "for", "to", "of", "into", "with",
    "military", "defense", "defence", "platform", "platforms", "systems",
    "system", "equipment", "components", "component", "products", "product",
}
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
        self._cache: Dict[str, Dict[str, Any]] = {}
        configured_dir = precomputed_dir or Path(
            os.getenv("ASK_MIMIR_CAPABILITY_DIR", str(DEFAULT_PRECOMPUTED_DIR))
        )
        self.precomputed_dir = configured_dir.resolve() if load_precomputed else None

    def search(self, query: str) -> Dict[str, Any]:
        capability_id = resolve_capability(query)
        return {
            "query": str(query or "").strip(),
            "resolved_capability_id": capability_id,
            "resolved_capability_name": (
                CAPABILITY_DEFINITIONS[capability_id]["display_name"]
                if capability_id
                else None
            ),
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
        minimum_matches = min(2, len(terms))
        code_rows = _rows(
            self.connection.execute(
                """
                SELECT code, description, term_matches
                FROM (
                    SELECT code, description,
                       LIST_SUM(LIST_TRANSFORM(?, term ->
                           CASE WHEN CONTAINS(LOWER(description), term) THEN 1 ELSE 0 END
                       )) AS term_matches
                    FROM read_parquet(?)
                    WHERE classification_type = 'PSC'
                      AND REGEXP_MATCHES(code, '^[A-Z0-9]{4}$')
                )
                WHERE term_matches >= ?
                ORDER BY term_matches DESC, code
                LIMIT 20
                """,
                [terms, str(self.paths["classifications"]), minimum_matches],
            )
        )
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
        }

    def _build(self, capability_id: str, definition: Dict[str, Any]) -> Dict[str, Any]:
        fsc_codes = definition["fsc_codes"]
        classification_matches = definition.get("classification_matches", [])
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
        use_codes = bool(fsc_codes)
        is_dynamic = bool(definition.get("dynamic"))
        term_stems = definition.get("term_stems", [])
        minimum_term_matches = 1 if use_codes else definition.get("minimum_term_matches", 1)
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
                    WHERE (NOT ? OR fsc_code IN (SELECT UNNEST(?)))
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
                ORDER BY mapped_platform_niin_count DESC,
                         active_authorized_niin_count DESC,
                         observed_procurement_niin_count DESC,
                         matching_niin_count DESC
                """,
                [
                    str(self.paths["references"]),
                    use_codes,
                    fsc_codes,
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
                WHERE (NOT ? OR fsc_code IN (SELECT UNNEST(?)))
                  AND ((NOT ? AND REGEXP_MATCHES(UPPER(COALESCE(description, '')), ?))
                       OR (? AND LIST_SUM(LIST_TRANSFORM(?, term ->
                           CASE WHEN CONTAINS(LOWER(COALESCE(description, '')), term)
                                THEN 1 ELSE 0 END
                       )) >= ?))
                  AND cage IS NOT NULL AND TRIM(cage) <> ''
                """,
                [str(self.paths["references"]), use_codes, fsc_codes, is_dynamic,
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
                       SUM(c.total_spend) AS net_prime_obligations_usd,
                       COUNT(DISTINCT c.award_key) AS prime_award_count,
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
                  AND c.year BETWEEN 2021 AND 2026
                  AND c.vendor_cage IS NOT NULL
                  AND UPPER(TRIM(l.state)) IN (SELECT UNNEST(?))
                  AND (NOT ? OR c.psc IN (SELECT UNNEST(?)))
                  AND ((NOT ? AND REGEXP_MATCHES(UPPER(COALESCE(
                           c.base_award_description, c.description, ''
                       )), ?)) OR (? AND LIST_SUM(LIST_TRANSFORM(?, term ->
                           CASE WHEN CONTAINS(LOWER(COALESCE(
                               c.base_award_description, c.description, ''
                           )), term) THEN 1 ELSE 0 END
                       )) >= ?))
                GROUP BY 1
                ORDER BY ABS(net_prime_obligations_usd) DESC
                LIMIT 100
                """,
                [
                    str(self.paths["locations"]), str(self.paths["contracts"]),
                    US_STATE_CODES, use_codes, fsc_codes, is_dynamic,
                    definition["item_pattern"], is_dynamic, term_stems,
                    minimum_term_matches,
                ],
            )
        )
        for index, row in enumerate(commercial_rows, start=1):
            row["rank"] = index
            row["evidence_basis"] = [
                label
                for present, label in (
                    (row["active_authorized_niin_count"] > 0, "DLA authorized-source relationships"),
                    (row["observed_procurement_niin_count"] > 0, "observed DLA procurement"),
                    (bool(row.get("item_evidence")), "NIIN and part-number references"),
                )
                if present
            ]
        return {
            "context_type": "capability_supplier_market",
            "scope": {
                "capability_id": capability_id,
                "display_name": definition["display_name"],
                "observation_window": "FY2021-FY2026 observed procurement; current DLA source references",
                "definition": definition["scope_note"],
                "matched_product_classifications": classification_matches,
            },
            "supplier_sites": commercial_rows,
            "prime_award_sites": prime_award_sites,
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
