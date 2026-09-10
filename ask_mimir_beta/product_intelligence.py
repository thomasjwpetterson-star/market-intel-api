"""Product-family resolution and public-record evidence retrieval for Ask Mimir."""

from __future__ import annotations

import base64
import json
import os
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import duckdb


ROOT = Path(__file__).resolve().parent
DEFAULT_DATA_ROOT = Path(
    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data"
)
DEFAULT_DEFINITIONS = ROOT / "product_family_definitions.json"
DEFAULT_PRECOMPUTED_DIR = ROOT / "validation-output" / "product-families"


def _rows(cursor: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _alias_pattern(alias: str) -> str:
    pieces = [
        re.escape(piece.upper())
        for piece in re.split(r"\s+", alias.strip())
        if piece
    ]
    body = r"\s*".join(pieces)
    return rf"(?:^|[^A-Z0-9]){body}(?:[^A-Z0-9]|$)"


def load_product_definitions(path: Path = DEFAULT_DEFINITIONS) -> Dict[str, Dict[str, Any]]:
    return json.loads(path.read_text())


def resolve_product_family(text: str) -> str | None:
    clean = str(text or "")
    for product_id, definition in load_product_definitions().items():
        if any(
            re.search(_alias_pattern(alias), clean, re.IGNORECASE)
            for alias in [
                *definition["aliases"],
                *definition.get("candidate_aliases", []),
            ]
        ):
            return product_id
        if (
            re.search(r"\b(?:flight|cockpit voice|incident)\s+record(?:er|ers|ing)\b", clean, re.IGNORECASE)
            and any(term.lower() in clean.lower() for term in definition["company_terms"])
        ):
            return product_id
    return None


def _dynamic_product_id(subject: str) -> str:
    normalized = unicodedata.normalize("NFKD", subject).encode(
        "ascii", "ignore"
    ).decode("ascii")[:120]
    encoded = base64.urlsafe_b64encode(normalized.encode("ascii")).decode("ascii")
    return f"dynamic:{encoded.rstrip('=')}"


def _dynamic_product_subject(product_id: str) -> str | None:
    prefix = "dynamic:"
    if not str(product_id or "").startswith(prefix):
        return None
    encoded = str(product_id)[len(prefix):]
    try:
        padding = "=" * (-len(encoded) % 4)
        subject = base64.urlsafe_b64decode(encoded + padding).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return None
    subject = re.sub(r"\s+", " ", subject).strip(" .,:;-\u2014")
    return subject if 3 <= len(subject) <= 120 else None


def extract_product_subject(text: str) -> str | None:
    """Extract an explicit product-level diligence target without guessing entities."""
    clean = re.sub(r"\s+", " ", str(text or "")).strip()
    if not clean:
        return None
    patterns = (
        r"(?:everything|overview|analysis|diligence|research)\s+(?:about|of|on)\s+"
        r"(.{3,120}?)\s+(?:product\s+line|product\s+family|product\s+portfolio|portfolio)\b",
        r"(?:tell\s+me\s+about|assess|analyse|analyze|research)\s+"
        r"(.{3,120}?)\s+(?:product\s+line|product\s+family|product\s+portfolio|portfolio)\b",
        r"\b(.{3,120}?)\s+(?:product\s+line|product\s+family|product\s+portfolio)\b"
        r"(?:\s+from\s+an?\s+acquisition\s+perspective)?",
    )
    for pattern in patterns:
        match = re.search(pattern, clean, re.IGNORECASE)
        if not match:
            continue
        subject = re.sub(
            r"^(?:the|this|that)\s+", "", match.group(1), flags=re.IGNORECASE
        ).strip(" .,:;-\u2014")
        if 3 <= len(subject) <= 120:
            return subject
    if re.search(r"\b(?:acquisition|commercial)\s+diligence\b", clean, re.IGNORECASE):
        match = re.search(
            r"(?:on|of|for)\s+(.{3,120}?)(?:\s*[-\u2014,:;]|\s+including\b|\?|$)",
            clean,
            re.IGNORECASE,
        )
        if match:
            return match.group(1).strip(" .,:;-\u2014")
    return None


def resolve_product_request(text: str) -> str | None:
    governed = resolve_product_family(text)
    if governed:
        return governed
    subject = extract_product_subject(text)
    return _dynamic_product_id(subject) if subject else None


def _dynamic_aliases(subject: str) -> List[str]:
    aliases = [subject]
    for token in re.findall(r"\b[A-Z]{2,}[A-Z0-9]*(?:[- ]+[A-Z0-9]{2,})+\b", subject):
        if token not in aliases:
            aliases.append(token)
    return aliases


def _name_tokens(value: str) -> List[str]:
    ignored = {
        "CO", "COMPANY", "CORP", "CORPORATION", "INC", "INCORPORATED", "LLC",
        "LIMITED", "LTD", "LP", "THE",
    }
    return [
        token for token in re.findall(r"[A-Z0-9]+", str(value or "").upper())
        if token not in ignored
    ]


def _flexible_phrase_pattern(value: str) -> str:
    pieces = [re.escape(piece) for piece in re.findall(r"[A-Z0-9]+", value.upper())]
    return rf"(?:^|[^A-Z0-9]){'[^A-Z0-9]*'.join(pieces)}(?:[^A-Z0-9]|$)"


def product_follow_up_intent(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(
        phrase in lowered
        for phrase in (
            "these products", "this product", "the product line", "which platforms",
            "which customers", "which sites", "which facilities", "future diligence",
            "acquisition risk", "key risks", "show me the evidence", "supporting records",
            "contracts with these names", "sources sought", "place of performance",
            "contracting location", "who manufactures", "design authority",
        )
    )


class ProductIntelligenceStore:
    """Join product aliases across opportunities, awards, subawards, items and sites."""

    def __init__(
        self,
        data_root: Path = DEFAULT_DATA_ROOT,
        definitions_path: Path = DEFAULT_DEFINITIONS,
        precomputed_dir: Path | None = None,
        *,
        load_precomputed: bool = True,
    ) -> None:
        self.data_root = data_root.resolve()
        self.definitions_path = definitions_path.resolve()
        self.definitions = load_product_definitions(self.definitions_path)
        self.paths = {
            "opportunities": self.data_root / "opportunities.parquet",
            "contracts": self.data_root / "contracts_rolled.parquet",
            "network": self.data_root / "network.parquet",
            "references": self.data_root / "nsn_cage_reference.parquet",
            "suppliers": self.data_root / "nsn_supplier_lookup.parquet",
            "locations": self.data_root / "cage_locations.parquet",
        }
        missing = [str(path) for path in self.paths.values() if not path.exists()]
        if missing:
            raise FileNotFoundError(f"product-intelligence sources are missing: {missing}")
        self.connection = duckdb.connect()
        self.connection.execute("SET preserve_insertion_order=false")
        self.connection.execute("SET threads=2")
        self.connection.execute("SET memory_limit='1GB'")
        duckdb_temp = os.getenv("ASK_MIMIR_DUCKDB_TEMP", "/tmp/ask-mimir-duckdb")
        self.connection.execute("SET temp_directory = ?", [duckdb_temp])
        self._cache: Dict[str, Dict[str, Any]] = {}
        self.precomputed_dir = (
            (precomputed_dir or DEFAULT_PRECOMPUTED_DIR).resolve()
            if load_precomputed
            else None
        )

    def _dynamic_definition(self, subject: str) -> Dict[str, Any]:
        subject_tokens = set(_name_tokens(subject))
        first_token = next(iter(_name_tokens(subject)), "")
        company_name = None
        if first_token:
            candidates = _rows(
                self.connection.execute(
                    """
                    SELECT MAX(vendor_name) AS vendor_name
                    FROM read_parquet(?)
                    WHERE UPPER(COALESCE(vendor_name, '')) LIKE ?
                    GROUP BY UPPER(TRIM(vendor_name))
                    LIMIT 250
                    """,
                    [str(self.paths["locations"]), f"%{first_token}%"],
                )
            )
            valid_names = [
                str(row.get("vendor_name") or "").strip()
                for row in candidates
                if set(_name_tokens(str(row.get("vendor_name") or ""))).issubset(
                    subject_tokens
                )
            ]
            if valid_names:
                company_name = max(valid_names, key=lambda value: len(_name_tokens(value)))

        company_tokens = set(_name_tokens(company_name or ""))
        product_tokens = [
            token for token in _name_tokens(subject)
            if token not in company_tokens
            and token not in {"PRODUCT", "LINE", "FAMILY", "PORTFOLIO"}
        ]
        product_phrase = " ".join(product_tokens) or subject
        aliases = [product_phrase]
        aliases.extend(
            token
            for token in _dynamic_aliases(subject)
            if any(character.isdigit() for character in token)
            and token not in aliases
        )
        return {
            "display_name": subject,
            "manufacturer": company_name,
            "aliases": aliases,
            "candidate_aliases": [],
            "official_sources": [],
            "dynamic_scope": True,
            "company_name": company_name,
        }

    def search(self, query: str) -> Dict[str, Any]:
        product_id = resolve_product_request(query)
        dynamic_subject = _dynamic_product_subject(product_id or "")
        return {
            "query": str(query or "").strip(),
            "resolved_product_id": product_id,
            "resolved_product_name": (
                self.definitions[product_id]["display_name"]
                if product_id in self.definitions
                else dynamic_subject
            ),
        }

    def get(self, product_id: str, limit: int = 100) -> Dict[str, Any]:
        raw_id = str(product_id or "").strip()
        dynamic_subject = _dynamic_product_subject(raw_id)
        clean_id = raw_id if dynamic_subject else raw_id.lower()
        if clean_id not in self.definitions and not dynamic_subject:
            raise KeyError(f"product family was not found: {product_id}")
        if clean_id not in self._cache:
            precomputed_path = (
                self.precomputed_dir / f"{clean_id}.json"
                if self.precomputed_dir is not None
                else None
            )
            if precomputed_path is not None and precomputed_path.exists():
                self._cache[clean_id] = json.loads(precomputed_path.read_text())
            else:
                definition = self.definitions.get(clean_id) or self._dynamic_definition(
                    dynamic_subject or ""
                )
                self._cache[clean_id] = self._build(
                    clean_id, definition
                )
        bounded = min(max(int(limit), 1), 250)
        pack = self._cache[clean_id]
        return {
            **pack,
            "opportunities": pack["opportunities"][:bounded],
            "prime_awards": pack["prime_awards"][:bounded],
            "reported_subawards": pack["reported_subawards"][:bounded],
            "item_references": pack["item_references"][:bounded],
        }

    def _build(self, product_id: str, definition: Dict[str, Any]) -> Dict[str, Any]:
        search_aliases = [
            *definition["aliases"],
            *definition.get("candidate_aliases", []),
        ]
        pattern = "|".join(_alias_pattern(alias) for alias in search_aliases)
        dynamic_scope = bool(definition.get("dynamic_scope"))
        company_pattern = (
            _flexible_phrase_pattern(str(definition["company_name"]))
            if definition.get("company_name")
            else None
        )
        product_pattern = "|".join(
            _flexible_phrase_pattern(alias) for alias in search_aliases
        ) if dynamic_scope else pattern
        prime_company_clause = (
            "AND REGEXP_MATCHES(UPPER(COALESCE(vendor_name, '')), ?)"
            if company_pattern else ""
        )
        network_company_clause = (
            "AND REGEXP_MATCHES(UPPER(COALESCE(prime_name, '') || ' ' || "
            "COALESCE(sub_name, '')), ?)"
            if company_pattern else ""
        )
        item_company_clause = (
            "AND REGEXP_MATCHES(UPPER(COALESCE(vendor_name, '')), ?)"
            if company_pattern else ""
        )
        opportunities = _rows(
            self.connection.execute(
                """
                SELECT id, sol_num, title, agency, sub_agency, deadline, set_aside_type,
                       CAST(naics AS VARCHAR) AS naics, psc, state, url,
                       LEFT(description, 5000) AS description
                FROM read_parquet(?)
                WHERE REGEXP_MATCHES(UPPER(COALESCE(title, '') || ' ' || COALESCE(description, '')), ?)
                ORDER BY TRY_CAST(LEFT(deadline, 10) AS DATE) DESC NULLS LAST
                """,
                [str(self.paths["opportunities"]), product_pattern],
            )
        )
        prime_awards = _rows(
            self.connection.execute(
                f"""
                SELECT contract_id, award_key, vendor_name, vendor_cage,
                       base_award_description, latest_action_description,
                       platform_family, platform_families, psc, parent_agency, sub_agency,
                       city AS contracting_city, state AS contracting_state,
                       country AS contracting_country,
                       place_of_performance_city, place_of_performance_state,
                       place_of_performance_country, total_spend, start_date, last_action_date,
                       obligations_fy2021, obligations_fy2022, obligations_fy2023,
                       obligations_fy2024, obligations_fy2025, obligations_fy2026
                FROM read_parquet(?)
                WHERE REGEXP_MATCHES(UPPER(
                    COALESCE(base_award_description, '') || ' ' ||
                    COALESCE(latest_action_description, '') || ' ' || COALESCE(description, '')
                ), ?)
                {prime_company_clause}
                ORDER BY ABS(total_spend) DESC NULLS LAST
                """,
                [str(self.paths["contracts"]), product_pattern]
                + ([company_pattern] if company_pattern else []),
            )
        )
        reported_subawards = _rows(
            self.connection.execute(
                f"""
                SELECT prime_name, sub_name, prime_cage, sub_cage, contract_id,
                       prime_award_description, description AS subaward_description,
                       action_date, year AS fiscal_year, subaward_value,
                       sub_city, sub_state, sub_country,
                       platform_family, psc
                FROM read_parquet(?)
                WHERE REGEXP_MATCHES(UPPER(
                    COALESCE(prime_award_description, '') || ' ' || COALESCE(description, '')
                ), ?)
                {network_company_clause}
                ORDER BY ABS(COALESCE(subaward_value, subaward_value_raw, 0)) DESC
                """,
                [str(self.paths["network"]), product_pattern]
                + ([company_pattern] if company_pattern else []),
            )
        )
        item_references = _rows(
            self.connection.execute(
                f"""
                WITH matched_references AS (
                    SELECT LPAD(TRIM(niin), 9, '0') AS niin,
                           MAX(nsn) AS nsn,
                           UPPER(TRIM(cage)) AS cage,
                           MAX(vendor_name) AS vendor_name,
                           MAX(description) AS description,
                           LIST_SLICE(LIST_DISTINCT(LIST(part_number) FILTER (
                               WHERE part_number IS NOT NULL AND TRIM(part_number) <> ''
                           )), 1, 20) AS sample_part_numbers,
                           BOOL_OR(COALESCE(is_active_authorized_source, false))
                               AS is_active_authorized_source,
                           LIST_SLICE(LIST_DISTINCT(LIST(platform_families) FILTER (
                               WHERE platform_families IS NOT NULL
                                 AND TRIM(platform_families) <> ''
                           )), 1, 20) AS platform_families
                    FROM read_parquet(?)
                    WHERE REGEXP_MATCHES(UPPER(
                        COALESCE(description, '') || ' ' || COALESCE(part_number, '')
                    ), ?)
                    {item_company_clause}
                    GROUP BY 1, 3
                ), observed_procurement AS (
                    SELECT LPAD(TRIM(niin), 9, '0') AS niin,
                           UPPER(TRIM(cage)) AS cage,
                           MAX(vendor) AS observed_supplier_name,
                           SUM(COALESCE(total_revenue, 0)) AS observed_dla_procurement_value_usd,
                           COUNT(DISTINCT contract_id) AS observed_contract_count,
                           MIN(year) AS first_observed_fiscal_year,
                           MAX(year) AS last_observed_fiscal_year
                    FROM read_parquet(?)
                    WHERE year BETWEEN 2019 AND 2026
                      AND cage IS NOT NULL AND TRIM(cage) <> ''
                    GROUP BY 1, 2
                )
                SELECT r.niin, r.nsn, r.cage,
                       COALESCE(o.observed_supplier_name, r.vendor_name) AS vendor_name,
                       r.description, r.sample_part_numbers,
                       r.is_active_authorized_source,
                       o.cage IS NOT NULL AS has_observed_procurement,
                       COALESCE(o.observed_dla_procurement_value_usd, 0)
                           AS observed_dla_procurement_value_usd,
                       COALESCE(o.observed_contract_count, 0) AS observed_contract_count,
                       o.first_observed_fiscal_year,
                       o.last_observed_fiscal_year,
                       r.platform_families
                FROM matched_references r
                LEFT JOIN observed_procurement o USING (niin, cage)
                ORDER BY COALESCE(o.observed_dla_procurement_value_usd, 0) DESC,
                         r.is_active_authorized_source DESC, r.niin, r.cage
                """,
                [
                    str(self.paths["references"]),
                    product_pattern,
                    *([company_pattern] if company_pattern else []),
                    str(self.paths["suppliers"]),
                ],
            )
        )
        cages = sorted(
            {
                str(row.get(field) or "").strip().upper()
                for rows, fields in (
                    (prime_awards, ("vendor_cage",)),
                    (reported_subawards, ("prime_cage", "sub_cage")),
                    (item_references, ("cage",)),
                )
                for row in rows
                for field in fields
                if re.fullmatch(r"[A-Z0-9]{5}", str(row.get(field) or "").strip().upper())
            }
        )
        sites = _rows(
            self.connection.execute(
                """
                SELECT UPPER(TRIM(cage_code)) AS cage, MAX(vendor_name) AS vendor_name,
                       MAX(city) AS city, MAX(state) AS state,
                       MAX(location_quality) AS location_quality,
                       MAX(entity_source) AS entity_source
                FROM read_parquet(?)
                WHERE UPPER(TRIM(cage_code)) IN (SELECT UNNEST(?))
                GROUP BY 1 ORDER BY vendor_name, cage
                """,
                [str(self.paths["locations"]), cages],
            )
        ) if cages else []
        performance_locations = sorted(
            {
                (
                    str(row.get("place_of_performance_city") or "").strip(),
                    str(row.get("place_of_performance_state") or "").strip(),
                    str(row.get("place_of_performance_country") or "").strip(),
                )
                for row in prime_awards
                if str(row.get("place_of_performance_city") or "").strip()
            }
        )
        evidence_text = " ".join(
            str(value or "")
            for row in [*opportunities, *prime_awards]
            for value in row.values()
        ).upper()
        signal_rules = {
            "sole_source_or_source_control": r"SOLE SOURCE|ONLY ONE RESPONSIBLE SOURCE|PROPRIETARY",
            "modernization_or_obsolescence": r"MID[- ]LIFE UPGRADE|MUP|OBSOLESCENCE|REDESIGN",
            "oem_or_design_authority_dependency": r"ORIGINAL EQUIPMENT MANUFACTURER|\bOEM\b|TECHNICAL DATA",
            "qualification_or_certification_dependency": r"QUALIFICATION|CERTIF|AIRWORTHINESS",
        }
        diligence_signals = [
            {"signal": label, "observed_in_public_records": True}
            for label, rule in signal_rules.items()
            if re.search(rule, evidence_text)
        ]
        return {
            "context_type": "product_family_dossier",
            "scope": {
                "product_id": product_id,
                "display_name": definition["display_name"],
                "manufacturer": definition["manufacturer"],
                "aliases": definition["aliases"],
                "candidate_aliases_requiring_validation": definition.get(
                    "candidate_aliases", []
                ),
                "scope_status": (
                    "request_defined_product_scope"
                    if definition.get("dynamic_scope")
                    else "governed_product_family"
                ),
                "observation_window": "FY2019-FY2026 public award evidence and current opportunity records",
            },
            "official_sources": definition.get("official_sources", []),
            "opportunities": opportunities,
            "prime_awards": prime_awards,
            "reported_subawards": reported_subawards,
            "item_references": item_references,
            "sites": sites,
            "place_of_performance_locations": [
                {"city": city, "state": state, "country": country}
                for city, state, country in performance_locations
            ],
            "diligence_signals": diligence_signals,
            "coverage": {
                "opportunity_records": len(opportunities),
                "prime_award_records": len(prime_awards),
                "reported_subaward_records": len(reported_subawards),
                "item_reference_records": len(item_references),
                "resolved_cage_sites": len(sites),
            },
        }


def build_precomputed_product_families(
    data_root: Path,
    output_dir: Path,
    definitions_path: Path = DEFAULT_DEFINITIONS,
) -> Dict[str, Any]:
    """Materialize product-family dossiers for an atomic Ask Mimir release."""
    output_dir.mkdir(parents=True, exist_ok=True)
    store = ProductIntelligenceStore(
        data_root,
        definitions_path=definitions_path,
        load_precomputed=False,
    )
    entries = []
    for product_id, definition in store.definitions.items():
        pack = store.get(product_id, limit=250)
        path = output_dir / f"{product_id}.json"
        path.write_text(json.dumps(pack, indent=2, default=str))
        entries.append(
            {
                "product_id": product_id,
                "display_name": definition["display_name"],
                "path": path.name,
                "coverage": pack["coverage"],
            }
        )
    store.connection.close()
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "product_families": entries,
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest
