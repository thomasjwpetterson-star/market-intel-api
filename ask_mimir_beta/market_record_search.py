"""Deterministic market-wide opportunity and award search for Ask Mimir."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import quote, unquote

import duckdb


DEFAULT_DATA_ROOT = Path(
    "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/local_data"
)

SEARCH_STOPWORDS = {
    "and", "or", "the", "a", "an", "for", "to", "of", "in", "with", "that",
    "us", "u", "s", "military", "defense", "defence", "current", "currently",
    "open", "recent", "related", "relevant", "companies", "company", "supplying",
    "supply", "manufacturer", "manufacturers", "platform", "platforms", "system",
    "systems", "equipment", "component", "components",
}

TERM_EXPANSIONS = {
    "avionic": ["AVIONIC", "COCKPIT", "FLIGHT CONTROL", "MISSION COMPUTER"],
    "unmanned": ["UNMANNED", "UNCREWED", "UAS", "UAV"],
    "aircraft": ["AIRCRAFT", "AVIATION", "AIRBORNE"],
    "vehicle": ["VEHICLE", "GROUND COMBAT", "TACTICAL VEHICLE"],
    "radar": ["RADAR", "RF SENSOR"],
    "missile": ["MISSILE", "INTERCEPTOR"],
}

CONTEXT_ONLY_TERMS = {"aircraft", "vehicle", "platform"}


def _rows(cursor: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _search_terms(phrase: str) -> List[str]:
    clean = re.sub(r"[^a-z0-9]+", " ", str(phrase or "").lower())
    normalized_tokens = []
    for token in clean.split():
        singular = token[:-1] if token.endswith("s") and len(token) > 4 else token
        if singular not in SEARCH_STOPWORDS and len(singular) >= 3:
            normalized_tokens.append(singular)
    has_specific_capability = any(
        token in TERM_EXPANSIONS and token not in CONTEXT_ONLY_TERMS
        for token in normalized_tokens
    )
    terms: List[str] = []
    for singular in normalized_tokens:
        if has_specific_capability and singular in CONTEXT_ONLY_TERMS:
            continue
        terms.extend(TERM_EXPANSIONS.get(singular, [singular.upper()]))
    return list(dict.fromkeys(terms))[:16]


def _subject(text: str) -> str:
    patterns = (
        r"relevant\s+to\s*:?\s*(?:companies\s+supplying\s+)?(.+?)(?:\?|\.|$)",
        r"related\s+to\s+(.+?)(?:\?|\.|$)",
        r"(?:opportunities|notices|sources?\s+sought|rfis?|requests?\s+for\s+information|solicitations)\s+(?:for|about|cover(?:ing)?|concern(?:ing)?)\s+(.+?)(?:\?|\.|$)",
        r"opportunities\s+(?:are\s+)?open\s+for\s+(.+?)(?:\?|\.|$)",
        r"(?:search|show|find)\s+(?:current|open|active|recent)?\s*(.+?)\s+(?:contracting\s+)?opportunities(?:\?|\.|$)",
        r"awards?\s+for\s+(.+?)(?:\?|\.|$)",
        r"awards?\s+(?:related\s+to|concerning)\s+(.+?)(?:\?|\.|$)",
        r"(?:significant|recent)\s+(.+?)\s+awards?(?:\?|\.|$)",
    )
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            subject = re.sub(r"\s+", " ", match.group(1)).strip()
            subject = re.sub(r"\s+(?:manufacturers?|work)$", "", subject, flags=re.IGNORECASE)
            return subject
    return ""


def resolve_market_record_search(text: str) -> Dict[str, Any] | None:
    clean = str(text or "").strip()
    lowered = clean.lower()
    has_search = any(term in lowered for term in ("find", "show", "which", "search")) or bool(
        re.search(r"\bwhat\s+(?:defen[sc]e\s+)?opportunities\s+are\s+open\b", lowered)
    )
    if not has_search:
        return None
    is_opportunity = any(
        term in lowered
        for term in ("opportunit", "sources sought", "source sought", "rfi", "request for information", "requests for information", "solicitation", "notice")
    )
    is_award = "award" in lowered and any(term in lowered for term in ("contract", "recent", "find", "show"))
    if not is_opportunity and not is_award:
        return None
    notice_type = "ANY"
    if "sources sought" in lowered or "source sought" in lowered:
        notice_type = "SOURCES_SOUGHT"
    elif re.search(r"\brfis?\b|request for information", lowered):
        notice_type = "RFI"
    elif "solicitation" in lowered:
        notice_type = "SOLICITATION"
    subject = _subject(clean)
    if not subject:
        return None
    return {
        "record_type": "opportunity" if is_opportunity else "award",
        "notice_type": notice_type,
        "subject": subject,
        "terms": _search_terms(subject),
    }


def record_search_scope_id(spec: Dict[str, Any]) -> str:
    kind = "OPP" if spec["record_type"] == "opportunity" else "AWARD"
    return f"{kind}|{spec.get('notice_type', 'ANY')}|{quote(spec['subject'].lower(), safe='-_')}"


def record_search_from_scope_id(scope_id: str) -> Dict[str, Any] | None:
    parts = str(scope_id or "").split("|", 2)
    if len(parts) != 3 or parts[0] not in {"OPP", "AWARD"}:
        return None
    subject = unquote(parts[2])
    return {
        "record_type": "opportunity" if parts[0] == "OPP" else "award",
        "notice_type": parts[1],
        "subject": subject,
        "terms": _search_terms(subject),
    }


def record_search_follow_up_intent(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(
        phrase in lowered
        for phrase in (
            "these opportunities", "those opportunities", "these awards", "those awards",
            "which opportunities", "which awards", "most relevant", "most significant",
            "five opportunities", "three awards", "third one", "second one", "first one",
            "investigate first", "what was purchased", "programs they support",
            "response deadline", "customer and requirement", "show me the evidence",
            "supporting that conclusion", "directly evidenced", "which are inferred",
            "which of these", "which of those", "actual industry capability",
            "broad market research", "most relevant notices",
        )
    )


class MarketRecordSearchStore:
    def __init__(self, data_root: Path = DEFAULT_DATA_ROOT) -> None:
        self.data_root = data_root.resolve()
        self.paths = {
            "opportunities": self.data_root / "opportunities.parquet",
            "contracts": self.data_root / "contracts_rolled.parquet",
            "classifications": self.data_root / "classification_reference.parquet",
            "locations": self.data_root / "cage_locations.parquet",
        }
        missing = [str(path) for path in self.paths.values() if not path.exists()]
        if missing:
            raise FileNotFoundError(f"market-record search sources are missing: {missing}")
        self.connection = duckdb.connect()
        self.connection.execute("SET preserve_insertion_order=false")
        self.connection.execute("SET threads=2")
        self.connection.execute("SET memory_limit='1GB'")
        self._cache: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _pattern(terms: List[str]) -> str:
        patterns = []
        for term in terms:
            escaped = re.escape(term)
            patterns.append(rf"\b{escaped}\b" if term.isalnum() and len(term) <= 4 else escaped)
        return "|".join(patterns) or r"a^"

    def get(self, spec: Dict[str, Any], limit: int = 50) -> Dict[str, Any]:
        scope_id = record_search_scope_id(spec)
        if scope_id not in self._cache:
            self._cache[scope_id] = self._build(spec, scope_id)
        bounded = min(max(int(limit), 1), 100)
        pack = self._cache[scope_id]
        records = pack["records"][:bounded]
        return {
            **pack,
            "records": records,
            "coverage": {**pack["coverage"], "records_returned": len(records)},
        }

    def _build(self, spec: Dict[str, Any], scope_id: str) -> Dict[str, Any]:
        pattern = self._pattern(spec.get("terms", []))
        if spec["record_type"] == "opportunity":
            notice_type = spec.get("notice_type", "ANY")
            notice_clause = {
                "SOURCES_SOUGHT": "REGEXP_MATCHES(UPPER(COALESCE(o.search_text, '')), 'SOURCES SOUGHT')",
                "RFI": "REGEXP_MATCHES(UPPER(COALESCE(o.search_text, '')), 'REQUEST FOR INFORMATION|\\bRFI\\b')",
                "SOLICITATION": "NOT REGEXP_MATCHES(UPPER(COALESCE(o.search_text, '')), 'SOURCES SOUGHT|REQUEST FOR INFORMATION|\\bRFI\\b')",
                "ANY": "TRUE",
            }[notice_type]
            records = _rows(
                self.connection.execute(
                    f"""
                    SELECT COALESCE(NULLIF(o.sol_num, ''), o.id) AS record_id,
                           o.title, o.agency AS customer, o.sub_agency,
                           SUBSTR(o.deadline, 1, 10) AS response_deadline,
                           o.psc, c.description AS psc_description,
                           CAST(o.naics AS VARCHAR) AS naics, o.set_aside_type,
                           o.state, o.url, o.description,
                           CASE WHEN REGEXP_MATCHES(UPPER(COALESCE(o.title, '')), ?) THEN 4 ELSE 0 END
                           + CASE WHEN REGEXP_MATCHES(UPPER(COALESCE(o.description, '')), ?) THEN 2 ELSE 0 END
                           AS relevance_score,
                           COUNT(*) OVER () AS total_available
                    FROM read_parquet(?) o
                    LEFT JOIN read_parquet(?) c
                      ON c.classification_type = 'PSC' AND c.code = o.psc
                    WHERE SUBSTR(COALESCE(o.deadline, ''), 1, 10) >= CAST(CURRENT_DATE AS VARCHAR)
                      AND {notice_clause}
                      AND REGEXP_MATCHES(UPPER(COALESCE(o.search_text, o.title, '')), ?)
                    ORDER BY relevance_score DESC, response_deadline, title
                    LIMIT 250
                    """,
                    [pattern, pattern, str(self.paths["opportunities"]),
                     str(self.paths["classifications"]), pattern],
                )
            )
            window = "Open notices with response deadlines on or after the search date"
        else:
            records = _rows(
                self.connection.execute(
                    """
                    WITH locations AS (
                        SELECT UPPER(TRIM(cage_code)) AS cage, MAX(city) AS city,
                               MAX(state) AS state FROM read_parquet(?) GROUP BY 1
                    )
                    SELECT a.contract_id AS record_id,
                           COALESCE(a.base_award_description, a.description) AS title,
                           a.vendor_name AS recipient, a.vendor_cage,
                           l.city, l.state, a.parent_agency AS customer, a.sub_agency,
                           a.psc, c.description AS psc_description, a.naics_code AS naics,
                           a.platform_family, a.total_spend AS net_prime_obligations_usd,
                           SUBSTR(a.last_action_date, 1, 10) AS latest_action_date,
                           CASE WHEN REGEXP_MATCHES(UPPER(COALESCE(a.base_award_description, '')), ?) THEN 4 ELSE 0 END
                           + CASE WHEN REGEXP_MATCHES(UPPER(COALESCE(a.latest_action_description, a.description, '')), ?) THEN 2 ELSE 0 END
                           AS relevance_score,
                           COUNT(*) OVER () AS total_available
                    FROM read_parquet(?) a
                    LEFT JOIN locations l ON UPPER(TRIM(a.vendor_cage)) = l.cage
                    LEFT JOIN read_parquet(?) c
                      ON c.classification_type = 'PSC' AND c.code = a.psc
                    WHERE a.source_system = 'USA_SPENDING'
                      AND a.year BETWEEN 2025 AND 2026
                      AND REGEXP_MATCHES(UPPER(COALESCE(a.base_award_description, '') || ' ' || COALESCE(a.latest_action_description, a.description, '')), ?)
                    ORDER BY relevance_score DESC, ABS(a.total_spend) DESC, latest_action_date DESC
                    LIMIT 250
                    """,
                    [str(self.paths["locations"]), pattern, pattern,
                     str(self.paths["contracts"]), str(self.paths["classifications"]), pattern],
                )
            )
            window = "FY2025-FY2026 observed contract awards"
        return {
            "context_type": "market_record_search",
            "scope": {
                "scope_id": scope_id,
                "record_type": spec["record_type"],
                "notice_type": spec.get("notice_type", "ANY"),
                "subject": spec["subject"],
                "observation_window": window,
                "search_terms": spec.get("terms", []),
            },
            "records": records,
            "coverage": {
                "matching_records": int(records[0].get("total_available") or 0) if records else 0,
                "records_returned": len(records),
            },
        }
