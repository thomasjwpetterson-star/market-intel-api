"""Server-side controls for the isolated Ask Mimir beta."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable
from urllib.parse import parse_qs, urlparse


@dataclass(frozen=True)
class TierPolicy:
    tier: str
    display_name: str
    queries_per_utc_day: int
    can_download_evidence: bool


TIER_POLICIES: Dict[str, TierPolicy] = {
    "public": TierPolicy("public", "Guest access", 1, False),
    "free": TierPolicy("free", "Free", 3, False),
    "trial": TierPolicy("trial", "Trial", 20, False),
    "lite": TierPolicy("lite", "Lite", 20, False),
    "professional": TierPolicy("professional", "Professional", 75, True),
    "enterprise": TierPolicy("enterprise", "Enterprise", 200, True),
}


def normalize_tier(value: str | None) -> str:
    candidate = str(value or "public").strip().lower()
    aliases = {"paid": "professional", "pro": "professional", "anonymous": "public"}
    candidate = aliases.get(candidate, candidate)
    return candidate if candidate in TIER_POLICIES else "public"


@dataclass(frozen=True)
class AccessContext:
    subject_id: str
    tier: str
    authenticated: bool

    @property
    def policy(self) -> TierPolicy:
        return TIER_POLICIES[normalize_tier(self.tier)]

    def public_dict(self, used_today: int = 0) -> Dict[str, Any]:
        policy = self.policy
        return {
            **asdict(policy),
            "authenticated": self.authenticated,
            "queries_used_today": used_today,
            "queries_remaining_today": max(policy.queries_per_utc_day - used_today, 0),
            "resets_at": next_utc_midnight_iso(),
        }


def utc_day() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def next_utc_midnight_iso() -> str:
    now = datetime.now(timezone.utc)
    tomorrow = now.date() + timedelta(days=1)
    return datetime.combine(tomorrow, datetime_time.min, tzinfo=timezone.utc).isoformat()


class DailyQuotaExceeded(RuntimeError):
    def __init__(self, policy: TierPolicy) -> None:
        self.policy = policy
        super().__init__(
            f"The {policy.display_name} allowance of {policy.queries_per_utc_day} "
            "Ask Mimir queries per UTC day has been used."
        )


class BetaStateStore:
    """Small SQLite ledger for quota enforcement, jobs and answer feedback."""

    def __init__(self, path: Path) -> None:
        self.path = path.resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.connection = sqlite3.connect(self.path, check_same_thread=False)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS query_events (
                request_id TEXT PRIMARY KEY,
                subject_id TEXT NOT NULL,
                tier TEXT NOT NULL,
                utc_day TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                release_binding_id TEXT,
                workflow TEXT,
                latency_ms REAL,
                estimated_cost_usd REAL
            );
            CREATE INDEX IF NOT EXISTS query_events_subject_day
                ON query_events(subject_id, utc_day, status);
            CREATE TABLE IF NOT EXISTS answer_feedback (
                feedback_id TEXT PRIMARY KEY,
                response_id TEXT NOT NULL,
                request_id TEXT,
                subject_id TEXT NOT NULL,
                rating TEXT NOT NULL,
                reason TEXT,
                created_at TEXT NOT NULL,
                release_binding_id TEXT
            );
            """
        )
        self.connection.commit()

    def used_today(self, subject_id: str) -> int:
        with self.lock:
            row = self.connection.execute(
                """
                SELECT COUNT(*) FROM query_events
                WHERE subject_id = ? AND utc_day = ?
                  AND status IN ('reserved', 'running', 'completed', 'failed')
                """,
                [subject_id, utc_day()],
            ).fetchone()
        return int(row[0])

    def reserve(
        self,
        request_id: str,
        access: AccessContext,
        release_binding_id: str,
        workflow: str,
    ) -> int:
        policy = access.policy
        now = datetime.now(timezone.utc).isoformat()
        with self.lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                used = int(
                    self.connection.execute(
                        """
                        SELECT COUNT(*) FROM query_events
                        WHERE subject_id = ? AND utc_day = ?
                          AND status IN ('reserved', 'running', 'completed', 'failed')
                        """,
                        [access.subject_id, utc_day()],
                    ).fetchone()[0]
                )
                if used >= policy.queries_per_utc_day:
                    raise DailyQuotaExceeded(policy)
                self.connection.execute(
                    """
                    INSERT INTO query_events (
                        request_id, subject_id, tier, utc_day, status, created_at,
                        release_binding_id, workflow
                    ) VALUES (?, ?, ?, ?, 'reserved', ?, ?, ?)
                    """,
                    [
                        request_id,
                        access.subject_id,
                        policy.tier,
                        utc_day(),
                        now,
                        release_binding_id,
                        workflow,
                    ],
                )
                self.connection.commit()
                return used + 1
            except Exception:
                self.connection.rollback()
                raise

    def mark_running(self, request_id: str) -> None:
        self._set_status(request_id, "running")

    def complete(
        self,
        request_id: str,
        *,
        latency_ms: float | None,
        estimated_cost_usd: float | None,
    ) -> None:
        with self.lock:
            self.connection.execute(
                """
                UPDATE query_events
                SET status='completed', completed_at=?, latency_ms=?, estimated_cost_usd=?
                WHERE request_id=?
                """,
                [
                    datetime.now(timezone.utc).isoformat(),
                    latency_ms,
                    estimated_cost_usd,
                    request_id,
                ],
            )
            self.connection.commit()

    def fail(self, request_id: str, *, refund: bool = True) -> None:
        self._set_status(request_id, "failed_refunded" if refund else "failed")

    def _set_status(self, request_id: str, status: str) -> None:
        with self.lock:
            self.connection.execute(
                "UPDATE query_events SET status=? WHERE request_id=?",
                [status, request_id],
            )
            self.connection.commit()

    def add_feedback(
        self,
        *,
        feedback_id: str,
        response_id: str,
        request_id: str | None,
        access: AccessContext,
        rating: str,
        reason: str | None,
        release_binding_id: str,
    ) -> None:
        with self.lock:
            self.connection.execute(
                """
                INSERT INTO answer_feedback (
                    feedback_id, response_id, request_id, subject_id, rating,
                    reason, created_at, release_binding_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    feedback_id,
                    response_id,
                    request_id,
                    access.subject_id,
                    rating,
                    reason,
                    datetime.now(timezone.utc).isoformat(),
                    release_binding_id,
                ],
            )
            self.connection.commit()


class EvidencePackCache:
    """Release-bound JSON cache for deterministic evidence packs."""

    def __init__(self, directory: Path, ttl_seconds: int = 86400) -> None:
        self.directory = directory.resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.ttl_seconds = max(int(ttl_seconds), 60)
        self.lock = threading.Lock()
        self.memory: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def cache_key(release_binding_id: str, name: str, arguments: Dict[str, Any]) -> str:
        payload = json.dumps(
            [release_binding_id, name, arguments], sort_keys=True, default=str
        ).encode()
        return hashlib.sha256(payload).hexdigest()

    def get(self, key: str) -> Dict[str, Any] | None:
        now = time.time()
        with self.lock:
            entry = self.memory.get(key)
            if entry and now - float(entry["cached_at_epoch"]) <= self.ttl_seconds:
                return entry["value"]
            path = self.directory / f"{key}.json"
            if not path.exists() or now - path.stat().st_mtime > self.ttl_seconds:
                return None
            try:
                value = json.loads(path.read_text())
            except (OSError, json.JSONDecodeError):
                return None
            self.memory[key] = {"cached_at_epoch": now, "value": value}
            return value

    def set(self, key: str, value: Dict[str, Any]) -> None:
        encoded = json.dumps(value, default=str, separators=(",", ":"))
        temp = self.directory / f".{key}.{os.getpid()}.tmp"
        target = self.directory / f"{key}.json"
        with self.lock:
            temp.write_text(encoded)
            os.replace(temp, target)
            self.memory[key] = {"cached_at_epoch": time.time(), "value": value}


class DataReleaseGuard:
    """Prevent a running beta process from silently mixing changed source files."""

    def __init__(self, metric_release_id: str, paths: Iterable[Path]) -> None:
        self.metric_release_id = metric_release_id
        self.paths = sorted({Path(path).resolve() for path in paths if Path(path).exists()})
        self.snapshot = self._snapshot()
        payload = json.dumps(
            [metric_release_id, self.snapshot], sort_keys=True, default=str
        ).encode()
        self.release_binding_id = hashlib.sha256(payload).hexdigest()[:20]

    def _snapshot(self) -> list[Dict[str, Any]]:
        return [
            {
                "path": str(path),
                "size": path.stat().st_size,
                "mtime_ns": path.stat().st_mtime_ns,
            }
            for path in self.paths
        ]

    def assert_unchanged(self) -> None:
        if self._snapshot() != self.snapshot:
            raise RuntimeError(
                "The active Ask Mimir source files changed while the service was running. "
                "Restart the service so the next answer uses one consistent data release."
            )


MARKDOWN_LINK = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
MARKDOWN_LINK_WITH_LABEL = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
FORBIDDEN_ANSWER_MARKERS = (
    "source_report_id",
    "source_dedup_key",
    "transaction_key",
    "internal_value_treatment",
    "/users/",
    "local_data/",
    "s3://",
)

CUSTOMER_HIDDEN_KEYS = {
    "source_report_id",
    "source_report_ids",
    "source_dedup_key",
    "transaction_key",
    "award_key",
    "internal_value_treatment",
    "included_in_adjusted_total",
    "source_snapshot_sha256",
    "evidence_fingerprint",
    "source_locator",
    "source_file",
    "ingestion_date",
}


def sanitize_customer_payload(value: Any) -> Any:
    """Remove internal lineage identifiers from the browser-facing response."""
    if isinstance(value, dict):
        return {
            key: sanitize_customer_payload(child)
            for key, child in value.items()
            if key.lower() not in CUSTOMER_HIDDEN_KEYS
            and not key.lower().startswith("internal_")
        }
    if isinstance(value, list):
        return [sanitize_customer_payload(child) for child in value]
    if isinstance(value, str) and (value.startswith("/Users/") or value.startswith("s3://")):
        return None
    return value


def _trace_urls(value: Any) -> set[str]:
    urls: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            if key in {"url", "canonical_url", "public_record_url", "public_notice_url"}:
                if isinstance(child, str) and child.startswith("https://"):
                    urls.add(child.rstrip("/"))
            urls.update(_trace_urls(child))
    elif isinstance(value, list):
        for child in value:
            urls.update(_trace_urls(child))
    return urls


IDENTIFIER_KEYS = {
    "cage": "cage",
    "cage_code": "cage",
    "vendor_cage": "cage",
    "sub_cage": "cage",
    "prime_cage": "cage",
    "target_cages": "cage",
    "contract_id": "award",
    "award_id": "award",
    "award_id_piid": "award",
    "sample_contract_ids": "award",
    "sol_num": "award",
    "solicitation_id": "award",
    "nsn": "nsn",
    "niin": "nsn",
    "sample_nsns": "nsn",
    "sample_shared_niins": "nsn",
    "platform_id": "platform",
    "platform": "platform",
    "platform_family": "platform",
    "platforms": "platform",
    "platform_families": "platform",
    "platform_universe": "platform",
}


def _add_identifier(bucket: set[str], value: Any) -> None:
    values = value if isinstance(value, list) else [value]
    for item in values:
        if item in (None, ""):
            continue
        for part in re.split(r"[,|]", str(item)):
            clean = part.strip().upper()
            if clean:
                bucket.add(clean)


def _trace_identifiers(value: Any, found: Dict[str, set[str]] | None = None) -> Dict[str, set[str]]:
    result = found or {"cage": set(), "award": set(), "nsn": set(), "platform": set()}
    if isinstance(value, dict):
        for key, child in value.items():
            kind = IDENTIFIER_KEYS.get(key.lower())
            if kind:
                _add_identifier(result[kind], child)
            _trace_identifiers(child, result)
    elif isinstance(value, list):
        for child in value:
            _trace_identifiers(child, result)
    return result


def validate_answer_citations(answer: str, tool_trace: list[Dict[str, Any]]) -> Dict[str, Any]:
    answer_lower = answer.lower()
    forbidden = [marker for marker in FORBIDDEN_ANSWER_MARKERS if marker in answer_lower]
    links = MARKDOWN_LINK.findall(answer)
    supplied_urls = _trace_urls(tool_trace)
    supplied_identifiers = _trace_identifiers(tool_trace)
    unsafe_links = []
    unsupported_mimir_links = []
    linked_to_supplied_evidence = 0
    mimir_links = 0
    external_links = 0
    for raw_url in links:
        url = raw_url.strip().rstrip("/")
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            unsafe_links.append(raw_url)
            continue
        host = (parsed.hostname or "").lower()
        if host in {"localhost", "127.0.0.1"} or host.endswith(".local"):
            unsafe_links.append(raw_url)
        elif host.endswith("mimiradvisors.org"):
            mimir_links += 1
            query = parse_qs(parsed.query)
            for parameter, kind in (
                ("cage", "cage"),
                ("award", "award"),
                ("nsn", "nsn"),
                ("platform", "platform"),
            ):
                linked_values = query.get(parameter, [])
                if linked_values and not any(
                    value.strip().upper() in supplied_identifiers[kind]
                    for value in linked_values
                ):
                    unsupported_mimir_links.append(raw_url)
        else:
            external_links += 1
            if url in supplied_urls:
                linked_to_supplied_evidence += 1
    warnings = []
    if not links:
        warnings.append("The answer contains no clickable source or Mimir drill-down links.")
    if external_links and linked_to_supplied_evidence < external_links:
        warnings.append(
            "Some external links came from live model research rather than the deterministic evidence pack."
        )
    if forbidden:
        warnings.append("The answer contains internal evidence identifiers or storage details.")
    if unsafe_links:
        warnings.append("The answer contains an unsafe or non-public link.")
    if unsupported_mimir_links:
        warnings.append("A Mimir drill-down link is not backed by an identifier in the evidence pack.")
    return {
        "status": (
            "pass"
            if not forbidden and not unsafe_links and not unsupported_mimir_links
            else "fail"
        ),
        "markdown_link_count": len(links),
        "mimir_drilldown_link_count": mimir_links,
        "external_source_link_count": external_links,
        "external_links_in_deterministic_pack": linked_to_supplied_evidence,
        "forbidden_markers": forbidden,
        "unsafe_links": unsafe_links,
        "unsupported_mimir_links": unsupported_mimir_links,
        "warnings": warnings,
    }


def remove_unsupported_mimir_links(answer: str, validation: Dict[str, Any]) -> str:
    """Render an unverified Mimir drill-down as text without discarding the answer."""
    unsupported = {
        str(url).strip().rstrip("/")
        for url in validation.get("unsupported_mimir_links", [])
    }
    if not unsupported:
        return answer

    def replace(match: re.Match[str]) -> str:
        label, raw_url = match.groups()
        return label if raw_url.strip().rstrip("/") in unsupported else match.group(0)

    return MARKDOWN_LINK_WITH_LABEL.sub(replace, answer)
