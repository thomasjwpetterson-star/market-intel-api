"""Server-side controls for the isolated Ask Mimir beta."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
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
    queries_per_utc_month: int
    can_download_evidence: bool
    can_download_report: bool


TIER_POLICIES: Dict[str, TierPolicy] = {
    "public": TierPolicy("public", "Guest access", 1, 10, False, False),
    "free": TierPolicy("free", "Free", 2, 30, False, True),
    "trial": TierPolicy("trial", "Trial", 5, 35, False, True),
    "lite": TierPolicy("lite", "Lite", 5, 100, False, True),
    "professional": TierPolicy("professional", "Professional", 15, 300, True, True),
    "enterprise": TierPolicy("enterprise", "Enterprise", 50, 1000, True, True),
}


CLARIFICATION_OPENING_PATTERNS = (
    r"do you mean\b",
    r"which .{1,120} did you mean\b",
    r"which scope do you mean\b",
    r"could you (?:clarify|specify|choose|confirm)\b",
    r"please (?:clarify|specify|choose|confirm)\b",
    r"before i answer.{0,80}(?:clarify|specify|choose|confirm)\b",
    r"i need (?:a|one) (?:quick |short )?clarification\b",
)

CLARIFICATION_QUESTION_PATTERNS = (
    r"\bare you looking for\b",
    r"\bwhat would you like to (?:analy[sz]e|explore|know|find)\b",
    r"\bwhich (?:company|supplier|site|platform|program|programme|market|category) "
    r"(?:are you asking about|should i use)\b",
    r"\bif so, (?:please )?(?:provide|specify|choose|confirm|enter)\b",
)


def response_requires_clarification(result: Dict[str, Any]) -> bool:
    """Identify a scope question that should not consume an Ask Mimir allowance."""
    if result.get("requires_clarification") is True:
        return True

    answer = re.sub(r"\s+", " ", str(result.get("answer") or "")).strip()
    if not answer or len(answer) > 1_500:
        return False

    answer = re.sub(r"^[#>*_`\-\s]+", "", answer).strip().lower()
    return (
        any(re.match(pattern, answer) for pattern in CLARIFICATION_OPENING_PATTERNS)
        or any(re.search(pattern, answer) for pattern in CLARIFICATION_QUESTION_PATTERNS)
    )


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

    def public_dict(
        self,
        used_today: int = 0,
        used_this_month: int = 0,
    ) -> Dict[str, Any]:
        policy = self.policy
        return {
            **asdict(policy),
            "authenticated": self.authenticated,
            "queries_used_today": used_today,
            "queries_remaining_today": max(policy.queries_per_utc_day - used_today, 0),
            "queries_used_this_month": used_this_month,
            "queries_remaining_this_month": max(
                policy.queries_per_utc_month - used_this_month,
                0,
            ),
            "daily_resets_at": next_utc_midnight_iso(),
            "monthly_resets_at": next_utc_month_iso(),
            # Retained for clients that currently read the daily reset from this key.
            "resets_at": next_utc_midnight_iso(),
        }


def utc_day() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def utc_month() -> str:
    return utc_day()[:7]


def next_utc_midnight_iso() -> str:
    now = datetime.now(timezone.utc)
    tomorrow = now.date() + timedelta(days=1)
    return datetime.combine(tomorrow, datetime_time.min, tzinfo=timezone.utc).isoformat()


def next_utc_month_iso() -> str:
    now = datetime.now(timezone.utc)
    if now.month == 12:
        first_of_next_month = now.date().replace(
            year=now.year + 1,
            month=1,
            day=1,
        )
    else:
        first_of_next_month = now.date().replace(month=now.month + 1, day=1)
    return datetime.combine(
        first_of_next_month,
        datetime_time.min,
        tzinfo=timezone.utc,
    ).isoformat()


class RequestPerformance:
    """Thread-local, customer-hidden timings for one Ask Mimir request."""

    def __init__(self, routing_ms: float = 0.0) -> None:
        self.routing_ms = max(float(routing_ms), 0.0)
        self.totals: Dict[str, float] = {}
        self.counts: Dict[str, int] = {}
        self.operations: list[Dict[str, Any]] = []

    def record(
        self,
        category: str,
        name: str,
        elapsed_ms: float,
        *,
        cache_hit: bool = False,
    ) -> None:
        duration = max(float(elapsed_ms), 0.0)
        self.totals[category] = self.totals.get(category, 0.0) + duration
        self.counts[category] = self.counts.get(category, 0) + 1
        self.operations.append(
            {
                "category": category,
                "name": name,
                "elapsed_ms": round(duration, 1),
                "cache_hit": bool(cache_hit),
            }
        )

    def snapshot(
        self,
        *,
        queue_wait_ms: float = 0.0,
        answer_generation_ms: float,
        validation_ms: float,
        total_request_ms: float,
    ) -> Dict[str, Any]:
        return {
            "routing_ms": round(self.routing_ms, 1),
            "queue_wait_ms": round(max(queue_wait_ms, 0.0), 1),
            "answer_generation_ms": round(max(answer_generation_ms, 0.0), 1),
            "evidence_retrieval_ms": round(
                self.totals.get("evidence_retrieval", 0.0), 1
            ),
            "model_ms": round(self.totals.get("model", 0.0), 1),
            "validation_and_formatting_ms": round(max(validation_ms, 0.0), 1),
            "total_request_ms": round(max(total_request_ms, 0.0), 1),
            "evidence_call_count": self.counts.get("evidence_retrieval", 0),
            "model_call_count": self.counts.get("model", 0),
            "evidence_cache_hit_count": self.counts.get("evidence_cache_hit", 0),
            "operations": list(self.operations),
        }


_REQUEST_PERFORMANCE = threading.local()


@contextmanager
def request_performance_scope(performance: RequestPerformance):
    previous = getattr(_REQUEST_PERFORMANCE, "current", None)
    _REQUEST_PERFORMANCE.current = performance
    try:
        yield performance
    finally:
        _REQUEST_PERFORMANCE.current = previous


def record_request_timing(
    category: str,
    name: str,
    elapsed_ms: float,
    *,
    cache_hit: bool = False,
) -> None:
    performance = getattr(_REQUEST_PERFORMANCE, "current", None)
    if performance is not None:
        performance.record(category, name, elapsed_ms, cache_hit=cache_hit)


class DailyQuotaExceeded(RuntimeError):
    def __init__(self, policy: TierPolicy, period: str = "day") -> None:
        self.policy = policy
        self.period = period
        if period == "month":
            message = (
                f"The {policy.display_name} allowance of "
                f"{policy.queries_per_utc_month} Ask Mimir queries per UTC month "
                "has been used."
            )
        else:
            message = (
                f"The {policy.display_name} allowance of "
                f"{policy.queries_per_utc_day} Ask Mimir queries per UTC day "
                "has been used."
            )
        super().__init__(message)


class DuplicateRequestError(RuntimeError):
    """Raised when an active logical request is submitted more than once."""


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
                request_fingerprint TEXT,
                subject_id TEXT NOT NULL,
                tier TEXT NOT NULL,
                utc_day TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                release_binding_id TEXT,
                workflow TEXT,
                latency_ms REAL,
                estimated_cost_usd REAL,
                performance_json TEXT
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
            CREATE TABLE IF NOT EXISTS ask_conversations (
                conversation_id TEXT PRIMARY KEY,
                subject_id TEXT NOT NULL,
                active_scope_json TEXT,
                last_workflow TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ask_conversations_subject
                ON ask_conversations(subject_id, updated_at);
            CREATE TABLE IF NOT EXISTS routing_events (
                request_id TEXT PRIMARY KEY,
                conversation_id TEXT,
                subject_id TEXT NOT NULL,
                question TEXT NOT NULL,
                intended_workflow TEXT,
                selected_workflow TEXT NOT NULL,
                candidates_json TEXT NOT NULL,
                confidence REAL NOT NULL,
                active_scope_json TEXT,
                resolved_entities_json TEXT NOT NULL,
                subject_changed INTEGER NOT NULL DEFAULT 0,
                clarification_needed INTEGER NOT NULL DEFAULT 0,
                clarification_outcome TEXT,
                user_correction INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                completed_at TEXT
            );
            CREATE INDEX IF NOT EXISTS routing_events_conversation
                ON routing_events(conversation_id, created_at);
            """
        )
        query_columns = {
            str(row[1])
            for row in self.connection.execute("PRAGMA table_info(query_events)")
        }
        if "performance_json" not in query_columns:
            self.connection.execute(
                "ALTER TABLE query_events ADD COLUMN performance_json TEXT"
            )
        if "request_fingerprint" not in query_columns:
            self.connection.execute(
                "ALTER TABLE query_events ADD COLUMN request_fingerprint TEXT"
            )
        self.connection.execute(
            """
            UPDATE query_events
            SET status = 'failed_refunded', completed_at = ?
            WHERE status IN ('reserved', 'running')
            """,
            [datetime.now(timezone.utc).isoformat()],
        )
        self.connection.commit()

    def load_conversation_scope(
        self,
        conversation_id: str | None,
        subject_id: str,
    ) -> Dict[str, Any] | None:
        if not conversation_id:
            return None
        with self.lock:
            row = self.connection.execute(
                """
                SELECT active_scope_json
                FROM ask_conversations
                WHERE conversation_id = ? AND subject_id = ?
                """,
                [conversation_id, subject_id],
            ).fetchone()
        if not row or not row[0]:
            return None
        try:
            value = json.loads(row[0])
        except (TypeError, ValueError):
            return None
        return value if isinstance(value, dict) else None

    def save_conversation_scope(
        self,
        conversation_id: str | None,
        subject_id: str,
        active_scope: Dict[str, Any] | None,
        workflow: str | None,
    ) -> None:
        if not conversation_id:
            return
        now = datetime.now(timezone.utc).isoformat()
        scope_json = json.dumps(active_scope, default=str) if active_scope else None
        with self.lock:
            self.connection.execute(
                """
                INSERT INTO ask_conversations (
                    conversation_id, subject_id, active_scope_json, last_workflow,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(conversation_id) DO UPDATE SET
                    active_scope_json = excluded.active_scope_json,
                    last_workflow = excluded.last_workflow,
                    updated_at = excluded.updated_at
                WHERE ask_conversations.subject_id = excluded.subject_id
                """,
                [conversation_id, subject_id, scope_json, workflow, now, now],
            )
            self.connection.commit()

    def record_routing_decision(
        self,
        *,
        request_id: str,
        conversation_id: str | None,
        subject_id: str,
        question: str,
        decision: Dict[str, Any],
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.lock:
            self.connection.execute(
                """
                INSERT OR REPLACE INTO routing_events (
                    request_id, conversation_id, subject_id, question,
                    intended_workflow, selected_workflow, candidates_json,
                    confidence, active_scope_json, resolved_entities_json,
                    subject_changed, clarification_needed, clarification_outcome,
                    user_correction, created_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, NULL)
                """,
                [
                    request_id,
                    conversation_id,
                    subject_id,
                    question,
                    decision.get("intended_workflow"),
                    decision.get("workflow"),
                    json.dumps(decision.get("candidates") or [], default=str),
                    float(decision.get("confidence") or 0),
                    json.dumps(decision.get("current_scope"), default=str)
                    if decision.get("current_scope")
                    else None,
                    json.dumps(decision.get("resolved_entities") or [], default=str),
                    int(bool(decision.get("subject_changed"))),
                    int(bool(decision.get("clarification_needed"))),
                    int(bool(decision.get("user_correction"))),
                    now,
                ],
            )
            self.connection.commit()

    def complete_routing_event(
        self,
        request_id: str,
        *,
        clarification_outcome: str | None = None,
    ) -> None:
        with self.lock:
            self.connection.execute(
                """
                UPDATE routing_events
                SET clarification_outcome = COALESCE(?, clarification_outcome),
                    completed_at = ?
                WHERE request_id = ?
                """,
                [
                    clarification_outcome,
                    datetime.now(timezone.utc).isoformat(),
                    request_id,
                ],
            )
            self.connection.commit()

    def mark_routing_correction(self, request_id: str | None) -> None:
        if not request_id:
            return
        with self.lock:
            self.connection.execute(
                """
                UPDATE routing_events
                SET user_correction = 1
                WHERE request_id = ?
                """,
                [request_id],
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

    def used_this_month(self, subject_id: str) -> int:
        with self.lock:
            row = self.connection.execute(
                """
                SELECT COUNT(*) FROM query_events
                WHERE subject_id = ? AND SUBSTR(utc_day, 1, 7) = ?
                  AND status IN ('reserved', 'running', 'completed', 'failed')
                """,
                [subject_id, utc_month()],
            ).fetchone()
        return int(row[0])

    def reserve(
        self,
        request_id: str,
        access: AccessContext,
        release_binding_id: str,
        workflow: str,
        request_fingerprint: str | None = None,
    ) -> int:
        policy = access.policy
        now = datetime.now(timezone.utc).isoformat()
        with self.lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                existing = self.connection.execute(
                    """
                    SELECT subject_id, status, request_fingerprint
                    FROM query_events WHERE request_id = ?
                    """,
                    [request_id],
                ).fetchone()
                if existing:
                    same_request = (
                        existing[0] == access.subject_id
                        and (
                            not existing[2]
                            or not request_fingerprint
                            or existing[2] == request_fingerprint
                        )
                    )
                    if not same_request:
                        raise DuplicateRequestError(
                            "That request identifier is already attached to another question."
                        )
                    if existing[1] != "failed_refunded":
                        raise DuplicateRequestError(
                            "That research request is already being processed."
                        )
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
                used_this_month = int(
                    self.connection.execute(
                        """
                        SELECT COUNT(*) FROM query_events
                        WHERE subject_id = ? AND SUBSTR(utc_day, 1, 7) = ?
                          AND status IN ('reserved', 'running', 'completed', 'failed')
                        """,
                        [access.subject_id, utc_month()],
                    ).fetchone()[0]
                )
                if used_this_month >= policy.queries_per_utc_month:
                    raise DailyQuotaExceeded(policy, period="month")
                if existing:
                    self.connection.execute(
                        """
                        UPDATE query_events
                        SET request_fingerprint = ?, tier = ?, utc_day = ?,
                            status = 'reserved', created_at = ?, completed_at = NULL,
                            release_binding_id = ?, workflow = ?, latency_ms = NULL,
                            estimated_cost_usd = NULL, performance_json = NULL
                        WHERE request_id = ?
                        """,
                        [
                            request_fingerprint, policy.tier, utc_day(), now,
                            release_binding_id, workflow, request_id,
                        ],
                    )
                else:
                    self.connection.execute(
                        """
                        INSERT INTO query_events (
                            request_id, request_fingerprint, subject_id, tier,
                            utc_day, status, created_at, release_binding_id, workflow
                        ) VALUES (?, ?, ?, ?, ?, 'reserved', ?, ?, ?)
                        """,
                        [
                            request_id, request_fingerprint, access.subject_id,
                            policy.tier, utc_day(), now, release_binding_id, workflow,
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
        billable: bool = True,
        performance: Dict[str, Any] | None = None,
    ) -> None:
        with self.lock:
            self.connection.execute(
                """
                UPDATE query_events
                SET status=?, completed_at=?, latency_ms=?, estimated_cost_usd=?,
                    performance_json=?
                WHERE request_id=?
                """,
                [
                    "completed" if billable else "completed_unbilled",
                    datetime.now(timezone.utc).isoformat(),
                    latency_ms,
                    estimated_cost_usd,
                    json.dumps(performance, default=str) if performance else None,
                    request_id,
                ],
            )
            self.connection.commit()

    def fail(
        self,
        request_id: str,
        *,
        refund: bool = True,
        performance: Dict[str, Any] | None = None,
    ) -> None:
        self._set_status(
            request_id,
            "failed_refunded" if refund else "failed",
            performance=performance,
        )

    def _set_status(
        self,
        request_id: str,
        status: str,
        *,
        performance: Dict[str, Any] | None = None,
    ) -> None:
        with self.lock:
            self.connection.execute(
                """
                UPDATE query_events
                SET status=?, performance_json=COALESCE(?, performance_json)
                WHERE request_id=?
                """,
                [
                    status,
                    json.dumps(performance, default=str) if performance else None,
                    request_id,
                ],
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
        self.paths = sorted(
            {Path(path).resolve() for path in paths if Path(path).is_file()}
        )
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
    "standardized output",
    "platform → supplier cage/site",
    "platform -> supplier cage/site",
)

CUSTOMER_BLOCKED_LINK_HOSTS = {
    "github.com",
    "gitlab.com",
    "bitbucket.org",
    "raw.githubusercontent.com",
}

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
    "evidence_chain",
    "calculation_version",
    "context_id",
    "generated_at",
    "identity_definition_version",
    "source_file_hashes",
    "release_id",
    "release_binding_id",
    "pack_id",
    "ranking_universe",
    "current_year_treatment",
    "completed_fiscal_year_window",
    "response_generated_at",
    "source_locator",
    "source_file",
    "ingestion_date",
    "source_type",
    "source_fetch_url",
    "source_fetch_method",
    "source_content_sha256",
    "search_text",
    "raw_text",
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
    if isinstance(value, str):
        if value.startswith("/Users/") or value.startswith("s3://"):
            return None
        if value.startswith(("http://", "https://")):
            host = (urlparse(value).hostname or "").lower()
            if host in CUSTOMER_BLOCKED_LINK_HOSTS or any(
                host.endswith(f".{blocked}") for blocked in CUSTOMER_BLOCKED_LINK_HOSTS
            ):
                return None
    return value


def _trace_urls(value: Any) -> set[str]:
    urls: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            if key in {
                "url",
                "canonical_url",
                "public_record_url",
                "public_notice_url",
                "source_url",
            }:
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
        if (
            host in {"localhost", "127.0.0.1"}
            or host.endswith(".local")
            or host in CUSTOMER_BLOCKED_LINK_HOSTS
            or any(host.endswith(f".{blocked}") for blocked in CUSTOMER_BLOCKED_LINK_HOSTS)
        ):
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


def remove_unsafe_and_internal_answer_content(
    answer: str, validation: Dict[str, Any]
) -> str:
    """Preserve a useful answer while removing non-public implementation details."""
    unsafe = {
        str(url).strip().rstrip("/")
        for url in validation.get("unsafe_links", [])
    }

    def replace_link(match: re.Match[str]) -> str:
        label, raw_url = match.groups()
        return label if raw_url.strip().rstrip("/") in unsafe else match.group(0)

    cleaned = MARKDOWN_LINK_WITH_LABEL.sub(replace_link, str(answer or ""))
    cleaned = re.sub(
        r"(?im)^.*(?:standardized output|platform\s*(?:→|->)\s*supplier cage/site).*(?:\n|$)",
        "",
        cleaned,
    )
    cleaned = re.sub(
        r"https?://(?:www\.)?(?:github\.com|gitlab\.com|bitbucket\.org|"
        r"raw\.githubusercontent\.com)/[^\s)\]]+",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    replacements = {
        "source_report_id": "public source record identifier",
        "source_dedup_key": "source record",
        "transaction_key": "contract action identifier",
        "internal_value_treatment": "value treatment",
    }
    for marker in validation.get("forbidden_markers", []):
        replacement = replacements.get(str(marker).lower(), "")
        cleaned = re.sub(re.escape(str(marker)), replacement, cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"\bMimir[- ]modelled (?:reported )?subcontract value\b",
        "reported subcontract value",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\b(?:a\s+)?residual\s+Missile systems\s*\(multiple programs\)\s+grouping\b",
        "Missile-related activity not attributable to one named program",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"(?:s3://|file://|/Users/|local_data/)[^\s)`\]]+", "", cleaned, flags=re.IGNORECASE)
    return re.sub(r"[ \t]{2,}", " ", cleaned).strip()
