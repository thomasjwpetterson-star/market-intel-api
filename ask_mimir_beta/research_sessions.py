"""Private research-session pilot using the existing Ask Mimir SQLite file.

This module does not replace the website's current query/credit policy. Both
adapters must migrate together before enabling commercial session allowances.
No question, answer, email, token or raw argument is stored in these tables.
"""
from __future__ import annotations

import sqlite3
import re
import time
import uuid
from contextlib import contextmanager
from pathlib import Path


LIMITS = {"free": (2, 30), "trial": (5, 35), "lite": (5, 100),
          "professional": (15, 300), "enterprise": (50, 1000), "preview": (1, 1)}
WINDOW = 900
LEASE = 120
WORK_BUDGET = 60
PREVIEW_WORK_BUDGET = 4
PREVIEW_SERVICE_DAILY_LIMIT = 50


class SessionLimit(RuntimeError):
    def __init__(self, code: str, usage: dict):
        self.code, self.usage = code, usage
        super().__init__(code)


class SessionConflict(ValueError):
    pass


class ResearchSessions:
    """Multi-connection atomic admission. Callers supply verified account identity.

    request_key and fingerprint are opaque server-generated identifiers/digests,
    never a question or caller-selected plan. Status is safe for model/UI output;
    internal reservation IDs stay exclusively between trusted server adapters.
    """
    def __init__(self, path: Path, clock=time.time):
        self.path, self.clock = str(path), clock
        with self._db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS research_session_windows (
                  id TEXT PRIMARY KEY, owner TEXT NOT NULL, created REAL NOT NULL,
                  started REAL, ends REAL);
                CREATE INDEX IF NOT EXISTS research_session_owner
                  ON research_session_windows(owner, started);
                CREATE TABLE IF NOT EXISTS research_session_calls (
                  request_key TEXT PRIMARY KEY, owner TEXT NOT NULL,
                  fingerprint TEXT NOT NULL, window_id TEXT NOT NULL,
                  state TEXT NOT NULL CHECK(state IN ('pending','success','failed')),
                  created REAL NOT NULL, lease REAL NOT NULL,
                  FOREIGN KEY(window_id) REFERENCES research_session_windows(id)
                    ON DELETE CASCADE);
                CREATE INDEX IF NOT EXISTS research_session_call_window
                  ON research_session_calls(window_id, state);
            """)

    @contextmanager
    def _db(self, connection=None):
        if connection is not None:
            if not connection.in_transaction:
                raise RuntimeError('Session accounting requires an active caller transaction')
            original_factory=connection.row_factory
            connection.row_factory=sqlite3.Row
            try:
                yield connection
            finally:
                connection.row_factory=original_factory
            return
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def _clean(self, db, now):
        db.execute("UPDATE research_session_calls SET state='failed' WHERE state='pending' AND lease<=?", (now,))
        # Existing job recovery can refund work before its long-running lease
        # expires. Reconcile within the same file without touching MCP-only calls.
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='query_events'").fetchone():
            db.execute("""UPDATE research_session_calls SET state='failed'
              WHERE state='pending' AND request_key IN (
                SELECT request_id FROM query_events WHERE status IN ('failed_refunded','completed_unbilled'))""")
        # The existing job connection may not enable foreign-key cascades.
        db.execute('DELETE FROM research_session_calls WHERE window_id IN (SELECT id FROM research_session_windows WHERE COALESCE(ends,created)<?)',(now-90*86400,))
        db.execute('DELETE FROM research_session_windows WHERE COALESCE(ends,created)<?',(now-90*86400,))

    def _usage(self, db, owner, tier, now):
        from datetime import datetime, timedelta, timezone
        if tier not in LIMITS or not owner or owner.startswith(('guest:', 'anonymous:')):
            raise ValueError("A verified Mimir account and supported plan are required")
        preview=tier=='preview'
        if preview != owner.startswith('preview:'):
            raise ValueError('Preview and account identities must be kept separate')
        current = datetime.fromtimestamp(now, timezone.utc)
        day = current.replace(hour=0, minute=0, second=0, microsecond=0)
        month = day.replace(day=1)
        next_month = (month.replace(year=month.year+1, month=1) if month.month == 12
                      else month.replace(month=month.month+1))
        day_end, month_end = (day+timedelta(days=1)).timestamp(), next_month.timestamp()
        counted = db.execute("""
          SELECT COALESCE(started,created) charged FROM research_session_windows w
          WHERE owner=? AND (started IS NOT NULL OR EXISTS (
            SELECT 1 FROM research_session_calls c WHERE c.window_id=w.id AND c.state='pending'))
        """, (owner,)).fetchall()
        daily = sum(day.timestamp() <= r['charged'] < day_end for r in counted)
        monthly = sum(month.timestamp() <= r['charged'] < month_end for r in counted)
        if preview: daily=monthly=len(counted)
        active = db.execute("""SELECT id,ends FROM research_session_windows
          WHERE owner=? AND started IS NOT NULL AND ends>? ORDER BY started DESC LIMIT 1""", (owner,now)).fetchone()
        calls = (db.execute("SELECT count(*) FROM research_session_calls WHERE window_id=? AND state IN ('pending','success')", (active['id'],)).fetchone()[0]
                 if active else 0)
        daily_limit, month_limit = LIMITS[tier]
        work_budget=PREVIEW_WORK_BUDGET if preview else WORK_BUDGET
        resets = [] if preview else ([day_end] if daily >= daily_limit else []) + ([month_end] if monthly >= month_limit else [])
        if active and calls >= work_budget and not preview: resets.append(active['ends'])
        iso = lambda value: datetime.fromtimestamp(value,timezone.utc).isoformat() if value is not None else None
        if preview:
            return dict(unit='preview_session',active_until=iso(active['ends']) if active else None,
                        can_continue=bool(active and calls<work_budget),can_start=not active and not counted,
                        remaining_work_calls=max(work_budget-calls,0) if active else 0,
                        lookups_per_preview=work_budget)
        return dict(unit='preview_session' if preview else 'research_session',plan=tier,
                    daily_limit=daily_limit,monthly_limit=month_limit,
                    remaining_today=max(daily_limit-daily,0),
                    remaining_this_month=max(month_limit-monthly,0),
                    active_until=iso(active['ends']) if active else None,
                    can_continue=bool(active and calls < work_budget),
                    can_start=not active and daily < daily_limit and monthly < month_limit,
                    remaining_work_calls=max(work_budget-calls,0) if active else 0,
                    daily_resets_at=iso(day_end),monthly_resets_at=iso(month_end),
                    next_available_at=iso(max(resets)) if resets else None)

    def status(self, owner, tier, *, _connection=None):
        with self._db(_connection) as db:
            now=self.clock(); self._clean(db,now)
            return self._usage(db,owner,tier,now)

    def admit(self, owner, tier, request_key, fingerprint, *, lease_seconds=LEASE, _connection=None):
        if not re.fullmatch(r'[A-Za-z0-9_-]{8,128}', request_key or '') or not re.fullmatch(r'[a-f0-9]{64}', fingerprint or ''):
            raise ValueError('Use bounded opaque server identifiers')
        if type(lease_seconds) is not int or not LEASE<=lease_seconds<=7500:
            raise ValueError('Invalid research lease')
        with self._db(_connection) as db:
            now=self.clock(); self._clean(db,now)
            usage=self._usage(db,owner,tier,now)
            existing=db.execute('SELECT * FROM research_session_calls WHERE request_key=?',(request_key,)).fetchone()
            if existing:
                if existing['owner']!=owner or existing['fingerprint']!=fingerprint:
                    raise SessionConflict('Request identifier belongs to different research')
                if existing['state']!='failed':
                    return dict(replay=True,state=existing['state'],usage=usage)
                # A refunded attempt is retryable with a new internal reservation,
                # but cannot rewrite a successful result or another owner.
                db.execute('DELETE FROM research_session_calls WHERE request_key=?',(request_key,))
            window=db.execute("""SELECT id,started,ends FROM research_session_windows w
              WHERE owner=? AND ((started IS NOT NULL AND ends>?) OR
                (started IS NULL AND EXISTS (SELECT 1 FROM research_session_calls c
                  WHERE c.window_id=w.id AND c.state='pending')))
              ORDER BY created DESC LIMIT 1""",(owner,now)).fetchone()
            if not window:
                if not usage['can_start']:
                    raise SessionLimit('quota_exhausted',usage)
                if tier=='preview':
                    from datetime import datetime,timezone
                    start=datetime.fromtimestamp(now,timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0).timestamp()
                    service_count=db.execute("""SELECT count(*) FROM research_session_windows w
                      WHERE owner LIKE 'preview:%' AND created>=? AND (started IS NOT NULL
                        OR EXISTS(SELECT 1 FROM research_session_calls c WHERE c.window_id=w.id AND c.state='pending'))""",(start,)).fetchone()[0]
                    if service_count>=PREVIEW_SERVICE_DAILY_LIMIT:raise SessionLimit('preview_unavailable',usage)
                window_id=uuid.uuid4().hex
                db.execute('INSERT INTO research_session_windows VALUES (?,?,?,NULL,NULL)',(window_id,owner,now))
            else:
                window_id=window['id']
                calls=db.execute("SELECT count(*) FROM research_session_calls WHERE window_id=? AND state IN ('pending','success')",(window_id,)).fetchone()[0]
                if calls>=(PREVIEW_WORK_BUDGET if tier=='preview' else WORK_BUDGET):
                    raise SessionLimit('session_work_limit',usage)
            db.execute("INSERT INTO research_session_calls VALUES (?,?,?,?,'pending',?,?)",(request_key,owner,fingerprint,window_id,now,now+lease_seconds))
            return dict(replay=False,state='pending',usage=self._usage(db,owner,tier,now))

    def finish(self, owner, tier, request_key, *, success, _connection=None):
        with self._db(_connection) as db:
            now=self.clock(); self._clean(db,now)
            row=db.execute('SELECT * FROM research_session_calls WHERE request_key=?',(request_key,)).fetchone()
            if not row or row['owner']!=owner:
                raise SessionConflict('Unknown owner-bound reservation')
            if row['state']=='pending':
                db.execute('UPDATE research_session_calls SET state=? WHERE request_key=?',('success' if success else 'failed',request_key))
                if success:
                    db.execute('UPDATE research_session_windows SET started=?,ends=? WHERE id=? AND started IS NULL',(now,now+WINDOW,row['window_id']))
            elif success and row['state']=='failed':
                raise SessionConflict('Expired/refunded work cannot be charged later')
            # A failure after success never refunds an already delivered session.
            return self._usage(db,owner,tier,now)

    def delete_account(self, owner):
        with self._db() as db:
            db.execute('DELETE FROM research_session_windows WHERE owner=?',(owner,))

    def purge_expired(self):
        with self._db() as db:
            now=self.clock(); self._clean(db,now)
            db.execute('DELETE FROM research_session_windows WHERE COALESCE(ends,created)<?',(now-90*86400,))
