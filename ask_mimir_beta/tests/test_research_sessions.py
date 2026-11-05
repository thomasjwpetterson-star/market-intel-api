import concurrent.futures
import hashlib
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from research_sessions import ResearchSessions, SessionLimit, SessionConflict, WORK_BUDGET


class ResearchSessionTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory()
        self.path=Path(self.directory.name)/'existing-beta-state.sqlite3'
        self.now=datetime(2026,11,5,10,tzinfo=timezone.utc).timestamp()
        self.store=ResearchSessions(self.path,lambda:self.now)

    def tearDown(self):
        self.directory.cleanup()

    def admit(self,key='request-1',owner='user:one',tier='free',fingerprint=None):
        return self.store.admit(owner,tier,key,fingerprint or hashlib.sha256(key.encode()).hexdigest())

    def finish(self,key='request-1',owner='user:one',success=True):
        return self.store.finish(owner,'free',key,success=success)

    def test_first_success_starts_window_and_followups_do_not_extend_it(self):
        r=self.admit(); self.assertIsNone(r['usage']['active_until'])
        self.now+=30
        usage=self.finish(); end=usage['active_until']
        self.assertEqual(usage['remaining_today'],1)
        self.now+=300; self.admit('followup-1')
        self.assertEqual(self.finish('followup-1')['active_until'],end)
        self.now+=601; self.admit('request-2')
        self.assertEqual(self.finish('request-2')['remaining_today'],0)

    def test_successful_retry_is_idempotent_and_owner_scope_is_enforced(self):
        self.admit(); self.finish()
        self.assertEqual(self.admit()['state'],'success')
        with self.assertRaises(SessionConflict): self.admit(owner='user:two')
        with self.assertRaises(SessionConflict): self.admit(fingerprint='a'*64)
        with self.assertRaises(SessionConflict): self.finish(owner='user:two')
        self.assertEqual(self.store.status('user:one','free')['remaining_today'],1)

    def test_failed_first_result_refunds_and_retries_can_succeed(self):
        self.admit(); usage=self.finish(success=False)
        self.assertEqual(usage['remaining_today'],2)
        self.admit(); self.finish()
        self.assertEqual(self.finish(success=False)['remaining_today'],1)

    def test_parallel_calls_and_separate_connections_share_one_session(self):
        def run(i):
            store=ResearchSessions(self.path,lambda:self.now)
            key=f'parallel-{i}'
            store.admit('user:one','free',key,hashlib.sha256(key.encode()).hexdigest())
            return store.finish('user:one','free',key,success=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            rows=list(pool.map(run,range(8)))
        self.assertTrue(all(row['remaining_today']==1 for row in rows))
        self.assertEqual(self.store.status('user:one','free')['remaining_work_calls'],WORK_BUDGET-8)

    def test_parallel_first_failures_preserve_other_pending_success(self):
        self.admit('request-a'); self.admit('request-b')
        self.finish('request-a',success=False)
        self.assertEqual(self.finish('request-b')['remaining_today'],1)

    def test_last_daily_unit_allows_followups_until_window_ends(self):
        self.admit(); self.finish(); self.now+=901
        self.admit('request-2'); self.finish('request-2')
        self.admit('followup-1'); usage=self.finish('followup-1')
        self.assertTrue(usage['can_continue']); self.assertFalse(usage['can_start'])
        self.now+=901
        with self.assertRaises(SessionLimit) as e: self.admit('request-3')
        self.assertEqual(e.exception.code,'quota_exhausted')
        self.assertEqual(e.exception.usage['next_available_at'],'2026-11-06T00:00:00+00:00')

    def test_work_guard_never_silently_starts_another_session(self):
        for i in range(WORK_BUDGET):
            self.admit(f'request-{i:03}'); self.finish(f'request-{i:03}')
        with self.assertRaises(SessionLimit) as e: self.admit('request-over-budget')
        self.assertEqual(e.exception.code,'session_work_limit')
        self.assertEqual(e.exception.usage['remaining_today'],1)

    def test_expired_pending_work_releases_and_cannot_charge_late(self):
        self.admit(); self.now+=121
        self.assertEqual(self.store.status('user:one','free')['remaining_today'],2)
        with self.assertRaises(SessionConflict): self.finish()

    def test_monthly_limit_and_later_of_both_resets(self):
        for day in range(15):
            self.now=datetime(2026,11,day+1,10,tzinfo=timezone.utc).timestamp()
            for i in range(2):
                key=f'day-{day:02}-{i}'; self.admit(key); self.finish(key); self.now+=901
        with self.assertRaises(SessionLimit) as e: self.admit('over-month')
        self.assertEqual(e.exception.usage['next_available_at'],'2026-12-01T00:00:00+00:00')
        self.now=datetime(2026,12,1,0,tzinfo=timezone.utc).timestamp()
        self.assertEqual(self.store.status('user:one','free')['remaining_this_month'],30)

    def test_pending_reservation_is_not_a_charge_until_success(self):
        self.admit(); self.assertEqual(self.store.status('user:one','free')['remaining_today'],1)
        self.finish(success=False)
        self.assertEqual(self.store.status('user:one','free')['remaining_today'],2)

    def test_account_closure_is_owner_scoped_and_retention_purges_only_old_rows(self):
        self.admit(); self.finish()
        self.admit('other-001',owner='user:two'); self.finish('other-001',owner='user:two')
        self.store.delete_account('user:one')
        self.assertEqual(self.store.status('user:one','free')['remaining_today'],2)
        self.assertEqual(self.store.status('user:two','free')['remaining_today'],1)
        self.now+=92*86400; self.store.purge_expired()
        import sqlite3
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM research_session_calls').fetchone()[0],0)

    def test_no_anonymous_identity_unknown_plan_or_raw_fingerprint(self):
        for owner,tier in [('guest:1','free'),('anonymous:1','free'),('user:one','unknown')]:
            with self.assertRaises(ValueError): self.admit(owner=owner,tier=tier)
        with self.assertRaises(ValueError): self.admit(fingerprint='a private raw question')
        self.assertEqual(self.store.status('user:one','free')['remaining_today'],2)

    def test_new_day_does_not_consume_another_unit_inside_active_window(self):
        self.now=datetime(2026,11,5,23,59,tzinfo=timezone.utc).timestamp()
        self.admit(); self.finish(); self.now+=120
        self.admit('nextday-1')
        self.assertEqual(self.finish('nextday-1')['remaining_today'],2)

    def test_plan_limits_are_separate_from_chatgpt_plan_and_status_has_no_identity(self):
        usage=self.store.status('user:one','lite')
        self.assertEqual((usage['daily_limit'],usage['monthly_limit']),(5,100))
        self.assertNotIn('user:one',str(usage))


if __name__=='__main__': unittest.main()
