import hashlib
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from beta_controls import BetaStateStore, AccessContext, DailyQuotaExceeded
from research_sessions import ResearchSessions, LIMITS


class SharedResearchSessionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.path=Path(self.temp.name)/'existing-state.sqlite3'
        self.owner='user:private-free'
        self.flags=patch.dict('os.environ',{'ASK_MIMIR_RESEARCH_SESSIONS_ENABLED':'1','ASK_MIMIR_SESSION_SUBJECT_ALLOWLIST':self.owner})
        self.flags.start()
        self.store=BetaStateStore(self.path)
        self.now=time.time()
        self.store.research_sessions.clock=lambda:self.now
        self.access=AccessContext(self.owner,'free',True)

    def tearDown(self):
        self.store.connection.close();self.flags.stop();self.temp.cleanup()

    def ask(self,key):
        self.store.reserve(key,self.access,'release','company',request_fingerprint=key)
        self.store.complete(key,latency_ms=1,estimated_cost_usd=0,billable=True)

    def test_mcp_and_website_followups_use_one_session_without_old_question_limit(self):
        meter=ResearchSessions(self.path,lambda:self.now)
        meter.admit(self.owner,'free','mcp-00001',hashlib.sha256(b'mcp').hexdigest())
        meter.finish(self.owner,'free','mcp-00001',success=True)
        for i in range(4):self.ask(f'ask-0000{i}')
        usage=self.store.public_access(self.access)
        self.assertEqual(usage['usage_unit'],'research_session')
        self.assertEqual(usage['queries_used_today'],1)
        self.assertEqual(usage['research_usage']['remaining_today'],1)
        self.assertEqual(self.store.connection.execute('SELECT count(*) FROM query_events').fetchone()[0],4)
        self.store.assert_allowance_available(self.access)

    def test_website_first_success_and_mcp_followup_share_the_fixed_window(self):
        self.ask('ask-first1')
        old=self.store.public_access(self.access)['research_usage']['active_until']
        self.now+=100
        meter=ResearchSessions(self.path,lambda:self.now)
        meter.admit(self.owner,'free','mcp-next1',hashlib.sha256(b'next').hexdigest())
        usage=meter.finish(self.owner,'free','mcp-next1',success=True)
        self.assertEqual(usage['active_until'],old)
        self.assertEqual(usage['remaining_today'],1)

    def test_legacy_insert_failure_rolls_back_session_reservation(self):
        self.store.connection.execute("CREATE TRIGGER fail_query BEFORE INSERT ON query_events BEGIN SELECT RAISE(FAIL,'fixture storage failure'); END")
        self.store.connection.commit()
        with self.assertRaises(sqlite3.IntegrityError):self.ask('ask-failed')
        self.assertEqual(self.store.public_access(self.access)['research_usage']['remaining_today'],2)

    def test_answer_and_session_commit_share_a_transaction(self):
        self.store.reserve('ask-fails1',self.access,'release','company')
        with patch.object(self.store,'_save_job_unlocked',side_effect=RuntimeError('fixture result save failed')):
            with self.assertRaises(RuntimeError):self.store.complete('ask-fails1',latency_ms=1,estimated_cost_usd=0,job={})
        self.assertIsNone(self.store.public_access(self.access)['research_usage']['active_until'])
        self.store.fail('ask-fails1')
        self.assertEqual(self.store.public_access(self.access)['research_usage']['remaining_today'],2)

    def test_clarification_is_unbilled_and_long_first_job_does_not_expire_at_two_minutes(self):
        self.store.reserve('ask-long01',self.access,'release','company')
        self.now+=301
        self.store.complete('ask-long01',latency_ms=300000,estimated_cost_usd=0,billable=False)
        self.assertEqual(self.store.public_access(self.access)['research_usage']['remaining_today'],2)

    def test_plan_limits_control_completed_sessions_and_preserve_last_followups(self):
        for tier in ('free','lite','professional'):
            owner='user:private-'+tier
            with patch.dict('os.environ',{'ASK_MIMIR_SESSION_SUBJECT_ALLOWLIST':owner}):
                access=AccessContext(owner,tier,True)
                for i in range(LIMITS[tier][0]):
                    key=f'{tier}-start-{i:03}'
                    self.store.reserve(key,access,'release','company')
                    self.store.complete(key,latency_ms=1,estimated_cost_usd=0)
                    self.now+=901
                with self.assertRaises(DailyQuotaExceeded):self.store.reserve(f'{tier}-too-many',access,'release','company')
                usage=self.store.public_access(access)['research_usage']
                self.assertEqual((usage['daily_limit'],usage['monthly_limit']),LIMITS[tier])

    def test_uninvited_website_user_retains_existing_question_policy(self):
        access=AccessContext('user:existing-customer','free',True)
        for i in range(2):self.store.reserve(f'old-policy-{i}',access,'release','company')
        with self.assertRaises(DailyQuotaExceeded):self.store.reserve('old-policy-third',access,'release','company')
        self.assertNotIn('research_usage',self.store.public_access(access))


if __name__=='__main__':unittest.main()
