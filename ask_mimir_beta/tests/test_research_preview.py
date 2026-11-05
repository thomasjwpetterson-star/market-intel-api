import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient
from research_session_api import create_research_session_router
from research_sessions import ResearchSessions, SessionLimit, PREVIEW_SERVICE_DAILY_LIMIT


class PreviewTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.path=Path(self.temp.name)/'state.sqlite'
        self.now=1793851200
        self.store=ResearchSessions(self.path,lambda:self.now)
        self.owner='preview:'+hashlib.sha256(b'chat-one').hexdigest()
    def tearDown(self):self.temp.cleanup()
    def start(self,owner,key):
        return self.store.admit(owner,'preview',key,hashlib.sha256(key.encode()).hexdigest())
    def test_one_preview_with_four_lookups_and_no_daily_renewal(self):
        for i in range(4):
            self.start(self.owner,f'preview-{i}')
            usage=self.store.finish(self.owner,'preview',f'preview-{i}',success=True)
        self.assertFalse(usage['can_continue']);self.assertEqual(usage['remaining_work_calls'],0)
        self.assertNotIn('daily_limit',usage);self.assertNotIn('daily_resets_at',usage)
        with self.assertRaises(SessionLimit):self.start(self.owner,'preview-five')
        self.now+=86400
        with self.assertRaises(SessionLimit):self.start(self.owner,'preview-tomorrow')
        self.assertTrue(self.store.status('user:signed-in','free')['can_start'])
    def test_failed_lookup_is_refunded_and_does_not_start_the_window(self):
        self.start(self.owner,'preview-failed')
        usage=self.store.finish(self.owner,'preview','preview-failed',success=False)
        self.assertTrue(usage['can_start']);self.assertIsNone(usage['active_until'])
        self.start(self.owner,'preview-success')
        self.store.finish(self.owner,'preview','preview-success',success=True)
        self.now+=901
        self.assertFalse(self.store.status(self.owner,'preview')['can_start'])
    def test_global_preview_budget_is_shared_across_chats_and_accounts_separate(self):
        for i in range(PREVIEW_SERVICE_DAILY_LIMIT):
            owner='preview:'+hashlib.sha256(str(i).encode()).hexdigest()
            self.start(owner,f'preview-global-{i}')
        with self.assertRaises(SessionLimit) as error:self.start(self.owner,'preview-global-over')
        self.assertEqual(error.exception.code,'preview_unavailable')
        self.assertTrue(self.store.status('user:one','free')['can_start'])
        self.now+=86400
        self.start(self.owner,'preview-next-day')
    def test_identity_namespaces_cannot_select_account_entitlements(self):
        for owner,tier in [(self.owner,'professional'),('user:one','preview')]:
            with self.assertRaises(ValueError):self.store.status(owner,tier)
    def test_preview_adapter_is_off_by_default_secret_only_and_bounded(self):
        app=FastAPI();app.include_router(create_research_session_router(lambda:self.path,lambda r:None))
        client=TestClient(app)
        headers={'x-ask-mimir-proxy-secret':'fixture-secret','x-ask-mimir-preview-subject':self.owner}
        payload={'action':'admit','request_key':'preview-api-1','fingerprint':'a'*64}
        with patch.dict(os.environ,{'ASK_MIMIR_ANON_PREVIEW_ENABLED':'0'}):
            self.assertEqual(client.post('/api/research-preview',json=payload,headers=headers).status_code,404)
        with patch.dict(os.environ,{'ASK_MIMIR_ANON_PREVIEW_ENABLED':'1','ASK_MIMIR_TRUSTED_PROXY_SECRET':'fixture-secret'}):
            self.assertEqual(client.post('/api/research-preview',json=payload).status_code,401)
            for bad in [{'action':'admit','tier':'enterprise'}, {'action':'fail','request_key':[]}, {'action':'status'}]:
                self.assertEqual(client.post('/api/research-preview',json=bad,headers=headers).status_code,400)
            response=client.post('/api/research-preview',json=payload,headers=headers)
            self.assertEqual(response.status_code,200)
            self.assertNotIn(self.owner,response.text)
            self.assertEqual(response.headers['cache-control'],'private, no-store')
