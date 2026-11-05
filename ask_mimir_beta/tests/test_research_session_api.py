import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient
from research_session_api import create_research_session_router


class ResearchSessionApiTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.env=patch.dict(os.environ,{'ASK_MIMIR_RESEARCH_SESSIONS_ENABLED':'1',
            'ASK_MIMIR_TRUSTED_PROXY_SECRET':'fixture-only-adapter-secret',
            'ASK_MIMIR_SESSION_SUBJECT_ALLOWLIST':'user:invited'})
        self.env.start()
        self.headers={'x-ask-mimir-proxy-secret':'fixture-only-adapter-secret'}
        self.access=SimpleNamespace(authenticated=True,subject_id='user:invited',tier='free')
        app=FastAPI()
        app.include_router(create_research_session_router(lambda:Path(self.temp.name)/'existing.sqlite',lambda request:self.access))
        self.client=TestClient(app)

    def tearDown(self): self.env.stop(); self.temp.cleanup()

    def post(self,payload,headers=None):
        return self.client.post('/api/research-session',json=payload,headers=self.headers if headers is None else headers)

    def test_disabled_and_missing_allowlist_are_closed(self):
        with patch.dict(os.environ,{'ASK_MIMIR_RESEARCH_SESSIONS_ENABLED':'0'}):
            self.assertEqual(self.post({'action':'status'}).status_code,404)
        with patch.dict(os.environ,{'ASK_MIMIR_SESSION_SUBJECT_ALLOWLIST':''}):
            self.assertEqual(self.post({'action':'status'}).status_code,403)

    def test_bearer_clients_cannot_refund_or_choose_owner_and_plan(self):
        self.assertEqual(self.post({'action':'status'},headers={'Authorization':'Bearer fixture'}).status_code,401)
        self.assertEqual(self.post({'action':'status','owner':'another','tier':'enterprise'}).status_code,400)
        self.access.subject_id='user:uninvited'
        self.assertEqual(self.post({'action':'status'}).status_code,403)

    def test_prepared_customer_mode_is_trusted_proxy_only_and_does_not_change_website_policy(self):
        from beta_controls import BetaStateStore
        self.access.subject_id='user:12345678-1234-4234-8234-123456789abc'
        self.assertEqual(self.post({'action':'status'}).status_code,403)
        with patch.dict(os.environ,{'ASK_MIMIR_MCP_ACCOUNTS_ENABLED':'1'}):
            self.assertEqual(self.post({'action':'status'}).status_code,200)
            self.assertEqual(self.post({'action':'status'},headers={}).status_code,401)
            self.assertFalse(BetaStateStore.session_enabled(self.access.subject_id))
            self.access.subject_id='guest:unregistered'
            self.assertEqual(self.post({'action':'status'}).status_code,403)
            self.access.subject_id='user:12345678123442348234123456789abc'
            self.assertEqual(self.post({'action':'status'}).status_code,403)

    def test_status_reserve_and_refund_without_identity_or_content(self):
        r=self.post({'action':'status'})
        self.assertEqual(r.status_code,200); self.assertEqual(r.json()['usage']['remaining_today'],2)
        self.assertEqual(r.headers['cache-control'],'private, no-store')
        self.assertNotIn('user:invited',r.text)
        r=self.post({'action':'admit','request_key':'research-123','fingerprint':hashlib.sha256(b'fixture').hexdigest()})
        self.assertEqual(r.json()['usage']['remaining_today'],1)
        r=self.post({'action':'fail','request_key':'research-123'})
        self.assertEqual(r.json()['usage']['remaining_today'],2)

    def test_malformed_and_oversized_requests_do_not_touch_usage(self):
        for payload in [None,[],{'action':'delete'},{'action':'admit','request_key':'a','fingerprint':'raw question'}]:
            self.assertEqual(self.post(payload).status_code,400)
        self.assertEqual(self.post({'action':'status','padding':'x'*3000}).status_code,413)
        self.assertEqual(self.post({'action':'status'}).json()['usage']['remaining_today'],2)


if __name__=='__main__': unittest.main()
