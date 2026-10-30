"""Security and query-boundary checks; no cloud calls or migrations."""
import os
import unittest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient
from opportunity_intelligence.api import Repository, create_router
from opportunity_intelligence.mount import verified_application_principal

class FakeRepository:
    def companies(self, query, limit):
        return [{'cage_code': '45934'}]

class CaptureAWS:
    def __init__(self): self.sql = ''
    def result(self, sql): self.sql = sql; return [], {}

class OpportunityPilotTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {'MIMIR_EXPORT_PROXY_SECRET': 'test-proxy-secret'})
        self.env.start()
        app = FastAPI()
        app.include_router(create_router(verified_application_principal, FakeRepository()))
        self.client = TestClient(app)
    def tearDown(self): self.env.stop()
    def test_forged_bearer_header_is_not_authentication(self):
        response = self.client.get('/opportunity-intelligence/companies?q=Ontic', headers={'Authorization': 'Bearer invented'})
        self.assertEqual(response.status_code, 401)
    def test_proxy_requires_verified_user_identity(self):
        response = self.client.get('/opportunity-intelligence/companies?q=Ontic', headers={'X-Mimir-Export-Key': 'test-proxy-secret'})
        self.assertEqual(response.status_code, 401)
    def test_authenticated_application_can_search(self):
        response = self.client.get('/opportunity-intelligence/companies?q=Ontic', headers={'X-Mimir-Export-Key': 'test-proxy-secret', 'X-Mimir-Opportunity-User': '11111111-1111-4111-8111-111111111111'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['records'][0]['cage_code'], '45934')
    def test_work_query_has_no_horizon_cutoff_and_uses_outer_sort(self):
        aws = CaptureAWS(); repo = Repository(aws); repo.table = lambda name: 'safe.' + name
        repo.manifest = lambda: {'tables': {}}
        repo.opportunities('CAGE:45934', 24, 0, False, 'work')
        self.assertIn('SELECT q.* FROM (SELECT DISTINCT', aws.sql)
        self.assertIn("o.lane NOT IN ('published_item_procurement','source_item_forecasts')", aws.sql)
        self.assertNotIn('18', aws.sql)
        self.assertNotIn('historical_watch\' THEN', aws.sql)
    def test_event_history_is_scoped_and_identifiers_are_validated(self):
        aws = CaptureAWS(); repo = Repository(aws); repo.table = lambda name: 'safe.' + name
        repo.manifest = lambda: {'tables': {'company_opportunity_events': {}}}
        repo.company_events('45934', 'a' * 64, 10, 0)
        self.assertIn("company_id='CAGE:45934'", aws.sql)
        self.assertIn("episode_id='" + 'a' * 64 + "'", aws.sql)
        for cage, episode, limit in [("45934'", None, 10), ('45934', "bad'", 10), ('45934', None, 1000)]:
            with self.assertRaises(ValueError): repo.company_events(cage, episode, limit)

    def test_invalid_company_or_kind_rejected(self):
        repo = Repository(CaptureAWS()); repo.table = lambda name: 'safe.' + name
        for company, kind in [("CAGE:45934' OR 1=1", 'work'), ('CAGE:45934', 'arbitrary')]:
            with self.assertRaises(ValueError): repo.opportunities(company, 24, 0, False, kind)

if __name__ == '__main__': unittest.main()
