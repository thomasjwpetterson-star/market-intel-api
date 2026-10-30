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
    def test_price_scenarios_inform_review_order_without_inventing_contract_values(self):
        aws = CaptureAWS(); repo = Repository(aws); repo.table = lambda name: 'safe.' + name
        repo.manifest = lambda: {'tables': {'company_item_valuations': {}, 'company_notice_item_prices': {}}}
        repo.opportunities('CAGE:45934', 24, 0, False, 'items')
        self.assertIn('least(v.government_reference_lot_scenario,v.supplier_average_lot_scenario)', aws.sql)
        self.assertIn('np.company_id=m.company_id', aws.sql)
        self.assertIn('np.record_hash=o.record_hash', aws.sql)
        self.assertIn('not a win probability or company revenue', aws.sql)

    def test_unknown_company_entry_is_context_not_a_ranked_work_opportunity(self):
        aws=CaptureAWS();repo=Repository(aws);repo.table=lambda name:'safe.'+name
        repo.manifest=lambda:{'tables':{'company_work_matches':{'columns':[]}}}
        repo.opportunities('CAGE:45934',24,0,False,'work')
        self.assertIn("cw.review_category<>'market_context'",aws.sql)
        repo.opportunities('CAGE:45934',24,0,True)
        self.assertIn("cw.review_category='market_context'",aws.sql)

    def test_supplier_lineage_is_scoped_to_the_opportunity(self):
        aws = CaptureAWS(); repo = Repository(aws); repo.table = lambda name: 'safe.' + name
        repo.manifest = lambda: {'tables': {'supplier_pull_prime_evidence': {}}}
        repo.supplier_evidence('a' * 64, 'primes', 25, 0)
        self.assertIn("WHERE episode_id='" + 'a' * 64 + "'", aws.sql)
        self.assertIn('safe.supplier_pull_prime_evidence', aws.sql)
        with self.assertRaises(ValueError): repo.supplier_evidence('a' * 64, "bad'", 25, 0)
        with self.assertRaises(ValueError): repo.supplier_evidence("bad'", 'primes', 25, 0)

    def test_complete_value_evidence_has_bounded_pagination(self):
        aws = CaptureAWS(); repo = Repository(aws); repo.table = lambda name: 'safe.' + name
        repo.manifest = lambda: {'tables': {'contract_value_observations': {}}}
        repo.cohort_awards('a' * 64, 25, 25)
        self.assertIn("WHERE cohort_id='" + 'a' * 64 + "'", aws.sql)
        self.assertIn('OFFSET 25 LIMIT 25', aws.sql)
        with self.assertRaises(ValueError): repo.cohort_awards("bad'", 25, 0)
        with self.assertRaises(ValueError): repo.cohort_awards('a' * 64, 1000, 0)

    def test_event_history_is_scoped_and_identifiers_are_validated(self):
        aws = CaptureAWS(); repo = Repository(aws); repo.table = lambda name: 'safe.' + name
        repo.manifest = lambda: {'tables': {'company_opportunity_events': {}}}
        repo.company_events('45934', 'a' * 64, 10, 0)
        self.assertIn("company_id='CAGE:45934'", aws.sql)
        self.assertIn("episode_id='" + 'a' * 64 + "'", aws.sql)
        for cage, episode, limit in [("45934'", None, 10), ('45934', "bad'", 10), ('45934', None, 1000)]:
            with self.assertRaises(ValueError): repo.company_events(cage, episode, limit)

    def test_negative_future_forecasts_are_withheld_without_suppressing_real_requests(self):
        aws=CaptureAWS();repo=Repository(aws);repo.table=lambda name:'safe.'+name
        repo.manifest=lambda:{'as_of':'2026-10-30','tables':{'dla_forecasts':{}}}
        repo.opportunities('CAGE:45934',24,0,False,'items')
        self.assertIn("forecast_qty<0",aws.sql)
        self.assertIn("DATE '2026-10-30'",aws.sql)
        self.assertIn("o.lane<>'source_item_forecasts' OR fq.niin IS NULL",aws.sql)
        self.assertIn("JOIN safe.company_matches",aws.sql)

    def test_registry_relationships_are_exact_and_distinguish_replacements(self):
        aws=CaptureAWS();repo=Repository(aws);repo.table=lambda name:'safe.'+name
        repo.manifest=lambda:{'tables':{'company_registry':{}}}
        repo.relationships('45934',25,0)
        self.assertIn("upper(trim(parent_cage))='45934'",aws.sql)
        self.assertIn("'registered_parent_cage' relationship_type",aws.sql)
        self.assertIn("'replacement_cage','replacement_cage'",aws.sql)
        self.assertNotIn('LIKE',aws.sql)
        self.assertNotIn('parent_name',aws.sql)
        with self.assertRaises(ValueError):repo.relationships("45934'",25,0)
        with self.assertRaises(ValueError):repo.relationships('45934',1000,0)

    def test_company_scope_drilldown_uses_exact_native_scope_and_company(self):
        aws=CaptureAWS();repo=Repository(aws);repo.table=lambda name:'safe.'+name
        repo.manifest=lambda:{'tables':{'company_subcontract_facts':{}}}
        repo.work_evidence=lambda doc:[{'company_id':'CAGE:45934','source_kind':'subaward_report','work_text':"PUMP FUEL BOOST"}]
        repo.work_sources('a'*64,25,0)
        self.assertIn("a.company_id='CAGE:45934'",aws.sql)
        self.assertIn("'PUMP FUEL BOOST'",aws.sql)
        self.assertIn('safe.company_subcontract_facts',aws.sql)
        self.assertIn('OFFSET 0 LIMIT 25',aws.sql)
        with self.assertRaises(ValueError):repo.work_sources("bad'",25,0)

    def test_invalid_company_or_kind_rejected(self):
        repo = Repository(CaptureAWS()); repo.table = lambda name: 'safe.' + name
        for company, kind in [("CAGE:45934' OR 1=1", 'work'), ('CAGE:45934', 'arbitrary')]:
            with self.assertRaises(ValueError): repo.opportunities(company, 24, 0, False, kind)

if __name__ == '__main__': unittest.main()
