"""Security and query-boundary checks; no cloud calls or migrations."""
import os
import datetime,io,json
import unittest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient
from opportunity_intelligence.api import Repository, create_router
from opportunity_intelligence.mount import LazyRepository, verified_application_principal

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
        self.assertIn('WITH candidates AS (SELECT DISTINCT', aws.sql)
        self.assertIn("o.lane NOT IN ('published_item_procurement','source_item_forecasts')", aws.sql)
        self.assertNotIn('18', aws.sql)
        self.assertNotIn('historical_watch\' THEN', aws.sql)
        self.assertIn("regexp_replace(upper(c.native_identifier),'-00$','')",aws.sql)
        self.assertIn('related_source_records_json',aws.sql)
        self.assertLess(aws.sql.index('q.source_family_rank=1'),aws.sql.index('OFFSET 0 LIMIT 24'))
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

    def test_source_quality_category_applies_before_pagination_and_preserves_unknowns(self):
        aws=CaptureAWS();repo=Repository(aws);repo.table=lambda name:'safe.'+name
        repo.manifest=lambda:{'tables':{'opportunity_quality_reviews':{},'company_notice_source_evidence':{},'company_work_matches':{'columns':[{'Name':'fit_verdict'}]}}}
        repo.opportunities('CAGE:45934',24,25,False,'work')
        self.assertIn("coalesce(qr.category_override,'')<>'items'",aws.sql)
        self.assertIn('qr.record_hash=o.record_hash',aws.sql)
        self.assertIn('cw.fit_reason',aws.sql)
        self.assertIn('source_evidence_json company_source_access_evidence_json',aws.sql)
        self.assertIn('OFFSET 25 LIMIT 24',aws.sql)
        repo.opportunities('CAGE:45934',24,0,False,'items')
        self.assertIn("OR coalesce(qr.category_override='items',false)",aws.sql)

    def test_supplier_lineage_is_scoped_to_the_opportunity(self):
        aws = CaptureAWS(); repo = Repository(aws); repo.table = lambda name: 'safe.' + name
        repo.manifest = lambda: {'tables': {'supplier_pull_prime_evidence': {}}}
        repo.supplier_evidence('a' * 64, 'primes', 25, 0)
        self.assertIn("WHERE e.episode_id='" + 'a' * 64 + "'", aws.sql)
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

    def test_supplier_roles_reuse_exact_profile_evidence_without_fuzzy_identity(self):
        aws=CaptureAWS();repo=Repository(aws);repo.table=lambda name:'safe.'+name
        repo.manifest=lambda:{'tables':{'supplier_pull_report_evidence':{},'company_profiles':{'columns':[{'Name':n} for n in ['role_assessment','manufacturer_declared_awards','profile_hash']]}}}
        repo.supplier_evidence('a'*64,'reports',25,0)
        self.assertIn('p.company_id=e.supplier_company_id',aws.sql)
        self.assertIn('p.role_assessment supplier_role_assessment',aws.sql)
        self.assertIn('p.profile_hash supplier_profile_hash',aws.sql)
        self.assertNotIn('LIKE',aws.sql)
        self.assertNotIn('p.manufacturing_data_rights',aws.sql)

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

    def test_one_request_cannot_mix_table_and_response_release_generations(self):
        old={'run_id':'old','tables':{'opportunities':{'table':'oi_old_opportunities'},'demand_decisions':{'table':'oi_old_decisions'}}}
        new={'run_id':'new','tables':{'opportunities':{'table':'oi_new_opportunities'},'demand_decisions':{'table':'oi_new_decisions'}}}
        aws=CaptureAWS();repo=Repository(aws);repo._manifest=old;repo._expires=float('inf')
        def result(sql):
            aws.sql=sql;repo._manifest=new;return [],{}
        aws.result=result
        app=FastAPI();app.include_router(create_router(verified_application_principal,repo));client=TestClient(app)
        response=client.get('/opportunity-intelligence/opportunities',headers={'X-Mimir-Export-Key':'test-proxy-secret','X-Mimir-Opportunity-User':'11111111-1111-4111-8111-111111111111'})
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.json()['release'],'old')
        self.assertIn('oi_old_opportunities',aws.sql)
        self.assertEqual(repo.manifest()['run_id'],'new')

    def test_invalid_company_or_kind_rejected(self):
        repo = Repository(CaptureAWS()); repo.table = lambda name: 'safe.' + name
        for company, kind in [("CAGE:45934' OR 1=1", 'work'), ('CAGE:45934', 'arbitrary')]:
            with self.assertRaises(ValueError): repo.opportunities(company, 24, 0, False, kind)

    def test_refresh_health_redacts_errors_and_detects_stale_running_reports(self):
        class S3:
            def __init__(self,value):self.value=value
            def get_object(self,**kwargs):return {'Body':io.BytesIO(json.dumps(self.value).encode())}
        aws=CaptureAWS();aws.s3=S3({'status':'failed_preserved_previous_current_pointer','error':'private infrastructure diagnostic','finished_at':'2026-10-30T05:00:00+00:00'})
        repo=Repository(aws);status=repo.refresh_status()
        self.assertNotIn('error',status)
        self.assertEqual(status['status'],'failed_preserved_previous_current_pointer')
        aws.s3.value={'status':'running','started_at':'2020-01-01T00:00:00+00:00'}
        self.assertEqual(repo.refresh_status()['status'],'status_stale')
        aws.s3.value={'status':'running','started_at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
        self.assertEqual(repo.refresh_status()['status'],'running')

    def test_deployed_lazy_mount_pins_the_concrete_repository_after_authentication(self):
        old={'run_id':'old','tables':{'opportunities':{'table':'oi_old_opportunities'},'demand_decisions':{'table':'oi_old_decisions'}}}
        new={'run_id':'new','tables':{'opportunities':{'table':'oi_new_opportunities'},'demand_decisions':{'table':'oi_new_decisions'}}}
        aws=CaptureAWS();repo=Repository(aws);repo._manifest=old;repo._expires=float('inf')
        def result(sql):
            aws.sql=sql;repo._manifest=new;return [],{}
        aws.result=result
        lazy=LazyRepository()
        with patch('opportunity_intelligence.mount.Repository',return_value=repo) as constructor:
            app=FastAPI();app.include_router(create_router(verified_application_principal,lazy));client=TestClient(app)
            self.assertEqual(client.get('/opportunity-intelligence/opportunities').status_code,401)
            constructor.assert_not_called()
            response=client.get('/opportunity-intelligence/opportunities',headers={'X-Mimir-Export-Key':'test-proxy-secret','X-Mimir-Opportunity-User':'11111111-1111-4111-8111-111111111111'})
            constructor.assert_called_once_with()
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.json()['release'],'old')
        self.assertIn('oi_old_opportunities',aws.sql)
        self.assertEqual(repo.manifest()['run_id'],'new')

if __name__ == '__main__': unittest.main()
