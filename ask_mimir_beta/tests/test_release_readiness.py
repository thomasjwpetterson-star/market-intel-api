import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import duckdb
from pydantic import ValidationError
from beta_controls import AccessContext, BetaStateStore, EvidencePackCache, sanitize_customer_payload, validate_answer_citations, link_evidenced_award_identifiers
from fastapi import FastAPI
from fastapi.testclient import TestClient
from platform_context import PlatformContextStore, _rows
from research_safety import SynchronizedStore, write_bounded_audit_record, configure_duckdb_scratch
from company_context import CompanyContextBuilder
from company_context_store import _bounded_precomputed_context, _customer_product_evidence
from reviewed_platform_links import recovered_platform_sql
from lab_test_support import load_lab

lab = load_lab()


class StopAtEvidence(Exception):
    pass


class ExecutionBoundaryTests(unittest.TestCase):
    def test_completed_answer_export_uses_saved_platform_evidence(self):
        import io, zipfile

        app = FastAPI()
        app.get('/api/evidence/answer.zip')(lab.answer_evidence_export_download)
        pack = {
            'scope': {'platform_id': 'M109 PALADIN', 'display_name': 'M109A7 Paladin'},
            'annual_activity': {'records': []},
            'direct_award_recipients': [],
            'reported_supplier_sites': [],
            'reported_component_categories': [],
            'item_and_component_evidence': {
                'top_items': [], 'top_item_supplier_sites': [],
                'authorized_source_depth': {},
            },
            'top_prime_awards': [],
            'current_opportunities': [],
            'evidence_index': [],
        }
        job = {
            'status': 'completed',
            'result': {
                'response_id': 'resp-owned',
                'answer': 'Completed research answer.',
                'answer_artifacts': {'platform_dossier': pack},
            },
        }
        with patch.object(
            lab, 'require_evidence_download', return_value=AccessContext('alice', 'enterprise', True)
        ), patch.object(lab, 'job_manager', SimpleNamespace(get=Mock(return_value=job)), create=True):
            response = TestClient(app).get(
                '/api/evidence/answer.zip',
                params={'request_id': 'request-owned', 'response_id': 'resp-owned'},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['content-type'], 'application/zip')
        self.assertIn('mimir-platform-m109-paladin-evidence.zip', response.headers['content-disposition'])
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            self.assertIn('README.txt', archive.namelist())

    def test_completed_answer_export_rejects_response_mismatch(self):
        app = FastAPI()
        app.get('/api/evidence/answer.zip')(lab.answer_evidence_export_download)
        job = {
            'status': 'completed',
            'result': {
                'response_id': 'resp-owned',
                'answer': 'Completed research answer.',
                'answer_artifacts': {},
            },
        }
        with patch.object(
            lab, 'require_evidence_download', return_value=AccessContext('alice', 'enterprise', True)
        ), patch.object(lab, 'job_manager', SimpleNamespace(get=Mock(return_value=job)), create=True):
            response = TestClient(app).get(
                '/api/evidence/answer.zip',
                params={'request_id': 'request-owned', 'response_id': 'resp-other'},
            )
        self.assertEqual(response.status_code, 409)

    def test_variant_export_preserves_requested_focus(self):
        app = FastAPI()
        app.get('/api/evidence/platform.zip')(lab.universal_platform_evidence_export)
        store = Mock(get_export_context=Mock(return_value={'scope': {'display_name': 'M109A7'}}))
        runtime = SimpleNamespace(platform_contexts=store, optional_program_outlook=Mock(return_value=None))
        with patch.object(lab, 'runtime', runtime, create=True), patch.object(lab, 'require_evidence_download'), patch.object(lab, 'build_platform_context_zip', return_value=b'zip'), patch.object(lab, 'platform_context_filename', return_value='m109a7.zip'):
            response = TestClient(app).get('/api/evidence/platform.zip', params={'platform_id':'M109 PALADIN','focus_id':'M109A7'})
            self.assertEqual(response.status_code, 200)
        store.get_export_context.assert_called_once_with('M109 PALADIN', limit=5000, focus_id='M109A7')

    def test_variant_totals_and_exports_are_independent_of_display_limit(self):
        import csv, io, zipfile
        from platform_context_export import build_platform_context_zip
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            con = duckdb.connect()
            con.execute("""COPY (SELECT 'USA_SPENDING' source_system, 'UNMAPPED' platform_family,
                'Award-' || i contract_id, 'Prime' vendor_name, '12345' vendor_cage,
                'M109A7 FOV PRODUCTION' base_award_description, '' action_description,
                '' description, 2025 AS year, 100.0 spend_amount, '2025-06-01' action_date,
                0.0 platform_attributed_spend_amount, 0.0 shared_use_exposure_amount,
                'Award-' || i award_key FROM range(45) t(i)) TO ? (FORMAT PARQUET)""", [str(root/'transactions.parquet')])
            con.execute("""COPY (SELECT lpad(i::varchar,5,'0') sub_cage, 'Supplier' sub_name,
                'City' sub_city, 'ST' sub_state, 10.0 subaward_value, 2025 AS year,
                'M109A7 FOV PRODUCTION' prime_award_description, 'Material' description,
                'UNMAPPED' platform_family FROM range(60) t(i)) TO ? (FORMAT PARQUET)""", [str(root/'network.parquet')])
            store = object.__new__(PlatformContextStore)
            store.connection = con
            store.paths = {name: root/(name+'.parquet') for name in ['network','transactions']}
            store.search = Mock(return_value={'resolved_platform_id': 'M109 PALADIN'})
            small = store.answer_projection('M109 PALADIN', supplier_limit=1, focus_id='M109A7')
            large = store.answer_projection('M109 PALADIN', supplier_limit=50, focus_id='M109A7')
            self.assertEqual(small['financial_totals'], large['financial_totals'])
            self.assertEqual(small['coverage']['reported_supplier_sites'], 60)
            self.assertEqual(small['coverage']['prime_awards'], 45)
            self.assertEqual(small['coverage']['direct_award_recipient_sites'], 1)
            self.assertEqual(len(small['direct_award_recipients']), 1)
            self.assertEqual(small['direct_award_recipients'][0]['award_count'], 45)
            self.assertNotIn('associated_niins', small['coverage'])
            self.assertNotIn('associated_niin_count', small['item_and_component_evidence'])
            self.assertEqual(len(small['reported_supplier_sites']), 1)
            self.assertEqual(len(large['reported_supplier_sites']), 50)
            self.assertEqual(small['financial_totals']['mimir_modelled_reported_subcontract_value_usd'], 600)
            self.assertEqual(sum(row['net_prime_obligations_usd'] for row in small['annual_activity']['records']), 4500)
            export = store.get_export_context('M109 PALADIN', focus_id='M109A7')
            bundle = zipfile.ZipFile(io.BytesIO(build_platform_context_zip(export)))
            for filename, expected in [('02_direct_award_recipients.csv',1), ('03_reported_supplier_sites.csv',60), ('07_prime_awards.csv',45)]:
                rows = list(csv.DictReader(io.StringIO(bundle.read(filename).decode('utf-8-sig'))))
                self.assertEqual(len(rows), expected)
            con.close()

    def test_reviewed_paladin_production_recovery_excludes_shared_fleet_support(self):
        con = duckdb.connect()
        expression = recovered_platform_sql('n', 'network')
        rows = con.execute(f"""SELECT {expression} FROM (VALUES
            ('UNMAPPED', 'LETTER CONTRACT FOR EARLY PROCUREMENT MATERIAL TO SUPPORT THE M109A7/M992A3 VEHICLE PRODUCTION.', ''),
            ('UNMAPPED', 'SUPPORT OF BRADLEY, AMPV AND PALADIN INTEGRATED MANAGEMENT', ''),
            ('OTHER', 'M109A7 FOV PRODUCTION', '')
            ) n(platform_family, prime_award_description, description)""").fetchall()
        self.assertEqual(rows, [('M109 PALADIN',), ('UNMAPPED',), ('OTHER',)])
        con.close()

    def test_outlook_with_suppliers_uses_forward_mode_not_generic_supplier_inventory(self):
        pack = {}
        def tool(name, arguments):
            if name == 'search_platform_contexts':
                return {'resolved_platform_id': 'T-7', 'matches': [{'platform_id': 'T-7'}]}
            if name == 'get_platform_context':
                return pack
            raise AssertionError(name)
        runtime = SimpleNamespace(mock_mode=False, external_evidence_allowed=True,
            platform_contexts=Mock(mentions=Mock(return_value=['T-7'])),
            company_contexts=Mock(search=Mock(return_value={'matches': []})),
            call_tool=tool, optional_program_outlook=Mock(return_value={'evidence_lanes': {}}),
            model='test', reasoning_effort='high', max_output_tokens=10000)
        client = Mock()
        client.responses.create.side_effect = StopAtEvidence
        request = lab.AskRequest(messages=[{'role': 'user', 'content': 'What is the five-year outlook for the T-7A Red Hawk program, and which suppliers appear best positioned?'}])
        route = lab.RoutingDecision(workflow='platform_intelligence', reason='explicit_platform', confidence=1)
        with patch.object(lab, 'runtime', runtime, create=True), patch.object(lab, 'OpenAI', return_value=client), patch.dict('os.environ', {'OPENAI_API_KEY': 'test-placeholder'}):
            with self.assertRaises(StopAtEvidence):
                lab.generate_answer(request, routing=route)
        self.assertEqual(pack['requested_answer_mode'], 'program_outlook')
        self.assertIn('Do not append a general parts/NIIN inventory', client.responses.create.call_args.kwargs['instructions'])

    def test_new_company_dispatch_is_independent_of_previous_scope(self):
        company = {"scope_type":"company_parent", "scope_id":"MAROTTA", "scope_name":"MAROTTA CONTROLS", "resolved_cages":["99657"]}
        for scope in [None, {"scope_type":"platform", "scope_id":"LCAC"}, {"scope_type":"product_family", "scope_id":"VALVES"}, {"scope_type":"capability_market", "scope_id":"MISSILES"}]:
            calls = []
            def tool(name, arguments):
                calls.append(name)
                if name == "search_company_contexts":
                    return {"matches":[company]}
                raise StopAtEvidence()
            runtime = SimpleNamespace(mock_mode=False, external_evidence_allowed=True,
                platform_contexts=Mock(mentions=Mock(return_value=[])),
                company_contexts=Mock(search=Mock(return_value={"matches":[company]})),
                call_tool=tool)
            request = lab.AskRequest(messages=[
                {"role":"user", "content":"Tell me about LCAC"},
                {"role":"assistant", "content":"LCAC report"},
                {"role":"user", "content":"What does Marotta Controls do in the US defense market?"}], active_scope=scope)
            with self.subTest(scope=scope), patch.object(lab, "runtime", runtime, create=True), patch.object(lab, "OpenAI", return_value=Mock()), patch.dict("os.environ", {"OPENAI_API_KEY":"test-placeholder"}):
                routing = lab.routing_decision_for_request(request)
                self.assertEqual(routing.workflow, "company_site_intelligence")
                with self.assertRaises(StopAtEvidence):
                    lab.generate_answer(request, routing=routing)
                self.assertEqual(calls, ["search_company_contexts", "get_company_context"])

    def test_generated_cage_site_follow_up_uses_dedicated_dossier_route(self):
        question = "Tell me more about the US defense activity at TELEDYNE BROWN ENGINEERING, INC., CAGE 14925."
        request = lab.AskRequest(messages=[{"role":"user", "content":question}])
        runtime = SimpleNamespace(
            platform_contexts=Mock(mentions=Mock(return_value=[])),
            company_contexts=Mock(search=Mock(return_value={"matches":[]})),
        )
        with patch.object(lab, "runtime", runtime, create=True):
            routing = lab.routing_decision_for_request(request)
        self.assertEqual(lab.company_site_dossier_cage(request.messages), "14925")
        self.assertEqual(routing.workflow, "company_site_intelligence")

    def test_genuine_follow_up_keeps_compatible_scope(self):
        request = lab.AskRequest(messages=[{"role":"user","content":"What is its forward outlook?"}], active_scope={"scope_type":"platform","scope_id":"AMRAAM"})
        route = lab.RoutingDecision(workflow="platform_intelligence", reason="follow_up", confidence=1)
        self.assertEqual(lab.request_for_execution(request, route).active_scope.scope_id, "AMRAAM")

    def test_long_assistant_report_is_valid_history_but_user_input_stays_bounded(self):
        request = lab.AskRequest(messages=[{"role":"assistant","content":"A" * 60000}, {"role":"user","content":"Explore suppliers"}])
        self.assertEqual(len(request.messages[0].content), 60000)
        with self.assertRaises(ValidationError):
            lab.ChatMessage(role="user", content="A" * 12001)


class ConcurrentEvidenceTests(unittest.TestCase):
    def test_duckdb_connections_do_not_share_spill_files(self):
        with tempfile.TemporaryDirectory() as root, patch.dict('os.environ', {'ASK_MIMIR_DUCKDB_TEMP': root}):
            first, second = duckdb.connect(), duckdb.connect()
            try:
                a = configure_duckdb_scratch(first, 'evidence')
                b = configure_duckdb_scratch(second, 'evidence')
                self.assertNotEqual(a, b)
                self.assertEqual(Path(first.execute("SELECT current_setting('temp_directory')").fetchone()[0]), a)
                self.assertEqual(Path(second.execute("SELECT current_setting('temp_directory')").fetchone()[0]), b)
            finally:
                first.close()
                second.close()

    def test_audit_history_is_bounded_and_keeps_recent_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'audit.jsonl'
            for index in range(20):
                write_bounded_audit_record(path, {'number': index, 'text': 'x'*100}, max_bytes=300, backups=2)
            self.assertLessEqual(len(list(Path(tmp).iterdir())), 3)
            self.assertIn('"number": 19', path.read_text())

    def test_immutable_catalog_lookup_is_not_blocked_by_evidence_work(self):
        lock=threading.RLock()
        store=SynchronizedStore(SimpleNamespace(search=lambda value:{'platform':value}),lock,frozenset({'search'}))
        complete=threading.Event()
        with lock:
            thread=threading.Thread(target=lambda:(store.search('AMRAAM'),complete.set()))
            thread.start()
            self.assertTrue(complete.wait(1),'Catalog lookup waited behind unrelated evidence work')
        thread.join(2)

    def test_complete_fetch_is_locked_and_cache_values_are_detached(self):
        class Store:
            def __init__(self):
                self.connection = duckdb.connect()
                self.cached = {"items": []}
            def read(self, name):
                cursor = self.connection.execute("SELECT ? AS scope", [name])
                time.sleep(0.01)
                return _rows(cursor)
            def get(self):
                return self.cached
        raw = Store()
        store = SynchronizedStore(raw, threading.RLock())
        results = {}
        threads = [threading.Thread(target=lambda name=name: results.update({name:store.read(name)})) for name in "ABCDEF"]
        for thread in threads: thread.start()
        for thread in threads: thread.join(5)
        self.assertEqual(results, {name:[{"scope":name}] for name in "ABCDEF"})
        store.get()["items"].append("private request decoration")
        self.assertEqual(raw.cached, {"items":[]})
        raw.connection.close()


class DurableJobTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ledger = BetaStateStore(Path(self.tmp.name)/"state.sqlite")
        self.access = AccessContext("alice", "professional", True)
        self.request = lab.AskRequest(messages=[{"role":"user","content":"Tell me about AMRAAM"}], client_request_id="a"*32)
        self.route = lab.RoutingDecision(workflow="platform_intelligence", reason="test", confidence=1)
        self.runtime = SimpleNamespace(beta_state=self.ledger, release_guard=SimpleNamespace(release_binding_id="test",assert_unchanged=lambda:None),write_audit_record=Mock())
        self.patch = patch.object(lab, "runtime", self.runtime, create=True)
        self.patch.start()
        self.manager = lab.AskJobManager()
        self.manager.executor.shutdown()
        self.manager.executor = Mock()

    def tearDown(self):
        self.patch.stop()
        self.ledger.connection.close()
        self.tmp.cleanup()

    def test_full_job_completion_replays_after_restart_without_rebilling(self):
        self.manager.create(self.request, self.access, self.route)
        answer = {
            "answer": "AMRAAM report",
            "response_id": "test-answer",
            "answer_artifacts": {
                "platform_dossier": {"scope": {"platform_id": "AMRAAM"}},
                "evidence_pack": {
                    "format": "zip",
                    "download_url": "/api/evidence/platform.zip?platform_id=AMRAAM",
                },
            },
            "tool_trace": [],
            "model": "test",
        }
        with patch.object(lab, "generate_answer", return_value=answer), patch.object(lab, "finalize_customer_result", side_effect=lambda result,*args:dict(result)):
            self.manager._run("a"*32, self.request, self.access, self.route)
        self.assertEqual(len(self.manager.jobs), 0)
        self.assertEqual(self.ledger.used_today("alice"), 1)
        restarted = lab.AskJobManager()
        try:
            recovered = restarted.get("a"*32, self.access)
            self.assertEqual(recovered["status"], "completed")
            self.assertEqual(recovered["result"]["answer"], "AMRAAM report")
            self.assertEqual(
                recovered["result"]["answer_artifacts"]["evidence_pack"]["download_url"],
                "/api/evidence/answer.zip?request_id=" + "a"*32 + "&response_id=test-answer",
            )
            duplicate, _ = restarted.create(self.request, self.access, self.route)
            self.assertTrue(duplicate["deduplicated"])
            self.assertEqual(self.ledger.used_today("alice"), 1)
            with self.assertRaises(KeyError):
                restarted.get("a"*32, AccessContext("bob","professional",True))
        finally:
            restarted.executor.shutdown()

    def test_admission_rejection_does_not_reserve_allowance(self):
        self.manager.capacity = 1
        self.manager.create(self.request, self.access, self.route)
        second = self.request.model_copy(update={"client_request_id":"b"*32})
        with self.assertRaises(lab.HTTPException) as exc:
            self.manager.create(second, self.access, self.route)
        self.assertEqual(exc.exception.status_code, 503)
        self.assertEqual(self.ledger.used_today("alice"), 1)

    def test_low_disk_rejection_does_not_reserve_allowance(self):
        with patch.object(self.ledger, 'has_write_capacity', return_value=False):
            with self.assertRaises(lab.HTTPException) as exc:
                self.manager.create(self.request, self.access, self.route)
        self.assertEqual(exc.exception.status_code, 503)
        self.assertEqual(self.ledger.used_today('alice'), 0)
        self.assertFalse(self.manager.jobs)

    def test_result_and_billing_commit_together(self):
        self.ledger.reserve("test", self.access,"release","workflow")
        with patch.object(self.ledger, "_save_job_unlocked", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                self.ledger.complete("test", latency_ms=1, estimated_cost_usd=0, job={})
        status = self.ledger.connection.execute("SELECT status FROM query_events WHERE request_id='test'").fetchone()[0]
        self.assertEqual(status, "reserved")

    def test_http_retry_uses_original_fingerprint_after_server_scope_changes(self):
        app = FastAPI()
        app.post('/api/ask/jobs', status_code=202)(lab.create_ask_job)
        request = self.request.model_copy(update={"conversation_id":"test-conversation", "active_scope":lab.ActiveScope(scope_type='platform',scope_id='LCAC')})
        client = TestClient(app)
        with patch.object(lab,'job_manager',self.manager,create=True), patch.object(lab,'access_from_request',return_value=self.access), patch.object(lab,'routing_decision_for_request',return_value=self.route), patch.object(lab,'validate_routing_decision',return_value=self.route):
            first=client.post('/api/ask/jobs',json=request.model_dump())
            self.assertEqual(first.status_code,202,first.text)
            self.ledger.save_conversation_scope('test-conversation','alice',{'scope_type':'platform','scope_id':'AMRAAM'},'platform_intelligence')
            second=client.post('/api/ask/jobs',json=request.model_dump())
            self.assertEqual(second.status_code,202,second.text)
            self.assertTrue(second.json()['deduplicated'])
            self.assertEqual(self.ledger.used_today('alice'),1)

    def test_pdf_uses_owned_saved_answer_and_rejects_clarification(self):
        app = FastAPI()
        app.post('/api/evidence/answer.pdf')(lab.answer_report_export)
        app.get('/api/evidence/answer.pdf')(lab.answer_report_export_download)
        job = {'request_id':'pdf-request','subject_id':'alice','status':'completed',
            '_question':'Original question', 'result':{'response_id':'pdf-response',
            'answer':'Saved research', 'active_scope':{'scope_name':'AMRAAM'}}}
        self.ledger.save_job(job)
        client = TestClient(app)
        payload = {'request_id':'pdf-request','response_id':'pdf-response','answer':'Forged answer'}
        with patch.object(lab,'job_manager',self.manager,create=True), patch.object(lab,'require_report_download',return_value=self.access), patch.object(lab,'build_branded_answer_pdf',return_value=b'%PDF-test') as render:
            response = client.post('/api/evidence/answer.pdf',json=payload)
            self.assertEqual(response.status_code,200,response.text)
            render.assert_called_once_with(question='Original question',answer='Saved research',scope_name='AMRAAM')
            get_response = client.get('/api/evidence/answer.pdf',params={'request_id':'pdf-request','response_id':'pdf-response'})
            self.assertEqual(get_response.status_code,200,get_response.text)
            self.assertEqual(get_response.headers['content-type'],'application/pdf')
            with patch.object(lab,'require_report_download',return_value=AccessContext('bob','enterprise',True)):
                self.assertEqual(client.post('/api/evidence/answer.pdf',json=payload).status_code,404)
            job['result']['requires_clarification'] = True
            self.ledger.save_job(job)
            self.assertEqual(client.post('/api/evidence/answer.pdf',json=payload).status_code,409)

    def test_sync_endpoint_cannot_bypass_production_queue(self):
        app = FastAPI()
        app.post('/api/ask')(lab.ask_direct)
        with patch.dict('os.environ',{'ASK_MIMIR_ENABLE_SYNC_EVALUATION':'0'}):
            response = TestClient(app).post('/api/ask',json=self.request.model_dump())
        self.assertEqual(response.status_code,410)
        self.assertEqual(self.ledger.used_today('alice'),0)


class EvidenceCacheTests(unittest.TestCase):
    def test_customer_links_use_real_mimir_routes(self):
        answer = '[Parker](https://askmimir.com/cage/59211) [Part](https://askmimir.com/niin/016506030) [Site](https://www.mimiradvisors.org/tools/cage-code-lookup?query=8MQW5) [Sentinel](https://www.mimiradvisors.org/ask-mimir?query=Sentinel)'
        clean = lab.sanitize_answer_text(answer)
        self.assertNotIn('askmimir.com', clean)
        self.assertIn('view=COMPANY&cage=59211', clean)
        self.assertIn('view=PARTS&nsn=016506030', clean)
        self.assertIn('view=COMPANY&cage=8MQW5', clean)
        self.assertIn('/ask-mimir?q=Sentinel', clean)
        self.assertEqual(lab.sanitize_answer_text('modelled reported subcontract value'), 'reported subcontract value')

    def test_internal_fydp_and_generic_financial_qualifications_are_removed(self):
        answer = (
            "Useful finding.\n\n"
            "- No explicit public FYDP linkage was found for the site's mapped platform positions in the structured evidence.\n"
            "Obligations are contract-action values, not site revenue or production output.\n"
            "This is a separate, non-additive lane totaling $12 million."
        )
        clean = lab.sanitize_answer_text(answer)
        self.assertNotIn("structured evidence", clean)
        self.assertNotIn("not site revenue", clean)
        self.assertIn("This totals $12 million", clean)
        self.assertNotIn("\n-\n", clean)

    def test_lookup_teaser_links_are_normalized_for_both_query_spellings(self):
        for parameter in ['q', 'query']:
            clean = lab.sanitize_answer_text(f'[Site](https://www.mimiradvisors.org/tools/cage-code-lookup?{parameter}=59211) [Part](https://www.mimiradvisors.org/tools/nsn-lookup?{parameter}=015427593)')
            self.assertIn('view=COMPANY&cage=59211', clean)
            self.assertIn('view=PARTS&nsn=015427593', clean)
            self.assertNotIn('/tools/', clean)

    def test_verified_award_mentions_are_clickable_without_rewriting_existing_links(self):
        url = 'https://www.mimiradvisors.org/dashboard?view=AWARDS&award=FA862622C0002'
        existing = f'[GE award]({url})'
        answer = f'**FA862622C0002** and {existing}; unrelated FA000000C9999; `FA862622C0002`'
        trace = [{'result': {'contract_id': 'FA862622C0002'}}]
        linked = link_evidenced_award_identifiers(answer, trace)
        self.assertIn(f'**[FA862622C0002]({url})**', linked)
        self.assertIn(existing, linked)
        self.assertIn('unrelated FA000000C9999', linked)
        self.assertIn('`FA862622C0002`', linked)
        self.assertEqual(link_evidenced_award_identifiers(linked, trace), linked)
        self.assertEqual(validate_answer_citations(linked, trace)['status'], 'pass')

    def test_source_links_cannot_execute_script_or_point_to_private_hosts(self):
        for url in ['javascript:alert(1)','java\nscript:alert(1)','http://10.0.0.1/report','http://[::1]/report','file:///private/report','https://user:password@example.com/report']:
            with self.subTest(url=url):
                self.assertIsNone(sanitize_customer_payload({'source_url':url})['source_url'])
        self.assertEqual(sanitize_customer_payload('https://www.navy.mil/report'),'https://www.navy.mil/report')
        self.assertEqual(validate_answer_citations('[Source](http://192.168.0.1/report)',[])['status'],'fail')

    def test_cache_returns_isolated_packs_and_bounds_memory(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache=EvidencePackCache(Path(tmp))
            key=cache.cache_key('release','platform',{'id':'AMRAAM'})
            pack={'scope':{'id':'AMRAAM'}}
            cache.set(key,pack)
            pack['scope']['question']='First private question'
            value=cache.get(key)
            self.assertNotIn('question',value['scope'])
            value['scope']['question']='Second private question'
            self.assertNotIn('question',cache.get(key)['scope'])
            cache.max_memory_bytes=10
            cache.set(cache.cache_key('release','other',{}),{'data':'x'*100})
            self.assertEqual(len(cache.memory),0)
            self.assertEqual(cache.get(key)['scope']['id'],'AMRAAM')


class AggregateAndLinkTests(unittest.TestCase):
    def test_platform_item_supplier_values_use_same_dla_transactions_as_totals(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            con = duckdb.connect()
            con.execute("""COPY (SELECT * FROM (VALUES
                ('000000001','AAAAA','Supplier A',100.0,100.0,0.0,'F-35','F-35',1,2025,'DLA','C1','2025-01-01'),
                ('000000001','AAAAA','Supplier A',-20.0,-20.0,0.0,'F-35','F-35',1,2025,'DLA','C1','2025-01-02'),
                ('000000002','BBBBB','Supplier B',60.0,0.0,60.0,NULL,'F-35 | F-16',2,2025,'DLA','C2','2025-01-03'),
                ('000000001','AAAAA','Supplier A',9000.0,9000.0,0.0,'F-35','F-35',1,2025,'USA_SPENDING','C3','2025-01-04')
                ) t(niin,vendor_cage,vendor_name,spend_amount,platform_attributed_spend_amount,shared_use_exposure_amount,platform_family,platform_families,platform_count,year,source_system,contract_id,action_date)) TO ? (FORMAT PARQUET)""", [str(root/'transactions.parquet')])
            con.execute("COPY (SELECT 'AAAAA' AS cage_code, 'City' AS city, 'VA' AS state, 'verified' AS location_quality) TO ? (FORMAT PARQUET)", [str(root/'locations.parquet')])
            store = object.__new__(PlatformContextStore)
            store.connection = con
            store.paths = {'transactions': root/'transactions.parquet', 'locations': root/'locations.parquet'}
            rows = store._item_supplier_sites('F-35')
            self.assertEqual(sum(row['attributed_dla_procurement_value_usd'] for row in rows), 80)
            self.assertEqual(sum(row['shared_use_niin_exposure_usd'] for row in rows), 60)
            self.assertEqual(store._item_supplier_sites('F-35', limit=1)[0]['total_supplier_sites'], 2)
            self.assertTrue(all(row['observed_units'] is None for row in rows))
            con.close()

    def test_reviewed_columbia_label_correction_does_not_remap_other_awards(self):
        con = duckdb.connect()
        expression = recovered_platform_sql('n', 'network')
        rows = con.execute(f"""SELECT {expression} FROM (VALUES
            ('COLUMBIA CLASS SSN', '', ''),
            ('VIRGINIA CLASS (SSN 774)', '', ''),
            ('OTHER PLATFORM', 'Ship-to-Shore Connector', '')
            ) n(platform_family, prime_award_description, description)""").fetchall()
        self.assertEqual(rows, [('COLUMBIA CLASS SSBN',), ('VIRGINIA CLASS (SSN 774)',), ('OTHER PLATFORM',)])
        con.close()

    def test_precomputed_platform_with_old_group_members_is_rebuilt(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'old.json'
            path.write_text(json.dumps({'scope': {'platform_id': 'COLUMBIA CLASS SSBN', 'included_platform_records': ['COLUMBIA CLASS SSBN']}}))
            store = object.__new__(PlatformContextStore)
            store.search = Mock(return_value={'resolved_platform_id': 'COLUMBIA CLASS SSBN'})
            store._cache = {}
            store._precomputed_paths = {'COLUMBIA CLASS SSBN': path}
            store._annual_activity = Mock(side_effect=StopAtEvidence)
            with self.assertRaises(StopAtEvidence):
                store.get('COLUMBIA CLASS SSBN')
            store._annual_activity.assert_called_once()

    def test_budget_names_resolve_without_erasing_specific_historical_variants(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'catalog.parquet'
            con=duckdb.connect()
            con.execute("COPY (SELECT unnest(['C-17A','E-3 AWACS','E-4 (AABNCP)','M88 RECOVERY VEHICLE','M88A2 HERCULES','AC-130']) AS platform_family) TO ? (FORMAT PARQUET)",[str(path)])
            store=object.__new__(PlatformContextStore)
            store.connection=con
            store.paths={key:path for key in ['network','transactions','platform_bom']}
            store.platforms=store._load_platform_catalog()
            for query,expected in [('C-17','C-17A'),('E-3','E-3 AWACS'),('E-4B','E-4 (AABNCP)'),('M88','M88'),('THAAD','THAAD'),('AC-130','AC-130'),('C-130','C-130')]:
                with self.subTest(query=query):
                    self.assertEqual(store.search(query)['resolved_platform_id'],expected)
            con.close()

    def test_bounded_items_keep_full_denominator_and_route_summary(self):
        product = {"summary":{"observed_financial_niin_count":3},
            "niin_financial_observations":[{"niin":str(i),"dla_procurement_value_usd":v} for i,v in enumerate([300,200,100])],
            "third_party_dla_procurement_routes":[{"recipient_cage":str(i),"niin":str(i),"dla_procurement_value_usd":v} for i,v in enumerate([300,200,100])]}
        bounded = _bounded_precomputed_context({"product_and_part_evidence":product},row_limit=2)
        customer = _customer_product_evidence(bounded["product_and_part_evidence"])
        self.assertEqual(customer["representative_niin_examples"][0]["share_of_observed_dla_procurement_pct"],50)
        self.assertEqual(customer["third_party_dla_route_summary"]["other_same_item_route_procurement_value_usd"],600)

    def test_supplier_concentration_uses_all_sites_not_250(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'network.parquet'
            con = duckdb.connect()
            con.execute("""COPY (SELECT 'F-35' AS platform_family, lpad(i::varchar,5,'0') AS sub_cage,
                1.0 AS subaward_value,2025 AS year FROM range(1000) t(i)) TO ? (FORMAT PARQUET)""",[str(path)])
            store = object.__new__(PlatformContextStore)
            store.connection = con
            store.paths = {"network":path}
            summary = store._supplier_concentration('F-35')
            self.assertEqual(summary['supplier_site_count'],1000)
            self.assertEqual(summary['top_supplier_share_pct'],0.1)
            con.close()

    def test_ssc_company_and_platform_recovery_agree(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            con=duckdb.connect()
            con.execute("""COPY (SELECT 'UNMAPPED' AS platform_family,'3BNF6' AS sub_cage,
                51013399.58 AS subaward_value, 2025 AS year, 'Ship-to-Shore Connector' AS prime_award_description,
                'Gearing' AS description, 'award1' AS source_dedup_key, '2025-01-01' AS action_date)
                TO ? (FORMAT PARQUET)""",[str(root/'network.parquet')])
            con.execute("""COPY (SELECT 'USA_SPENDING' AS source_system, 'UNMAPPED' AS platform_family,
                'OTHER' AS vendor_cage, 2025 AS year, 0.0 AS spend_amount, 'award' AS award_key,
                '2025-01-01' AS action_date, 'none' AS base_award_description, '' AS action_description, '' AS description)
                TO ? (FORMAT PARQUET)""",[str(root/'transactions.parquet')])
            store=object.__new__(PlatformContextStore)
            store.connection=con
            store.paths={'network':root/'network.parquet'}
            company=object.__new__(CompanyContextBuilder)
            company.connection=con
            company.paths={**store.paths,'transactions':root/'transactions.parquet'}
            exposure=company._platform_exposure(['3BNF6'],[2025])
            self.assertEqual(exposure[0]['platform_family'],'LCAC')
            self.assertAlmostEqual(float(exposure[0]['observed_value_usd']),store._supplier_concentration('LCAC')['positive_reported_subcontract_value_usd'])
            con.close()

    def test_article_context_rejects_external_or_traversal_paths(self):
        for value in ['https://evil.example/', '/analysis/../../secret', '/analysis/test?redirect=elsewhere']:
            with self.subTest(value=value), self.assertRaises(ValidationError):
                lab.AskRequest(messages=[{'role':'user','content':'Explain implications'}],article_context=value)

    def test_outlook_failure_is_retried_and_marked_not_missing_evidence(self):
        runtime=object.__new__(lab.LabRuntime)
        runtime.program_outlook=Mock(supports=Mock(return_value=True))
        runtime.call_tool=Mock(side_effect=RuntimeError('temporary read failure'))
        with patch.object(lab.LOGGER,'exception'):
            result=runtime.optional_program_outlook('T-7')
        self.assertEqual(runtime.call_tool.call_count,2)
        self.assertEqual(result['retrieval_status'],'temporarily_unavailable')
