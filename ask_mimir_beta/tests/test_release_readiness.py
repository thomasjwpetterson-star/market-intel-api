import json
import base64
import hashlib
import hmac
import os
import tempfile
import threading
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
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


def direct_access_token(secret, subject, tier="public", *, user_agent="test-browser", expires_in=3600):
    now = int(time.time())
    payload = base64.urlsafe_b64encode(json.dumps({
        "aud": "ask-mimir-direct-v1",
        "sub": subject,
        "tier": tier,
        "iat": now,
        "exp": now + expires_in,
        "uah": hashlib.sha256(user_agent.encode()).hexdigest(),
    }, separators=(",", ":")).encode()).decode().rstrip("=")
    signature = base64.urlsafe_b64encode(
        hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest()
    ).decode().rstrip("=")
    return f"{payload}.{signature}"


class DirectAccessTests(unittest.TestCase):
    def setUp(self):
        self.secret = "direct-access-test-secret"
        self.user_agent = "test-browser"
        app = FastAPI()

        @app.get("/access")
        def access(request: lab.Request):
            resolved = lab.access_from_request(request)
            return {
                "subject_id": resolved.subject_id,
                "tier": resolved.tier,
                "authenticated": resolved.authenticated,
            }

        self.client = TestClient(app)

    def request(self, token):
        with patch.dict(os.environ, {"ASK_MIMIR_TRUSTED_PROXY_SECRET": self.secret}):
            return self.client.get("/access", headers={
                "Authorization": f"Bearer {token}",
                "User-Agent": self.user_agent,
            })

    def test_direct_grant_preserves_the_proxy_quota_subject(self):
        subject = "guest:68fe3c4f-5787-4f18-85ce-93023919d39a"
        response = self.request(direct_access_token(
            self.secret, subject, user_agent=self.user_agent
        ))
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json(), {
            "subject_id": f"user:{subject}",
            "tier": "public",
            "authenticated": False,
        })

    def test_direct_grant_rejects_tampering_expiry_and_a_different_browser(self):
        valid = direct_access_token(self.secret, "guest:abc", user_agent=self.user_agent)
        tampered = f"{valid[:-1]}{'A' if valid[-1] != 'A' else 'B'}"
        self.assertEqual(self.request(tampered).status_code, 401)
        expired = direct_access_token(
            self.secret, "guest:abc", user_agent=self.user_agent, expires_in=-1
        )
        self.assertEqual(self.request(expired).status_code, 401)
        with patch.dict(os.environ, {"ASK_MIMIR_TRUSTED_PROXY_SECRET": self.secret}):
            wrong_browser = self.client.get("/access", headers={
                "Authorization": f"Bearer {valid}",
                "User-Agent": "another-browser",
            })
        self.assertEqual(wrong_browser.status_code, 401)

    def test_production_site_can_preflight_the_direct_job_endpoint(self):
        source = (Path(__file__).resolve().parents[1] / "lab_api.py").read_text()
        app_definition = source.index('app = FastAPI(title="Ask Mimir"')
        route_definition = source.index('@app.post("/api/ask/jobs"')
        cors_definition = source.index("app.add_middleware(\n    CORSMiddleware", app_definition)
        self.assertLess(cors_definition, route_definition)
        self.assertIn('"https://www.mimiradvisors.org"', source[app_definition:route_definition])
        self.assertIn('allow_headers=["Authorization", "Content-Type", "X-Ask-Mimir-Client-Request-Id"]', source[app_definition:route_definition])


class StopAtEvidence(Exception):
    pass


class HttpBoundaryTests(unittest.TestCase):
    def setUp(self):
        app = FastAPI()
        app.middleware("http")(lab.audit_ask_http)
        app.post("/api/ask/jobs")(lab.create_ask_job)
        self.client = TestClient(app)
        self.request_id = str(uuid.uuid4())

    def test_malformed_and_incomplete_requests_are_correlated_before_validation(self):
        with patch.object(lab, "lifecycle") as events:
            for payload in [{}, {"messages": []}, {"messages": [{"role": "user", "content": "   "}]},
                            {"messages": [{"role": "assistant", "content": "Wrong final role"}]}]:
                with self.subTest(payload=payload):
                    response = self.client.post("/api/ask/jobs", json=payload,
                        headers={"X-Ask-Mimir-Client-Request-Id": self.request_id})
                    self.assertEqual(response.status_code, 422)
            events.assert_any_call("ask_api_arrived", client_request_id=self.request_id)
            self.assertEqual(sum(c.args[0] == "ask_api_responded" and c.kwargs["http_status"] == 422
                                 for c in events.call_args_list), 4)

    def test_production_direct_requests_cannot_bypass_the_signed_quota_identity(self):
        with patch.dict(os.environ, {"ASK_MIMIR_TRUSTED_PROXY_SECRET": "test-secret", "ASK_MIMIR_ALLOW_TEST_IDENTITIES": "0"}), patch.object(lab, "lifecycle") as events:
            response = self.client.post("/api/ask/jobs", json={"messages": [{"role": "user", "content": "Tell me about AMRAAM"}], "client_request_id": self.request_id},
                headers={"X-Ask-Mimir-Client-Request-Id": self.request_id})
            self.assertEqual(response.status_code, 401)
            self.assertTrue(any(c.args[0] == "ask_api_responded" and c.kwargs["http_status"] == 401
                                for c in events.call_args_list))


class AnswerTypeTests(unittest.TestCase):
    def test_expired_overall_deadline_stops_before_another_provider_call(self):
        responses = Mock()
        token = lab.JOB_DEADLINE.set(time.monotonic() - 1)
        try:
            with self.assertRaisesRegex(TimeoutError, "overall research deadline"):
                lab.TimedResponses(responses).create(model="test")
        finally:
            lab.JOB_DEADLINE.reset(token)
        responses.create.assert_not_called()

    def test_failure_taxonomy_distinguishes_queue_and_provider_limits(self):
        queue = lab.request_failure_details(lab.HTTPException(
            status_code=503,
            detail="The research queue took too long.",
        ))
        self.assertEqual(queue["failure_stage"], "queue")
        self.assertEqual(queue["error_code"], "queue_timeout")
        self.assertTrue(queue["retryable"])

        class ProviderLimitError(Exception):
            status_code = 429

        provider = lab.request_failure_details(ProviderLimitError("limited"))
        self.assertEqual(provider["failure_stage"], "model")
        self.assertEqual(provider["error_code"], "provider_rate_limited")
        self.assertEqual(provider["http_status"], 503)
        self.assertTrue(provider["retryable"])

    def test_short_provider_correction_is_clarification_not_research(self):
        result = {
            "answer": "I couldn't resolve that company. Please add a CAGE code or location.",
            "response_id": "resp_provider_generated",
            "answer_artifacts": {},
            "tool_trace": [],
        }
        self.assertEqual(lab.answer_type_for_result(result), "clarification")

    def test_short_validation_notice_is_validation_not_research(self):
        result = {
            "answer": "That request is missing a valid subject identifier.",
            "response_id": "resp_provider_generated",
            "answer_artifacts": {},
            "tool_trace": [],
        }
        self.assertEqual(lab.answer_type_for_result(result), "validation")
        self.assertTrue(lab.validation_requires_user_correction(result))

    def test_long_provider_failure_is_not_substantive(self):
        result = {
            "answer": (
                "I could not complete this research request because the provider failed. "
                + "Please retry shortly. " * 40
            ),
            "response_id": "resp_provider_failure",
            "answer_artifacts": {},
            "tool_trace": [],
        }
        self.assertEqual(lab.answer_type_for_result(result), "error")

    def test_failure_text_is_not_substantive_even_with_partial_evidence(self):
        result = {
            "answer": (
                "I’m sorry, but I couldn’t complete this research request because the provider failed. "
                + "Please retry shortly. " * 10
            ),
            "response_id": "resp_provider_failure",
            "answer_artifacts": {"platform_dossier": {"scope": {}}},
            "tool_trace": [{"tool": "get_platform_context", "result": {}}],
        }
        self.assertEqual(lab.answer_type_for_result(result), "error")

    def test_entity_selection_is_clarification(self):
        self.assertEqual(
            lab.answer_type_for_result(
                {
                    "answer": "Which Acme site did you mean?",
                    "response_id": "company-site-disambiguation",
                    "requires_clarification": True,
                }
            ),
            "clarification",
        )

    def test_metadata_only_artifact_is_not_substantive(self):
        self.assertEqual(
            lab.answer_type_for_result(
                {
                    "answer": "The cited award supports the conclusion.",
                    "response_id": "resp_research",
                    "answer_artifacts": {"company_site_dossier": {"scope": {}}},
                }
            ),
            "validation",
        )

    def test_empty_tool_records_are_not_substantive(self):
        self.assertEqual(
            lab.answer_type_for_result(
                {
                    "answer": "The cited award supports the conclusion.",
                    "response_id": "resp_research",
                    "tool_trace": [
                        {"tool": "get_metric_evidence", "result": {"records": [{}]}}
                    ],
                }
            ),
            "validation",
        )

    def test_long_entity_correction_is_clarification_not_research(self):
        result = {
            "answer": (
                "I could not resolve the requested company to a unique legal entity. "
                "Several similarly named organizations appear in the available records, and "
                "selecting one without confirmation could attach awards to the wrong business. "
                "Please provide the exact legal name, CAGE code, or operating location. "
            ) * 4,
            "response_id": "resp_provider_generated",
            "answer_artifacts": {},
            "tool_trace": [],
        }
        self.assertGreater(len(result["answer"]), 400)
        self.assertEqual(lab.answer_type_for_result(result), "clarification")
        self.assertTrue(lab.validation_requires_user_correction(result))
        self.assertFalse(lab.result_counts_toward_quota(result))

    def test_empty_company_lookup_is_not_research_evidence(self):
        result = {
            "answer": "No matching company was found. Enter a CAGE code or exact legal name.",
            "response_id": "resp_provider_generated",
            "answer_artifacts": {},
            "tool_trace": [{"tool": "get_company_context", "result": {}}],
        }
        self.assertFalse(lab.result_has_research_evidence(result))
        self.assertEqual(lab.answer_type_for_result(result), "validation")
        self.assertFalse(lab.result_counts_toward_quota(result))

    def test_ambiguous_company_selection_is_returned_as_clarification(self):
        matches = [
            {
                "scope_type": "company_site",
                "scope_id": "11111",
                "scope_name": "COLLINS AEROSPACE",
                "city": "CEDAR RAPIDS",
                "state": "IA",
                "option_label": "COLLINS AEROSPACE — Cedar Rapids, IA — CAGE 11111",
            },
            {
                "scope_type": "company_site",
                "scope_id": "22222",
                "scope_name": "COLLINS AEROSPACE",
                "city": "WINDSOR LOCKS",
                "state": "CT",
                "option_label": "COLLINS AEROSPACE — Windsor Locks, CT — CAGE 22222",
            },
        ]
        resolution = {
            "matches": matches,
            "requires_disambiguation": True,
            "disambiguation_options": [row["option_label"] for row in matches],
        }
        runtime = SimpleNamespace(
            mock_mode=False,
            platform_contexts=Mock(mentions=Mock(return_value=[])),
            company_contexts=Mock(search=Mock(return_value=resolution)),
            call_tool=Mock(return_value=resolution),
            store=SimpleNamespace(manifest={"release_id": "test-release"}),
        )
        request = lab.AskRequest(messages=[{
            "role": "user",
            "content": "Tell me about Collins Aerospace",
        }])
        route = lab.RoutingDecision(
            workflow="company_site_intelligence",
            reason="explicit_company_name",
            confidence=0.9,
        )
        with patch.object(lab, "runtime", runtime, create=True):
            result = lab.generate_answer(request, routing=route)
        self.assertTrue(result["requires_clarification"])
        self.assertEqual(result["answer_type"], "clarification")
        self.assertEqual(result["response_id"], "company-site-disambiguation")
        self.assertFalse(lab.result_counts_toward_quota(result))

    def test_unknown_exact_cage_is_returned_as_clarification(self):
        runtime = SimpleNamespace(
            mock_mode=False,
            external_evidence_allowed=True,
            model="test-model",
            reasoning_effort="medium",
            max_output_tokens=1000,
            platform_contexts=Mock(mentions=Mock(return_value=[])),
            company_contexts=Mock(search=Mock(return_value={"matches": []})),
            call_tool=Mock(side_effect=KeyError("unknown CAGE")),
            store=SimpleNamespace(manifest={"release_id": "test-release"}),
        )
        request = lab.AskRequest(messages=[{
            "role": "user",
            "content": "CAGE 9ZZ99",
        }])
        route = lab.RoutingDecision(
            workflow="company_site_intelligence",
            reason="explicit_cage_identifier",
            confidence=1,
        )
        with patch.object(lab, "runtime", runtime, create=True), patch.object(
            lab, "OpenAI", return_value=Mock()
        ), patch.dict("os.environ", {"OPENAI_API_KEY": "test-placeholder"}):
            result = lab.generate_answer(request, routing=route)
        self.assertTrue(result["requires_clarification"])
        self.assertEqual(result["answer_type"], "clarification")
        self.assertEqual(result["response_id"], "company-site-disambiguation")
        self.assertFalse(lab.result_counts_toward_quota(result))


class ExecutionBoundaryTests(unittest.TestCase):
    def test_supplier_question_about_named_platform_does_not_offer_sentence_fragment_as_capability(self):
        runtime = SimpleNamespace(platform_contexts=Mock(mentions=Mock(return_value=["AMRAAM"])))
        with patch.object(lab, "runtime", runtime, create=True):
            for question in (
                "Which companies supply AMRAAM and what do they provide?",
                "Which companies supply AMRAAM?",
                "Find suppliers that manufacture AMRAAM components.",
            ):
                with self.subTest(question=question):
                    route = lab.routing_decision_for_request(lab.AskRequest(messages=[{"role":"user", "content":question}]))
                    self.assertEqual(route.workflow, "platform_intelligence")
                    self.assertFalse(route.clarification_needed)

    def test_completed_answer_export_expands_owned_platform_scope_on_demand(self):
        import csv, io, zipfile

        app = FastAPI()
        app.get('/api/evidence/answer.zip')(lab.answer_evidence_export_download)
        pack = {
            'scope': {
                'platform_id': 'M109 PALADIN',
                'display_name': 'M109A7 Paladin',
                'requested_focus': {'focus_id': 'M109A7'},
            },
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
                'answer_type': 'substantive',
                'answer_artifacts': {'platform_dossier': pack},
            },
        }
        expanded_pack = {
            **pack,
            'reported_supplier_sites': [
                {'cage': f'{index:05d}', 'supplier_name': f'Supplier {index}'}
                for index in range(30)
            ],
        }
        evidence_cache = Mock(
            cache_key=Mock(return_value='platform-export-key'),
            get_bytes=Mock(return_value=None),
            set_bytes=Mock(),
        )
        platform_contexts = Mock(
            get_export_context=Mock(return_value=expanded_pack),
        )
        runtime = SimpleNamespace(
            evidence_cache=evidence_cache,
            release_guard=SimpleNamespace(release_binding_id='release-binding'),
            platform_contexts=platform_contexts,
            optional_program_outlook=Mock(return_value=None),
        )
        with patch.object(
            lab, 'require_evidence_download', return_value=AccessContext('alice', 'professional', True)
        ), patch.object(
            lab, 'job_manager', SimpleNamespace(get=Mock(return_value=job)), create=True
        ), patch.object(lab, 'runtime', runtime, create=True):
            response = TestClient(app).get(
                '/api/evidence/answer.zip',
                params={'request_id': 'request-owned', 'response_id': 'resp-owned'},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['content-type'], 'application/zip')
        self.assertIn('mimir-platform-m109-paladin-evidence.zip', response.headers['content-disposition'])
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            self.assertIn('README.txt', archive.namelist())
            supplier_rows = list(csv.reader(io.StringIO(
                archive.read('03_reported_supplier_sites.csv').decode('utf-8-sig')
            )))
        self.assertEqual(len(supplier_rows) - 1, 30)
        platform_contexts.get_export_context.assert_called_once_with(
            'M109 PALADIN', limit=5000, focus_id='M109A7'
        )
        evidence_cache.set_bytes.assert_called_once_with(
            'platform-export-key', response.content
        )

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

    def test_completed_answer_export_expands_owned_company_scope_on_demand(self):
        import csv, io, zipfile

        app = FastAPI()
        app.get('/api/evidence/answer.zip')(lab.answer_evidence_export_download)
        pack = {
            'scope': {
                'scope_type': 'company_site',
                'scope_id': '14925',
                'scope_name': 'Teledyne Brown Engineering, Inc.',
                'observation_window': 'FY2021-FY2026 observed records',
            },
            'identity': {'sites': []},
            'observed_financials': [],
            'reported_subcontract_relationships': {},
            'location_footprint': {},
            'site_capability_evidence': [],
            'platform_exposure': [],
            'product_and_part_evidence': {},
            'future_demand_context': {},
            'evidence_index': {'records': []},
        }
        job = {
            'status': 'completed',
            'result': {
                'response_id': 'resp-company',
                'answer': 'Completed company research answer.',
                'answer_type': 'substantive',
                'release_binding_id': 'release-binding',
                'answer_artifacts': {'company_site_dossier': pack},
            },
        }
        expanded_pack = {
            **pack,
            'top_awards': [
                {'contract_id': f'AWARD-{index}', 'recipient_cage': '14925'}
                for index in range(25)
            ],
        }
        evidence_cache = Mock(
            cache_key=Mock(return_value='company-export-key'),
            get_bytes=Mock(return_value=None),
            set_bytes=Mock(),
        )
        company_contexts = SimpleNamespace(
            context_dir=Path('.'),
            get_export_context=Mock(return_value=expanded_pack),
        )
        runtime = SimpleNamespace(
            company_contexts=company_contexts,
            evidence_cache=evidence_cache,
            release_guard=SimpleNamespace(release_binding_id='release-binding'),
        )
        with patch.object(
            lab, 'require_evidence_download', return_value=AccessContext('alice', 'enterprise', True)
        ), patch.object(
            lab, 'job_manager', SimpleNamespace(get=Mock(return_value=job)), create=True
        ), patch.object(lab, 'runtime', runtime, create=True):
            response = TestClient(app).get(
                '/api/evidence/answer.zip',
                params={'request_id': 'request-company', 'response_id': 'resp-company'},
            )
        self.assertEqual(response.status_code, 200)
        self.assertIn('mimir-company-site-14925-evidence.zip', response.headers['content-disposition'])
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            readme = archive.read('README.txt').decode()
            award_rows = list(csv.reader(io.StringIO(
                archive.read('02_prime_awards.csv').decode('utf-8-sig')
            )))
        self.assertIn('Evidence basis: expanded records for the scope resolved in the completed Ask Mimir answer.', readme)
        self.assertNotIn('Calculation version', readme)
        self.assertEqual(len(award_rows) - 1, 25)
        company_contexts.get_export_context.assert_called_once_with(
            'company_site', '14925', limit=5000, prepared=True
        )
        evidence_cache.set_bytes.assert_called_once_with(
            'company-export-key', response.content
        )
        evidence_cache.get_bytes.return_value = response.content
        with patch.object(
            lab, 'require_evidence_download', return_value=AccessContext('alice', 'professional', True)
        ), patch.object(
            lab, 'job_manager', SimpleNamespace(get=Mock(return_value=job)), create=True
        ), patch.object(lab, 'runtime', runtime, create=True):
            cached_response = TestClient(app).get(
                '/api/evidence/answer.zip',
                params={'request_id': 'request-company', 'response_id': 'resp-company'},
            )
        self.assertEqual(cached_response.content, response.content)
        company_contexts.get_export_context.assert_called_once()

    def test_variant_export_preserves_requested_focus(self):
        app = FastAPI()
        app.get('/api/evidence/platform.zip')(lab.universal_platform_evidence_export)
        store = Mock(get_export_context=Mock(return_value={'scope': {'display_name': 'M109A7'}}))
        runtime = SimpleNamespace(
            platform_contexts=store,
            optional_program_outlook=Mock(return_value=None),
            evidence_cache=Mock(
                cache_key=Mock(return_value='platform-export-key'),
                get_bytes=Mock(return_value=None),
                set_bytes=Mock(),
            ),
            release_guard=SimpleNamespace(release_binding_id='release-binding'),
        )
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
            self.assertEqual(len(small['top_prime_awards']), 45)
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

    def test_record_search_subject_matches_do_not_trigger_clarification(self):
        questions = (
            "Find current US defense opportunities relevant to electronic-warfare equipment manufacturers.",
            "Find current US defense opportunities relevant to: aircraft thermal management",
            "What defense opportunities are open for radar manufacturers?",
            "Show recent defence awards involving missile propulsion.",
            "Find recent US defense contract awards related to missile propulsion systems.",
        )
        runtime = SimpleNamespace(
            platform_contexts=Mock(mentions=Mock(side_effect=lambda text: (
                ["AMRAAM"] if "AMRAAM" in str(text) else []
            ))),
            company_contexts=Mock(search=Mock(return_value={"matches": []})),
        )
        subject_workflows = {
            "product_intelligence",
            "platform_intelligence",
            "capability_discovery",
            "market_segment_intelligence",
            "state_industrial_base",
            "competitor_discovery",
            "program_momentum",
        }
        with patch.object(lab, "runtime", runtime, create=True):
            for question in questions:
                with self.subTest(question=question):
                    request = lab.AskRequest(messages=[{"role": "user", "content": question}])
                    routing = lab.routing_decision_for_request(request)
                    self.assertEqual(routing.workflow, "market_record_search")
                    self.assertFalse(routing.clarification_needed)
                    self.assertFalse(
                        subject_workflows.intersection(
                            candidate.workflow for candidate in routing.candidates
                        )
                    )

            starter = lab.AskRequest(messages=[{
                "role": "user",
                "content": (
                    "Who supplies AMRAAM, what do they provide, and what evidence "
                    "supports those positions?"
                ),
            }])
            starter_routing = lab.routing_decision_for_request(starter)
            self.assertEqual(starter_routing.workflow, "platform_intelligence")
            self.assertFalse(starter_routing.clarification_needed)

    def test_routing_labels_never_expose_capability_ontology_ids(self):
        entity = lab.RoutingEntity(
            entity_type="capability_market",
            entity_id="electronic_warfare",
            source="capability_ontology",
            confidence=0.96,
        )
        label = lab._routing_entity_label(entity, "capability_discovery")
        self.assertEqual(
            label,
            "Military electronic-warfare and countermeasure equipment",
        )
        self.assertNotIn("_", label)

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

    def test_audit_write_failure_is_never_an_answer_failure(self):
        runtime = lab.LabRuntime.__new__(lab.LabRuntime)
        runtime.audit_log = Path("/tmp/ask-mimir-test-audit.jsonl")
        runtime.audit_lock = threading.Lock()
        with patch.object(
            lab, "write_bounded_audit_record", side_effect=OSError("audit unavailable")
        ) as writer:
            record = {"answer": "Raw answer", "request_messages": []}
            runtime.write_audit_record(record)
        self.assertEqual(writer.call_args.args[1], record)

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
    def test_concurrent_duplicate_admission_schedules_one_job(self):
        with ThreadPoolExecutor(max_workers=20) as pool:
            results = list(pool.map(lambda _: self.manager.create(self.request, self.access, self.route)[0], range(20)))
        self.assertEqual(sum(not r.get("deduplicated", False) for r in results), 1)
        self.manager.executor.submit.assert_called_once()
        self.assertEqual(self.ledger.used_today("alice"), 0)

    def test_concurrency_1_5_10_20_has_bounded_queue_and_rejections_never_charge(self):
        for count in (1, 5, 10, 20):
            with self.subTest(concurrent=count):
                self.manager.jobs.clear()
                def submit(index):
                    request = lab.AskRequest(messages=[{"role":"user","content":"Tell me about AMRAAM"}], client_request_id=str(uuid.uuid4()))
                    access = AccessContext(f"load-{count}-{index}", "public", False)
                    try:
                        self.manager.create(request, access, self.route)
                        return 202
                    except lab.HTTPException as exc:
                        return exc.status_code
                with ThreadPoolExecutor(max_workers=count) as pool:
                    results = list(pool.map(submit, range(count)))
                self.assertEqual(results.count(202), min(count, self.manager.capacity))
                self.assertEqual(results.count(503), max(count - self.manager.capacity, 0))
                for index in range(count):
                    self.assertEqual(self.ledger.used_today(f"load-{count}-{index}"), 0)

    def test_provider_transient_failures_release_credit_and_preserve_classification(self):
        for status in (429, 500, 502, 503):
            with self.subTest(provider_status=status):
                request = self.request.model_copy(update={"client_request_id":str(uuid.uuid4())})
                job, _ = self.manager.create(request, self.access, self.route)
                error = RuntimeError("Provider temporarily unavailable")
                error.status_code = status
                with patch.object(lab, "generate_answer", side_effect=error):
                    self.manager._run(job["request_id"], request, self.access, self.route)
                result = self.manager.get(job["request_id"], self.access)
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["failure_stage"], "model")
                self.assertTrue(result["retryable"])
                self.assertEqual(self.ledger.used_today("alice"), 0)

    def test_accepted_job_can_resume_from_only_its_id_after_restart(self):
        self.manager.create(self.request, self.access, self.route)
        self.assertEqual(self.ledger.used_today("alice"), 0)
        saved = self.ledger.load_job("a" * 32)
        self.assertEqual(saved["status"], "queued")
        self.manager.jobs.clear()  # Simulate process memory loss before reservation.
        with self.assertRaises(KeyError):
            self.manager.get("a" * 32, AccessContext("other", "professional", True))
        self.manager.executor.reset_mock()
        first = self.manager.get("a" * 32, self.access)
        second = self.manager.get("a" * 32, self.access)
        self.assertEqual(first["status"], "queued")
        self.assertEqual(second["request_id"], first["request_id"])
        self.manager.executor.submit.assert_called_once()
        self.assertNotIn("_request_body", first)
        self.assertEqual(self.ledger.used_today("alice"), 0)

    def test_acceptance_is_refused_without_durable_recovery_and_without_credit(self):
        with patch.object(self.ledger, "save_job", side_effect=OSError("disk unavailable")):
            with self.assertRaises(OSError):
                self.manager.create(self.request, self.access, self.route)
        self.assertEqual(self.manager.jobs, {})
        self.manager.executor.submit.assert_not_called()
        self.assertEqual(self.ledger.used_today("alice"), 0)

    def test_delivery_receipts_are_authorized_idempotent_and_do_not_rebill(self):
        job = {"request_id":"a" * 32, "subject_id":"alice", "status":"completed", "result":{"response_id":"answer-1", "answer":"Report"}}
        self.ledger.save_job(job)
        before = self.ledger.used_today("alice")
        with self.assertRaises(KeyError):
            self.ledger.record_delivery_receipt("a" * 32, "other", "answer-1", "rendered")
        with self.assertRaises(ValueError):
            self.ledger.record_delivery_receipt("a" * 32, "alice", "wrong-answer", "rendered")
        received = self.ledger.record_delivery_receipt("a" * 32, "alice", "answer-1", "received")
        rendered = self.ledger.record_delivery_receipt("a" * 32, "alice", "answer-1", "rendered")
        duplicate = self.ledger.record_delivery_receipt("a" * 32, "alice", "answer-1", "rendered")
        self.assertIn("browser_received_at", received)
        self.assertEqual(rendered, duplicate)
        self.assertEqual(self.ledger.used_today("alice"), before)

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
                "platform_dossier": {
                    "scope": {"platform_id": "AMRAAM"},
                    "top_prime_awards": [{"contract_id": "TEST-AWARD"}],
                },
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
            self.assertGreaterEqual(recovered["timings"]["total_request_ms"], 0)
            self.assertNotIn("operations", recovered["timings"])
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

    def test_legacy_result_timings_are_owner_bound_and_do_not_include_tool_payloads(self):
        self.ledger.reserve("legacy", self.access, "test", "platform_intelligence")
        job = {"request_id": "legacy", "subject_id": "alice", "status": "completed", "result": {"answer": "Report"}}
        self.ledger.complete("legacy", latency_ms=1000, estimated_cost_usd=None,
                             performance={"model_ms": 900, "routing_ms": 10, "operations": [{"question": "PRIVATE"}]}, job=job)
        self.assertEqual(self.manager.get("legacy", self.access)["timings"], {"model_ms": 900, "routing_ms": 10})
        self.assertEqual(self.ledger.load_job_timings("legacy", "another-owner"), {})
        with self.assertRaises(KeyError):
            self.manager.get("legacy", AccessContext("another-owner", "public", False))

    def test_timing_read_failure_does_not_block_completed_answer(self):
        self.ledger.save_job({"request_id": "legacy", "subject_id": "alice", "status": "completed", "result": {"answer": "Report"}})
        with patch.object(self.ledger, "load_job_timings", side_effect=ValueError("malformed timing")):
            self.assertEqual(self.manager.get("legacy", self.access)["result"]["answer"], "Report")

    def test_timing_summary_rejects_non_numeric_private_or_invalid_values(self):
        summary = lab.request_timing_summary({"model_ms": 20, "queue_wait_ms": float("inf"), "routing_ms": -1,
                                              "total_request_ms": "30", "model_call_count": True,
                                              "operations": ["private"], "estimated_cost_usd": 0.1})
        self.assertEqual(summary, {"model_ms": 20})

    def test_passive_policy_gets_do_not_reserve_credit_or_create_research(self):
        app = FastAPI()
        app.get("/api/beta/policy")(lab.beta_policy)
        client = TestClient(app)
        with patch.dict(os.environ, {"ASK_MIMIR_TRUSTED_PROXY_SECRET": "test-secret"}):
            for identity in ["guest:scanner", "guest:scanner", "guest:human"]:
                response = client.get("/api/beta/policy", headers={"X-Ask-Mimir-Proxy-Secret": "test-secret", "X-Ask-Mimir-Subject": identity, "X-Ask-Mimir-Tier": "public"})
                self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ledger.connection.execute("SELECT count(*) FROM query_events").fetchone()[0], 0)
        self.assertEqual(self.ledger.connection.execute("SELECT count(*) FROM research_results").fetchone()[0], 0)
        self.assertEqual(self.ledger.connection.execute("SELECT count(*) FROM ask_conversations").fetchone()[0], 0)
        self.manager.executor.submit.assert_not_called()

    def test_routing_analytics_failure_does_not_block_job_admission(self):
        with patch.object(
            self.ledger,
            "record_routing_decision",
            side_effect=OSError("routing analytics unavailable"),
        ):
            job, _ = self.manager.create(self.request, self.access, self.route)
        self.assertEqual(job["status"], "queued")

    def test_clarification_continuation_does_not_depend_on_routing_analytics(self):
        public = AccessContext("guest", "public", False)
        request = self.request.model_copy(update={"conversation_id": "conversation-1"})
        route = self.route.model_copy(update={"clarification_needed": True})
        clarification = {
            "answer": "Which company site did you mean?",
            "response_id": "company-site-disambiguation",
            "requires_clarification": True,
            "answer_type": "clarification",
            "answer_artifacts": {"company_resolution": {"matches": []}},
            "tool_trace": [],
        }
        with patch.object(
            self.ledger, "record_routing_decision", side_effect=OSError("analytics down")
        ):
            self.manager.create(request, public, route)
        with patch.object(
            self.ledger, "complete_routing_event", side_effect=OSError("analytics down")
        ), patch.object(
            lab, "routing_clarification_result", return_value=clarification
        ), patch.object(
            lab, "finalize_customer_result", side_effect=lambda result, *args: dict(result)
        ):
            self.manager._run("a" * 32, request, public, route)

        # Prove the durable operational grant works independently of the
        # in-process fallback and the routing-events analytics table.
        self.manager.continuation_grants.clear()
        self.ledger.reserve("other-answer", public, "release", "platform")
        self.ledger.complete("other-answer", latency_ms=1, estimated_cost_usd=0)
        correction = request.model_copy(update={"client_request_id": "b" * 32})
        correction_job, _ = self.manager.create(correction, public, self.route)
        self.assertTrue(correction_job["clarification_continuation"])

    def test_auxiliary_failures_cannot_turn_a_delivered_answer_into_a_charged_failure(self):
        self.manager.create(self.request, self.access, self.route)
        answer = {
            "answer": "AMRAAM report with evidence. " * 20,
            "response_id": "test-answer",
            "answer_artifacts": {
                "platform_dossier": {
                    "scope": {"platform_id": "AMRAAM"},
                    "top_prime_awards": [{"contract_id": "TEST-AWARD"}],
                }
            },
            "tool_trace": [],
        }
        real_used_today = self.ledger.used_today

        def fail_only_after_completion(subject_id):
            row = self.ledger.connection.execute(
                "SELECT status FROM query_events WHERE request_id = ?",
                ["a" * 32],
            ).fetchone()
            if row and row[0] == "completed":
                raise OSError("allowance summary temporarily unavailable")
            return real_used_today(subject_id)

        self.runtime.write_audit_record.side_effect = OSError("audit unavailable")
        with patch.object(
            self.ledger, "save_conversation_scope", side_effect=OSError("scope unavailable")
        ), patch.object(
            self.ledger, "complete_routing_event", side_effect=OSError("routing unavailable")
        ), patch.object(
            self.ledger, "used_today", side_effect=fail_only_after_completion
        ), patch.object(
            lab, "generate_answer", return_value=answer
        ), patch.object(
            lab, "finalize_customer_result", side_effect=lambda result, *args: dict(result)
        ):
            self.manager._run("a" * 32, self.request, self.access, self.route)

        recovered = self.ledger.load_job("a" * 32)
        self.assertEqual(recovered["status"], "completed")
        self.assertEqual(recovered["result"]["answer"], answer["answer"])
        self.assertEqual(real_used_today("alice"), 1)

    def test_credit_release_is_retried_if_the_first_refund_attempt_fails(self):
        self.manager.create(self.request, self.access, self.route)
        real_fail = self.ledger.fail
        attempts = 0

        def flaky_fail(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise OSError("transient state-store failure")
            return real_fail(*args, **kwargs)

        with patch.object(self.ledger, "fail", side_effect=flaky_fail), patch.object(
            lab, "generate_answer", side_effect=TimeoutError("provider timeout")
        ):
            self.manager._run("a" * 32, self.request, self.access, self.route)

        status = self.ledger.connection.execute(
            "SELECT status FROM query_events WHERE request_id = ?", ["a" * 32]
        ).fetchone()[0]
        self.assertEqual(attempts, 2)
        self.assertEqual(status, "failed_refunded")
        self.assertEqual(self.ledger.used_today("alice"), 0)

    def test_persistent_refund_failure_is_scheduled_for_reconciliation(self):
        self.manager.create(self.request, self.access, self.route)
        with patch.object(
            self.ledger, "fail", side_effect=OSError("state store unavailable")
        ), patch.object(
            self.manager, "_schedule_credit_release"
        ) as schedule, patch.object(
            lab, "generate_answer", side_effect=TimeoutError("provider timeout")
        ):
            self.manager._run("a" * 32, self.request, self.access, self.route)

        schedule.assert_called_once_with(
            "a" * 32, self.request, self.access, self.route
        )

    def test_health_snapshot_reports_pending_credit_reconciliation(self):
        self.manager.pending_credit_releases.add("a" * 32)
        self.assertEqual(self.manager.snapshot()["pending_credit_releases"], 1)

    def test_clarification_remains_deliverable_when_durable_result_save_fails(self):
        clarification_route = self.route.model_copy(update={"clarification_needed": True})
        self.manager.create(self.request, self.access, clarification_route)
        clarification = {
            "answer": "Which platform did you mean?",
            "response_id": "routing-clarification",
            "requires_clarification": True,
            "answer_type": "clarification",
            "answer_artifacts": {"routing_clarification": {"options": []}},
            "tool_trace": [],
        }
        with patch.object(
            lab, "routing_clarification_result", return_value=clarification
        ), patch.object(
            lab, "finalize_customer_result", side_effect=lambda result, *args: dict(result)
        ), patch.object(
            self.ledger, "save_job", side_effect=OSError("result store unavailable")
        ):
            self.manager._run(
                "a" * 32, self.request, self.access, clarification_route
            )

        recovered = self.manager.get("a" * 32, self.access)
        self.assertEqual(recovered["status"], "completed")
        self.assertEqual(recovered["result"]["answer_type"], "clarification")
        self.assertEqual(self.ledger.used_today("alice"), 0)
        self.manager.check_lifecycle()
        self.assertEqual(self.ledger.load_job("a" * 32)["status"], "completed")
        self.assertNotIn("a" * 32, self.manager.jobs)

    def test_quota_race_is_logged_as_rejection_not_server_failure(self):
        self.manager.create(self.request, self.access, self.route)
        with patch.object(self.ledger, "reserve", side_effect=lab.DailyQuotaExceeded(self.access.policy)), patch.object(lab, "lifecycle") as log:
            self.manager._run("a" * 32, self.request, self.access, self.route)
        events = [call.args[0] for call in log.call_args_list]
        self.assertIn("ask_request_rejected", events)
        self.assertNotIn("ask_server_failed", events)
        self.assertEqual(self.ledger.load_job("a" * 32)["failure_stage"], "quota")

    def test_admission_rejection_does_not_reserve_allowance(self):
        self.manager.capacity = 1
        self.manager.create(self.request, self.access, self.route)
        second = self.request.model_copy(update={"client_request_id":"b"*32})
        with self.assertRaises(lab.HTTPException) as exc:
            self.manager.create(second, self.access, self.route)
        self.assertEqual(exc.exception.status_code, 503)
        self.assertEqual(self.ledger.used_today("alice"), 0)

    def test_low_disk_rejection_does_not_reserve_allowance(self):
        with patch.object(self.ledger, 'has_write_capacity', return_value=False):
            with self.assertRaises(lab.HTTPException) as exc:
                self.manager.create(self.request, self.access, self.route)
        self.assertEqual(exc.exception.status_code, 503)
        self.assertEqual(self.ledger.used_today('alice'), 0)
        self.assertFalse(self.manager.jobs)

    def test_exhausted_allowance_is_rejected_before_queueing(self):
        public = AccessContext("guest", "public", False)
        self.ledger.reserve("already-used", public, "release", "platform")
        self.ledger.complete(
            "already-used", latency_ms=1, estimated_cost_usd=0.01
        )
        with self.assertRaises(lab.DailyQuotaExceeded):
            self.manager.create(self.request, public, self.route)
        self.assertFalse(self.manager.jobs)
        self.assertEqual(self.ledger.used_today("guest"), 1)

    def test_public_guest_gets_exactly_one_substantive_answer(self):
        public = AccessContext("guest", "public", False)
        self.manager.create(self.request, public, self.route)
        answer = {
            "answer": "Substantive AMRAAM research with evidence. " * 20,
            "response_id": "resp_public_answer",
            "answer_artifacts": {
                "platform_dossier": {
                    "scope": {"platform_id": "AMRAAM"},
                    "top_prime_awards": [{"contract_id": "TEST-AWARD"}],
                }
            },
            "tool_trace": [],
        }
        with patch.object(
            lab, "generate_answer", return_value=answer
        ), patch.object(
            lab, "finalize_customer_result", side_effect=lambda result, *args: dict(result)
        ):
            self.manager._run("a" * 32, self.request, public, self.route)

        delivered = self.ledger.load_job("a" * 32)
        self.assertEqual(delivered["status"], "completed")
        self.assertEqual(delivered["result"]["answer_type"], "substantive")
        self.assertEqual(self.ledger.used_today("guest"), 1)
        second = self.request.model_copy(update={"client_request_id": "b" * 32})
        with self.assertRaises(lab.DailyQuotaExceeded):
            self.manager.create(second, public, self.route)

    def test_clarification_is_accepted_after_allowance_is_exhausted(self):
        public = AccessContext("guest", "public", False)
        self.ledger.reserve("already-used", public, "release", "platform")
        self.ledger.complete(
            "already-used", latency_ms=1, estimated_cost_usd=0.01
        )
        clarification_request = self.request.model_copy(
            update={"conversation_id": "new-over-limit-question"}
        )
        clarification_route = self.route.model_copy(
            update={"clarification_needed": True}
        )
        job, _ = self.manager.create(
            clarification_request, public, clarification_route
        )
        self.assertEqual(job["status"], "queued")
        self.assertFalse(job["continuation_eligible"])
        self.assertEqual(self.ledger.used_today("guest"), 1)

        clarification = {
            "answer": "Which platform did you mean?",
            "response_id": "routing-clarification",
            "requires_clarification": True,
            "answer_type": "clarification",
            "answer_artifacts": {"routing_clarification": {"options": []}},
            "tool_trace": [],
        }
        with patch.object(lab, "routing_clarification_result", return_value=clarification), patch.object(
            lab,
            "finalize_customer_result",
            side_effect=lambda result, *args: dict(result),
        ):
            self.manager._run(
                "a" * 32, clarification_request, public, clarification_route
            )

        correction = clarification_request.model_copy(
            update={"client_request_id": "b" * 32}
        )
        with self.assertRaises(lab.DailyQuotaExceeded):
            self.manager.create(correction, public, self.route)

    def test_quota_race_returns_a_recoverable_429_job(self):
        public = AccessContext("guest", "public", False)
        self.manager.create(self.request, public, self.route)
        self.ledger.reserve("racing-answer", public, "release", "platform")
        self.ledger.complete(
            "racing-answer", latency_ms=1, estimated_cost_usd=0.01
        )

        self.manager._run("a" * 32, self.request, public, self.route)

        recovered = self.ledger.load_job("a" * 32)
        self.assertEqual(recovered["status"], "failed")
        self.assertEqual(recovered["http_status"], 429)
        self.assertEqual(recovered["error_code"], "quota_exceeded")
        self.assertEqual(self.ledger.used_today("guest"), 1)

    def test_eligible_clarification_can_complete_after_allowance_race(self):
        public = AccessContext("guest", "public", False)
        clarification_request = self.request.model_copy(
            update={"conversation_id": "eligible-clarification"}
        )
        clarification_route = self.route.model_copy(
            update={"clarification_needed": True}
        )
        job, _ = self.manager.create(
            clarification_request, public, clarification_route
        )
        self.assertTrue(job["continuation_eligible"])
        clarification = {
            "answer": "Which platform did you mean?",
            "response_id": "routing-clarification",
            "requires_clarification": True,
            "answer_type": "clarification",
            "answer_artifacts": {"routing_clarification": {"options": []}},
            "tool_trace": [],
        }
        with patch.object(lab, "routing_clarification_result", return_value=clarification), patch.object(
            lab,
            "finalize_customer_result",
            side_effect=lambda result, *args: dict(result),
        ):
            self.manager._run(
                "a" * 32, clarification_request, public, clarification_route
            )

        self.ledger.reserve("racing-answer", public, "release", "platform")
        self.ledger.complete(
            "racing-answer", latency_ms=1, estimated_cost_usd=0.01
        )
        correction = clarification_request.model_copy(
            update={"client_request_id": "b" * 32}
        )
        correction_job, _ = self.manager.create(correction, public, self.route)
        self.assertTrue(correction_job["clarification_continuation"])

        answer = {
            "answer": "Substantive platform research " + "with evidence. " * 30,
            "response_id": "resp_answer",
            "answer_artifacts": {
                "platform_dossier": {
                    "scope": {},
                    "top_prime_awards": [{"contract_id": "TEST-AWARD"}],
                }
            },
            "tool_trace": [],
        }
        with patch.object(lab, "generate_answer", return_value=answer), patch.object(
            lab,
            "finalize_customer_result",
            side_effect=lambda result, *args: dict(result),
        ):
            self.manager._run("b" * 32, correction, public, self.route)

        self.assertEqual(self.ledger.used_today("guest"), 2)
        self.assertFalse(
            self.ledger.clarification_continuation_allowed(
                "eligible-clarification", "guest", "platform_intelligence"
            )
        )

    def test_actionable_validation_releases_credit_and_keeps_correction_open(self):
        public = AccessContext("guest", "public", False)
        request = self.request.model_copy(
            update={"conversation_id": "validation-correction"}
        )
        self.manager.create(request, public, self.route)
        validation = {
            "answer": "That request is missing a valid subject identifier. Please add the company or CAGE code.",
            "response_id": "resp_validation",
            "answer_artifacts": {},
            "tool_trace": [],
        }
        with patch.object(lab, "generate_answer", return_value=validation), patch.object(
            lab,
            "finalize_customer_result",
            side_effect=lambda result, *args: dict(result),
        ):
            self.manager._run("a" * 32, request, public, self.route)

        recovered = self.ledger.load_job("a" * 32)
        self.assertEqual(recovered["result"]["answer_type"], "validation")
        self.assertTrue(recovered["result"]["requires_user_correction"])
        self.assertEqual(self.ledger.used_today("guest"), 0)
        self.assertTrue(
            self.ledger.clarification_continuation_allowed(
                "validation-correction", "guest", "platform_intelligence"
            )
        )

        self.ledger.reserve("racing-answer", public, "release", "platform")
        self.ledger.complete(
            "racing-answer", latency_ms=1, estimated_cost_usd=0.01
        )
        correction = request.model_copy(update={"client_request_id": "b" * 32})
        correction_job, _ = self.manager.create(correction, public, self.route)
        self.assertTrue(correction_job["clarification_continuation"])

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
            self.assertEqual(self.ledger.used_today('alice'),0)

    def test_worker_releases_credit_for_entity_clarification(self):
        self.manager.create(self.request, self.access, self.route)
        clarification = {
            "answer": "Which company site did you mean?",
            "response_id": "company-site-disambiguation",
            "requires_clarification": True,
            "answer_artifacts": {"company_resolution": {"matches": []}},
            "tool_trace": [],
            "model": "deterministic-resolution",
        }
        with patch.object(
            lab, "generate_answer", return_value=clarification
        ), patch.object(
            lab, "finalize_customer_result", side_effect=lambda result, *args: dict(result)
        ):
            self.manager._run("a" * 32, self.request, self.access, self.route)

        self.assertEqual(self.ledger.used_today("alice"), 0)
        status = self.ledger.connection.execute(
            "SELECT status FROM query_events WHERE request_id = ?", ["a" * 32]
        ).fetchone()[0]
        self.assertEqual(status, "completed_unbilled")
        recovered = self.ledger.load_job("a" * 32)
        self.assertEqual(recovered["result"]["access"]["queries_used_today"], 0)
        event_types = [
            call.args[0]["record_type"]
            for call in self.runtime.write_audit_record.call_args_list
            if call.args and call.args[0].get("record_type", "").startswith("ask_credit_")
        ]
        self.assertEqual(event_types, ["ask_credit_reserved", "ask_credit_released"])
        release_event = next(
            call.args[0]
            for call in self.runtime.write_audit_record.call_args_list
            if call.args and call.args[0].get("record_type") == "ask_credit_released"
        )
        self.assertEqual(
            {
                key: release_event[key]
                for key in (
                    "request_id", "conversation_id", "tier", "workflow",
                    "reason", "answer_type",
                )
            },
            {
                "request_id": "a" * 32,
                "conversation_id": None,
                "tier": "professional",
                "workflow": "platform_intelligence",
                "reason": "clarification_answer_delivered",
                "answer_type": "clarification",
            },
        )

    def test_short_provider_correction_releases_credit_and_keeps_turn_open(self):
        request = self.request.model_copy(
            update={"conversation_id": "test-conversation"}
        )
        self.manager.create(request, self.access, self.route)
        correction = {
            "answer": "I couldn't resolve that company. Please add a CAGE code or location.",
            "response_id": "resp_provider_generated",
            "answer_artifacts": {},
            "tool_trace": [{"tool": "search_company_contexts"}],
            "model": "test",
        }
        with patch.object(
            lab, "generate_answer", return_value=correction
        ), patch.object(
            lab, "finalize_customer_result", side_effect=lambda result, *args: dict(result)
        ):
            self.manager._run("a" * 32, request, self.access, self.route)

        recovered = self.ledger.load_job("a" * 32)
        self.assertEqual(self.ledger.used_today("alice"), 0)
        self.assertEqual(recovered["result"]["answer_type"], "clarification")
        self.assertTrue(recovered["result"]["requires_clarification"])
        self.assertTrue(
            self.ledger.clarification_continuation_allowed(
                "test-conversation", "alice"
            )
        )

    def test_timeout_releases_reserved_credit(self):
        self.manager.create(self.request, self.access, self.route)
        with patch.object(
            lab, "generate_answer", side_effect=TimeoutError("provider timeout")
        ):
            self.manager._run("a" * 32, self.request, self.access, self.route)

        self.assertEqual(self.ledger.used_today("alice"), 0)
        status = self.ledger.connection.execute(
            "SELECT status FROM query_events WHERE request_id = ?", ["a" * 32]
        ).fetchone()[0]
        self.assertEqual(status, "failed_refunded")
        release_event = next(
            call.args[0]
            for call in self.runtime.write_audit_record.call_args_list
            if call.args and call.args[0].get("record_type") == "ask_credit_released"
        )
        self.assertEqual(release_event["reason"], "provider_timeout")
        self.assertEqual(release_event["answer_type"], "error")
        recovered = self.ledger.load_job("a" * 32)
        self.assertEqual(recovered["failure_stage"], "model")
        self.assertEqual(recovered["error_code"], "provider_timeout")
        self.assertEqual(recovered["http_status"], 504)
        self.assertTrue(recovered["retryable"])

    def test_empty_provider_response_fails_and_releases_credit(self):
        self.manager.create(self.request, self.access, self.route)
        empty = {
            "answer": "",
            "response_id": "resp_empty",
            "answer_artifacts": {},
            "tool_trace": [],
            "model": "test",
        }
        with patch.object(lab, "generate_answer", return_value=empty):
            self.manager._run("a" * 32, self.request, self.access, self.route)

        self.assertEqual(self.ledger.used_today("alice"), 0)
        recovered = self.ledger.load_job("a" * 32)
        self.assertEqual(recovered["status"], "failed")
        status = self.ledger.connection.execute(
            "SELECT status FROM query_events WHERE request_id = ?", ["a" * 32]
        ).fetchone()[0]
        self.assertEqual(status, "failed_refunded")

    def test_provider_failure_message_fails_and_releases_credit(self):
        self.manager.create(self.request, self.access, self.route)
        failure = {
            "answer": (
                "I could not complete this research request because the provider failed. "
                + "Please retry shortly. " * 40
            ),
            "response_id": "resp_provider_failure",
            "answer_artifacts": {"platform_dossier": {"scope": {}}},
            "tool_trace": [{"tool": "get_platform_context", "result": {}}],
            "model": "test",
        }
        with patch.object(lab, "generate_answer", return_value=failure):
            self.manager._run("a" * 32, self.request, self.access, self.route)

        recovered = self.ledger.load_job("a" * 32)
        self.assertEqual(recovered["status"], "failed")
        self.assertEqual(self.ledger.used_today("alice"), 0)
        status = self.ledger.connection.execute(
            "SELECT status FROM query_events WHERE request_id = ?", ["a" * 32]
        ).fetchone()[0]
        self.assertEqual(status, "failed_refunded")

    def test_pdf_uses_owned_saved_answer_and_rejects_clarification(self):
        app = FastAPI()
        app.post('/api/evidence/answer.pdf')(lab.answer_report_export)
        app.get('/api/evidence/answer.pdf')(lab.answer_report_export_download)
        job = {'request_id':'pdf-request','subject_id':'alice','status':'completed',
            '_question':'Original question', 'result':{'response_id':'pdf-response',
            'answer':'Saved research', 'answer_type':'substantive',
            'active_scope':{'scope_name':'AMRAAM'}}}
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
            job['result'].pop('requires_clarification')
            job['result']['answer_type'] = 'validation'
            job['result']['requires_user_correction'] = True
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

    def test_cache_stores_completed_zip_without_json_rehydration(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = EvidencePackCache(Path(tmp))
            key = cache.cache_key('release', 'company-evidence-zip', {'id': '14925'})
            payload = b'PK\x03\x04completed evidence pack'
            cache.set_bytes(key, payload)
            self.assertEqual(cache.get_bytes(key), payload)
            self.assertTrue((Path(tmp) / f'{key}.zip').is_file())


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
