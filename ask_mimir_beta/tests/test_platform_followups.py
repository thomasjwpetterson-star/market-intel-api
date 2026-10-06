import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from lab_test_support import load_lab
from platform_context import PlatformContextStore

lab = load_lab()


class StopBeforeModel(Exception):
    pass


class PlatformFollowupTests(unittest.TestCase):
    def setUp(self):
        store = object.__new__(PlatformContextStore)
        store.platforms = ["B-52", "F-35"]
        self.pack = {}
        def tool(name, arguments):
            if name == "search_platform_contexts":
                return {"resolved_platform_id": "B-52"}
            if name == "get_platform_context":
                return self.pack
            raise AssertionError(name)
        self.runtime = SimpleNamespace(
            platform_contexts=store,
            company_contexts=Mock(search=Mock(return_value={"matches": []})),
            mock_mode=False, external_evidence_allowed=True,
            call_tool=tool, optional_program_outlook=Mock(return_value=None),
            write_audit_record=Mock(),
            model="test", reasoning_effort="high", max_output_tokens=10000,
        )
        self.scope = {"scope_type": "platform", "scope_id": "B-52", "scope_name": "B-52"}
        self.history = [
            {"role": "user", "content": "Tell me everything about the B-52 supply chain"},
            {"role": "assistant", "content": "B-52 supply-chain overview: Boeing integrates the modernisation work."},
        ]

    def test_award_followup_keeps_platform_and_conversation(self):
        for question in (
            "What about the larger modernisation awards ongoing",
            "What about the larger modernization awards ongoing?",
            "Show me the major active contracts",
            "What are those awards worth?",
        ):
            with self.subTest(question=question), patch.object(lab, "runtime", self.runtime, create=True):
                request = lab.AskRequest(messages=self.history + [{"role": "user", "content": question}], active_scope=self.scope)
                route = lab.routing_decision_for_request(request)
                self.assertEqual(route.workflow, "platform_intelligence")
                self.assertFalse(route.subject_changed)
                self.assertFalse(route.clarification_needed)
                execution = lab.request_for_execution(request, route)
                self.assertEqual(execution.active_scope.scope_id, "B-52")
                self.assertEqual(len(execution.messages), 3)

    def test_real_new_company_or_platform_still_changes_subject(self):
        for question, workflow in (
            ("What about Honeywell?", "company_site_intelligence"),
            ("Tell me about F-35", "platform_intelligence"),
        ):
            with self.subTest(question=question), patch.object(lab, "runtime", self.runtime, create=True):
                request = lab.AskRequest(messages=self.history + [{"role": "user", "content": question}], active_scope=self.scope)
                route = lab.routing_decision_for_request(request)
                self.assertEqual(route.workflow, workflow)
                self.assertTrue(route.subject_changed)
                self.assertEqual(len(lab.request_for_execution(request, route).messages), 1)

    def test_platform_confirmation_answers_pending_question_with_history(self):
        messages = self.history + [
            {"role": "user", "content": "What about the larger modernisation awards ongoing"},
            {"role": "assistant", "content": "Which company, platform, or modernization portfolio do you mean?"},
            {"role": "user", "content": "B-52 - as mentioned"},
        ]
        client = Mock()
        client.responses.create.side_effect = StopBeforeModel
        with patch.object(lab, "runtime", self.runtime, create=True), patch.object(lab, "OpenAI", return_value=client), patch.dict("os.environ", {"OPENAI_API_KEY": "test-placeholder"}):
            request = lab.AskRequest(messages=messages)
            route = lab.routing_decision_for_request(request)
            self.assertEqual(route.workflow, "platform_intelligence")
            self.assertFalse(route.clarification_needed)
            with self.assertRaises(StopBeforeModel):
                lab.generate_answer(request, routing=route)
        self.assertEqual(self.pack["requested_answer_mode"], "platform_awards")
        self.assertEqual(self.pack["requested_question"], messages[2]["content"])
        self.assertEqual(client.responses.create.call_args.kwargs["input"][:5], messages)

    def test_direct_followup_reaches_model_with_prior_context(self):
        messages = self.history + [{"role": "user", "content": "What about the larger modernisation awards ongoing"}]
        client = Mock()
        client.responses.create.side_effect = StopBeforeModel
        with patch.object(lab, "runtime", self.runtime, create=True), patch.object(lab, "OpenAI", return_value=client), patch.dict("os.environ", {"OPENAI_API_KEY": "test-placeholder"}):
            request = lab.AskRequest(messages=messages, active_scope=self.scope)
            route = lab.routing_decision_for_request(request)
            with self.assertRaises(StopBeforeModel):
                lab.generate_answer(request, routing=route)
        self.assertEqual(self.pack["requested_answer_mode"], "platform_awards")
        self.assertEqual(client.responses.create.call_args.kwargs["input"][:3], messages)

    def test_compact_supplier_years_change_only_model_input(self):
        annual = {
            "cage": "12345", "fiscal_year": 2026,
            "mimir_modelled_reported_subcontract_value_usd": 125.0,
            "source_reported_value_usd": 120.0,
            "selected_report_count": 2,
            "prime_award_count": 1,
        }
        self.pack["reported_supplier_sites"] = [{
            "cage": "12345", "supplier_name": "Example supplier",
            "annual_reported_subcontract_activity": [annual],
        }]
        client = Mock()
        client.responses.create.side_effect = StopBeforeModel
        request = lab.AskRequest(messages=[{
            "role": "user", "content": "Which companies supply B-52?",
        }])
        route = lab.RoutingDecision(
            workflow="platform_intelligence", reason="explicit_platform", confidence=1,
        )
        with patch.object(lab, "runtime", self.runtime, create=True), \
             patch.object(lab, "OpenAI", return_value=client), \
             patch.dict("os.environ", {
                 "OPENAI_API_KEY": "test-placeholder",
                 "ASK_MIMIR_COMPACT_PLATFORM_EVIDENCE": "1",
             }):
            with self.assertRaises(StopBeforeModel):
                lab.generate_answer(request, routing=route)
        model_text = client.responses.create.call_args.kwargs["input"][-1]["content"]
        self.assertIn('"supplier_annual_activity_table":', model_text)
        self.assertIn('[2026,125.0,120.0,2,1,null]', model_text)
        self.assertEqual(
            self.pack["reported_supplier_sites"][0]["annual_reported_subcontract_activity"],
            [annual],
        )
        client.reset_mock()
        client.responses.create.side_effect = StopBeforeModel
        with patch.object(lab, "runtime", self.runtime, create=True), \
             patch.object(lab, "OpenAI", return_value=client), \
             patch.dict("os.environ", {
                 "OPENAI_API_KEY": "test-placeholder",
                 "ASK_MIMIR_COMPACT_PLATFORM_EVIDENCE": "0",
             }):
            with self.assertRaises(StopBeforeModel):
                lab.generate_answer(request, routing=route)
        original_model_text = client.responses.create.call_args.kwargs["input"][-1]["content"]
        self.assertNotIn('"supplier_annual_activity_table"', original_model_text)
        self.assertIn('"fiscal_year": 2026', original_model_text)


if __name__ == "__main__":
    unittest.main()
