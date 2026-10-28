import unittest
import os
from types import SimpleNamespace
from unittest.mock import Mock, patch

from lab_test_support import load_lab


lab = load_lab()


class StopBeforeModel(Exception):
    pass


class GeographicClarificationTests(unittest.TestCase):
    def setUp(self):
        self.runtime = SimpleNamespace(
            platform_contexts=Mock(mentions=Mock(return_value=[])),
            company_contexts=Mock(search=Mock(return_value={"matches": []})),
            mock_mode=False,
            external_evidence_allowed=True,
            call_tool=Mock(side_effect=lambda name, arguments: {
                "scope": {
                    "state_code": arguments["state_code"],
                    "state_name": lab.STATE_NAMES[arguments["state_code"]],
                },
                "ranked_registered_facilities": [
                    {"cage": "12345", "net_prime_obligations_usd": 1000}
                ],
                "leading_places_of_performance": [],
            }),
            model="test-model",
            reasoning_effort="low",
            max_output_tokens=4000,
            store=SimpleNamespace(manifest={"release_id": "test-release"}),
            write_audit_record=Mock(),
        )

    def request(self, *turns, scope=None, conversation_id="test-conversation-123"):
        messages = []
        for index, turn in enumerate(turns):
            messages.append({"role": "user" if index % 2 == 0 else "assistant", "content": turn})
        return lab.AskRequest(messages=messages, active_scope=scope, conversation_id=conversation_id)

    def route_and_execution(self, request):
        with patch.object(lab, "runtime", self.runtime, create=True):
            route = lab.routing_decision_for_request(request)
            return route, lab.request_for_execution(request, route)

    def model_question(self, request, route):
        provider = Mock()
        provider.responses.create.side_effect = StopBeforeModel
        with patch.object(lab, "runtime", self.runtime, create=True), \
             patch.dict(os.environ, {"OPENAI_API_KEY": "test-only"}), \
             patch.object(lab, "OpenAI", return_value=Mock()), \
             patch.object(lab, "TimedOpenAIClient", return_value=provider):
            with self.assertRaises(StopBeforeModel):
                lab._generate_answer_impl(request, routing=route)
        return provider.responses.create.call_args.kwargs["input"][0]["content"]

    def test_original_three_turn_clarification_retains_location_topic_and_measure(self):
        first = self.request("Florida")
        first_route, _ = self.route_and_execution(first)
        self.assertEqual(first_route.workflow, "general_defense_research")

        second = self.request("Florida", "What would you like to know about Florida?", "Major contractors")
        second_route, second_execution = self.route_and_execution(second)
        self.assertEqual(second_route.workflow, "state_industrial_base")
        self.assertEqual(len(second_execution.messages), 1)
        self.assertIn("Florida", self.model_question(second, second_route))

        third = self.request(
            "Florida", "What would you like to know about Florida?",
            "Major contractors", "Here are the major contractors in Florida.",
            "Prime obligations",
            scope={"scope_type": "state_market", "scope_id": "FL", "scope_name": "Florida"},
        )
        third_route, third_execution = self.route_and_execution(third)
        self.assertEqual(third_route.workflow, "state_industrial_base")
        self.assertFalse(third_route.subject_changed)
        self.assertEqual(third_execution.active_scope.scope_id, "FL")
        self.assertIn("ranked by prime contract obligations", self.model_question(third, third_route))
        self.runtime.call_tool.assert_called_with(
            "get_state_industrial_base", {"state_code": "FL", "limit": 24}
        )

    def test_location_then_subcontractor_topic(self):
        request = self.request("Texas", "What would you like to know about Texas?", "Defence subcontractors")
        route, _ = self.route_and_execution(request)
        self.assertEqual(route.workflow, "state_industrial_base")
        self.assertIn("Texas by reported subcontract value", self.model_question(request, route))

    def test_question_then_location_clarification(self):
        request = self.request(
            "Which companies have the largest defence contracts?",
            "Which location should I use?", "In California",
        )
        route, _ = self.route_and_execution(request)
        self.assertEqual(route.workflow, "state_industrial_base")
        self.assertEqual(
            self.model_question(request, route),
            "Which companies have the largest defence contracts in California?",
        )

    def test_independent_definition_does_not_inherit_florida(self):
        request = self.request("Defence spending in Florida", "Florida overview", "What is an NSN?")
        route, execution = self.route_and_execution(request)
        self.assertEqual(route.workflow, "general_defense_research")
        self.assertIsNone(lab._geographic_clarification_question(request))
        self.assertEqual(
            [item.content for item in lab.answer_messages_for_request(execution, route, route.workflow)],
            ["What is an NSN?"],
        )
        scoped = self.request(
            "Defence spending in Florida", "Florida overview", "What is an NSN?",
            scope={"scope_type": "state_market", "scope_id": "FL"},
        )
        scoped_route, scoped_execution = self.route_and_execution(scoped)
        self.assertEqual(scoped_route.workflow, "general_defense_research")
        self.assertTrue(scoped_route.subject_changed)
        self.assertIsNone(scoped_execution.active_scope)

    def test_standalone_state_question_is_unchanged(self):
        question = "Top defence contractors in Virginia by FY2025 prime obligations."
        request = self.request(question)
        route, execution = self.route_and_execution(request)
        self.assertEqual(route.workflow, "state_industrial_base")
        self.assertEqual(execution.messages[-1].content, question)
        self.assertEqual(self.model_question(request, route), question)

    def test_reconnected_history_resolves_with_same_conversation_id(self):
        conversation_id = "restored-conversation-123"
        restored = self.request(
            "Florida", "What would you like to know about Florida?",
            "Major contractors", "Here are the major contractors in Florida.",
            "Prime obligations", conversation_id=conversation_id,
        )
        replayed = lab.AskRequest(**restored.model_dump())
        route, execution = self.route_and_execution(replayed)
        self.assertEqual(replayed.conversation_id, conversation_id)
        self.assertEqual(route.workflow, "state_industrial_base")
        self.assertIn("Florida, ranked by prime contract obligations", execution.messages[-1].content)

        # The server's saved state scope also supplies the location when an
        # older user turn has fallen outside the browser's recent-message slice.
        shorter = self.request(
            "Major contractors", "Here are the major contractors in Florida.",
            "Prime obligations", conversation_id=conversation_id,
            scope={"scope_type": "state_market", "scope_id": "FL"},
        )
        short_route, short_execution = self.route_and_execution(shorter)
        self.assertEqual(short_route.workflow, "state_industrial_base")
        self.assertIn("Florida, ranked by prime contract obligations", short_execution.messages[-1].content)


if __name__ == "__main__":
    unittest.main()
