import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from lab_test_support import load_lab


lab = load_lab()


class ClothingConversationTests(unittest.TestCase):
    def setUp(self):
        self.runtime = SimpleNamespace(
            platform_contexts=Mock(mentions=Mock(return_value=[])),
            company_contexts=Mock(search=Mock(return_value={"matches": []})),
            routing_company_contexts=Mock(search=Mock(return_value={"matches": []})),
        )

    def request(self, *turns, scope=None):
        return lab.AskRequest(
            messages=[
                {"role": "user" if index % 2 == 0 else "assistant", "content": turn}
                for index, turn in enumerate(turns)
            ],
            active_scope=scope,
        )

    def route(self, request):
        with patch.object(lab, "runtime", self.runtime, create=True):
            decision = lab.routing_decision_for_request(request)
            return decision, lab.request_for_execution(request, decision)

    def test_named_garment_does_not_become_an_unresolved_company(self):
        for question in (
            "M81 woodland jacket",
            "What do you know about the M81 woodland jacket?",
            "Give me an overview of the M81 woodland jacket",
        ):
            with self.subTest(question=question):
                request = self.request(question)
                decision, execution = self.route(request)
                self.assertEqual(decision.workflow, "general_defense_research")
                self.assertFalse(decision.clarification_needed)
                self.assertEqual(execution.messages[-1].content, question)

    def test_stock_number_reply_keeps_the_garment_subject(self):
        request = self.request(
            "M81 woodland jacket", "What would you like to know about it?", "Stock NO"
        )
        decision, execution = self.route(request)
        self.assertEqual(decision.workflow, "general_defense_research")
        self.assertEqual(
            execution.messages[-1].content,
            "What is the National Stock Number (NSN) for the M81 woodland jacket?",
        )
        self.assertEqual(request.messages[-1].content, "Stock NO")
        self.assertTrue(lab._needs_clothing_web_search(execution.messages[-1].content))

    def test_named_garment_can_use_current_sources_without_widening_other_research(self):
        self.assertTrue(lab._needs_clothing_web_search("M81 woodland jacket"))
        self.assertFalse(lab._needs_clothing_web_search("What are AMRAAM suppliers?"))

    def test_identifier_and_supplier_follow_up_stay_with_the_item(self):
        request = self.request("8415-01-099-7839")
        decision, _ = self.route(request)
        self.assertEqual(decision.workflow, "item_intelligence")
        scope = {"scope_type": "item", "scope_id": "010997839", "scope_name": "8415-01-099-7839"}
        follow_up = self.request(
            "8415-01-099-7839", "The item dossier for that NSN.",
            "Was Alpha Industries the manufacturer, which branch ordered it, and when was it made?",
            scope=scope,
        )
        decision, execution = self.route(follow_up)
        self.assertEqual(decision.workflow, "item_intelligence")
        self.assertFalse(decision.subject_changed)
        self.assertEqual(execution.active_scope.scope_id, "010997839")
        self.assertEqual(len(execution.messages), 3)

    def test_stock_number_without_a_garment_does_not_invent_one(self):
        request = self.request("Stock NO")
        _, execution = self.route(request)
        self.assertEqual(execution.messages[-1].content, "Stock NO")


if __name__ == "__main__":
    unittest.main()
