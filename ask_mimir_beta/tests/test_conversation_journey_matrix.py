"""Data-free Conversation routing journeys, including changes of subject.

The real parsers and route selector run here; only the multi-GB evidence stores
are replaced. This catches a plausible answer being fed the previous subject's
pack before the model is called.
"""

import ast
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from lab_test_support import load_lab
from platform_context import PLATFORM_ALIASES, PlatformContextStore


lab = load_lab()


class ConversationJourneyMatrixTests(unittest.TestCase):
    def setUp(self):
        platforms = object.__new__(PlatformContextStore)
        platforms.platforms = sorted(set(PLATFORM_ALIASES.values()) | {
            "AMRAAM", "B-52", "F-16", "F-35", "THAAD", "CH-53K",
            "TOMAHAWK", "HIMARS", "SM-6", "UH-60", "CH-47",
            "STRYKER", "M2 BRADLEY", "P-8A", "PAC-3 MSE",
            "PATRIOT AIR DEFENSE SYSTEM",
        })
        self.runtime = SimpleNamespace(
            platform_contexts=platforms,
            company_contexts=Mock(search=Mock(return_value={"matches": []})),
        )

    def route(self, question, scope=None, history=None, mode="conversation"):
        messages = (history or []) + [{"role": "user", "content": question}]
        with patch.object(lab, "runtime", self.runtime, create=True):
            return lab.routing_decision_for_request(lab.AskRequest(
                messages=messages, active_scope=scope, answer_mode=mode,
            ))

    def test_existing_routing_corpus_in_conversation_mode(self):
        """Apply the established broad/fuzzy corpus with Conversation selected."""
        path = Path(__file__).resolve().parents[1] / "evaluate_workflow_routing.py"
        tree = ast.parse(path.read_text())

        def literal(name):
            for node in tree.body:
                if isinstance(node, ast.Assign) and any(
                    isinstance(target, ast.Name) and target.id == name
                    for target in node.targets
                ):
                    return ast.literal_eval(node.value)
            raise AssertionError(name)

        rows = [
            (workflow, question)
            for workflow, questions in literal("ROUTING_CASES").items()
            for question in questions
        ]
        rows.extend(
            (workflow, template.format(subject=subject))
            for workflow, family in literal("FUZZY_PARAPHRASE_FAMILIES").items()
            for subject in family["subjects"]
            for template in family["templates"]
        )
        rows.extend(
            case
            for cases in literal("ROUTING_ROBUSTNESS_CASES").values()
            for case in cases
        )
        self.assertGreaterEqual(len(rows), 400)
        failures = [
            (question, workflow, actual)
            for workflow, question in rows
            if (actual := self.route(question).workflow) != workflow
        ]
        self.assertEqual(failures, [])

    def test_isolated_workflow_breadth(self):
        cases = [
            ("Give me the quick picture on Curtiss-Wright as a defence supplier.", "company_site_intelligence"),
            ("What does Honeywell supply into the US defence market?", "company_site_intelligence"),
            ("What does CAGE 19645 manufacture?", "company_site_intelligence"),
            ("Tell me about AMRAAM.", "platform_intelligence"),
            ("Which suppliers support THAAD?", "platform_intelligence"),
            ("What is NSN 1560-00-817-5790?", "item_intelligence"),
            ("Explain contract N0001923F2616.", "contract_or_opportunity"),
            ("Find open Sources Sought notices for military avionics.", "market_record_search"),
            ("Please show me sources sought opportunities that an avionics supplier may be able to respond to", "market_record_search"),
            ("Give me an overview of the US C-UAS market.", "market_segment_intelligence"),
            ("Map Alabama's defense industrial base.", "state_industrial_base"),
            ("Identify suppliers of rugged mission computers.", "capability_discovery"),
            ("Compare F-16 and F-35 suppliers.", "platform_comparison"),
            ("Which missile programs are accelerating fastest?", "program_momentum"),
            ("What is the outlook for DFIRS 2100?", "product_intelligence"),
            ("Who competes with Eaton Aerospace?", "competitor_discovery"),
            ("What is the weather forecast?", "out_of_domain"),
        ]
        for question, workflow in cases:
            with self.subTest(question=question):
                route = self.route(question)
                self.assertEqual(route.workflow, workflow)
                self.assertFalse(route.clarification_needed)

    def test_company_pronouns_and_natural_followups(self):
        scope = {"scope_type": "company_parent", "scope_id": "PARENT_CURTISS",
                 "scope_name": "CURTISS-WRIGHT CORPORATION", "resolved_cages": ["04808"]}
        history = [
            {"role": "user", "content": "Give me the quick picture on Curtiss-Wright as a defence supplier."},
            {"role": "assistant", "content": "Curtiss-Wright has prime and reported subcontract activity."},
        ]
        for question in (
            "Which sites and customers matter most?",
            "What about their subcontracts?",
            "Does it have any significant prime awards?",
            "Who are its biggest prime-contractor customers?",
            "How has that changed since FY2021?",
            "What evidence supports those links?",
            "And what about subcontractors beneath its own awards?",
        ):
            with self.subTest(question=question):
                route = self.route(question, scope, history)
                self.assertEqual(route.workflow, "company_site_intelligence")
                self.assertFalse(route.subject_changed)
                self.assertFalse(route.clarification_needed)

    def test_cross_workflow_subject_changes(self):
        scopes = {
            "contract": {"scope_type": "contract", "scope_id": "N0001923F2616", "scope_name": "N0001923F2616"},
            "company": {"scope_type": "company_parent", "scope_id": "PARENT_CURTISS", "scope_name": "CURTISS-WRIGHT CORPORATION"},
            "platform": {"scope_type": "platform", "scope_id": "AMRAAM", "scope_name": "AMRAAM"},
            "item": {"scope_type": "item", "scope_id": "008175790", "scope_name": "1560-00-817-5790"},
            "search": {"scope_type": "record_search", "scope_id": "OPP|avionics", "scope_name": "Avionics opportunities"},
        }
        cases = [
            ("contract", "Please show me sources sought opportunities that an avionics supplier may be able to respond to", "market_record_search"),
            ("contract", "Give me the quick picture on Curtiss-Wright as a defence supplier.", "company_site_intelligence"),
            ("contract", "Tell me about AMRAAM.", "platform_intelligence"),
            ("company", "Tell me about Honeywell as a defence supplier.", "company_site_intelligence"),
            ("company", "Now tell me about AMRAAM.", "platform_intelligence"),
            ("company", "Find current avionics Sources Sought notices.", "market_record_search"),
            ("platform", "Give me the quick picture on Curtiss-Wright as a defence supplier.", "company_site_intelligence"),
            ("platform", "Explain contract N0001923F2616.", "contract_or_opportunity"),
            ("item", "Explain contract N0001923F2616.", "contract_or_opportunity"),
            ("search", "Give me the quick picture on Curtiss-Wright as a defence supplier.", "company_site_intelligence"),
            ("search", "Tell me about THAAD.", "platform_intelligence"),
        ]
        for old, question, workflow in cases:
            with self.subTest(old=old, question=question):
                route = self.route(question, scopes[old])
                self.assertEqual(route.workflow, workflow)
                self.assertTrue(route.subject_changed)
                self.assertFalse(route.clarification_needed)

    def test_modes_route_identically(self):
        for question in (
            "Tell me about AMRAAM.",
            "Give me the quick picture on Curtiss-Wright as a defence supplier.",
            "Explain contract N0001923F2616.",
        ):
            with self.subTest(question=question):
                self.assertEqual(
                    self.route(question, mode="conversation").workflow,
                    self.route(question, mode="deep_research").workflow,
                )


if __name__ == "__main__":
    unittest.main()
