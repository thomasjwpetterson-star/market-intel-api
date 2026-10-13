import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock, patch

from lab_test_support import load_lab
from platform_context import PlatformContextStore
from platform_conversation_segments import platform_segment_projection, platform_topic_options

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
                if arguments.get("segment"):
                    return platform_segment_projection(self.pack, arguments["segment"])
                return self.pack
            raise AssertionError(name)
        self.runtime = SimpleNamespace(
            platform_contexts=store,
            company_contexts=Mock(search=Mock(return_value={"matches": []})),
            mock_mode=False, external_evidence_allowed=True,
            call_tool=tool, optional_program_outlook=Mock(return_value=None),
            write_audit_record=Mock(),
            model="test", reasoning_effort="high", max_output_tokens=10000,
            store=SimpleNamespace(manifest={"release_id": "test-release"}),
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
            ("What about F-35 instead?", "platform_intelligence"),
        ):
            with self.subTest(question=question), patch.object(lab, "runtime", self.runtime, create=True):
                request = lab.AskRequest(messages=self.history + [{"role": "user", "content": question}], active_scope=self.scope)
                route = lab.routing_decision_for_request(request)
                self.assertEqual(route.workflow, workflow)
                self.assertTrue(route.subject_changed)
                self.assertEqual(len(lab.request_for_execution(request, route).messages), 1)

    def test_natural_platform_followups_keep_scope_without_a_menu(self):
        for question in (
            "Is that all the suppliers?",
            "What about its parts?",
            "Okay, and FY2026?",
            "Could you show the underlying award evidence?",
        ):
            with self.subTest(question=question), patch.object(lab, "runtime", self.runtime, create=True):
                request = lab.AskRequest(
                    messages=self.history + [{"role": "user", "content": question}],
                    active_scope=self.scope, answer_mode="conversation",
                )
                route = lab.routing_decision_for_request(request)
                self.assertEqual(route.workflow, "platform_intelligence")
                self.assertFalse(route.subject_changed)
                self.assertFalse(lab.is_fast_conversation_topic_menu(request, route))

    def test_natural_named_platform_questions_route_to_platform(self):
        for question in (
            "What is B-52 and why does it matter?",
            "How much was spent on B-52 in FY2025 and FY2026?",
            "Show me topic options for B-52",
        ):
            with self.subTest(question=question), patch.object(lab, "runtime", self.runtime, create=True):
                request = lab.AskRequest(
                    messages=[{"role": "user", "content": question}],
                    answer_mode="conversation",
                )
                route = lab.routing_decision_for_request(request)
                self.assertEqual(route.workflow, "platform_intelligence")

    def test_natural_company_opening_extracts_named_subject(self):
        question = ("I'm assessing Curtiss-Wright as a defense supplier. Give me the quick picture: "
                    "its important sites, customers, awards, and platform exposure.")
        self.assertEqual(
            lab.explicit_company_name_query([lab.ChatMessage(role="user", content=question)]),
            "Curtiss-Wright",
        )
        self.assertFalse(lab.company_detail_requested(question))
        self.assertEqual(
            lab.explicit_company_name_query([
                lab.ChatMessage(role="user", content="Give me the quick picture on Curtiss-Wright as a defence supplier.")
            ]),
            "Curtiss-Wright",
        )

    def test_company_pronoun_followup_retains_parent_scope(self):
        history = [
            {"role": "user", "content": "Give me the quick picture on Curtiss-Wright as a defence supplier."},
            {"role": "assistant", "content": "Curtiss-Wright has prime and reported subcontract activity."},
            {"role": "user", "content": "Which sites and customers matter most"},
            {"role": "assistant", "content": "Electric Boat is a leading reported prime-contractor route."},
            {"role": "user", "content": "what about their subcontracts"},
        ]
        request = lab.AskRequest(
            messages=history,
            active_scope={"scope_type": "company_parent", "scope_id": "PARENT_CURTISS",
                          "scope_name": "CURTISS-WRIGHT CORPORATION", "resolved_cages": ["04808"]},
            answer_mode="conversation",
        )
        self.assertIsNone(lab.explicit_company_name_query(request.messages))
        with patch.object(lab, "runtime", self.runtime, create=True):
            route = lab.routing_decision_for_request(request)
        self.assertEqual(route.workflow, "company_site_intelligence")
        self.assertFalse(route.clarification_needed)
        self.assertFalse(route.subject_changed)
        self.assertTrue(route.candidates[0].uses_current_scope)

    def test_new_subject_escapes_a_prior_contract_thread(self):
        contract_scope = {"scope_type": "contract", "scope_id": "N0001923F2616",
                          "scope_name": "N0001923F2616"}
        history = [
            {"role": "user", "content": "Explain contract N0001923F2616"},
            {"role": "assistant", "content": "It buys Tomahawk engineering services."},
        ]
        cases = (
            ("Please show me sources sought opportunities that an avionics supplier may be able to respond to",
             "market_record_search"),
            ("Give me the quick picture on Curtiss-Wright as a defence supplier.",
             "company_site_intelligence"),
        )
        with patch.object(lab, "runtime", self.runtime, create=True):
            for question, workflow in cases:
                with self.subTest(question=question):
                    request = lab.AskRequest(
                        messages=history + [{"role": "user", "content": question}],
                        active_scope=contract_scope, answer_mode="conversation",
                    )
                    route = lab.routing_decision_for_request(request)
                    self.assertEqual(route.workflow, workflow)
                    self.assertTrue(route.subject_changed)
                    self.assertFalse(route.clarification_needed)

    def test_general_research_company_evidence_establishes_only_unambiguous_scope(self):
        def company_entry(scope_id):
            return {"tool": "get_company_context", "result": {
                "scope": {"scope_type": "company_parent", "scope_id": scope_id,
                          "scope_name": scope_id},
                "identity": {"resolved_cages": ["04808"],
                             "scope_kind": "observed_company_group"},
            }}
        single = lab.active_scope_from_result({"tool_trace": [company_entry("PARENT_CURTISS")]})
        self.assertEqual(single.scope_id, "PARENT_CURTISS")
        self.assertEqual(single.resolved_cages, ["04808"])
        comparison = lab.active_scope_from_result({"tool_trace": [
            company_entry("PARENT_CURTISS"), company_entry("PARENT_OTHER")
        ]})
        self.assertIsNone(comparison)

    def test_broad_conversation_language_routes_across_main_workflows(self):
        cases = (
            ("Tell me about B-52.", "platform_intelligence"),
            ("How much was spent on B-52 in FY2025 and FY2026?", "platform_intelligence"),
            ("I'm assessing Curtiss-Wright as a defense supplier. Give me the quick picture: its important sites, customers, awards, and platform exposure.", "company_site_intelligence"),
            ("Give me the quick picture on Curtiss-Wright as a defence supplier.", "company_site_intelligence"),
            ("What does NSN 1560-00-817-5790 tell us about demand, available sources, and competition?", "item_intelligence"),
            ("Explain contract N0001923F2616 in plain language: what it buys, who received it, how much has been obligated, and how the actions changed over time.", "contract_or_opportunity"),
            ("Across US missile programs, what has actually grown in the observed fiscal years, and which rankings are sensitive to data gaps?", "program_momentum"),
            ("Are there currently open US Sources Sought notices for military aircraft avionics? Give me the strongest matches and say what still needs checking before pursuit.", "market_record_search"),
        )
        with patch.object(lab, "runtime", self.runtime, create=True):
            for question, workflow in cases:
                with self.subTest(question=question):
                    request = lab.AskRequest(messages=[{"role": "user", "content": question}],
                                             answer_mode="conversation")
                    route = lab.routing_decision_for_request(request)
                    self.assertEqual(route.workflow, workflow)
                    self.assertFalse(route.clarification_needed)

    def test_record_search_follow_up_keeps_opportunity_scope(self):
        request = lab.AskRequest(
            messages=[{"role": "user", "content": "Which deadline is soonest?"}],
            active_scope={"scope_type": "record_search", "scope_id": "OPP::aircraft avionics",
                          "scope_name": "aircraft avionics"},
            answer_mode="conversation",
        )
        with patch.object(lab, "runtime", self.runtime, create=True):
            route = lab.routing_decision_for_request(request)
        self.assertEqual(route.workflow, "market_record_search")
        self.assertFalse(route.subject_changed)

    def test_current_opportunity_question_requires_live_status_check(self):
        self.assertTrue(lab.current_opportunity_status_requested(
            "Are there currently open Sources Sought notices?", "opportunity"
        ))
        self.assertFalse(lab.explicit_sam_verification_requested(
            "Are there currently open Sources Sought notices?", "opportunity"
        ))
        self.assertTrue(lab.explicit_sam_verification_requested(
            "Can you confirm on SAM.gov whether the first notice is still open right now?",
            "opportunity",
        ))
        self.assertFalse(lab.current_opportunity_status_requested(
            "Show recent awards", "award"
        ))

    def test_conversation_uses_snapshot_until_live_status_is_explicitly_requested(self):
        pack = {"context_type": "market_record_search", "scope": {}, "records": [], "coverage": {}}
        self.runtime.call_tool = Mock(return_value=pack)
        self.runtime.store.manifest["generated_at"] = "2026-10-05T10:53:47Z"
        client = Mock()
        client.responses.create.side_effect = StopBeforeModel
        cases = (
            ("Are there currently open US Sources Sought notices for military aircraft avionics?",
             "auto", "snapshot_unverified", False),
            ("Can you confirm on SAM.gov whether the aircraft avionics Sources Sought notices are still open right now?",
             "required", "live_sam_required", True),
            ("Find recent awards concerning electronic warfare.", "auto", None, False),
        )
        for question, expected_choice, expected_status, has_web_tool in cases:
            with self.subTest(question=question), \
                 patch.object(lab, "runtime", self.runtime, create=True), \
                 patch.object(lab, "OpenAI", return_value=client), \
                 patch.dict("os.environ", {"OPENAI_API_KEY": "test-placeholder"}):
                request = lab.AskRequest(messages=[{"role": "user", "content": question}],
                                         answer_mode="conversation",
                                         active_scope=(
                                             {"scope_type": "record_search",
                                              "scope_id": "OPP|SOURCES_SOUGHT|aircraft-avionics",
                                              "scope_name": "aircraft avionics"}
                                             if expected_status == "live_sam_required" else None
                                         ))
                route = lab.routing_decision_for_request(request)
                self.assertEqual(route.workflow, "market_record_search")
                with self.assertRaises(StopBeforeModel):
                    lab.generate_answer(request, routing=route)
                model_call = client.responses.create.call_args.kwargs
                self.assertEqual(model_call["tool_choice"], expected_choice)
                self.assertEqual(bool(model_call["tools"]), has_web_tool)
                if expected_status:
                    self.assertIn(f'"status_verification": "{expected_status}"',
                                  model_call["input"][-1]["content"])
                    self.assertIn('"release_generated_at": "2026-10-05T10:53:47Z"',
                                  model_call["input"][-1]["content"])

    def test_conversation_fiscal_note_rolls_over_in_october(self):
        september = lab.conversation_fiscal_status_note(date(2026, 9, 30))
        october = lab.conversation_fiscal_status_note(date(2026, 10, 1))
        self.assertIn("latest ended U.S. federal fiscal year is FY2025", september)
        self.assertIn("FY2026 is underway", september)
        self.assertIn("latest ended U.S. federal fiscal year is FY2026", october)
        self.assertIn("FY2027 is underway", october)

    def test_company_overview_does_not_hide_explicit_site_or_item_detail(self):
        for question in (
            "Give me an overview, then identify which CAGE sites received the awards.",
            "Give me an overview of every site and its awards.",
            "Give me an overview for NSN 1560-00-817-5790.",
        ):
            with self.subTest(question=question):
                self.assertTrue(lab.company_detail_requested(question))

        self.assertTrue(lab.company_award_site_requested(
            "Which specific recent awards support that, and which CAGE sites received them?"
        ))
        self.assertFalse(lab.company_award_site_requested(
            "How has the supply chain changed?"
        ))

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

    def test_conversation_explicit_options_request_returns_free_topic_menu(self):
        request = lab.AskRequest(messages=[{"role": "user", "content": "Show me topic options for B-52"}],
                                 answer_mode="conversation")
        route = lab.RoutingDecision(workflow="platform_intelligence", reason="explicit_platform", confidence=1)
        with patch.object(lab, "runtime", self.runtime, create=True), \
             patch.dict("os.environ", {"OPENAI_API_KEY": "test-placeholder"}):
            result = lab.generate_answer(request, routing=route)
        self.assertEqual(result["answer_type"], "clarification")
        self.assertEqual(result["response_calls"], 0)
        self.assertIsNone(result["estimated_cost"])
        self.assertEqual(result["answer_artifacts"]["platform_dossier"]["scope"]["platform_id"], "B-52")
        self.assertEqual(len(result["answer_artifacts"]["conversation_topics"]["options"]), 5)

    def test_conversation_broad_and_all_topics_questions_reach_a_bounded_overview(self):
        self.pack.update({
            "scope": {"platform_id": "B-52"},
            "coverage": {"reported_supplier_sites": 2},
            "direct_award_recipients": [{"cage": "prime"}],
            "reported_supplier_sites": [{"cage": "supplier"}],
            "top_prime_awards": [{"contract_id": "award"}],
            "item_and_component_evidence": {"top_items": [{"niin": "123456789"}]},
        })
        client = Mock()
        client.responses.create.side_effect = StopBeforeModel
        for question in ("Tell me everything about B-52", "Research all of the above for B-52"):
            with self.subTest(question=question), \
                 patch.object(lab, "runtime", self.runtime, create=True), \
                 patch.object(lab, "OpenAI", return_value=client), \
                 patch.dict("os.environ", {"OPENAI_API_KEY": "test-placeholder"}):
                request = lab.AskRequest(
                    messages=[{"role": "user", "content": question}],
                    active_scope=self.scope, answer_mode="conversation",
                )
                route = lab.routing_decision_for_request(request)
                self.assertEqual(route.workflow, "platform_intelligence")
                self.assertFalse(lab.is_fast_conversation_topic_menu(request, route))
                with self.assertRaises(StopBeforeModel):
                    lab.generate_answer(request, routing=route)
                model_input = client.responses.create.call_args.kwargs["input"][-1]["content"]
                self.assertIn('"requested_segment":"overview"', model_input)
                self.assertIn("item_and_component_evidence", model_input)

    def test_conversation_all_topics_reply_keeps_platform_scope(self):
        request = lab.AskRequest(
            messages=[{"role": "user", "content": "Research all of the above for B-52"}],
            active_scope=self.scope, answer_mode="conversation",
        )
        with patch.object(lab, "runtime", self.runtime, create=True):
            route = lab.routing_decision_for_request(request)
            self.assertEqual(route.workflow, "platform_intelligence")
            self.assertFalse(route.subject_changed)

    def test_bare_conversation_topic_replies_keep_platform_routing(self):
        history = [
            {"role": "user", "content": "Tell me everything about B-52"},
            {"role": "assistant", "content": "What would you like to explore?"},
        ]
        with patch.object(lab, "runtime", self.runtime, create=True):
            for topic in ("Prime contractor", "Awards and obligations", "NSNs and parts",
                          "Supply chain", "All of the above"):
                with self.subTest(topic=topic):
                    request = lab.AskRequest(
                        messages=history + [{"role": "user", "content": topic}],
                        active_scope=self.scope, answer_mode="conversation",
                    )
                    route = lab.routing_decision_for_request(request)
                    self.assertEqual(route.workflow, "platform_intelligence")
                    self.assertFalse(route.subject_changed)
                    if topic == "All of the above":
                        self.assertFalse(lab.is_fast_conversation_topic_menu(request, route))

    def test_conversation_supply_chain_uses_focused_pack_and_skips_outlook(self):
        self.pack.update({
            "scope": {"platform_id": "B-52", "observation_window": "FY2021–FY2027"},
            "coverage": {"reported_supplier_sites": 20},
            "reported_supplier_sites": [{"cage": str(i)} for i in range(20)],
            "direct_award_recipients": [{"cage": "prime"}],
            "top_prime_awards": [{"contract_id": "example"}],
        })
        request = lab.AskRequest(messages=[{"role": "user", "content": "Show me the B-52 supply chain"}],
                                 answer_mode="conversation")
        route = lab.RoutingDecision(workflow="platform_intelligence", reason="explicit_platform", confidence=1)
        client = Mock()
        client.responses.create.side_effect = StopBeforeModel
        with patch.object(lab, "runtime", self.runtime, create=True), \
             patch.object(lab, "OpenAI", return_value=client), \
             patch.dict("os.environ", {"OPENAI_API_KEY": "test-placeholder"}):
            with self.assertRaises(StopBeforeModel):
                lab.generate_answer(request, routing=route)
        model_call = client.responses.create.call_args.kwargs
        self.assertIn("MIMIR PLATFORM TOPIC EVIDENCE", model_call["input"][-1]["content"])
        self.assertNotIn("top_prime_awards", model_call["input"][-1]["content"].split('"evidence_scope"')[0])
        self.assertEqual(model_call["tools"], [])
        self.assertEqual(self.runtime.optional_program_outlook.call_count, 0)

    def test_each_topic_button_prompt_resolves_the_same_platform(self):
        store = object.__new__(PlatformContextStore)
        store.platforms = ["AMRAAM"]
        for option in platform_topic_options("AMRAAM"):
            with self.subTest(option=option["topic"]):
                messages = [lab.ChatMessage(role="user", content=option["prompt"])]
                self.assertEqual(lab.explicit_platform_query(messages, store), "AMRAAM")


if __name__ == "__main__":
    unittest.main()
