import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from lab_test_support import load_lab


lab = load_lab()


RTX_OPTIONS = """Which RTX site did you mean?

- RAYTHEON COMPANY (CAGE 15090)
- RTX CORPORATION - EAST HARTFORD, CT (CAGE 52661)
- RAYTHEON COMPANY (CAGE 4U884)
- RTX CORPORATION - parent view (207 sites)

Choose an option, or type another CAGE code or location."""


class CompanyClarificationFollowupTests(unittest.TestCase):
    def request(self, reply, options=RTX_OPTIONS):
        return lab.AskRequest(messages=[
            {"role": "user", "content": "Tell me everything about this defense supplier: RTX"},
            {"role": "assistant", "content": options},
            {"role": "user", "content": reply},
        ], answer_mode="conversation")

    def test_east_hartford_selects_offered_rtx_cage(self):
        request = self.request("East Hartford")
        resolved = lab._request_with_resolved_company_clarification(request)
        self.assertEqual(len(resolved.messages), 1)
        self.assertEqual(resolved.messages[0].content, "52661")
        self.assertFalse(lab.company_detail_requested(resolved.messages[0].content))
        self.assertEqual(lab.company_site_dossier_cage(resolved.messages), "52661")

        runtime = SimpleNamespace(
            platform_contexts=Mock(mentions=Mock(return_value=[])),
            company_contexts=Mock(),
        )
        with patch.object(lab, "runtime", runtime, create=True):
            route = lab.routing_decision_for_request(request)
            execution = lab.request_for_execution(request, route)
        self.assertEqual(route.workflow, "company_site_intelligence")
        self.assertEqual(route.reason, "explicit_cage_identifier")
        self.assertEqual(lab.company_site_dossier_cage(execution.messages), "52661")

    def test_clicked_site_option_uses_same_conversational_focus(self):
        request = self.request("Tell me everything about this defense supplier site: CAGE 52661")
        resolved = lab._request_with_resolved_company_clarification(request)
        self.assertEqual(resolved.messages[0].content, "52661")
        self.assertFalse(lab.company_detail_requested(resolved.messages[0].content))

    def test_unoffered_location_cannot_select_unrelated_supplier(self):
        request = self.request("Cambridge")
        self.assertIs(lab._request_with_resolved_company_clarification(request), request)
        route = lab.routing_decision_for_request(request)
        self.assertEqual(route.reason, "unmatched_company_clarification")
        self.assertTrue(route.clarification_needed)

    def test_ambiguous_location_is_not_forced_to_one_site(self):
        options = RTX_OPTIONS.replace(
            "- RAYTHEON COMPANY (CAGE 4U884)",
            "- RAYTHEON COMPANY - EAST HARTFORD, CT (CAGE 4U884)",
        )
        request = self.request("East Hartford", options)
        self.assertIs(lab._request_with_resolved_company_clarification(request), request)

    def test_new_question_is_not_a_clarification_selection(self):
        request = self.request("Tell me about Boeing")
        self.assertIs(lab._request_with_resolved_company_clarification(request), request)

    def test_parent_name_does_not_select_one_site(self):
        request = self.request("RTX")
        self.assertIs(lab._request_with_resolved_company_clarification(request), request)


if __name__ == "__main__":
    unittest.main()
