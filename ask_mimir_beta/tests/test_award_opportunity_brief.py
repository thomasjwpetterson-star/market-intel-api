import unittest
from unittest.mock import Mock

from award_opportunity_context import AwardOpportunityContextStore
from lab_test_support import load_lab


lab = load_lab()


class AwardOpportunityBriefTests(unittest.TestCase):
    def test_contract_brief_retains_financial_history_and_marks_supplier_lanes_omitted(self):
        store = AwardOpportunityContextStore.__new__(AwardOpportunityContextStore)
        store._cache = {}
        store._contract_row = Mock(return_value={
            "contract_id": "TEST123", "award_key": "award-1", "total_spend": 125.0,
            "obligations_fy2025": 125.0, "action_count_fy2025": 1,
            "vendor_name": "Example", "base_award_description": "Radar support",
        })
        store._contract_actions = Mock(return_value=[{"contract_id": "TEST123", "prime_obligation_usd": 125.0}])
        store._related_opportunities = Mock(return_value=[])
        store._contract_sources = Mock(return_value=[{"source": "USA_SPENDING"}])
        store._contract_subawards = Mock(side_effect=AssertionError("subawards should not be queried"))
        store._comparable_suppliers = Mock(side_effect=AssertionError("comparables should not be queried"))

        brief = store.answer_projection("contract", "TEST123", depth="brief")

        self.assertEqual(brief["identity"]["base_award_description"], "Radar support")
        self.assertEqual(brief["financial_summary"]["annual_obligations"][0]["net_prime_obligations_usd"], 125.0)
        self.assertEqual(len(brief["action_history"]), 1)
        self.assertEqual(brief["evidence_scope"]["depth"], "brief")
        self.assertIn("reported_subaward_suppliers", brief["evidence_scope"]["omitted_lanes"])
        self.assertEqual(brief, store.answer_projection("contract", "TEST123", depth="brief"))
        store._contract_subawards.assert_not_called()
        store._comparable_suppliers.assert_not_called()

    def test_supplier_questions_require_full_contract_evidence(self):
        self.assertFalse(lab.contract_supplier_detail_requested("Tell me about N0001923F2616"))
        for question in ("Who are its suppliers?", "Any reported sub-awards?",
                         "Show the supply chain", "Which competitors might bid?",
                         "Which companies make its components?"):
            self.assertTrue(lab.contract_supplier_detail_requested(question), question)

    def test_specific_company_details_require_full_dossier(self):
        self.assertFalse(lab.company_detail_requested("How is Curtiss-Wright positioned in U.S. defense?"))
        self.assertFalse(lab.company_detail_requested("Which recent awards support that position?"))
        for question in ("What does CAGE 04808 make?", "List every site",
                         "Show the supply chain", "Which subcontractors appear?",
                         "Which FYDP budget lines matter?"):
            self.assertTrue(lab.company_detail_requested(question), question)


if __name__ == "__main__":
    unittest.main()
