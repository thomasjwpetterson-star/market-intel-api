from __future__ import annotations

import ast
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def literal_assignment(path: Path, name: str):
    tree = ast.parse(path.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} was not found in {path.name}")


class EvaluationSuiteContractTests(unittest.TestCase):
    def test_routing_matrix_covers_required_robustness_dimensions(self):
        cases = literal_assignment(
            ROOT / "evaluate_workflow_routing.py", "ROUTING_ROBUSTNESS_CASES"
        )
        self.assertEqual(
            set(cases),
            {
                "misspellings",
                "ambiguous_company_names",
                "multiple_entities",
                "current_events",
                "unsupported_and_non_defense",
            },
        )
        self.assertTrue(all(len(rows) >= 4 for rows in cases.values()))

    def test_offline_evidence_cases_cover_customer_research_workflows(self):
        cases = json.loads((ROOT / "offline_regression_cases.json").read_text())
        workflows = {case["expected_workflow"] for case in cases}
        self.assertTrue(
            {
                "platform_intelligence",
                "company_site_intelligence",
                "item_intelligence",
                "contract_or_opportunity",
                "market_record_search",
                "state_industrial_base",
                "capability_discovery",
                "market_segment_intelligence",
                "platform_comparison",
            }.issubset(workflows)
        )

    def test_live_sample_stays_small_and_representative(self):
        cases = json.loads(
            (ROOT / "representative_live_eval_cases.json").read_text()
        )
        self.assertGreaterEqual(len(cases), 5)
        self.assertLessEqual(len(cases), 8)
        required_tools = {
            tool for case in cases for tool in case.get("required_tools", [])
        }
        self.assertTrue(
            {
                "get_platform_context",
                "search_company_contexts",
                "get_item_context",
                "get_capability_market",
                "get_market_record_search",
            }.issubset(required_tools)
        )

    def test_suite_default_files_exist(self):
        for name in (
            "expanded_workflow_question_journeys.json",
            "offline_regression_cases.json",
            "representative_live_eval_cases.json",
        ):
            self.assertTrue((ROOT / name).is_file(), name)


if __name__ == "__main__":
    unittest.main()
