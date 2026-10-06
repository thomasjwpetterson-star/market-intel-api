from __future__ import annotations

import unittest
from unittest.mock import patch
import json
from pathlib import Path

from ask_mimir_beta.evaluate_job_speed import evaluate_job_case, first_visible_text


class JobSpeedEvaluatorTests(unittest.TestCase):
    def test_sample_spans_common_workflows(self):
        cases = json.loads((Path(__file__).resolve().parents[1] / "speed_eval_cases.json").read_text())
        self.assertEqual(
            {case["expected_workflow"] for case in cases},
            {"platform_intelligence", "company_site_intelligence", "item_intelligence",
             "contract_or_opportunity", "market_record_search", "program_momentum"},
        )

    def test_first_text_distinguishes_preview_from_final(self):
        self.assertIsNone(first_visible_text({"status": "running"}))
        self.assertEqual(first_visible_text({"status": "running", "provisional_answer": {"text": "Writing."}}), "provisional_answer")
        self.assertEqual(first_visible_text({"status": "running", "evidence_preview": {"text": "Grounded."}}), "evidence_preview")
        self.assertEqual(first_visible_text({"status": "completed", "result": {"answer": "Final."}}), "completed_answer")

    @patch("ask_mimir_beta.evaluate_job_speed.time.sleep")
    @patch("ask_mimir_beta.evaluate_job_speed.request_json")
    def test_job_flow_records_first_text_and_omits_answer(self, request_json, _sleep):
        def response(_url, *, method, payload, headers):
            if method == "POST":
                return {"request_id": payload["client_request_id"], "status": "queued", "workflow": "contract_or_opportunity"}
            return {
                "status": "completed", "workflow": "contract_or_opportunity",
                "result": {"answer": "Contract N0001923F2616 has an action history.", "tool_trace": []},
            }
        request_json.side_effect = response
        result = evaluate_job_case(
            "http://127.0.0.1:10100",
            {"case_id": "contract", "question": "Tell me about N0001923F2616", "expected_workflow": "contract_or_opportunity"},
            tier="enterprise", subject="speed-test", poll_seconds=0,
        )
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["first_text_mode"], "completed_answer")
        self.assertTrue(result["workflow_correct"])
        self.assertTrue(result["basic_answer_checks_passed"])
        self.assertNotIn("answer", result)

    @patch("ask_mimir_beta.evaluate_job_speed.time.sleep")
    @patch("ask_mimir_beta.evaluate_job_speed.request_json")
    def test_job_flow_separates_preview_and_model_draft(self, request_json, _sleep):
        polls = iter([
            {"status": "running", "workflow": "contract_or_opportunity",
             "evidence_preview": {"text": "Official contract actions found."}},
            {"status": "running", "workflow": "contract_or_opportunity",
             "provisional_answer": {"text": "Writing from contract evidence."}},
            {"status": "completed", "workflow": "contract_or_opportunity",
             "timings": {"first_model_text_ms": 1400},
             "result": {"answer": "Contract N0001923F2616 has an action history.", "tool_trace": []}},
        ])

        def response(_url, *, method, payload, headers):
            if method == "POST":
                return {"request_id": payload["client_request_id"], "status": "queued",
                        "workflow": "contract_or_opportunity"}
            return next(polls)

        request_json.side_effect = response
        result = evaluate_job_case(
            "http://127.0.0.1:10100",
            {"case_id": "contract", "question": "Tell me about N0001923F2616",
             "expected_workflow": "contract_or_opportunity"},
            tier="enterprise", subject="speed-test", poll_seconds=0,
        )
        self.assertEqual(result["first_text_mode"], "evidence_preview")
        self.assertIsNotNone(result["first_evidence_preview_ms"])
        self.assertIsNotNone(result["first_model_draft_ms"])
        self.assertEqual(result["server_first_model_text_ms"], 1400)


if __name__ == "__main__":
    unittest.main()
