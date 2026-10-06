from __future__ import annotations

import unittest
from unittest.mock import patch
import json
from pathlib import Path
import sys
import tempfile

from ask_mimir_beta import evaluate_job_speed
from ask_mimir_beta.evaluate_job_speed import (
    evaluate_job_case, first_visible_text, prepare_case, timing_summary,
)


class JobSpeedEvaluatorTests(unittest.TestCase):
    def test_sample_spans_common_workflows(self):
        cases = json.loads((Path(__file__).resolve().parents[1] / "speed_eval_cases.json").read_text())
        self.assertEqual(
            {case["expected_workflow"] for case in cases},
            {"platform_intelligence", "company_site_intelligence", "item_intelligence",
             "contract_or_opportunity", "market_record_search", "program_momentum"},
        )
        self.assertEqual({case["follows"] for case in cases if case.get("follows")},
                         {"platform_amraam", "company_curtiss_wright"})

    def test_followup_uses_actual_prior_answer_and_scope_without_saving_it(self):
        context = {"first": {
            "messages": [{"role": "user", "content": "Which firms?"}],
            "answer": "A real prior answer", "conversation_id": "conversation-1",
            "active_scope": {"scope_type": "platform", "scope_id": "AMRAAM"},
        }}
        prepared = prepare_case({"case_id": "second", "follows": "first",
                                 "question": "What about FY2026?"}, context)
        self.assertEqual(prepared["conversation_id"], "conversation-1")
        self.assertEqual(prepared["active_scope"]["scope_id"], "AMRAAM")
        self.assertEqual(prepared["messages"][-2],
                         {"role": "assistant", "content": "A real prior answer"})
        self.assertIsNone(prepare_case({"case_id": "orphan", "follows": "missing"}, context))

    def test_timing_summary_reports_only_completed_numeric_samples(self):
        summary = timing_summary([
            {"status": "completed", "accepted_ms": 100, "first_visible_text_ms": 900},
            {"status": "completed", "accepted_ms": 200, "first_visible_text_ms": 1100},
            {"status": "failed", "accepted_ms": 5000, "first_visible_text_ms": 9000},
        ])
        self.assertEqual(summary["first_visible_text_ms"],
                         {"n": 2, "p50": 1000, "p90": 1100})
        self.assertNotIn("completed_ms", summary)

    @patch("ask_mimir_beta.evaluate_job_speed.time.sleep")
    @patch("ask_mimir_beta.evaluate_job_speed.request_json")
    def test_report_uses_real_followup_context_but_omits_answer_text(self, request_json, _sleep):
        posts = []
        def response(_url, *, method, payload, headers):
            if method == "POST":
                posts.append((payload, headers))
                return {"request_id": payload["client_request_id"], "status": "queued",
                        "workflow": "platform_intelligence"}
            answer = "Sensitive first answer" if len(posts) == 1 else "Follow-up answer"
            return {"status": "completed", "workflow": "platform_intelligence",
                    "result": {"answer": answer, "active_scope": {
                        "scope_type": "platform", "scope_id": "AMRAAM"}}}
        request_json.side_effect = response
        with tempfile.TemporaryDirectory() as directory:
            cases = Path(directory) / "cases.json"
            output = Path(directory) / "report.json"
            cases.write_text(json.dumps([
                {"case_id": "first", "question": "Who supplies AMRAAM?",
                 "expected_workflow": "platform_intelligence"},
                {"case_id": "second", "follows": "first", "question": "And in 2026?",
                 "expected_workflow": "platform_intelligence"},
            ]))
            argv = ["evaluate_job_speed", "--cases-file", str(cases),
                    "--case-id", "second", "--shared-subject", "--subject", "private-test",
                    "--output", str(output)]
            with patch.object(sys, "argv", argv):
                evaluate_job_speed.main()
            report = output.read_text()
        self.assertEqual(len(posts), 2)
        self.assertEqual(posts[0][1]["X-Ask-Mimir-Subject"], "private-test")
        self.assertEqual(posts[1][1]["X-Ask-Mimir-Subject"], "private-test")
        self.assertEqual(posts[0][0]["conversation_id"], posts[1][0]["conversation_id"])
        self.assertEqual(posts[1][0]["messages"][-2]["content"], "Sensitive first answer")
        self.assertNotIn("Sensitive first answer", report)
        self.assertNotIn("Follow-up answer", report)
        self.assertEqual(json.loads(report)["timing_summary_ms"]["completed_ms"]["n"], 2)

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
