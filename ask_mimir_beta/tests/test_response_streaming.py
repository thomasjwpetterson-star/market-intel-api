"""The customer sees draft text only from an evidence-ready model turn."""

import json
import unittest
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch

import httpx
from openai import OpenAI

from lab_test_support import load_lab


lab = load_lab()


def event(kind, **values):
    return SimpleNamespace(type=kind, **values)


class ResponseStreamingTests(unittest.TestCase):
    def setUp(self):
        self.drafts = []
        self.responses = Mock()
        self.timed = lab.TimedResponses(self.responses, self.drafts.append)
        self.patches = [
            patch.object(lab, "lifecycle"),
            patch.object(lab, "record_request_timing"),
            patch.object(lab, "remaining_job_seconds", return_value=None),
        ]
        for item in self.patches:
            item.start()

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()

    def test_evidence_ready_answer_streams_and_returns_full_response(self):
        final = SimpleNamespace(id="resp-1", output=[SimpleNamespace(type="message")])
        self.responses.create.return_value = iter([
            event("response.output_text.delta", delta="The source "),
            event("response.output_text.delta", delta="shows FY2026."),
            event("response.completed", response=final),
        ])
        self.assertIs(self.timed.create(model="test", input="evidence"), final)
        self.assertEqual(self.drafts[-1], "The source shows FY2026.")
        self.assertTrue(self.responses.create.call_args.kwargs["stream"])

    def test_installed_sdk_decodes_delta_and_completed_response(self):
        final = {
            "id": "resp_mock", "object": "response", "created_at": 1720000000,
            "status": "completed", "model": "gpt-test",
            "output": [{
                "id": "msg_mock", "type": "message", "status": "completed",
                "role": "assistant", "content": [{
                    "type": "output_text", "text": "Answer from source.", "annotations": [],
                }],
            }],
            "usage": {"input_tokens": 5, "output_tokens": 5, "total_tokens": 10},
        }
        events = [
            ("response.output_text.delta", {
                "type": "response.output_text.delta", "delta": "Answer from source.",
                "item_id": "msg_mock", "output_index": 0, "content_index": 0,
                "sequence_number": 1,
            }),
            ("response.completed", {
                "type": "response.completed", "response": final,
                "sequence_number": 2,
            }),
        ]
        body = "".join(
            f"event: {name}\ndata: {json.dumps(payload)}\n\n"
            for name, payload in events
        )
        client = OpenAI(
            api_key="test",
            http_client=httpx.Client(transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, headers={"content-type": "text/event-stream"}, content=body
                )
            )),
        )
        try:
            timed = lab.TimedResponses(client.responses, self.drafts.append)
            response = timed.create(model="gpt-test", input="Evidence")
            self.assertEqual(response.output_text, "Answer from source.")
            self.assertEqual(response.usage.total_tokens, 10)
            self.assertEqual(self.drafts[-1], "Answer from source.")
        finally:
            client.close()

    def test_tool_planning_is_hidden_then_retracted_if_more_tools_are_needed(self):
        planned = SimpleNamespace(id="resp-plan", output=[SimpleNamespace(type="function_call")])
        self.responses.create.return_value = planned
        tools = [{"type": "function", "name": "get_metric_evidence"}]
        self.assertIs(self.timed.create(model="test", tools=tools), planned)
        self.assertNotIn("stream", self.responses.create.call_args.kwargs)
        self.assertEqual(self.drafts, [])

        self.timed.allow_function_streaming = True
        self.responses.create.return_value = iter([
            event("response.output_text.delta", delta="Early draft"),
            event("response.completed", response=planned),
        ])
        self.timed.create(model="test", tools=tools)
        self.assertEqual(self.drafts[-1], None)

    def test_failed_stream_clears_draft_and_raises(self):
        self.responses.create.return_value = iter([
            event("response.output_text.delta", delta="Partial"),
            event("response.failed", response=SimpleNamespace(error="provider failure")),
        ])
        with self.assertRaisesRegex(RuntimeError, "model stream failed"):
            self.timed.create(model="test")
        self.assertEqual(self.drafts[-1], None)

    def test_interrupted_stream_clears_visible_draft(self):
        def interrupted():
            yield event("response.output_text.delta", delta="Partial")
            raise ConnectionError("stream interrupted")

        self.responses.create.return_value = interrupted()
        with self.assertRaisesRegex(ConnectionError, "stream interrupted"):
            self.timed.create(model="test")
        self.assertEqual(self.drafts[-1], None)

    def test_job_poll_exposes_only_current_in_memory_draft(self):
        manager = lab.AskJobManager()
        request_id = "draft-request"
        manager.jobs[request_id] = {"status": "running", "subject_id": "alice"}
        runtime = SimpleNamespace(store=SimpleNamespace(manifest={"release_id": "pinned-release"}))
        try:
            with patch.object(lab, "runtime", runtime, create=True):
                manager.set_provisional_answer(request_id, "Working from evidence", time.perf_counter() - 1)
                public = manager.public_job(manager.jobs[request_id])
                self.assertEqual(public["provisional_answer"]["text"], "Working from evidence")
                self.assertNotIn("source_release_id", public["provisional_answer"])
                self.assertNotIn("_first_model_text_ms", public)
                manager.set_provisional_answer(request_id, "See file:///private/internal", time.perf_counter() - 1)
                self.assertNotIn("provisional_answer", manager.public_job(manager.jobs[request_id]))
                manager.set_provisional_answer(request_id, "source_report_id: private", time.perf_counter() - 1)
                self.assertNotIn("provisional_answer", manager.public_job(manager.jobs[request_id]))
                manager.set_provisional_answer(request_id, None, time.perf_counter() - 1)
                self.assertNotIn("provisional_answer", manager.jobs[request_id])
                manager.jobs[request_id]["status"] = "completed"
                manager.set_provisional_answer(request_id, "Late text", time.perf_counter() - 1)
                self.assertNotIn("provisional_answer", manager.jobs[request_id])
        finally:
            manager.executor.shutdown(wait=False, cancel_futures=True)

    def test_general_research_can_request_independent_evidence_in_one_model_turn(self):
        first = SimpleNamespace(
            id="planning", status="completed", usage=None,
            output=[
                SimpleNamespace(type="function_call", name="get_program_outlook",
                                arguments=json.dumps({"program_id": "A"}), call_id="call-a"),
                SimpleNamespace(type="function_call", name="get_platform_context",
                                arguments=json.dumps({"platform_id": "B"}), call_id="call-b"),
            ],
        )
        final = SimpleNamespace(
            id="answered", status="completed", usage=None,
            output=[SimpleNamespace(type="message")],
            output_text="The checked answer uses both evidence packs.",
        )
        provider = Mock()
        provider.responses.create.side_effect = [first, final]
        runtime = SimpleNamespace(
            mock_mode=False, external_evidence_allowed=True,
            platform_contexts=Mock(mentions=Mock(return_value=[])),
            model="test-model", reasoning_effort="high", max_output_tokens=4000,
            max_evidence_records=20,
            store=SimpleNamespace(manifest={"release_id": "test-release"}),
            call_tool=Mock(side_effect=lambda name, arguments: {"source": name}),
            write_audit_record=Mock(),
        )
        request = lab.AskRequest(messages=[{"role": "user", "content": "Compare these two research areas."}])
        routing = lab.RoutingDecision(workflow="general_research", reason="test", confidence=1)
        with patch.object(lab, "runtime", runtime, create=True), \
             patch.object(lab, "OpenAI", return_value=provider), \
             patch.dict("os.environ", {"OPENAI_API_KEY": "test-placeholder"}):
            result = lab.generate_answer(request, routing=routing)
        self.assertEqual(result["answer"], final.output_text)
        self.assertEqual(result["response_calls"], 2)
        self.assertEqual(runtime.call_tool.call_count, 2)
        self.assertTrue(provider.responses.create.call_args_list[0].kwargs["parallel_tool_calls"])
        second_input = provider.responses.create.call_args_list[1].kwargs["input"]
        outputs = [item for item in second_input if isinstance(item, dict)
                   and item.get("type") == "function_call_output"]
        self.assertEqual({item["call_id"] for item in outputs}, {"call-a", "call-b"})


if __name__ == "__main__":
    unittest.main()
