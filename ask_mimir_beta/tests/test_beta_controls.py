import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from beta_controls import (
    AccessContext,
    BetaStateStore,
    DataReleaseGuard,
    DailyQuotaExceeded,
    DuplicateRequestError,
    RequestPerformance,
    TIER_POLICIES,
    record_request_timing,
    request_performance_scope,
    response_requires_clarification,
    sanitize_customer_payload,
    validate_answer_citations,
)


class BetaStateStoreTests(unittest.TestCase):
    def test_duplicate_request_id_cannot_consume_a_second_allowance(self):
        with tempfile.TemporaryDirectory() as directory:
            store = BetaStateStore(Path(directory) / "beta-state.sqlite3")
            access = AccessContext("test-user", "professional", True)
            store.reserve("logical-request", access, "release-1", "platform", "hash-1")
            with self.assertRaises(DuplicateRequestError):
                store.reserve("logical-request", access, "release-1", "platform", "hash-1")
            self.assertEqual(store.used_today(access.subject_id), 1)
            store.connection.close()

    def test_interrupted_request_can_resume_with_same_id_without_double_counting(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "beta-state.sqlite3"
            access = AccessContext("test-user", "professional", True)
            first = BetaStateStore(path)
            first.reserve("logical-request", access, "release-1", "platform", "hash-1")
            first.mark_running("logical-request")
            first.connection.close()

            restarted = BetaStateStore(path)
            self.assertEqual(restarted.used_today(access.subject_id), 0)
            used = restarted.reserve(
                "logical-request", access, "release-1", "platform", "hash-1"
            )
            self.assertEqual(used, 1)
            self.assertEqual(restarted.used_today(access.subject_id), 1)
            restarted.connection.close()

    def test_completion_persists_internal_performance_breakdown(self):
        with tempfile.TemporaryDirectory() as directory:
            store = BetaStateStore(Path(directory) / "beta-state.sqlite3")
            access = AccessContext("test-user", "professional", True)
            performance = {"model_ms": 1200.5, "evidence_retrieval_ms": 48.2}
            store.reserve("request-1", access, "release-1", "company")
            store.complete(
                "request-1",
                latency_ms=1300,
                estimated_cost_usd=0.1,
                performance=performance,
            )

            stored = store.connection.execute(
                "SELECT performance_json FROM query_events WHERE request_id = ?",
                ["request-1"],
            ).fetchone()[0]
            self.assertEqual(json.loads(stored), performance)
            store.connection.close()

    def test_conversation_scope_is_server_side_and_subject_isolated(self):
        with tempfile.TemporaryDirectory() as directory:
            store = BetaStateStore(Path(directory) / "beta-state.sqlite3")
            scope = {
                "scope_type": "item",
                "scope_id": "004050631",
                "scope_name": "1280-00-405-0631",
            }
            store.save_conversation_scope(
                "conversation-123", "guest-a", scope, "item_intelligence"
            )

            self.assertEqual(
                store.load_conversation_scope("conversation-123", "guest-a"), scope
            )
            self.assertIsNone(
                store.load_conversation_scope("conversation-123", "guest-b")
            )
            store.connection.close()

    def test_routing_decision_records_clarification_and_correction(self):
        with tempfile.TemporaryDirectory() as directory:
            store = BetaStateStore(Path(directory) / "beta-state.sqlite3")
            store.record_routing_decision(
                request_id="request-1",
                conversation_id="conversation-123",
                subject_id="guest-a",
                question="Which one?",
                decision={
                    "intended_workflow": "platform_intelligence",
                    "workflow": "platform_intelligence",
                    "candidates": [{"workflow": "platform_intelligence"}],
                    "confidence": 0.7,
                    "current_scope": {"scope_type": "platform", "scope_id": "F-16"},
                    "resolved_entities": [],
                    "subject_changed": False,
                    "clarification_needed": True,
                },
            )
            store.complete_routing_event(
                "request-1", clarification_outcome="clarification_requested"
            )
            store.mark_routing_correction("request-1")

            row = store.connection.execute(
                """
                SELECT selected_workflow, clarification_outcome, user_correction
                FROM routing_events WHERE request_id = ?
                """,
                ["request-1"],
            ).fetchone()
            self.assertEqual(
                row,
                ("platform_intelligence", "clarification_requested", 1),
            )
            store.connection.close()

    def test_unbilled_clarification_restores_public_query_allowance(self):
        with tempfile.TemporaryDirectory() as directory:
            store = BetaStateStore(Path(directory) / "beta-state.sqlite3")
            access = AccessContext("guest", "public", False)
            store.reserve("clarification", access, "release-1", "capability")
            store.complete(
                "clarification",
                latency_ms=1,
                estimated_cost_usd=0.01,
                billable=False,
            )

            self.assertEqual(store.used_today(access.subject_id), 0)
            store.reserve("answer", access, "release-1", "capability")
            self.assertEqual(store.used_today(access.subject_id), 1)
            store.connection.close()

    def test_restart_refunds_an_interrupted_query(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "beta-state.sqlite3"
            access = AccessContext(
                subject_id="test-user",
                tier="free",
                authenticated=True,
            )

            first_process = BetaStateStore(path)
            first_process.reserve("request-1", access, "release-1", "company")
            first_process.mark_running("request-1")
            self.assertEqual(first_process.used_today(access.subject_id), 1)
            first_process.connection.close()

            restarted_process = BetaStateStore(path)
            self.assertEqual(restarted_process.used_today(access.subject_id), 0)
            status = restarted_process.connection.execute(
                "SELECT status FROM query_events WHERE request_id = ?",
                ["request-1"],
            ).fetchone()[0]
            self.assertEqual(status, "failed_refunded")
            restarted_process.connection.close()

    def test_free_tier_has_two_queries_per_day(self):
        with tempfile.TemporaryDirectory() as directory:
            store = BetaStateStore(Path(directory) / "beta-state.sqlite3")
            access = AccessContext("free-user", "free", True)
            store.reserve("request-1", access, "release-1", "company")
            store.reserve("request-2", access, "release-1", "platform")
            with self.assertRaises(DailyQuotaExceeded) as raised:
                store.reserve("request-3", access, "release-1", "company")
            self.assertEqual(raised.exception.period, "day")
            self.assertEqual(store.used_today(access.subject_id), 2)
            store.connection.close()

    def test_monthly_allowance_is_enforced_across_days(self):
        with tempfile.TemporaryDirectory() as directory:
            store = BetaStateStore(Path(directory) / "beta-state.sqlite3")
            access = AccessContext("guest", "public", False)
            for index in range(TIER_POLICIES["public"].queries_per_utc_month):
                day = f"2026-09-{index + 1:02d}"
                with patch("beta_controls.utc_day", return_value=day):
                    store.reserve(
                        f"request-{index}",
                        access,
                        "release-1",
                        "company",
                    )
                    store.complete(
                        f"request-{index}",
                        latency_ms=1,
                        estimated_cost_usd=0.01,
                    )
            with patch("beta_controls.utc_day", return_value="2026-09-30"):
                with self.assertRaises(DailyQuotaExceeded) as raised:
                    store.reserve("request-over-month", access, "release-1", "company")
            self.assertEqual(raised.exception.period, "month")
            store.connection.close()


class DataReleaseGuardTests(unittest.TestCase):
    def test_guard_tracks_files_but_never_registers_a_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.parquet"
            source.write_bytes(b"initial")
            guard = DataReleaseGuard("release-1", [root, source])

            self.assertEqual(guard.paths, [source.resolve()])
            (root / "runtime-cache.json").write_text("cache")
            guard.assert_unchanged()

            source.write_bytes(b"changed")
            with self.assertRaises(RuntimeError):
                guard.assert_unchanged()


class RequestPerformanceTests(unittest.TestCase):
    def test_records_model_evidence_and_cache_timings_within_request_scope(self):
        performance = RequestPerformance(routing_ms=12.25)
        with request_performance_scope(performance):
            record_request_timing("evidence_retrieval", "get_company_context", 40)
            record_request_timing(
                "evidence_cache_hit",
                "get_company_context",
                0,
                cache_hit=True,
            )
            record_request_timing("model", "gpt-test", 800)

        snapshot = performance.snapshot(
            queue_wait_ms=3.5,
            answer_generation_ms=850,
            validation_ms=15,
            total_request_ms=900,
        )
        self.assertEqual(snapshot["routing_ms"], 12.2)
        self.assertEqual(snapshot["queue_wait_ms"], 3.5)
        self.assertEqual(snapshot["evidence_retrieval_ms"], 40.0)
        self.assertEqual(snapshot["model_ms"], 800.0)
        self.assertEqual(snapshot["evidence_call_count"], 1)
        self.assertEqual(snapshot["model_call_count"], 1)
        self.assertEqual(snapshot["evidence_cache_hit_count"], 1)


class ClarificationDetectionTests(unittest.TestCase):
    def test_model_generated_scope_question_is_a_clarification(self):
        result = {
            "answer": (
                "Do you mean the US defense market for complete aircraft-engine fuel-control "
                "units, or the broader fuel-control ecosystem?"
            )
        }
        self.assertTrue(response_requires_clarification(result))

    def test_completed_answer_with_follow_up_question_is_not_a_clarification(self):
        result = {
            "answer": (
                "The market is led by sustainment demand across military aircraft fleets. "
                "Would you like the supporting supplier records?"
            )
        }
        self.assertFalse(response_requires_clarification(result))

    def test_context_preface_followed_by_scope_question_is_a_clarification(self):
        result = {
            "answer": (
                "Ask Mimir focuses on the U.S. defense industrial base. "
                "Are you looking for electrician jobs with defense contractors, "
                "military installations, or shipyards? If so, provide your city/state."
            )
        }
        self.assertTrue(response_requires_clarification(result))

    def test_category_menu_question_is_a_clarification(self):
        result = {
            "answer": (
                "What would you like to analyze in the U.S. defense semiconductor "
                "market? Key suppliers, awards, programs, or DLA demand?"
            )
        }
        self.assertTrue(response_requires_clarification(result))


class CustomerProvenanceTests(unittest.TestCase):
    def test_internal_fetch_fields_are_hidden_but_public_source_is_retained(self):
        payload = sanitize_customer_payload(
            {
                "source_type": "DOD_CONTRACT_ANNOUNCEMENT",
                "source_name": "Official U.S. Department of Defense contract announcement",
                "source_url": "https://www.defense.gov/example",
                "source_fetch_url": "https://r.jina.ai/http://www.defense.gov/example",
                "source_fetch_method": "text_renderer",
            }
        )
        self.assertNotIn("source_type", payload)
        self.assertNotIn("source_fetch_url", payload)
        self.assertNotIn("source_fetch_method", payload)
        self.assertEqual(payload["source_url"], "https://www.defense.gov/example")

    def test_official_source_url_is_recognized_as_supplied_evidence(self):
        answer = "See the [official announcement](https://www.defense.gov/example)."
        validation = validate_answer_citations(
            answer,
            [{"result": {"source_url": "https://www.defense.gov/example"}}],
        )
        self.assertEqual(validation["external_source_link_count"], 1)
        self.assertEqual(validation["external_links_in_deterministic_pack"], 1)


if __name__ == "__main__":
    unittest.main()
