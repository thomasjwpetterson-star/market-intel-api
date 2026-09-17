import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from beta_controls import AccessContext, BetaStateStore
from lab_test_support import load_lab

lab = load_lab()


class LifecycleMonitorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = BetaStateStore(Path(self.tmp.name) / "state.sqlite3")
        self.now = datetime.now(timezone.utc).timestamp()
        self.access = AccessContext("alice", "professional", True)

    def tearDown(self):
        self.store.connection.close()
        self.tmp.cleanup()

    def job(self, request_id, status="queued", age=6000, **fields):
        job = {"request_id": request_id, "subject_id": "alice", "status": status,
               "created_at": datetime.fromtimestamp(self.now - age, timezone.utc).isoformat(),
               "_request_body": {"messages": []}, "result": {"response_id": "answer-1"}, **fields}
        if status == "completed":
            job["completed_at"] = job["created_at"]
        self.store.save_job(job)
        return job

    def inspect(self, active=()):
        return self.store.reconcile_job_lifecycle(set(active), orphan_after_seconds=4500, now=self.now)

    def test_abandoned_jobs_expire_and_refund_atomically_once(self):
        self.job("old")
        self.store.reserve("old", self.access, "test", "platform_intelligence")
        self.assertEqual(self.store.used_today("alice"), 1)
        first = self.inspect()
        self.assertEqual(len(first["expired"]), 1)
        result = self.store.load_job("old")
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["error_code"], "interrupted_job_expired")
        self.assertEqual(self.store.used_today("alice"), 0)
        self.assertEqual(self.inspect()["expired"], [])
        self.assertEqual(self.store.load_job("old")["completed_at"], result["completed_at"])

    def test_expiry_write_failure_rolls_back_refund_and_result_together(self):
        self.job("old")
        self.store.reserve("old", self.access, "test", "platform_intelligence")
        self.store.connection.execute("CREATE TRIGGER reject_result BEFORE UPDATE ON research_results BEGIN SELECT RAISE(ABORT, 'disk failure'); END")
        with self.assertRaises(Exception):
            self.inspect()
        self.assertEqual(self.store.used_today("alice"), 1)
        self.assertEqual(self.store.load_job("old")["status"], "queued")

    def test_active_overdue_jobs_are_flagged_without_refund_or_overwrite(self):
        self.job("live", status="running")
        self.store.reserve("live", self.access, "test", "platform_intelligence")
        report = self.inspect(["live"])
        self.assertEqual(report["counts"]["overdue_active"], 1)
        self.assertEqual(report["expired"], [])
        self.assertEqual(self.store.load_job("live")["status"], "running")
        self.assertEqual(self.store.used_today("alice"), 1)

    def test_recent_interrupted_job_keeps_recovery_window(self):
        self.job("recent", age=4000)
        self.assertEqual(self.inspect()["expired"], [])
        self.assertEqual(self.store.load_job("recent")["status"], "queued")

    def test_completed_answer_missing_receipt_is_not_failed_or_refunded(self):
        job = self.job("done", "completed")
        self.store.reserve("done", self.access, "test", "platform_intelligence")
        self.store.complete("done", latency_ms=1, estimated_cost_usd=None, job=job)
        report = self.inspect()
        self.assertEqual(report["counts"]["delivery_unconfirmed"], 1)
        self.assertEqual(report["counts"]["server_failed"], 0)
        self.assertEqual(self.store.used_today("alice"), 1)
        self.store.record_delivery_receipt("done", "alice", "answer-1", "received")
        self.assertTrue(self.inspect()["issues"][0]["browser_received"])
        self.store.record_delivery_receipt("done", "alice", "answer-1", "rendered")
        self.assertEqual(self.inspect()["counts"]["delivery_unconfirmed"], 0)
        self.assertEqual(self.inspect()["counts"]["browser_rendered"], 1)

    def test_quota_and_legacy_results_do_not_count_as_technical_or_delivery_failures(self):
        self.job("quota", "failed", failure_stage="quota")
        self.job("legacy", "completed", _request_body=None)
        counts = self.inspect()["counts"]
        self.assertEqual(counts["quota_rejected"], 1)
        self.assertEqual(counts["server_failed"], 0)
        self.assertEqual(counts["delivery_unconfirmed"], 0)

    def test_completed_ledger_is_never_overwritten_by_expiry(self):
        self.job("inconsistent")
        self.store.reserve("inconsistent", self.access, "test", "platform_intelligence")
        self.store.complete("inconsistent", latency_ms=1, estimated_cost_usd=None)
        report = self.inspect()
        self.assertEqual(report["counts"]["invalid_saved_jobs"], 1)
        self.assertEqual(report["expired"], [])
        self.assertEqual(self.store.used_today("alice"), 1)

    def test_corrupt_saved_job_does_not_hide_other_requests(self):
        self.job("broken")
        self.store.connection.execute("UPDATE research_results SET job_json='invalid' WHERE request_id='broken'")
        self.store.connection.commit()
        self.job("done", "completed")
        report = self.inspect()
        self.assertEqual(report["counts"]["invalid_saved_jobs"], 1)
        self.assertEqual(report["counts"]["delivery_unconfirmed"], 1)

    def test_monitor_logs_transitions_and_health_contains_no_request_content(self):
        self.job("done", "completed", _question="PRIVATE QUESTION")
        manager = lab.AskJobManager()
        manager.executor.shutdown()
        try:
            with patch.object(lab, "runtime", SimpleNamespace(beta_state=self.store), create=True), patch.object(lab, "lifecycle") as log:
                manager.check_lifecycle()
                manager.check_lifecycle()
                self.assertEqual(log.call_count, 1)
                self.assertEqual(log.call_args.args[0], "ask_delivery_unconfirmed")
                self.assertNotIn("PRIVATE QUESTION", str(log.call_args))
                self.assertNotIn("done", json.dumps(manager.snapshot()))
                self.store.record_delivery_receipt("done", "alice", "answer-1", "rendered")
                manager.check_lifecycle()
                self.assertEqual(log.call_args.args[0], "ask_lifecycle_issue_cleared")
        finally:
            manager.stop_monitor()

    def test_expired_job_cannot_restart_on_later_owner_poll(self):
        self.job("old")
        self.inspect()
        manager = lab.AskJobManager()
        manager.executor.shutdown()
        manager.executor = Mock()
        with patch.object(lab, "runtime", SimpleNamespace(beta_state=self.store), create=True):
            self.assertEqual(manager.get("old", self.access)["status"], "failed")
        manager.executor.submit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
