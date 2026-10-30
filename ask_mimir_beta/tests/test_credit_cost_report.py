import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from credit_cost_report import report_from_ledger, summarize_rows


class CreditCostReportTests(unittest.TestCase):
    def test_summary_keeps_classes_separate_and_flags_small_samples(self):
        rows = [
            ("conversation", "conversation_focused", 1, 0.02, 9000, json.dumps({"model_web_search_call_count": 0})),
            ("conversation", "conversation_focused", 1, 0.04, 15000, json.dumps({"model_web_search_call_count": 1})),
            ("deep_research", "deep_standard", 3, 0.60, 80000, "invalid"),
            ("deep_research", "deep_standard", 3, None, 90000, None),
        ]
        report = summarize_rows(rows, min_samples=3)
        self.assertEqual(report["rows_without_valid_cost"], 1)
        self.assertEqual(report["classes"][0]["work_class"], "conversation_focused")
        self.assertEqual(report["classes"][0]["estimated_cost_usd"],
                         {"p50": 0.03, "p90": 0.04, "max": 0.04})
        self.assertFalse(report["classes"][0]["enough_samples_for_review"])
        self.assertEqual(report["classes"][1]["estimated_cost_per_proposed_unit_usd"]["p50"], 0.2)

    def test_ledger_query_is_read_only_and_excludes_refunds(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ask.sqlite"
            with sqlite3.connect(path) as connection:
                connection.execute("""
                    CREATE TABLE query_events (
                        status TEXT, completed_at TEXT, answer_mode TEXT,
                        proposed_work_class TEXT, proposed_credit_units INTEGER,
                        estimated_cost_usd REAL, latency_ms REAL, performance_json TEXT
                    )
                """)
                rows = [
                    ("completed", "2026-10-28T00:00:00+00:00", "conversation", "conversation_overview", 2, 0.10, 20000, "{}"),
                    ("failed_refunded", "2026-10-28T00:00:00+00:00", "conversation", "conversation_overview", 2, 5.00, 20000, "{}"),
                    ("completed_unbilled", "2026-10-28T00:00:00+00:00", "conversation", "conversation_menu", 0, 0.01, 100, "{}"),
                    ("completed", "2026-10-01T00:00:00+00:00", "deep_research", "deep_standard", 3, 0.80, 90000, "{}"),
                ]
                connection.executemany("INSERT INTO query_events VALUES (?,?,?,?,?,?,?,?)", rows)
            report = report_from_ledger(path, since="2026-10-20", min_samples=1)
            self.assertEqual(report["completed_rows"], 1)
            self.assertEqual(report["classes"][0]["estimated_cost_usd"]["p50"], 0.10)
            with sqlite3.connect(path) as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM query_events").fetchone()[0], 4)


if __name__ == "__main__":
    unittest.main()
