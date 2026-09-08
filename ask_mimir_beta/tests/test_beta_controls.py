import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from beta_controls import (
    AccessContext,
    BetaStateStore,
    DailyQuotaExceeded,
    TIER_POLICIES,
)


class BetaStateStoreTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
