import tempfile
import unittest
from pathlib import Path

from beta_controls import AccessContext, BetaStateStore


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


if __name__ == "__main__":
    unittest.main()
