from datetime import date
import unittest
from unittest.mock import patch

from etl_automation.usaspending_coverage import (
    CoverageError, audit_recent_months, official_count, recent_closed_months,
)


class Response:
    def raise_for_status(self):
        return None

    def json(self):
        return {"calculated_transaction_count": 280554,
                "transaction_rows_gt_limit": False}


class CoverageTests(unittest.TestCase):
    def test_closed_month_windows_cross_fiscal_year(self):
        months = recent_closed_months(date(2026, 10, 28))
        self.assertEqual(months[0], ("2026-04", "2026-04-01", "2026-04-30"))
        self.assertEqual(months[-1], ("2026-09", "2026-09-01", "2026-09-30"))

    def test_official_count_uses_dod_primes_and_action_dates(self):
        calls = []
        def post(url, **kwargs):
            calls.append((url, kwargs))
            return Response()
        self.assertEqual(official_count("2026-07-01", "2026-07-31", post), 280554)
        filters = calls[0][1]["json"]["filters"]
        self.assertEqual(filters["agencies"][0]["name"], "Department of Defense")
        self.assertEqual(filters["time_period"][0]["start_date"], "2026-07-01")
        self.assertEqual(len(filters["award_type_codes"]), 12)

    def test_large_july_gap_blocks_candidate(self):
        observed = {"2026-07": 22093}
        with patch("etl_automation.usaspending_coverage.silver_counts", return_value=observed):
            with self.assertRaisesRegex(CoverageError, "2026-07"):
                audit_recent_months(None, "bucket", date(2026, 10, 28),
                                    lambda start, end: 280554 if start == "2026-07-01" else 0)

    def test_small_publication_drift_is_reported(self):
        observed = {"2026-07": 280500}
        with patch("etl_automation.usaspending_coverage.silver_counts", return_value=observed):
            rows = audit_recent_months(None, "bucket", date(2026, 10, 28),
                                       lambda start, end: 280554 if start == "2026-07-01" else 0)
        july = next(row for row in rows if row["month"] == "2026-07")
        self.assertEqual(july["gap"], 54)


if __name__ == "__main__":
    unittest.main()
