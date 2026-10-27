import unittest
from datetime import date
from unittest.mock import patch

from source_automation import glue_usaspending_embargo_reconcile as reconcile


class EmbargoReconcileTests(unittest.TestCase):
    def test_weekly_windows_cover_month_once(self):
        windows = list(reconcile.weekly_windows(date(2026, 7, 1), date(2026, 7, 31)))
        self.assertEqual(windows, [
            (date(2026, 7, 1), date(2026, 7, 7)),
            (date(2026, 7, 8), date(2026, 7, 14)),
            (date(2026, 7, 15), date(2026, 7, 21)),
            (date(2026, 7, 22), date(2026, 7, 28)),
            (date(2026, 7, 29), date(2026, 7, 31)),
        ])

    def test_material_gap_selects_missing_month(self):
        months = reconcile.month_windows(date(2026, 10, 28))
        self.assertEqual((months[0][0], months[-1][0]), (date(2025, 10, 1), date(2026, 9, 1)))
        observed = {start.strftime("%Y-%m"): 1000 for start, _ in months}
        observed["2026-07"] = 22093
        with patch.object(reconcile, "silver_counts", return_value=observed), patch.object(
            reconcile, "official_count", side_effect=lambda start, end: 280554 if start.month == 7 else 1000
        ):
            selected = reconcile.select_gap(None, date(2026, 10, 28))
        self.assertEqual(selected, (date(2026, 7, 1), date(2026, 7, 31), 280554, 22093))


if __name__ == "__main__":
    unittest.main()
