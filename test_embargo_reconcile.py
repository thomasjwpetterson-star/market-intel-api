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
        with patch.object(reconcile, "silver_counts", return_value={
            "2026-04": 1000, "2026-05": 1000, "2026-06": 1000,
            "2026-07": 22093, "2026-08": 1000, "2026-09": 1000,
        }), patch.object(reconcile, "official_count", side_effect=[
            1000, 1000, 1000, 280554, 1000, 1000,
        ]):
            selected = reconcile.select_gap(None, date(2026, 10, 28))
        self.assertEqual(selected, (date(2026, 7, 1), date(2026, 7, 31), 280554, 22093))


if __name__ == "__main__":
    unittest.main()
