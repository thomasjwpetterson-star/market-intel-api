from __future__ import annotations

import io
import unittest
import zipfile

from platform_context_export import build_platform_context_zip


class PlatformContextExportTests(unittest.TestCase):
    @staticmethod
    def _context():
        return {
            "scope": {"display_name": "Test platform", "platform_id": "TEST"},
            "annual_activity": {"records": []},
            "direct_award_recipients": [],
            "reported_supplier_sites": [],
            "reported_component_categories": [],
            "item_and_component_evidence": {
                "top_items": [],
                "authorized_source_depth": {},
                "top_item_supplier_sites": [],
            },
            "top_prime_awards": [],
            "current_opportunities": [],
            "evidence_index": [],
        }

    def test_forward_evidence_is_part_of_the_platform_pack(self):
        payload = build_platform_context_zip(
            self._context(),
            outlook={
                "evidence_lanes": {
                    "budget_and_fydp": [
                        {
                            "fiscal_year": 2028,
                            "planning_phase": "projection",
                            "budget_line_item_title": "TEST PROGRAM",
                            "measure_type": "net_procurement_p1",
                            "amount_usd": 100.0,
                        }
                    ],
                    "official_contract_announcements": [
                        {
                            "announcement_date": "2026-09-10",
                            "recipient_text": "Example Corp",
                            "announced_value_usd": 500.0,
                        }
                    ],
                }
            },
        )
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            names = set(archive.namelist())
            self.assertIn("10_budget_and_fydp.csv", names)
            self.assertIn("11_official_contract_announcements.csv", names)
            self.assertIn("TEST PROGRAM", archive.read("10_budget_and_fydp.csv").decode())

    def test_legacy_platform_pack_still_exports_without_forward_evidence(self):
        payload = build_platform_context_zip(self._context())
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            names = set(archive.namelist())
            self.assertNotIn("10_budget_and_fydp.csv", names)
            self.assertIn("09_source_guide.csv", names)


if __name__ == "__main__":
    unittest.main()
