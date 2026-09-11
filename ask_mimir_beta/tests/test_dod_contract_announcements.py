import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingest_dod_contract_announcements import (
    _merge_records,
    discover_articles,
    fetch_article_text,
    parse_announcement,
)


class DodContractAnnouncementTests(unittest.TestCase):
    def test_merge_preserves_first_observed_record(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "announcements.parquet"
            pd.DataFrame(
                [
                    {
                        "announcement_id": "ANN-1",
                        "announcement_date": "2026-09-10",
                        "source_article_id": "ARTICLE-1",
                        "entry_index": 1,
                        "retrieved_at": "first",
                    }
                ]
            ).to_parquet(output, index=False)

            merged = _merge_records(
                output,
                [
                    {
                        "announcement_id": "ANN-1",
                        "announcement_date": "2026-09-10",
                        "source_article_id": "ARTICLE-1",
                        "entry_index": 1,
                        "retrieved_at": "second",
                    }
                ],
            )

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged.iloc[0]["retrieved_at"], "first")

    @patch("ingest_dod_contract_announcements._fetch")
    def test_html_official_response_uses_text_renderer(self, fetch):
        fetch.side_effect = [
            b"<!doctype html><html><body>official page</body></html>",
            b"Markdown Content:\n\n**AIR FORCE**\n\nExample award.",
        ]
        text, fetch_url, method = fetch_article_text("https://www.defense.gov/example")
        self.assertIn("Markdown Content", text)
        self.assertEqual(fetch_url, "https://r.jina.ai/http://www.defense.gov/example")
        self.assertEqual(method, "text_renderer")

    def test_discovers_only_contract_articles_inside_window(self):
        feed = b"""<?xml version="1.0"?><rss><channel>
          <item><title>Contracts for Sept. 10, 2026</title>
          <link>https://www.war.gov/News/Contracts/Contract/Article/4596373/example/</link>
          <pubDate>Thu, 10 Sep 2026 21:00:19 GMT</pubDate></item>
          <item><title>Other release</title><link>https://example.com</link>
          <pubDate>Thu, 10 Sep 2026 21:00:19 GMT</pubDate></item>
        </channel></rss>"""
        rows = discover_articles(feed, datetime(2026, 9, 1, tzinfo=timezone.utc))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["article_id"], "4596373")

    def test_parses_financial_fields_without_combining_them(self):
        page = """Title: Contracts For Jun. 16, 2025

Markdown Content:
**AIR FORCE**

Borsight Inc., Ogden, Utah, has been awarded a ceiling $2,180,000,000 firm-fixed-price contract for the T-6A avionics replacement program. Work will be performed at Ogden, Utah, and is expected to be completed by Jan. 6, 2034. This contract was a competitive acquisition and 12 offers were received. Fiscal 2025 funds in the amount of $8,774,306 are being obligated at the time of award. The Air Force Life Cycle Management Center is the contracting activity (FA8106-25-D-B001).

**NAVY**

American Electronic Warfare Associates Inc., California, Maryland, is awarded a $466,653,786 contract for aircraft research. No funds will be obligated at the time of award. Naval Air Warfare Center Aircraft Division is the contracting activity (N0042125D0303).
"""
        article = {
            "article_id": "4218062",
            "title": "Contracts for Jun. 16, 2025",
            "url": "https://www.defense.gov/News/Contracts/Contract/Article/4218062/",
            "published_at": "2025-06-16T21:00:00+00:00",
        }
        rows = parse_announcement(
            article,
            page,
            fetch_url="https://r.jina.ai/http://www.defense.gov/example",
            fetch_method="text_renderer",
            retrieved_at=datetime(2026, 9, 11, tzinfo=timezone.utc),
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["service"], "AIR FORCE")
        self.assertEqual(rows[0]["recipient_text"], "Borsight Inc., Ogden, Utah")
        self.assertEqual(rows[0]["announced_value_usd"], 2_180_000_000)
        self.assertEqual(rows[0]["obligated_at_announcement_usd"], 8_774_306)
        self.assertIn("FA8106-25-D-B001", rows[0]["contract_ids"])
        self.assertEqual(rows[1]["obligated_at_announcement_usd"], 0)
        self.assertEqual(rows[1]["primary_contract_id"], "N0042125D0303")

    def test_marks_corrections_separately(self):
        page = """Markdown Content:
**ARMY**

CORRECTION: The contract announced for Example Corp. (W91234-25-C-0001), for $20,000,000, stated an incorrect date.
"""
        article = {
            "article_id": "1",
            "title": "Contracts for Jun. 16, 2025",
            "url": "https://www.defense.gov/example",
            "published_at": "2025-06-16T21:00:00+00:00",
        }
        rows = parse_announcement(
            article,
            page,
            fetch_url=article["url"],
            fetch_method="official_page",
            retrieved_at=datetime(2026, 9, 11, tzinfo=timezone.utc),
        )
        self.assertEqual(rows[0]["entry_type"], "correction")

    def test_excludes_funding_explicitly_deferred_until_after_announcement(self):
        page = """Markdown Content:
**DEFENSE FINANCE AND ACCOUNTING SERVICE**

Example LLP is being awarded a contract with a maximum value of $989,996,050 for audit services. Total face value of this award action is $388,273,413. Fiscal 2026 funds in the amount of $62,593,591 will be obligated at the time of award; and fiscal 2027 funds in the amount of $325,679,822 will be obligated when funds are available. The office is the contracting activity (HQ042326FE061).
"""
        article = {
            "article_id": "2",
            "title": "Contracts for Sept. 10, 2026",
            "url": "https://www.defense.gov/example",
            "published_at": "2026-09-10T21:00:00+00:00",
        }
        rows = parse_announcement(
            article,
            page,
            fetch_url="https://r.jina.ai/http://www.defense.gov/example",
            fetch_method="text_renderer",
            retrieved_at=datetime(2026, 9, 11, tzinfo=timezone.utc),
        )
        self.assertEqual(rows[0]["announced_value_usd"], 989_996_050)
        self.assertEqual(rows[0]["obligated_at_announcement_usd"], 62_593_591)

    def test_prefers_full_contract_number_and_ignores_modification_number(self):
        page = """Markdown Content:
**AIR FORCE**

Rolls-Royce Corp., Indianapolis, Indiana, was awarded a $43,203,798 modification (P00022) to contract FA8504-22-C-0001 for engine support.
"""
        article = {
            "article_id": "2",
            "title": "Contracts for Sept. 10, 2026",
            "url": "https://www.defense.gov/example",
            "published_at": "2026-09-10T21:00:00+00:00",
        }
        rows = parse_announcement(
            article,
            page,
            fetch_url=article["url"],
            fetch_method="official_page",
            retrieved_at=datetime(2026, 9, 11, tzinfo=timezone.utc),
        )
        self.assertEqual(rows[0]["primary_contract_id"], "FA8504-22-C-0001")
        self.assertEqual(rows[0]["contract_ids"], ["FA8504-22-C-0001"])


if __name__ == "__main__":
    unittest.main()
