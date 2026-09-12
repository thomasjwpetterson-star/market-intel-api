import unittest

from answer_report_pdf import answer_report_filename, build_branded_answer_pdf


class AnswerReportPdfTests(unittest.TestCase):
    def test_builds_a_branded_multi_format_pdf(self):
        answer = """# Bottom line

AMRAAM remains a major air-to-air missile program with a broad production and sustainment base.

## Financial view

| Measure | Period | Value |
| --- | --- | ---: |
| Net prime obligations | FY2021–FY2025 completed | $6.739B |
| Net prime obligations | FY2026 partial | $2.244B |

- Completed years are the historical comparison baseline.
- FY2026 is shown separately because it is incomplete.
"""

        payload = build_branded_answer_pdf(
            question="Tell me everything about this defense platform or program: AMRAAM",
            answer=answer,
            scope_name="AMRAAM",
        )

        self.assertTrue(payload.startswith(b"%PDF-"))
        self.assertGreater(len(payload), 3000)
        self.assertEqual(answer_report_filename("AMRAAM", "unused"), "mimir-amraam.pdf")


if __name__ == "__main__":
    unittest.main()
