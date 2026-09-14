import unittest
from unittest.mock import patch
from reportlab.platypus import CondPageBreak, Paragraph, Table

from answer_report_pdf import answer_report_filename, build_branded_answer_pdf


class AnswerReportPdfTests(unittest.TestCase):
    def test_heading_reserves_space_before_a_bullet_list(self):
        with patch('answer_report_pdf._BrandedDocTemplate.build') as build:
            build_branded_answer_pdf(question='Test', answer='## Evidence used\n\n- Award records\n- Budget documents')
        story = build.call_args.args[0]
        heading = next(index for index, item in enumerate(story) if isinstance(item, Paragraph) and item.getPlainText() == 'Evidence used')
        self.assertIsInstance(story[heading - 1], CondPageBreak)

    def test_heading_keeps_direct_connection_to_following_table(self):
        with patch('answer_report_pdf._BrandedDocTemplate.build') as build:
            build_branded_answer_pdf(question='Test', answer='## Platform exposure\n\n| Platform | Value |\n| --- | --- |\n| T-7 | 12% |')
        story = build.call_args.args[0]
        heading = next(index for index, item in enumerate(story) if isinstance(item, Paragraph) and item.getPlainText() == 'Platform exposure')
        self.assertFalse(story[heading].getKeepWithNext())
        self.assertIsInstance(story[heading - 1], CondPageBreak)
        self.assertIsInstance(story[heading + 1], Table)

    def test_long_table_starts_on_available_page_instead_of_leaving_it_empty(self):
        pages = []
        def record(document, flowable):
            if isinstance(flowable, Table):
                for row in flowable._cellvalues:
                    for cell in row:
                        for part in cell if isinstance(cell, (list, tuple)) else [cell]:
                            if isinstance(part, Paragraph) and part.getPlainText() == 'Site 0':
                                pages.append(document.page)
        answer = '## Overview\n\n' + ('Research evidence for this company. ' * 30)
        answer += '\n\n## Supplier positions\n\n| Supplier | Role |\n| --- | --- |\n'
        for index in range(13):
            answer += f'| Site {index} | ' + ('Aerospace component design and manufacturing. ' * 5) + ' |\n'
        with patch('answer_report_pdf._BrandedDocTemplate.afterFlowable', record):
            build_branded_answer_pdf(question='Company overview', answer=answer, scope_name='Example')
        self.assertEqual(pages, [1])

    def test_long_table_cells_can_continue_onto_another_page(self):
        answer = '| Supplier | Role |\n| --- | --- |\n| Test supplier | ' + ('Detailed capability evidence. ' * 600) + ' |'
        payload = build_branded_answer_pdf(question='Test question',answer=answer,scope_name='Test scope')
        self.assertTrue(payload.startswith(b'%PDF-'))

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
