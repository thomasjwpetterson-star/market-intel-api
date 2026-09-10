from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from company_context import CompanyContextBuilder
from company_context_store import CompanyContextStore


class CompanyFydpLinkageTests(unittest.TestCase):
    def test_platform_evidence_links_to_explicit_budget_lines(self):
        with tempfile.TemporaryDirectory() as directory:
            budget_path = Path(directory) / "budget.parquet"
            connection = duckdb.connect()
            connection.execute(
                """
                COPY (
                    SELECT * FROM (VALUES
                        ('Army', 15, 'HIMARS', 'High Mobility Artillery Rocket System (HIMARS)',
                         false, 2025, 'actual', 'net_procurement_p1', 100.0, NULL,
                         'PUBLISHED', 'source-1', 'Army book', 10, 'https://example.gov',
                         'https://example.gov/book.pdf', 'P-1 line 15'),
                        ('Army', 15, 'HIMARS', 'High Mobility Artillery Rocket System (HIMARS)',
                         false, 2027, 'total_request', 'net_procurement_p1', 150.0, NULL,
                         'PUBLISHED', 'source-1', 'Army book', 10, 'https://example.gov',
                         'https://example.gov/book.pdf', 'P-1 line 15'),
                        ('Army', 99, 'OTHER', 'Unrelated Vehicle', false, 2027,
                         'total_request', 'net_procurement_p1', 999.0, NULL,
                         'PUBLISHED', 'source-1', 'Army book', 20, 'https://example.gov',
                         'https://example.gov/book.pdf', 'P-1 line 99')
                    ) AS t(component, p1_line_number, budget_line_item,
                           budget_line_item_title, is_advance_procurement_exhibit,
                           fiscal_year, funding_status, measure_type, amount_usd,
                           quantity, availability_status, source_id,
                           source_document_title, source_page_number,
                           source_landing_page, source_download_url, source_locator)
                ) TO ? (FORMAT PARQUET)
                """,
                [str(budget_path)],
            )

            builder = object.__new__(CompanyContextBuilder)
            builder.connection = connection
            builder.fydp_budget_path = budget_path
            builder.dod_budget_path = Path(directory) / "missing-near-term.parquet"
            builder.fydp_platform_linkages = {
                "definition_version": "test-v1",
                "scope_note": "test scope",
                "linkages": [
                    {
                        "program_id": "HIMARS",
                        "display_name": "HIMARS",
                        "platform_aliases": ["HIMARS"],
                        "budget_title_aliases": [
                            "HIGH MOBILITY ARTILLERY ROCKET SYSTEM (HIMARS)"
                        ],
                    }
                ],
            }

            result = builder._future_demand_context(
                [
                    {
                        "platform_family": "HIMARS",
                        "evidence_layer": "reported_subaward",
                        "source_system": "USA_SPENDING_SUBAWARD",
                        "observed_value_usd": 25.0,
                    }
                ],
                {"programs": []},
            )

            self.assertEqual(result["definition_version"], "test-v1")
            self.assertEqual(len(result["programs"]), 1)
            program = result["programs"][0]
            self.assertEqual(program["program_id"], "HIMARS")
            self.assertEqual(program["matched_company_platforms"], ["HIMARS"])
            self.assertEqual(len(program["budget_projection_rows"]), 2)
            self.assertEqual(
                {row["fiscal_year"] for row in program["budget_projection_rows"]},
                {2025, 2027},
            )

    def test_unrelated_platform_does_not_gain_budget_context(self):
        with tempfile.TemporaryDirectory() as directory:
            budget_path = Path(directory) / "budget.parquet"
            duckdb.sql(
                """
                COPY (
                    SELECT 'Army' AS component,
                           15 AS p1_line_number,
                           'HIMARS' AS budget_line_item,
                           'High Mobility Artillery Rocket System (HIMARS)'
                               AS budget_line_item_title,
                           false AS is_advance_procurement_exhibit,
                           2027 AS fiscal_year,
                           'total_request' AS funding_status,
                           'net_procurement_p1' AS measure_type,
                           150.0 AS amount_usd,
                           NULL::DOUBLE AS quantity,
                           'PUBLISHED' AS availability_status,
                           'source-1' AS source_id,
                           'Army book' AS source_document_title,
                           10 AS source_page_number,
                           'https://example.gov' AS source_landing_page,
                           'https://example.gov/book.pdf' AS source_download_url,
                           'P-1 line 15' AS source_locator
                ) TO ? (FORMAT PARQUET)
                """,
                params=[str(budget_path)],
            )

            builder = object.__new__(CompanyContextBuilder)
            builder.connection = duckdb.connect()
            builder.fydp_budget_path = budget_path
            builder.dod_budget_path = Path(directory) / "missing-near-term.parquet"
            builder.fydp_platform_linkages = {
                "definition_version": "test-v1",
                "scope_note": "test scope",
                "linkages": [
                    {
                        "program_id": "HIMARS",
                        "display_name": "HIMARS",
                        "platform_aliases": ["HIMARS"],
                        "budget_title_aliases": [
                            "HIGH MOBILITY ARTILLERY ROCKET SYSTEM (HIMARS)"
                        ],
                    }
                ],
            }

            result = builder._future_demand_context(
                [{"platform_family": "C-130", "observed_value_usd": 500.0}],
                {"programs": []},
            )

            self.assertEqual(result["programs"], [])

    def test_near_term_budget_uses_additive_p1_total_without_double_counting(self):
        with tempfile.TemporaryDirectory() as directory:
            budget_path = Path(directory) / "dod-budget.parquet"
            connection = duckdb.connect()
            connection.execute(
                """
                COPY (
                    SELECT * FROM (VALUES
                        ('P-1', 'Total', 2027, true, 'HIMARS', 'HIMARS',
                         'Army', '15', 'amount', 100.0, NULL, 'p1.xlsx', 10),
                        ('P-1', 'Total', 2027, true, 'HIMARS', 'HIMARS',
                         'Army', '15', 'amount', -10.0, NULL, 'p1.xlsx', 11),
                        ('P-1', 'Total', 2027, true, 'HIMARS', 'HIMARS',
                         'Army', '15', 'quantity', NULL, 2.0, 'p1.xlsx', 10),
                        ('P-1', 'Discretionary Request', 2027, true, 'HIMARS',
                         'HIMARS', 'Army', '15', 'amount', 500.0, NULL,
                         'p1.xlsx', 10)
                    ) AS t(exhibit_type, funding_status, fiscal_year, is_additive,
                           budget_line_item, budget_line_item_title, organization,
                           line_number, measure_type, amount_usd, quantity,
                           source_file, source_row_number)
                ) TO ? (FORMAT PARQUET)
                """,
                [str(budget_path)],
            )

            builder = object.__new__(CompanyContextBuilder)
            builder.connection = connection
            builder.dod_budget_path = budget_path
            builder.fydp_budget_path = Path(directory) / "missing-fydp.parquet"
            builder.fydp_platform_linkages = {
                "definition_version": "test-v1",
                "scope_note": "test scope",
                "linkages": [
                    {
                        "program_id": "HIMARS",
                        "display_name": "HIMARS",
                        "platform_aliases": ["HIMARS"],
                        "budget_title_aliases": ["HIMARS"],
                    }
                ],
            }

            result = builder._future_demand_context(
                [{"platform_family": "HIMARS", "observed_value_usd": 10.0}],
                {"programs": []},
            )

            rows = result["programs"][0]["budget_projection_rows"]
            amount = next(row for row in rows if row["measure_type"] == "net_procurement_p1")
            quantity = next(
                row for row in rows if row["measure_type"] == "procurement_quantity"
            )
            self.assertEqual(amount["amount_usd"], 90.0)
            self.assertEqual(quantity["quantity"], 2.0)
            self.assertEqual(amount["funding_status"], "total_request")

    def test_compact_answer_pack_keeps_company_linkage_evidence(self):
        compact = CompanyContextStore._compact_section(
            "future_demand_context",
            {
                "programs": [
                    {
                        "program_id": "HIMARS",
                        "program_name": "HIMARS",
                        "matched_company_platforms": ["HIMARS"],
                        "relationship_basis": "Explicit platform-to-budget linkage",
                        "company_platform_evidence": [
                            {
                                "platform_family": "HIMARS",
                                "evidence_layer": "reported_subaward",
                                "observed_value_usd": 25.0,
                            }
                        ],
                        "budget_projection_rows": [],
                    }
                ]
            },
        )

        program = compact["programs"][0]
        self.assertEqual(program["matched_company_platforms"], ["HIMARS"])
        self.assertEqual(len(program["company_platform_evidence"]), 1)
        self.assertEqual(
            program["relationship_basis"],
            "Explicit platform-to-budget linkage",
        )


if __name__ == "__main__":
    unittest.main()
