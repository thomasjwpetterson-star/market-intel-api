from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from program_outlook_store import ProgramOutlookStore, is_program_outlook_language


class FakePlatformContexts:
    def search(self, query: str):
        normalized = query.strip().upper()
        resolved = "F-35" if normalized in {"F-35", "F35", "F35_TEST"} else None
        return {
            "resolved_platform_id": resolved,
            "matches": [{"platform_id": resolved}] if resolved else [],
            "requires_disambiguation": False,
        }

    def _platform_members(self, platform_id: str):
        return ["F-35"]

    def get(self, platform_id: str):
        return {
            "annual_activity": {
                "records": [
                    {
                        "fiscal_year": 2025,
                        "source_system": "USA_SPENDING",
                        "net_prime_obligations_usd": 90.0,
                        "positive_prime_obligations_usd": 100.0,
                        "prime_deobligations_usd": -10.0,
                        "award_count": 2,
                        "action_or_line_count": 4,
                    },
                    {
                        "fiscal_year": 2026,
                        "source_system": "USA_SPENDING",
                        "net_prime_obligations_usd": 25.0,
                        "positive_prime_obligations_usd": 25.0,
                        "prime_deobligations_usd": 0.0,
                        "award_count": 1,
                        "action_or_line_count": 1,
                    },
                    {
                        "fiscal_year": 2025,
                        "source_system": "DLA",
                        "net_prime_obligations_usd": 999.0,
                    },
                ]
            },
            "current_opportunities": [
                {"id": "OPEN-1", "response_status": "OPEN", "url": "https://sam.gov/1"},
                {"id": "CLOSED-1", "response_status": "CLOSED", "url": "https://sam.gov/2"},
            ],
            "top_prime_awards": [{"contract_id": "FA123", "net_prime_obligations_usd": 90.0}],
        }


class ProgramOutlookStoreTests(unittest.TestCase):
    def test_default_linkages_include_amraam(self):
        definitions = json.loads(
            (Path(__file__).resolve().parents[1] / "fydp_platform_linkages.json").read_text()
        )
        amraam = next(
            row for row in definitions["linkages"] if row["program_id"] == "AMRAAM"
        )
        self.assertIn("AMRAAM", amraam["platform_aliases"])
        self.assertIn(
            "ADVANCED MEDIUM RANGE AIR-TO-AIR MISSILE (AMRAAM)",
            amraam["budget_title_aliases"],
        )

    def test_default_linkages_include_t7a_fydp(self):
        definitions = json.loads(
            (Path(__file__).resolve().parents[1] / "fydp_platform_linkages.json").read_text()
        )
        t7a = next(
            row for row in definitions["linkages"] if row["program_id"] == "T7A"
        )
        self.assertIn("T-7", t7a["platform_aliases"])
        self.assertIn("ADVANCED PILOT TRAINING T-7A", t7a["budget_title_aliases"])
        facts = t7a["published_budget_supplement"]["facts"]
        self.assertEqual(
            [row["quantity"] for row in facts if row["measure_type"] == "procurement_quantity"],
            [36.0, 42.0, 60.0, 60.0],
        )
        self.assertEqual(
            sum(
                row["amount_usd"]
                for row in facts
                if row["measure_type"] == "net_procurement_p1"
            ),
            3917137000.0,
        )

    def test_default_linkages_cover_current_aircraft_book_titles(self):
        definitions = json.loads(
            (Path(__file__).resolve().parents[1] / "fydp_platform_linkages.json").read_text()
        )
        by_id = {row["program_id"]: row for row in definitions["linkages"]}
        expected_titles = {
            "T7A": "ADVANCED PILOT TRAINING T-7A AP",
            "CH53K": "CH-53K (HEAVY LIFT)",
            "C130": "C-130J",
            "C17A": "C-17A",
            "C135": "C-135",
            "E2D": "E-2D AHE",
            "E4B": "E-4",
            "E7A": "E-7",
            "F35": "JOINT STRIKE FIGHTER CV",
            "FA18": "FA-18E/F",
            "MH139A": "MH-139A",
            "MQ25": "MQ-25",
            "P8A": "P-8A POSEIDON",
            "T1A": "T-1",
            "T38": "T-38",
            "T6": "T-6",
            "U2": "U-2 MODS",
            "UH1Y_AH1Z": "UH-1Y/AH-1Z",
        }
        for program_id, title in expected_titles.items():
            with self.subTest(program_id=program_id):
                self.assertIn(title, by_id[program_id]["budget_title_aliases"])

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.connection = duckdb.connect()
        self.definitions = self.root / "definitions.json"
        self.definitions.write_text(
            json.dumps(
                {
                    "definition_version": "test-program-links-v1",
                    "linkages": [
                        {
                            "program_id": "F35_TEST",
                            "display_name": "F-35 Lightning II",
                            "platform_aliases": ["F-35"],
                            "budget_title_aliases": ["F-35"],
                        }
                    ],
                }
            )
        )
        self._write_transactions()
        self._write_announcements()
        self._write_near_term_budget()
        self._write_fydp_budget()

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def _write_transactions(self):
        self.connection.execute(
            """
            COPY (
                SELECT 'FA123' AS contract_id, 'F-35' AS platform_family
            ) TO ? (FORMAT PARQUET)
            """,
            [str(self.root / "transactions.parquet")],
        )

    def _write_announcements(self):
        self.connection.execute(
            """
            COPY (
                SELECT * FROM (VALUES
                    ('ANN-1', '2026-09-10', 'Air Force', 1, 'award', 'Example Corp',
                     'FA123', ['FA123'], 500.0, 50.0, 'Fort Worth, Texas',
                     'Work through 2030', 'One offer', 'Air Force office',
                     'Unrelated wording linked through its contract', 'unrelated wording',
                     'Contracts for September 10, 2026', 'https://defense.gov/ann-1',
                     '2026-09-10T17:00:00Z', '2026-09-11T00:00:00Z'),
                    ('ANN-2', '2026-09-09', 'Air Force', 2, 'award', 'Another Corp',
                     'OTHER', ['OTHER'], 300.0, NULL, 'California',
                     'Work through 2029', 'Competitive', 'Air Force office',
                     'F-35 production support', 'F-35 production support',
                     'Contracts for September 9, 2026', 'https://defense.gov/ann-2',
                     '2026-09-09T17:00:00Z', '2026-09-10T00:00:00Z'),
                    ('ANN-3', '2026-09-08', 'Army', 3, 'award', 'Unrelated Corp',
                     'OTHER2', ['OTHER2'], 900.0, NULL, 'Alabama',
                     'Work through 2028', 'Competitive', 'Army office',
                     'Truck support', 'truck support',
                     'Contracts for September 8, 2026', 'https://defense.gov/ann-3',
                     '2026-09-08T17:00:00Z', '2026-09-09T00:00:00Z')
                ) AS t(
                    announcement_id, announcement_date, service, entry_index,
                    entry_type, recipient_text, primary_contract_id, contract_ids,
                    announced_value_usd, obligated_at_announcement_usd,
                    work_locations, completion_text, competition_text,
                    contracting_activity, description, search_text, source_title,
                    source_url, source_published_at, retrieved_at
                )
            ) TO ? (FORMAT PARQUET)
            """,
            [str(self.root / "dod_contract_announcements.parquet")],
        )

    def _write_near_term_budget(self):
        self.connection.execute(
            """
            COPY (
                SELECT * FROM (VALUES
                    ('P-1', 'Total', 2025, true, 'F35', 'F-35', 'A', 'Weapon System Cost', 'Air Force', '1',
                     'amount', 100.0, NULL, 'p1.xlsx', 10),
                    ('P-1', 'Total', 2026, true, 'F35', 'F-35', 'A', 'Weapon System Cost', 'Air Force', '1',
                     'quantity', NULL, 2.0, 'p1.xlsx', 10),
                    ('R-1', 'Total', 2027, true, 'F35', 'F-35', NULL, NULL, 'Air Force', '7',
                     'amount', 50.0, NULL, 'r1.xlsx', 20)
                ) AS t(
                    exhibit_type, funding_status, fiscal_year, is_additive,
                    budget_line_item, budget_line_item_title, cost_type,
                    cost_type_title, organization,
                    line_number, measure_type, amount_usd, quantity, source_file,
                    source_row_number
                )
            ) TO ? (FORMAT PARQUET)
            """,
            [str(self.root / "dod_budget_facts.parquet")],
        )

    def _write_fydp_budget(self):
        self.connection.execute(
            """
            COPY (
                SELECT * FROM (VALUES
                    ('Air Force', 1, 'F35', 'F-35', false, 2028, 'projected',
                     'net_procurement_p1', 120.0, NULL, 'PUBLISHED', 'p40.pdf',
                     'F-35 P-40', 1, 'https://comptroller.defense.gov',
                     'https://example.gov/p40.pdf', 'page 1'),
                    ('Air Force', 1, 'F35', 'F-35', false, 2028, 'projected',
                     'procurement_quantity', NULL, 3.0, 'PUBLISHED', 'p40.pdf',
                     'F-35 P-40', 1, 'https://comptroller.defense.gov',
                     'https://example.gov/p40.pdf', 'page 1')
                ) AS t(
                    component, p1_line_number, budget_line_item,
                    budget_line_item_title, is_advance_procurement_exhibit,
                    fiscal_year, funding_status, measure_type, amount_usd,
                    quantity, availability_status, source_id,
                    source_document_title, source_page_number,
                    source_landing_page, source_download_url, source_locator
                )
            ) TO ? (FORMAT PARQUET)
            """,
            [str(self.root / "dod_fydp_budget_facts.parquet")],
        )

    def _store(self):
        return ProgramOutlookStore(
            self.root,
            FakePlatformContexts(),
            definitions_path=self.definitions,
            dod_budget_path=self.root / "dod_budget_facts.parquet",
            fydp_budget_path=self.root / "dod_fydp_budget_facts.parquet",
            announcement_path=self.root / "dod_contract_announcements.parquet",
        )

    def test_combines_current_release_evidence_without_blending_measures(self):
        store = self._store()
        result = store.answer_projection(platform_id="F-35")
        lanes = result["evidence_lanes"]

        self.assertEqual(result["scope"]["program_id"], "F35_TEST")
        self.assertEqual(
            [row["period_status"] for row in lanes["historical_prime_obligations"]],
            ["completed", "partial"],
        )
        self.assertEqual(len(lanes["official_contract_announcements"]), 2)
        self.assertEqual(
            {row["match_basis"] for row in lanes["official_contract_announcements"]},
            {"LINKED_CONTRACT_ID", "PROGRAM_NAME_IN_ANNOUNCEMENT"},
        )
        self.assertEqual(
            {row["planning_phase"] for row in lanes["budget_and_fydp"]},
            {"actual", "enacted", "request", "projection"},
        )
        self.assertEqual(
            [row["quantity"] for row in lanes["explicit_procurement_quantities"]],
            [2.0, 3.0],
        )
        self.assertEqual(
            [row["id"] for row in lanes["open_solicitations"]],
            ["OPEN-1"],
        )
        self.assertIn("must not be added", result["interpretation_rules"]["non_additive_measures"])
        store.connection.close()

    def test_published_budget_supplement_extends_missing_outyears(self):
        definitions = json.loads(self.definitions.read_text())
        definitions["linkages"][0]["published_budget_supplement"] = {
            "component": "Air Force",
            "budget_line_number": "1",
            "budget_line_item": "F35",
            "budget_line_item_title": "F-35",
            "source_id": "official-p40-supplement",
            "source_document_title": "F-35 P-40",
            "source_landing_page": "https://example.gov/budget",
            "source_download_url": "https://example.gov/p40.pdf",
            "source_locator": "P-1 line 1",
            "facts": [
                {
                    "fiscal_year": 2029,
                    "measure_type": "net_procurement_p1",
                    "amount_usd": 140.0,
                },
                {
                    "fiscal_year": 2029,
                    "measure_type": "procurement_quantity",
                    "quantity": 4.0,
                },
            ],
        }
        self.definitions.write_text(json.dumps(definitions))
        store = self._store()
        rows = store.get(platform_id="F-35")["evidence_lanes"]["budget_and_fydp"]
        supplements = [row for row in rows if row.get("source_id") == "official-p40-supplement"]
        self.assertEqual(len(supplements), 2)
        self.assertEqual({row["fiscal_year"] for row in supplements}, {2029})
        self.assertEqual({row["planning_phase"] for row in supplements}, {"projection"})
        store.connection.close()

    def test_missing_optional_sources_yields_a_bounded_partial_view(self):
        store = ProgramOutlookStore(
            self.root,
            FakePlatformContexts(),
            definitions_path=self.definitions,
            dod_budget_path=self.root / "missing-budget.parquet",
            fydp_budget_path=self.root / "missing-fydp.parquet",
            announcement_path=self.root / "missing-announcements.parquet",
        )
        result = store.get(platform_id="F35_TEST")
        self.assertEqual(result["evidence_lanes"]["budget_and_fydp"], [])
        self.assertEqual(result["evidence_lanes"]["official_contract_announcements"], [])
        self.assertEqual(
            result["coverage"]["available_lanes"],
            ["historical_prime_obligations", "open_solicitations"],
        )
        store.connection.close()

    def test_outlook_language_is_specific_to_forward_questions(self):
        for question in (
            "What is the five-year outlook for the F-35?",
            "Show the FYDP and future funding for Virginia class.",
            "What is the production trajectory for AMRAAM?",
            "What is happening with CH-53K production?",
            "Show the current F-35 budget request.",
        ):
            with self.subTest(question=question):
                self.assertTrue(is_program_outlook_language(question))
        self.assertFalse(is_program_outlook_language("Who supplies the F-35?"))


if __name__ == "__main__":
    unittest.main()
