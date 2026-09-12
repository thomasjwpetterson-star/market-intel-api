import unittest
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock

import duckdb

from platform_context import PlatformContextStore, _canonical_fingerprint_value
from company_intent import company_follow_up_intent
from platform_intent import (
    is_open_capability_discovery_request,
    is_platform_centered_request,
    platform_answer_mode,
    platform_comparison_answer_mode,
    platform_comparison_follow_up_intent,
    platform_follow_up_intent,
    platform_follow_up_retains_scope,
)


class PlatformResolutionAndIntentTests(unittest.TestCase):
    def setUp(self):
        self.store = object.__new__(PlatformContextStore)
        self.store.platforms = [
            "PAC-3",
            "PAC-3 MSE",
            "PATRIOT",
            "PATRIOT AIR DEFENSE SYSTEM",
            "LTAMDS",
            "TOMAHAWK",
            "UH-60",
            "CH-47",
            "F-16",
            "F-35",
            "B-52",
            "AMRAAM",
            "SM-6",
            "STRYKER",
            "AH-64",
            "UH-60",
            "CH-53K",
            "P-8A",
            "NEXT GEN OPIR",
            "VIRGINIA CLASS (SSN 774)",
            "ARRW",
            "AVENGER (FAADS LOS-R)",
            "MK15 CLOSE IN WPN SYS",
            "INTERMEDIATE RANGE CONVENTIONAL PROMPT STRIKE (IRCPS)",
            "HACM",
            "IFPC INC 2",
            "AIM-260 JATM",
            "LRSO",
            "GBU-39 SMALL DIAMETER BOMB",
            "SDB I",
            "SDB II",
            "AN/SLQ-25 TORPEDO COUNTERMEASURE",
        ]

    def test_patriot_resolves_to_the_umbrella_system(self):
        result = self.store.search("Patriot air defence system")
        self.assertEqual(result["resolved_platform_id"], "PATRIOT AIR DEFENSE SYSTEM")

    def test_pac3_mse_remains_independently_queryable(self):
        result = self.store.search("PAC-3 MSE")
        self.assertEqual(result["resolved_platform_id"], "PAC-3 MSE")

    def test_patriot_umbrella_includes_the_new_radar(self):
        self.assertEqual(
            self.store._platform_members("PATRIOT AIR DEFENSE SYSTEM"),
            ["PATRIOT", "PAC-3", "PAC-3 MSE", "LTAMDS"],
        )

    def test_pac3_mse_mention_does_not_also_return_pac3(self):
        result = self.store.mentions("Who supplies PAC-3 MSE?")
        self.assertEqual(result, ["PAC-3 MSE"])

    def test_patriot_mention_does_not_collapse_to_a_pac3_member(self):
        result = self.store.mentions(
            "Who are the major suppliers associated with the Patriot air defence system?"
        )
        self.assertEqual(result, ["PATRIOT AIR DEFENSE SYSTEM"])

    def test_supplier_role_follow_up_has_its_own_mode(self):
        self.assertEqual(
            platform_answer_mode("What does each of the main suppliers provide?"),
            "supplier_roles",
        )

    def test_supplier_activity_ranking_has_its_own_mode(self):
        self.assertEqual(
            platform_answer_mode("Rank the suppliers by visible or mapped activity."),
            "supplier_value_ranking",
        )

    def test_strongest_supplier_positions_are_a_value_ranking(self):
        self.assertEqual(
            platform_answer_mode("Which suppliers have the strongest positions?"),
            "supplier_value_ranking",
        )

    def test_supplier_role_follow_up_retains_platform_scope(self):
        self.assertTrue(
            platform_follow_up_intent("What does each of the main suppliers provide?")
        )

    def test_supplier_ranking_follow_up_retains_platform_scope(self):
        self.assertTrue(
            platform_follow_up_intent("Rank the suppliers by visible or mapped activity.")
        )

    def test_general_conclusions_do_not_inherit_a_prior_specialist_lens(self):
        self.assertEqual(
            platform_answer_mode("What are the three most important conclusions?"),
            "platform_conclusions",
        )
        self.assertTrue(
            platform_follow_up_intent("What are the three most important conclusions?")
        )

    def test_cross_program_dependency_question_has_a_distinct_mode(self):
        self.assertEqual(
            platform_answer_mode("Which suppliers create cross-program dependencies?"),
            "supplier_cross_program",
        )

    def test_cross_program_follow_up_retains_the_active_platform(self):
        self.assertTrue(
            platform_follow_up_retains_scope(
                "Which of those suppliers are also important to other US Army aviation programs?"
            )
        )

    def test_explicit_new_platform_question_can_change_scope(self):
        self.assertFalse(
            platform_follow_up_retains_scope("Who supplies the Black Hawk?")
        )

    def test_alternative_source_question_has_a_distinct_mode(self):
        self.assertEqual(
            platform_answer_mode("Which suppliers have the fewest alternative sources?"),
            "supplier_source_depth",
        )

    def test_small_supplier_base_dependency_is_concentration(self):
        self.assertEqual(
            platform_answer_mode(
                "Which parts of the supply chain appear dependent on a small number of supplier sites?"
            ),
            "supplier_concentration",
        )

    def test_two_named_rotorcraft_are_both_detected(self):
        self.assertEqual(
            set(self.store.mentions(
                "Compare the supplier bases of the UH-60 Black Hawk and CH-47 Chinook."
            )),
            {"UH-60", "CH-47"},
        )

    def test_compact_platform_designations_are_detected(self):
        cases = {
            "Show suppliers for F16": ["F-16"],
            "Who supplies CH53K?": ["CH-53K"],
            "SM6 production outlook": ["SM-6"],
            "What vendors support P8A?": ["P-8A"],
        }
        for question, expected in cases.items():
            with self.subTest(question=question):
                self.assertEqual(self.store.mentions(question), expected)

    def test_common_platform_names_are_detected(self):
        self.assertEqual(
            set(self.store.mentions("Where do Apache and Black Hawk suppliers overlap?")),
            {"AH-64", "UH-60"},
        )

    def test_next_gen_opir_is_detected_as_a_named_platform(self):
        self.assertEqual(
            self.store.mentions("Tell me about Next Gen OPIR and its supplier base."),
            ["NEXT GEN OPIR"],
        )

    def test_budget_program_aliases_resolve_to_existing_platform_records(self):
        cases = {
            "What is the outlook for AGM-183A ARRW?": ["ARRW"],
            "Show the CIWS budget trajectory.": ["MK15 CLOSE IN WPN SYS"],
            "What lies ahead for Conventional Prompt Strike?": [
                "INTERMEDIATE RANGE CONVENTIONAL PROMPT STRIKE (IRCPS)"
            ],
            "Show the JATM production outlook.": ["AIM-260 JATM"],
            "What is the future funding for SDB II?": ["SDB II"],
            "What is the outlook for Small Diameter Bomb II?": ["SDB II"],
        }
        for question, expected in cases.items():
            with self.subTest(question=question):
                self.assertEqual(self.store.mentions(question), expected)

    def test_universal_projection_uses_the_resolved_platform_for_supplier_years(self):
        store = object.__new__(PlatformContextStore)
        store.get = Mock(
            return_value={
                "scope": {"platform_id": "NEXT GEN OPIR"},
                "calculation_version": "test",
                "generated_at": "test",
                "evidence_fingerprint": "test",
                "direct_award_recipients": [],
                "reported_supplier_sites": [{"cage": "TEST1"}],
                "reported_component_categories": [],
                "top_prime_awards": [],
                "current_opportunities": [],
                "coverage": {},
                "item_and_component_evidence": {
                    "top_items": [],
                    "top_item_supplier_sites": [],
                },
            }
        )
        store._attach_supplier_annual_activity = Mock()

        result = PlatformContextStore.answer_projection(
            store,
            "Next Gen OPIR",
            supplier_limit=10,
        )

        store._attach_supplier_annual_activity.assert_called_once_with(
            "NEXT GEN OPIR",
            result["reported_supplier_sites"],
        )

    def test_comparison_follow_up_retains_both_platforms(self):
        self.assertTrue(
            platform_comparison_follow_up_intent(
                "Which of those suppliers are also important to other US Army aviation programs?"
            )
        )
        self.assertEqual(
            platform_comparison_answer_mode(
                "Which of those suppliers are also important to other US Army aviation programs?"
            ),
            "comparison_cross_program_exposure",
        )

    def test_comparison_conclusion_mode(self):
        self.assertEqual(
            platform_comparison_answer_mode(
                "What is the most commercially interesting conclusion from the overlap?"
            ),
            "comparison_conclusions",
        )

    def test_open_capability_discovery_is_detected(self):
        self.assertTrue(
            is_open_capability_discovery_request(
                "Find US manufacturers with demonstrated experience supplying electrical power generation equipment to military aircraft."
            )
        )

    def test_platform_supplier_commands_outrank_company_word_search(self):
        for question in (
            "Show me F-16 suppliers",
            "Who are the F-16 suppliers?",
            "F-16 suppliers",
            "Suppliers to F-16",
            "List the suppliers for the F-35",
            "Give me F-35 vendors",
            "Which companies support the F-35?",
            "Show me firms involved in F-16",
            "Who builds the F-35?",
            "F-16 production outlook",
            "Map the Stryker supply chain",
            "Tell me about the F-16",
            "How is B-52 modernization progressing?",
            "What does Raytheon provide on AMRAAM?",
            "Which facilities support Tomahawk?",
            "I need the industrial picture for Virginia class.",
            "Who's actually involved with Tomahawk?",
            "Show me the F-16 suply chain.",
        ):
            with self.subTest(question=question):
                self.assertTrue(
                    is_platform_centered_request(
                        question,
                        has_platform_mention=bool(self.store.mentions(question)),
                    )
                )

    def test_minor_platform_name_typo_is_resolved(self):
        self.assertEqual(
            self.store.mentions("Who are the main Tomahwk suppliers?"),
            ["TOMAHAWK"],
        )

    def test_company_platform_evidence_question_is_not_reclassified(self):
        self.assertFalse(
            is_platform_centered_request(
                "What evidence shows L3Harris supplies the F-16?",
                has_platform_mention=True,
            )
        )

    def test_aircraft_braking_discovery_is_detected(self):
        self.assertTrue(
            is_open_capability_discovery_request(
                "Find US manufacturers that supply braking systems or brake components to military aircraft."
            )
        )

    def test_precomputed_context_is_used_for_a_resolved_platform(self):
        with tempfile.TemporaryDirectory() as directory:
            context_path = Path(directory) / "f16.json"
            expected = {"scope": {"platform_id": "F-16"}, "marker": "precomputed"}
            context_path.write_text(json.dumps(expected))
            self.store._cache = {}
            self.store._precomputed_paths = {"F-16": context_path}

            self.assertEqual(self.store.get("F-16"), expected)

    def test_fingerprint_input_ignores_sub_cent_and_list_order_noise(self):
        first = [{"id": "b", "value": 10.0001}, {"id": "a", "value": 2.0}]
        second = [{"id": "a", "value": 2.0}, {"id": "b", "value": 10.0002}]
        self.assertEqual(
            _canonical_fingerprint_value(first),
            _canonical_fingerprint_value(second),
        )

    def test_supplier_annual_activity_reconciles_to_platform_total(self):
        with tempfile.TemporaryDirectory() as directory:
            network = Path(directory) / "network.parquet"
            connection = duckdb.connect()
            connection.execute(
                """
                COPY (
                    SELECT * FROM (VALUES
                        ('73293', 'AMRAAM', 2022, 56673158.0, 56673158.0, 'r1', 'a1'),
                        ('73293', 'AMRAAM', 2023, 33510000.0, 33510000.0, 'r2', 'a2'),
                        ('73293', 'AMRAAM', 2024, 5584500.0, 5584500.0, 'r3', 'a3'),
                        ('73293', 'AMRAAM', 2025, 16600500.0, 16600500.0, 'r4', 'a4'),
                        ('73293', 'AMRAAM', 2026, 1141463.0, 1141463.0, 'r5', 'a5')
                    ) AS t(sub_cage, platform_family, year, subaward_value,
                           subaward_value_raw, source_dedup_key, contract_id)
                ) TO ? (FORMAT PARQUET)
                """,
                [str(network)],
            )
            store = object.__new__(PlatformContextStore)
            store.connection = connection
            store.paths = {"network": network}
            suppliers = [{"cage": "73293"}]

            store._attach_supplier_annual_activity("AMRAAM", suppliers)

            observations = suppliers[0]["annual_reported_subcontract_activity"]
            self.assertIsNone(
                observations[0]["mimir_modelled_reported_subcontract_value_usd"]
            )
            self.assertAlmostEqual(
                sum(
                    row["mimir_modelled_reported_subcontract_value_usd"] or 0
                    for row in observations
                ),
                113509621.0,
            )

    def test_singular_platform_value_question_retains_company_scope(self):
        self.assertTrue(
            company_follow_up_intent(
                "For each platform, show reported value with this company over time."
            )
        )


if __name__ == "__main__":
    unittest.main()
