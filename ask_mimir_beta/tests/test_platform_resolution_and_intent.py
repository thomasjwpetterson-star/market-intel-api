import unittest

from platform_context import PlatformContextStore
from platform_intent import platform_answer_mode, platform_follow_up_intent


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

    def test_alternative_source_question_has_a_distinct_mode(self):
        self.assertEqual(
            platform_answer_mode("Which suppliers have the fewest alternative sources?"),
            "supplier_source_depth",
        )


if __name__ == "__main__":
    unittest.main()
