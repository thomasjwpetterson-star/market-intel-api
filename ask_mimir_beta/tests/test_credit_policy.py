import unittest

from credit_policy import quote_ask_work


class CreditPolicyTests(unittest.TestCase):
    def test_each_mode_has_a_focused_and_costlier_work_class(self):
        focused = quote_ask_work(
            answer_mode="conversation", workflow="platform_intelligence",
            conversation_segment="supply_chain",
        )
        broad = quote_ask_work(
            answer_mode="conversation", workflow="platform_intelligence",
        )
        standard = quote_ask_work(
            answer_mode="deep_research", workflow="platform_intelligence",
        )
        extended = quote_ask_work(
            answer_mode="deep_research", workflow="platform_comparison",
        )
        self.assertEqual((focused.units, broad.units, standard.units, extended.units),
                         (1, 2, 3, 5))

    def test_live_check_is_quoted_before_the_job(self):
        self.assertEqual(quote_ask_work(
            answer_mode="conversation", workflow="opportunity_discovery",
            live_check=True,
        ).units, 3)
        self.assertEqual(quote_ask_work(
            answer_mode="conversation", workflow="opportunity_discovery",
        ).units, 2)
        self.assertEqual(quote_ask_work(
            answer_mode="deep_research", workflow="platform_intelligence",
            live_check=True,
        ).units, 5)

    def test_menu_is_free_and_mode_validated(self):
        self.assertEqual(quote_ask_work(
            answer_mode="conversation", workflow="platform_intelligence",
            menu_only=True,
        ).units, 0)
        with self.assertRaises(ValueError):
            quote_ask_work(answer_mode="deep_research", workflow="platform_intelligence",
                           menu_only=True)
