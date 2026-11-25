import unittest

from market_scope_membership import (Binding, FACT_COLUMNS, MEMBERSHIP_COLUMNS,
                                     NativeMarketSelection)


class PlatformScopeTokenTests(unittest.TestCase):
    def setUp(self):
        binding = Binding('a' * 64, 'b' * 64, 'c' * 64, 'd' * 64)
        self.selector = NativeMarketSelection(
            binding, 'market_scope_financial_facts_test',
            'market_scope_memberships_test', FACT_COLUMNS, MEMBERSHIP_COLUMNS)

    def test_current_canonical_digit_prefixed_platform_ids(self):
        for platform in ('2GEN_FLIR', '3DELRR', '3KW_TQG'):
            with self.subTest(platform=platform):
                sql = self.selector.selected_sql([], [2026], ['PRIME_AWARD'],
                                                 platforms=[platform])
                self.assertIn("m.target_id IN ('" + platform + "')", sql)

    def test_market_token_rules_remain_distinct(self):
        with self.assertRaisesRegex(ValueError, 'Invalid market union'):
            self.selector.selected_sql(['2GEN_FLIR'], [2026], ['PRIME_AWARD'])
        self.assertIn('FIGHTER_AIRCRAFT_FIFTH_GENERATION',
                      self.selector.selected_sql(['FIGHTER_AIRCRAFT_FIFTH_GENERATION'],
                                                 [2026], ['PRIME_AWARD']))

    def test_platform_punctuation_or_lowercase_rejects(self):
        for platform in ('2GEN-FLIR', '2gen_flir', 'x;DROP', ''):
            with self.subTest(platform=platform):
                with self.assertRaisesRegex(ValueError, 'Invalid market union'):
                    self.selector.selected_sql([], [2026], ['PRIME_AWARD'],
                                               platforms=[platform])

    def test_scalar_platform_is_not_split_into_characters(self):
        with self.assertRaisesRegex(ValueError, 'collections'):
            self.selector.selected_sql([], [2026], ['PRIME_AWARD'],
                                       platforms='2GEN_FLIR')


if __name__ == '__main__':
    unittest.main()
