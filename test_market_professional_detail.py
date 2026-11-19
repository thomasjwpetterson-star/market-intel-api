import unittest

from market_professional_detail import query_for


class MarketNativeDetailTests(unittest.TestCase):
    def setUp(self):
        self.request = {
            'view': 'contract-actions', 'years': [2025, 2026],
            'nodes': ['US_COUNTER_UAS'],
            'table': 'market_professional_detail_' + 'a' * 20,
            'award': 'CONT_AWD_ABC_9700_-NONE-_-NONE-',
            'nativeDetail': {
                'factTable': 'market_native_financial_facts_' + 'a' * 20,
                'stockTable': 'market_native_stock_links_' + 'a' * 20,
                'inventorySha256': 'b' * 64,
            },
        }

    def test_action_query_keeps_native_grain_and_signed_amounts(self):
        sql, suffix = query_for(self.request)
        self.assertEqual(suffix, 'a' * 20)
        self.assertIn('native_record_id AS id, SUM(net_usd) AS net', sql)
        self.assertIn("source_record_id='CONT_AWD_ABC_9700_-NONE-_-NONE-'", sql)
        self.assertIn('LEFT JOIN stock ON actions.id=stock.id', sql)
        self.assertIn('ORDER BY ABS(net) DESC, actions.id ASC', sql)

    def test_cross_release_or_unsafe_native_selection_is_rejected(self):
        self.request['nativeDetail']['factTable'] = 'market_native_financial_facts_' + 'c' * 20
        with self.assertRaisesRegex(ValueError, 'do not match'):
            query_for(self.request)
        self.request['nativeDetail']['factTable'] = 'market_native_financial_facts_' + 'a' * 20
        self.request['award'] = "ABC' OR 1=1"
        with self.assertRaisesRegex(ValueError, 'Invalid native'):
            query_for(self.request)

    def test_existing_contract_view_still_uses_reviewed_fact_table(self):
        self.request['view'] = 'contracts'
        sql, _ = query_for(self.request)
        self.assertIn('market_professional_detail_' + 'a' * 20, sql)
        self.assertNotIn('market_native_financial_facts_', sql)


if __name__ == '__main__':
    unittest.main()
