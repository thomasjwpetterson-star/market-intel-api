import unittest
import test_market_scope_membership as fixtures

class FilterContainerTests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixtures.MembershipTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def test_scalar_and_mapping_filters_fail_closed(self):
        for field in ('markets','years','sources','platforms'):
            for invalid in ('US',b'US',{'US': True},None):
                args=dict(markets=['US'],years=[2026],sources=['PRIME_AWARD'],platforms=())
                args[field]=invalid
                with self.subTest(field=field,value=invalid),self.assertRaises(ValueError):
                    self.fixture.selector.selected_sql(**args)

    def test_supported_collections_keep_exact_money_and_platform_selection(self):
        for kind in (list,tuple,set,frozenset):
            sql=self.fixture.selector.selected_sql(kind(['US']),kind([2026]),kind(['PRIME_AWARD']),platforms=kind([]))
            self.assertEqual(self.fixture.db.execute('SELECT sum(net_usd) FROM ('+sql+')').fetchone()[0],200)
            sql=self.fixture.selector.selected_sql(kind([]),kind([2026]),kind(['PRIME_AWARD']),platforms=kind([fixtures.F35]))
            self.assertEqual(self.fixture.db.execute('SELECT sum(net_usd) FROM ('+sql+')').fetchone()[0],100)
