import unittest
import duckdb
from private_research_context import aggregate
from private_research_query import build_queries


class AdjustedSubcontractResearch(unittest.TestCase):
    def setUp(self):
        self.db=duckdb.connect();self.addCleanup(self.db.close)
        self.db.execute('CREATE TABLE v_subcontracts(year INTEGER,subcontractor_cage VARCHAR,subcontractor_name VARCHAR,prime_cage VARCHAR,prime_name VARCHAR,prime_award_id VARCHAR,psc_code VARCHAR,platform_family VARCHAR,subcontract_value_usd DOUBLE,subcontract_value_raw_usd DOUBLE)')
        self.db.execute("INSERT INTO v_subcontracts VALUES (2024,'19645','DDC','11111','PRIME A','AWARD1','5998','AMRAAM',100,1000),(2024,'19645','DDC','11111','PRIME A','AWARD1','5998','AMRAAM',0,1000),(2025,'19645','DDC','22222','PRIME B','AWARD2','5998','JASSM',75,75),(2025,'19645','DDC','22222','PRIME B','AWARD2','5998',NULL,-5,-5),(2025,'99999','OTHER','22222','PRIME B','AWARD2','5998','JASSM',900,900)")
    def read(self,sql,params):
        c=self.db.execute(sql,params);return [dict(zip([x[0] for x in c.description],r)) for r in c.fetchall()]
    def run_query(self,**kwargs):
        return aggregate({'dataset':'reported_subcontracts','filters':{'cage':'19645'},**kwargs},object(),self.read)
    def test_full_year_totals_use_existing_adjusted_values_not_raw_reports_or_prime_obligations(self):
        result=self.run_query(group_by=['year'])
        self.assertEqual(result['reported_subcontract_value_usd'],170)
        self.assertEqual(result['groups'],[{'year':2024,'reported_subcontract_value_usd':100},{'year':2025,'reported_subcontract_value_usd':70}])
        self.assertNotIn('observed_obligations_usd',result)
        self.assertEqual(result['measure'],'platform_adjusted_reported_subcontract_value')
        self.assertEqual(result['source_row_count'],4)
    def test_prime_customer_and_award_filters_intersect_exact_site_and_year(self):
        result=self.run_query(filters={'cage':'19645','prime_cage':'22222','years':[2025],'contract_id':'AWARD2'},group_by=['prime_company'])
        self.assertEqual(result['reported_subcontract_value_usd'],70)
        self.assertEqual(result['groups'],[{'prime_company':'PRIME B','reported_subcontract_value_usd':70}])
    def test_programme_label_is_not_promoted_to_verified_v7_component_membership(self):
        result=self.run_query(group_by=['program'],limit=1)
        self.assertEqual(result['programme_mapping_basis'],'SOURCE_REPORTED_SUBCONTRACT_LABELS')
        self.assertFalse(result['parent_award_cascade_enabled'])
        self.assertNotIn('mapping_basis',result)
        self.assertTrue(result['groups_truncated'])
        following=self.run_query(group_by=['program'],limit=1,offset=1)
        self.assertEqual(following['reported_subcontract_value_usd'],result['reported_subcontract_value_usd'])
        self.assertNotEqual(following['groups'],result['groups'])
    def test_missing_records_stay_missing_and_unavailable_dimensions_do_not_reach_sql(self):
        result=self.run_query(filters={'cage':'ABCDE'})
        self.assertIsNone(result['reported_subcontract_value_usd'])
        for args in [{'group_by':['agency']},{'filters':{'cage':'19645','parent':'DDC'}},{'filters':{'cage':'19645','prime_cage':"' OR 1=1"}}]:
            with self.assertRaises(ValueError):build_queries({'dataset':'reported_subcontracts','filters':{'cage':'19645'},**args})
