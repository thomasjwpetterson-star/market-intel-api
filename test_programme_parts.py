import unittest
import duckdb
from programme_parts import build_parts_query


class BuyerItemFunding(unittest.TestCase):
    def setUp(self):
        self.db=duckdb.connect();self.addCleanup(self.db.close)
        self.db.execute('CREATE TABLE v_platform_bom(niin VARCHAR,platform_family VARCHAR)')
        self.db.execute("INSERT INTO v_platform_bom VALUES ('000000001','AMRAAM'),('000000001','AMRAAM'),('000000001','F-35'),('000000002','AMRAAM'),('000000003','AMRAAM'),('000000004','AMRAAM')")
        self.db.execute('CREATE TABLE v_nsn_profile_lookup(niin VARCHAR,nsn VARCHAR,item_name VARCHAR,fsc_code VARCHAR)')
        self.db.execute("INSERT INTO v_nsn_profile_lookup VALUES ('000000001','1234000000001','ITEM A','1234'),('000000001','1234000000001','ITEM A','1234'),('000000002','1234000000002','ITEM B','1234'),('000000003','1234000000003','ITEM C','1234')")
        self.db.execute('''CREATE TABLE v_transactions(niin VARCHAR,nsn VARCHAR,year INTEGER,vendor_cage VARCHAR,vendor_name VARCHAR,
            sub_agency VARCHAR,parent_agency VARCHAR,market_segment VARCHAR,spend_amount DOUBLE,action_date VARCHAR,platform_count INTEGER)''')
        for niin,year,cage,buyer,value in [('000000001',2025,'11111','DEPARTMENT OF THE NAVY',100),('000000001',2025,'11111','DEPARTMENT OF THE NAVY',-20),('000000001',2025,'22222','DEPARTMENT OF THE AIR FORCE',200),('000000001',2025,'33333','DEFENSE LOGISTICS AGENCY',10),('000000001',2024,'44444','DEPARTMENT OF THE NAVY',40),('000000002',2025,'11111','DEPARTMENT OF THE NAVY',0),('000000003',2025,'22222','DEPARTMENT OF THE AIR FORCE',-50),('000000999',2025,'11111','DEPARTMENT OF THE NAVY',999)]:
            self.db.execute('INSERT INTO v_transactions VALUES (?,?,?,?,?,?,?,?,?,?,?)',[niin,'1234'+niin,year,cage,'SUPPLIER '+cage,buyer,'DEPARTMENT OF DEFENSE','AIR',value,str(year)+'-01-01',2])
        self.db.execute('''CREATE TABLE v_nsn_supplier_lookup(niin VARCHAR,year INTEGER,cage VARCHAR,vendor VARCHAR,
            sub_agency VARCHAR,parent_agency VARCHAR,market_segment VARCHAR,total_revenue DOUBLE,total_units_sold DOUBLE,last_sold VARCHAR,platform_count INTEGER)''')
        self.db.execute("INSERT INTO v_nsn_supplier_lookup VALUES ('000000001',2020,'33333','OLD SUPPLIER','DEFENSE LOGISTICS AGENCY','DEPARTMENT OF DEFENSE','AIR',7,1,'2020-01-01',2),('000000001',2025,'33333','DUPLICATE HISTORY','DEFENSE LOGISTICS AGENCY','DEPARTMENT OF DEFENSE','AIR',10000,100,'2025-01-01',2)")

    def read(self,**kwargs):
        sql,params=build_parts_query('AMRAAM',**kwargs)
        cursor=self.db.execute(sql,params)
        return [dict(zip([c[0] for c in cursor.description],r)) for r in cursor.fetchall()]

    def test_buyers_get_different_real_values_and_matching_items(self):
        navy=self.read(agency='Navy',years=[2025]);air=self.read(agency='Air Force',years=[2025])
        self.assertEqual([(r['niin'],r['amount']) for r in navy],[('000000001',80),('000000002',0)])
        self.assertEqual([(r['niin'],r['amount']) for r in air],[('000000001',200),('000000003',-50)])
        self.assertEqual(navy[0]['top_vendor'],'11111');self.assertEqual(air[0]['top_vendor'],'22222')

    def test_buyer_year_and_department_filters_intersect(self):
        self.assertEqual(self.read(agency='Navy',years=[2024])[0]['amount'],40)
        self.assertEqual(sum(r['amount'] for r in self.read(agency='DoD',years=[2025])),240)
        self.assertEqual(self.read(agency='DLA',years=[2025])[0]['amount'],10)

    def test_empty_matching_scope_is_a_valid_empty_result(self):
        self.assertEqual(self.read(agency='Navy',years=[2026]),[])
        self.assertEqual(self.read(agency='Navy',years=[2026],count_only=True),[{'count':0,'buyer_history_start_fy':2024}])

    def test_no_financial_multiplication_from_duplicate_bom_or_profiles(self):
        rows=self.read()
        self.assertEqual(len(rows),4)
        self.assertEqual(rows[0]['amount'],337)

    def test_historical_history_preserved_without_adding_cached_years_twice(self):
        self.assertEqual(self.read(agency='DLA',years=[2020])[0]['amount'],7)
        self.assertEqual(self.read(agency='DLA',years=[2025])[0]['amount'],10)

    def test_counts_match_filtered_pages_and_keep_negative_reductions(self):
        for options in [{},{'agency':'Navy','years':[2025]},{'agency':'Air Force','years':[2025]},{'include_zero':False}]:
            self.assertEqual(self.read(count_only=True,**options)[0]['count'],len(self.read(**options)))
        self.assertEqual([r['amount'] for r in self.read(include_zero=False)],[337,-50])

    def test_quantities_are_unknown_when_actions_have_no_quantity(self):
        self.assertIsNone(self.read(agency='Navy',years=[2025])[0]['total_units_sold'])
        self.assertEqual(self.read(agency='DLA',years=[2020])[0]['total_units_sold'],1)

    def test_year_trends_reconcile_to_item_total(self):
        row=self.read()[0]
        self.assertEqual(sum(float(bit.split(':')[1]) for bit in row['annual_revenue_trend'].split('|')),row['amount'])

    def test_pagination_and_scope_are_stable(self):
        first=self.read(agency='Air Force',years=[2025],limit=1)
        second=self.read(agency='Air Force',years=[2025],limit=1,offset=1)
        self.assertEqual(first[0]['niin'],'000000001');self.assertEqual(second[0]['niin'],'000000003')
        self.assertEqual(first[0]['matching_item_count'],2);self.assertEqual(second[0]['matching_item_count'],2)

    def test_buyer_totals_reconcile_to_department_total(self):
        by_buyer=sum(sum(r['amount'] for r in self.read(agency=buyer)) for buyer in ['Navy','Air Force','DLA'])
        self.assertEqual(by_buyer,sum(r['amount'] for r in self.read(agency='DoD')))

    def test_negative_minimum_is_applied_without_discarding_all_reductions(self):
        self.assertEqual([r['amount'] for r in self.read(include_zero=False,min_spend=-20)],[337])

    def test_filter_values_remain_parameters(self):
        sql,params=build_parts_query("AMRAAM' OR 1=1 --",agency="NAVY' OR 1=1 --")
        self.assertNotIn("OR 1=1 --",sql);self.assertIn("AMRAAM' OR 1=1 --",params)
        self.assertEqual(self.read(agency="NAVY' OR 1=1 --"),[])


if __name__=='__main__':unittest.main()
