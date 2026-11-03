import unittest
import duckdb
from programme_parts import build_parts_query
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import Mock
from test_programme_scope_counts import namespace


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

    def test_private_route_authentication_and_reviewed_item_null_money(self):
        import ast,hmac,os
        from typing import Any,Dict
        from fastapi import HTTPException,Request,Response
        from unittest.mock import patch
        from pathlib import Path
        node=next(n for n in ast.parse(Path('main.py').read_text()).body if isinstance(n,ast.FunctionDef)and n.name=='private_programme_parts');node.decorator_list=[]
        ns=dict(Dict=Dict,Any=Any,Request=Request,Response=Response,HTTPException=HTTPException,os=os,hmac=hmac,logger=Mock(),GLOBAL_CACHE={'last_loaded':1,'is_loading':False},
            df_sanitize_for_json=lambda df:df.astype(object).where(df.notna(),None),programme_read_df=lambda sql,params:self.db.execute(sql,params).fetchdf(),
            private_v7_coverage=lambda payload:{'reviewed_items':[{'niin':'014812863','nsn':'5999014812863','item_name':'ADAPTER,TEST,AMRAAM','evidence_count':2}]})
        exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),ns)
        app=FastAPI();app.post('/items')(ns['private_programme_parts']);client=TestClient(app)
        payload={'name':'AMRAAM','years':[2025],'agency':'Navy','include_zero':True,'min_spend':0}
        with patch.dict(os.environ,{'MIMIR_EXPORT_PROXY_SECRET':'fixture-key'}):
            self.assertEqual(client.post('/items',json=payload).status_code,401)
            headers={'X-Mimir-Export-Key':'fixture-key'}
            response=client.post('/items',json=payload,headers=headers);self.assertEqual(response.status_code,200)
            item=next(r for r in response.json()if r['niin']=='014812863')
            self.assertIsNone(item['amount']);self.assertIsNone(item['total_units_sold'])
            self.assertEqual(item['financial_grain'],'ITEM_REFERENCE_ONLY')
            self.assertIn('private, no-store',response.headers['Cache-Control'])
            count=client.post('/items',json={**payload,'count_only':True},headers=headers)
            self.assertEqual(count.json()['count'],len(response.json()))
            self.assertEqual(client.post('/items',json={**payload,'sql':'SELECT 1'},headers=headers).status_code,400)
            ns['GLOBAL_CACHE']['is_loading']=True
            self.assertEqual(client.post('/items',json=payload,headers=headers).status_code,503)

    def test_private_item_history_uses_native_actions_and_preserves_psc_scope(self):
        from private_research_context import item_history
        def read(sql,params):
            c=self.db.execute(sql,params)
            return [dict(zip([v[0]for v in c.description],r))for r in c.fetchall()]
        value=item_history({'nsn':'1234000000001','years':[2025],'agency':'Navy'},read)
        self.assertEqual(value['annual'],[{'label':'2025','spend':80.0}])
        self.assertIsNone(value['recorded_quantity'])
        self.db.execute('ALTER TABLE v_transactions ADD COLUMN psc VARCHAR')
        self.db.execute("UPDATE v_transactions SET psc='1234'")
        self.assertEqual(item_history({'nsn':'1234000000001','years':[2025],'agency':'Navy','psc':'9999'},read)['annual'],[])
        self.assertEqual(item_history({'nsn':'1234000000001','years':[2025],'agency':'Navy','psc':'1234'},read)['annual'],[{'label':'2025','spend':80.0}])

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

    def test_reviewed_award_item_link_is_visible_without_inventing_item_funding(self):
        evidence=[{'niin':'014812863','nsn':'5999014812863','item_name':'ADAPTER,TEST,AMRAAM','evidence_count':2}]
        rows=self.read(agency='Navy',years=[2025],reviewed_items=evidence)
        row=next(r for r in rows if r['niin']=='014812863')
        self.assertEqual(row['nsn'],'5999014812863');self.assertEqual(row['reviewed_award_item_evidence_count'],2)
        self.assertEqual(row['financial_record_count'],0);self.assertIsNone(row['amount']);self.assertIsNone(row['total_units_sold'])
        self.assertEqual(row['matching_item_count'],3)
        self.assertEqual(self.read(agency='Navy',years=[2025],reviewed_items=evidence,count_only=True)[0]['count'],3)
        self.assertTrue(all(r['niin']!='014812863'for r in self.read(agency='Navy',years=[2025],reviewed_items=evidence,min_spend=1)))

    def test_native_funding_is_not_duplicated_when_reviewed_link_matches_existing_item(self):
        rows=self.read(agency='Navy',years=[2025],reviewed_items=[{'niin':'000000001','evidence_count':2}])
        self.assertEqual(len(rows),2);self.assertEqual(rows[0]['amount'],80)

    def test_real_http_routes_return_buyer_filtered_json_and_matching_count(self):
        ns=namespace();ns['duck_fetch_df']=lambda sql,params:self.db.execute(sql,params).fetchdf();ns['logger']=Mock()
        app=FastAPI();app.get('/parts')(ns['get_platform_parts']);app.get('/count')(ns['get_platform_parts_count'])
        client=TestClient(app)
        response=client.get('/parts',params={'name':'AMRAAM','agency':'Navy','years':2025})
        self.assertEqual(response.status_code,200)
        rows=response.json();self.assertEqual(rows[0]['amount'],80);self.assertIsNone(rows[0]['total_units_sold'])
        self.assertEqual(rows[0]['financial_grain'],'NIIN_BUYER_FISCAL_YEAR')
        count=client.get('/count',params={'name':'AMRAAM','agency':'Navy','years':2025})
        self.assertEqual(count.status_code,200);self.assertEqual(count.json()['count'],len(rows))


if __name__=='__main__':unittest.main()
