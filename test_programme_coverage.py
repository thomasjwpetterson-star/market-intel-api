import ast
import hmac
import os
import unittest
from pathlib import Path
from typing import Any, Dict
from unittest.mock import Mock, patch

import duckdb
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.testclient import TestClient
from programme_coverage import run_coverage
from research_descriptions import description_predicate


class ProgrammeCoverage(unittest.TestCase):
    def setUp(self):
        self.db=duckdb.connect();self.addCleanup(self.db.close)
        self.db.execute('CREATE TABLE v_platform_bom(niin VARCHAR,platform_family VARCHAR)')
        self.db.execute("INSERT INTO v_platform_bom VALUES ('000000001','AMRAAM'),('000000001','AMRAAM'),('000000002','AMRAAM')")
        self.db.execute('''CREATE TABLE v_transactions(year INTEGER,source_system VARCHAR,sub_agency VARCHAR,parent_agency VARCHAR,
            platform_family VARCHAR,niin VARCHAR,nsn VARCHAR,description VARCHAR,base_award_description VARCHAR,action_description VARCHAR,spend_amount DOUBLE)''')
        rows=[(2021,'Air Force',None,None,'Old action',0),(2025,'Air Force','AMRAAM',None,'Missile production',1000000000),
              (2025,'Air Force','AMRAAM',None,'Reduction',-25),(2025,'Navy',None,None,'ADAPTER,TEST,AMRAAM',25),
              (2025,'Navy',None,None,'AIM-120 test support',30),(2025,'Navy','F/A-18','000000999','Aircraft part',100),
              (2025,'Defense Logistics Agency','AMRAAM','000000001','Stock item',10),
              (2025,'Defense Logistics Agency','F-16','000000002','Shared stock item',20),
              (2025,'Army','AMRAAM','000000999','Item outside reference',50),
              (2024,'Navy','AMRAAM','000000001','Earlier stock item',4),(2025,'Air Force','F-35',None,'Aircraft production',200)]
        for year,buyer,programme,item,description,value in rows:
            agency='DEFENSE LOGISTICS AGENCY' if buyer=='Defense Logistics Agency' else 'DEPARTMENT OF THE '+buyer.upper()
            self.db.execute('INSERT INTO v_transactions VALUES (?,?,?,?,?,?,?,?,?,?,?)',[year,'DLA' if buyer=='Defense Logistics Agency' else 'USA_SPENDING',agency,'DEPARTMENT OF DEFENSE',programme,item,'1234'+item if item else None,description,None,None,value])
        self.columns={r[0] for r in self.db.execute('DESCRIBE v_transactions').fetchall()}

    def read(self,sql,params):
        result=self.db.execute(sql,params)
        return [dict(zip([c[0] for c in result.description],r)) for r in result.fetchall()]

    def coverage(self,**kw):
        return run_coverage({'program':'AMRAAM','years':[2025],**kw},self.columns,self.read)

    def test_prime_funding_without_stock_numbers_is_explained(self):
        r=self.coverage(agency='Air Force')
        self.assertEqual(r['reason'],'programme_actions_have_no_item_numbers')
        self.assertEqual(r['selected_scope']['programme_action_count'],2)
        self.assertEqual(r['selected_scope']['programme_net_obligations_usd'],999999975)
        self.assertEqual(r['selected_scope']['programme_actions_with_item_number'],0)

    def test_unmapped_navy_description_matches_are_not_added_to_programme_total(self):
        r=self.coverage(agency='Navy');s=r['selected_scope']
        self.assertEqual(r['reason'],'description_matches_outside_primary_programme_mapping')
        self.assertEqual(s['programme_action_count'],0);self.assertEqual(s['programme_net_obligations_usd'],0)
        self.assertEqual(s['unmapped_description_action_count'],2)
        self.assertEqual(s['description_candidate_net_obligations_usd'],55)
        self.assertTrue(any(b['agency']=='DEPARTMENT OF THE AIR FORCE' for b in r['programme_recorded_agencies']))

    def test_missing_crosswalk_is_distinguished_from_missing_item_identifier(self):
        r=self.coverage(agency='Army')
        self.assertEqual(r['reason'],'programme_item_numbers_not_in_reference_crosswalk')
        self.assertEqual(r['selected_scope']['programme_actions_with_item_number'],1)

    def test_other_programme_candidates_are_not_described_as_primary_programme_matches(self):
        self.db.execute("INSERT INTO v_transactions SELECT 2025,'USA_SPENDING','DEPARTMENT OF THE NAVY','DEPARTMENT OF DEFENSE','OTHER MISSILE',NULL,NULL,'AMRAAM shared support',NULL,NULL,5")
        r=self.coverage(agency='Navy');s=r['selected_scope']
        self.assertEqual(s['description_candidate_action_count'],3)
        self.assertEqual(s['unmapped_description_action_count'],2)
        self.assertEqual(s['description_candidates_with_other_programme_mapping'],1)
        self.assertEqual(s['programme_action_count'],0)
        self.assertIn('Neither subset is already included',r['description_candidate_interpretation'])

    def test_duplicate_reference_rows_do_not_multiply_funding(self):
        r=self.coverage(agency='DLA');s=r['selected_scope']
        self.assertEqual(r['reference_item_count'],2)
        self.assertEqual(s['reference_linked_item_count'],2)
        self.assertEqual(s['programme_net_obligations_usd'],10)

    def test_buyer_and_year_filters_intersect_and_parent_scope_avoids_distinct_sum(self):
        r=self.coverage(agency='Navy',years=[2024])
        self.assertEqual(r['selected_scope']['programme_net_obligations_usd'],4)
        self.assertEqual(r['selected_scope']['description_candidate_action_count'],0)
        self.assertNotIn('reference_linked_item_count',self.coverage(agency='DoD')['selected_scope'])

    def test_empty_and_pre_cache_scopes_are_distinct(self):
        r=self.coverage(agency='Unknown agency',years=[2025])
        self.assertEqual(r['reason'],'no_primary_programme_actions_for_recorded_agency')
        self.assertEqual(r['selected_scope']['buyer_action_count'],0)
        r=self.coverage(agency='Navy',years=[2020])
        self.assertEqual(r['reason'],'requested_years_outside_cached_action_range')
        self.assertEqual(self.coverage(agency='Navy',years=[2026])['reason'],'requested_years_outside_cached_action_range')

    def test_identifier_gap_also_resolves_for_other_programmes(self):
        r=self.coverage(program='F-35',agency='Air Force')
        self.assertEqual(r['reason'],'programme_actions_have_no_item_numbers')
        self.assertEqual(r['programme_context']['programme'],'F-35')
        self.assertTrue(r['programme_context']['sources'])

    def test_sourced_joint_programme_context_has_no_invented_financial_split(self):
        context=self.coverage()['programme_context']
        self.assertEqual(context['source']['url'],'https://www.navair.navy.mil/product/AMRAAM')
        self.assertIn('lead service',context['facts'][0])
        self.assertNotIn('funding_split',context)

    def test_description_terms_are_literal_and_parameterized(self):
        sql,params=description_predicate(["AMRAAM' OR 1=1 --",'100%_test'])
        self.assertNotIn('OR 1=1',sql)
        self.assertIn('%100\\%\\_TEST%',params)
        self.assertEqual(self.read('SELECT COUNT(*) AS count FROM v_transactions WHERE '+sql,params)[0]['count'],0)

    def route_client(self):
        node=next(n for n in ast.parse(Path('main.py').read_text()).body if isinstance(n,ast.FunctionDef) and n.name=='private_programme_coverage');node.decorator_list=[]
        ns=dict(Dict=Dict,Any=Any,Request=Request,Response=Response,HTTPException=HTTPException,os=os,hmac=hmac,logger=Mock(),GLOBAL_CACHE={'last_loaded':1,'is_loading':False},get_duck_table_columns=lambda _:self.columns,duck_fetch_df=lambda sql,params:self.db.execute(sql,params).fetchdf(),df_sanitize_for_json=lambda df:df)
        exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),ns)
        ns['programme_read_df']=lambda sql,params:ns['duck_fetch_df'](sql,params)
        ns['private_v7_coverage']=lambda payload:None
        app=FastAPI();app.post('/coverage')(ns['private_programme_coverage'])
        return TestClient(app),ns

    def test_summary_route_uses_current_v7_counts_without_full_description_scan(self):
        client,ns=self.route_client()
        ns['private_v7_coverage']=lambda p:{'effective_programme_scope':{'action_count':22,'actions_with_native_item_number':0,'source_net_obligations_usd':4420542.77},'reviewed_award_count':7}
        ns['programme_read_df']=Mock(side_effect=AssertionError('Full description scan should not run'))
        with patch.dict(os.environ,{'MIMIR_EXPORT_PROXY_SECRET':'fixture-key'}):
            r=client.post('/coverage',json={'program':'AMRAAM','agency':'Navy','years':[2025],'mode':'summary'},headers={'X-Mimir-Export-Key':'fixture-key'})
            self.assertEqual(r.status_code,200);self.assertEqual(r.json()['selected_scope']['programme_action_count'],22)
            self.assertEqual(r.json()['count_basis'],'CURRENT_V7_ASSOCIATED_ACTIONS')
            self.assertFalse(r.json()['description_candidates_evaluated'])
            ns['programme_read_df'].assert_not_called()

    def test_route_requires_existing_proxy_key_and_retains_private_cache_control(self):
        client,ns=self.route_client()
        from programme_read_cache import ProgrammeReadCache
        cache=ProgrammeReadCache(); reads=Mock(side_effect=ns['duck_fetch_df'])
        ns['programme_read_df']=lambda sql,params:cache.read(ns['GLOBAL_CACHE']['last_loaded'],sql,params,lambda:reads(sql,params))
        with patch.dict(os.environ,{'MIMIR_EXPORT_PROXY_SECRET':'fixture-only-key'}):
            self.assertEqual(client.post('/coverage',json={'program':'AMRAAM'}).status_code,401)
            self.assertEqual(client.post('/coverage',json={'program':'AMRAAM'},headers={'X-Mimir-Export-Key':'wrong'}).status_code,401)
            r=client.post('/coverage',json={'program':'AMRAAM','agency':'Navy','years':[2025]},headers={'X-Mimir-Export-Key':'fixture-only-key'})
            self.assertEqual(r.status_code,200);self.assertEqual(r.json()['source_generation'],'1')
            self.assertIn('private, no-store',r.headers['Cache-Control'])
            payload={'program':'AMRAAM','agency':'Navy','years':[2025]}
            self.assertEqual(client.post('/coverage',json=payload,headers={'X-Mimir-Export-Key':'fixture-only-key'}).status_code,200)
            self.assertEqual(reads.call_count,1)
            # A warm analytical result never bypasses proxy authentication.
            self.assertEqual(client.post('/coverage',json=payload).status_code,401)
            self.assertEqual(reads.call_count,1)
            ns['GLOBAL_CACHE']['is_loading']=True
            self.assertEqual(client.post('/coverage',json={'program':'AMRAAM'},headers={'X-Mimir-Export-Key':'fixture-only-key'}).status_code,503)


if __name__=='__main__':unittest.main()
