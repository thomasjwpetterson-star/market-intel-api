import ast
import hmac
import os
import unittest
from pathlib import Path
from typing import Any,Dict
from unittest.mock import patch

import duckdb
from fastapi import FastAPI,HTTPException,Request,Response
from fastapi.testclient import TestClient

from private_research_query import build_queries,run_aggregate

class ResearchQueries(unittest.TestCase):
    def setUp(self):
        self.db=duckdb.connect();self.addCleanup(self.db.close)
        self.db.execute('CREATE TABLE v_summary(year INTEGER,cage_code VARCHAR,clean_parent VARCHAR,sub_agency VARCHAR,psc_code VARCHAR,platform_family VARCHAR,total_spend DOUBLE)')
        self.db.execute("INSERT INTO v_summary VALUES (2025,'11111','A','Navy','5840','Radar',100),(2025,'22222','B','Army','5840','Radar',200),(2025,'33333','C','NASA','5840','Radar',300),(2025,'11111','A','Air Force','5840','Radar',50),(2025,'11111','A','Navy','5840','Radar',-10)")
        self.db.execute('CREATE TABLE v_transactions(year INTEGER,vendor_cage VARCHAR,vendor_name VARCHAR,parent_agency VARCHAR,sub_agency VARCHAR,psc VARCHAR,platform_family VARCHAR,contract_id VARCHAR,spend_amount DOUBLE)')
        self.db.execute("INSERT INTO v_transactions VALUES (2025,'11111','A','DEPARTMENT OF DEFENSE','Navy','5840','Radar','ONE',100),(2025,'11111','A','DEPARTMENT OF DEFENSE','Navy','5840','Radar','ONE',-10),(2025,'22222','B','Department of Defense','Army','5840','Radar','TWO',200),(2025,'11111','A','Department of Defense','Air Force','5840','Radar','THREE',50),(2025,'33333','C','NASA','NASA','5840','Radar','FOUR',300)")
        self.db.execute('CREATE TABLE v_contracts_rolled AS SELECT DISTINCT parent_agency,sub_agency FROM v_transactions')

    def read(self,sql,params):
        cursor=self.db.execute(sql,params)
        fields=[x[0] for x in cursor.description]
        return [dict(zip(fields,row)) for row in cursor.fetchall()]

    def test_department_resolves_children_without_multiplying_rows(self):
        value=run_aggregate({'filters':{'years':[2025],'department':'DoD','psc':'5840'}},self.read)
        self.assertEqual(value['observed_obligations_usd'],340)
        self.assertEqual(value['source_row_count'],4)
        self.assertEqual(value['groups'],[{'year':2025,'observed_obligations_usd':340}])

    def test_all_buyer_scope_remains_distinct(self):
        self.assertEqual(run_aggregate({'filters':{'years':[2025],'psc':'5840'}},self.read)['observed_obligations_usd'],640)

    def test_exact_buyer_and_department_intersect(self):
        value=run_aggregate({'filters':{'years':[2025],'department':'DoD','agency':'Navy'}},self.read)
        self.assertEqual(value['observed_obligations_usd'],90)

    def test_signed_action_aggregate_and_contract_identity(self):
        value=run_aggregate({'dataset':'contract_actions','filters':{'contract_id':'ONE'},'group_by':['year']},self.read)
        self.assertEqual(value['observed_obligations_usd'],90)
        self.assertEqual(value['company_grouping'],'recorded_vendor')

    def test_missing_series_is_null(self):
        value=run_aggregate({'filters':{'years':[2024]}},self.read)
        self.assertIsNone(value['observed_obligations_usd'])
        self.assertEqual(value['groups'],[])

    def test_industry_filter_and_grouping_use_recorded_naics(self):
        self.db.execute('ALTER TABLE v_summary ADD COLUMN naics_code VARCHAR')
        self.db.execute("UPDATE v_summary SET naics_code='336411'")
        value=run_aggregate({'filters':{'years':[2025],'naics':'336411'},'group_by':['naics']},self.read)
        self.assertEqual(value['groups'],[{'naics':'336411','observed_obligations_usd':640}])

    def test_group_paging_keeps_whole_scope_total(self):
        args={'filters':{'years':[2025],'department':'DoD'},'group_by':['company'],'limit':1}
        first=run_aggregate(args,self.read);second=run_aggregate({**args,'offset':1},self.read)
        self.assertEqual(first['groups'][0]['company'],'B');self.assertEqual(second['groups'][0]['company'],'A')
        self.assertTrue(first['groups_truncated']);self.assertFalse(second['groups_truncated'])
        self.assertEqual(first['observed_obligations_usd'],second['observed_obligations_usd'])

    def test_year_duplicates_do_not_duplicate_funding(self):
        self.assertEqual(run_aggregate({'filters':{'years':[2025,2025]}},self.read)['observed_obligations_usd'],640)

    def test_values_are_bound_parameters(self):
        query=build_queries({'filters':{'agency':"Navy' OR 1=1 --"}})
        self.assertNotIn('OR 1=1',query['total_sql'])
        self.assertIn("NAVY' OR 1=1 --",query['total_params'])
        self.assertIsNone(run_aggregate({'filters':{'agency':"Navy' OR 1=1 --"}},self.read)['observed_obligations_usd'])

    def test_schema_rejects_sql_tables_identifiers_and_unsupported_filters(self):
        invalid=[{'sql':'DROP TABLE v_summary'},{'dataset':'auth.users','filters':{'years':[2025]}},{'dataset':[],'filters':{'years':[2025]}},{'group_by':['year; DROP TABLE x'],'filters':{'years':[2025]}},{'group_by':[{}],'filters':{'years':[2025]}},{'filters':{}},{'filters':{'years':[True]}},{'filters':{'years':[2025],'unknown':'x'}},{'filters':{'cage':'../db'}},{'filters':{'years':[2025]},'offset':-1},{'filters':{'years':[2025]},'limit':5000},{'dataset':'contract_actions','filters':{'parent':'A'}}]
        for value in invalid:
            with self.subTest(value=value),self.assertRaises(ValueError):build_queries(value)

    def route_client(self):
        tree=ast.parse(Path(__file__).with_name('main.py').read_text())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='private_research_aggregate');node.decorator_list=[]
        ns={'Dict':Dict,'Any':Any,'Request':Request,'Response':Response,'HTTPException':HTTPException,'os':os,'hmac':hmac,'GLOBAL_CACHE':{'last_loaded':1,'is_loading':False},'duck_fetch_df':lambda sql,params:self.db.execute(sql,params).fetchdf(),'df_sanitize_for_json':lambda df:df}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),ns)
        app=FastAPI();app.post('/api/research/aggregate')(ns['private_research_aggregate'])
        return TestClient(app),ns

    def test_proxy_auth_is_required_and_response_is_private(self):
        client,_=self.route_client();body={'filters':{'years':[2025]}}
        with patch.dict(os.environ,{'MIMIR_EXPORT_PROXY_SECRET':'fixture-proxy-key'}):
            self.assertEqual(client.post('/api/research/aggregate',json=body).status_code,401)
            self.assertEqual(client.post('/api/research/aggregate',json=body,headers={'X-Mimir-Export-Key':'wrong'}).status_code,401)
            response=client.post('/api/research/aggregate',json=body,headers={'X-Mimir-Export-Key':'fixture-proxy-key'})
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.json()['observed_obligations_usd'],640)
        self.assertEqual(response.json()['source_generation'],'1')
        self.assertEqual(response.headers['cache-control'],'private, no-store')

    def test_data_generation_change_rejects_combined_response(self):
        client,ns=self.route_client();original=ns['duck_fetch_df']
        def changing(sql,params):
            value=original(sql,params);ns['GLOBAL_CACHE']['last_loaded']=2;return value
        ns['duck_fetch_df']=changing
        with patch.dict(os.environ,{'MIMIR_EXPORT_PROXY_SECRET':'fixture-proxy-key'}):
            response=client.post('/api/research/aggregate',json={'filters':{'years':[2025]}},headers={'X-Mimir-Export-Key':'fixture-proxy-key'})
        self.assertEqual(response.status_code,503)

if __name__=='__main__':unittest.main()
