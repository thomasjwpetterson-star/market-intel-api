import ast,hmac,os,unittest
from pathlib import Path
from typing import Any,Dict
from unittest.mock import Mock,patch
import duckdb
from funding_action_history import action_history

class FundingActionHistory(unittest.TestCase):
    def setUp(self):
        self.db=duckdb.connect();self.addCleanup(self.db.close)
        self.db.execute('CREATE TABLE v_transactions(award_key VARCHAR,transaction_key VARCHAR,source_system VARCHAR,contract_id VARCHAR,action_date VARCHAR,year INTEGER,vendor_cage VARCHAR,vendor_name VARCHAR,parent_agency VARCHAR,sub_agency VARCHAR,psc VARCHAR,naics_code VARCHAR,description VARCHAR,platform_family VARCHAR,spend_amount DOUBLE)')
        values=[2142594563.55,-4285803155.10,614028,2144246723.55]
        for i,v in enumerate(values):self.add('KEY'+str(i),'2025-07-15',v)
        for i in range(60):self.add('ZERO'+str(i),'2025-09-30',0)
        self.add('OUTSIDE','2025-07-15',999999,cage='99999')
    def add(self,key,date,value,cage='15090'):
        self.db.execute('INSERT INTO v_transactions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',['NATIVE',key,'USA_SPENDING','TEST',date,2025,cage,'VENDOR','DEPARTMENT OF DEFENSE','DEPARTMENT OF THE NAVY','1410','336414','Description','AMRAAM',value])
    def read(self,sql,params):
        r=self.db.execute(sql,params);return [dict(zip([c[0]for c in r.description],row))for row in r.fetchall()]
    def test_large_positive_modifications_reconcile_with_negative_offset(self):
        r=action_history({'filters':{'cage':'15090','years':[2025]},'limit':6},self.read)
        s=r['scope_summary'];self.assertEqual(s['action_count'],64);self.assertEqual(s['contract_count'],1)
        self.assertAlmostEqual(s['net_obligations_usd'],1652160,places=2)
        self.assertAlmostEqual(s['increases_usd']+s['reductions_usd'],s['net_obligations_usd'],places=2)
        self.assertEqual(len(r['largest_reductions']),1);self.assertEqual(r['largest_reductions'][0]['spend_amount'],-4285803155.10)
        self.assertTrue(all(x['spend_amount']==0for x in r['rows']))
    def test_pages_continue_past_fifty_and_keep_native_transactions(self):
        r=action_history({'filters':{'contract_id':'TEST','cage':'15090','years':[2025]},'offset':60,'limit':10},self.read)
        self.assertEqual(len(r['rows']),4);self.assertEqual(len({x['transaction_key']for x in r['rows']}),4)
        self.assertEqual(r['scope_summary']['action_count'],64)
    def test_reviewed_clear_changes_attribution_without_changing_funding(self):
        from private_platform_reference import Reference
        from private_research_context import wrap_action_read
        ref=Reference.__new__(Reference);ref.registry=[];ref.rows=[{'award_key':'NATIVE','target_entity_ids':''}]
        r=action_history({'filters':{'cage':'15090'},'offset':60,'limit':10},wrap_action_read(ref,self.read))
        self.assertTrue(all(x['platform_family']is None for x in r['rows']))
        self.assertAlmostEqual(r['scope_summary']['net_obligations_usd'],1652160,places=2)
    def test_filters_empty_scope_and_invalid_input(self):
        r=action_history({'filters':{'cage':'15090','agency':'ARMY'}},self.read)
        self.assertEqual(r['rows'],[]);self.assertIsNone(r['scope_summary']['net_obligations_usd'])
        for body in [{'filters':{'years':[2025]}},{'filters':{'cage':'15090'},'sql':'SELECT secret'},{'filters':{'cage':'15090'},'limit':12}]:
            with self.assertRaises(ValueError):action_history(body,self.read)
    def test_private_route_auth_and_generation_guards(self):
        from fastapi import FastAPI,Request,Response,HTTPException
        from fastapi.testclient import TestClient
        node=next(n for n in ast.parse(Path('main.py').read_text()).body if isinstance(n,ast.FunctionDef)and n.name=='private_action_history');node.decorator_list=[]
        state={'last_loaded':1,'is_loading':False,'platform_release':{'components':{'main':{'release_id':'test'}}}}
        ns=dict(Dict=Dict,Any=Any,Request=Request,Response=Response,HTTPException=HTTPException,os=os,hmac=hmac,GLOBAL_CACHE=state,programme_read_df=lambda sql,p:self.db.execute(sql,p).fetchdf(),df_sanitize_for_json=lambda df:df)
        exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),ns)
        app=FastAPI();app.post('/history')(ns['private_action_history']);client=TestClient(app)
        with patch.dict(os.environ,{'MIMIR_EXPORT_PROXY_SECRET':'test-key'}),patch('private_platform_reference.reference_for',return_value=object()),patch('private_research_context.wrap_action_read',side_effect=lambda ref,read:read),patch('private_research_context.metadata',return_value={'mapping_basis':'CURRENT_V7'}):
            body={'filters':{'cage':'15090','years':[2025]}}
            self.assertEqual(client.post('/history',json=body).status_code,401)
            r=client.post('/history',json=body,headers={'X-Mimir-Export-Key':'test-key'})
            self.assertEqual(r.status_code,200,r.text);self.assertEqual(r.json()['scope_summary']['action_count'],64)
            self.assertEqual(r.headers['Cache-Control'],'private, no-store')
            state['is_loading']=True
            self.assertEqual(client.post('/history',json=body,headers={'X-Mimir-Export-Key':'test-key'}).status_code,503)

if __name__=='__main__':unittest.main()
