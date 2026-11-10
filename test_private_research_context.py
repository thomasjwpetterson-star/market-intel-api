import unittest
from types import SimpleNamespace
import duckdb
from private_research_context import aggregate,wrap_action_read,wrap_item_read,search_programmes
from private_platform_reference import Reference

class CurrentV7Scopes(unittest.TestCase):
    def setUp(self):
        self.db=duckdb.connect();self.addCleanup(self.db.close)
        r=Reference.__new__(Reference);r.registry=[{'canonical_entity_id':'AMRAAM','canonical_label':'AMRAAM'},{'canonical_entity_id':'OTHER','canonical_label':'OTHER'}];r.aliases=[];r.manifest={'release_id':'v7-test'}
        r.rows=[{'award_key':'navy-reviewed','target_entity_ids':'AMRAAM'},{'award_key':'cleared','target_entity_ids':''},{'award_key':'shared','target_entity_ids':'AMRAAM | OTHER'}];r.items=[{'niin':'015006962','canonical_entity_id':'AMRAAM'}];self.ref=r
        self.db.execute('CREATE TABLE v_transactions(award_key VARCHAR,source_system VARCHAR,contract_id VARCHAR,year INTEGER,sub_agency VARCHAR,parent_agency VARCHAR,platform_family VARCHAR,vendor_cage VARCHAR,vendor_name VARCHAR,spend_amount DOUBLE,psc VARCHAR,naics_code VARCHAR,market_segment VARCHAR)')
        self.db.execute("INSERT INTO v_transactions VALUES ('af','USA_SPENDING','ONE',2025,'DEPARTMENT OF THE AIR FORCE','DEPARTMENT OF DEFENSE','AMRAAM','11111','VENDOR',100,'1410','336414','AIR'),('af','USA_SPENDING','ONE',2025,'DEFENSE CONTRACT MANAGEMENT AGENCY','DEPARTMENT OF DEFENSE','AMRAAM','11111','VENDOR',-10,'1410','336414','AIR'),('navy-reviewed','USA_SPENDING','TWO',2025,'DEPARTMENT OF THE NAVY','DEPARTMENT OF DEFENSE',NULL,'22222','NAVY VENDOR',20,'1410','336414','AIR'),('cleared','USA_SPENDING','THREE',2025,'DEPARTMENT OF THE NAVY','DEPARTMENT OF DEFENSE','AMRAAM','22222','NAVY VENDOR',900,'1410','336414','AIR'),('shared','USA_SPENDING','FOUR',2025,'DEPARTMENT OF THE NAVY','DEPARTMENT OF DEFENSE',NULL,'22222','NAVY VENDOR',5,'1410','336414','AIR')")
        self.db.execute('CREATE TABLE v_summary(year INTEGER,cage_code VARCHAR,clean_parent VARCHAR,sub_agency VARCHAR,platform_family VARCHAR,total_spend DOUBLE,psc_code VARCHAR)');self.db.execute("INSERT INTO v_summary VALUES (2025,'11111','PARENT','DEPARTMENT OF THE AIR FORCE','AMRAAM',90,'1410'),(2025,'11111','PARENT','DEPARTMENT OF THE AIR FORCE','OTHER',50,'1410'),(2025,'22222','NAVY PARENT','DEPARTMENT OF THE NAVY','AMRAAM',900,'1410')")
    def read(self,sql,params):
        c=self.db.execute(sql,params);return[dict(zip([x[0]for x in c.description],r))for r in c.fetchall()]
    def test_air_force_filter_does_not_pull_in_dcma_reduction(self):
        for dataset in ['contract_actions','funding_summary']:
            out=aggregate({'dataset':dataset,'filters':{'program':'AMRAAM','agency':'DEPARTMENT OF THE AIR FORCE','years':[2025]},'group_by':['year']},self.ref,self.read)
            self.assertEqual(out['observed_obligations_usd'],100);self.assertEqual(out['mapping_basis'],'CURRENT_V7')
        out=aggregate({'dataset':'contract_actions','filters':{'program':'AMRAAM','department':'DoD','years':[2025]},'group_by':['year']},self.ref,self.read)
        self.assertEqual(out['observed_obligations_usd'],115)
        self.assertFalse(out['programme_groups_additive'])
        grouped=aggregate({'dataset':'contract_actions','filters':{'years':[2025]},'group_by':['program']},self.ref,self.read)
        self.assertTrue(grouped['programme_groups_additive'])
        self.assertEqual(grouped['observed_obligations_usd'],1015)
    def test_parent_mapping_deduplicates_site_year_context(self):
        out=aggregate({'dataset':'funding_summary','filters':{'program':'AMRAAM','years':[2025]},'group_by':['company']},self.ref,self.read)
        self.assertEqual(out['observed_obligations_usd'],115);self.assertEqual({r['company']for r in out['groups']},{'PARENT','NAVY PARENT'})
    def test_action_date_parent_wins_over_ambiguous_site_year(self):
        self.db.execute("ALTER TABLE v_transactions ADD COLUMN clean_parent VARCHAR")
        self.db.execute("UPDATE v_transactions SET clean_parent=CASE WHEN spend_amount<0 THEN 'NEW PARENT' ELSE 'OLD PARENT' END WHERE award_key='af'")
        self.db.execute("INSERT INTO v_summary VALUES (2025,'11111','NEW PARENT','DEFENSE CONTRACT MANAGEMENT AGENCY','AMRAAM',-10,'1410')")
        out=aggregate({'dataset':'funding_summary','filters':{'program':'AMRAAM','years':[2025]},'group_by':['company']},self.ref,self.read)
        self.assertEqual(out['observed_obligations_usd'],115)
        self.assertEqual({r['company'] for r in out['groups']},{'OLD PARENT','NEW PARENT','NAVY PARENT'})

    def test_clears_and_shared_context_do_not_duplicate_unfiltered_totals(self):
        reader=wrap_action_read(self.ref,self.read)
        rows=reader('SELECT platform_family,SUM(spend_amount) AS total FROM v_transactions GROUP BY 1',[])
        self.assertEqual(sum(r['total']for r in rows),1015)
        self.assertEqual(next(r['total']for r in rows if r['platform_family']=='AMRAAM | OTHER'),5)
        self.assertEqual(next(r['total']for r in rows if r['platform_family']is None),900)
    def test_item_link_is_discoverable_without_legacy_bom_membership(self):
        self.db.execute('CREATE TABLE v_platform_bom(niin VARCHAR,platform_family VARCHAR)');self.db.execute('CREATE TABLE v_nsn_cage_reference(niin VARCHAR,platform_families VARCHAR,part_number VARCHAR)');self.db.execute("INSERT INTO v_nsn_cage_reference VALUES('015006962',NULL,'CABLE')");self.db.execute('CREATE VIEW v_nsn_cage_reference_explorer AS SELECT * FROM v_nsn_cage_reference')
        reader=wrap_item_read(self.ref,self.read,'AMRAAM',{'platform_families','niin','part_number'})
        rows=reader('SELECT niin,platform_families FROM v_nsn_cage_reference_explorer',[])
        self.assertEqual(rows,[{'niin':'015006962','platform_families':'AMRAAM'}])
    def test_unknown_and_ambiguous_identities_are_not_financially_promoted(self):
        with self.assertRaises(ValueError):aggregate({'filters':{'program':'NOT A PROGRAM'}},self.ref,self.read)
        self.ref.aliases=[{'source_label':'MISSILE FAMILY','effective_canonical_entity_id':'AMRAAM','disposition':'IDENTITY_ONLY_NO_FINANCIAL_PROMOTION'}]
        out=search_programmes(self.ref,'MISSILE FAMILY');self.assertTrue(out['entries'][0]['financial_promotion_held'])

if __name__=='__main__':unittest.main()

class PrivateDatasetRoutes(unittest.TestCase):
    setUp=CurrentV7Scopes.setUp
    read=CurrentV7Scopes.read
    def test_private_subcontract_read_exposes_financial_treatment_without_widening_public_explorer(self):
        from unittest.mock import patch
        import os
        self.db.execute('CREATE TABLE v_subcontracts(year INTEGER,subcontractor_cage VARCHAR,prime_award_id VARCHAR,subcontract_value_usd DOUBLE,included_in_adjusted_total BOOLEAN,subcontract_value_treatment VARCHAR,subcontract_methodology VARCHAR,source_report_id VARCHAR,source_report_last_modified_date VARCHAR)')
        self.db.execute("INSERT INTO v_subcontracts VALUES(2025,'19645','AWARD1',100,true,'adjusted_existing_platform_value','Reported subcontract value v3','NATIVE-REPORT','2026-11-01')")
        client,ns=self.client()
        body={'table':'v_subcontracts','columns':['subcontract_value_usd','included_in_adjusted_total','subcontract_value_treatment','source_report_id'],'filters':{'subcontractor_cage':'19645'},'limit':2}
        with patch.dict(os.environ,{'MIMIR_EXPORT_PROXY_SECRET':'fixture'}),patch('private_platform_reference.reference_for',return_value=self.ref):
            self.assertEqual(client.post('/query',json=body).status_code,401)
            result=client.post('/query',json=body,headers={'X-Mimir-Export-Key':'fixture'})
            self.assertEqual(result.status_code,200,result.text)
            row=result.json()['rows'][0]
            self.assertTrue(row['included_in_adjusted_total'])
            self.assertEqual(row['subcontract_value_treatment'],'adjusted_existing_platform_value')
            self.assertEqual(row['source_report_id'],'NATIVE-REPORT')
        sql,_=ns['build_explorer_query'](ns['ExplorerRequest'](**body),row_limit=2)
        self.assertNotIn('included_in_adjusted_total',sql)
        self.assertNotIn('subcontract_value_treatment',sql)
        # The public explorer already supports report identity; only financial
        # treatment metadata is newly available through the private adapter.
        self.assertIn('source_report_id',sql)
    def client(self):
        import ast,typing,re,os,hmac
        import pandas as pd,numpy as np
        from pathlib import Path
        from unittest.mock import Mock
        from fastapi import FastAPI,HTTPException,Request,Response,Query
        from fastapi.testclient import TestClient
        from pydantic import BaseModel
        wanted={'safe_int','safe_years','get_niin','sanitize','sql_literal','quote_ident','normalised_niin_filter_expr','_null_expr','_source_expr','_contract_award_type_description_expr','resolve_explorer_column','_like_param_contains','multi_platform_component_expr','build_contract_awards_explorer_query_from_actions','build_explorer_query','build_contract_awards_explorer_query','build_contract_awards_explorer_query_from_rollup','_rolled_contract_years','private_research_dataset','df_sanitize_for_json'}
        nodes=[]
        for n in ast.parse(Path('main.py').read_text()).body:
            if isinstance(n,ast.FunctionDef)and n.name in wanted:n.decorator_list=[];nodes.append(n)
            elif isinstance(n,ast.ClassDef)and n.name=='ExplorerRequest':nodes.append(n)
            elif isinstance(n,ast.Assign)and any(isinstance(t,ast.Name)and t.id.startswith(('NSN_','CONTRACT_AWARD_','SUBCONTRACT_','ALLOWED_EXPLORER_','EXPLORER_'))for t in n.targets):nodes.append(n)
        ns={**vars(typing),'re':re,'os':os,'hmac':hmac,'pd':pd,'np':np,'BaseModel':BaseModel,'Request':Request,'Response':Response,'Query':Query,'HTTPException':HTTPException,'logger':Mock(),'GLOBAL_CACHE':{'last_loaded':1,'is_loading':False,'platform_release':{'components':{'main':{'release_id':'fixture'}}}}}
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'main.py','exec'),ns)
        ns['get_duck_table_columns']=lambda t:{r[0]for r in self.db.execute('DESCRIBE '+t).fetchall()}
        ns['programme_read_df']=lambda sql,params:self.db.execute(sql,params).fetchdf()
        app=FastAPI();app.post('/query')(ns['private_research_dataset']);return TestClient(app),ns
    def test_current_v7_private_award_counts_and_buyer_totals_share_actions(self):
        from unittest.mock import patch
        import os
        for column,kind in [('action_date','DATE'),('description','VARCHAR'),('base_award_description','VARCHAR'),('action_description','VARCHAR')]:self.db.execute('ALTER TABLE v_transactions ADD COLUMN '+column+' '+kind)
        self.db.execute("UPDATE v_transactions SET action_date='2025-01-01'")
        [self.db.execute('ALTER TABLE v_summary ADD COLUMN '+c+' VARCHAR')for c in ['psc_description','naics_code','naics_description']]
        client,ns=self.client();body={'table':'v_contract_awards_enriched','columns':['award_key','contract_id','sub_agency','platform_family','obligations_in_selected_period_usd','number_of_actions_in_selected_period'],'filters':{'platform_family':'AMRAAM','year':[2025],'sub_agency':'DEPARTMENT OF THE AIR FORCE'},'limit':10}
        with patch.dict(os.environ,{'MIMIR_EXPORT_PROXY_SECRET':'fixture'}),patch('private_platform_reference.reference_for',return_value=self.ref):
            self.assertEqual(client.post('/query',json=body).status_code,401)
            headers={'X-Mimir-Export-Key':'fixture'}
            response=client.post('/query',json=body,headers=headers);self.assertEqual(response.status_code,200,response.json())
            out=response.json();self.assertEqual(out['mapping_basis'],'CURRENT_V7');self.assertEqual(len(out['rows']),1)
            self.assertEqual(out['rows'][0]['obligations_in_selected_period_usd'],100)
            count=client.post('/query',json={**body,'operation':'count'},headers=headers);self.assertEqual(count.json()['count'],1)
            self.assertIn('private, no-store',response.headers['Cache-Control'])
            self.assertEqual(client.post('/query',json={**body,'table':'auth.users'},headers=headers).status_code,400)
