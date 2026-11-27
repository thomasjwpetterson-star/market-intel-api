"""Execute shared Explorer routes against independent native-record fixtures."""
import ast
import csv
import datetime
import hmac
import json
import logging
import os
from pathlib import Path
import re
import time
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
import uuid

import duckdb
from fastapi import BackgroundTasks, HTTPException, Query, Request
import numpy as np
import pandas as pd
import pytest
from starlette.responses import FileResponse

from analytical_admission import ANALYTICAL_DEADLINE
from analytical_deadline import query_deadline
from dashboard_projection import build_code_projection, build_explorer_projection, build_item_filter_projection, build_explorer_action_projection
from explorer_page_read import read_snapshot_page


@pytest.fixture
def explorer(tmp_path, monkeypatch):
    db = duckdb.connect()
    # Signed facts, a zero-value award, multiple scopes, and literal search
    # metacharacters. Every award has one action in each of two fiscal years.
    records = [
        ('A','3DELRR','AIR','11111','NAVY','MA','5840','334511',100.25,-10.5,'RADAR %_ literal'),
        ('B','3DELRR','AIR','22222','AIR FORCE','NY','5840','334511',0.,-5.,'RADAR support'),
        ('C','2GEN FLIR','GROUND','11111','ARMY','MA','5855','334511',50.,30.,'FLIR support'),
        ('D','AMRAAM','MISSILES','33333','NAVY','TX','1410','336414',400.75,-50.,'MISSILE support'),
        ('E','OTHER','NAVAL','44444','NAVY','VA','1905','336611',25.,0.,'SHIP support'),
        ('F','OTHER','SPACE','11111','SPACE FORCE','CA','1810','336414',0.,0.,'SPACE support'),
    ]
    db.execute('''CREATE TABLE v_contracts_rolled(
        award_key VARCHAR,contract_id VARCHAR,platform_family VARCHAR,market_segment VARCHAR,
        vendor_cage VARCHAR,sub_agency VARCHAR,place_of_performance_state VARCHAR,psc VARCHAR,
        naics_code VARCHAR,obligations_fy2024 DOUBLE,obligations_fy2025 DOUBLE,
        base_award_description VARCHAR,latest_action_description VARCHAR,description VARCHAR,
        vendor_name VARCHAR,parent_agency VARCHAR,city VARCHAR,state VARCHAR,country VARCHAR,
        place_of_performance_city VARCHAR,place_of_performance_country VARCHAR,place_of_performance_zip VARCHAR,
        naics_description VARCHAR,nsn VARCHAR,niin VARCHAR,total_spend DOUBLE,year INTEGER,
        action_count_fy2024 INTEGER,action_count_fy2025 INTEGER,
        earliest_action_date_fy2024 DATE,latest_action_date_fy2024 DATE,
        earliest_action_date_fy2025 DATE,latest_action_date_fy2025 DATE)''')
    for ident, platform, domain, cage, agency, state, psc, naics, prior, current, text in records:
        values = [ident,ident,platform,domain,cage,agency,state,psc,naics,prior,current,
                  text,text,text,'Vendor '+cage,'DOD','CITY',state,'USA','CITY','USA','00001',
                  None,'1234000000001','000000001',prior+current,2025,1,1,
                  '2024-01-01','2024-09-30','2025-01-01','2025-09-30']
        db.execute('INSERT INTO v_contracts_rolled VALUES ('+','.join(['?']*len(values))+')',values)
    db.execute('ALTER TABLE v_contracts_rolled ADD COLUMN platform_count INTEGER DEFAULT 2')
    db.execute('''CREATE TABLE v_transactions AS SELECT award_key,contract_id,platform_family,market_segment,
        vendor_cage,vendor_name,sub_agency,parent_agency,city,state,country,
        place_of_performance_city,place_of_performance_state,place_of_performance_country,place_of_performance_zip,
        psc,naics_code,description,base_award_description,latest_action_description AS action_description,
        nsn,niin,'PART-1' AS part_number,'S9C' AS source_of_supply,
        2024 AS year,obligations_fy2024 AS spend_amount,DATE '2024-01-01' AS action_date
        FROM v_contracts_rolled UNION ALL SELECT award_key,contract_id,platform_family,market_segment,
        vendor_cage,vendor_name,sub_agency,parent_agency,city,state,country,
        place_of_performance_city,place_of_performance_state,place_of_performance_country,place_of_performance_zip,
        psc,naics_code,description,base_award_description,latest_action_description,
        nsn,niin,'PART-1','S9C',2025,obligations_fy2025,DATE '2025-01-01' FROM v_contracts_rolled''')
    db.execute('''CREATE TABLE v_summary AS SELECT contract_id,year,vendor_cage AS cage_code,vendor_name,
        'PARENT' AS clean_parent,sub_agency,parent_agency,market_segment,platform_family,
        city,state,country,psc AS psc_code,'PSC '||psc AS psc_description,
        naics_code,'NAICS '||naics_code AS naics_description,spend_amount AS total_spend,1 AS contract_count
        FROM v_transactions''')
    db.execute('''CREATE TABLE v_subcontracts AS SELECT contract_id AS prime_award_id,year,
        vendor_cage AS subcontractor_cage,vendor_name AS subcontractor_name,'99999' AS prime_cage,
        'PRIME' AS prime_name,sub_agency,parent_agency,market_segment,platform_family,
        psc AS psc_code,naics_code,spend_amount AS subcontract_value_usd,
        description AS subcontract_description,action_date AS subcontract_action_date,
        state AS subcontractor_state,city AS subcontractor_city FROM v_transactions''')
    db.execute('''CREATE TABLE v_nsn_cage_reference AS SELECT contract_id AS part_number,
        CASE WHEN contract_id IN ('A','B') THEN '000000001' ELSE '000000002' END AS niin,
        CASE WHEN contract_id IN ('A','B') THEN '1234000000001' ELSE '1234000000002' END AS nsn,
        vendor_cage AS cage_code,vendor_name,description,psc AS fsc_code,
        platform_family||' | SHARED' AS platform_families,2 AS platform_count,
        'REFERENCE' AS supplier_status FROM v_contracts_rolled''')
    db.execute('''CREATE TABLE v_subcontract_descriptions AS SELECT
        'REPORT' AS description_lookup_key,'REPORT' AS source_report_id,'' AS source_dedup_key,
        2 AS reported_description_count,1 AS equal_value_report_count,2 AS source_record_count,
        0 AS superseded_source_version_count,'2025-01-01' AS earliest_reported_action_date,
        '2025-01-02' AS latest_reported_action_date,'REPORT'||i AS report_id,
        'KEY'||i AS report_dedup_key,'2025-01-03' AS report_last_modified_date,
        '2025-01-02' AS report_action_date,10.25 AS report_amount,
        'Description '||i AS report_description,true AS is_current_source_version
        FROM range(2) t(i)''')
    names = {'get_duck_table_columns','quote_ident','safe_int','_like_param_contains','_null_expr','_source_expr',
             '_contract_award_type_description_expr','_rolled_contract_years','resolve_explorer_column',
             'multi_platform_component_expr','normalised_niin_filter_expr','get_niin','df_sanitize_for_json',
             'build_contract_awards_explorer_query_from_actions','build_contract_awards_explorer_query_from_rollup',
             'build_contract_awards_explorer_query','build_explorer_query','fetch_explorer_page',
             'explorer_preview','explorer_page','explorer_count','explorer_export',
             'explorer_vendor_search','explorer_taxonomy_search','explorer_subcontract_descriptions'}
    constants = {'NSN_REF_TABLE','NSN_REF_EXPLORER_SOURCE','NSN_SUPPLY_STATE_TABLE','NSN_PRICE_SUMMARY_TABLE',
                 'NSN_OPPORTUNITY_SUMMARY_TABLE','NSN_OPPORTUNITY_DETAIL_TABLE','CONTRACT_AWARD_EXPLORER_TABLE',
                 'SUBCONTRACT_EXPLORER_TABLE','CONTRACT_AWARD_SYNTHETIC_COLUMNS','ALLOWED_EXPLORER_TABLES',
                 'ALLOWED_EXPLORER_COLUMNS','EXPLORER_DEFAULT_COLUMNS','EXPLORER_COLUMN_ALIASES',
                 'EXPLORER_FILTER_REQUIRED_TABLES','SUBCONTRACT_INTERNAL_EXPORT_COLUMNS','SUBCONTRACT_RESEARCH_COLUMNS',
                 'INTERNAL_DUCK_RELATIONS'}
    nodes = []
    for node in ast.parse(Path('main.py').read_text()).body:
        if isinstance(node,ast.FunctionDef) and node.name in names:
            node.decorator_list=[]
            nodes.append(node)
        elif isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id in constants for t in node.targets):
            nodes.append(node)
    def columns(table):
        try: return {r[0] for r in db.execute('DESCRIBE '+table).fetchall()}
        except duckdb.CatalogException: return set()
    calls=[]
    def fetch(sql,params,followup=None,sanitize=True):
        cursor=db.cursor()
        calls.append({'sql':sql,'params':params,'followup':followup is not None})
        try:
            with query_deadline(cursor):
                result=read_snapshot_page(cursor,sql,params,followup) if followup else cursor.execute(sql,params).fetchdf()
                return ns['df_sanitize_for_json'](result) if sanitize else result
        finally: cursor.close()
    ns=dict(Any=Any,Dict=Dict,List=List,Optional=Optional,pd=pd,np=np,re=re,os=os,hmac=hmac,
            csv=csv,uuid=uuid,Path=Path,HTTPException=HTTPException,Request=Request,BackgroundTasks=BackgroundTasks,
            FileResponse=FileResponse,ExplorerRequest=SimpleNamespace,SubcontractDescriptionsRequest=SimpleNamespace,
            Query=Query,LOCAL_CACHE_DIR=tmp_path,
            get_duck_table_columns=columns,_get_duck_table_columns_cached=lambda table,generation:columns(table),
            GLOBAL_CACHE={},duck_fetch_df=fetch,logger=logging.getLogger('audit'),
            cleanup_temp_file=lambda path:Path(path).unlink(missing_ok=True))
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'main.py','exec'),ns)
    build_code_projection(db);build_explorer_projection(db)
    monkeypatch.setenv('MIMIR_EXPORT_PROXY_SECRET','local-test-key')
    try: yield db,ns,calls
    finally: db.close()


AWARD_CASES = [
    ({'platform_family':['3DELRR']},['A','B']),
    ({'platform_family':['3DELRR','2GEN FLIR']},['A','B','C']),
    ({'market_segment':['AIR']},['A','B']),
    ({'market_segment':['AIR','GROUND']},['A','B','C']),
    ({'vendor_cage':'11111'},['A','C','F']),
    ({'vendor_cage':['11111','22222']},['A','B','C','F']),
    ({'cage_code':['11111']},['A','C','F']),
    ({'sub_agency':['NAVY']},['A','D','E']),
    ({'parent_agency':['DOD']},['A','B','C','D','E','F']),
    ({'platform_family':['3DELRR'],'place_of_performance_state':['MA']},['A']),
    ({'platform_family':['3DELRR'],'place_of_performance_city':['CITY']},['A','B']),
    ({'country':['USA'],'state':['TX']},['D']),
    ({'psc_code':['5840'],'naics_code':['334511']},['A','B']),
    ({'psc':['1410']},['D']),
    ({'year':[2025],'min_spend':-10.5,'max_spend':0},['A','B','E','F']),
    ({'platform_family':['3DELRR'],'year':[]},['A','B']),
    ({'platform_family':['3DELRR'],'year':[2023]},[]),
    ({'platform_family':['3DELRR'],'search':'RADAR'},['A','B']),
    ({'platform_family':['3DELRR'],'search':'%_'},['A']),
    ({'platform_family':['3DELRR'],'search':"' OR 1=1 --"},[]),
    ({'platform_family':['__MISSING__']},[]),
    ({'nsn':'1234-00-000-0001'},['A','B','C','D','E','F']),
    ({'nsn':['1234000000001'],'platform_family':['3DELRR']},['A','B']),
    ({'source_of_supply':'S9C','platform_family':['3DELRR'],'search':'RADAR'},['A','B']),
    ({'source_of_supply':'S9C','vendor_cage':['11111','22222'],'year':[2025]},['A','B','C','F']),
]


@pytest.mark.parametrize('filters,expected', AWARD_CASES)
def test_award_filters_agree_across_preview_count_page_and_csv(explorer,filters,expected):
    db,ns,calls=explorer
    fields=['contract_id','obligations_in_selected_period_usd','number_of_actions_in_selected_period',
            'base_award_description','latest_action_description','psc_description','naics_description']
    payload=SimpleNamespace(table='v_contract_awards_enriched',columns=fields,filters=filters,limit=1,offset=0)
    preview=ns['explorer_preview'](payload)
    assert sorted(r['contract_id']for r in preview)==expected
    assert ns['explorer_count'](payload)=={'count':len(expected)}
    pages=[]
    for offset in range(len(expected)+1):
        payload.offset=offset
        pages.extend(ns['explorer_page'](payload))
    assert pages==preview
    response=ns['explorer_export'](payload,BackgroundTasks(),SimpleNamespace(headers={
        'X-Mimir-Export-Key':'local-test-key','X-Mimir-Plan-Tier':'lite'}))
    with open(response.path,newline='')as f:exported=list(csv.DictReader(f))
    assert [r['contract_id']for r in exported]==[r['contract_id']for r in preview]
    assert int(response.headers['x-export-row-count'])==len(expected)
    for actual,wanted in zip(exported,preview):
        assert float(actual['obligations_in_selected_period_usd'])==wanted['obligations_in_selected_period_usd']
        assert actual['base_award_description']==wanted['base_award_description']


@pytest.mark.parametrize('field',['nsn','niin'])
@pytest.mark.parametrize('table',['v_transactions','v_contract_awards_enriched'])
def test_prepared_item_key_filters_preserve_native_formats_and_values(explorer,field,table):
    db,ns,calls=explorer
    for contract,value in zip('ABCDEF',['1234000000001','1234-00-000-0001','000000001','1','NSN:1234-00-000-0001',None]):
        db.execute(f'UPDATE v_transactions SET {field}=? WHERE contract_id=?',[value,contract])
    build_item_filter_projection(db)
    build_explorer_action_projection(db)
    # Repeated actions share a raw key; neither formatting nor missing keys
    # may cause records to disappear or match a different native field.
    assert db.execute(f'SELECT COUNT(*) FROM dashboard_explorer_{field}_filter').fetchone()[0]==5
    fields=['contract_id','spend_amount'] if table=='v_transactions' else [
        'contract_id','obligations_in_selected_period_usd','number_of_actions_in_selected_period',
        'base_award_description','latest_action_description']
    payload=SimpleNamespace(table=table,columns=fields,filters={field:['1234000000001']},limit=2,offset=2)
    prepared=ns['explorer_preview'](payload)
    assert len(prepared)==(10 if table=='v_transactions' else 5)
    assert 'REGEXP_REPLACE' not in calls[-1]['sql']
    assert f'dashboard_explorer_{field}_filter' in calls[-1]['sql']
    prepared_count=ns['explorer_count'](payload)
    prepared_page=ns['explorer_page'](payload)
    response=ns['explorer_export'](payload,BackgroundTasks(),SimpleNamespace(headers={
        'X-Mimir-Export-Key':'local-test-key','X-Mimir-Plan-Tier':'lite'}))
    with open(response.path,newline='') as f:prepared_csv=list(csv.reader(f))
    db.execute('DROP TABLE dashboard_explorer_nsn_filter')
    db.execute('DROP TABLE dashboard_explorer_niin_filter')
    db.execute('DROP TABLE dashboard_explorer_actions')
    assert ns['explorer_preview'](payload)==prepared
    assert ns['explorer_count'](payload)==prepared_count
    assert ns['explorer_page'](payload)==prepared_page
    response=ns['explorer_export'](payload,BackgroundTasks(),SimpleNamespace(headers={
        'X-Mimir-Export-Key':'local-test-key','X-Mimir-Plan-Tier':'lite'}))
    with open(response.path,newline='') as f:assert list(csv.reader(f))==prepared_csv


@pytest.mark.parametrize('filters',[{'platform_family':['__MISSING__']},{'nsn':'9999999999999'}])
def test_empty_award_export_keeps_requested_customer_headers(explorer,filters):
    db,ns,calls=explorer
    fields=['contract_id','obligations_in_selected_period_usd','base_award_description']
    payload=SimpleNamespace(table='v_contract_awards_enriched',columns=fields,filters=filters)
    response=ns['explorer_export'](payload,BackgroundTasks(),SimpleNamespace(headers={
        'X-Mimir-Export-Key':'local-test-key','X-Mimir-Plan-Tier':'lite'}))
    with open(response.path,newline='') as f:rows=list(csv.reader(f))
    assert rows==[fields]
    assert response.headers['x-export-row-count']=='0'


@pytest.mark.parametrize('offset',[0,1,4])
def test_item_award_details_bind_actual_contracts_and_keep_native_values(explorer,offset):
    db,ns,calls=explorer
    # Two native award identities can share a reported contract identifier.
    db.execute("UPDATE v_transactions SET contract_id='A' WHERE contract_id='B'")
    db.execute('DROP TABLE dashboard_explorer_actions')
    fields=sorted(ns['CONTRACT_AWARD_SYNTHETIC_COLUMNS'])
    payload=SimpleNamespace(table='v_contract_awards_enriched',columns=fields,
        filters={'nsn':'1234-00-000-0001','year':[2025]},limit=2,offset=offset)
    sql,params=ns['build_contract_awards_explorer_query_from_actions'](payload,2,offset)
    expected=ns['df_sanitize_for_json'](db.execute(sql,params).fetchdf())
    build_explorer_action_projection(db)
    captured=[]
    original=ns['duck_fetch_df']
    def fetch(sql,params,followup=None,**kw):
        def details(ranked):
            query,values=followup(ranked)
            captured.append((ranked,query,values))
            return query,values
        return original(sql,params,followup=details,**kw)
    ns['duck_fetch_df']=fetch
    actual=ns['fetch_explorer_page'](payload,2,offset)
    pd.testing.assert_frame_equal(actual.reset_index(drop=True),expected.reset_index(drop=True))
    assert calls[-1]['followup']
    assert len(captured)==1
    ranked,detail_sql,detail_params=captured[0]
    assert None not in detail_params
    identities=list(dict.fromkeys(int(v) for ids in ranked['action_row_ids'] for v in ids))
    assert detail_params[:len(identities)]==identities
    assert 'FROM dashboard_explorer_actions WHERE "_explorer_action_row_id" IN (' in detail_sql
    assert 'selected_actions AS MATERIALIZED' in detail_sql
    assert detail_params[-2:]==[len(ranked),0]
    plan=json.loads(db.execute('EXPLAIN (FORMAT JSON) '+detail_sql,detail_params).fetchone()[1])
    def action_scans(nodes):
        for node in nodes:
            extra=node.get('extra_info',{})
            if 'dashboard_explorer_actions' in extra.get('Table',''):yield extra
            yield from action_scans(node.get('children',[]))
    scans=list(action_scans(plan))
    assert scans and all('_explorer_action_row_id' in str(scan.get('Filters','')) for scan in scans)


def test_action_row_selection_retains_lifetime_rows_and_fallback_award_keys(explorer):
    db,ns,calls=explorer
    db.execute("UPDATE v_transactions SET contract_id=NULL,nsn=NULL,niin=NULL,part_number=NULL,source_of_supply=NULL WHERE award_key='A' AND year=2024")
    db.execute("UPDATE v_transactions SET base_award_description=CASE WHEN year=2024 THEN 'Earlier native base' ELSE NULL END WHERE award_key='A'")
    db.execute("UPDATE v_transactions SET award_key=NULL WHERE award_key='C'")
    fields=sorted(ns['CONTRACT_AWARD_SYNTHETIC_COLUMNS'])
    payload=SimpleNamespace(table='v_contract_awards_enriched',columns=fields,
        filters={'nsn':'1234000000001','year':[2025]})
    db.execute('DROP TABLE dashboard_explorer_actions')
    sql,params=ns['build_contract_awards_explorer_query_from_actions'](payload,50)
    expected=ns['df_sanitize_for_json'](db.execute(sql,params).fetchdf())
    build_explorer_action_projection(db)
    actual=ns['fetch_explorer_page'](payload,50)
    pd.testing.assert_frame_equal(actual,expected)
    assert any(r['award_key']=='UNKNOWN|PIID|C|CAGE|11111' for r in actual.to_dict('records'))
    a=actual[actual['award_key']=='A'].iloc[0]
    assert a['number_of_actions_in_selected_period']==1
    assert a['obligations_in_selected_period_usd']==-10.5
    assert a['base_award_description']=='Earlier native base'


def test_item_projection_excludes_unrelated_awards_without_hiding_source_column_results(explorer):
    db,ns,calls=explorer
    db.execute("UPDATE v_transactions SET nsn=NULL,niin=NULL,part_number=NULL,source_of_supply=NULL WHERE award_key='F'")
    build_explorer_action_projection(db)
    assert db.execute('SELECT COUNT(*) FROM dashboard_explorer_actions').fetchone()[0]==10
    assert db.execute("SELECT COUNT(*) FROM dashboard_explorer_actions WHERE award_key='F'").fetchone()[0]==0
    fields=['award_key','contract_id','source_of_supply','lifetime_obligations_usd','number_of_actions_in_selected_period']
    payload=SimpleNamespace(table='v_contract_awards_enriched',columns=fields,
        filters={'year':[2025]},limit=2,offset=2)
    sql,params=ns['build_contract_awards_explorer_query_from_actions'](payload,50,use_prepared=False)
    expected=ns['df_sanitize_for_json'](db.execute(sql,params).fetchdf())
    actual=ns['fetch_explorer_page'](payload,50)
    pd.testing.assert_frame_equal(actual,expected)
    assert set(actual.award_key)==set('ABCDEF')
    assert ns['explorer_count'](payload)['count']==6
    page=ns['explorer_page'](payload)
    assert [r['award_key'] for r in page]==list(actual.award_key)[2:4]
    response=ns['explorer_export'](payload,BackgroundTasks(),SimpleNamespace(headers={
        'X-Mimir-Export-Key':'local-test-key','X-Mimir-Plan-Tier':'lite'}))
    with open(response.path,newline='')as f:exported=list(csv.DictReader(f))
    assert set(r['award_key'] for r in exported)==set('ABCDEF')


GENERIC_CASES=[
    ('v_transactions',{'platform_family':['3DELRR']},4),
    ('v_transactions',{'vendor_cage':['11111','22222'],'year':[2025]},4),
    ('v_transactions',{'year':[2025],'description_terms':['RADAR']},2),
    ('v_transactions',{'year':[2025],'description_terms':['RADAR','MISSILE']},3),
    ('v_transactions',{'year':[2025],'min_spend':-10.5,'max_spend':0},4),
    ('v_transactions',{'nsn':['1234-00-000-0001','1234-00-000-0002']},12),
    ('v_contracts_rolled',{'platform_family':['3DELRR']},2),
    ('v_contracts_rolled',{'vendor_cage':['11111','22222']},4),
    ('v_contracts_rolled',{'nsn':'1234-00-000-0001'},6),
    ('v_subcontracts',{'cage_code':['11111','22222'],'year':[2025]},4),
    ('v_subcontracts',{'platform_family':['3DELRR'],'max_spend':0},3),
    ('v_summary',{'cage_code':['11111'],'year':[2025]},3),
    ('v_summary',{'psc_code':['5840'],'year':[2025]},2),
    ('v_nsn_cage_reference',{'nsn':'1234-00-000-0001'},2),
    ('v_nsn_cage_reference',{'niin':['000000001','000000002']},6),
    ('v_nsn_cage_reference',{'cage_code':['11111']},3),
    ('v_nsn_cage_reference',{'platform_family':['3DELRR','SHARED']},6),
    ('v_nsn_cage_reference',{'search':'%_'},1),
    ('v_nsn_cage_reference',{'is_multi_platform_component':True},6),
    ('v_nsn_cage_reference',{'niin':'999999999'},0),
]


@pytest.mark.parametrize('table,filters,count',GENERIC_CASES)
def test_other_datasets_keep_count_paging_and_csv_values(explorer,table,filters,count):
    db,ns,calls=explorer
    fields={'v_transactions':['contract_id','year','spend_amount','description','action_date'],
            'v_contracts_rolled':['contract_id','total_spend','description','is_multi_platform_component'],
            'v_subcontracts':['prime_award_id','subcontractor_cage','subcontract_value_usd','subcontract_description','subcontract_action_date'],
            'v_summary':['contract_id','year','total_spend'],
            'v_nsn_cage_reference':['niin','nsn','part_number','description']}[table]
    payload=SimpleNamespace(table=table,columns=fields,filters=filters,limit=2,offset=0)
    preview=ns['explorer_preview'](payload)
    assert len(preview)==count
    assert ns['explorer_count'](payload)=={'count':count}
    pages=[]
    for offset in range(0,count+1,2):
        payload.offset=offset;pages.extend(ns['explorer_page'](payload))
    assert pages==preview
    response=ns['explorer_export'](payload,BackgroundTasks(),SimpleNamespace(headers={
        'X-Mimir-Export-Key':'local-test-key','X-Mimir-Plan-Tier':'lite'}))
    with open(response.path,newline='')as f:rows=list(csv.reader(f))
    assert len(rows)-1==count
    assert int(response.headers['x-export-row-count'])==count
    # Compare actual native CSV values, including quoting, multiline-capable
    # descriptions and signed amounts; customer header names are checked apart.
    sql,params=ns['build_explorer_query'](payload,100)
    native_path=Path(response.path).with_suffix('.native.csv')
    db.execute('COPY ('+sql+") TO '"+str(native_path)+"' (HEADER, DELIMITER ',')",params)
    with native_path.open(newline='')as f:native=list(csv.reader(f))
    assert rows[1:]==native[1:]


@pytest.mark.parametrize('table',['v_contract_awards_enriched','v_transactions','v_contracts_rolled','v_subcontracts','v_nsn_cage_reference'])
def test_empty_selection_does_not_authorize_a_whole_table_scan(explorer,table):
    _,ns,_=explorer
    payload=SimpleNamespace(table=table,columns=[],filters={'platform_family':[]})
    with pytest.raises(HTTPException)as error:ns['explorer_preview'](payload)
    assert error.value.status_code==400


@pytest.mark.parametrize('plan,limit',[('lite',100),('professional',5000),('enterprise',100000)])
def test_exports_keep_account_cap_and_only_bind_selected_details(explorer,plan,limit):
    db,ns,calls=explorer
    payload=SimpleNamespace(table='v_contract_awards_enriched',columns=['contract_id'],filters={'platform_family':['3DELRR']})
    params=[]
    original=ns['duck_fetch_df']
    def fetch(sql,values,followup=None,**options):
        def capture(ranked):
            detail_sql,identities=followup(ranked)
            params.extend(identities)
            assert detail_sql.count('?')==2
            return detail_sql,identities
        return original(sql,values,capture if followup else None,**options)
    ns['duck_fetch_df']=fetch
    response=ns['explorer_export'](payload,BackgroundTasks(),SimpleNamespace(headers={
        'X-Mimir-Export-Key':'local-test-key','X-Mimir-Plan-Tier':plan}))
    assert int(response.headers['x-export-row-count'])==2
    assert len(params)==2 and all(value is not None for value in params)
    assert calls[-1]['params'][-2:]==[limit,0]


def test_export_authorization_deadline_and_failure_cleanup(explorer,monkeypatch):
    _,ns,calls=explorer
    payload=SimpleNamespace(table='v_contract_awards_enriched',columns=['contract_id'],filters={'platform_family':['3DELRR']})
    for headers in ({},{'X-Mimir-Export-Key':'wrong','X-Mimir-Plan-Tier':'lite'},
                    {'X-Mimir-Export-Key':'local-test-key','X-Mimir-Plan-Tier':'free'}):
        with pytest.raises(HTTPException)as error:ns['explorer_export'](payload,BackgroundTasks(),SimpleNamespace(headers=headers))
        assert error.value.status_code==403
    assert not calls
    headers={'X-Mimir-Export-Key':'local-test-key','X-Mimir-Plan-Tier':'lite'}
    token=ANALYTICAL_DEADLINE.set(time.monotonic()-1)
    try:
        with pytest.raises(HTTPException)as error:ns['explorer_export'](payload,BackgroundTasks(),SimpleNamespace(headers=headers))
        assert error.value.status_code==504
    finally:ANALYTICAL_DEADLINE.reset(token)
    def failed_csv(self,path,*args,**kwargs):
        Path(path).write_text('partial export')
        raise OSError('write failed')
    monkeypatch.setattr(pd.DataFrame,'to_csv',failed_csv)
    with pytest.raises(HTTPException)as error:ns['explorer_export'](payload,BackgroundTasks(),SimpleNamespace(headers=headers))
    assert error.value.status_code==500
    assert not list(ns['LOCAL_CACHE_DIR'].glob('mimir_export_*.csv'))


@pytest.mark.parametrize('plan,limit',[('lite',100),('professional',5000),('enterprise',100000)])
def test_export_limits_are_enforced_on_a_larger_reference_selection(explorer,plan,limit):
    db,ns,_=explorer
    db.execute('DROP TABLE v_nsn_cage_reference')
    db.execute("""CREATE TABLE v_nsn_cage_reference AS SELECT
        lpad(i::VARCHAR,9,'0') AS niin,'11111' AS cage_code,('PART-'||i) AS part_number
        FROM range(100005) t(i)""")
    payload=SimpleNamespace(table='v_nsn_cage_reference',columns=['niin','cage_code','part_number'],
                            filters={'cage_code':'11111'})
    response=ns['explorer_export'](payload,BackgroundTasks(),SimpleNamespace(headers={
        'X-Mimir-Export-Key':'local-test-key','X-Mimir-Plan-Tier':plan}))
    with open(response.path,newline='')as f:
        reader=csv.reader(f)
        assert next(reader)==['National Item Identification Number (NIIN)','CAGE Code','Part Number']
        assert sum(1 for _ in reader)==limit
    assert response.headers['x-export-row-count']==str(limit)


def test_filter_typeaheads_use_exact_prepared_dictionaries(explorer):
    db,ns,calls=explorer
    vendor=ns['explorer_vendor_search']('11111')
    assert [r['cage_code']for r in vendor]==['11111']
    expected={kind:ns['explorer_taxonomy_search'](kind,code) for kind,code in [('psc','5840'),('naics','334511')]}
    assert expected['psc']==[{'code':'5840','description':'PSC 5840'}]
    assert expected['naics']==[{'code':'334511','description':'NAICS 334511'}]
    db.execute('DROP TABLE v_summary')
    for kind,code in [('psc','5840'),('naics','334511')]:
        assert ns['explorer_taxonomy_search'](kind,code)==expected[kind]
        assert 'FROM dashboard_' in calls[-1]['sql']


def test_subcontract_source_history_preserves_reports_and_deadline(explorer):
    _,ns,_=explorer
    payload=SimpleNamespace(source_report_id='REPORT',source_dedup_key=None,primary_description=None)
    result=ns['explorer_subcontract_descriptions'](payload)
    assert result['reported_description_count']==2
    assert sorted(result['descriptions'])==['Description 0','Description 1']
    assert result['audit']['source_record_count']==2
    assert all(r['reported_amount']==10.25 for r in result['audit']['reports'])
    def timeout(*args,**kwargs):raise HTTPException(status_code=504,detail='deadline')
    ns['duck_fetch_df']=timeout
    with pytest.raises(HTTPException)as error:ns['explorer_subcontract_descriptions'](payload)
    assert error.value.status_code==504
