"""Real SQL regression for website/research mapping, paging, filters and failures."""
import ast
import math
from pathlib import Path
from typing import Any, List, Optional
from unittest.mock import Mock

import duckdb
import pandas as pd
import pytest
from fastapi import HTTPException, Query
from private_platform_reference import Reference


def routes(db, monkeypatch):
    reference=Reference.__new__(Reference)
    reference.registry=[{'canonical_entity_id':'VIRGINIA','canonical_label':'Virginia Class (SSN 774)'}]
    reference.aliases=[]
    reference.rows=[{'award_key':'B','canonical_entity_id':'VIRGINIA'}, {'award_key':'C','target_entity_ids':''}]
    monkeypatch.setattr('private_platform_reference.reference_for',lambda release:reference)
    names={'platform_action_scope','platform_scoped_read','get_platform_profile','get_platform_contractors','get_platform_awards','get_platform_subcontracts','get_spend_trend','get_pipeline_live'}
    nodes=[]
    for node in ast.parse(Path('main.py').read_text()).body:
        if isinstance(node,ast.FunctionDef) and node.name in names:
            node.decorator_list=[];nodes.append(node)
    ns=dict(Optional=Optional,List=List,Any=Any,Query=Query,HTTPException=HTTPException,math=math,pd=pd,
            GLOBAL_CACHE={'last_loaded':1,'is_loading':False},logger=Mock(),
            programme_read_df=lambda sql,params:db.execute(sql,params).fetchdf(),df_sanitize_for_json=lambda df:df.where(pd.notnull(df),None))
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'main.py','exec'),ns)
    return ns


@pytest.fixture
def dashboard(monkeypatch):
    db=duckdb.connect()
    db.execute('''CREATE TABLE v_transactions(award_key VARCHAR,transaction_key VARCHAR,source_system VARCHAR,contract_id VARCHAR,
        platform_family VARCHAR,year INTEGER,sub_agency VARCHAR,parent_agency VARCHAR,market_segment VARCHAR,psc VARCHAR,
        vendor_name VARCHAR,vendor_cage VARCHAR,spend_amount DOUBLE,action_date VARCHAR,description VARCHAR,naics_code VARCHAR)''')
    for key,label,year,buyer,domain,value,cage in [
        ('A','Virginia Class (SSN 774)',2025,'DEPARTMENT OF THE NAVY','AIR',100,'11111'),
        ('A','Virginia Class (SSN 774)',2025,'DEPARTMENT OF THE NAVY','AIR',0,'11111'),
        ('B','OLD WRONG',2025,'DEPARTMENT OF THE NAVY','AIR',50,'22222'),
        ('C','Virginia Class (SSN 774)',2024,'DEPARTMENT OF THE NAVY','AIR',900,'11111'),
        ('D','Virginia Class (SSN 774)',2025,'DEPARTMENT OF THE ARMY','AIR',0,'33333'),
        ('E','Virginia Class (SSN 774)',2025,'DEPARTMENT OF THE NAVY','OTHER',10,'11111'),
        ('G','Virginia Class (SSN 774)',2025,'DEPARTMENT OF THE NAVY','AIR',-5,'11111')]:
        db.execute('INSERT INTO v_transactions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',[key,key+str(value),'FPDS',key,label,year,buyer,'DEPARTMENT OF DEFENSE',domain,'1510','Vendor '+cage,cage,value,'2025-02-28','Test action','336413'])
    db.execute('''CREATE TABLE v_network(award_key VARCHAR,platform_family VARCHAR,year INTEGER,market_segment VARCHAR,psc VARCHAR,
        prime_name VARCHAR,prime_cage VARCHAR,sub_name VARCHAR,sub_cage VARCHAR,description VARCHAR,subaward_value DOUBLE,action_date VARCHAR,source_dedup_key VARCHAR)''')
    for key,year in [('A',2025),('A',2024),('D',2025)]:
        db.execute('INSERT INTO v_network VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',[key,'Virginia Class (SSN 774)',year,'AIR','1510','Prime','11111',"O'BRIEN COMPONENTS",'55555','Subcontract',20,'2025-02-28',key+str(year)])
    db.execute("CREATE TABLE v_summary AS SELECT '1510' AS psc_code,'Aircraft' AS psc_description")
    ns=routes(db,monkeypatch)
    yield db,ns
    db.close()


def test_overview_contractors_and_actions_use_the_same_v7_scope(dashboard):
    _,ns=dashboard
    scope=dict(name='VIRGINIA CLASS (SSN 774)',years=[2025],agency='Navy',domain='AIR')
    profile=ns['get_platform_profile'](**scope)
    assert profile['total_obligations']==145
    assert profile['funding_action_count']==4 and profile['contract_count']==3
    assert profile['contractor_count']==2
    vendors=ns['get_platform_contractors'](**scope)
    assert sum(row['total'] for row in vendors)==145
    actions=ns['get_platform_awards'](**scope)
    assert {row['contract_id'] for row in actions}=={'A','B','G'}
    assert any(row['spend']==-5 for row in actions) and any(row['spend']==0 for row in actions)
    assert all(row['total_actions']==4 for row in actions)
    assert len(actions)==profile['funding_action_count']
    assert ns['get_spend_trend'](platform=scope['name'],years=[2025],agency='Navy',domain='AIR',mode='yearly')==[{'label':2025,'spend':145}]


def test_mixed_case_native_subcontracts_preserve_year_buyer_domain_psc_and_names(dashboard):
    _,ns=dashboard;read=ns['get_platform_subcontracts']
    scope=dict(name='VIRGINIA CLASS (SSN 774)',years=[2025],agency='Navy',domain='AIR',psc='1510 - Aircraft')
    rows=read(**scope)
    assert len(rows)==1 and rows[0]['contract_count']==1
    assert rows[0]['sub_vendor']=="O'BRIEN COMPONENTS"
    assert len(read(**scope,mode='lines',sub_vendor="O'BRIEN COMPONENTS"))==1
    assert read(**{**scope,'years':[1900]})==[]
    assert read(**{**scope,'agency':'Air Force'})==[]
    assert read(**{**scope,'domain':'OTHER'})==[]
    assert read(**{**scope,'psc':'9999'})==[]


def test_action_paging_is_stable_complete_and_threshold_is_explicit(dashboard):
    _,ns=dashboard;read=ns['get_platform_awards'];scope=dict(name='Virginia Class (SSN 774)',years=[2025],agency='Navy',domain='AIR')
    all_rows=read(**scope);paged=[row for offset in range(0,4,2) for row in read(**scope,limit=2,offset=offset)]
    assert paged==all_rows
    assert read(**scope,threshold=0.000075)[0]['spend']==100
    with pytest.raises(HTTPException):read(**scope,threshold=float('nan'))
    assert read(**{**scope,'years':[2024]})==[] # reviewed clear suppresses the old label


def test_database_failure_and_generation_change_never_return_empty_success(dashboard):
    _,ns=dashboard
    ns['programme_read_df']=Mock(side_effect=RuntimeError('database busy'))
    with pytest.raises(HTTPException) as failure:ns['get_platform_awards']('Virginia Class (SSN 774)',years=[2025])
    assert failure.value.status_code==503
    def changed(sql,params):ns['GLOBAL_CACHE']['last_loaded']=2;return pd.DataFrame()
    ns['programme_read_df']=changed
    with pytest.raises(HTTPException) as failure:ns['platform_scoped_read']('SELECT 1',[])
    assert failure.value.status_code==503


def test_pipeline_classification_filters_work_with_numeric_source_columns(dashboard):
    from datetime import date,datetime
    db,ns=dashboard
    db.execute("""CREATE TABLE v_opportunities AS SELECT 'notice' AS id,'SPE123' AS sol_num,
        '2099-01-01' AS deadline,336413.0::DOUBLE AS naics,1510.0::DOUBLE AS psc,'Aircraft' AS title""")
    ns.update(date=date,datetime=datetime,duck_fetch_df=lambda sql,params:db.execute(sql,params).fetchdf(),
              active_solicitation_deadline_sql=lambda:'1=1',sanitize=lambda value:str(value).strip().upper())
    assert len(ns['get_pipeline_live'](naics='336413',years=None,limit=1))==1
    assert len(ns['get_pipeline_live'](domain='336',years=None,limit=1))==1
    assert len(ns['get_pipeline_live'](psc='1510',years=None,limit=1))==1
    assert ns['get_pipeline_live'](psc='9999',years=None,limit=1)==[]
