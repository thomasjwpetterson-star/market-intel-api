import hashlib
import io
import json
import zipfile
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from opportunity_engine.agency_forecasts import (FIELDS,normalize,fiscal_period,value_band,
    workbook_rows,deduplicate,write_source,prepare,link_incumbents)
from opportunity_engine import rules


def test_source_fiscal_precision_and_bands_do_not_invent_points_or_deadlines():
    assert fiscal_period('FY27','Q1')==('2026-10-01','2026-12-31','fiscal_quarter')
    assert fiscal_period('FY 2028','QTR 3')==('2028-04-01','2028-06-30','fiscal_quarter')
    assert fiscal_period('FY27','TBD')==('2026-10-01','2027-09-30','fiscal_year')
    assert fiscal_period('TBD','Q2')==(None,None,'unresolved')
    assert value_band('> $250M - < $1B')==(250e6,1e9,None,'source_range')
    assert value_band('< $2M')==(None,2e6,None,'source_upper_bound')
    assert value_band('> $1B+')==(1e9,None,None,'source_lower_bound')
    assert value_band('700K')==(700e3,700e3,700e3,'source_stated_amount')
    assert value_band('< $7.5M - $50M')==(None,None,None,'unresolved')
    plan=dict(lane='explicit_work_package_plan',source_name='agency_acquisition_forecasts',source_evidence_id='source',customer_name='Navy',window_start='2027-07-01',window_end='2028-06-30',procurement_state='agency_planned_requirement')
    decision=rules.evaluate(plan,'2026-10-31')
    assert decision['stage']=='emerging_demand' and not decision['response_window_verified']
    assert decision['company_addressable_value'] is None
    for state in ('agency_plan_schedule_revalidation_required','agency_plan_newer_document_reconciliation_required'):
        assert rules.evaluate(dict(plan,procurement_state=state),'2026-10-31')['stage']=='evidence_gap'
    assert rules.evaluate(dict(plan,procurement_state='agency_plan_resolved_to_published_notice'),'2026-10-31')['stage']=='resolved_or_elapsed'


def test_workbook_requires_named_headers_and_bounds_styled_empty_columns():
    ns='http://schemas.openxmlformats.org/spreadsheetml/2006/main'
    header={0:'Requirement Title',3:'Anticipated Total Value (Including Options)',28:'PID Number'}
    content={0:'Avionics development',1:'Develop avionics',7:'N00019 - HQ',28:'PLAN-1'}
    def cell(index,value):
        letter='';n=index+1
        while n: n,k=divmod(n-1,26);letter=chr(65+k)+letter
        return f'<c r="{letter}1" t="inlineStr"><is><t>{value}</t></is></c>'
    xml=f'<worksheet xmlns="{ns}"><dimension ref="A1:XFC1706"/><sheetData>'
    xml+='<row r="8">'+''.join(cell(k,v) for k,v in header.items())+'</row>'
    xml+='<row r="10">'+''.join(cell(k,v) for k,v in content.items())+cell(16000,'ignore')+'</row></sheetData></worksheet>'
    raw=io.BytesIO()
    with zipfile.ZipFile(raw,'w') as z:z.writestr('xl/worksheets/sheet1.xml',xml)
    rows=list(workbook_rows(raw.getvalue()))
    assert len(rows)==1 and rows[0][0]['native_plan_id']=='PLAN-1'
    assert rows[0][1]=='xl/worksheets/sheet1.xml:row 10'


@pytest.mark.parametrize('ambiguous',[False,True])
def test_actual_preparation_preserves_bands_and_requires_one_incumbent_site(tmp_path,ambiguous):
    record=dict(title='F/A-18 Sensor System Support',description='Hardware and software anomaly investigation, development and test',buying_office='N68936 / NAWCWD China Lake',value_band='> $250M - < $1B',procurement_method='Sole-Source',prior_contract='N6893623D0034',incumbent='Raytheon Company',solicitation_fy='FY2027',solicitation_quarter='Q4',award_fy='FY2028',award_quarter='Q3')
    row=normalize(record,'https://www.navair.navy.mil/source.pdf','a'*64,'page 1, row 4','2026-10-31T12:00:00Z','nawcwd')
    changed=normalize(dict(record,award_fy='FY2029'),row['source_url'],'b'*64,'page 2, row 4',row['source_observed_at'],'nawcwd')
    assert changed['forecast_id']==row['forecast_id'] and changed['record_hash']!=row['record_hash']
    assert all(x['status']=='duplicate_identity_review' for x in deduplicate([dict(row),dict(changed)]))
    source=write_source([row],tmp_path/'source',[],[],row['source_observed_at']);c=duckdb.connect()
    schemas=json.loads((Path(__file__).parent/'fixtures/validation_schemas.json').read_text())
    fields=schemas['opportunities']
    # Production opportunity shape is compatible with the retained demand source.
    types={'string':'VARCHAR','double':'DOUBLE','bigint':'BIGINT','boolean':'BOOLEAN'}
    path=tmp_path/'prior.parquet';columns=','.join('NULL::'+types[x['Type']]+' "'+x['Name']+'"' for x in fields)
    c.execute('COPY (SELECT '+columns+' WHERE false) TO ? (FORMAT PARQUET)',[str(path)])
    sources={'agency_acquisition_forecasts':source,'demand_candidates':{'files':[{'path':str(path)}]},'company_award_facts':{'table':'oi_award_source'}}
    a=dict(award_key='award',piid='N6893623D0034',cage_code='4U884',uei='NBSLHP77ZJQ1',company_name='Raytheon Company',work_text='F/A-18 engineering support',psc='K016',naics='541330',latest_action_date='2026-07-14',evidence_id='award-evidence',source_url='https://www.usaspending.gov/award/source')
    aws=SimpleNamespace(result=lambda sql:([a,dict(a,cage_code='15090')] if ambiguous else [a],{'QueryExecutionId':'query'}))
    out=prepare(sources,tmp_path/'derived','2026-10-31',aws)
    q=c.execute('SELECT * FROM read_parquet(?)',[out['demand_candidates']['path']]);actual=dict(zip([x[0] for x in q.description],q.fetchone()))
    assert actual['value_low']==250e6 and actual['value_high']==1e9 and actual['value_median'] is None
    assert actual['window_start']=='2027-07-01' and actual['window_end']=='2028-06-30'
    inputs=json.loads(actual['value_inputs_json']);assert inputs['company_addressable_value'] is None
    assert inputs['incumbent_company_ids']==([] if ambiguous else ['CAGE:4U884'])
    assert out['agency_forecast_incumbents']['rows']==(0 if ambiguous else 1)


def test_incumbent_link_is_exact_site_current_version_and_retention_route(tmp_path):
    c=duckdb.connect();tables={}
    def put(name,sql):
        path=tmp_path/(name+'.parquet');c.execute('COPY ('+sql+') TO ? (FORMAT PARQUET)',[str(path)]);tables[name]={'path':str(path)}
    put('company_profiles',"SELECT 'CAGE:4U884' company_id,'profile' profile_hash")
    put('opportunities',"SELECT 'episode' episode_id,'current' record_hash,'agency_acquisition_forecasts' source_name,'F/A-18' programme")
    put('demand_decisions',"SELECT 'episode' episode_id,'emerging_demand' stage")
    put('agency_forecast_incumbents',"SELECT 'CAGE:4U884' company_id,'episode' episode_id,'N6893623D0034' piid,'award-evidence' evidence_id,'award-document' award_key,'F/A-18 avionics test support' work_text,'2026-07-14' latest_action_date")
    put('company_matches',"SELECT NULL::VARCHAR match_id,NULL::VARCHAR company_id,NULL::VARCHAR episode_id,NULL::DOUBLE score,NULL::VARCHAR match_route,NULL::VARCHAR matched_terms,NULL::VARCHAR company_evidence_caveat,NULL::VARCHAR sample_evidence_id,NULL::VARCHAR match_hash,NULL::VARCHAR opportunity_hash,NULL::VARCHAR profile_hash WHERE false")
    put('company_work_matches',"SELECT NULL::VARCHAR company_id,NULL::VARCHAR episode_id,NULL::VARCHAR doc_id WHERE false")
    (tmp_path/'manifest.json').write_text(json.dumps({'tables':tables}));link_incumbents(tmp_path)
    assert c.execute('SELECT company_id,opportunity_hash FROM read_parquet(?)',[str(tmp_path/'company_matches.parquet')]).fetchall()==[('CAGE:4U884','current')]
    row=c.execute('SELECT doc_id,company_entry_status,company_evidence_caveat FROM read_parquet(?)',[str(tmp_path/'company_work_matches.parquet')]).fetchone()
    assert row[0]=='award-document' and row[1]=='documented_incumbent_contract'
    assert 'not a promised award or transferable qualification' in row[2]
