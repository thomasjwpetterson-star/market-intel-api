import json
import pytest
from opportunity_engine.financial_readiness import guard,validated,risk_sql,LANES,VALUE_BASIS,SOURCE_VIEW


def manifest(valid=False):
    return dict(run_id='test',as_of='2026-11-01',tables={name:dict(table='oi_'+name,columns=[dict(Name=k) for k in ('value_basis','included_in_adjusted_total','source_report_identity')]) for name in ['company_subcontract_facts','company_matches','opportunities','demand_decisions','company_history_coverage','company_opportunity_events']},subcontract_opportunity_finance=dict(state='validated' if valid else 'not_validated',value_basis=VALUE_BASIS,source_view=SOURCE_VIEW,validation_query_id='reconciled-query'))


@pytest.mark.parametrize('lane',LANES)
@pytest.mark.parametrize('amount',[200000,39157943915794])
def test_unreconciled_subcontract_values_are_withheld_systematically_not_by_size(lane,amount):
    raw=dict(lane=lane,episode_id='a'*64,record_hash='b'*64,value_low=amount,value_median=amount,value_high=amount,commercial_value=amount,value_inputs_json=json.dumps({'comparators':[{'reported_amount':amount,'source_url':'https://example.test/report'}]}))
    result=guard(raw,manifest())
    assert result['financial_validation']=='not_validated'
    assert all(result[k] is None for k in ['value_low','value_median','value_high','commercial_value'])
    assert json.loads(result['unvalidated_financial_evidence_json'])['amounts']['value_median']==amount
    assert result['value_inputs_json']==raw['value_inputs_json'] and result['record_hash']==raw['record_hash']
    assert guard(result,manifest())==result
    assert raw['value_median']==amount  # read projection cannot mutate the source record.


@pytest.mark.parametrize('lane',['funded_contract_follow_on','recurring_government_work','published_procurement','source_item_forecasts','agency_acquisition_forecasts'])
def test_other_authorities_and_large_legitimate_prime_references_are_unchanged(lane):
    row=dict(lane=lane,value_median=10**12,commercial_value=10**12,government_reference_unit_price=84079.48)
    assert guard(row,manifest())==row


def test_unmasking_requires_explicit_reconciliation_and_the_correct_source_schema():
    m=manifest(True)
    assert validated(m)
    assert guard(dict(lane=LANES[0],value_median=2e6),m)['value_median']==2e6
    m['tables']['company_subcontract_facts']['columns']=[]
    assert not validated(m)
    assert guard(dict(lane=LANES[0],value_median=2e6),m)['value_median'] is None
    assert risk_sql(manifest(True))=='false'
    assert all(lane in risk_sql(manifest()) for lane in LANES)


def test_report_evidence_and_change_events_keep_identity_but_not_unvalidated_highlights():
    r=guard(dict(report_key='report',reported_amount=39157943915794,work_text='Satellite equipment'),manifest(),source_report=True)
    assert r['reported_amount'] is None and r['work_text']=='Satellite equipment' and r['report_key']=='report'
    r=guard(dict(lane=LANES[0],previous_value_median=10,current_value_median=30,event_id='event'),manifest())
    assert r['previous_value_median'] is None and r['current_value_median'] is None and r['event_id']=='event'


@pytest.mark.parametrize('module',['opportunity_engine.api','opportunity_intelligence.api'])
def test_list_detail_and_sql_ranking_use_the_same_guard(module):
    import importlib
    api=importlib.import_module(module)
    raw=dict(episode_id='a'*64,record_hash='b'*64,lane=LANES[0],title='Recurring subcontract demand',value_median=39157943915794,commercial_value=39157943915794,value_basis='historical_recurring_package_benchmark',value_inputs_json='{}')
    class Local(api.Repository):
        def __init__(self):self.queries=[]
        def manifest(self):return manifest()
        def rows(self,sql):self.queries.append(sql);return [dict(raw)]
    repo=Local()
    assert repo.opportunities(company='CAGE:ABCDE')[0]['value_median'] is None
    assert repo.detail('a'*64)[0]['commercial_value'] is None
    sql=repo.opportunities(company='CAGE:ABCDE',_query=True)
    assert 'THEN 0.0 ELSE' in sql and all(lane in sql for lane in LANES)
    assert any('withheld' in x or 'not_validated' in x for x in [repo.detail('a'*64)[0]['value_basis'],repo.detail('a'*64)[0]['financial_validation']])


def test_main_raw_report_and_value_evidence_are_explicitly_unvalidated():
    from opportunity_intelligence.api import Repository
    class Local(Repository):
        def __init__(self):pass
        def manifest(self):return manifest()
        def rows(self,sql):return [dict(episode_id='a'*64,record_hash='b'*64,lane=LANES[1],report_key='report',reported_amount=2000000,value_median=2000000,value_inputs_json='{}')]
    repo=Local()
    assert repo.company_history('ABCDE','subcontracts')[0]['reported_amount'] is None
    assert repo.value_evidence('a'*64)['financial_validation']=='not_validated'
    assert repo.company_events('ABCDE')[0]['financial_validation']=='not_validated'
