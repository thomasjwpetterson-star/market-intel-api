import json
import duckdb
from opportunity_engine.demand_history import refresh_forecast_episodes
from opportunity_engine.rules import evaluate
from opportunity_engine.company_enrichment import admissible

def test_full_native_forecast_horizon_and_stable_existing_identity(tmp_path):
    c=duckdb.connect();sources={}
    def put(name,sql):
        p=tmp_path/(name+'.parquet');c.execute("COPY ("+sql+") TO '"+str(p)+"' (FORMAT PARQUET)");sources[name]={'files':[{'path':str(p)}]}
    put('demand_candidates',"""SELECT 'stable-forecast' episode_id,'old-hash' record_hash,'source_item_forecasts' lane,'Prior forecast' title,'Forecast' scope_text,'DLA' customer_name,NULL::VARCHAR programme,NULL::VARCHAR psc,NULL::VARCHAR naics,'2026-11-01' window_start,'2028-04-01' window_end,'source months' date_basis,NULL::DOUBLE value_low,NULL::DOUBLE value_median,NULL::DOUBLE value_high,'basis' value_basis,'scope' value_scope,'{"niin":"012345678"}' value_inputs_json,'DLA' source_name,'https://example.gov/forecast' source_url,'old' source_evidence_id,'file' source_locator,'forecast' procurement_state,'unresolved' reconciled_state,'unknown' access_state,'why' rationale,'limitations' limitations""")
    put('dla_forecasts',"""SELECT '012345678' niin,DATE '2026-11-01' forecast_month,10::BIGINT forecast_qty,'release' source_release,'source.txt' source_file,'row-1' source_item_id
    UNION ALL SELECT '012345678',DATE '2030-04-01',20,'release','source.txt','row-2'
    UNION ALL SELECT '012345678',DATE '2026-09-01',100,'release','source.txt','row-3'
    UNION ALL SELECT '012345678',DATE '2031-05-01',-9,'release','source.txt','later-correction'
    UNION ALL SELECT NULL,DATE '2026-11-01',999,'release','source.txt','unmapped-row'
    UNION ALL SELECT 'invalid',DATE '2026-11-01',999,'release','source.txt','invalid-row'""")
    out=refresh_forecast_episodes(sources,tmp_path/'derived','2026-10-30')
    row=c.execute('SELECT episode_id,window_start,window_end,value_inputs_json,record_hash FROM read_parquet(?)',[out['demand_candidates']['path']]).fetchone()
    assert row[:3]==('stable-forecast','2026-11-01','2030-04-01')
    assert json.loads(row[3])['forecast_quantity']==30
    assert c.execute('SELECT count(*) FROM read_parquet(?)',[out['demand_candidates']['path']]).fetchone()[0]==1
    assert out['demand_candidates']['forecast_identity_quality']['unmapped_positive_future_rows']==2
    assert c.execute('SELECT count(*) FROM read_parquet(?)',[sources['dla_forecasts']['files'][0]['path']]).fetchone()[0]==6
    # A signed correction outside the positive window is still a source change.
    revised=tmp_path/'revised-forecast.parquet'
    c.execute("COPY (SELECT * REPLACE(CASE WHEN source_item_id='later-correction' THEN -8 ELSE forecast_qty END AS forecast_qty) FROM read_parquet(?)) TO '"+str(revised).replace("'","''")+"' (FORMAT PARQUET)",[sources['dla_forecasts']['files'][0]['path']])
    second=dict(sources,dla_forecasts={'files':[{'path':str(revised)}]})
    updated=refresh_forecast_episodes(second,tmp_path/'derived-revised','2026-10-30')
    version=c.execute('SELECT episode_id,record_hash,value_inputs_json FROM read_parquet(?)',[updated['demand_candidates']['path']]).fetchone()
    assert version[0]==row[0] and version[1]!=row[4]
    assert json.loads(version[2])['forecast_quantity']==30

def test_observed_supplier_placement_resolves_expected_work_with_actual_evidence():
    o={'lane':'new_prime_historical_supplier_flow','source_evidence_id':'new-prime','record_hash':'version','customer_name':'Prime','scope_text':'Fuel boost pump','window_end':'2029-12-31','reconciled_state':'source_confirmed_award','value_inputs_json':json.dumps({'new_prime_evidence':'new-prime','prior_awards':2,'source_reports':3,'support_digest':'support','first_award_date':'2026-05-01','outcome_evidence_ids':['actual-subcontract-report']})}
    d=evaluate(o,'2026-10-30')
    assert d['stage']=='resolved_or_elapsed'
    assert d['negative_evidence'][0]['evidence_id']=='actual-subcontract-report'
    assert 'actual-subcontract-report' in d['source_basis']['evidence_ids']

def test_parent_program_context_does_not_transfer_work_to_an_incompatible_platform():
    o={'title':'Fuel boost pump package','scope_text':'Fuel boost pump for F-35','lane':'new_prime_historical_supplier_flow'}
    assert not admissible('Fuel boost pump',o,'emerging_demand','["F-16"]')
    assert admissible('Fuel boost pump',o,'emerging_demand','["F-35"]')


def test_two_term_supplier_scope_requires_actual_supplier_lineage():
    opportunity={'title':'Circuit card package','scope_text':'Circuit card for F-35','lane':'new_prime_historical_supplier_flow'}
    assert not admissible('Circuit card',opportunity,'emerging_demand','["F-35"]')
    assert admissible('Circuit card',opportunity,'emerging_demand','["F-35"]',True)
    assert not admissible('Circuit card',opportunity,'emerging_demand','["F-16"]',True)


def test_entry_route_does_not_turn_equivalent_hardware_into_accessible_work():
    from opportunity_engine.company_enrichment import company_entry_route
    lane='new_prime_historical_supplier_flow'
    scope='Fuel boost pump. Current funded scope example: F-16 production option exercised'
    assert company_entry_route('Fuel boost pump','Fuel boost pump',scope,lane)=='entry_route_unestablished'
    assert company_entry_route('Fuel boost pump','Fuel boost pump',scope,lane,True)=='reported_supplier_relationship'
    assert company_entry_route('Fuel boost pump','Fuel boost pump','Current funded scope example: Prototype development of fuel boost pump',lane)=='development_or_source_opening_context'
    assert company_entry_route('Software testing services','Software testing services',scope,lane)=='defined_services_scope'
