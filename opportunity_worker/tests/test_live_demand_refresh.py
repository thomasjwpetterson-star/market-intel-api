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
    put('dla_forecasts',"""SELECT '012345678' niin,'2026-11-01' forecast_month,10::BIGINT forecast_qty,'release' source_release,'source.txt' source_file,'row-1' source_item_id
    UNION ALL SELECT '012345678','2030-04-01',20,'release','source.txt','row-2'
    UNION ALL SELECT '012345678','2026-09-01',100,'release','source.txt','row-3'""")
    out=refresh_forecast_episodes(sources,tmp_path/'derived','2026-10-30')
    row=c.execute('SELECT episode_id,window_start,window_end,value_inputs_json FROM read_parquet(?)',[out['demand_candidates']['path']]).fetchone()
    assert row[:3]==('stable-forecast','2026-11-01','2030-04-01')
    assert json.loads(row[3])['forecast_quantity']==30

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
