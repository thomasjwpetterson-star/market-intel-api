import json
import duckdb
from opportunity_engine.demand_history import rebuild

def test_new_prime_pull_requires_exact_lineage_and_keeps_package_values_separate(tmp_path):
    c=duckdb.connect();sources={}
    def source(name,sql):
        p=tmp_path/(name+'.parquet');c.execute("COPY ("+sql+") TO '"+str(p)+"' (FORMAT PARQUET)");sources[name]={'files':[{'path':str(p)}]}
    source('company_award_facts',"""SELECT 'old-award' award_key,'old-piid' piid,'vehicle-1' parent_piid,'9700' parent_agency,'PRIMEUEI0001' uei,'Prime' company_name,'2024-01-01' first_action_date,'2025-01-01' latest_action_date,'2027-12-31' period_end,'2028-12-31' potential_end,'2024-01-01' period_start,1000000.0 observed_net_obligations,'Technical aircraft support' work_text,'1680' psc,'336413' naics,'NONE' programme_name,'000' programme_code,'old-evidence' evidence_id,'Office' buyer_office,'office-code' buyer_office_code,'Air Force' buyer_name,'https://example.gov/old' source_url,1::BIGINT action_count
    UNION ALL SELECT 'new-award','new-piid','vehicle-1','9700','PRIMEUEI0001','Prime','2026-06-01','2026-06-01','2029-12-31','2030-12-31','2026-06-01',90000000.0,'Technical aircraft support','1680','336413','NONE','000','new-evidence','Office','office-code','Air Force','https://example.gov/new',1
    UNION ALL SELECT 'unrelated','other-piid','vehicle-2','9700','PRIMEUEI0001','Prime','2026-06-01','2026-06-01','2029-12-31','2030-12-31','2026-06-01',500000000.0,'Technical aircraft support','1680','336413','NONE','000','other-evidence','Office','office-code','Air Force','https://example.gov/other',1""")
    source('company_subcontract_facts',"""SELECT 'report-1' report_key,'old-award' parent_award_key,'old-piid' parent_piid,'PRIMEUEI0001' prime_uei,'SUBUEI000001' sub_uei,'Supplier' sub_name,'CAGE:12345' company_id,'sub-1' subaward_number,'2024-07-01' modified_date,'2024-06-01' event_date,'Fuel boost pump' work_text,120000.0 reported_amount,'report-evidence-1' evidence_id,'https://example.gov/report' source_url
    UNION ALL SELECT 'report-2','old-award','old-piid','PRIMEUEI0001','SUBUEI000001','Supplier','CAGE:12345','sub-1','2025-07-01','2024-06-01','Fuel boost pump',150000.0,'report-evidence-2','https://example.gov/report'""")
    source('demand_candidates',"""SELECT 'legacy' episode_id,'hash' record_hash,'new_prime_historical_supplier_flow' lane,'Old placeholder' title,'old' scope_text,'Prime' customer_name,NULL::VARCHAR programme,'1680' psc,'336413' naics,'2026-01-01' window_start,'2027-01-01' window_end,'basis' date_basis,1.0::DOUBLE value_low,1.0::DOUBLE value_median,1.0::DOUBLE value_high,'basis' value_basis,'scope' value_scope,'{}' value_inputs_json,'old' source_name,'https://example.gov' source_url,'old' source_evidence_id,'old' source_locator,'old' native_identifier,'old' procurement_state,'old' reconciled_state,'old' access_state,'old' rationale,'old' limitations""")
    out=rebuild(sources,tmp_path/'derived','2026-10-30')
    rows=c.execute('SELECT native_identifier,value_median,value_inputs_json FROM read_parquet(?)',[out['demand_candidates']['path']]).fetchall()
    assert len(rows)==1 and rows[0][:2]==('new-piid',150000.0)
    inputs=json.loads(rows[0][2]);assert inputs['source_reports']==1
    assert inputs['new_prime_observed_net']==90000000.0
    assert inputs['preview'][0]['report_evidence']=='report-evidence-2'
    assert inputs['preview'][0]['supplier_company_id']=='CAGE:12345'
