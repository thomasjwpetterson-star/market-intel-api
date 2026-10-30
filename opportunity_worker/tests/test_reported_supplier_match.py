"""An evidenced prior supplier does not depend on short-text search or previews."""
import hashlib,json
import duckdb
from opportunity_engine.company_enrichment import augment

def test_reported_supplier_edge_is_retained_without_a_lexical_hit(tmp_path):
    c=duckdb.connect();tables={};sources={}
    def put(name,sql,source=False):
        p=tmp_path/(name+'.parquet');c.execute("COPY ("+sql+") TO '"+str(p)+"' (FORMAT PARQUET)")
        entry=dict(path=str(p),files=[dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest())])
        (sources if source else tables)[name]=entry
    put('company_profiles',"SELECT 'CAGE:45934' company_id,'Ontic' company_name,'profile' profile_hash")
    put('opportunities',"SELECT 'private' episode_id,'current' record_hash,'new_prime_historical_supplier_flow' lane,'Sensor platform work' title,'Current funded scope example: Sensor platform production' scope_text,'Prime' customer_name,NULL::VARCHAR value_inputs_json")
    put('demand_decisions',"SELECT 'private' episode_id,'emerging_demand' stage")
    put('company_matches',"SELECT NULL::VARCHAR match_id,NULL::VARCHAR company_id,NULL::VARCHAR episode_id,NULL::DOUBLE score,NULL::VARCHAR match_route,NULL::VARCHAR matched_terms,NULL::VARCHAR company_evidence_caveat,NULL::VARCHAR sample_evidence_id,NULL::VARCHAR match_hash,NULL::VARCHAR opportunity_hash,NULL::VARCHAR profile_hash WHERE false")
    put('company_work',"SELECT sha256('CAGE:45934|subaward_report|SENSORPLATFORM') doc_id,'CAGE:45934' company_id,'subaward_report' source_kind,'SENSOR PLATFORM' work_text,NULL::VARCHAR psc,NULL::VARCHAR naics,'2025-06-01' last_observed,'old-report' sample_evidence_id,'digest' evidence_digest",True)
    put('supplier_pull_report_evidence',"SELECT 'private' episode_id,'CAGE:45934' supplier_company_id,'proof-report' evidence_id,'2025-06-01' event_date,'SENSOR PLATFORM' work_text",True)
    (tmp_path/'manifest.json').write_text(json.dumps(dict(tables=tables,sources=sources,as_of='2026-10-30')))
    augment(tmp_path)
    row=c.execute('SELECT match_route,sample_evidence_id,review_category,company_entry_status FROM read_parquet(?)',[str(tmp_path/'company_work_matches.parquet')]).fetchone()
    assert row==('reported-supplier-lineage-v1','proof-report','work_lead','reported_supplier_relationship')
