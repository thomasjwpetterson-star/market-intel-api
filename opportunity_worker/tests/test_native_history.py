import json
import duckdb
from opportunity_engine.company_history import rebuild
from opportunity_engine.company_enrichment import scope_terms,scope_fragments,admissible
from opportunity_engine.demand_history import scope_key,work_mode

def test_native_profiles_replace_partial_history_and_preserve_ambiguous_sites(tmp_path):
    c=duckdb.connect();sources={}
    def source(name,sql):
        p=tmp_path/(name+'.parquet');c.execute("COPY ("+sql+") TO '"+str(p)+"' (FORMAT PARQUET)");sources[name]={'files':[{'path':str(p)}]}
    source('company_profiles',"SELECT 'CAGE:12345' company_id,'Old profile' company_name,'12345' cage_code,'old' profile_hash,1::BIGINT prime_awards,10.0 observed_prime_net_obligations,1::BIGINT subcontract_reports,NULL::VARCHAR first_prime_action,NULL::VARCHAR last_prime_action,NULL::VARCHAR last_subcontract_report,'old-item' item_evidence_digest,NULL::VARCHAR research_evidence_digest,'unknown' role_assessment")
    source('company_work',"SELECT 'old-doc' doc_id,'CAGE:12345' company_id,'prime_award' source_kind,'obsolete scope' work_text,'1680' psc,'336413' naics,'2020-01-01' last_observed,'old' sample_evidence_id,'old' evidence_digest")
    source('sam_company_capabilities',"SELECT 'SHAREDUEI001' uei,'12345' cage_code UNION ALL SELECT 'SHAREDUEI001','67890'")
    source('native_prime_awards',"""SELECT 'a1' award_key,'12345' cage_code,'SHAREDUEI001' uei,'New profile' company_name,'2025-01-01' first_action_date,'2026-01-01' latest_action_date,100.0 observed_net_obligations,'Y' manufacturer_of_goods,'336413' naics,'1680' psc,'Parent' parent_name,'PARENTUEI001' parent_uei,'a1-evidence' evidence_id,'Fuel boost pump' work_text,'Fuel boost pump' latest_description,'Air Force' buyer_name,'Office' buyer_office,'NONE' programme_name
    UNION ALL SELECT 'a2','12345','SHAREDUEI001','New profile','2025-01-02','2026-02-01',-20.0,'Y','336413','1680','Parent','PARENTUEI001','a2-evidence','Hydraulic actuation manufacture','Hydraulic actuation manufacture','Air Force','Office','NONE'
    UNION ALL SELECT 'a3','67890','SHAREDUEI001','Other site','2025-01-02','2026-02-01',50.0,'Y','336413','1680','Parent','PARENTUEI001','a3-evidence','Fuel boost pump','Fuel boost pump','Air Force','Office','NONE'""")
    source('native_subcontract_reports',"SELECT 'report' report_key,'SHAREDUEI001' sub_uei,'PRIMEUEI0001' prime_uei,'Supplier' sub_name,'Prime' prime_name,'2026-01-01' event_date,'Fuel boost pump' work_text,'report-evidence' evidence_id")
    result=rebuild(sources,tmp_path/'derived','2026-10-30')
    profiles=c.execute("SELECT company_id,prime_awards,observed_prime_net_obligations,subcontract_reports,item_evidence_digest FROM read_parquet(?) ORDER BY company_id",[result['company_profiles']['path']]).fetchall()
    assert profiles==[('CAGE:12345',2,80.0,0,'old-item'),('CAGE:67890',1,50.0,0,None),('UEI:SHAREDUEI001',0,None,1,None)]
    assert c.execute('SELECT site_resolution FROM read_parquet(?)',[result['company_subcontract_facts']['path']]).fetchone()[0]=='multiple_sites_uei_only'
    assert c.execute("SELECT count(*) FROM read_parquet(?) WHERE work_text='obsolete scope'",[result['company_work']['path']]).fetchone()[0]==0

def test_product_supply_and_specific_manufacturing_are_usable_but_admin_is_not():
    assert scope_terms('Manufacturing aircraft fuel boost pumps')==['boost','fuel','pump']
    assert scope_terms('GOVERNMENT FIRST ARTICLE TEST')==[]
    assert scope_key('8512204787!FUEL BOOST PUMP')==scope_key('Repair of aircraft fuel boost pump')
    assert work_mode('repair fuel boost pump','1680')=='repair'
    assert work_mode('fuel boost pump','1680')=='supply_or_unspecified'

def test_long_work_and_notice_body_retain_evidence_without_transferring_qualification():
    text='This contract provides specialist engineering and manufacturing services across several facilities and includes the following work. Fuel boost pump overhaul; centrifugal clutch overhaul and repair.'
    assert any('fuel' in scope_terms(x) for x in scope_fragments(text))
    assert admissible('Fuel boost pump overhaul',{'title':'Aircraft support requirement','scope_text':'Seeking sources for fuel boost pump overhaul','lane':'published_procurement'},'qualification_opening')
    assert not admissible('F-16 radar repair',{'title':'F-35 support','scope_text':'Seeking sources for F-35 radar repair','lane':'published_procurement'},'qualification_opening')
