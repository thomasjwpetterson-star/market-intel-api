"""Build complete company work, award and supplier facts from native projections."""
import hashlib,json
from pathlib import Path
import duckdb

def rebuild(sources,folder,as_of):
    if 'native_company_prime_stats' in sources:
        from .company_history_distributed import rebuild as compact_rebuild
        return compact_rebuild(sources,folder,as_of)
    if 'native_prime_awards' not in sources or 'native_subcontract_reports' not in sources:
        raise ValueError('Both native award and subcontract histories are required')
    folder=Path(folder);folder.mkdir(parents=True,exist_ok=True)
    c=duckdb.connect();c.execute("SET threads=2;SET memory_limit='1800MB';SET preserve_insertion_order=false")
    c.execute("SET temp_directory='"+str(folder/'tmp').replace("'","''")+"'");c.execute("SET max_temp_directory_size='20GB'")
    for name in ('native_prime_awards','native_subcontract_reports','company_profiles','company_work','sam_company_capabilities'):
        if name in sources:c.read_parquet([f['path'] for f in sources[name]['files']]).create_view(name)
    c.execute("CREATE MACRO valid_cage(x) AS CASE WHEN regexp_full_match(upper(trim(x)),'[A-Z0-9]{5}') THEN upper(trim(x)) END")
    c.execute("CREATE MACRO norm(x) AS regexp_replace(upper(regexp_replace(coalesce(x,''),'^[0-9]{8,12}!\\s*','')),'[^A-Z0-9]','','g')")
    c.execute("""CREATE TEMP TABLE identity AS SELECT uei,CASE WHEN count(DISTINCT cage_code)=1 THEN min(cage_code) END cage_code,count(DISTINCT cage_code) candidate_sites
    FROM (SELECT uei,valid_cage(cage_code) cage_code FROM native_prime_awards
    UNION SELECT uei,valid_cage(cage_code) FROM sam_company_capabilities)
    WHERE nullif(trim(uei),'') IS NOT NULL AND cage_code IS NOT NULL GROUP BY uei""")
    c.execute("""CREATE TEMP VIEW prime AS SELECT *,CASE WHEN valid_cage(cage_code) IS NOT NULL THEN 'CAGE:'||valid_cage(cage_code) WHEN nullif(trim(uei),'') IS NOT NULL THEN 'UEI:'||uei END company_id FROM native_prime_awards""")
    c.execute("""CREATE TEMP VIEW subs AS SELECT s.*,CASE WHEN i.cage_code IS NOT NULL THEN 'CAGE:'||i.cage_code WHEN nullif(s.sub_uei,'') IS NOT NULL THEN 'UEI:'||s.sub_uei END company_id,
    i.cage_code,CASE WHEN i.cage_code IS NOT NULL THEN 'unique_observed_uei_cage' WHEN i.candidate_sites>1 THEN 'multiple_sites_uei_only' ELSE 'unresolved_uei_only' END site_resolution,
    p.cage_code prime_cage,p.candidate_sites prime_site_candidates
    FROM native_subcontract_reports s LEFT JOIN identity i ON i.uei=s.sub_uei LEFT JOIN identity p ON p.uei=s.prime_uei""")
    outputs={}
    def output(name,sql):
        path=folder/(name+'.parquet');c.execute("COPY ("+sql+") TO '"+str(path).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)")
        count=c.execute('SELECT count(*) FROM read_parquet(?)',[str(path)]).fetchone()[0]
        outputs[name]=dict(path=str(path),rows=count,sha256=hashlib.sha256(path.read_bytes()).hexdigest(),files=[dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest())],as_of=as_of,kind='native_warehouse_complete_derived',status='SUCCEEDED')
        c.read_parquet(str(path)).create_view('new_'+name);print(name,count,flush=True)
    output('company_award_facts',"SELECT * FROM prime WHERE company_id IS NOT NULL")
    output('company_subcontract_facts',"SELECT * FROM subs WHERE company_id IS NOT NULL")
    # Stable work records keep every substantive scope, with a path to all
    # supporting awards/reports rather than an undocumented sample limit.
    output('company_work',"""WITH raw AS (
    SELECT company_id,'prime_award' source_kind,work_text,psc,naics,latest_action_date last_observed,evidence_id FROM prime WHERE company_id IS NOT NULL
    UNION ALL SELECT company_id,'latest_prime_action',latest_description,psc,naics,latest_action_date,evidence_id FROM prime WHERE company_id IS NOT NULL AND norm(latest_description)<>norm(work_text)
    UNION ALL SELECT company_id,'subaward_report',work_text,NULL,NULL,event_date,evidence_id FROM subs WHERE company_id IS NOT NULL
    UNION ALL SELECT company_id,source_kind,work_text,psc,naics,last_observed,sample_evidence_id FROM company_work WHERE source_kind NOT IN ('prime_award','latest_prime_action','subaward_report')
    ), grouped AS(SELECT company_id,source_kind,norm(work_text) scope_key,psc,naics,min(work_text) work_text,max(last_observed) last_observed,
    min(evidence_id) sample_evidence_id,sha256(string_agg(DISTINCT evidence_id,'|' ORDER BY evidence_id)) evidence_digest,count(*) source_records
    FROM raw WHERE length(norm(work_text))>=8 GROUP BY company_id,source_kind,norm(work_text),psc,naics)
    SELECT sha256(concat_ws('|',company_id,source_kind,scope_key,psc,naics)) doc_id,company_id,source_kind,work_text,psc,naics,last_observed,sample_evidence_id,evidence_digest,source_records FROM grouped""")
    c.execute("""CREATE TEMP TABLE prime_stats AS SELECT company_id,arg_max(company_name,latest_action_date) company_name,arg_max(cage_code,latest_action_date) cage_code,arg_max(uei,latest_action_date) uei,
    count(*) prime_awards,sum(observed_net_obligations) observed_prime_net_obligations,min(first_action_date) first_prime_action,max(latest_action_date) last_prime_action,
    count(*) FILTER(WHERE lower(manufacturer_of_goods) IN ('t','true','y','yes')) manufacturer_declared_awards,
    count(*) FILTER(WHERE left(naics,2) IN ('31','32','33')) manufacturing_naics_awards,count(*) FILTER(WHERE left(naics,2)='42') wholesale_naics_awards,
    arg_max(parent_name,latest_action_date) parent_name,arg_max(parent_uei,latest_action_date) parent_uei,sha256(string_agg(evidence_id,'|' ORDER BY evidence_id)) prime_evidence_digest
    FROM prime WHERE company_id IS NOT NULL GROUP BY company_id""")
    c.execute("""CREATE TEMP TABLE sub_stats AS SELECT company_id,arg_max(sub_name,event_date) company_name,count(*) subcontract_reports,count(DISTINCT coalesce(nullif(prime_uei,''),prime_name)) commercial_customers,max(event_date) last_subcontract_report,
    sha256(string_agg(evidence_id,'|' ORDER BY evidence_id)) sub_evidence_digest FROM subs WHERE company_id IS NOT NULL GROUP BY company_id""")
    c.execute("CREATE TEMP VIEW profile_keys AS SELECT company_id FROM company_profiles UNION SELECT company_id FROM prime_stats UNION SELECT company_id FROM sub_stats")
    replacements={
        'company_id':'k.company_id','company_name':'coalesce(a.company_name,b.company_name,p.company_name)',
        'cage_code':"CASE WHEN starts_with(k.company_id,'CAGE:') THEN substr(k.company_id,6) END",
        'unresolved_uei':"CASE WHEN starts_with(k.company_id,'UEI:') THEN substr(k.company_id,5) END",
        'prime_awards':'coalesce(a.prime_awards,0)','observed_prime_net_obligations':'a.observed_prime_net_obligations',
        'first_prime_action':'a.first_prime_action','last_prime_action':'a.last_prime_action',
        'subcontract_reports':'coalesce(b.subcontract_reports,0)','commercial_customers':'coalesce(b.commercial_customers,0)','last_subcontract_report':'b.last_subcontract_report',
        'manufacturer_declared_awards':'coalesce(a.manufacturer_declared_awards,0)','manufacturing_naics_awards':'coalesce(a.manufacturing_naics_awards,0)','wholesale_naics_awards':'coalesce(a.wholesale_naics_awards,0)',
        'prime_evidence_digest':'a.prime_evidence_digest','sub_evidence_digest':'b.sub_evidence_digest',
        'parent_name':"coalesce(nullif(p.parent_name,''),nullif(a.parent_name,''))",'parent_uei':"coalesce(nullif(p.parent_uei,''),nullif(a.parent_uei,''))",
        'role_assessment':"CASE WHEN a.manufacturing_naics_awards>0 OR a.manufacturer_declared_awards>0 THEN 'manufacturing_source_indicated' WHEN a.wholesale_naics_awards>0 THEN 'distribution_activity_indicated' ELSE coalesce(p.role_assessment,'unknown') END",
        'profile_hash':"sha256(concat_ws('|',k.company_id,a.prime_evidence_digest,b.sub_evidence_digest,p.item_evidence_digest,p.research_evidence_digest))",
        'public_release_approved':'false'}
    fields=[]
    for name,typ,*_ in c.execute('DESCRIBE company_profiles').fetchall():
        expression=replacements.get(name,'p."'+name+'"')
        fields.append('cast('+expression+' AS '+typ+') "'+name+'"')
    output('company_profiles','SELECT '+','.join(fields)+' FROM profile_keys k LEFT JOIN company_profiles p USING(company_id) LEFT JOIN prime_stats a USING(company_id) LEFT JOIN sub_stats b USING(company_id)')
    # Facets are inspectable company evidence, not classification-only fit.
    output('company_observed_features',"""SELECT company_id,'psc' feature_type,psc feature_value,count(*) supporting_records,min(evidence_id) sample_evidence_id FROM prime WHERE company_id IS NOT NULL AND nullif(psc,'') IS NOT NULL GROUP BY company_id,psc
    UNION ALL SELECT company_id,'naics',naics,count(*),min(evidence_id) FROM prime WHERE company_id IS NOT NULL AND nullif(naics,'') IS NOT NULL GROUP BY company_id,naics
    UNION ALL SELECT company_id,'government_customer',buyer_name||' / '||buyer_office,count(*),min(evidence_id) FROM prime WHERE company_id IS NOT NULL GROUP BY company_id,buyer_name,buyer_office
    UNION ALL SELECT company_id,'programme',programme_name,count(*),min(evidence_id) FROM prime WHERE company_id IS NOT NULL AND nullif(programme_name,'') IS NOT NULL AND upper(programme_name) NOT IN ('NONE','NOT APPLICABLE','UNKNOWN') GROUP BY company_id,programme_name
    UNION ALL SELECT company_id,'commercial_customer',prime_name,count(*),min(evidence_id) FROM subs WHERE company_id IS NOT NULL GROUP BY company_id,prime_name""")
    output('company_history_coverage',"""SELECT p.company_id,p.prime_awards,p.subcontract_reports,p.observed_prime_net_obligations,p.first_prime_action,p.last_prime_action,p.last_subcontract_report,
    'Whole native award/report projection; action revisions resolved before net award obligations; report amounts remain non-additive' methodology,
    '"""+as_of+"""' refresh_as_of FROM new_company_profiles p""")
    c.close();return outputs
