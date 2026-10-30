"""Merge compact distributed aggregates into the live company schema."""
import hashlib,json
from pathlib import Path
import duckdb

def rebuild(sources,folder,as_of):
    folder=Path(folder);folder.mkdir(parents=True,exist_ok=True);c=duckdb.connect();c.execute("SET threads=2;SET memory_limit='1800MB';SET preserve_insertion_order=false")
    c.execute("SET temp_directory='"+str(folder/'tmp').replace("'","''")+"'")
    for name in ['company_profiles','company_work','native_company_prime_stats','native_company_sub_stats','native_company_work','native_company_features','native_company_subcontracts']:
        c.read_parquet([f['path'] for f in sources[name]['files']]).create_view(name)
    c.execute('CREATE VIEW prime_stats AS SELECT * FROM native_company_prime_stats');c.execute('CREATE VIEW sub_stats AS SELECT * FROM native_company_sub_stats')
    outputs={}
    def output(name,sql):
        p=folder/(name+'.parquet');c.execute("COPY ("+sql+") TO '"+str(p).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)");sha=hashlib.sha256(p.read_bytes()).hexdigest();count=c.execute('SELECT count(*) FROM read_parquet(?)',[str(p)]).fetchone()[0]
        outputs[name]=dict(path=str(p),files=[dict(path=str(p),sha256=sha)],rows=count,sha256=sha,as_of=as_of,kind='complete_native_warehouse_derived',status='SUCCEEDED');c.read_parquet(str(p)).create_view('new_'+name);print(name,count,flush=True)
    # Keep all award evidence in the distributed table, without a second copy on
    # the worker disk. The source manifest has every object's content hash.
    outputs['company_award_facts']=dict(sources['native_company_awards'])
    outputs['company_award_facts']['profileable_rows']=c.execute('SELECT sum(prime_awards) FROM prime_stats').fetchone()[0]
    outputs['company_award_facts']['profile_observed_net_total']=c.execute('SELECT sum(observed_prime_net_obligations) FROM prime_stats').fetchone()[0]
    output('company_subcontract_facts','SELECT * FROM native_company_subcontracts')
    cols='doc_id,company_id,source_kind,work_text,psc,naics,last_observed,sample_evidence_id,evidence_digest'
    output('company_work','SELECT '+cols+',source_records,programme_context_json FROM native_company_work UNION ALL SELECT '+cols+",1::BIGINT source_records,NULL::VARCHAR programme_context_json FROM company_work WHERE source_kind NOT IN ('prime_award','latest_prime_action','subaward_report')")
    c.execute('CREATE VIEW profile_keys AS SELECT company_id FROM company_profiles UNION SELECT company_id FROM prime_stats UNION SELECT company_id FROM sub_stats')
    values={
    'company_id':'k.company_id','company_name':'coalesce(a.company_name,b.company_name,p.company_name)',
    'cage_code':"CASE WHEN starts_with(k.company_id,'CAGE:') THEN substr(k.company_id,6) END",'unresolved_uei':"CASE WHEN starts_with(k.company_id,'UEI:') THEN substr(k.company_id,5) END",
    'prime_awards':'coalesce(a.prime_awards,0)','observed_prime_net_obligations':'a.observed_prime_net_obligations','first_prime_action':'a.first_prime_action','last_prime_action':'a.last_prime_action',
    'subcontract_reports':'coalesce(b.subcontract_reports,0)','commercial_customers':'coalesce(b.commercial_customers,0)','last_subcontract_report':'b.last_subcontract_report',
    'manufacturer_declared_awards':'coalesce(a.manufacturer_declared_awards,0)','manufacturing_naics_awards':'coalesce(a.manufacturing_naics_awards,0)','wholesale_naics_awards':'coalesce(a.wholesale_naics_awards,0)',
    'prime_evidence_digest':'a.prime_evidence_digest','sub_evidence_digest':'b.sub_evidence_digest','parent_name':"coalesce(nullif(p.parent_name,''),nullif(a.parent_name,''))",'parent_uei':"coalesce(nullif(p.parent_uei,''),nullif(a.parent_uei,''))",
    'role_assessment':"CASE WHEN nullif(p.role_assessment,'unknown') IS NOT NULL THEN p.role_assessment WHEN a.manufacturer_declared_awards>0 AND a.manufacturing_naics_awards>0 THEN 'manufacturing_activity_indicated' WHEN a.manufacturing_naics_awards>0 THEN 'manufacturing_classification_indicated' WHEN a.wholesale_naics_awards>0 THEN 'distribution_activity_indicated' ELSE coalesce(p.role_assessment,'unknown') END",
    'profile_hash':"sha256(concat_ws('|',k.company_id,a.prime_evidence_digest,b.sub_evidence_digest,p.item_evidence_digest,p.research_evidence_digest))",'public_release_approved':'false'}
    projection=','.join('cast('+values.get(n,'p."'+n+'"')+' AS '+t+') "'+n+'"' for n,t,*_ in c.execute('DESCRIBE company_profiles').fetchall())
    output('company_profiles','SELECT '+projection+' FROM profile_keys k LEFT JOIN company_profiles p USING(company_id) LEFT JOIN prime_stats a USING(company_id) LEFT JOIN sub_stats b USING(company_id)')
    output('company_observed_features','SELECT * FROM native_company_features')
    output('company_history_coverage',"SELECT company_id,prime_awards,subcontract_reports,observed_prime_net_obligations,first_prime_action,last_prime_action,last_subcontract_report,'Whole native source; revisions resolved before award obligations, supplier reports remain non-additive' methodology,'"+as_of+"' refresh_as_of FROM new_company_profiles")
    c.close();return outputs
