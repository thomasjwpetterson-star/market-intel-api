"""All registered identities plus explicit evidence coverage, never synthetic capabilities."""
import hashlib,json
from pathlib import Path
import duckdb

def augment(directory,manifests):
 r=Path(directory);m=json.loads((r/'manifest.json').read_text());c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='1800MB';SET preserve_insertion_order=false")
 c.execute("SET temp_directory='"+str(r/'registry_tmp').replace("'","''")+"'")
 for path in manifests:
  for name,s in json.loads(Path(path).read_text())['sources'].items():
   for f in s['files']:
    if hashlib.sha256(Path(f['path']).read_bytes()).hexdigest()!=f['sha256']:raise ValueError('Source checksum mismatch: '+name)
   c.read_parquet([f['path'] for f in s['files']]).create_view(name);m['sources'][name]=s
 c.read_parquet(m['tables']['company_profiles']['path']).create_view('profiles')
 def output(name,sql):
  c.execute('CREATE OR REPLACE TABLE output AS '+sql);p=r/(name+'.parquet')
  for col,typ,*_ in c.execute('DESCRIBE output').fetchall():
   if typ not in ('VARCHAR','BIGINT','INTEGER','DOUBLE','FLOAT','BOOLEAN'):c.execute('ALTER TABLE output ALTER COLUMN "'+col+'" TYPE VARCHAR')
  c.execute("COPY output TO '"+str(p).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)");columns=[dict(Name=n,Type='bigint' if typ in ('BIGINT','INTEGER') else 'double' if typ in ('DOUBLE','FLOAT') else 'boolean' if typ=='BOOLEAN' else 'string') for n,typ,*_ in c.execute('DESCRIBE output').fetchall()];n=c.execute('SELECT count(*) FROM output').fetchone()[0];m['tables'][name]=dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),rows=n,columns=columns);c.read_parquet(str(p)).create_view('oi_'+name);print(name,n,flush=True)
 if 'sam_company_capabilities' in m['sources'] and 'cage_registry' in m['sources']:
  c.execute("""CREATE VIEW sam_head AS SELECT * FROM sam_company_capabilities QUALIFY row_number() OVER(PARTITION BY cage_code,uei ORDER BY last_update_date DESC,sam_extract_code,legal_business_name)=1""")
  output('company_declared_capabilities',"""SELECT *,sha256(concat_ws('|',cage_code,uei,last_update_date,primary_naics,naics_code_string,psc_code_string)) declaration_id,'Self-declared SAM registration classifications; not evidence of delivered products, technical approval, current registration or bidder eligibility' interpretation FROM sam_head""")
  c.execute("""CREATE VIEW sam_by_cage AS SELECT cage_code,CASE WHEN count(DISTINCT uei)=1 THEN min(uei) END unique_uei,to_json(list(DISTINCT uei ORDER BY uei)) candidate_ueis_json,count(DISTINCT uei) uei_candidates,max_by(legal_business_name,last_update_date) legal_business_name,max_by(city,last_update_date) city,max_by(region,last_update_date) region,max_by(country,last_update_date) country,max_by(primary_naics,last_update_date) primary_naics,max_by(naics_code_string,last_update_date) naics_code_string,max_by(psc_code_string,last_update_date) psc_code_string,max_by(sam_extract_code,last_update_date) sam_extract_code,max(last_update_date) sam_last_update,max(registration_expiration_date) registration_expiration_date FROM sam_head GROUP BY cage_code""")
  output('company_registry',"""WITH keys AS(SELECT upper(trim(cage_code)) cage_code FROM cage_registry WHERE regexp_full_match(upper(trim(cage_code)),'[A-Z0-9]{5}') UNION SELECT cage_code FROM sam_by_cage UNION SELECT cage_code FROM profiles WHERE regexp_full_match(cage_code,'[A-Z0-9]{5}'))
  SELECT 'CAGE:'||k.cage_code company_id,k.cage_code,coalesce(nullif(p.company_name,''),nullif(s.legal_business_name,''),nullif(d.company_name,''),'Name unavailable') company_name,
  coalesce(s.city,d.city) city,coalesce(s.region,d.state) region,coalesce(s.country,d.country) country,d.cage_status,d.cage_type,d.parent_cage,d.replacement_cage,d.last_update dla_last_update,s.sam_last_update,s.sam_extract_code,s.registration_expiration_date,s.unique_uei,s.candidate_ueis_json,s.uei_candidates,s.primary_naics,s.naics_code_string declared_naics,s.psc_code_string declared_pscs,
  coalesce(p.prime_awards,0)::BIGINT prime_awards,coalesce(p.subcontract_reports,0)::BIGINT subcontract_reports,coalesce(p.sbir_awards,0)::BIGINT research_awards,coalesce(p.referenced_items,0)::BIGINT referenced_items,
  CASE WHEN p.company_id IS NOT NULL THEN 'observed_evidence_profile' WHEN s.cage_code IS NOT NULL THEN 'declared_classifications_only' ELSE 'registered_identity_only' END coverage_state,
  coalesce(p.role_assessment,'unknown') role_assessment,p.parent_name,
  CASE WHEN p.company_id IS NOT NULL THEN 'Retained observed-work/item/research profile; source dates and gaps remain attached' WHEN s.cage_code IS NOT NULL THEN 'Declared SAM capabilities only. No observed delivery or technical qualification is established.' ELSE 'Registered identity only. Limited observed defence evidence; do not infer inability or generate a generic match.' END coverage_note,
  'Native CAGE/SAM dates, not extraction date, govern reference freshness' freshness_note
  FROM keys k LEFT JOIN cage_registry d ON k.cage_code=upper(trim(d.cage_code)) LEFT JOIN sam_by_cage s ON k.cage_code=s.cage_code LEFT JOIN profiles p ON k.cage_code=p.cage_code""")
  output('company_registry_coverage',"SELECT coverage_state,count(*) records FROM oi_company_registry GROUP BY coverage_state")
 if 'innovation_signals' in m['sources']:output('innovation_signals','SELECT * FROM innovation_signals')
 m['registry_logic_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest();m['source_scope']='All held valid CAGE identities, declared registrations and separately labelled observed evidence; source freshness is retained, not inferred from extraction';(r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close();return m
