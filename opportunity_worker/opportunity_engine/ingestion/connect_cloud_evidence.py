import os
"""Join research to actual contract actions, platform mappings and older budget editions in AWS."""
import datetime,hashlib,json,re
from pathlib import Path
import duckdb
from opportunity_engine.aws import AWS,BUCKET,PREFIX,DATABASE
R=Path(os.environ.get('MIMIR_OPPORTUNITY_WORKDIR',str(Path(__file__).resolve().parent)));build=json.loads((R/'latest_build.json').read_text());run=Path(build.get('directory') or build['path']);m=json.loads((run/'manifest.json').read_text());a=AWS();t=m['tables']['research_awards'];name='oi_source_'+t['sha256'][:16]+'_research_awards';base=PREFIX+'/sources/research_normalised/'+t['sha256']+'/data/'
a.upload(t['path'],base+'part-00000.parquet')
try:a.register(name,'s3://'+BUCKET+'/'+base,t['columns'])
except a.glue.exceptions.AlreadyExistsException:pass
research=DATABASE+'.'+name;root=R/'inputs'/('connections_'+m['run_id']);root.mkdir(parents=True,exist_ok=True);results={}
asof=m['as_of']
sql="""WITH keys AS(SELECT DISTINCT contract_key,uei FROM RESEARCH WHERE uei IS NOT NULL AND length(contract_key)>6 AND evidence_state='observed_research_activity'),
p AS(SELECT contract_transaction_unique_key,contract_award_unique_key,award_id_piid,parent_award_id_piid,recipient_uei,cage_code,awarding_sub_agency_name,awarding_office_name,action_date,transaction_description,prime_award_base_transaction_description,dod_acquisition_program_code,dod_acquisition_program_description,federal_action_obligation,modification_number,last_modified_date,ingest_ts,usaspending_permalink,
row_number() OVER(PARTITION BY contract_transaction_unique_key ORDER BY last_modified_date DESC,ingest_ts DESC) rn
FROM market_intel_silver.dataset_prime_contracts WHERE awarding_agency_code='097' AND try_cast(action_date AS date)<=DATE 'ASOF'
AND regexp_replace(upper(award_id_piid),'[^A-Z0-9]','') IN(SELECT contract_key FROM keys)),
g AS(SELECT r.award_id,p.contract_award_unique_key,arbitrary(r.company_id) research_company_id,arbitrary(r.uei) uei,arbitrary(r.observed_research_award_usd) observed_research_award_usd,
max_by(p.award_id_piid,p.action_date) award_id_piid,max_by(p.parent_award_id_piid,p.action_date) parent_award_id_piid,max_by(p.cage_code,p.action_date) prime_cage,max_by(p.awarding_sub_agency_name,p.action_date) government_customer,max_by(p.awarding_office_name,p.action_date) buying_office,
min(p.action_date) first_action_date,max(p.action_date) latest_action_date,count(*) native_action_rows,sum(coalesce(try_cast(p.federal_action_obligation AS double),0)) observed_net_obligations,
max_by(p.prime_award_base_transaction_description,p.action_date) award_description,max_by(p.dod_acquisition_program_code,p.action_date) acquisition_programme_code,max_by(p.dod_acquisition_program_description,p.action_date) acquisition_programme_description,max_by(p.contract_transaction_unique_key,p.action_date) latest_transaction_key,max_by(p.usaspending_permalink,p.action_date) source_url
FROM RESEARCH r JOIN p ON r.contract_key=regexp_replace(upper(p.award_id_piid),'[^A-Z0-9]','') AND r.uei=upper(p.recipient_uei)
WHERE p.rn=1 AND r.evidence_state='observed_research_activity' GROUP BY r.award_id,p.contract_award_unique_key)
SELECT *,CASE WHEN count(*) OVER(PARTITION BY award_id)=1 THEN 'exact_contract_and_uei' ELSE 'multiple_award_keys_review' END link_status,
'Observed same-contract evidence; SBIR award amount and net obligations must not be added. No follow-on or Phase III is inferred.' interpretation FROM g""".replace('RESEARCH',research).replace('ASOF',asof)
print('Joining SBIR to native award actions',flush=True);results['research_prime_links']=a.extract('research_prime_links',sql,root,run.name)
# Register the result as an immutable source for the platform mapping join.
files=results['research_prime_links']['files'];c=duckdb.connect();desc=c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[[f['path'] for f in files]]).fetchall();cols=[]
for n,typ,*_ in desc:cols.append(dict(Name=n,Type='bigint' if typ=='BIGINT' else 'double' if typ=='DOUBLE' else 'string'))
linktable='oi_source_'+re.sub('[^a-z0-9_]','_',run.name)+'_research_prime_links';location=files[0]['s3_uri'].rsplit('/',1)[0]+'/'
try:a.register(linktable,location,cols)
except a.glue.exceptions.AlreadyExistsException:
 assert a.glue.get_table(DatabaseName=DATABASE,Name=linktable)['Table']['StorageDescriptor']['Location']==location
map_sql="""WITH k AS(SELECT award_id,contract_award_unique_key,prime_cage,regexp_replace(upper(award_id_piid),'[^A-Z0-9]','') contract_key,'own_award_mapping' relationship FROM LINKS WHERE link_status='exact_contract_and_uei'
UNION ALL SELECT award_id,contract_award_unique_key,prime_cage,regexp_replace(upper(parent_award_id_piid),'[^A-Z0-9]','') contract_key,'parent_vehicle_mapping_context' relationship FROM LINKS WHERE link_status='exact_contract_and_uei' AND length(parent_award_id_piid)>6),
m AS(SELECT *,regexp_replace(upper(contract_id),'[^A-Z0-9]','') contract_key FROM market_intel_silver.ref_manual_platform_map_from_contracts)
SELECT DISTINCT k.award_id,k.contract_award_unique_key,k.prime_cage,m.platform_family,m.clean_variant,m.capability_name,m.market_segment,m.tech_type,k.relationship,
'Existing platform mapping, no component-value allocation or transfer of technical approvals' interpretation FROM k JOIN m ON k.contract_key=m.contract_key AND (nullif(m.vendor_cage,'') IS NULL OR m.vendor_cage=k.prime_cage) WHERE length(k.contract_key)>6""".replace('LINKS',DATABASE+'.'+linktable)
results['research_platform_links']=a.extract('research_platform_links',map_sql,root,run.name)
results['fydp_previous']=a.extract('fydp_previous','SELECT * FROM market_intel_silver.ref_dod_fydp_budget_facts_fy2026',root,run.name)
for x in results.values():x['as_of']=asof
manifest=dict(run_id=run.name,as_of=asof,sources=results);(root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n');(R/'inputs/connections_latest.json').write_text(json.dumps(dict(path=str(root/'manifest.json')),indent=2)+'\n');a.put_json(PREFIX+'/sources/connections_'+run.name+'/manifest.json',manifest);print('Cloud evidence connections complete',flush=True)
