"""Native all-CAGE registry and declared SAM capabilities; no contact harvesting."""
import argparse,concurrent.futures,datetime,json,os,uuid
from pathlib import Path
from opportunity_engine.aws import AWS
ap=argparse.ArgumentParser();ap.add_argument('--resume');args=ap.parse_args();R=Path(os.environ.get('MIMIR_OPPORTUNITY_WORKDIR',str(Path(__file__).resolve().parents[2])));run=args.resume or 'company_'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+uuid.uuid4().hex[:5];root=R/'inputs'/run;root.mkdir(parents=True,exist_ok=True);a=AWS()
queries={'sam_company_capabilities':"""SELECT upper(trim(cage_code)) cage_code,upper(trim(unique_entity_id)) uei,legal_business_name,dba_name,physical_address_city city,physical_address_province_or_state region,physical_address_country_code country,sam_extract_code,purpose_of_registration,initial_registration_date,registration_expiration_date,last_update_date,activation_date,entity_url,entity_structure,bus_type_string,primary_naics,naics_code_string,psc_code_string,'market_intel_silver.ref_sam_entities' source_table FROM market_intel_silver.ref_sam_entities WHERE regexp_like(upper(trim(cage_code)),'^[A-Z0-9]{5}$')"""}
# Reuse a native CAGE snapshot during initial migration; routine refreshes read upstream again.
legacy=R/'inputs/registry_latest.json';sources={}
if args.resume and legacy.exists():sources.update(json.loads(Path(json.loads(legacy.read_text())['path']).read_text())['sources'])
else:queries['cage_registry']="""SELECT cage_code,company_name,company_name_2,city,state,country,cage_status,cage_type,replacement_cage,parent_cage,last_update,date_est,foreign_domestic,business_size,primary_business,type_of_business,'market_intel_silver.ref_dla_cage_entities' source_table FROM market_intel_silver.ref_dla_cage_entities"""
def one(pair):
 name,sql=pair;s=a.extract(name,sql,root,run);s.update(as_of='native_reference_dates_per_row',kind='native_reference_snapshot',freshness_note='Extraction time is not upstream publication time; codes and registrations may be stale');return name,s
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:sources.update(dict(pool.map(one,queries.items())))
p=root/'manifest.json';p.write_text(json.dumps(dict(run_id=run,sources=sources),indent=2)+'\n');(R/'inputs/company_registry_latest.json').write_text(json.dumps(dict(path=str(p)),indent=2)+'\n');print('Full company reference connections complete',flush=True)
