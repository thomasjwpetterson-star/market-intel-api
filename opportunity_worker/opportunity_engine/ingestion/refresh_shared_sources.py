import os
"""Read the live SAM and budget sources into immutable AWS engine snapshots."""
import argparse,concurrent.futures,datetime,json,uuid
from pathlib import Path
from opportunity_engine.aws import AWS
ap=argparse.ArgumentParser();ap.add_argument('--as-of',default=datetime.datetime.now(datetime.timezone.utc).date().isoformat());args=ap.parse_args()
R=Path(os.environ.get('MIMIR_OPPORTUNITY_WORKDIR',str(Path(__file__).resolve().parent)));asof=datetime.date.fromisoformat(args.as_of);run=asof.strftime('%Y%m%d')+'_'+uuid.uuid4().hex[:12];root=R/'inputs'/run;root.mkdir(parents=True)
start=(asof-datetime.timedelta(days=731)).isoformat();end=(asof+datetime.timedelta(days=1)).isoformat()
queries={
'sam_notices':f"""SELECT notice_id,solicitation_id,title,department_ind_agency,sub_tier,office,aac_code,cast(posted_at as varchar) posted_at,notice_type,base_type,cast(response_deadline as varchar) response_deadline,active_flag,set_aside_code,set_aside,naics_code,psc_code,award_number,cast(award_date as varchar) award_date,award_amount_usd,awardee_name,source_url,description,selected_notice_version_id,cast(source_observed_at as varchar) source_observed_at,source_s3_uri,source_s3_version_id,source_sha256,provenance_status FROM market_intel_silver.v_sam_opportunities_canonical WHERE (department_ind_agency IN ('DEPT OF DEFENSE','DEPT OF WAR','DEPARTMENT OF DEFENSE','DEPARTMENT OF WAR') OR cast(cgac as varchar) IN ('097','97')) AND posted_at>=TIMESTAMP '{start} 00:00:00' AND posted_at<TIMESTAMP '{end} 00:00:00'""",
'fydp_facts':'SELECT * FROM market_intel_silver.ref_dod_fydp_budget_facts',
'budget_pages':'SELECT * FROM market_intel_silver.candidate_dod_budget_justification_pages WHERE submission_fiscal_year>=2026'}
aws=AWS()
def one(item):
 name,sql=item;print('Extracting',name,flush=True);r=aws.extract(name,sql,root,run);r['as_of']=asof.isoformat();print('Extracted',name,r['scanned_bytes'],'bytes scanned',flush=True);return name,r
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:results=dict(pool.map(one,queries.items()))
m=dict(run_id=run,as_of=asof.isoformat(),sources=results);(root/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');(R/'inputs/shared_sources_latest.json').write_text(json.dumps(dict(path=str(root/'manifest.json')),indent=2)+'\n');aws.put_json('governance/opportunity_intelligence/sources/'+run+'/manifest.json',m);print(json.dumps(m,indent=2),flush=True)
