"""Deterministic shared-source windows. A current deadline survives an old posting.

Keep inactive/terminal versions too: native notice-chain resolution decides
which version is current; this extraction must not discard negative evidence.
"""
import datetime

def queries(as_of):
 asof=datetime.date.fromisoformat(as_of)
 start=(asof-datetime.timedelta(days=731)).isoformat()
 end=(asof+datetime.timedelta(days=1)).isoformat()
 return {
'sam_notices':f"""SELECT notice_id,solicitation_id,title,department_ind_agency,sub_tier,office,aac_code,cast(posted_at as varchar) posted_at,notice_type,base_type,cast(response_deadline as varchar) response_deadline,active_flag,response_deadline_raw,response_deadline_precision,source_date_quality_status,set_aside_code,set_aside,naics_code,psc_code,award_number,cast(award_date as varchar) award_date,award_amount_usd,awardee_name,source_url,description,selected_notice_version_id,cast(source_observed_at as varchar) source_observed_at,source_s3_uri,source_s3_version_id,source_sha256,provenance_status FROM market_intel_silver.v_sam_opportunities_canonical WHERE (department_ind_agency IN ('DEPT OF DEFENSE','DEPT OF WAR','DEPARTMENT OF DEFENSE','DEPARTMENT OF WAR') OR cast(cgac as varchar) IN ('097','97')) AND posted_at<TIMESTAMP '{end} 00:00:00' AND (posted_at>=TIMESTAMP '{start} 00:00:00' OR response_deadline>=TIMESTAMP '{asof.isoformat()} 00:00:00')""",
'fydp_facts':'SELECT * FROM market_intel_silver.ref_dod_fydp_budget_facts',
'budget_pages':'SELECT * FROM market_intel_silver.candidate_dod_budget_justification_pages WHERE submission_fiscal_year>=2026'}
