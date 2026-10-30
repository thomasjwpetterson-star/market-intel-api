"""Warehouse projections for the live opportunity service, without company sampling.

Transaction revisions are resolved before award financials are aggregated. Award
obligations, ceilings and subcontract reports remain distinct measures.
"""
import datetime

def queries(as_of):
    datetime.date.fromisoformat(as_of)
    return {
        'native_prime_awards': """WITH actions AS (
 SELECT contract_transaction_unique_key,contract_award_unique_key,award_id_piid,parent_award_id_piid,parent_award_agency_id,
 upper(trim(recipient_uei)) uei,upper(trim(cage_code)) cage_code,recipient_name company_name,
 recipient_parent_uei,parent_award_type,recipient_parent_name,action_date,period_of_performance_start_date,
 period_of_performance_current_end_date,period_of_performance_potential_end_date,
 transaction_description,prime_award_base_transaction_description,product_or_service_code psc,naics_code naics,
 dod_acquisition_program_code programme_code,dod_acquisition_program_description programme_name,
 awarding_office_code buyer_office_code,awarding_office_name buyer_office,awarding_sub_agency_name buyer_name,
 federal_action_obligation,base_and_all_options_value,manufacturer_of_goods,number_of_offers_received,
 extent_competed,type_of_set_aside,usaspending_permalink,
 primary_place_of_performance_city_name performance_city,primary_place_of_performance_state_code performance_region,
 row_number() OVER(PARTITION BY contract_transaction_unique_key ORDER BY last_modified_date DESC,ingest_ts DESC) revision
 FROM market_intel_silver.dataset_prime_contracts
 WHERE contract_transaction_unique_key IS NOT NULL AND contract_award_unique_key IS NOT NULL
 AND try_cast(action_date AS date)<=DATE 'ASOF'
 ), awards AS (
 SELECT contract_award_unique_key award_key,max_by(award_id_piid,action_date) piid,
 max_by(parent_award_id_piid,action_date) parent_piid,max_by(parent_award_agency_id,action_date) parent_agency,
 max_by(cage_code,action_date) cage_code,max_by(uei,action_date) uei,max_by(company_name,action_date) company_name,
 max_by(recipient_parent_uei,action_date) parent_uei,max_by(recipient_parent_name,action_date) parent_name,
 min(action_date) first_action_date,max(action_date) latest_action_date,count(*) action_count,
 sum(coalesce(try_cast(federal_action_obligation AS double),0)) observed_net_obligations,
 sum(greatest(coalesce(try_cast(federal_action_obligation AS double),0),0)) observed_positive_obligations,
 max(try_cast(base_and_all_options_value AS double)) reported_ceiling,
 max_by(coalesce(nullif(trim(prime_award_base_transaction_description),''),transaction_description),action_date) work_text,
 max_by(transaction_description,action_date) latest_description,max_by(psc,action_date) psc,max_by(naics,action_date) naics,
 max_by(programme_code,action_date) programme_code,max_by(programme_name,action_date) programme_name,
 max_by(buyer_office_code,action_date) buyer_office_code,max_by(buyer_office,action_date) buyer_office,max_by(buyer_name,action_date) buyer_name,
 max_by(period_of_performance_start_date,action_date) period_start,max_by(period_of_performance_current_end_date,action_date) period_end,
 max_by(period_of_performance_potential_end_date,action_date) potential_end,
 max_by(manufacturer_of_goods,action_date) manufacturer_of_goods,max_by(number_of_offers_received,action_date) offer_count,
 max_by(extent_competed,action_date) extent_competed,max_by(type_of_set_aside,action_date) set_aside,
 max_by(performance_city,action_date) performance_city,max_by(performance_region,action_date) performance_region,
 max_by(contract_transaction_unique_key,action_date) latest_transaction_key,max_by(usaspending_permalink,action_date) source_url
 FROM actions WHERE revision=1 GROUP BY contract_award_unique_key
 ) SELECT *,to_hex(sha256(to_utf8(concat_ws('|',award_key,latest_transaction_key,cast(action_count AS varchar),cast(observed_net_obligations AS varchar))))) evidence_id
 FROM awards""".replace('ASOF', as_of),
        'native_subcontract_reports': """WITH reports AS (
 SELECT *,row_number() OVER(PARTITION BY coalesce(nullif(dedup_key,''),concat_ws('|',prime_award_unique_key,subaward_number,subawardee_uei,subaward_action_date,subaward_amount))
 ORDER BY subaward_sam_report_last_modified_date DESC,ingest_ts DESC) revision
 FROM market_intel_silver.dataset_sub_contracts
 WHERE try_cast(subaward_action_date AS date)<=DATE 'ASOF'
 ) SELECT coalesce(nullif(dedup_key,''),concat_ws('|',prime_award_unique_key,subaward_number,subawardee_uei,subaward_action_date,subaward_amount)) report_key,
 prime_award_unique_key parent_award_key,prime_award_piid parent_piid,prime_award_parent_piid vehicle_piid,
 upper(trim(prime_awardee_uei)) prime_uei,prime_awardee_name prime_name,upper(trim(subawardee_uei)) sub_uei,subawardee_name sub_name,
 subaward_number,subaward_sam_report_id,subaward_action_date event_date,subaward_sam_report_last_modified_date modified_date,
 subaward_description work_text,try_cast(subaward_amount AS double) reported_amount,
 prime_award_base_transaction_description parent_description,prime_award_naics_code parent_naics,
 prime_award_awarding_sub_agency_name government_customer,prime_award_awarding_office_name government_office,
 subaward_primary_place_of_performance_city_name performance_city,subaward_primary_place_of_performance_state_code performance_region,
 usaspending_permalink source_url,
 to_hex(sha256(to_utf8(concat_ws('|',dedup_key,subaward_number,subawardee_uei,subaward_action_date,subaward_amount,subaward_description,subaward_sam_report_last_modified_date)))) evidence_id
 FROM reports WHERE revision=1""".replace('ASOF', as_of),
    }
