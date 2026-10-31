CREATE OR REPLACE VIEW market_intel_silver.v_sam_opportunities_canonical AS
WITH
  annual_versions AS (
   SELECT
     notice_id
   , title
   , solicitation_id
   , department_ind_agency
   , cgac
   , sub_tier
   , fpds_code
   , office
   , aac_code
   , posted_at
   , notice_type
   , base_type
   , archive_type
   , archive_date
   , set_aside_code
   , set_aside
   , response_deadline
   , naics_code
   , psc_code
   , place_of_performance_street
   , place_of_performance_city
   , place_of_performance_state
   , place_of_performance_zip
   , place_of_performance_country
   , active_flag
   , award_number
   , award_date
   , award_amount_usd
   , awardee_name
   , organization_type
   , organization_state
   , organization_city
   , organization_zip
   , organization_country_code
   , additional_info_url
   , source_url
   , description
   , sam_notice_version_id selected_notice_version_id
   , 'SAM_ANNUAL_ARCHIVE' source_dataset
   , 2 source_priority
   , last_ingested_at source_observed_at
   , element_at(source_occurrences, 1).source_s3_uri source_s3_uri
   , element_at(source_occurrences, 1).source_s3_version_id source_s3_version_id
   , element_at(source_occurrences, 1).source_sha256 source_sha256
   , source_occurrence_count
   , 'FULL_OBJECT_VERSION_AND_HASH_PROVENANCE' provenance_status
   , source_date_quality_status
   , CAST(null AS varchar) response_deadline_raw
   , CASE WHEN response_deadline IS NULL THEN 'not_supplied' ELSE 'legacy_typed_precision_unverified' END response_deadline_precision
   FROM
     market_intel_silver.ref_sam_archived_opportunity_notice_versions
) 
, daily_ranked AS (
   SELECT
     *
   , row_number() OVER (PARTITION BY trim(BOTH FROM noticeid) ORDER BY ingestion_date DESC, posteddate DESC, title DESC) daily_rank
   FROM
     market_intel_silver.fact_sam_opportunities
   WHERE ((noticeid IS NOT NULL) AND (trim(BOTH FROM noticeid) <> ''))
) 
, daily_latest AS (
   SELECT
     trim(BOTH FROM noticeid) notice_id
   , title
   , "sol#" solicitation_id
   , department_indagency department_ind_agency
   , cgac
   , sub_tier
   , fpds_code
   , office
   , aac_code
   , TRY_CAST(posteddate AS timestamp) posted_at
   , type notice_type
   , basetype base_type
   , archivetype archive_type
   , TRY_CAST(archivedate AS date) archive_date
   , setasidecode set_aside_code
   , setaside set_aside
   , COALESCE(CAST(at_timezone(TRY(from_iso8601_timestamp(NULLIF(trim(responsedeadline), ''))), 'UTC') AS timestamp), TRY_CAST(responsedeadline AS timestamp)) response_deadline
   , naicscode naics_code
   , classificationcode psc_code
   , popstreetaddress place_of_performance_street
   , popcity place_of_performance_city
   , popstate place_of_performance_state
   , popzip place_of_performance_zip
   , popcountry place_of_performance_country
   , (CASE WHEN (upper(trim(BOTH FROM active)) IN ('YES', 'TRUE', 'Y', '1')) THEN true WHEN (upper(trim(BOTH FROM active)) IN ('NO', 'FALSE', 'N', '0')) THEN false ELSE null END) active_flag
   , awardnumber award_number
   , TRY_CAST(awarddate AS date) award_date
   , TRY_CAST(regexp_replace("award$", '[^0-9.-]', '') AS decimal(30, 6)) award_amount_usd
   , awardee awardee_name
   , organizationtype organization_type
   , state organization_state
   , city organization_city
   , zipcode organization_zip
   , countrycode organization_country_code
   , additionalinfolink additional_info_url
   , link source_url
   , description
   , CAST(null AS varchar) selected_notice_version_id
   , 'SAM_DAILY_SNAPSHOT' source_dataset
   , (CASE WHEN (upper(trim(BOTH FROM active)) IN ('YES', 'TRUE', 'Y', '1')) THEN 3 ELSE 1 END) source_priority
   , TRY_CAST(concat(ingestion_date, ' 00:00:00') AS timestamp) source_observed_at
   , concat('s3://a-and-d-intel-lake-newaccount/bronze/sam/Solicitations/year=', substr(ingestion_date, 1, 4), '/month=', substr(ingestion_date, 6, 2), '/day=', substr(ingestion_date, 9, 2), '/ContractOpportunitiesFullCSV.csv') source_s3_uri
   , CAST(null AS varchar) source_s3_version_id
   , CAST(null AS varchar) source_sha256
   , CAST(1 AS bigint) source_occurrence_count
   , 'LEGACY_PATH_PROVENANCE_NO_OBJECT_VERSION_OR_HASH' provenance_status
   , 'LEGACY_DATES_ISO_RESPONSE_NORMALIZED_UTC_V1' source_date_quality_status
   , NULLIF(trim(responsedeadline), '') response_deadline_raw
   , CASE WHEN NULLIF(trim(responsedeadline),'') IS NULL THEN 'not_supplied' WHEN regexp_like(trim(responsedeadline),'^[0-9]{4}-[0-9]{2}-[0-9]{2}$') THEN 'date_only' WHEN regexp_like(trim(responsedeadline),'(?i)T.*(Z|[+-][0-9]{2}:[0-9]{2})$') AND TRY(from_iso8601_timestamp(responsedeadline)) IS NOT NULL THEN 'timestamp_utc' ELSE 'timezone_not_supplied_or_unparsed' END response_deadline_precision
   FROM
     daily_ranked
   WHERE (daily_rank = 1)
) 
, all_candidates AS (
   SELECT *
   FROM
     annual_versions
UNION ALL    SELECT *
   FROM
     daily_latest
) 
, ranked AS (
   SELECT
     *
   , row_number() OVER (PARTITION BY notice_id ORDER BY source_priority DESC, source_observed_at DESC NULLS LAST, posted_at DESC NULLS LAST, COALESCE(selected_notice_version_id, '') DESC) canonical_rank
   , count(*) OVER (PARTITION BY notice_id) retained_candidate_count
   FROM
     all_candidates
) 
, source_rollup AS (
   SELECT
     notice_id
   , array_sort(array_distinct(array_agg(source_dataset))) available_source_datasets
   FROM
     all_candidates
   GROUP BY notice_id
) 
SELECT
  r.notice_id
, r.title
, r.solicitation_id
, r.department_ind_agency
, r.cgac
, r.sub_tier
, r.fpds_code
, r.office
, r.aac_code
, r.posted_at
, r.notice_type
, r.base_type
, r.archive_type
, r.archive_date
, r.set_aside_code
, r.set_aside
, r.response_deadline
, r.naics_code
, r.psc_code
, r.place_of_performance_street
, r.place_of_performance_city
, r.place_of_performance_state
, r.place_of_performance_zip
, r.place_of_performance_country
, r.active_flag
, r.award_number
, r.award_date
, r.award_amount_usd
, r.awardee_name
, r.organization_type
, r.organization_state
, r.organization_city
, r.organization_zip
, r.organization_country_code
, r.additional_info_url
, r.source_url
, r.description
, r.selected_notice_version_id
, r.source_dataset selected_source_dataset
, s.available_source_datasets
, r.retained_candidate_count
, r.source_observed_at
, r.source_s3_uri
, r.source_s3_version_id
, r.source_sha256
, r.provenance_status
, r.source_date_quality_status
, r.response_deadline_raw
, r.response_deadline_precision
FROM
  (ranked r
INNER JOIN source_rollup s ON (r.notice_id = s.notice_id))
WHERE (r.canonical_rank = 1)
