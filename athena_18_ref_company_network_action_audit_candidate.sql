CREATE OR REPLACE VIEW "market_intel_gold"."ref_company_network_action_audit_candidate" AS
WITH current_source_versions AS (
    SELECT *
    FROM "market_intel_gold"."ref_company_network_report_history_candidate"
    WHERE source_report_version_rank = 1
      AND reported_amount_numeric IS NOT NULL
), exact_ranked AS (
    SELECT
        s.*,
        COUNT(*) OVER (
            PARTITION BY
                normalized_prime_award_key,
                COALESCE(normalized_subaward_number, '<NO_SUBAWARD_NUMBER>'),
                COALESCE(normalized_subaward_action_date, '<NO_ACTION_DATE>'),
                normalized_subawardee_identity,
                reported_amount_numeric,
                normalized_subaward_description,
                COALESCE(UPPER(TRIM(subaward_primary_place_of_performance_city_name)), ''),
                COALESCE(UPPER(TRIM(subaward_primary_place_of_performance_state_code)), ''),
                COALESCE(UPPER(TRIM(subaward_primary_place_of_performance_country_code)), ''),
                COALESCE(UPPER(TRIM(subaward_primary_place_of_performance_address_zip_code)), '')
        ) AS exact_repeat_count,
        ROW_NUMBER() OVER (
            PARTITION BY
                normalized_prime_award_key,
                COALESCE(normalized_subaward_number, '<NO_SUBAWARD_NUMBER>'),
                COALESCE(normalized_subaward_action_date, '<NO_ACTION_DATE>'),
                normalized_subawardee_identity,
                reported_amount_numeric,
                normalized_subaward_description,
                COALESCE(UPPER(TRIM(subaward_primary_place_of_performance_city_name)), ''),
                COALESCE(UPPER(TRIM(subaward_primary_place_of_performance_state_code)), ''),
                COALESCE(UPPER(TRIM(subaward_primary_place_of_performance_country_code)), ''),
                COALESCE(UPPER(TRIM(subaward_primary_place_of_performance_address_zip_code)), '')
            ORDER BY
                COALESCE(subaward_sam_report_last_modified_date, '') DESC,
                COALESCE(subaward_sam_report_id, '') DESC,
                COALESCE(dedup_key, '') DESC
        ) AS exact_business_rank
    FROM current_source_versions s
), exact_current AS (
    SELECT
        e.*,
        CASE
            WHEN normalized_subaward_number IS NOT NULL
             AND normalized_subaward_action_date IS NOT NULL
                THEN CONCAT(
                    normalized_prime_award_key, '|',
                    normalized_subaward_number, '|',
                    normalized_subaward_action_date, '|',
                    normalized_subawardee_identity
                )
            ELSE CONCAT('SOURCE|', source_report_identity)
        END AS reported_action_identity
    FROM exact_ranked e
    WHERE exact_business_rank = 1
), action_ranked AS (
    SELECT
        e.*,
        COUNT(*) OVER (
            PARTITION BY reported_action_identity
        ) AS reported_action_version_count,
        ROW_NUMBER() OVER (
            PARTITION BY reported_action_identity
            ORDER BY
                COALESCE(subaward_sam_report_last_modified_date, '') DESC,
                COALESCE(subaward_sam_report_id, '') DESC,
                COALESCE(dedup_key, '') DESC
        ) AS reported_action_rank
    FROM exact_current e
)
SELECT *
FROM action_ranked;
