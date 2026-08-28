CREATE OR REPLACE VIEW "market_intel_gold"."ref_company_network_curated_actions_candidate" AS
WITH parent_candidates AS (
    SELECT
        TRIM(recipient_parent_uei) AS uei,
        UPPER(TRIM(recipient_parent_name)) AS raw_name,
        CASE
            WHEN UPPER(recipient_parent_name) LIKE 'LOCKHEED MARTIN%' THEN 'LOCKHEED MARTIN CORPORATION'
            WHEN UPPER(recipient_parent_name) LIKE 'SIKORSKY%' THEN 'LOCKHEED MARTIN CORPORATION'
            WHEN UPPER(recipient_parent_name) LIKE 'BOEING%' THEN 'THE BOEING COMPANY'
            WHEN UPPER(recipient_parent_name) LIKE 'NORTHROP GRUMMAN%' THEN 'NORTHROP GRUMMAN CORPORATION'
            WHEN UPPER(recipient_parent_name) LIKE 'RAYTHEON%' THEN 'RTX CORPORATION'
            WHEN UPPER(recipient_parent_name) LIKE 'RTX%' THEN 'RTX CORPORATION'
            WHEN UPPER(recipient_parent_name) LIKE 'GENERAL DYNAMICS%' THEN 'GENERAL DYNAMICS CORPORATION'
            WHEN UPPER(recipient_parent_name) LIKE 'L3HARRIS%' THEN 'L3HARRIS TECHNOLOGIES, INC.'
            ELSE UPPER(TRIM(recipient_parent_name))
        END AS gold_parent_name,
        COALESCE(CAST(last_modified_date AS VARCHAR), '') AS source_sort
    FROM "market_intel_silver"."dataset_prime_contracts"
    WHERE recipient_parent_uei IS NOT NULL
), parent_map_uei AS (
    SELECT uei, MAX_BY(gold_parent_name, source_sort) AS gold_parent_name
    FROM parent_candidates
    WHERE uei <> ''
    GROUP BY 1
), parent_map_name AS (
    SELECT raw_name, MAX_BY(gold_parent_name, source_sort) AS gold_parent_name
    FROM parent_candidates
    WHERE raw_name IS NOT NULL AND raw_name <> ''
    GROUP BY 1
), prime_details AS (
    SELECT
        contract_award_unique_key AS award_key,
        MAX(TRY_CAST(base_and_all_options_value AS DOUBLE)) AS official_ceiling
    FROM "market_intel_silver"."dataset_prime_contracts"
    WHERE contract_award_unique_key IS NOT NULL
    GROUP BY 1
), historical_sam_latest AS (
    SELECT unique_entity_id, cage_code, source_period
    FROM (
        SELECT
            UPPER(TRIM(unique_entity_id)) AS unique_entity_id,
            NULLIF(UPPER(TRIM(cage_code)), '') AS cage_code,
            source_period,
            ROW_NUMBER() OVER (
                PARTITION BY UPPER(TRIM(unique_entity_id))
                ORDER BY source_period DESC,
                         COALESCE(TRY_CAST(last_update_date AS INTEGER), 0) DESC
            ) AS row_rank
        FROM "market_intel_silver"."ref_sam_historical_uei_cage_lookup"
        WHERE has_uei = 'Y'
          AND NULLIF(TRIM(unique_entity_id), '') IS NOT NULL
          AND NULLIF(TRIM(cage_code), '') IS NOT NULL
    )
    WHERE row_rank = 1
), current_sam_candidates AS (
    SELECT
        UPPER(TRIM(unique_entity_id)) AS unique_entity_id,
        NULLIF(UPPER(TRIM(cage_code)), '') AS cage_code,
        ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(unique_entity_id))
            ORDER BY COALESCE(TRY_CAST(registration_expiration_date AS BIGINT), 0) DESC,
                     COALESCE(TRY_CAST(last_update_date AS BIGINT), 0) DESC,
                     NULLIF(UPPER(TRIM(cage_code)), '') ASC
        ) AS row_rank
    FROM "market_intel_silver"."ref_sam_entities"
    WHERE NULLIF(TRIM(unique_entity_id), '') IS NOT NULL
      AND NULLIF(TRIM(cage_code), '') IS NOT NULL
), current_sam_latest AS (
    SELECT unique_entity_id, cage_code
    FROM current_sam_candidates
    WHERE row_rank = 1
), current_sam_summary AS (
    SELECT
        unique_entity_id,
        COUNT(DISTINCT cage_code) AS cage_candidate_count,
        ARRAY_JOIN(ARRAY_SORT(ARRAY_DISTINCT(ARRAY_AGG(cage_code))), '|') AS cage_candidates
    FROM current_sam_candidates
    GROUP BY 1
), selected_actions AS (
    SELECT *
    FROM "market_intel_gold"."ref_company_network_action_audit_candidate"
), enriched AS (
    SELECT
        a.*,
        UPPER(COALESCE(pmn.gold_parent_name, pmu.gold_parent_name, a.prime_awardee_parent_name)) AS prime_gold_parent,
        UPPER(COALESCE(smn.gold_parent_name, smu.gold_parent_name, a.subawardee_parent_name)) AS sub_gold_parent,
        COALESCE(ps.cage_code, 'UNKNOWN') AS prime_cage,
        COALESCE(ss.cage_code, hs.cage_code, 'UNKNOWN') AS sub_cage,
        CASE
            WHEN ss.cage_code IS NOT NULL THEN 'CURRENT_SAM'
            WHEN hs.cage_code IS NOT NULL THEN 'HISTORICAL_SAM'
            WHEN NULLIF(TRIM(a.subawardee_uei), '') IS NOT NULL THEN 'NO_CAGE_FOUND'
            ELSE 'NO_UEI_REPORTED'
        END AS sub_cage_resolution,
        hs.source_period AS sub_cage_source_period,
        COALESCE(css.cage_candidate_count, IF(hs.cage_code IS NOT NULL, 1, 0)) AS sub_cage_candidate_count,
        COALESCE(css.cage_candidates, hs.cage_code) AS sub_cage_candidates,
        p.official_ceiling
    FROM selected_actions a
    LEFT JOIN parent_map_uei pmu ON TRIM(a.prime_awardee_parent_uei) = pmu.uei
    LEFT JOIN parent_map_name pmn ON UPPER(TRIM(a.prime_awardee_parent_name)) = pmn.raw_name
    LEFT JOIN parent_map_uei smu ON TRIM(a.subawardee_parent_uei) = smu.uei
    LEFT JOIN parent_map_name smn ON UPPER(TRIM(a.subawardee_parent_name)) = smn.raw_name
    LEFT JOIN current_sam_latest ps ON UPPER(TRIM(a.prime_awardee_uei)) = ps.unique_entity_id
    LEFT JOIN current_sam_latest ss ON UPPER(TRIM(a.subawardee_uei)) = ss.unique_entity_id
    LEFT JOIN current_sam_summary css ON UPPER(TRIM(a.subawardee_uei)) = css.unique_entity_id
    LEFT JOIN historical_sam_latest hs ON UPPER(TRIM(a.subawardee_uei)) = hs.unique_entity_id
    LEFT JOIN prime_details p ON a.prime_award_unique_key = p.award_key
)
SELECT
    COALESCE(prime_awardee_name, 'Unknown Prime') AS prime_name,
    COALESCE(subawardee_name, 'Unknown Sub') AS sub_name,
    prime_gold_parent,
    sub_gold_parent,
    prime_cage,
    sub_cage,
    prime_award_piid AS contract_id,
    subaward_number AS invoice_id,
    subaward_description,
    subaward_action_date,
    CAST(
        YEAR(TRY(FROM_ISO8601_DATE(SUBSTR(TRIM(subaward_action_date), 1, 10))))
        + IF(MONTH(TRY(FROM_ISO8601_DATE(SUBSTR(TRIM(subaward_action_date), 1, 10)))) >= 10, 1, 0)
        AS INTEGER
    ) AS year,
    reported_amount_numeric AS flow_amount_raw,
    CAST(
        CASE
            WHEN reported_amount_numeric IS NULL THEN NULL
            WHEN official_ceiling > 0
             AND ABS(reported_amount_numeric) > official_ceiling THEN NULL
            WHEN ABS(reported_amount_numeric) > 2000000000 THEN NULL
            ELSE reported_amount_numeric
        END AS DOUBLE
    ) AS flow_amount_capped,
    subaward_primary_place_of_performance_city_name AS sub_city,
    subaward_primary_place_of_performance_state_code AS sub_state,
    sub_cage_resolution,
    sub_cage_source_period,
    sub_cage_candidate_count,
    sub_cage_candidates,
    subawardee_uei,
    prime_award_base_transaction_description AS prime_award_description,
    COALESCE(
        NULLIF(subaward_primary_place_of_performance_country_name, ''),
        NULLIF(subaward_primary_place_of_performance_country_code, '')
    ) AS sub_country,
    subaward_primary_place_of_performance_address_zip_code AS sub_zip,
    prime_award_unique_key AS award_key,
    subaward_sam_report_id AS source_report_id,
    subaward_sam_report_last_modified_date AS source_report_last_modified_date,
    dedup_key AS source_dedup_key,
    source_report_identity,
    reported_action_identity,
    reported_action_rank,
    normalized_prime_award_key,
    normalized_subaward_number,
    normalized_subaward_action_date,
    normalized_subawardee_identity,
    source_report_version_count,
    exact_repeat_count,
    reported_action_version_count,
    (reported_action_version_count > 1) AS possible_same_date_re_report,
    official_ceiling,
    CASE
        WHEN official_ceiling > 0
         AND ABS(reported_amount_numeric) > official_ceiling
            THEN 'EXCLUDED_ABOVE_PRIME_CEILING'
        WHEN ABS(reported_amount_numeric) > 2000000000
            THEN 'EXTREME_VALUE_EXCLUDED_NO_CEILING'
        WHEN reported_amount_numeric < 0 THEN 'REPORTED_NEGATIVE_ADJUSTMENT'
        WHEN reported_amount_numeric = 0 THEN 'ZERO_VALUE_ACTION'
        ELSE 'UNCHANGED'
    END AS internal_value_treatment,
    CASE
        WHEN official_ceiling > 0
         AND ABS(reported_amount_numeric) > official_ceiling THEN false
        WHEN ABS(reported_amount_numeric) > 2000000000 THEN false
        ELSE true
    END AS included_in_adjusted_total
FROM enriched;
