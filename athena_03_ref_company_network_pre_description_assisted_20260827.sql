CREATE OR REPLACE VIEW "market_intel_gold"."ref_company_network" AS
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
),
parent_map_uei AS (
    SELECT
        uei,
        MAX_BY(gold_parent_name, source_sort) AS gold_parent_name
    FROM parent_candidates
    WHERE uei <> ''
    GROUP BY uei
),
parent_map_name AS (
    SELECT
        raw_name,
        MAX_BY(gold_parent_name, source_sort) AS gold_parent_name
    FROM parent_candidates
    WHERE raw_name IS NOT NULL
      AND raw_name <> ''
    GROUP BY raw_name
),
prime_details AS (
    SELECT
        contract_award_unique_key AS award_key,
        MAX(TRY_CAST(base_and_all_options_value AS DOUBLE)) AS official_ceiling
    FROM "market_intel_silver"."dataset_prime_contracts"
    WHERE contract_award_unique_key IS NOT NULL
    GROUP BY contract_award_unique_key
),
historical_sam_latest AS (
    SELECT *
    FROM (
        SELECT
            UPPER(TRIM(unique_entity_id)) AS unique_entity_id,
            NULLIF(UPPER(TRIM(cage_code)), '') AS cage_code,
            source_period,
            ROW_NUMBER() OVER (
                PARTITION BY UPPER(TRIM(unique_entity_id))
                ORDER BY
                    source_period DESC,
                    COALESCE(TRY_CAST(last_update_date AS INTEGER), 0) DESC
            ) AS row_rank
        FROM "market_intel_silver"."ref_sam_historical_uei_cage_lookup"
        WHERE has_uei = 'Y'
          AND unique_entity_id IS NOT NULL
          AND TRIM(unique_entity_id) <> ''
          AND cage_code IS NOT NULL
          AND TRIM(cage_code) <> ''
    )
    WHERE row_rank = 1
),
current_sam_candidates AS (
    SELECT
        UPPER(TRIM(unique_entity_id)) AS unique_entity_id,
        NULLIF(UPPER(TRIM(cage_code)), '') AS cage_code,
        ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(unique_entity_id))
            ORDER BY
                COALESCE(TRY_CAST(registration_expiration_date AS BIGINT), 0) DESC,
                COALESCE(TRY_CAST(last_update_date AS BIGINT), 0) DESC,
                NULLIF(UPPER(TRIM(cage_code)), '') ASC
        ) AS row_rank
    FROM "market_intel_silver"."ref_sam_entities"
    WHERE unique_entity_id IS NOT NULL
      AND TRIM(unique_entity_id) <> ''
      AND cage_code IS NOT NULL
      AND TRIM(cage_code) <> ''
),
current_sam_latest AS (
    SELECT unique_entity_id, cage_code
    FROM current_sam_candidates
    WHERE row_rank = 1
),
current_sam_summary AS (
    SELECT
        unique_entity_id,
        COUNT(DISTINCT cage_code) AS cage_candidate_count,
        ARRAY_JOIN(ARRAY_SORT(ARRAY_DISTINCT(ARRAY_AGG(cage_code))), '|') AS cage_candidates
    FROM current_sam_candidates
    GROUP BY unique_entity_id
),
raw_transactions_ranked AS (
    SELECT
        s.prime_award_unique_key AS award_key,
        s.prime_award_piid,
        s.subaward_sam_report_id AS source_report_id,
        s.subaward_sam_report_last_modified_date AS source_report_last_modified_date,
        s.dedup_key AS source_dedup_key,
        s.subaward_number,
        s.subaward_action_date,
        s.subaward_amount,
        s.subaward_description,
        s.prime_award_base_transaction_description AS prime_award_description,
        TRIM(s.prime_awardee_parent_uei) AS prime_parent_uei,
        s.prime_awardee_parent_name AS prime_parent_name_raw,
        TRIM(s.subawardee_parent_uei) AS sub_parent_uei,
        s.subawardee_parent_name AS sub_parent_name_raw,
        s.prime_awardee_uei,
        s.subawardee_uei,
        COALESCE(s.prime_awardee_name, 'Unknown Prime') AS prime_name_raw,
        COALESCE(s.subawardee_name, 'Unknown Sub') AS sub_name_raw,
        s.subaward_primary_place_of_performance_city_name AS sub_city,
        s.subaward_primary_place_of_performance_state_code AS sub_state,
        COALESCE(
            NULLIF(s.subaward_primary_place_of_performance_country_name, ''),
            NULLIF(s.subaward_primary_place_of_performance_country_code, '')
        ) AS sub_country,
        s.subaward_primary_place_of_performance_address_zip_code AS sub_zip,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(
                NULLIF(TRIM(s.subaward_sam_report_id), ''),
                NULLIF(TRIM(s.dedup_key), '')
            )
            ORDER BY
                s.subaward_sam_report_last_modified_date DESC,
                s.dedup_key DESC
        ) AS source_row_rank
    FROM "market_intel_silver"."dataset_sub_contracts" s
    WHERE s.subaward_amount IS NOT NULL
      AND CAST(s.subaward_amount AS DOUBLE) > 0
),
raw_transactions AS (
    SELECT *
    FROM raw_transactions_ranked
    WHERE source_row_rank = 1
),
enriched_transactions AS (
    SELECT
        t.*,
        UPPER(COALESCE(p_map_uei.gold_parent_name, p_map_name.gold_parent_name, t.prime_parent_name_raw)) AS prime_gold_parent,
        UPPER(COALESCE(s_map_uei.gold_parent_name, s_map_name.gold_parent_name, t.sub_parent_name_raw)) AS sub_gold_parent,
        COALESCE(prime_sam.cage_code, 'UNKNOWN') AS prime_cage,
        COALESCE(sub_sam.cage_code, hist_sub_sam.cage_code, 'UNKNOWN') AS sub_cage,
        CASE
            WHEN sub_sam.cage_code IS NOT NULL THEN 'CURRENT_SAM'
            WHEN hist_sub_sam.cage_code IS NOT NULL THEN 'HISTORICAL_SAM'
            ELSE 'UNRESOLVED'
        END AS sub_cage_resolution,
        hist_sub_sam.source_period AS sub_cage_source_period,
        COALESCE(sub_sam_summary.cage_candidate_count, IF(hist_sub_sam.cage_code IS NOT NULL, 1, 0)) AS sub_cage_candidate_count,
        COALESCE(sub_sam_summary.cage_candidates, hist_sub_sam.cage_code) AS sub_cage_candidates,
        t.prime_name_raw AS prime_name,
        t.sub_name_raw AS sub_name,
        p.official_ceiling
    FROM raw_transactions t
    LEFT JOIN parent_map_uei p_map_uei
        ON t.prime_parent_uei = p_map_uei.uei
    LEFT JOIN parent_map_name p_map_name
        ON UPPER(TRIM(t.prime_parent_name_raw)) = p_map_name.raw_name
    LEFT JOIN parent_map_uei s_map_uei
        ON t.sub_parent_uei = s_map_uei.uei
    LEFT JOIN parent_map_name s_map_name
        ON UPPER(TRIM(t.sub_parent_name_raw)) = s_map_name.raw_name
    LEFT JOIN current_sam_latest prime_sam
        ON UPPER(TRIM(t.prime_awardee_uei)) = prime_sam.unique_entity_id
    LEFT JOIN current_sam_latest sub_sam
        ON UPPER(TRIM(t.subawardee_uei)) = sub_sam.unique_entity_id
    LEFT JOIN current_sam_summary sub_sam_summary
        ON UPPER(TRIM(t.subawardee_uei)) = sub_sam_summary.unique_entity_id
    LEFT JOIN historical_sam_latest hist_sub_sam
        ON UPPER(TRIM(t.subawardee_uei)) = hist_sub_sam.unique_entity_id
    LEFT JOIN prime_details p
        ON t.award_key = p.award_key
),
financially_deduplicated AS (
    SELECT
        e.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                e.prime_award_piid,
                e.subaward_number,
                e.sub_parent_uei,
                e.subaward_amount
            ORDER BY
                e.subaward_action_date DESC,
                e.source_report_last_modified_date DESC,
                e.source_report_id DESC
        ) AS financial_row_rank
    FROM enriched_transactions e
)
SELECT
    e.prime_name,
    e.sub_name,
    e.prime_gold_parent,
    e.sub_gold_parent,
    e.prime_cage,
    e.sub_cage,
    e.prime_award_piid AS contract_id,
    e.subaward_number AS invoice_id,
    e.subaward_description,
    e.subaward_action_date,
    CAST(
        CASE
            WHEN MONTH(TRY(FROM_ISO8601_DATE(e.subaward_action_date))) >= 10
            THEN YEAR(TRY(FROM_ISO8601_DATE(e.subaward_action_date))) + 1
            ELSE YEAR(TRY(FROM_ISO8601_DATE(e.subaward_action_date)))
        END AS INTEGER
    ) AS year,
    CAST(e.subaward_amount AS DOUBLE) AS flow_amount_raw,
    CAST(
        CASE
            WHEN CAST(e.subaward_amount AS DOUBLE) > 2000000000
            THEN CASE
                WHEN e.official_ceiling > 0 THEN e.official_ceiling
                ELSE 100000000
            END
            WHEN e.official_ceiling > 0
             AND CAST(e.subaward_amount AS DOUBLE) > e.official_ceiling * 50
            THEN e.official_ceiling
            ELSE CAST(e.subaward_amount AS DOUBLE)
        END AS DOUBLE
    ) AS flow_amount_capped,
    e.sub_city,
    e.sub_state,
    e.sub_cage_resolution,
    e.sub_cage_source_period,
    e.sub_cage_candidate_count,
    e.sub_cage_candidates,
    e.subawardee_uei,
    e.prime_award_description,
    e.sub_country,
    e.sub_zip,
    e.award_key,
    e.source_report_id,
    e.source_report_last_modified_date,
    e.source_dedup_key
FROM financially_deduplicated e
WHERE e.financial_row_rank = 1;
