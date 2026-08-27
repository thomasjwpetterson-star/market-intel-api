CREATE OR REPLACE VIEW "market_intel_gold"."ref_company_network_report_history_candidate" AS
WITH normalized AS (
    SELECT
        s.*,
        TRY_CAST(s.subaward_amount AS DOUBLE) AS reported_amount_numeric,
        COALESCE(
            NULLIF(TRIM(s.prime_award_unique_key), ''),
            NULLIF(TRIM(s.prime_award_piid), ''),
            '<NO_PRIME_AWARD>'
        ) AS normalized_prime_award_key,
        NULLIF(
            REGEXP_REPLACE(UPPER(TRIM(s.subaward_number)), '[[:space:]]+', ' '),
            ''
        ) AS normalized_subaward_number,
        COALESCE(
            CAST(TRY(FROM_ISO8601_DATE(SUBSTR(TRIM(s.subaward_action_date), 1, 10))) AS VARCHAR),
            NULLIF(SUBSTR(TRIM(s.subaward_action_date), 1, 10), '')
        ) AS normalized_subaward_action_date,
        COALESCE(
            NULLIF(UPPER(TRIM(s.subawardee_uei)), ''),
            NULLIF(UPPER(TRIM(s.subawardee_parent_uei)), ''),
            NULLIF(REGEXP_REPLACE(UPPER(TRIM(s.subawardee_name)), '[[:space:]]+', ' '), ''),
            '<NO_SUBAWARDEE>'
        ) AS normalized_subawardee_identity,
        COALESCE(
            NULLIF(TRIM(s.subaward_sam_report_id), ''),
            NULLIF(TRIM(s.dedup_key), ''),
            CONCAT(
                'FALLBACK|',
                COALESCE(TRIM(s.prime_award_unique_key), ''), '|',
                COALESCE(TRIM(s.prime_award_piid), ''), '|',
                COALESCE(TRIM(s.subaward_number), ''), '|',
                COALESCE(TRIM(s.subawardee_uei), ''), '|',
                COALESCE(TRIM(s.subaward_amount), ''), '|',
                COALESCE(TRIM(s.subaward_action_date), ''), '|',
                COALESCE(REGEXP_REPLACE(UPPER(TRIM(s.subaward_description)), '[[:space:]]+', ' '), '')
            )
        ) AS source_report_identity,
        REGEXP_REPLACE(
            UPPER(TRIM(COALESCE(s.subaward_description, ''))),
            '[[:space:]]+',
            ' '
        ) AS normalized_subaward_description
    FROM "market_intel_silver"."dataset_sub_contracts" s
), version_ranked AS (
    SELECT
        n.*,
        COUNT(*) OVER (
            PARTITION BY source_report_identity
        ) AS source_report_version_count,
        ROW_NUMBER() OVER (
            PARTITION BY source_report_identity
            ORDER BY
                COALESCE(subaward_sam_report_last_modified_date, '') DESC,
                COALESCE(dedup_key, '') DESC,
                COALESCE(subaward_action_date, '') DESC
        ) AS source_report_version_rank
    FROM normalized n
)
SELECT *
FROM version_ranked;
