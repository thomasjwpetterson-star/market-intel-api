CREATE OR REPLACE VIEW "market_intel_gold"."ref_company_network_description_assisted_candidate" AS
WITH normalized AS (
    SELECT
        c.*,
        NULLIF(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    UPPER(TRIM(COALESCE(c.subaward_description, ''))),
                    '[[:punct:]]+',
                    ' '
                ),
                '[[:space:]]+',
                ' '
            ),
            ''
        ) AS financial_description_key,
        COALESCE(UPPER(TRIM(c.sub_city)), '') AS financial_city_key,
        COALESCE(UPPER(TRIM(c.sub_state)), '') AS financial_state_key,
        COALESCE(UPPER(TRIM(c.sub_country)), '') AS financial_country_key,
        COALESCE(UPPER(TRIM(c.sub_zip)), '') AS financial_zip_key
    FROM "market_intel_gold"."ref_company_network_curated_actions_candidate" c
), same_date_description_ranked AS (
    SELECT
        n.*,
        CASE
            WHEN n.normalized_subaward_number IS NOT NULL
             AND n.normalized_subaward_action_date IS NOT NULL
             AND n.financial_description_key IS NOT NULL
                THEN CONCAT(
                    n.normalized_prime_award_key, '|',
                    n.normalized_subaward_number, '|',
                    n.normalized_subaward_action_date, '|',
                    n.normalized_subawardee_identity, '|',
                    n.financial_description_key, '|',
                    n.financial_city_key, '|',
                    n.financial_state_key, '|',
                    n.financial_country_key, '|',
                    n.financial_zip_key
                )
            ELSE CONCAT('SOURCE|', n.source_report_identity)
        END AS same_date_description_identity,
        COUNT(*) OVER (
            PARTITION BY
                CASE
                    WHEN n.normalized_subaward_number IS NOT NULL
                     AND n.normalized_subaward_action_date IS NOT NULL
                     AND n.financial_description_key IS NOT NULL
                        THEN CONCAT(
                            n.normalized_prime_award_key, '|',
                            n.normalized_subaward_number, '|',
                            n.normalized_subaward_action_date, '|',
                            n.normalized_subawardee_identity, '|',
                            n.financial_description_key, '|',
                            n.financial_city_key, '|',
                            n.financial_state_key, '|',
                            n.financial_country_key, '|',
                            n.financial_zip_key
                        )
                    ELSE CONCAT('SOURCE|', n.source_report_identity)
                END
        ) AS same_date_description_version_count,
        ROW_NUMBER() OVER (
            PARTITION BY
                CASE
                    WHEN n.normalized_subaward_number IS NOT NULL
                     AND n.normalized_subaward_action_date IS NOT NULL
                     AND n.financial_description_key IS NOT NULL
                        THEN CONCAT(
                            n.normalized_prime_award_key, '|',
                            n.normalized_subaward_number, '|',
                            n.normalized_subaward_action_date, '|',
                            n.normalized_subawardee_identity, '|',
                            n.financial_description_key, '|',
                            n.financial_city_key, '|',
                            n.financial_state_key, '|',
                            n.financial_country_key, '|',
                            n.financial_zip_key
                        )
                    ELSE CONCAT('SOURCE|', n.source_report_identity)
                END
            ORDER BY
                COALESCE(n.source_report_last_modified_date, '') DESC,
                COALESCE(n.source_report_id, '') DESC,
                COALESCE(n.source_dedup_key, '') DESC
        ) AS same_date_description_rank
    FROM normalized n
), same_date_current AS (
    SELECT *
    FROM same_date_description_ranked
    WHERE same_date_description_rank = 1
), equal_value_description_ranked AS (
    SELECT
        s.*,
        CASE
            WHEN s.normalized_subaward_number IS NOT NULL
             AND s.financial_description_key IS NOT NULL
                THEN CONCAT(
                    s.normalized_prime_award_key, '|',
                    s.normalized_subaward_number, '|',
                    s.normalized_subawardee_identity, '|',
                    CAST(s.flow_amount_raw AS VARCHAR), '|',
                    s.financial_description_key, '|',
                    s.financial_city_key, '|',
                    s.financial_state_key, '|',
                    s.financial_country_key, '|',
                    s.financial_zip_key
                )
            ELSE CONCAT('SOURCE|', s.source_report_identity)
        END AS equal_value_description_identity,
        COUNT(*) OVER (
            PARTITION BY
                CASE
                    WHEN s.normalized_subaward_number IS NOT NULL
                     AND s.financial_description_key IS NOT NULL
                        THEN CONCAT(
                            s.normalized_prime_award_key, '|',
                            s.normalized_subaward_number, '|',
                            s.normalized_subawardee_identity, '|',
                            CAST(s.flow_amount_raw AS VARCHAR), '|',
                            s.financial_description_key, '|',
                            s.financial_city_key, '|',
                            s.financial_state_key, '|',
                            s.financial_country_key, '|',
                            s.financial_zip_key
                        )
                    ELSE CONCAT('SOURCE|', s.source_report_identity)
                END
        ) AS equal_value_description_report_count,
        ROW_NUMBER() OVER (
            PARTITION BY
                CASE
                    WHEN s.normalized_subaward_number IS NOT NULL
                     AND s.financial_description_key IS NOT NULL
                        THEN CONCAT(
                            s.normalized_prime_award_key, '|',
                            s.normalized_subaward_number, '|',
                            s.normalized_subawardee_identity, '|',
                            CAST(s.flow_amount_raw AS VARCHAR), '|',
                            s.financial_description_key, '|',
                            s.financial_city_key, '|',
                            s.financial_state_key, '|',
                            s.financial_country_key, '|',
                            s.financial_zip_key
                        )
                    ELSE CONCAT('SOURCE|', s.source_report_identity)
                END
            ORDER BY
                COALESCE(s.normalized_subaward_action_date, '') DESC,
                COALESCE(s.source_report_last_modified_date, '') DESC,
                COALESCE(s.source_report_id, '') DESC,
                COALESCE(s.source_dedup_key, '') DESC
        ) AS equal_value_description_rank
    FROM same_date_current s
)
SELECT
    e.*,
    (e.same_date_description_version_count > 1) AS description_supported_same_date_revision,
    (e.equal_value_description_report_count > 1) AS description_supported_equal_value_re_report
FROM equal_value_description_ranked e
WHERE e.equal_value_description_rank = 1;
