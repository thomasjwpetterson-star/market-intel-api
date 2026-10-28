CREATE TABLE __TARGET_TABLE__
WITH (format = 'PARQUET', parquet_compression = 'ZSTD', external_location = '__TARGET_LOCATION__') AS
WITH label_bridge AS (
    SELECT
        UPPER(TRIM(legacy_platform_family)) AS normalized_label,
        CASE WHEN COUNT(DISTINCT effective_canonical_entity_id) = 1
            THEN MIN(effective_canonical_entity_id) END AS canonical_entity_id,
        CASE WHEN COUNT(DISTINCT effective_canonical_entity_id) = 1
            THEN MIN(effective_canonical_label) END AS canonical_label,
        CASE WHEN COUNT(DISTINCT effective_canonical_entity_id) = 1
            THEN MIN(entity_type) END AS entity_type
    FROM __REVIEWED_LEGACY_BRIDGE_TABLE__
    WHERE match_status = 'MATCHED'
      AND platform_visible
      AND effective_canonical_entity_id IS NOT NULL
    GROUP BY 1
), hidden_label_bridge AS (
    SELECT UPPER(TRIM(normalized_source_label)) AS normalized_label
    FROM __REVIEWED_VISIBILITY_TABLE__
    WHERE review_status = 'APPROVED' AND platform_visible = FALSE
    GROUP BY 1
    UNION
    SELECT UPPER(TRIM(legacy_platform_family)) AS normalized_label
    FROM __REVIEWED_LEGACY_BRIDGE_TABLE__
    WHERE match_status = 'MATCHED' AND platform_visible = FALSE
    GROUP BY 1
    UNION SELECT 'HELLFIRE / JAGM' AS normalized_label
)
SELECT
    c."prime_name" AS "prime_name",
    c."sub_name" AS "sub_name",
    c."prime_gold_parent" AS "prime_gold_parent",
    c."sub_gold_parent" AS "sub_gold_parent",
    c."prime_cage" AS "prime_cage",
    c."sub_cage" AS "sub_cage",
    c."contract_id" AS "contract_id",
    c."award_key" AS "award_key",
    c."invoice_id" AS "invoice_id",
    c."source_report_id" AS "source_report_id",
    c."source_report_last_modified_date" AS "source_report_last_modified_date",
    c."source_dedup_key" AS "source_dedup_key",
    c."prime_award_control_value" AS "prime_award_control_value",
    c."internal_value_treatment" AS "internal_value_treatment",
    c."included_in_adjusted_total" AS "included_in_adjusted_total",
    c."source_report_version_count" AS "source_report_version_count",
    c."exact_repeat_count" AS "exact_repeat_count",
    c."reported_action_version_count" AS "reported_action_version_count",
    c."same_date_description_version_count" AS "same_date_description_version_count",
    c."equal_value_description_report_count" AS "equal_value_description_report_count",
    c."sub_cage_resolution" AS "sub_cage_resolution",
    c."sub_cage_source_period" AS "sub_cage_source_period",
    c."sub_cage_candidate_count" AS "sub_cage_candidate_count",
    c."sub_cage_candidates" AS "sub_cage_candidates",
    c."subawardee_uei" AS "subawardee_uei",
    c."prime_award_description" AS "prime_award_description",
    c."description" AS "description",
    c."action_date" AS "action_date",
    c."year" AS "year",
    c."subaward_value" AS "subaward_value",
    c."subaward_value_raw" AS "subaward_value_raw",
    c."sub_city" AS "sub_city",
    c."sub_state" AS "sub_state",
    c."sub_country" AS "sub_country",
    c."sub_zip" AS "sub_zip",
    CASE WHEN a.award_key IS NOT NULL THEN a.platform_family
            WHEN UPPER(TRIM(CAST(c.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN NULL
            WHEN h.normalized_label IS NOT NULL THEN NULL
            WHEN NULLIF(TRIM(CAST(c.platform_family AS VARCHAR)), '') IS NOT NULL THEN COALESCE(l.canonical_label, c.platform_family)
            ELSE c.platform_family END AS "platform_family",
    c."psc" AS "psc",
    c."market_segment" AS "market_segment",
    COALESCE(a.canonical_entity_id, l.canonical_entity_id) AS "platform_entity_id",
    COALESCE(
            a.canonical_entity_id,
            l.canonical_entity_id,
            CASE WHEN h.normalized_label IS NULL AND NULLIF(TRIM(CAST(c.platform_family AS VARCHAR)), '') IS NOT NULL
                THEN CONCAT('LEGACY_LABEL:', UPPER(TRIM(c.platform_family))) END
        ) AS "platform_identity_key",
    COALESCE(a.entity_type, l.entity_type) AS "platform_entity_type",
    CASE WHEN a.award_key IS NOT NULL
            THEN a.association_scope WHEN h.normalized_label IS NULL AND NULLIF(TRIM(CAST(c.platform_family AS VARCHAR)), '') IS NOT NULL
            THEN 'LEGACY_PLATFORM_LABEL' END AS "platform_association_scope",
    CASE WHEN a.award_key IS NOT NULL
            THEN COALESCE(a.canonical_entity_id IS NOT NULL
                AND a.pricing_evidence_eligible, FALSE)
            ELSE FALSE END AS "pricing_evidence_eligible",
    '2026-10-15-hellfire-jagm-split-v7' AS "platform_reference_release_id"
FROM __SOURCE_TABLE__ c
LEFT JOIN "market_intel_platform_candidate"."award_platform_cache_sidecar_20261015_hellfire_jagm_v7" a ON c.award_key = a.award_key
LEFT JOIN label_bridge l ON UPPER(TRIM(CAST(c.platform_family AS VARCHAR))) = l.normalized_label
LEFT JOIN hidden_label_bridge h ON UPPER(TRIM(CAST(c.platform_family AS VARCHAR))) = h.normalized_label
