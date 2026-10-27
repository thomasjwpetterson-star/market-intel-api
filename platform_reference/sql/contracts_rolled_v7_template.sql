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
    FROM "market_intel_platform_candidate"."legacy_to_canonical_bridge_reviewed_overlay_20260924_agav_high_value_v5"
    WHERE match_status = 'MATCHED'
      AND platform_visible
      AND effective_canonical_entity_id IS NOT NULL
    GROUP BY 1
), hidden_label_bridge AS (
    SELECT UPPER(TRIM(legacy_platform_family)) AS normalized_label
    FROM "market_intel_platform_candidate"."legacy_to_canonical_bridge_reviewed_overlay_20260924_agav_high_value_v5"
    WHERE match_status = 'MATCHED' AND platform_visible = FALSE
    GROUP BY 1
)
SELECT
    c."source_system" AS "source_system",
    c."award_key" AS "award_key",
    c."contract_id" AS "contract_id",
    c."last_action_date" AS "last_action_date",
    c."start_date" AS "start_date",
    c."first_observed_action_date" AS "first_observed_action_date",
    c."total_spend" AS "total_spend",
    c."observed_net_obligations" AS "observed_net_obligations",
    c."action_count" AS "action_count",
    c."first_year" AS "first_year",
    c."vendor_name" AS "vendor_name",
    c."vendor_cage" AS "vendor_cage",
    c."sub_agency" AS "sub_agency",
    c."parent_agency" AS "parent_agency",
    c."description" AS "description",
    c."base_award_description" AS "base_award_description",
    c."latest_action_description" AS "latest_action_description",
    CASE
        WHEN a.award_key IS NOT NULL THEN a.platform_family
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.niin IS NOT NULL THEN
            CASE WHEN UPPER(TRIM(CAST(n.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN NULL ELSE n.platform_family END
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' THEN NULL
        WHEN UPPER(TRIM(CAST(c.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN NULL
        WHEN h.normalized_label IS NOT NULL THEN NULL
        WHEN NULLIF(TRIM(CAST(c.platform_family AS VARCHAR)), '') IS NOT NULL THEN COALESCE(l.canonical_label, c.platform_family)
        ELSE c.platform_family END AS "platform_family",
    CASE
            WHEN a.award_key IS NOT NULL THEN a.platform_family
            WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.niin IS NOT NULL AND UPPER(TRIM(CAST(n.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN NULL
            WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.niin IS NOT NULL THEN n.platform_families
            WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' THEN NULL
            WHEN UPPER(TRIM(CAST(c.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN NULL
            WHEN h.normalized_label IS NOT NULL THEN NULL
            WHEN NULLIF(TRIM(CAST(c.platform_family AS VARCHAR)), '') IS NOT NULL THEN COALESCE(l.canonical_label, c.platform_family)
            ELSE c.platform_families END AS "platform_families",
    CASE
            WHEN a.award_key IS NOT NULL THEN CAST(1 AS BIGINT)
            WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.niin IS NOT NULL AND UPPER(TRIM(CAST(n.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN CAST(0 AS BIGINT)
            WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.niin IS NOT NULL THEN n.platform_count
            WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' THEN CAST(0 AS BIGINT)
            WHEN UPPER(TRIM(CAST(c.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN CAST(0 AS BIGINT)
            WHEN h.normalized_label IS NOT NULL THEN CAST(0 AS BIGINT)
            ELSE c.platform_count END AS "platform_count",
    CASE
        WHEN a.award_key IS NOT NULL THEN 'MANUAL_AWARD_MAPPING'
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.niin IS NOT NULL AND UPPER(TRIM(CAST(n.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM'
            THEN 'UNRESOLVED_MIXED_LEGACY_FAMILY'
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.niin IS NOT NULL THEN n.platform_attribution_status
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' THEN 'UNMAPPED'
        WHEN UPPER(TRIM(CAST(c.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN 'UNRESOLVED_MIXED_LEGACY_FAMILY'
        WHEN h.normalized_label IS NOT NULL THEN 'REVIEWED_NON_PLATFORM'
        ELSE c.platform_attribution_status END AS "platform_attribution_status",
    CAST(CASE WHEN (CASE
        WHEN a.award_key IS NOT NULL THEN
            COALESCE(a.canonical_entity_id IS NOT NULL
                AND a.pricing_evidence_eligible, FALSE)
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' THEN
            COALESCE(n.canonical_entity_id IS NOT NULL
                AND n.financial_allocation_eligible, FALSE)
        ELSE FALSE END) THEN COALESCE(c.total_spend, 0) ELSE 0 END AS DOUBLE) AS "platform_attributed_spend_amount",
    CAST(CASE WHEN (CASE
        WHEN a.award_key IS NOT NULL THEN 'MANUAL_AWARD_MAPPING'
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.niin IS NOT NULL AND UPPER(TRIM(CAST(n.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM'
            THEN 'UNRESOLVED_MIXED_LEGACY_FAMILY'
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.niin IS NOT NULL THEN n.platform_attribution_status
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' THEN 'UNMAPPED'
        WHEN UPPER(TRIM(CAST(c.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN 'UNRESOLVED_MIXED_LEGACY_FAMILY'
        WHEN h.normalized_label IS NOT NULL THEN 'REVIEWED_NON_PLATFORM'
        ELSE c.platform_attribution_status END) = 'MULTI_PLATFORM_REFERENCE' OR (CASE
        WHEN a.award_key IS NOT NULL THEN 'MANUAL_AWARD_MAPPING'
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.niin IS NOT NULL AND UPPER(TRIM(CAST(n.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM'
            THEN 'UNRESOLVED_MIXED_LEGACY_FAMILY'
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.niin IS NOT NULL THEN n.platform_attribution_status
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' THEN 'UNMAPPED'
        WHEN UPPER(TRIM(CAST(c.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN 'UNRESOLVED_MIXED_LEGACY_FAMILY'
        WHEN h.normalized_label IS NOT NULL THEN 'REVIEWED_NON_PLATFORM'
        ELSE c.platform_attribution_status END) = 'SHARED_NIIN_UNALLOCATED' THEN COALESCE(c.total_spend, 0) ELSE 0 END AS DOUBLE) AS "shared_use_exposure_amount",
    CASE
            WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.platform_count = 1
             AND NULLIF(TRIM(n.market_segment), '') IS NOT NULL THEN n.market_segment
            ELSE c.market_segment END AS "market_segment",
    c."tech_type" AS "tech_type",
    c."capability_name" AS "capability_name",
    c."naics_code" AS "naics_code",
    c."naics_description" AS "naics_description",
    c."psc" AS "psc",
    c."city" AS "city",
    c."state" AS "state",
    c."country" AS "country",
    c."location_quality" AS "location_quality",
    c."place_of_performance_city" AS "place_of_performance_city",
    c."place_of_performance_state" AS "place_of_performance_state",
    c."place_of_performance_country" AS "place_of_performance_country",
    c."place_of_performance_zip" AS "place_of_performance_zip",
    c."pricing_type" AS "pricing_type",
    c."competition_type" AS "competition_type",
    c."offers_count" AS "offers_count",
    c."set_aside_type" AS "set_aside_type",
    c."solicitation_id" AS "solicitation_id",
    c."nsn" AS "nsn",
    c."niin" AS "niin",
    c."nsn_source_system" AS "nsn_source_system",
    c."nsn_derivation_method" AS "nsn_derivation_method",
    c."nsn_resolution_status" AS "nsn_resolution_status",
    c."year" AS "year",
    c."obligations_fy2019" AS "obligations_fy2019",
    c."action_count_fy2019" AS "action_count_fy2019",
    c."earliest_action_date_fy2019" AS "earliest_action_date_fy2019",
    c."latest_action_date_fy2019" AS "latest_action_date_fy2019",
    c."obligations_fy2020" AS "obligations_fy2020",
    c."action_count_fy2020" AS "action_count_fy2020",
    c."earliest_action_date_fy2020" AS "earliest_action_date_fy2020",
    c."latest_action_date_fy2020" AS "latest_action_date_fy2020",
    c."obligations_fy2021" AS "obligations_fy2021",
    c."action_count_fy2021" AS "action_count_fy2021",
    c."earliest_action_date_fy2021" AS "earliest_action_date_fy2021",
    c."latest_action_date_fy2021" AS "latest_action_date_fy2021",
    c."obligations_fy2022" AS "obligations_fy2022",
    c."action_count_fy2022" AS "action_count_fy2022",
    c."earliest_action_date_fy2022" AS "earliest_action_date_fy2022",
    c."latest_action_date_fy2022" AS "latest_action_date_fy2022",
    c."obligations_fy2023" AS "obligations_fy2023",
    c."action_count_fy2023" AS "action_count_fy2023",
    c."earliest_action_date_fy2023" AS "earliest_action_date_fy2023",
    c."latest_action_date_fy2023" AS "latest_action_date_fy2023",
    c."obligations_fy2024" AS "obligations_fy2024",
    c."action_count_fy2024" AS "action_count_fy2024",
    c."earliest_action_date_fy2024" AS "earliest_action_date_fy2024",
    c."latest_action_date_fy2024" AS "latest_action_date_fy2024",
    c."obligations_fy2025" AS "obligations_fy2025",
    c."action_count_fy2025" AS "action_count_fy2025",
    c."earliest_action_date_fy2025" AS "earliest_action_date_fy2025",
    c."latest_action_date_fy2025" AS "latest_action_date_fy2025",
    c."obligations_fy2026" AS "obligations_fy2026",
    c."action_count_fy2026" AS "action_count_fy2026",
    c."earliest_action_date_fy2026" AS "earliest_action_date_fy2026",
    c."latest_action_date_fy2026" AS "latest_action_date_fy2026",
    c."obligations_fy2027" AS "obligations_fy2027",
    c."action_count_fy2027" AS "action_count_fy2027",
    c."earliest_action_date_fy2027" AS "earliest_action_date_fy2027",
    c."latest_action_date_fy2027" AS "latest_action_date_fy2027",
    CASE
            WHEN a.award_key IS NOT NULL THEN a.canonical_entity_id
            WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.platform_count = 1 THEN n.canonical_entity_id
            ELSE l.canonical_entity_id END AS "platform_entity_id",
    CASE
            WHEN a.award_key IS NOT NULL THEN a.canonical_entity_id
            WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.platform_count = 1 THEN n.platform_identity_key
            WHEN l.canonical_entity_id IS NOT NULL THEN l.canonical_entity_id
            WHEN h.normalized_label IS NOT NULL THEN NULL
            WHEN NULLIF(TRIM(CAST(c.platform_family AS VARCHAR)), '') IS NOT NULL
                THEN CONCAT('LEGACY_LABEL:', UPPER(TRIM(c.platform_family)))
        END AS "platform_identity_key",
    CASE
            WHEN a.award_key IS NOT NULL THEN a.entity_type
            WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.platform_count = 1 THEN n.entity_type
            ELSE l.entity_type END AS "platform_entity_type",
    CASE
            WHEN a.award_key IS NOT NULL THEN a.association_scope
            WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.platform_count = 1 THEN 'NIIN_MEMBERSHIP'
            WHEN h.normalized_label IS NOT NULL THEN NULL
            WHEN NULLIF(TRIM(CAST(c.platform_family AS VARCHAR)), '') IS NOT NULL THEN 'LEGACY_PLATFORM_LABEL'
        END AS "platform_association_scope",
    CASE
        WHEN a.award_key IS NOT NULL THEN
            COALESCE(a.canonical_entity_id IS NOT NULL
                AND a.pricing_evidence_eligible, FALSE)
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' THEN
            COALESCE(n.canonical_entity_id IS NOT NULL
                AND n.financial_allocation_eligible, FALSE)
        ELSE FALSE END AS "pricing_evidence_eligible",
    '2026-10-15-hellfire-jagm-split-v7' AS "platform_reference_release_id"
FROM __SOURCE_TABLE__ c
LEFT JOIN "market_intel_platform_candidate"."award_platform_cache_sidecar_20261015_hellfire_jagm_v7" a ON c.award_key = a.award_key
LEFT JOIN label_bridge l ON UPPER(TRIM(CAST(c.platform_family AS VARCHAR))) = l.normalized_label
LEFT JOIN hidden_label_bridge h ON UPPER(TRIM(CAST(c.platform_family AS VARCHAR))) = h.normalized_label
LEFT JOIN "market_intel_platform_candidate"."niin_platform_cache_sidecar_20261015_hellfire_jagm_v7" n
              ON LPAD(TRIM(CAST(c.niin AS VARCHAR)), 9, '0') = n.niin
