CREATE TABLE __TARGET_TABLE__
WITH (format='PARQUET', parquet_compression='ZSTD', external_location='__TARGET_LOCATION__') AS
WITH hidden_label_bridge AS (
    SELECT UPPER(TRIM(legacy_platform_family)) AS normalized_label
    FROM "market_intel_platform_candidate"."legacy_to_canonical_bridge_reviewed_overlay_20260924_agav_high_value_v5"
    WHERE match_status = 'MATCHED' AND platform_visible = FALSE
    GROUP BY 1
)
SELECT
    n.niin,
    CASE WHEN h.normalized_label IS NOT NULL THEN NULL ELSE n.canonical_entity_id END AS canonical_entity_id,
    CASE WHEN h.normalized_label IS NOT NULL THEN NULL ELSE n.canonical_label END AS canonical_label,
    CASE WHEN h.normalized_label IS NOT NULL THEN NULL ELSE n.entity_type END AS entity_type,
    CASE WHEN h.normalized_label IS NOT NULL THEN NULL ELSE n.platform_identity_key END AS platform_identity_key,
    CASE WHEN h.normalized_label IS NOT NULL THEN 'REVIEWED_NON_PLATFORM' ELSE n.identity_resolution_status END AS identity_resolution_status,
    CASE WHEN h.normalized_label IS NOT NULL THEN NULL ELSE n.platform_family END AS platform_family,
    n.market_segment,
    CASE WHEN h.normalized_label IS NOT NULL THEN NULL ELSE n.platform_families END AS platform_families,
    n.market_segments,
    CASE WHEN h.normalized_label IS NOT NULL THEN CAST(0 AS BIGINT) ELSE n.platform_count END AS platform_count,
    n.market_segment_count,
    CASE WHEN h.normalized_label IS NOT NULL THEN FALSE ELSE n.has_multiple_platforms END AS has_multiple_platforms,
    CASE WHEN h.normalized_label IS NOT NULL THEN 'REVIEWED_NON_PLATFORM' ELSE n.platform_attribution_status END AS platform_attribution_status,
    CASE WHEN h.normalized_label IS NOT NULL THEN 'REVIEWED_NON_PLATFORM' ELSE n.platform_attribution_source END AS platform_attribution_source,
    CASE WHEN h.normalized_label IS NOT NULL THEN FALSE ELSE n.financial_allocation_eligible END AS financial_allocation_eligible,
    CASE WHEN h.normalized_label IS NOT NULL THEN FALSE ELSE n.candidate_assignment_eligible END AS candidate_assignment_eligible,
    n.platform_reference_release_id
FROM "market_intel_platform_candidate"."niin_platform_cache_sidecar_20261015_hellfire_jagm_v7" n
LEFT JOIN hidden_label_bridge h ON UPPER(TRIM(n.platform_family)) = h.normalized_label
