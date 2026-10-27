CREATE TABLE __TARGET_TABLE__
WITH (format='PARQUET', parquet_compression='ZSTD',
      external_location='__TARGET_LOCATION__') AS
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
SELECT c.contract_id,c.award_key,c.spend_amount,c.completion_date,c.vendor_name,
       c.parent_agency,c.sub_agency,
       CASE WHEN a.award_key IS NOT NULL THEN a.platform_family
            WHEN UPPER(TRIM(c.platform_family))='HELLFIRE / JAGM' THEN NULL
            WHEN h.normalized_label IS NOT NULL THEN NULL
            ELSE COALESCE(l.canonical_label,c.platform_family) END AS platform_family,
       COALESCE(a.canonical_entity_id,l.canonical_entity_id) AS platform_entity_id,
       c.market_segment,c.psc,c.cage_code,c.clean_parent,
       '2026-10-15-hellfire-jagm-split-v7' AS platform_reference_release_id
FROM __SOURCE_TABLE__ c
LEFT JOIN "market_intel_platform_candidate"."award_platform_cache_sidecar_20261015_hellfire_jagm_v7" a ON c.award_key=a.award_key
LEFT JOIN label_bridge l ON UPPER(TRIM(c.platform_family))=l.normalized_label
LEFT JOIN hidden_label_bridge h ON UPPER(TRIM(c.platform_family))=h.normalized_label
