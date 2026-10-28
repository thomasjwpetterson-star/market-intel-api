CREATE TABLE __TARGET_TABLE__
WITH (format = 'PARQUET', parquet_compression = 'ZSTD', external_location = '__TARGET_LOCATION__') AS
WITH hidden_label_bridge AS (
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
    p.platform_family,
    p.platform_family AS legacy_platform_family,
    p.niin,
    p.wsdc_code,
    p.association_source,
    p.canonical_entity_id AS platform_entity_id,
    p.platform_identity_key,
    p.entity_type AS platform_entity_type,
    p.association_type AS platform_association_scope,
    p.financial_allocation_eligible AS pricing_evidence_eligible,
    p.platform_reference_release_id
FROM "market_intel_platform_candidate"."platform_bom_cache_candidate_20261015_hellfire_jagm_v7" p
LEFT JOIN hidden_label_bridge h ON UPPER(TRIM(p.platform_family)) = h.normalized_label
WHERE h.normalized_label IS NULL
