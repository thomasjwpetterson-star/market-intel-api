CREATE TABLE __TARGET_TABLE__
WITH (format = 'PARQUET', parquet_compression = 'ZSTD', external_location = '__TARGET_LOCATION__') AS
SELECT
    CASE WHEN UPPER(TRIM(platform_family)) = 'HELLFIRE / JAGM'
        THEN NULL ELSE platform_family END AS platform_family,
    platform_family AS legacy_platform_family,
    niin,
    wsdc_code,
    association_source,
    canonical_entity_id AS platform_entity_id,
    platform_identity_key,
    entity_type AS platform_entity_type,
    association_type AS platform_association_scope,
    financial_allocation_eligible AS pricing_evidence_eligible,
    platform_reference_release_id
FROM "market_intel_platform_candidate"."platform_bom_cache_candidate_20261015_hellfire_jagm_v7"