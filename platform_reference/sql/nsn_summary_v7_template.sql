CREATE TABLE __TARGET_TABLE__
WITH (format = 'PARQUET', parquet_compression = 'ZSTD', external_location = '__TARGET_LOCATION__') AS

SELECT
    c."niin" AS "niin",
    c."year" AS "year",
    CASE WHEN UPPER(TRIM(CAST(n.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN NULL ELSE n.platform_family END AS "platform_family",
    n.market_segment AS "market_segment",
    CASE WHEN UPPER(TRIM(CAST(n.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN NULL ELSE n.platform_families END AS "platform_families",
    n.market_segments AS "market_segments",
    CASE WHEN UPPER(TRIM(CAST(n.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN CAST(0 AS BIGINT) ELSE n.platform_count END AS "platform_count",
    n.has_multiple_platforms AS "has_multiple_platforms",
    n.platform_attribution_status AS "platform_attribution_status",
    c."sub_agency" AS "sub_agency",
    c."parent_agency" AS "parent_agency",
    CASE WHEN n.platform_count = 1 THEN c.psc END AS "psc",
    c."spend_amount" AS "spend_amount",
    c."contracts" AS "contracts",
    n.canonical_entity_id AS "platform_entity_id",
    n.platform_identity_key AS "platform_identity_key",
    n.entity_type AS "platform_entity_type",
    CASE WHEN n.platform_count = 1 THEN 'NIIN_MEMBERSHIP' END AS "platform_association_scope",
    COALESCE(n.canonical_entity_id IS NOT NULL AND n.financial_allocation_eligible, FALSE) AS "pricing_evidence_eligible",
    '2026-10-15-hellfire-jagm-split-v7' AS "platform_reference_release_id"
FROM __SOURCE_TABLE__ c
LEFT JOIN "market_intel_platform_candidate"."niin_platform_cache_sidecar_20261015_hellfire_jagm_v7" n
              ON LPAD(TRIM(CAST(c.niin AS VARCHAR)), 9, '0') = n.niin