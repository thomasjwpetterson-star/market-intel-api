CREATE TABLE __TARGET_TABLE__
WITH (format = 'PARQUET', parquet_compression = 'ZSTD', external_location = '__TARGET_LOCATION__') AS

SELECT
    c."niin" AS "niin",
    c."year" AS "year",
    c."cage" AS "cage",
    c."vendor" AS "vendor",
    c."vendor_entity_source" AS "vendor_entity_source",
    c."cage_status" AS "cage_status",
    c."replacement_cage" AS "replacement_cage",
    c."parent_agency" AS "parent_agency",
    c."sub_agency" AS "sub_agency",
    n.market_segment AS "market_segment",
    CASE WHEN UPPER(TRIM(CAST(n.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN NULL ELSE n.platform_family END AS "platform_family",
    CASE WHEN UPPER(TRIM(CAST(n.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN NULL ELSE n.platform_families END AS "platform_families",
    n.market_segments AS "market_segments",
    CASE WHEN UPPER(TRIM(CAST(n.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN CAST(0 AS BIGINT) ELSE n.platform_count END AS "platform_count",
    n.has_multiple_platforms AS "has_multiple_platforms",
    n.platform_attribution_status AS "platform_attribution_status",
    CASE WHEN n.platform_count = 1 THEN c.psc END AS "psc",
    c."contract_id" AS "contract_id",
    c."last_sold" AS "last_sold",
    c."total_revenue" AS "total_revenue",
    c."total_units_sold" AS "total_units_sold",
    n.canonical_entity_id AS "platform_entity_id",
    n.platform_identity_key AS "platform_identity_key",
    n.entity_type AS "platform_entity_type",
    CASE WHEN n.platform_count = 1 THEN 'NIIN_MEMBERSHIP' END AS "platform_association_scope",
    COALESCE(n.canonical_entity_id IS NOT NULL AND n.financial_allocation_eligible, FALSE) AS "pricing_evidence_eligible",
    '2026-10-15-hellfire-jagm-split-v7' AS "platform_reference_release_id"
FROM __SOURCE_TABLE__ c
LEFT JOIN __NIIN_SIDECAR_TABLE__ n
              ON LPAD(TRIM(CAST(c.niin AS VARCHAR)), 9, '0') = n.niin