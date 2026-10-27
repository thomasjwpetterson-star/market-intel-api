CREATE TABLE __TARGET_TABLE__
WITH (format = 'PARQUET', parquet_compression = 'ZSTD', external_location = '__TARGET_LOCATION__') AS

SELECT
    c."niin" AS "niin",
    c."nsn" AS "nsn",
    c."cage" AS "cage",
    c."description" AS "description",
    c."part_number" AS "part_number",
    c."fsc_code" AS "fsc_code",
    c."total_revenue" AS "total_revenue",
    c."total_units_sold" AS "total_units_sold",
    c."avg_unit_price" AS "avg_unit_price",
    c."last_sold_date" AS "last_sold_date",
    c."annual_revenue_trend" AS "annual_revenue_trend",
    c."market_share_pct" AS "market_share_pct",
    c."direct_sales_market_share_pct" AS "direct_sales_market_share_pct",
    CASE WHEN UPPER(TRIM(CAST(n.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN NULL ELSE n.platform_family END AS "platform_family",
    CASE WHEN UPPER(TRIM(CAST(n.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN NULL ELSE n.platform_families END AS "platform_families",
    CASE WHEN UPPER(TRIM(CAST(n.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN CAST(0 AS BIGINT) ELSE n.platform_count END AS "platform_count",
    n.has_multiple_platforms AS "has_multiple_platforms",
    n.platform_attribution_status AS "platform_attribution_status",
    c."demil_code" AS "demil_code",
    c."shelf_life_code" AS "shelf_life_code",
    c."mgmt_control_code" AS "mgmt_control_code",
    c."unit_of_issue" AS "unit_of_issue",
    c."source_of_supply" AS "source_of_supply",
    c."govt_estimated_price" AS "govt_estimated_price",
    c."acquisition_advice_code" AS "acquisition_advice_code",
    n.canonical_entity_id AS "platform_entity_id",
    n.platform_identity_key AS "platform_identity_key",
    n.entity_type AS "platform_entity_type",
    CASE WHEN n.platform_count = 1 THEN 'NIIN_MEMBERSHIP' END AS "platform_association_scope",
    COALESCE(n.canonical_entity_id IS NOT NULL AND n.financial_allocation_eligible, FALSE) AS "pricing_evidence_eligible",
    '2026-10-15-hellfire-jagm-split-v7' AS "platform_reference_release_id"
FROM __SOURCE_TABLE__ c
LEFT JOIN __NIIN_SIDECAR_TABLE__ n
              ON LPAD(TRIM(CAST(c.niin AS VARCHAR)), 9, '0') = n.niin