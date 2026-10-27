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
),
hidden_label_bridge AS (
    SELECT UPPER(TRIM(legacy_platform_family)) AS normalized_label
    FROM "market_intel_platform_candidate"."legacy_to_canonical_bridge_reviewed_overlay_20260924_agav_high_value_v5"
    WHERE match_status = 'MATCHED' AND platform_visible = FALSE
    GROUP BY 1
),
resolved AS (
 SELECT c.vendor_name, c.vendor_cage AS cage_code, c.sub_agency,
        c.year, SUBSTR(c.action_date,6,2) AS month, c.action_date,
        c.psc, c.naics_code, c.city, c.state, c.country, c.spend_amount,
        CASE
            WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.platform_count = 1
             AND NULLIF(TRIM(n.market_segment), '') IS NOT NULL THEN n.market_segment
            ELSE c.market_segment END AS market_segment,
        CASE
        WHEN a.award_key IS NOT NULL THEN a.platform_family
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.niin IS NOT NULL THEN
            CASE WHEN UPPER(TRIM(CAST(n.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN NULL ELSE n.platform_family END
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' THEN NULL
        WHEN UPPER(TRIM(CAST(c.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN NULL
        WHEN h.normalized_label IS NOT NULL THEN NULL
        WHEN NULLIF(TRIM(CAST(c.platform_family AS VARCHAR)), '') IS NOT NULL THEN COALESCE(l.canonical_label, c.platform_family)
        ELSE c.platform_family END AS platform_family,
        CASE
        WHEN a.award_key IS NOT NULL THEN 'MANUAL_AWARD_MAPPING'
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.niin IS NOT NULL AND UPPER(TRIM(CAST(n.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM'
            THEN 'UNRESOLVED_MIXED_LEGACY_FAMILY'
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.niin IS NOT NULL THEN n.platform_attribution_status
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' THEN 'UNMAPPED'
        WHEN UPPER(TRIM(CAST(c.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN 'UNRESOLVED_MIXED_LEGACY_FAMILY'
        WHEN h.normalized_label IS NOT NULL THEN 'REVIEWED_NON_PLATFORM'
        ELSE c.platform_attribution_status END AS platform_attribution_status,
        CASE
            WHEN a.award_key IS NOT NULL THEN a.platform_attribution_source
            WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.niin IS NOT NULL THEN n.platform_attribution_source
            WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' THEN 'UNMAPPED'
            WHEN l.canonical_entity_id IS NOT NULL THEN 'CANONICAL_LABEL_BRIDGE'
            WHEN UPPER(TRIM(CAST(c.platform_family AS VARCHAR))) = 'HELLFIRE / JAGM' THEN 'UNRESOLVED_MIXED_LEGACY_FAMILY'
            WHEN h.normalized_label IS NOT NULL THEN 'REVIEWED_NON_PLATFORM'
            ELSE c.platform_attribution_source END AS platform_attribution_source,
        CASE
            WHEN a.award_key IS NOT NULL THEN a.canonical_entity_id
            WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.platform_count = 1 THEN n.canonical_entity_id
            ELSE l.canonical_entity_id END AS platform_entity_id,
        CASE
            WHEN a.award_key IS NOT NULL THEN a.canonical_entity_id
            WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' AND n.platform_count = 1 THEN n.platform_identity_key
            WHEN l.canonical_entity_id IS NOT NULL THEN l.canonical_entity_id
            WHEN h.normalized_label IS NOT NULL THEN NULL
            WHEN NULLIF(TRIM(CAST(c.platform_family AS VARCHAR)), '') IS NOT NULL
                THEN CONCAT('LEGACY_LABEL:', UPPER(TRIM(c.platform_family)))
        END AS platform_identity_key,
        CAST(CASE WHEN (CASE
        WHEN a.award_key IS NOT NULL THEN
            COALESCE(a.canonical_entity_id IS NOT NULL
                AND a.pricing_evidence_eligible, FALSE)
        WHEN UPPER(TRIM(COALESCE(CAST(c.source_system AS VARCHAR), ''))) = 'DLA' THEN
            COALESCE(n.canonical_entity_id IS NOT NULL
                AND n.financial_allocation_eligible, FALSE)
        ELSE FALSE END) THEN COALESCE(c.spend_amount, 0) ELSE 0 END AS DOUBLE) AS platform_attributed_spend_amount,
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
        ELSE c.platform_attribution_status END) = 'SHARED_NIIN_UNALLOCATED' THEN COALESCE(c.spend_amount, 0) ELSE 0 END AS DOUBLE) AS shared_use_exposure_amount
 FROM market_intel_gold.dashboard_master_view c
 LEFT JOIN "market_intel_platform_candidate"."award_platform_cache_sidecar_20261015_hellfire_jagm_v7" a ON c.award_key=a.award_key
 LEFT JOIN label_bridge l ON UPPER(TRIM(CAST(c.platform_family AS VARCHAR)))=l.normalized_label
 LEFT JOIN hidden_label_bridge h ON UPPER(TRIM(CAST(c.platform_family AS VARCHAR)))=h.normalized_label
 LEFT JOIN "market_intel_platform_candidate"."niin_platform_cache_sidecar_20261015_hellfire_jagm_v7" n ON LPAD(TRIM(CAST(c.niin AS VARCHAR)),9,'0')=n.niin
), psc_ref AS (
 SELECT psc_code, MAX(product_and_service_code_name) AS psc_name
 FROM market_intel_silver.ref_psc GROUP BY 1
), naics_ref AS (
 SELECT code, MAX(title) AS naics_title FROM market_intel_silver.ref_naics GROUP BY 1
), grouped AS (
 SELECT m.vendor_name, m.cage_code, m.sub_agency, m.market_segment,
        m.platform_family, m.platform_entity_id, m.platform_identity_key,
        m.platform_attribution_status, m.platform_attribution_source,
        m.psc AS psc_code, COALESCE(p.psc_name,m.psc) AS psc_description,
        CAST(m.naics_code AS VARCHAR) AS naics_code,
        n.naics_title AS naics_description,
        MAX(m.city) AS city, MAX(m.state) AS state, MAX(m.country) AS country,
        m.month, m.year, CAST(SUM(m.spend_amount) AS DOUBLE) AS total_spend,
        CAST(SUM(m.platform_attributed_spend_amount) AS DOUBLE) AS platform_attributed_spend,
        CAST(SUM(m.shared_use_exposure_amount) AS DOUBLE) AS shared_use_exposure,
        COUNT(*) AS contract_count
 FROM resolved m
 LEFT JOIN psc_ref p ON TRIM(CAST(m.psc AS VARCHAR))=TRIM(CAST(p.psc_code AS VARCHAR))
 LEFT JOIN naics_ref n ON TRIM(CAST(m.naics_code AS VARCHAR))=TRIM(CAST(n.code AS VARCHAR))
 GROUP BY m.vendor_name,m.cage_code,m.sub_agency,m.market_segment,m.platform_family,
          m.platform_entity_id,m.platform_identity_key,m.platform_attribution_status,
          m.platform_attribution_source,m.psc,COALESCE(p.psc_name,m.psc),
          CAST(m.naics_code AS VARCHAR),n.naics_title,m.month,m.year
)
SELECT *, UPPER(CONCAT(COALESCE(vendor_name,''),' ',COALESCE(platform_family,''),
    ' ',COALESCE(cage_code,''))) AS fast_search,
    '2026-10-15-hellfire-jagm-split-v7' AS platform_reference_release_id
FROM grouped
