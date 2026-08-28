CREATE OR REPLACE VIEW "market_intel_gold"."dashboard_summary_v2" AS
WITH clean_ref_psc AS (
    SELECT
        psc_code,
        MAX(product_and_service_code_name) AS psc_name
    FROM "market_intel_silver"."ref_psc"
    GROUP BY psc_code
),
clean_ref_naics AS (
    SELECT
        code,
        MAX(title) AS naics_title
    FROM "market_intel_silver"."ref_naics"
    GROUP BY code
)
SELECT
    COALESCE(m.ultimate_parent_name, m.vendor_name) AS parent_name,
    m.vendor_name,
    m.vendor_cage AS cage_code,
    m.year,
    SUBSTR(m.action_date, 6, 2) AS month,
    MIN(m.action_date) AS sort_date,
    m.sub_agency,
    m.market_segment,
    m.platform_family,
    m.platform_attribution_status,
    m.platform_attribution_source,
    m.psc AS psc_code,
    COALESCE(r.psc_name, m.psc) AS psc_description,
    m.naics_code,
    n.naics_title AS naics_description,
    MAX(m.city) AS city,
    MAX(m.state) AS state,
    MAX(m.country) AS country,
    COUNT(*) AS contract_count,
    CAST(SUM(m.spend_amount) AS DOUBLE) AS total_spend,
    CAST(SUM(m.platform_attributed_spend_amount) AS DOUBLE) AS platform_attributed_spend,
    CAST(SUM(m.shared_use_exposure_amount) AS DOUBLE) AS shared_use_exposure
FROM "market_intel_gold"."dashboard_master_view" m
LEFT JOIN clean_ref_psc r
    ON TRIM(CAST(m.psc AS VARCHAR)) = TRIM(CAST(r.psc_code AS VARCHAR))
LEFT JOIN clean_ref_naics n
    ON TRIM(CAST(m.naics_code AS VARCHAR)) = TRIM(CAST(n.code AS VARCHAR))
GROUP BY
    COALESCE(m.ultimate_parent_name, m.vendor_name),
    m.vendor_name,
    m.vendor_cage,
    m.year,
    SUBSTR(m.action_date, 6, 2),
    m.sub_agency,
    m.market_segment,
    m.platform_family,
    m.platform_attribution_status,
    m.platform_attribution_source,
    m.psc,
    COALESCE(r.psc_name, m.psc),
    m.naics_code,
    n.naics_title;
