CREATE TABLE __TARGET_TABLE__
WITH (format='PARQUET', parquet_compression='ZSTD',
      external_location='__TARGET_LOCATION__') AS
WITH platform_values AS (
 SELECT cage_code, platform_family FROM __SUMMARY_TABLE__
 WHERE cage_code IS NOT NULL AND platform_family IS NOT NULL
 UNION SELECT prime_cage AS cage_code, platform_family FROM __NETWORK_TABLE__
 WHERE prime_cage IS NOT NULL AND platform_family IS NOT NULL
 UNION SELECT sub_cage AS cage_code, platform_family FROM __NETWORK_TABLE__
 WHERE sub_cage IS NOT NULL AND platform_family IS NOT NULL
), platform_rollup AS (
 SELECT cage_code,
        ARRAY_JOIN(SLICE(ARRAY_SORT(ARRAY_DISTINCT(ARRAY_AGG(platform_family))),1,5),',')
          AS top_platforms
 FROM platform_values GROUP BY cage_code
)
SELECT p.cage_code,p.vendor_name,p.uei,p.ultimate_parent_name,p.ultimate_parent_uei,
       p.total_lifetime_spend,p.total_contracts,p.last_active_year,p.top_naics_codes,
       COALESCE(r.top_platforms,'') AS top_platforms,p.profile_source,
       p.network_flow_total,p.network_included_report_count,
       p.network_excluded_report_count,p.network_source_report_count,
       p.network_contract_count,p.network_last_active_year,
       '2026-10-15-hellfire-jagm-split-v7' AS platform_reference_release_id
FROM __SOURCE_TABLE__ p
LEFT JOIN platform_rollup r ON p.cage_code=r.cage_code
