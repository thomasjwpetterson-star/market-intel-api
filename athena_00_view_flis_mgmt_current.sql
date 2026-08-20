CREATE OR REPLACE VIEW "market_intel_silver"."view_flis_mgmt_current" AS
WITH normalized AS (
    SELECT
        LPAD(TRIM(CAST(niin AS VARCHAR)), 9, '0') AS niin,
        NULLIF(TRIM(CAST(effective_date AS VARCHAR)), '') AS effective_date,
        NULLIF(TRIM(CAST(moe AS VARCHAR)), '') AS moe,
        NULLIF(TRIM(CAST(aac AS VARCHAR)), '') AS aac,
        NULLIF(TRIM(CAST(sos AS VARCHAR)), '') AS source_of_supply,
        NULLIF(TRIM(CAST(ui AS VARCHAR)), '') AS ui,
        TRY_CAST(unit_price AS DOUBLE) AS unit_price,
        NULLIF(TRIM(CAST(ciic AS VARCHAR)), '') AS ciic,
        NULLIF(TRIM(CAST(slc AS VARCHAR)), '') AS slc,
        NULLIF(TRIM(CAST(mgmt_ctl AS VARCHAR)), '') AS mgmt_ctl,
        NULLIF(TRIM(CAST(row_obs_dt AS VARCHAR)), '') AS row_obs_dt
    FROM "market_intel_silver"."ref_flis_mgmt"
    WHERE niin IS NOT NULL
      AND TRIM(CAST(niin AS VARCHAR)) <> ''
),
ranked AS (
    SELECT
        n.*,
        ROW_NUMBER() OVER (
            PARTITION BY niin
            ORDER BY
                COALESCE(
                    TRY_CAST(effective_date AS DATE),
                    TRY(DATE_PARSE(effective_date, '%Y%m%d')),
                    TRY(DATE_PARSE(effective_date, '%d-%b-%Y')),
                    DATE '1900-01-01'
                ) DESC,
                COALESCE(
                    TRY_CAST(row_obs_dt AS DATE),
                    TRY(DATE_PARSE(row_obs_dt, '%Y%m%d')),
                    DATE '1900-01-01'
                ) DESC,
                COALESCE(moe, '') ASC,
                COALESCE(source_of_supply, '') ASC,
                COALESCE(mgmt_ctl, '') ASC
        ) AS current_rank
    FROM normalized n
),
summaries AS (
    SELECT
        niin,
        ARRAY_JOIN(
            ARRAY_SORT(ARRAY_AGG(DISTINCT source_of_supply) FILTER (WHERE source_of_supply IS NOT NULL)),
            ' | '
        ) AS source_of_supply_codes,
        ARRAY_JOIN(
            ARRAY_SORT(ARRAY_AGG(DISTINCT moe) FILTER (WHERE moe IS NOT NULL)),
            ' | '
        ) AS management_organizations,
        COUNT(*) AS management_record_count
    FROM normalized
    GROUP BY niin
)
SELECT
    r.niin,
    r.effective_date,
    r.moe,
    r.aac,
    r.source_of_supply,
    r.ui,
    r.unit_price,
    r.ciic,
    r.slc,
    r.mgmt_ctl,
    r.row_obs_dt,
    s.source_of_supply_codes,
    s.management_organizations,
    s.management_record_count
FROM ranked r
INNER JOIN summaries s
    ON r.niin = s.niin
WHERE r.current_rank = 1;
