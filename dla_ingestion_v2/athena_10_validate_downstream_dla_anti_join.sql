WITH usa_dla_contracts AS (
    SELECT DISTINCT UPPER(TRIM(CAST(award_id_piid AS VARCHAR))) AS contract_id
    FROM market_intel_silver.dataset_prime_contracts
    WHERE award_id_piid IS NOT NULL
      AND (
          UPPER(COALESCE(awarding_sub_agency_name, '')) LIKE '%DEFENSE LOGISTICS AGENCY%'
          OR UPPER(COALESCE(awarding_agency_name, '')) LIKE '%DEFENSE LOGISTICS AGENCY%'
      )
),
current_rows AS (
    SELECT
        CAST(
            YEAR(h.award_date) + IF(MONTH(h.award_date) >= 10, 1, 0)
            AS INTEGER
        ) AS fiscal_year,
        COUNT(*) AS row_count,
        SUM(TRY_CAST(h.netprice AS DOUBLE) * TRY_CAST(h.order_qty AS DOUBLE)) AS observed_value
    FROM market_intel_silver.view_dla_contract_history_financial h
    WHERE NOT EXISTS (
        SELECT 1
        FROM usa_dla_contracts u
        WHERE u.contract_id = UPPER(TRIM(CAST(h.contract_number AS VARCHAR)))
    )
    GROUP BY 1
),
candidate_rows AS (
    SELECT
        CAST(
            YEAR(h.award_date) + IF(MONTH(h.award_date) >= 10, 1, 0)
            AS INTEGER
        ) AS fiscal_year,
        COUNT(*) AS row_count,
        SUM(TRY_CAST(h.netprice AS DOUBLE) * TRY_CAST(h.order_qty AS DOUBLE)) AS observed_value
    FROM market_intel_silver.view_dla_contract_history_financial_v2_candidate h
    WHERE NOT EXISTS (
        SELECT 1
        FROM usa_dla_contracts u
        WHERE u.contract_id = UPPER(TRIM(CAST(h.contract_number AS VARCHAR)))
    )
    GROUP BY 1
)
SELECT
    COALESCE(c.fiscal_year, n.fiscal_year) AS fiscal_year,
    c.row_count AS current_rows,
    n.row_count AS candidate_rows,
    n.row_count - c.row_count AS row_change,
    c.observed_value AS current_value,
    n.observed_value AS candidate_value,
    n.observed_value - c.observed_value AS value_change
FROM current_rows c
FULL OUTER JOIN candidate_rows n
    ON c.fiscal_year = n.fiscal_year
ORDER BY 1;
