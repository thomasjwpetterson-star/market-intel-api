CREATE OR REPLACE VIEW market_intel_silver.view_dla_part_number_procurement_observed_v2_candidate AS
SELECT
    LPAD(TRIM(CAST(niin AS VARCHAR)), 9, '0') AS niin,
    CASE
        WHEN REGEXP_LIKE(TRIM(CAST(fsc AS VARCHAR)), '^[0-9]{4}$')
         AND REGEXP_LIKE(LPAD(TRIM(CAST(niin AS VARCHAR)), 9, '0'), '^[0-9]{9}$')
        THEN CONCAT(
            TRIM(CAST(fsc AS VARCHAR)),
            LPAD(TRIM(CAST(niin AS VARCHAR)), 9, '0')
        )
    END AS nsn,
    UPPER(TRIM(CAST(cage AS VARCHAR))) AS cage,
    TRIM(CAST(part_number AS VARCHAR)) AS part_number,
    CAST(
        YEAR(award_date) + IF(MONTH(award_date) >= 10, 1, 0)
        AS INTEGER
    ) AS fiscal_year,
    MIN(award_date) AS first_observed_date,
    MAX(award_date) AS last_observed_date,
    COUNT(*) AS procurement_line_count,
    COUNT(DISTINCT contract_number) AS contract_count,
    SUM(TRY_CAST(order_qty AS DOUBLE)) AS observed_units,
    SUM(TRY_CAST(netprice AS DOUBLE) * TRY_CAST(order_qty AS DOUBLE)) AS observed_value,
    SUM(TRY_CAST(netprice AS DOUBLE) * TRY_CAST(order_qty AS DOUBLE))
        / NULLIF(SUM(TRY_CAST(order_qty AS DOUBLE)), 0) AS weighted_average_unit_price,
    MIN(TRY_CAST(netprice AS DOUBLE)) AS minimum_unit_price,
    MAX(TRY_CAST(netprice AS DOUBLE)) AS maximum_unit_price,
    'SINGLE_REPORTED_PART_REFERENCE' AS financial_attribution_basis
FROM market_intel_silver.view_dla_contract_history_financial_v2_candidate
WHERE part_number_reference_status = 'SINGLE_PART_REFERENCE'
  AND NULLIF(TRIM(CAST(part_number AS VARCHAR)), '') IS NOT NULL
GROUP BY 1, 2, 3, 4, 5;
