WITH current_view AS (
    SELECT
        TRY_CAST(award_year AS INTEGER) AS award_year,
        COUNT(*) AS row_count,
        SUM(TRY_CAST(order_qty AS DOUBLE) * TRY_CAST(netprice AS DOUBLE)) AS observed_value
    FROM market_intel_silver.view_dla_contract_history_financial
    GROUP BY 1
),
legacy_view AS (
    SELECT
        TRY_CAST(award_year AS INTEGER) AS award_year,
        COUNT(*) AS row_count,
        SUM(TRY_CAST(order_qty AS DOUBLE) * TRY_CAST(netprice AS DOUBLE)) AS observed_value
    FROM market_intel_silver.view_dla_contract_history_financial_legacy_20260910
    GROUP BY 1
)
SELECT
    COALESCE(c.award_year, l.award_year) AS award_year,
    c.row_count AS current_rows,
    l.row_count AS legacy_rows,
    l.row_count - c.row_count AS row_difference,
    c.observed_value AS current_value,
    l.observed_value AS legacy_value,
    l.observed_value - c.observed_value AS value_difference
FROM current_view c
FULL OUTER JOIN legacy_view l
    ON c.award_year = l.award_year
ORDER BY 1;
