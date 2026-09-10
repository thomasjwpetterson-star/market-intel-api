WITH current_view AS (
    SELECT
        TRY_CAST(award_year AS INTEGER) AS award_year,
        COUNT(*) AS current_rows,
        SUM(TRY_CAST(order_qty AS DOUBLE) * TRY_CAST(netprice AS DOUBLE)) AS current_value
    FROM market_intel_silver.view_dla_contract_history_financial
    GROUP BY 1
),
candidate_view AS (
    SELECT
        TRY_CAST(award_year AS INTEGER) AS award_year,
        COUNT(*) AS candidate_rows,
        SUM(TRY_CAST(order_qty AS DOUBLE) * TRY_CAST(netprice AS DOUBLE)) AS candidate_value,
        MIN(award_date) AS candidate_first_date,
        MAX(award_date) AS candidate_last_date
    FROM market_intel_silver.view_dla_contract_history_financial_v2_candidate
    GROUP BY 1
)
SELECT
    COALESCE(c.award_year, n.award_year) AS award_year,
    c.current_rows,
    n.candidate_rows,
    n.candidate_rows - c.current_rows AS row_change,
    c.current_value,
    n.candidate_value,
    n.candidate_value - c.current_value AS value_change,
    n.candidate_first_date,
    n.candidate_last_date
FROM current_view c
FULL OUTER JOIN candidate_view n
    ON c.award_year = n.award_year
ORDER BY 1;
