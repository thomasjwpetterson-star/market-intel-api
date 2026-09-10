SELECT
    'SILVER_FINANCIAL_CALENDAR_2026' AS dataset,
    COUNT(*) AS row_count,
    SUM(TRY_CAST(order_qty AS DOUBLE) * TRY_CAST(netprice AS DOUBLE)) AS observed_value,
    MIN(award_date) AS first_date,
    MAX(award_date) AS last_date
FROM market_intel_silver.view_dla_contract_history_financial
WHERE TRY_CAST(award_year AS INTEGER) = 2026

UNION ALL

SELECT
    'GOLD_GLOBAL_SPEND_DLA_FY2026' AS dataset,
    COUNT(*) AS row_count,
    SUM(spend_amount) AS observed_value,
    MIN(TRY_CAST(action_date AS DATE)) AS first_date,
    MAX(TRY_CAST(action_date AS DATE)) AS last_date
FROM market_intel_gold.global_spend_transactions
WHERE source_system = 'DLA'
  AND year = 2026

UNION ALL

SELECT
    'GOLD_NIIN_CAGE_DLA_FY2026' AS dataset,
    SUM(financial_line_count) AS row_count,
    SUM(observed_dla_value_usd) AS observed_value,
    MIN(earliest_action_date) AS first_date,
    MAX(latest_action_date) AS last_date
FROM market_intel_gold.view_dla_niin_cage_fiscal
WHERE fiscal_year = 2026;
