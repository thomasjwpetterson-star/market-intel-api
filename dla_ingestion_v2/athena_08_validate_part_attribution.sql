SELECT
    part_number_reference_status,
    COUNT(*) AS financial_lines,
    COUNT(DISTINCT niin) AS distinct_niins,
    COUNT(DISTINCT cage) AS distinct_cages,
    SUM(TRY_CAST(order_qty AS DOUBLE) * TRY_CAST(netprice AS DOUBLE)) AS observed_value,
    100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS line_share_pct,
    100.0 * SUM(TRY_CAST(order_qty AS DOUBLE) * TRY_CAST(netprice AS DOUBLE))
        / SUM(SUM(TRY_CAST(order_qty AS DOUBLE) * TRY_CAST(netprice AS DOUBLE))) OVER ()
        AS value_share_pct
FROM market_intel_silver.view_dla_contract_history_financial_v2_candidate
WHERE TRY_CAST(award_year AS INTEGER) = 2026
GROUP BY 1
ORDER BY 1;
