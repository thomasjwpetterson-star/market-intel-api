SELECT
    source_snapshot_date,
    award_year,
    COUNT(*) AS financial_lines,
    COUNT(DISTINCT financial_line_id) AS distinct_financial_lines,
    SUM(line_value) AS observed_value,
    MIN(award_date) AS first_award_date,
    MAX(award_date) AS last_award_date,
    SUM(CASE WHEN reference_part_number_count = 1 THEN 1 ELSE 0 END) AS single_part_lines,
    SUM(CASE WHEN reference_part_number_count > 1 THEN 1 ELSE 0 END) AS multiple_part_lines,
    SUM(CASE WHEN reference_nsn_count > 1 THEN 1 ELSE 0 END) AS alternate_reported_nsn_lines
FROM market_intel_silver.fact_procurement_history_v2
WHERE source_snapshot_date = DATE '2026-08-17'
GROUP BY 1, 2
ORDER BY 2;
