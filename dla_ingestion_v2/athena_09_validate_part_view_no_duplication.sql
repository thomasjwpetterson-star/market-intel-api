WITH financial_single_part AS (
    SELECT
        CAST(
            YEAR(award_date) + IF(MONTH(award_date) >= 10, 1, 0)
            AS INTEGER
        ) AS fiscal_year,
        COUNT(*) AS financial_lines,
        SUM(TRY_CAST(order_qty AS DOUBLE) * TRY_CAST(netprice AS DOUBLE)) AS financial_value
    FROM market_intel_silver.view_dla_contract_history_financial_v2_candidate
    WHERE part_number_reference_status = 'SINGLE_PART_REFERENCE'
    GROUP BY 1
),
part_view AS (
    SELECT
        fiscal_year,
        SUM(procurement_line_count) AS part_view_lines,
        SUM(observed_value) AS part_view_value
    FROM market_intel_silver.view_dla_part_number_procurement_observed_v2_candidate
    GROUP BY 1
)
SELECT
    COALESCE(f.fiscal_year, p.fiscal_year) AS fiscal_year,
    f.financial_lines,
    p.part_view_lines,
    p.part_view_lines - f.financial_lines AS line_difference,
    f.financial_value,
    p.part_view_value,
    p.part_view_value - f.financial_value AS value_difference
FROM financial_single_part f
FULL OUTER JOIN part_view p
    ON f.fiscal_year = p.fiscal_year
ORDER BY 1;
