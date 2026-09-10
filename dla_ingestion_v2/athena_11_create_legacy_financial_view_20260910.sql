CREATE OR REPLACE VIEW market_intel_silver.view_dla_contract_history_financial_legacy_20260910 AS
WITH prepared AS (
    SELECT
        h.*,
        COALESCE(NULLIF(TRIM(CAST(h.contract_number AS VARCHAR)), ''), '<NULL>') AS k_contract_number,
        COALESCE(NULLIF(TRIM(CAST(h.po_num AS VARCHAR)), ''), '<NULL>') AS k_po_num,
        COALESCE(NULLIF(TRIM(CAST(h.po_itmno AS VARCHAR)), ''), '<NULL>') AS k_po_itmno,
        COALESCE(CAST(h.award_date AS VARCHAR), '<NULL>') AS k_award_date,
        COALESCE(NULLIF(TRIM(CAST(h.cage AS VARCHAR)), ''), '<NULL>') AS k_cage,
        COALESCE(NULLIF(TRIM(CAST(h.niin AS VARCHAR)), ''), '<NULL>') AS k_niin,
        COALESCE(NULLIF(TRIM(CAST(h.fsc AS VARCHAR)), ''), '<NULL>') AS k_fsc,
        COALESCE(NULLIF(TRIM(CAST(h.unit AS VARCHAR)), ''), '<NULL>') AS k_unit,
        COALESCE(NULLIF(TRIM(CAST(h.order_qty AS VARCHAR)), ''), '<NULL>') AS k_order_qty,
        COALESCE(NULLIF(TRIM(CAST(h.netprice AS VARCHAR)), ''), '<NULL>') AS k_netprice
    FROM market_intel_silver.fact_contract_history h
),
line_summary AS (
    SELECT
        k_contract_number,
        k_po_num,
        k_po_itmno,
        k_award_date,
        k_cage,
        k_niin,
        k_fsc,
        k_unit,
        k_order_qty,
        k_netprice,
        COUNT(*) AS source_reference_rows,
        COUNT(DISTINCT NULLIF(TRIM(CAST(part_number AS VARCHAR)), '')) AS reference_part_number_count,
        MIN(NULLIF(TRIM(CAST(part_number AS VARCHAR)), '')) AS single_reference_part_number
    FROM prepared
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),
ranked AS (
    SELECT
        p.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                k_contract_number,
                k_po_num,
                k_po_itmno,
                k_award_date,
                k_cage,
                k_niin,
                k_fsc,
                k_unit,
                k_order_qty,
                k_netprice
            ORDER BY
                CASE
                    WHEN NULLIF(TRIM(CAST(part_number AS VARCHAR)), '') IS NOT NULL THEN 0
                    ELSE 1
                END,
                NULLIF(TRIM(CAST(part_number AS VARCHAR)), '')
        ) AS financial_line_rank
    FROM prepared p
)
SELECT
    r.niin,
    r.security_classification,
    r.fsc,
    r.unit,
    r.cage,
    r.contract_number,
    r.order_qty,
    r.award_date,
    r.netprice,
    r.po_num,
    r.po_itmno,
    r.item_name,
    CASE
        WHEN s.reference_part_number_count = 1 THEN s.single_reference_part_number
        ELSE CAST(NULL AS VARCHAR)
    END AS part_number,
    r.std_u_price,
    r.nsn,
    r.award_year,
    s.source_reference_rows,
    s.reference_part_number_count,
    CAST(
        CASE
            WHEN s.reference_part_number_count = 0 THEN 'NO_PART_REFERENCE'
            WHEN s.reference_part_number_count = 1 THEN 'SINGLE_PART_REFERENCE'
            ELSE 'MULTIPLE_PART_REFERENCES'
        END
        AS VARCHAR(24)
    ) AS part_number_reference_status
FROM ranked r
INNER JOIN line_summary s
    ON r.k_contract_number = s.k_contract_number
   AND r.k_po_num = s.k_po_num
   AND r.k_po_itmno = s.k_po_itmno
   AND r.k_award_date = s.k_award_date
   AND r.k_cage = s.k_cage
   AND r.k_niin = s.k_niin
   AND r.k_fsc = s.k_fsc
   AND r.k_unit = s.k_unit
   AND r.k_order_qty = s.k_order_qty
   AND r.k_netprice = s.k_netprice
WHERE r.financial_line_rank = 1;
