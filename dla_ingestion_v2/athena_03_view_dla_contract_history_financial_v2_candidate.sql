CREATE OR REPLACE VIEW market_intel_silver.view_dla_contract_history_financial_v2_candidate AS
WITH latest_snapshots AS (
    SELECT
        award_year,
        MAX(source_snapshot_date) AS source_snapshot_date
    FROM market_intel_silver.fact_procurement_history_v2
    GROUP BY 1
),
legacy_history AS (
    SELECT
        niin,
        security_classification,
        fsc,
        unit,
        cage,
        contract_number,
        order_qty,
        award_date,
        netprice,
        po_num,
        po_itmno,
        item_name,
        part_number,
        std_u_price,
        nsn,
        award_year,
        source_reference_rows,
        reference_part_number_count,
        CAST(part_number_reference_status AS VARCHAR(24)) AS part_number_reference_status
    FROM market_intel_silver.view_dla_contract_history_financial
    WHERE TRY_CAST(award_year AS INTEGER) NOT IN (
        SELECT award_year FROM latest_snapshots
    )
),
monthly_history AS (
    SELECT
        h.niin,
        h.security_classification,
        h.fsc,
        h.unit,
        h.cage,
        h.contract_number,
        h.order_qty,
        h.award_date,
        h.netprice,
        h.po_num,
        h.po_itmno,
        h.item_name,
        h.part_number,
        h.std_u_price,
        h.nsn,
        CAST(h.award_year AS VARCHAR) AS award_year,
        h.source_reference_rows,
        h.reference_part_number_count,
        CAST(h.part_number_reference_status AS VARCHAR(24)) AS part_number_reference_status
    FROM market_intel_silver.fact_procurement_history_v2 h
    INNER JOIN latest_snapshots s
        ON h.award_year = s.award_year
       AND h.source_snapshot_date = s.source_snapshot_date
)
SELECT * FROM legacy_history
UNION ALL
SELECT * FROM monthly_history
