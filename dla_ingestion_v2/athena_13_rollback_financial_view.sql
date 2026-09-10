CREATE OR REPLACE VIEW market_intel_silver.view_dla_contract_history_financial AS
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
FROM market_intel_silver.view_dla_contract_history_financial_legacy_20260910;
