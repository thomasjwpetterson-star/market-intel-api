CREATE EXTERNAL TABLE IF NOT EXISTS market_intel_silver.fact_procurement_history_v2 (
    financial_line_id string,
    niin string,
    fsc string,
    unit string,
    cage string,
    contract_number string,
    order_qty string,
    netprice string,
    po_num string,
    po_itmno string,
    procurement_source_rows bigint,
    procurement_attribute_variants bigint,
    security_classification string,
    clin string,
    referenced_piid string,
    std_u_price string,
    piinspiinmod string,
    solic_amendment_number string,
    procurement_source_file string,
    source_reference_rows bigint,
    reference_part_number_count bigint,
    reference_item_name_count bigint,
    reference_nsn_count bigint,
    part_numbers array<string>,
    item_names array<string>,
    reported_nsns array<string>,
    contract_source_file string,
    award_date date,
    fiscal_year int,
    order_quantity decimal(20,3),
    net_price decimal(20,6),
    standard_unit_price decimal(20,6),
    line_value decimal(30,6),
    item_name string,
    nsn string,
    part_number string,
    part_number_reference_status string
)
PARTITIONED BY (
    source_snapshot_date date,
    award_year int
)
STORED AS PARQUET
LOCATION 's3://a-and-d-intel-lake-newaccount/silver/dla/fact_procurement_history_v2/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.source_snapshot_date.type'='date',
    'projection.source_snapshot_date.range'='2026-01-01,NOW',
    'projection.source_snapshot_date.format'='yyyy-MM-dd',
    'projection.source_snapshot_date.interval'='1',
    'projection.source_snapshot_date.interval.unit'='DAYS',
    'projection.award_year.type'='integer',
    'projection.award_year.range'='2019,2035',
    'storage.location.template'='s3://a-and-d-intel-lake-newaccount/silver/dla/fact_procurement_history_v2/source_snapshot_date=${source_snapshot_date}/award_year=${award_year}/'
)
