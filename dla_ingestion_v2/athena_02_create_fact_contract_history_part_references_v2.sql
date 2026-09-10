CREATE EXTERNAL TABLE IF NOT EXISTS market_intel_silver.fact_contract_history_part_references_v2 (
    niin string,
    security_classification string,
    fsc string,
    unit string,
    cage string,
    contract_number string,
    order_qty string,
    netprice string,
    po_num string,
    po_itmno string,
    item_name string,
    part_number string,
    std_u_price string,
    nsn string,
    source_file string,
    financial_line_id string,
    award_date date,
    relationship_source string
)
PARTITIONED BY (
    source_snapshot_date date,
    award_year int
)
STORED AS PARQUET
LOCATION 's3://a-and-d-intel-lake-newaccount/silver/dla/fact_contract_history_part_references_v2/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.source_snapshot_date.type'='date',
    'projection.source_snapshot_date.range'='2026-01-01,NOW',
    'projection.source_snapshot_date.format'='yyyy-MM-dd',
    'projection.source_snapshot_date.interval'='1',
    'projection.source_snapshot_date.interval.unit'='DAYS',
    'projection.award_year.type'='integer',
    'projection.award_year.range'='2019,2035',
    'storage.location.template'='s3://a-and-d-intel-lake-newaccount/silver/dla/fact_contract_history_part_references_v2/source_snapshot_date=${source_snapshot_date}/award_year=${award_year}/'
)
