"""Public held-source projections used by every native opportunity refresh."""
DIBBS="""WITH source_rows AS(
SELECT DISTINCT solicitation_number,nsn,unit_of_issue,try_cast(quantity AS bigint) quantity,
cast(try(date_parse(return_by_date,'%m/%d/%Y')) AS varchar) closing_date,small_business_set_aside_indicator set_aside_code,
try_cast(delivery_days AS bigint) delivery_days,cast(NULL AS varchar) ship_to_dodaac,solicitation_line_number line_item_num,purchase_request_number,fob_point,
cast(NULL AS varchar) ingestion_date
FROM market_intel_silver.fact_solicitations
WHERE try(date_parse(return_by_date,'%m/%d/%Y'))>=current_timestamp-INTERVAL '90' DAY AND solicitation_number LIKE 'SPE%'),
checked AS(SELECT *,count(*) OVER(PARTITION BY solicitation_number,line_item_num,nsn) source_variants FROM source_rows),
names AS(SELECT niin,min(item_name) item_name FROM market_intel_silver.ref_flis_nsn GROUP BY niin)
SELECT s.solicitation_number,s.nsn,coalesce(n.item_name,'NSN '||s.nsn) nomenclature,s.unit_of_issue,s.quantity,s.closing_date,s.set_aside_code,s.delivery_days,s.ship_to_dodaac,s.line_item_num,s.purchase_request_number,s.fob_point,s.ingestion_date
FROM checked s LEFT JOIN names n ON n.niin=substr(s.nsn,5,9) WHERE s.source_variants=1"""
MARKET_PRICES="""SELECT pricing_record_id,niin,nsn,part_number,item_name,quantity_unit,cast(quantity AS double) quantity,cast(unit_price_usd AS double) unit_price_usd,cast(record_amount_usd AS double) record_amount_usd,cast(transaction_date AS varchar) transaction_date,supplier_cage,supplier_name,source_system,price_basis,source_record_id,source_row_locator,source_url,source_s3_uri,source_sha256,provenance_status,include_in_mimir_range FROM market_intel_silver.fact_equipment_pricing_records WHERE include_in_mimir_range=true AND unit_price_usd>0 AND niin IS NOT NULL AND source_system='DLA' AND record_kind='PROCUREMENT_LINE_UNIT_PRICE'"""
REORDER="""SELECT nsn,niin,cast(total_stock AS double) total_stock,cast(reorder_point AS double) reorder_point,condition_code,public_release_eligible,cast(snapshot_date AS varchar) snapshot_date,source_release,source_file,cast(retrieval_date AS varchar) retrieval_date FROM market_intel_silver.inventory_reorder_point WHERE snapshot_date IN (SELECT DISTINCT snapshot_date FROM market_intel_silver.inventory_reorder_point ORDER BY snapshot_date DESC LIMIT 2)"""

def queries():return dict(dla_solicitations=DIBBS,item_market_price_lines=MARKET_PRICES,dla_reorder_points=REORDER)
