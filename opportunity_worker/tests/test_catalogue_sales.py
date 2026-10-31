import json
import duckdb
from opportunity_engine.catalogue_sales import augment

def test_reseller_sales_are_product_context_and_physical_lines_not_manufacturer_revenue(tmp_path):
 c=duckdb.connect();refs=tmp_path/'refs.parquet';prices=tmp_path/'prices.parquet'
 c.execute("COPY (SELECT * FROM (VALUES ('45934','012345678','PART-A','ref1','snapshot'),('11111','012345678','PART-A','ref2','snapshot'),('45934','012345678','PART-B','ref3','snapshot')) t(cage_code,niin,part_number,evidence_id,source_snapshot_sha256)) TO ? (FORMAT PARQUET)",[str(refs)])
 c.execute("COPY (SELECT 'p1' pricing_record_id,'physical1' source_record_id,'012345678' niin,'1234012345678' nsn,'PART-A' part_number,'Widget' item_name,'27541' supplier_cage,'Seller' supplier_name,'2026-06-01' transaction_date,1000::DOUBLE record_amount_usd,2::DOUBLE quantity,'EA' quantity_unit,500::DOUBLE unit_price_usd,'https://source' source_url,'s3://price' source_s3_uri,'row1' source_row_locator,'sha' source_sha256) TO ? (FORMAT PARQUET)",[str(prices)])
 (tmp_path/'manifest.json').write_text(json.dumps({'as_of':'2026-10-31','sources':{'item_reference_facts':{'files':[{'path':str(refs)}]}},'tables':{'item_market_price_evidence':{'path':str(prices)}}}))
 augment(tmp_path);m=json.loads((tmp_path/'manifest.json').read_text());rows=c.execute("SELECT * FROM read_parquet(?) WHERE company_id='CAGE:45934'",[m['tables']['company_catalogue_sales']['path']]).fetchall();names=[x[0] for x in c.description];r=dict(zip(names,rows[0]))
 assert r['physical_price_lines']==1
 assert r['other_seller_item_sales']==1000
 assert r['exact_part_other_seller_sales']==1000
 assert r['single_catalogue_part_other_seller_sales']==0
 assert r['direct_seller_sales']==0
 assert 'not manufacturer revenue' in r['interpretation']
