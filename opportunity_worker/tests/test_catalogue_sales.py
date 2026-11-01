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

def test_lag_comparison_keeps_august_history_while_trailing_year_summary_remains_twelve_months(tmp_path):
 from opportunity_engine.product_market import build as build_market
 c=duckdb.connect();refs=tmp_path/'refs.parquet';prices=tmp_path/'prices.parquet';history=tmp_path/'history.parquet';profiles=tmp_path/'profiles.parquet';lag=tmp_path/'lag.parquet'
 c.execute("COPY (SELECT '45934' cage_code,'012345678' niin,'PART-A' part_number,'ref' evidence_id,'snapshot' source_snapshot_sha256,'3' rncc_codes,'2' rnvc_codes,'D' rnsc_codes) TO ? (FORMAT PARQUET)",[str(refs)])
 base="""SELECT id pricing_record_id,id source_record_id,'012345678' niin,'1234012345678' nsn,'PART-A' part_number,'Widget' item_name,'27541' supplier_cage,'Seller' supplier_name,dt transaction_date,amount::DOUBLE record_amount_usd,1::DOUBLE quantity,'EA' quantity_unit,amount::DOUBLE unit_price_usd,'https://source' source_url,'s3://price' source_s3_uri,locator source_row_locator,'sha' source_sha256 FROM (VALUES VALUES_ROWS) p(id,dt,amount,locator)"""
 c.execute('COPY ('+base.replace('VALUES_ROWS',"('current','2026-06-01',1000,'PO1 | 1')")+') TO ? (FORMAT PARQUET)',[str(prices)])
 c.execute('COPY ('+base.replace('VALUES_ROWS',"('older-revision','2026-06-01',9999,'PO1 | 1'),('august','2025-08-15',2000,'PO2 | 1'),('november','2025-11-10',3000,'PO3 | 1'),('too-old','2025-04-01',4000,'PO4 | 1')")+') TO ? (FORMAT PARQUET)',[str(history)])
 c.execute("COPY (SELECT 'CAGE:45934' company_id) TO ? (FORMAT PARQUET)",[str(profiles)])
 c.execute("COPY (SELECT 'CAGE:45934' company_id,1000000::DOUBLE lag_adjusted_annual_net_obligations,1000000::DOUBLE lag_adjusted_annual_dod_net_obligations,'2025-08-01' lag_adjusted_period_start,'2026-07-31' lag_adjusted_period_end) TO ? (FORMAT PARQUET)",[str(lag)])
 m={'as_of':'2026-10-31','sources':{'item_reference_facts':{'files':[{'path':str(refs)}]},'historical_item_purchase_lines':{'files':[{'path':str(history)}]}},'tables':{'item_market_price_evidence':{'path':str(prices)},'company_profiles':{'path':str(profiles)},'company_reporting_lag_baselines':{'path':str(lag)}}};(tmp_path/'manifest.json').write_text(json.dumps(m));augment(tmp_path);m=json.loads((tmp_path/'manifest.json').read_text())
 assert c.execute('SELECT count(*),sum(record_amount_usd) FROM read_parquet(?)',[m['tables']['catalogue_sales_evidence']['path']]).fetchone()==(3,6000.0)
 assert c.execute('SELECT physical_price_lines,observed_catalogue_item_sales,first_sale_date FROM read_parquet(?)',[m['tables']['company_catalogue_sales']['path']]).fetchone()==(2,4000.0,'2025-11-10')
 build_market(tmp_path);m=json.loads((tmp_path/'manifest.json').read_text())
 assert c.execute('SELECT aligned_other_seller_current_part_purchases,market_months_observed,direct_and_linked_part_market_reference FROM read_parquet(?)',[m['tables']['company_product_market_coverage']['path']]).fetchone()==(6000.0,3,1006000.0)
