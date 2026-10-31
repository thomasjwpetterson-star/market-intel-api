import json
import duckdb
from opportunity_engine.product_market import build


def test_aligned_other_seller_market_keeps_part_roles_dates_and_direct_sales_separate(tmp_path):
 c=duckdb.connect();tables={}
 def table(name,sql):
  p=tmp_path/(name+'.parquet');c.execute('COPY ('+sql+') TO ? (FORMAT PARQUET)',[str(p)]);tables[name]={'path':str(p)};return p
 table('company_profiles',"SELECT 'CAGE:45934' company_id")
 table('company_activity_baselines',"SELECT 'CAGE:45934' company_id,100::DOUBLE annual_net_obligations,'2025-11-01' period_start,'2026-10-31' period_end")
 table('company_reporting_lag_baselines',"SELECT 'CAGE:45934' company_id,200::DOUBLE lag_adjusted_annual_net_obligations,200::DOUBLE lag_adjusted_annual_dod_net_obligations,'2025-08-01' lag_adjusted_period_start,'2026-07-31' lag_adjusted_period_end")
 refs=table('refs',"SELECT * FROM (VALUES ('45934','012345678','PART-A','2','3','D'),('45934','012345678','PART-A','2','5','D'),('45934','012345679','PART-B','9','3','B'),('45934','012345680','PART-C','2','5','L'),('45934','012345681','PART-D','2','5','K')) t(cage_code,niin,part_number,rnvc_codes,rncc_codes,rnsc_codes)")
 table('catalogue_sales_evidence',"SELECT * FROM (VALUES ('CAGE:45934','012345678','PART-A','2026-07-01','11111',false,300::DOUBLE),('CAGE:45934','012345678','PART-A','2026-07-01','45934',true,80::DOUBLE),('CAGE:45934','012345678','PART-Z','2026-07-01','22222',false,20::DOUBLE),('CAGE:45934','012345678','PART-A','2026-08-01','11111',false,900::DOUBLE),('CAGE:45934','012345679','PART-B','2026-07-01','11111',false,50::DOUBLE)) t(company_id,niin,part_number,transaction_date,supplier_cage,direct_seller,record_amount_usd)")
 (tmp_path/'manifest.json').write_text(json.dumps(dict(as_of='2026-10-31',sources={'item_reference_facts':{'files':[{'path':str(refs)}]}},tables=tables)))
 build(tmp_path);r=c.execute('SELECT * FROM read_parquet(?)',[str(tmp_path/'company_product_market_coverage.parquet')]);row=dict(zip([x[0] for x in r.description],r.fetchone()))
 assert row['aligned_referenced_item_purchases']==450
 assert row['aligned_other_seller_item_purchases']==370
 assert row['aligned_other_seller_current_part_purchases']==300
 assert row['aligned_other_seller_design_part_purchases']==300
 assert row['direct_and_linked_part_market_reference']==500
 assert row['linked_market_coverage_reference']==1500
 assert row['market_months_observed']==1 and row['referenced_items']==4
 assert row['distributor_reference_items']==1 and row['manufacturer_or_design_control_items']==1
 assert row['procurement_authorized_items']==0 and row['only_nonidentifying_or_inactive_items']==1
 assert 'not manufacturer revenue' in row['product_market_interpretation']
