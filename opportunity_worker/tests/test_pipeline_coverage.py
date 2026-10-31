import json
import duckdb
from opportunity_engine.pipeline_coverage import build

def test_full_market_coverage_does_not_add_duplicate_requests_or_historical_guides(tmp_path):
 c=duckdb.connect();tables={}
 def put(n,q):
  p=tmp_path/(n+'.parquet');c.execute('COPY ('+q+') TO ? (FORMAT PARQUET)',[str(p)]);tables[n]={'path':str(p)}
 put('company_profiles',"SELECT 'CAGE:11111' company_id,'Manufacturer' company_name")
 put('company_activity_baselines',"SELECT 'CAGE:11111' company_id,'Manufacturer' company_name,100000000.0::DOUBLE annual_net_obligations,100000000.0::DOUBLE annual_dod_net_obligations,100::BIGINT annual_award_count,'2025-11-01' period_start,'2026-10-31' period_end")
 put('opportunities',"""SELECT id episode_id,'hash' record_hash,ln lane,'DLA' customer_name,inputs value_inputs_json,v::DOUBLE value_median,'dla_solicitations' source_name,id native_identifier,'Widget' title,'2026-10-20' notice_posted_date FROM (VALUES ('a','published_item_procurement','{"niin":"012345678","purchase_request_number":"123","quantity":10,"observed_unit_price":100,"request_unit":"EA"}',1000),('b','published_item_procurement','{"niin":"012345678","purchase_request_number":"123","quantity":10,"observed_unit_price":100,"request_unit":"EA"}',1000),('forecast','source_item_forecasts','{"niin":"012345678"}',NULL),('work','published_procurement','{}',9000000)) t(id,ln,inputs,v)""")
 put('company_matches',"SELECT 'CAGE:11111' company_id,episode_id,'hash' opportunity_hash FROM read_parquet('"+tables['opportunities']['path']+"')")
 put('demand_decisions',"SELECT episode_id,CASE WHEN lane='source_item_forecasts' THEN 'source_forecast' ELSE 'published_requirement' END stage FROM read_parquet('"+tables['opportunities']['path']+"')")
 put('company_item_valuations',"SELECT 'CAGE:11111' company_id,'forecast' episode_id,'hash' record_hash,'catalogue_EA_assumed_forecast_unit_not_stated' valuation_status,5000.0 market_price_lot_scenario,10000.0 government_reference_lot_scenario")
 put('company_work_matches',"SELECT NULL::VARCHAR company_id,NULL::VARCHAR episode_id,NULL::VARCHAR opportunity_hash,NULL::VARCHAR review_category WHERE false")
 put('opportunity_quality_reviews',"SELECT NULL::VARCHAR episode_id,NULL::VARCHAR record_hash,NULL::VARCHAR category_override WHERE false")
 put('company_notice_item_prices',"SELECT NULL::VARCHAR company_id,NULL::VARCHAR episode_id,NULL::VARCHAR record_hash,NULL::VARCHAR items_json WHERE false")
 (tmp_path/'manifest.json').write_text(json.dumps(dict(as_of='2026-10-31',tables=tables)))
 build(tmp_path);m=json.loads((tmp_path/'manifest.json').read_text());row=c.execute('SELECT * FROM read_parquet(?)',[m['tables']['company_pipeline_coverage']['path']]).fetchone();names=[x[0] for x in c.description];r=dict(zip(names,row))
 assert r['coverage_benchmark']==300000000
 assert r['matched_opportunities']==4 and r['work_opportunities']==1 and r['item_opportunities']==3
 assert r['priced_published_demand']==1000 and r['full_horizon_forecast_scenario']==5000
 assert r['prospects_with_material_value_guide']==1
 assert r['coverage_status']=='priced_coverage_below_benchmark_before_overlap'
