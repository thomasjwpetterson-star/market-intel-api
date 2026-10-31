import hashlib,json
import duckdb
import pytest
from opportunity_engine.item_demand import prepare,augment

def test_full_dibbs_refresh_reuses_ids_and_prices_exact_unit_from_dated_lines(tmp_path):
 c=duckdb.connect();sources={}
 def put(n,q):
  p=tmp_path/(n+'.parquet');c.execute("COPY ("+q+") TO '"+str(p)+"' (FORMAT PARQUET)");sources[n]={'files':[{'path':str(p)}]}
 put('demand_candidates',"""SELECT repeat('a',64) episode_id,'old' record_hash,'published_item_procurement' lane,'old' title,'old' scope_text,'DLA' customer_name,NULL::VARCHAR programme,NULL::VARCHAR psc,NULL::VARCHAR naics,NULL::VARCHAR window_start,'2026-11-20' window_end,'date' date_basis,NULL::DOUBLE value_low,NULL::DOUBLE value_median,NULL::DOUBLE value_high,'unpriced' value_basis,'scope' value_scope,'{}' value_inputs_json,'dla_solicitations' source_name,NULL::VARCHAR source_url,'old' source_evidence_id,'SPE4A727Q0117|0001|2915003538074' source_locator,'SPE4A727Q0117' native_identifier,'procurement_published' procurement_state,'outcome_not_established' reconciled_state,'unknown' access_state,'why' rationale,'limits' limitations""")
 put('dla_solicitations',"""SELECT 'SPE4A727Q0117' solicitation_number,'2915003538074' nsn,'Diaphragm' nomenclature,'EA' unit_of_issue,10::BIGINT quantity,'2026-11-20' closing_date,'N' set_aside_code,190::BIGINT delivery_days,NULL::VARCHAR ship_to_dodaac,'0001' line_item_num,'12345' purchase_request_number,'Origin' fob_point,'2026-10-31' ingestion_date""")
 put('item_reference_facts',"SELECT '003538074' niin,'EA' unit_of_issue,1000.0 govt_estimated_price,'ref' evidence_id,'snapshot' source_snapshot_sha256")
 put('dla_forecasts',"SELECT '003538074' niin,DATE '2026-11-01' forecast_month,10::BIGINT forecast_qty")
 put('item_market_price_lines',"""SELECT 'price1' pricing_record_id,'physical1' source_record_id,'003538074' niin,'EA' quantity_unit,4.0 quantity,200.0 unit_price_usd,TRUE include_in_mimir_range,'2026-08-01' transaction_date,'27541' supplier_cage,'https://example.gov' source_url,'s3://source' source_s3_uri,'line 1' source_row_locator,'hash' source_sha256
 UNION ALL SELECT 'price2','physical1','003538074','EA',4,200,TRUE,'2026-08-01','27541','https://example.gov','s3://source','line 1','hash'
 UNION ALL SELECT 'price3','physical3','003538074','EA',8,400,TRUE,'2026-09-01','1Y249','https://example.gov','s3://source','line 3','hash'
 UNION ALL SELECT 'future','future','003538074','EA',1,9000,TRUE,'2027-01-01','45934','https://example.gov','s3://source','future','hash'""")
 out=prepare(sources,tmp_path/'derived','2026-10-31')
 row=c.execute('SELECT episode_id,value_median,value_inputs_json FROM read_parquet(?)',[out['demand_candidates']['path']]).fetchone()
 assert row[0]=='a'*64
 assert row[1]==3000
 data=json.loads(row[2]);assert data['market_observations']==2
 assert data['observed_unit_price']==300
 assert data['quantity']==10 and data['request_unit']=='EA'
 assert out['item_market_price_evidence']['rows']==2
 # Same item with wrong unit never borrows the EA price.
 price=c.execute('SELECT unit,market_price_median,latest_price_date FROM read_parquet(?)',[out['item_price_references']['path']]).fetchone()
 assert price==('EA',300,'2026-09-01')
 c.close()

@pytest.mark.parametrize('correction_month',[None,'2026-11-01','2027-03-01'])
def test_forecast_years_prices_stock_and_unverified_pack_units_remain_separate(tmp_path,correction_month):
 c=duckdb.connect();tables={};sources={}
 def put(n,q,source=False):
  p=tmp_path/(n+'.parquet');c.execute("COPY ("+q+") TO '"+str(p)+"' (FORMAT PARQUET)")
  sha=hashlib.sha256(p.read_bytes()).hexdigest()
  if source:sources[n]={'files':[{'path':str(p),'sha256':sha}]}
  else:tables[n]={'path':str(p),'sha256':sha}
 put('opportunities',"SELECT 'op' episode_id,'hash' record_hash,'source_item_forecasts' lane,'{\"niin\":\"003538074\"}' value_inputs_json,'2026-12-01' window_start,'2027-01-01' window_end UNION ALL SELECT 'pack','h','source_item_forecasts','{\"niin\":\"001671344\"}','2026-11-01','2027-01-01'")
 put('company_matches',"SELECT 'CAGE:45934' company_id,'op' episode_id,'hash' opportunity_hash UNION ALL SELECT 'CAGE:45934','pack','h'")
 put('dla_forecasts',"""SELECT '003538074' niin,'2026-12-01' forecast_month,10::BIGINT forecast_qty,'release' source_release,'file' source_file,'1' source_item_id
 UNION ALL SELECT '003538074','2027-01-01',20,'release','file','2' UNION ALL SELECT '001671344','2026-11-01',10000,'release','file','3'"""+(" UNION ALL SELECT '003538074','"+correction_month+"',-1,'release','file','correction'" if correction_month else ''))
 put('dla_inventory',"""SELECT '003538074' niin,'2026-08-31' snapshot_date,'7' total_stock,'2' backorder_qty,'30' annual_demand_quantity,'A' condition_code,'release' source_release,'stock-file' source_file,TRUE public_release_eligible
 UNION ALL SELECT '003538074','2026-07-31','40','0','30','A','release','old-file',TRUE""")
 put('dla_reorder_points',"""SELECT '003538074' niin,'2026-08-31' snapshot_date,7.0 total_stock,10.0 reorder_point,'A' condition_code,'release' source_release,'file' source_file
 UNION ALL SELECT '003538074','2026-07-31',40,10,'A','release','file'""",True)
 put('item_price_references',"""SELECT '003538074' niin,'EA' unit,'EA' catalogue_unit,1000.0 government_reference_unit_price,200.0 market_price_low,300.0 market_price_median,400.0 market_price_high,2::BIGINT market_observations,2::BIGINT market_sellers,'2026-08-01' first_price_date,'2026-09-01' latest_price_date,'[]' price_preview_json,'ref' sample_reference_evidence,'snapshot' source_snapshot_sha256
 UNION ALL SELECT '001671344','HD','HD',40,20,30,40,2,2,'2026-08-01','2026-09-01','[]','packref','snapshot'""",True)
 put('item_market_price_evidence',"SELECT 'line' pricing_record_id,'003538074' niin",True)
 (tmp_path/'manifest.json').write_text(json.dumps(dict(as_of='2026-10-31',tables=tables,sources=sources)))
 augment(tmp_path);m=json.loads((tmp_path/'manifest.json').read_text())
 rows=c.execute('SELECT niin,quantity,government_reference_lot_scenario,market_price_lot_scenario,stock_on_hand,inventory_signal,annual_scenarios_json,valuation_status FROM read_parquet(?) ORDER BY niin',[m['tables']['company_item_valuations']['path']]).fetchall()
 assert rows[0][0]=='001671344' and rows[0][2:4]==(None,None)
 assert rows[0][-1]=='forecast_unit_conversion_unverified'
 if correction_month:
  assert rows[1][1:6]==(29,None,None,7,'crossed_below_reorder_point')
  assert rows[1][-1]=='negative_source_quantity' and rows[1][6] is None
 else:
  assert rows[1][1:6]==(30,30000,9000,7,'crossed_below_reorder_point')
  years=json.loads(rows[1][6]);assert [y['quantity'] for y in years]==[10,20]
  assert sum(y['market_price_scenario'] for y in years)==9000
 # A quality pass has downloaded the immutable output tables; source paths
 # from another worker may no longer exist.
 for name in ('item_price_references','item_market_price_evidence'):
  m['sources'][name]['files'][0]['path']=str(tmp_path/'previous-worker-missing.parquet')
 (tmp_path/'manifest.json').write_text(json.dumps(m));augment(tmp_path)
 again=c.execute('SELECT market_price_lot_scenario,valuation_status FROM read_parquet(?) WHERE niin=\'003538074\'',[m['tables']['company_item_valuations']['path']]).fetchone()
 assert again==((None,'negative_source_quantity') if correction_month else (9000,'catalogue_EA_assumed_forecast_unit_not_stated'))
 c.close()
