import hashlib,json
import duckdb
from opportunity_engine.item_requests import link

def test_same_item_request_is_linked_without_resolving_forecast_or_deducting_quantity(tmp_path):
 c=duckdb.connect();tables={}
 def put(n,q):
  p=tmp_path/(n+'.parquet');c.execute("COPY ("+q+") TO '"+str(p)+"' (FORMAT PARQUET)");tables[n]={'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
 put('opportunities',"""SELECT 'forecast' episode_id,'fh' record_hash,'source_item_forecasts' lane,'Forecast' title,'{\"niin\":\"003538074\"}' value_inputs_json,'2026-11-01' window_start,'2029-01-01' window_end,NULL::VARCHAR native_identifier,NULL::VARCHAR source_url,'f' source_evidence_id,'forecast-file' source_locator
 UNION ALL SELECT 'request','rh','published_item_procurement','Diaphragm request','{\"niin\":\"003538074\",\"quantity\":10,\"request_unit\":\"EA\",\"purchase_request_number\":\"12345\"}',NULL,'2026-11-20','SPE4A727Q0117','https://example.gov/request','r','request-line'
 UNION ALL SELECT 'elapsed','old','published_item_procurement','Old request','{\"niin\":\"003538074\"}',NULL,'2026-10-01','old',NULL,'old','old'
 UNION ALL SELECT 'different','different','published_item_procurement','Other request','{\"niin\":\"999999999\"}',NULL,'2026-11-20','other',NULL,'other','other'""")
 put('demand_decisions',"SELECT 'forecast' episode_id,'source_forecast' stage UNION ALL SELECT 'request','published_requirement' UNION ALL SELECT 'elapsed','resolved_or_elapsed' UNION ALL SELECT 'different','published_requirement'")
 put('company_item_valuations',"SELECT 'CAGE:45934' company_id,'forecast' episode_id,'fh' record_hash,100::BIGINT quantity,30000.0::DOUBLE market_price_lot_scenario")
 (tmp_path/'manifest.json').write_text(json.dumps(dict(as_of='2026-10-31',tables=tables)));link(tmp_path);m=json.loads((tmp_path/'manifest.json').read_text())
 rows=c.execute('SELECT forecast_episode_id,request_episode_id,requested_quantity FROM read_parquet(?)',[m['tables']['item_request_progression']['path']]).fetchall()
 assert rows==[('forecast','request',10)]
 row=c.execute('SELECT quantity,market_price_lot_scenario,current_request_count,related_requests_json FROM read_parquet(?)',[m['tables']['company_item_valuations']['path']]).fetchone()
 assert row[:3]==(100,30000,1)
 assert json.loads(row[3])[0]['source_evidence_id']=='r'
 assert c.execute('SELECT stage FROM read_parquet(?) WHERE episode_id=\'forecast\'',[tables['demand_decisions']['path']]).fetchone()[0]=='source_forecast'
 c.close()
