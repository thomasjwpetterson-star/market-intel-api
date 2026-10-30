"""Exercise actual SQL retrieval and alternative-price calculations on small records."""
import datetime,json
from pathlib import Path
import duckdb
from opportunity_engine.company_enrichment import augment

def test_portable_pipeline_preserves_scope_provenance_and_prices_forecast(tmp_path):
 c=duckdb.connect();tables={};sources={}
 def put(name,sql,source=False):
  p=tmp_path/(name+'.parquet');c.execute("COPY ("+sql+") TO '"+str(p)+"' (FORMAT PARQUET)")
  import hashlib
  if source:sources[name]={'files':[{'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}]}
  else:tables[name]={'path':str(p)}
 put('company_profiles',"SELECT 'CAGE:45934' company_id,'Ontic' company_name,'profile-hash' profile_hash")
 put('opportunities',"""SELECT 'call' episode_id,'call-hash' record_hash,'published_procurement' lane,'Repair aircraft fuel boost pump' title,'Market research for NSN 1680-01-531-2588' scope_text,'Air Force' customer_name,NULL::VARCHAR value_inputs_json,NULL::VARCHAR window_start,NULL::VARCHAR window_end UNION ALL SELECT 'forecast','forecast-hash','source_item_forecasts','Forecast pressure indicator','Forecast quantities','DLA','{\"niin\":\"012710711\"}','2026-11-01','2028-04-01' UNION ALL SELECT 'unseeded-forecast','new-forecast-hash','source_item_forecasts','Forecast second item','Forecast quantities','DLA','{\"niin\":\"012710712\"}','2026-11-01','2028-04-01' UNION ALL SELECT 'new-item-request','request-hash','published_item_procurement','NSN 6620012710712 / RFQ123','Exact item request','DLA','{\"quantity\":5,\"request_unit\":\"EA\"}',NULL,NULL""")
 put('demand_decisions',"SELECT 'call' episode_id,'qualification_opening' stage UNION ALL SELECT 'forecast','source_forecast' UNION ALL SELECT 'unseeded-forecast','source_forecast' UNION ALL SELECT 'new-item-request','published_requirement'")
 put('company_matches',"SELECT 'existing' match_id,'CAGE:45934' company_id,'forecast' episode_id,95.0::DOUBLE score,'exact-item' match_route,'012710711' matched_terms,'not an approval' company_evidence_caveat,'reference' sample_evidence_id,'match-hash' match_hash,'forecast-hash' opportunity_hash,'profile-hash' profile_hash")
 put('published_calls',"SELECT 'call' episode_id,'Sources Sought' notice_type")
 put('work',"SELECT 'work-id' doc_id,'CAGE:45934' company_id,'subaward_report' source_kind,'PUMP FUEL BOOST' work_text,NULL::VARCHAR psc,NULL::VARCHAR naics,'2026-06-01' last_observed,'source-report-id' sample_evidence_id,'source-digest' evidence_digest",source=True);sources['company_work']=sources.pop('work')
 put('dla_forecasts',"SELECT n.niin,4::BIGINT forecast_qty,cast(DATE '2026-11-01'+cast(i AS INTEGER)*INTERVAL 1 MONTH AS VARCHAR) forecast_month,'2026-09' source_release,'forecast.txt' source_file,'source-item' source_item_id FROM range(18) t(i) CROSS JOIN (VALUES ('012710711'),('012710712')) n(niin)")
 put('item_reference_facts',"SELECT '45934' cage_code,'012710711' niin,'EA' unit_of_issue,16265.48::DOUBLE govt_estimated_price,8569.59::DOUBLE supplier_average_price,'2024-02-20' last_sold_date,'reference-row-id' evidence_id,'217-21242' part_number,'INDICATOR,PRESSURE' description,'source-snapshot-hash' source_snapshot_sha256,5::BIGINT source_row UNION ALL SELECT '45934','012710712','EA',500.0,250.0,'2024-02-20','second-reference','second-part','SECOND ITEM','source-snapshot-hash',6",source=True)
 (tmp_path/'manifest.json').write_text(json.dumps({'tables':tables,'sources':sources,'as_of':'2026-10-30'}));c.close();augment(tmp_path);c=duckdb.connect()
 matches=c.execute('SELECT company_id,episode_id,sample_evidence_id FROM read_parquet(?)',[str(tmp_path/'company_work_matches.parquet')]).fetchall();assert matches==[('CAGE:45934','call','source-report-id')]
 p=c.execute('SELECT quantity,catalogue_unit,government_reference_lot_scenario,supplier_average_lot_scenario FROM read_parquet(?) ORDER BY niin',[str(tmp_path/'company_item_valuations.parquet')]).fetchone();assert p[0:2]==(72,'EA');assert round(p[2],2)==1171114.56;assert round(p[3],2)==617010.48

 assert c.execute('SELECT count(*) FROM read_parquet(?)',[str(tmp_path/'company_matches.parquet')]).fetchone()[0]==4
 new=c.execute("SELECT quantity,government_reference_lot_scenario,supplier_average_lot_scenario FROM read_parquet(?) WHERE episode_id='unseeded-forecast'",[str(tmp_path/'company_item_valuations.parquet')]).fetchone();assert new==(72,36000.0,18000.0)
 # An in-place rebuild must not duplicate edges or price conflicting catalogue units.
 conflict=tmp_path/'conflicting_references.parquet';original=tmp_path/'item_reference_facts.parquet'
 c.execute("COPY (SELECT * REPLACE(CASE WHEN niin='012710711' THEN '2030-01-01' ELSE last_sold_date END AS last_sold_date) FROM read_parquet(?) UNION ALL SELECT '45934','012710712','BX',500.0,250.0,'2024-02-20','conflicting-unit','second-part','SECOND ITEM','source-snapshot-hash',7) TO '"+str(conflict)+"' (FORMAT PARQUET)",[str(original)]);c.close()
 manifest=json.loads((tmp_path/'manifest.json').read_text());manifest['sources']['item_reference_facts']['files'][0]['path']=str(conflict);(tmp_path/'manifest.json').write_text(json.dumps(manifest));augment(tmp_path,skip_work=True);c=duckdb.connect()
 assert c.execute('SELECT count(*) FROM read_parquet(?)',[str(tmp_path/'company_matches.parquet')]).fetchone()[0]==4
 values=c.execute('SELECT niin,supplier_average_unit_price,government_reference_lot_scenario FROM read_parquet(?)',[str(tmp_path/'company_item_valuations.parquet')]).fetchall();assert values==[('012710711',None,1171114.56)]
