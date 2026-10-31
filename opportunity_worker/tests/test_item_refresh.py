import json
import duckdb
from opportunity_engine.item_refresh import preserve_nonitem


def test_item_update_preserves_nonitem_versions_and_does_not_promote_usage_to_order(tmp_path):
 c=duckdb.connect()
 def put(name,sql):
  p=tmp_path/(name+'.parquet');c.execute('COPY ('+sql+') TO ? (FORMAT PARQUET)',[str(p)]);return p
 old=put('old',"SELECT * FROM (VALUES ('work','original','published_procurement','Avionics research'),('forecast','prior','source_item_forecasts','Old forecast')) t(episode_id,record_hash,lane,title)")
 decisions=put('decisions',"SELECT 'work' episode_id,'published_requirement' stage")
 new=put('new',"SELECT * FROM (VALUES ('work','wrong','published_procurement','Do not replace this work'),('new-item','new-version','stock_supported_item_demand','Widget')) t(episode_id,record_hash,lane,title) CROSS JOIN (SELECT 'Defense Logistics Agency' customer_name,'source' source_evidence_id,'2026-10-31' window_start,'2027-10-30' window_end,'dla_reported_annual_demand' source_name)")
 records=preserve_nonitem(c,old,decisions,new,'2026-10-31')
 assert c.execute("SELECT record_hash,title FROM updated_opportunities WHERE episode_id='work'").fetchone()==('original','Avionics research')
 assert c.execute("SELECT count(*) FROM updated_opportunities WHERE episode_id='forecast'").fetchone()[0]==0
 assert len(records)==1 and records[0]['stage']=='emerging_demand'
 d=json.loads(records[0]['decision_json']);assert d['company_addressable_value'] is None and not d['response_window_verified']
