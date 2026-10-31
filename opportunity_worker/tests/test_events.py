import hashlib,json,shutil
from pathlib import Path
import duckdb
from opportunity_engine.events import track

def test_watch_history_distinguishes_source_resolution_from_missing_snapshot(tmp_path):
    c=duckdb.connect();old={};current={};uris={}
    def put(name,sql,prior=False):
        p=tmp_path/(('old_' if prior else '')+name+'.parquet');c.execute("COPY ("+sql+") TO '"+str(p)+"' (FORMAT PARQUET)");uri='s3://lake/'+name+'/';t=dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),s3_uri=uri)
        (old if prior else current)[name]=t
        if prior:uris[name+'/part-00000.parquet']=p
    put('opportunities',"SELECT id episode_id,'old-'||id record_hash,'Work '||id title,'https://example.gov/'||id source_url,'source-'||id source_evidence_id,100.0::DOUBLE value_median,'2029-01-01' window_end FROM (VALUES ('resolved'),('missing'),('same'),('inactive')) t(id)",True)
    put('demand_decisions',"SELECT id episode_id,'emerging_demand' stage FROM (VALUES ('resolved'),('missing'),('same'),('inactive')) t(id)",True)
    put('company_matches',"SELECT 'CAGE:12345' company_id,id episode_id,'match-'||id match_hash,'old-'||id opportunity_hash FROM (VALUES ('resolved'),('missing'),('same'),('inactive')) t(id)",True)
    put('opportunities',"SELECT id episode_id,CASE WHEN id='same' THEN 'old-same' ELSE 'new-'||id END record_hash,'Work '||id title,'https://example.gov/'||id source_url,'source-'||id source_evidence_id,100.0::DOUBLE value_median,'2029-01-01' window_end FROM (VALUES ('resolved'),('new'),('same')) t(id)")
    put('demand_decisions',"SELECT id episode_id,CASE WHEN id='resolved' THEN 'resolved_or_elapsed' ELSE 'emerging_demand' END stage FROM (VALUES ('resolved'),('new'),('same')) t(id)")
    put('company_matches',"SELECT 'CAGE:12345' company_id,id episode_id,CASE WHEN id='same' THEN 'changed-match' ELSE 'match-'||id END match_hash,CASE WHEN id='same' THEN 'old-same' ELSE 'new-'||id END opportunity_hash FROM (VALUES ('new'),('same')) t(id)")
    put('published_calls',"SELECT 'inactive' episode_id,'new-inactive' record_hash,'Work inactive' title,'https://example.gov/inactive' source_url,'inactive-notice' source_evidence_id,NULL::DOUBLE value_median,NULL::VARCHAR window_end,'source_inactive' procurement_state")
    (tmp_path/'manifest.json').write_text(json.dumps(dict(run_id='new_release',as_of='2026-10-30',tables=current)))
    class S3:
        def download_file(self,bucket,key,target):shutil.copyfile(uris[key],target)
    class AWS:s3=S3()
    result=track(tmp_path,dict(run_id='old_release',tables=old),AWS())
    rows=c.execute('SELECT episode_id,event_type FROM read_parquet(?) ORDER BY episode_id',[result['tables']['company_opportunity_events']['path']]).fetchall()
    assert rows==[('inactive','source_no_longer_active'),('missing','not_in_latest_snapshot'),('new','first_observed_company_match'),('resolved','resolved_by_source_or_elapsed_window'),('same','company_matching_evidence_updated')]
    inactive=c.execute("SELECT current_source_evidence_id,explanation FROM read_parquet(?) WHERE episode_id='inactive'",[result['tables']['company_opportunity_events']['path']]).fetchone()
    assert inactive[0]=='inactive-notice' and 'does not independently establish cancellation' in inactive[1]

def test_watch_records_price_changes_even_when_forecast_and_match_ids_are_unchanged(tmp_path):
    c=duckdb.connect();old={};current={};uris={}
    def put(name,sql,prior=False):
        p=tmp_path/(('old_' if prior else '')+name+'.parquet');c.execute("COPY ("+sql+") TO '"+str(p)+"' (FORMAT PARQUET)")
        t=dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),s3_uri='s3://lake/'+name+'/');(old if prior else current)[name]=t
        if prior:uris[name+'/part-00000.parquet']=p
    for prior in (True,False):
        put('opportunities',"SELECT 'forecast' episode_id,'unchanged' record_hash,'Matrix' title,NULL::VARCHAR source_url,'forecast-source' source_evidence_id,NULL::DOUBLE value_median,'2029-01-01' window_end",prior)
        put('demand_decisions',"SELECT 'forecast' episode_id,'source_forecast' stage",prior)
        put('company_matches',"SELECT 'CAGE:45934' company_id,'forecast' episode_id,'same-match' match_hash,'unchanged' opportunity_hash",prior)
        price=100 if prior else 120
        put('company_item_valuations',"SELECT 'CAGE:45934' company_id,'forecast' episode_id,'unchanged' record_hash,10::BIGINT quantity,'EA' catalogue_unit,"+str(price)+"::DOUBLE government_reference_unit_price,"+str(price*10)+"::DOUBLE government_reference_lot_scenario",prior)
    put('published_calls',"SELECT NULL::VARCHAR episode_id,NULL::VARCHAR record_hash,NULL::VARCHAR title,NULL::VARCHAR source_url,NULL::VARCHAR source_evidence_id,NULL::DOUBLE value_median,NULL::VARCHAR window_end,NULL::VARCHAR procurement_state WHERE false")
    (tmp_path/'manifest.json').write_text(json.dumps(dict(run_id='new',as_of='2026-10-31',tables=current)))
    class S3:
        def download_file(self,bucket,key,target):shutil.copyfile(uris[key],target)
    class AWS:s3=S3()
    result=track(tmp_path,dict(run_id='old',tables=old),AWS())
    rows=c.execute('SELECT event_type,previous_value_context_json,current_value_context_json FROM read_parquet(?)',[result['tables']['company_opportunity_events']['path']]).fetchall()
    assert len(rows)==1 and rows[0][0]=='demand_value_or_supply_context_updated'
    assert json.loads(rows[0][1])['government_reference_unit_price']==100
    assert json.loads(rows[0][2])['government_reference_unit_price']==120
