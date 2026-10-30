"""Evidence-change history for company watches; no user-managed CRM stages."""
import hashlib,json
from pathlib import Path
import duckdb
from .aws import BUCKET

def track(directory,previous,aws):
    r=Path(directory);m=json.loads((r/'manifest.json').read_text());prior=r/'previous';prior.mkdir(exist_ok=True);c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='1200MB';SET preserve_insertion_order=false")
    c.execute("SET temp_directory='"+str(r/'events_tmp').replace("'","''")+"'")
    for name in ('opportunities','demand_decisions','company_matches','published_calls'):
        c.read_parquet(m['tables'][name]['path']).create_view('current_'+name)
    for name in ('opportunities','demand_decisions','company_matches','company_opportunity_events'):
        if name not in previous.get('tables',{}):continue
        t=previous['tables'][name];uri=t['s3_uri']+'part-00000.parquet';p=prior/(name+'.parquet');aws.s3.download_file(BUCKET,uri.split('/',3)[3],str(p))
        if hashlib.sha256(p.read_bytes()).hexdigest()!=t['sha256']:raise ValueError('Prior event/source release hash changed')
        c.read_parquet(str(p)).create_view('prior_'+name)
    c.execute("""CREATE TEMP VIEW old_state AS SELECT o.*,d.stage FROM prior_opportunities o JOIN prior_demand_decisions d USING(episode_id)""")
    c.execute("""CREATE TEMP VIEW new_state AS SELECT o.episode_id,o.record_hash,o.title,o.source_url,o.source_evidence_id,o.value_median,o.window_end,d.stage FROM current_opportunities o JOIN current_demand_decisions d USING(episode_id)
    UNION ALL SELECT p.episode_id,p.record_hash,p.title,p.source_url,p.source_evidence_id,p.value_median,p.window_end,'resolved_or_elapsed' FROM current_published_calls p
    WHERE p.procurement_state IN ('awarded_or_ordered','cancelled') AND NOT EXISTS(SELECT 1 FROM current_opportunities o WHERE o.episode_id=p.episode_id)""")
    c.execute("""CREATE TEMP VIEW old_edges AS SELECT x.company_id,x.episode_id,min(x.match_hash) match_hash FROM prior_company_matches x JOIN old_state o ON x.episode_id=o.episode_id AND x.opportunity_hash=o.record_hash GROUP BY x.company_id,x.episode_id""")
    c.execute("""CREATE TEMP VIEW new_edges AS SELECT x.company_id,x.episode_id,min(x.match_hash) match_hash FROM current_company_matches x JOIN current_opportunities o ON x.episode_id=o.episode_id AND x.opportunity_hash=o.record_hash GROUP BY x.company_id,x.episode_id""")
    c.execute("""CREATE TEMP TABLE changed AS WITH edges AS(SELECT company_id,episode_id FROM old_edges UNION SELECT company_id,episode_id FROM new_edges)
    SELECT e.company_id,e.episode_id,coalesce(n.title,o.title) title,coalesce(n.source_url,o.source_url) source_url,
    o.record_hash previous_record_hash,n.record_hash current_record_hash,a.match_hash previous_match_hash,b.match_hash current_match_hash,o.stage previous_stage,n.stage current_stage,
    o.source_evidence_id previous_source_evidence_id,n.source_evidence_id current_source_evidence_id,o.value_median previous_value_median,n.value_median current_value_median,o.window_end previous_window_end,n.window_end current_window_end,
    CASE WHEN n.stage='resolved_or_elapsed' THEN 'resolved_by_source_or_elapsed_window' WHEN n.episode_id IS NULL THEN 'not_in_latest_snapshot' WHEN a.company_id IS NULL THEN 'first_observed_company_match' WHEN b.company_id IS NULL THEN 'company_fit_changed' WHEN a.match_hash IS DISTINCT FROM b.match_hash THEN 'company_matching_evidence_updated' ELSE 'evidence_or_methodology_updated' END event_type
    FROM edges e LEFT JOIN old_state o USING(episode_id) LEFT JOIN new_state n USING(episode_id)
    LEFT JOIN old_edges a ON a.company_id=e.company_id AND a.episode_id=e.episode_id LEFT JOIN new_edges b ON b.company_id=e.company_id AND b.episode_id=e.episode_id
    WHERE a.company_id IS NULL OR b.company_id IS NULL OR o.record_hash IS DISTINCT FROM n.record_hash OR o.stage IS DISTINCT FROM n.stage OR a.match_hash IS DISTINCT FROM b.match_hash OR o.value_median IS DISTINCT FROM n.value_median OR o.window_end IS DISTINCT FROM n.window_end OR o.title IS DISTINCT FROM n.title OR o.source_url IS DISTINCT FROM n.source_url""")
    # Missing from a snapshot is explicitly not treated as cancellation/loss.
    sql="""SELECT sha256(concat_ws('|','"""+m['run_id']+"""',company_id,episode_id,event_type)) event_id,'"""+m['as_of']+"""' event_date,'"""+m['run_id']+"""' release_id,'"""+previous['run_id']+"""' previous_release_id,*,
    CASE event_type WHEN 'resolved_by_source_or_elapsed_window' THEN 'The latest source or dated window changes this demand to resolved/elapsed; inspect the current evidence for the specific outcome.' WHEN 'not_in_latest_snapshot' THEN 'This record was not reissued in the latest snapshot. That does not establish cancellation or a lost award.' WHEN 'first_observed_company_match' THEN 'First matched for this company in this release. Source dates determine whether the underlying demand is new.' WHEN 'company_matching_evidence_updated' THEN 'Company work, profile or matching evidence changed; compare the source-backed company match versions. No new procurement or sales outcome is inferred.' WHEN 'company_fit_changed' THEN 'The current evidence no longer produces the same company fit; no sales outcome is inferred.' ELSE 'Source evidence or methodology changed; compare the linked record versions and source inputs.' END explanation FROM changed"""
    if 'company_opportunity_events' in previous.get('tables',{}):sql='SELECT * FROM prior_company_opportunity_events UNION ALL BY NAME '+sql
    path=r/'company_opportunity_events.parquet';c.execute("COPY ("+sql+") TO '"+str(path).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)")
    c.read_parquet(str(path)).create_view('result');cols=[dict(Name=n,Type='double' if t in ('DOUBLE','FLOAT') else 'bigint' if t in ('BIGINT','INTEGER') else 'string') for n,t,*_ in c.execute('DESCRIBE result').fetchall()]
    count=c.execute('SELECT count(*) FROM result').fetchone()[0];m['tables']['company_opportunity_events']=dict(path=str(path),rows=count,sha256=hashlib.sha256(path.read_bytes()).hexdigest(),columns=cols);m['previous_release']=previous['run_id'];(r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close();print('Company opportunity evidence events',count,flush=True);return m
