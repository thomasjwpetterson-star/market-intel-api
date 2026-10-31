"""Evidence-change history for company watches; no user-managed CRM stages."""
import hashlib,json
from pathlib import Path
import duckdb
from .aws import BUCKET

def track(directory,previous,aws):
    r=Path(directory);m=json.loads((r/'manifest.json').read_text());prior=r/'previous';prior.mkdir(exist_ok=True);c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='3GB';SET preserve_insertion_order=false")
    c.execute("SET temp_directory='"+str(r/'events_tmp').replace("'","''")+"'")
    for name in ('opportunities','demand_decisions','company_matches','published_calls'):
        c.read_parquet(m['tables'][name]['path']).create_view('current_'+name)
    if 'company_item_valuations' in m['tables']:c.read_parquet(m['tables']['company_item_valuations']['path']).create_view('current_company_item_valuations')
    for name in ('opportunities','demand_decisions','company_matches','company_opportunity_events','company_item_valuations'):
        if name not in previous.get('tables',{}):continue
        t=previous['tables'][name];uri=t['s3_uri']+'part-00000.parquet';p=prior/(name+'.parquet');aws.s3.download_file(BUCKET,uri.split('/',3)[3],str(p))
        if hashlib.sha256(p.read_bytes()).hexdigest()!=t['sha256']:raise ValueError('Prior event/source release hash changed')
        c.read_parquet(str(p)).create_view('prior_'+name)
    c.execute("""CREATE TEMP VIEW old_state AS SELECT o.*,d.stage FROM prior_opportunities o JOIN prior_demand_decisions d USING(episode_id)""")
    c.execute("""CREATE TEMP VIEW new_state AS SELECT o.episode_id,o.record_hash,o.title,o.source_url,o.source_evidence_id,o.value_median,o.window_end,d.stage FROM current_opportunities o JOIN current_demand_decisions d USING(episode_id)
    UNION ALL SELECT p.episode_id,p.record_hash,p.title,p.source_url,p.source_evidence_id,p.value_median,p.window_end,CASE WHEN p.procurement_state='source_inactive' THEN 'source_inactive' ELSE 'resolved_or_elapsed' END FROM current_published_calls p
    WHERE p.procurement_state IN ('awarded_or_ordered','cancelled','source_inactive') AND NOT EXISTS(SELECT 1 FROM current_opportunities o WHERE o.episode_id=p.episode_id)""")
    def value_context(prefix,meta):
        if 'company_item_valuations' not in meta.get('tables',{}):
            c.execute('CREATE TEMP VIEW '+prefix+"_value_context AS SELECT NULL::VARCHAR company_id,NULL::VARCHAR episode_id,NULL::VARCHAR record_hash,NULL::VARCHAR value_context_hash,NULL::VARCHAR value_context_json WHERE false");return
        table=prefix+'_company_item_valuations';columns={n for n,*_ in c.execute('DESCRIBE '+table).fetchall()}
        fields=('quantity','catalogue_unit','government_reference_unit_price','government_reference_lot_scenario','market_price_median','market_price_lot_scenario','latest_price_date','annual_scenarios_json','valuation_status','inventory_date','stock_on_hand','backorder_quantity','inventory_signal','reorder_date','reorder_point','source_snapshot_sha256','current_request_count','next_request_deadline','related_requests_json','company_reference_digest')
        context='to_json(struct_pack('+','.join(n+':=coalesce('+n+',0)' if n=='current_request_count' and n in columns else n+':=0' if n=='current_request_count' else n+':='+n if n in columns else n+':=NULL' for n in fields)+'))'
        # Serialize each evidence context once; company partitions reuse it.
        c.execute('CREATE TEMP TABLE '+prefix+'_value_context AS SELECT company_id,episode_id,record_hash,sha256('+context+') value_context_hash,'+context+' value_context_json FROM '+table)
    value_context('prior',previous);value_context('current',m)
    old_edges_sql="""SELECT x.company_id,x.episode_id,min(x.match_hash) match_hash,min(v.value_context_hash) value_context_hash,min(v.value_context_json) value_context_json FROM prior_company_matches x JOIN old_state o ON x.episode_id=o.episode_id AND x.opportunity_hash=o.record_hash LEFT JOIN prior_value_context v ON v.company_id=x.company_id AND v.episode_id=x.episode_id AND v.record_hash=o.record_hash WHERE hash(x.company_id)%32=COMPANY_PARTITION GROUP BY x.company_id,x.episode_id"""
    new_edges_sql="""SELECT x.company_id,x.episode_id,min(x.match_hash) match_hash,min(v.value_context_hash) value_context_hash,min(v.value_context_json) value_context_json FROM current_company_matches x JOIN current_opportunities o ON x.episode_id=o.episode_id AND x.opportunity_hash=o.record_hash LEFT JOIN current_value_context v ON v.company_id=x.company_id AND v.episode_id=x.episode_id AND v.record_hash=o.record_hash WHERE hash(x.company_id)%32=COMPANY_PARTITION GROUP BY x.company_id,x.episode_id"""
    for name,query in (('old_edges',old_edges_sql),('new_edges',new_edges_sql)):
        for partition in range(32):
            c.execute(('CREATE TEMP TABLE '+name+' AS ' if partition==0 else 'INSERT INTO '+name+' ')+query.replace('COMPANY_PARTITION',str(partition)))
    changed_sql="""WITH edges AS(SELECT company_id,episode_id FROM old_edges WHERE hash(company_id)%32=COMPANY_PARTITION UNION SELECT company_id,episode_id FROM new_edges WHERE hash(company_id)%32=COMPANY_PARTITION)
    SELECT e.company_id,e.episode_id,coalesce(n.title,o.title) title,coalesce(n.source_url,o.source_url) source_url,
    o.record_hash previous_record_hash,n.record_hash current_record_hash,a.match_hash previous_match_hash,b.match_hash current_match_hash,o.stage previous_stage,n.stage current_stage,a.value_context_hash previous_value_context_hash,b.value_context_hash current_value_context_hash,a.value_context_json previous_value_context_json,b.value_context_json current_value_context_json,
    o.source_evidence_id previous_source_evidence_id,n.source_evidence_id current_source_evidence_id,o.value_median previous_value_median,n.value_median current_value_median,o.window_end previous_window_end,n.window_end current_window_end,
    CASE WHEN n.stage='source_inactive' THEN 'source_no_longer_active' WHEN n.stage='resolved_or_elapsed' THEN 'resolved_by_source_or_elapsed_window' WHEN n.episode_id IS NULL THEN 'not_in_latest_snapshot' WHEN a.company_id IS NULL THEN 'first_observed_company_match' WHEN b.company_id IS NULL THEN 'company_fit_changed' WHEN a.match_hash IS DISTINCT FROM b.match_hash THEN 'company_matching_evidence_updated' WHEN a.value_context_hash IS DISTINCT FROM b.value_context_hash THEN 'demand_value_or_supply_context_updated' ELSE 'evidence_or_methodology_updated' END event_type
    FROM edges e LEFT JOIN old_state o USING(episode_id) LEFT JOIN new_state n USING(episode_id)
    LEFT JOIN old_edges a ON a.company_id=e.company_id AND a.episode_id=e.episode_id LEFT JOIN new_edges b ON b.company_id=e.company_id AND b.episode_id=e.episode_id
    WHERE a.company_id IS NULL OR b.company_id IS NULL OR o.record_hash IS DISTINCT FROM n.record_hash OR o.stage IS DISTINCT FROM n.stage OR a.match_hash IS DISTINCT FROM b.match_hash OR o.value_median IS DISTINCT FROM n.value_median OR o.window_end IS DISTINCT FROM n.window_end OR o.title IS DISTINCT FROM n.title OR o.source_url IS DISTINCT FROM n.source_url OR a.value_context_hash IS DISTINCT FROM b.value_context_hash"""
    for partition in range(32):
        c.execute(('CREATE TEMP TABLE changed AS ' if partition==0 else 'INSERT INTO changed ')+changed_sql.replace('COMPANY_PARTITION',str(partition)))
        print('Company evidence change partition',partition+1,'/ 32',flush=True)
    # Missing from a snapshot is explicitly not treated as cancellation/loss.
    sql="""SELECT sha256(concat_ws('|','"""+m['run_id']+"""',company_id,episode_id,event_type)) event_id,'"""+m['as_of']+"""' event_date,'"""+m['run_id']+"""' release_id,'"""+previous['run_id']+"""' previous_release_id,*,
    CASE event_type WHEN 'source_no_longer_active' THEN 'The agency now marks this notice inactive. This does not independently establish cancellation, an award or the reason it closed; inspect the retained latest notice.' WHEN 'demand_value_or_supply_context_updated' THEN 'Pricing, forecast valuation, related requests or stock/reorder evidence changed. Inspect the retained before/after context and source releases; this is not an order or company revenue.' WHEN 'resolved_by_source_or_elapsed_window' THEN 'The latest source or dated window changes this demand to resolved/elapsed; inspect the current evidence for the specific outcome.' WHEN 'not_in_latest_snapshot' THEN 'This record was not reissued in the latest snapshot. That does not establish cancellation or a lost award.' WHEN 'first_observed_company_match' THEN 'First matched for this company in this release. Source dates determine whether the underlying demand is new.' WHEN 'company_matching_evidence_updated' THEN 'Company work, profile or matching evidence changed; compare the source-backed company match versions. No new procurement or sales outcome is inferred.' WHEN 'company_fit_changed' THEN 'The current evidence no longer produces the same company fit; no sales outcome is inferred.' ELSE 'Source evidence or methodology changed; compare the linked record versions and source inputs.' END explanation FROM changed"""
    if 'company_opportunity_events' in previous.get('tables',{}):sql='SELECT * FROM prior_company_opportunity_events UNION ALL BY NAME '+sql
    path=r/'company_opportunity_events.parquet';c.execute("COPY ("+sql+") TO '"+str(path).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 2048)")
    c.read_parquet(str(path)).create_view('result');cols=[dict(Name=n,Type='double' if t in ('DOUBLE','FLOAT') else 'bigint' if t in ('BIGINT','INTEGER') else 'string') for n,t,*_ in c.execute('DESCRIBE result').fetchall()]
    count=c.execute('SELECT count(*) FROM result').fetchone()[0];m['tables']['company_opportunity_events']=dict(path=str(path),rows=count,sha256=hashlib.sha256(path.read_bytes()).hexdigest(),columns=cols);m['previous_release']=previous['run_id'];(r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close();print('Company opportunity evidence events',count,flush=True);return m
