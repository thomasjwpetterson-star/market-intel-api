"""Quarantine missing/conflicting current requests without declaring cancellation."""
import hashlib,json
from pathlib import Path
import duckdb
POLICY='complete-held-dibbs-presence-v1'

def hold(directory):
 r=Path(directory);m=json.loads((r/'manifest.json').read_text());source=m['sources'].get('dla_solicitations')
 if not source or not all(f.get('path') and Path(f['path']).exists() for f in source['files']):return
 c=duckdb.connect();c.execute("SET threads=2;SET memory_limit='2GB'")
 for n in ('opportunities','demand_decisions'):c.read_parquet(m['tables'][n]['path']).create_view(n)
 c.read_parquet([f['path'] for f in source['files']]).create_view('current_source')
 c.execute("""CREATE TEMP TABLE present AS SELECT DISTINCT concat_ws('|',solicitation_number,line_item_num,nsn) source_locator FROM current_source WHERE quantity>0 AND nullif(unit_of_issue,'') IS NOT NULL""")
 digest=hashlib.sha256(json.dumps(sorted(f['sha256'] for f in source['files'])).encode()).hexdigest()
 def emit(n,sql):
  c.execute('CREATE OR REPLACE TEMP VIEW result AS '+sql);p=r/(n+'.parquet');tmp=r/(n+'.presence.parquet');cols=[dict(Name=k,Type={'VARCHAR':'string','BIGINT':'bigint','BOOLEAN':'boolean','DOUBLE':'double'}[t]) for k,t,*_ in c.execute('DESCRIBE result').fetchall()]
  c.execute("COPY result TO '"+str(tmp).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)");rows=c.execute('SELECT count(*) FROM read_parquet(?)',[str(tmp)]).fetchone()[0];tmp.replace(p);m['tables'][n]=dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),columns=cols,rows=rows);print('Request source presence',n,rows,flush=True)
 emit('item_request_presence',"SELECT o.episode_id,o.record_hash,o.source_locator,p.source_locator IS NOT NULL present_in_current_unambiguous_extract,'"+digest+"' source_snapshot_sha256,'"+m['as_of']+"' assessed_as_of,'"+POLICY+"' policy_version FROM opportunities o LEFT JOIN present p USING(source_locator) WHERE o.lane='published_item_procurement' AND o.window_end>='"+m['as_of']+"'")
 c.read_parquet(m['tables']['item_request_presence']['path']).create_view('presence')
 def held(raw):
  d=json.loads(raw);d.update(stage='evidence_gap',participation_route='source_revalidation_required',next_action='Recheck the current DIBBS line and amendments. The retained line is absent from the latest unambiguous extract; this does not establish cancellation.',current_source_presence=dict(status='absent_or_conflicting',source_snapshot_sha256=digest,policy=POLICY))
  d.setdefault('unresolved',[]).append('The current held source does not retain this line unambiguously. It is withheld pending source revalidation, not declared cancelled.')
  d.pop('decision_hash',None);d['decision_hash']=hashlib.sha256(json.dumps(d,sort_keys=True,default=str).encode()).hexdigest()
  return json.dumps(d,sort_keys=True)
 c.create_function('hold_decision',held,['VARCHAR'],'VARCHAR')
 emit('demand_decisions',"SELECT d.* REPLACE(CASE WHEN p.present_in_current_unambiguous_extract=false AND d.stage='published_requirement' THEN 'evidence_gap' ELSE d.stage END AS stage,CASE WHEN p.present_in_current_unambiguous_extract=false AND d.stage='published_requirement' THEN hold_decision(d.decision_json) ELSE d.decision_json END AS decision_json,CASE WHEN p.present_in_current_unambiguous_extract=false AND d.stage='published_requirement' THEN json_extract_string(hold_decision(d.decision_json),'$.decision_hash') ELSE d.decision_hash END AS decision_hash) FROM demand_decisions d LEFT JOIN presence p USING(episode_id)")
 m['item_request_presence_policy']=dict(version=POLICY,source_snapshot_sha256=digest,interpretation='Absence from the current unambiguous held extract removes a published request from actionable serving. Original source content and identity remain retained. Absence is not cancellation.')
 (r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close()
