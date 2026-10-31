"""Link future item demand to later published requests without inventing conversion."""
import hashlib,json
from pathlib import Path
import duckdb

def link(directory):
 r=Path(directory);m=json.loads((r/'manifest.json').read_text())
 if 'company_item_valuations' not in m['tables']:return
 c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='2GB';SET preserve_insertion_order=false")
 c.execute("SET temp_directory='"+str(r/'request_links_tmp').replace("'","''")+"'")
 for n in ('opportunities','demand_decisions','company_item_valuations'):c.read_parquet(m['tables'][n]['path']).create_view(n)
 def emit(n,sql):
  p=r/(n+'.parquet');tmp=r/(n+'.links.parquet');c.execute('CREATE OR REPLACE TEMP VIEW result AS '+sql)
  types={'VARCHAR':'string','DOUBLE':'double','BIGINT':'bigint','INTEGER':'bigint','BOOLEAN':'boolean'};cols=[dict(Name=name,Type=types[t]) for name,t,*_ in c.execute('DESCRIBE result').fetchall()]
  # Each company edge carries the full monthly evidence. Bound the writer's
  # row group instead of buffering 122,880 copies of these wide JSON fields.
  c.execute("COPY result TO '"+str(tmp).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 2048)");count=c.execute('SELECT count(*) FROM read_parquet(?)',[str(tmp)]).fetchone()[0];tmp.replace(p)
  m['tables'][n]=dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),rows=count,columns=cols);print('Item request links',n,count,flush=True)
 c.execute("""CREATE TEMP TABLE current_requests AS SELECT o.*,json_extract_string(o.value_inputs_json,'$.niin') niin,try_cast(json_extract_string(o.value_inputs_json,'$.quantity') AS BIGINT) requested_quantity,json_extract_string(o.value_inputs_json,'$.request_unit') requested_unit,json_extract_string(o.value_inputs_json,'$.purchase_request_number') purchase_request_number
 FROM opportunities o JOIN demand_decisions d USING(episode_id) WHERE o.lane='published_item_procurement' AND d.stage='published_requirement' AND substr(o.window_end,1,10)>='"""+m['as_of']+"'")
 emit('item_request_progression',"""SELECT sha256(f.episode_id||'|'||q.episode_id) relationship_id,f.episode_id forecast_episode_id,f.record_hash forecast_record_hash,q.episode_id request_episode_id,q.record_hash request_record_hash,q.niin,q.title request_title,q.native_identifier,q.purchase_request_number,q.requested_quantity,q.requested_unit,q.window_end response_deadline,q.source_url,q.source_evidence_id,q.source_locator,f.window_start forecast_start,f.window_end forecast_end,
 CASE WHEN q.window_end<f.window_start THEN 'Request precedes forecast window; future delivery may overlap' ELSE 'Published request within the forecast horizon' END timing_relationship,
 'Exact NIIN association. This is not proof the request fulfils this forecast, an award, or permission to subtract its quantity. The request is a separate actionable source with its own eligibility.' interpretation
 FROM opportunities f JOIN current_requests q ON q.niin=json_extract_string(f.value_inputs_json,'$.niin')
 WHERE f.lane='source_item_forecasts' AND regexp_full_match(q.niin,'[0-9]{9}') AND q.window_end<=f.window_end""")
 c.read_parquet(m['tables']['item_request_progression']['path']).create_view('progression')
 c.execute("""CREATE TEMP TABLE request_groups AS SELECT forecast_episode_id,forecast_record_hash,count(*) current_request_count,min(response_deadline) next_request_deadline,
 to_json(list(struct_pack(request_episode_id:=request_episode_id,request_record_hash:=request_record_hash,title:=request_title,native_identifier:=native_identifier,purchase_request_number:=purchase_request_number,quantity:=requested_quantity,unit:=requested_unit,response_deadline:=response_deadline,source_url:=source_url,source_evidence_id:=source_evidence_id,timing_relationship:=timing_relationship) ORDER BY response_deadline,request_episode_id)[:20])::VARCHAR related_requests_json
 FROM progression GROUP BY forecast_episode_id,forecast_record_hash""")
 cols={n for n,*_ in c.execute('DESCRIBE company_item_valuations').fetchall()};remove=cols&{'current_request_count','next_request_deadline','related_requests_json'}
 base='v.*'+(' EXCLUDE('+','.join(sorted(remove))+')' if remove else '')
 emit('company_item_valuations','SELECT '+base+',coalesce(g.current_request_count,0)::BIGINT current_request_count,g.next_request_deadline,g.related_requests_json FROM company_item_valuations v LEFT JOIN request_groups g ON g.forecast_episode_id=v.episode_id AND g.forecast_record_hash=v.record_hash')
 m['item_request_relationship_policy']='exact-niin-published-request-v1';(r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close()
