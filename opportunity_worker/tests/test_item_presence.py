import json,hashlib
import duckdb
from opportunity_engine.item_presence import hold

def test_missing_current_line_is_held_not_cancelled_or_erased(tmp_path):
 c=duckdb.connect();tables={}
 for n,q in {'opportunities':"SELECT 'op' episode_id,'original' record_hash,'published_item_procurement' lane,'missing-line' source_locator,'2026-11-20' window_end",'demand_decisions':"SELECT 'op' episode_id,'original' source_record_hash,'published_requirement' stage,'old-decision' decision_hash,'{\"stage\":\"published_requirement\"}' decision_json"}.items():
  p=tmp_path/(n+'.parquet');c.execute('COPY ('+q+') TO ? (FORMAT PARQUET)',[str(p)]);tables[n]={'path':str(p)}
 source=tmp_path/'source.parquet';c.execute("COPY (SELECT 'other' solicitation_number,'0001' line_item_num,'1234012345678' nsn,5::BIGINT quantity,'EA' unit_of_issue) TO ? (FORMAT PARQUET)",[str(source)])
 (tmp_path/'manifest.json').write_text(json.dumps(dict(as_of='2026-10-31',sources={'dla_solicitations':{'files':[{'path':str(source),'sha256':hashlib.sha256(source.read_bytes()).hexdigest()}]}},tables=tables)))
 hold(tmp_path);m=json.loads((tmp_path/'manifest.json').read_text());row=c.execute('SELECT episode_id,source_record_hash,stage,decision_json,decision_hash FROM read_parquet(?)',[m['tables']['demand_decisions']['path']]).fetchone()
 assert row[1]=='original' and row[2]=='evidence_gap'
 assert json.loads(row[3])['current_source_presence']['status']=='absent_or_conflicting'
 assert json.loads(row[3])['stage']=='evidence_gap'
 assert json.loads(row[3])['decision_hash']==row[4] and row[4]!='old-decision'
 assert c.execute('SELECT count(*) FROM read_parquet(?)',[tables['opportunities']['path']]).fetchone()[0]==1
