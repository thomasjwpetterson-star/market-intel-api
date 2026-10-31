import json
import duckdb
from opportunity_engine.item_sources import augment

def test_source_reference_evidence_belongs_to_selected_cage_and_keeps_codes(tmp_path):
 c=duckdb.connect();tables={}
 for n,q in {'company_item_matches':"SELECT 'CAGE:45934' company_id,'op' episode_id,'hash' opportunity_hash,'012345678' niin",'company_matches':"SELECT 'CAGE:45934' company_id,'op' episode_id,'hash' opportunity_hash"}.items():
  p=tmp_path/(n+'.parquet');c.execute('COPY ('+q+') TO ? (FORMAT PARQUET)',[str(p)]);tables[n]={'path':str(p)}
 p=tmp_path/'refs.parquet';c.execute("COPY (SELECT * FROM (VALUES ('45934','012345678','1234012345678','PART-A','3','2','A','ref1','snapshot',1::BIGINT,'Catalogue reference'),('27541','012345678','1234012345678','OTHER-PART','5','9','B','ref2','snapshot',2,'Other seller')) t(cage_code,niin,nsn,part_number,rncc_codes,rnvc_codes,rnsc_codes,evidence_id,source_snapshot_sha256,source_row,interpretation)) TO ? (FORMAT PARQUET)",[str(p)])
 (tmp_path/'manifest.json').write_text(json.dumps(dict(sources={'item_reference_facts':{'files':[{'path':str(p)}]}},tables=tables)))
 augment(tmp_path);m=json.loads((tmp_path/'manifest.json').read_text());row=c.execute('SELECT cage_code,part_number,rncc_codes,rnvc_codes,rnsc_codes FROM read_parquet(?)',[m['tables']['company_item_reference_evidence']['path']]).fetchall()
 assert row==[('45934','PART-A','3','2','A')]
 summary=c.execute('SELECT company_reference_count,company_reference_preview_json FROM read_parquet(?)',[m['tables']['company_item_reference_summaries']['path']]).fetchone();assert summary[0]==1;assert json.loads(summary[1])[0]['evidence_id']=='ref1'
