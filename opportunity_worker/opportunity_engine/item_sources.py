"""Company-specific catalogue source evidence on every exact-item demand edge."""
import hashlib,json
from pathlib import Path
import duckdb

def augment(directory):
 r=Path(directory);m=json.loads((r/'manifest.json').read_text());source=m['sources'].get('item_reference_facts')
 if not source or 'company_item_matches' not in m['tables']:return
 c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='3GB';SET preserve_insertion_order=false");c.execute("SET temp_directory='"+str(r/'item_sources_tmp').replace("'","''")+"'")
 c.read_parquet([f['path'] for f in source['files']]).create_view('refs')
 for n in ('company_item_matches','company_matches'):c.read_parquet(m['tables'][n]['path']).create_view(n)
 c.execute("CREATE TEMP TABLE keys AS SELECT DISTINCT i.company_id,i.episode_id,i.opportunity_hash,i.niin FROM company_item_matches i JOIN company_matches m ON m.company_id=i.company_id AND m.episode_id=i.episode_id AND m.opportunity_hash=i.opportunity_hash")
 def emit(n,sql):
  c.execute('CREATE OR REPLACE TEMP VIEW result AS '+sql);p=r/(n+'.parquet');tmp=r/(n+'.sources.parquet');cols=[dict(Name=k,Type={'VARCHAR':'string','DOUBLE':'double','BIGINT':'bigint','BOOLEAN':'boolean','JSON':'string'}[t]) for k,t,*_ in c.execute('DESCRIBE result').fetchall()]
  c.execute("COPY result TO '"+str(tmp).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 2048)");count=c.execute('SELECT count(*) FROM read_parquet(?)',[str(tmp)]).fetchone()[0];tmp.replace(p);m['tables'][n]=dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),rows=count,columns=cols);c.read_parquet(str(p)).create_view('new_'+n);print('Company item sources',n,count,flush=True)
 emit('company_item_reference_evidence',"""SELECT DISTINCT k.company_id,k.episode_id,k.opportunity_hash,r.niin,r.nsn,r.cage_code,r.part_number,r.rncc_codes,r.rnvc_codes,r.rnsc_codes,r.evidence_id,r.source_snapshot_sha256,r.source_row,r.interpretation,
 sha256(concat_ws('|',r.cage_code,r.niin,r.part_number,r.rncc_codes,r.rnvc_codes,r.rnsc_codes)) factual_reference_digest
 FROM keys k JOIN refs r ON k.niin=r.niin AND r.cage_code=substr(k.company_id,6)""")
 emit('company_item_reference_summaries',"""SELECT company_id,episode_id,opportunity_hash,count(*)::BIGINT company_reference_count,
 sha256(string_agg(DISTINCT factual_reference_digest,'|' ORDER BY factual_reference_digest)) company_reference_digest,
 to_json(list(struct_pack(cage:=cage_code,part_number:=part_number,rncc:=rncc_codes,rnvc:=rnvc_codes,rnsc:=rnsc_codes,evidence_id:=evidence_id,source_snapshot_sha256:=source_snapshot_sha256,source_row:=source_row,interpretation:=interpretation) ORDER BY part_number,evidence_id)[:12])::VARCHAR company_reference_preview_json
 FROM new_company_item_reference_evidence GROUP BY company_id,episode_id,opportunity_hash""")
 if 'company_item_valuations' in m['tables']:
  c.read_parquet(m['tables']['company_item_valuations']['path']).create_view('old_values');cols={n for n,*_ in c.execute('DESCRIBE old_values').fetchall()};removed=cols&{'company_reference_digest'};base='v.*'+(' EXCLUDE('+','.join(removed)+')' if removed else '')
  emit('company_item_valuations','SELECT '+base+',s.company_reference_digest FROM old_values v LEFT JOIN new_company_item_reference_summaries s ON s.company_id=v.company_id AND s.episode_id=v.episode_id AND s.opportunity_hash=v.record_hash')
 m['company_item_reference_policy']='exact-cage-niin-source-evidence-v1';(r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close()
