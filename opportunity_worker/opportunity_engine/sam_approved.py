"""Direct source-stated DLA approval tuples, independent of text similarity."""
import hashlib,json,re
from pathlib import Path
import duckdb
from .quality_core import plain,notice_quality
from .company_enrichment import nsns
POLICY='agency-approved-cage-part-niin-v1'

def approvals(row,as_of):
 if row.get('source_name')!='sam_notices':return []
 text=plain(row.get('scope_text'));title=str(row.get('title') or '')
 if re.search(r'\brepair\w*|\boverhaul\w*|\bmodification\b',title,re.I) or str(row.get('psc') or '').startswith('J'):return []
 header=re.search(r'\bProposed procurement for NSN\s+(\d{13})\b',text,re.I)
 if not header or nsns(text+' '+title)!=[header.group(1)]:return []
 if notice_quality(title,text,row.get('window_end'),as_of,'sam_notices')['verdict']!='usable':return []
 out=[]
 for block in re.finditer(r'\bApproved sources?\s+(?:is|are)\s+([^.;]+(?:;\s*[A-Z0-9]{5}\s+[A-Z0-9][A-Z0-9./_-]*)*)',text,re.I):
  for cage,part in re.findall(r'(?:^|;\s*)([A-Z0-9]{5})\s+([A-Z0-9][A-Z0-9./_-]{1,80})',block.group(1),re.I):
   if cage.upper() in ('00000','99999','XXXXX','ZZZZZ'):continue
   out.append(dict(company_id='CAGE:'+cage.upper(),niin=header.group(1)[4:],part_number=part.rstrip('.,;'),part_key=re.sub('[^A-Z0-9]','',part.upper()),approval_quote=block.group(0)))
 return out

def augment(directory):
 r=Path(directory);m=json.loads((r/'manifest.json').read_text())
 if not all(n in m['tables'] for n in ('company_item_matches','company_work_matches','company_matches')):return 0
 c=duckdb.connect();c.execute("SET threads=2;SET memory_limit='4GB';SET preserve_insertion_order=false");c.execute("SET temp_directory='"+str(r/'approved_sources_tmp').replace("'","''")+"'")
 for n in ('opportunities','demand_decisions','company_profiles','company_item_matches','company_work_matches','company_matches'):c.read_parquet(m['tables'][n]['path']).create_view('base_'+n)
 f=r/'approved_notice_rows.jsonl';rows=c.execute("SELECT o.* FROM base_opportunities o JOIN base_demand_decisions d USING(episode_id) WHERE o.source_name='sam_notices' AND d.stage IN ('published_requirement','qualification_opening')").fetchall();names=[x[0] for x in c.description];count=0
 with f.open('w') as stream:
  for values in rows:
   row=dict(zip(names,values))
   for source in approvals(row,m['as_of']):
    source.update(episode_id=row['episode_id'],record_hash=row['record_hash'],source_evidence_id=row['source_evidence_id'],source_url=row['source_url']);stream.write(json.dumps(source)+'\n');count+=1
 if not count:c.close();return 0
 c.read_json(str(f),format='newline_delimited',columns={n:'VARCHAR' for n in ('company_id','niin','part_number','part_key','approval_quote','episode_id','record_hash','source_evidence_id','source_url')}).create_view('stated')
 source=m['sources']['item_reference_facts'];c.read_parquet([x['path'] for x in source['files']]).create_view('refs')
 c.execute("""CREATE TEMP TABLE exact AS SELECT s.*,p.profile_hash,i.evidence_id,i.description reference_description,i.part_number reference_part_number,i.rncc_codes,i.rnvc_codes,i.rnsc_codes,i.source_snapshot_sha256,i.source_row
 FROM stated s JOIN refs i ON i.cage_code=substr(s.company_id,6) AND i.niin=s.niin AND regexp_replace(upper(i.part_number),'[^A-Z0-9]','','g')=s.part_key JOIN base_company_profiles p USING(company_id)
 QUALIFY row_number() OVER(PARTITION BY s.company_id,s.episode_id ORDER BY i.evidence_id,s.part_number)=1""")
 # Reuse an already matched current DIBBS requirement with the exact RFQ and
 # NIIN. Keep the SAM approval as separate source evidence, rather than adding
 # the same procurement as another company opportunity.
 opcols={x[0] for x in c.execute('DESCRIBE base_opportunities').fetchall()}
 if {'native_identifier','lane','value_inputs_json'}.issubset(opcols):
  c.execute("""CREATE TEMP TABLE targeted AS SELECT x.* EXCLUDE(episode_id,record_hash),x.episode_id agency_notice_episode_id,x.record_hash agency_notice_record_hash,
  coalesce(b.episode_id,x.episode_id) episode_id,coalesce(b.record_hash,x.record_hash) record_hash
  FROM exact x JOIN base_opportunities n ON n.episode_id=x.episode_id
  LEFT JOIN (SELECT o.*,cm.company_id FROM base_opportunities o JOIN base_company_matches cm ON cm.episode_id=o.episode_id AND cm.opportunity_hash=o.record_hash JOIN base_demand_decisions d ON d.episode_id=o.episode_id WHERE o.lane='published_item_procurement' AND d.stage='published_requirement') b
  ON b.company_id=x.company_id AND regexp_replace(upper(b.native_identifier),'-00$','')=regexp_replace(upper(n.native_identifier),'-00$','') AND json_extract_string(b.value_inputs_json,'$.niin')=x.niin AND regexp_full_match(regexp_replace(upper(n.native_identifier),'-00$',''),'SP[A-Z0-9]{4}[0-9]{2}[A-Z][0-9]{4}')
  QUALIFY row_number() OVER(PARTITION BY x.company_id,x.episode_id ORDER BY b.window_end DESC NULLS LAST,b.episode_id)=1""")
 else:c.execute('CREATE TEMP TABLE targeted AS SELECT *,episode_id agency_notice_episode_id,record_hash agency_notice_record_hash FROM exact')
 c.execute('DROP TABLE exact');c.execute('ALTER TABLE targeted RENAME TO exact')
 count=c.execute('SELECT count(*) FROM exact').fetchone()[0]
 if not count:c.close();return 0
 posted='n.notice_posted_date' if 'notice_posted_date' in opcols else 'NULL::VARCHAR'
 c.execute('CREATE TEMP TABLE direct_sources AS SELECT x.* FROM exact x JOIN base_opportunities n ON n.episode_id=x.agency_notice_episode_id QUALIFY row_number() OVER(PARTITION BY x.company_id,x.episode_id ORDER BY '+posted+' DESC NULLS LAST,x.agency_notice_episode_id)=1')
 source_records=count;count=c.execute('SELECT count(*) FROM direct_sources').fetchone()[0]
 def emit(n,query):
  c.execute('CREATE OR REPLACE TEMP VIEW result AS '+query);tmp=r/(n+'.approved.parquet');p=r/(n+'.parquet');cols=[dict(Name=k,Type='double' if t=='DOUBLE' else 'bigint' if t in ('BIGINT','INTEGER') else 'string') for k,t,*_ in c.execute('DESCRIBE result').fetchall()];c.execute('COPY result TO ? (FORMAT PARQUET,COMPRESSION ZSTD)',[str(tmp)]);rows=c.execute('SELECT count(*) FROM read_parquet(?)',[str(tmp)]).fetchone()[0];tmp.replace(p);m['tables'][n]=dict(path=str(p),rows=rows,sha256=hashlib.sha256(p.read_bytes()).hexdigest(),columns=cols);c.read_parquet(str(p)).create_view('new_'+n);print('Agency-approved item links',n,rows,flush=True)
 c.execute("""CREATE TEMP TABLE direct_matches AS SELECT sha256('agency-approved-v1|'||company_id||'|'||episode_id) match_id,company_id,episode_id,95.0::DOUBLE score,'agency_named_approved_source-v1' match_route,
 'The current agency notice names this CAGE, NSN and part as an approved source' matched_terms,
 'Source-stated approval for this notice item and part, corroborated by the exact company catalogue tuple. Current configuration, delivery, quotation conditions and manufacturing rights remain separate checks.' company_evidence_caveat,
 evidence_id sample_evidence_id,sha256(concat_ws('|',company_id,episode_id,record_hash,evidence_id,part_number,approval_quote)) match_hash,record_hash opportunity_hash,profile_hash,niin,reference_description,reference_part_number,
 approval_quote agency_approval_quote,source_url agency_approval_source_url,part_number agency_approval_part_number,(SELECT to_json(list(DISTINCT struct_pack(company_id:=s.company_id,part_number:=s.part_number))) FROM stated s WHERE s.episode_id=x.agency_notice_episode_id AND s.company_id<>x.company_id) agency_other_sources_json FROM direct_sources x""")
 # Snapshot old edges before replacement so reading/writing never mixes files.
 c.execute('CREATE TEMP TABLE replaced_keys AS SELECT company_id,episode_id FROM exact UNION SELECT company_id,agency_notice_episode_id episode_id FROM exact')
 for n in ('company_matches','company_item_matches','company_work_matches'):c.execute('CREATE TEMP TABLE saved_'+n+' AS SELECT * FROM base_'+n)
 emit('company_matches','SELECT b.* FROM saved_company_matches b ANTI JOIN replaced_keys d USING(company_id,episode_id) UNION ALL BY NAME SELECT match_id,company_id,episode_id,score,match_route,matched_terms,company_evidence_caveat,sample_evidence_id,match_hash,opportunity_hash,profile_hash FROM direct_matches')
 emit('company_item_matches','SELECT b.* FROM saved_company_item_matches b ANTI JOIN replaced_keys d USING(company_id,episode_id) UNION ALL BY NAME SELECT * FROM direct_matches')
 emit('company_work_matches','SELECT b.* FROM saved_company_work_matches b ANTI JOIN replaced_keys d USING(company_id,episode_id)')
 emit('company_approved_notice_sources',"SELECT *, '"+POLICY+"' policy_version FROM exact")
 m['agency_approved_notice_policy']=dict(version=POLICY,matched_company_items=count,source_approval_records=source_records,qualification='The agency explicitly names the site for this exact item/part; no approval is inferred from similarity or an unrelated catalogue item.')
 (r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close();return count
