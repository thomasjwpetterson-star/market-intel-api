"""Market-wide, source-backed precision gates over already retrieved company edges.

The original requirements and selected history remain inspectable. Rejected fit
is removed from serving links; unresolved access stays in review context.
"""
import datetime,hashlib,json,uuid
from pathlib import Path
import duckdb
from . import quality_core as q
from .aws import BUCKET,PREFIX
from .company_enrichment import nsns
from .delivery_modes import required_mode,mode_fit,PSC_REFERENCE
POLICY_HASH=hashlib.sha256(q.LOGIC_HASH.encode()+Path(__file__).read_bytes()).hexdigest()


def apply(directory):
 r=Path(directory);m=json.loads((r/'manifest.json').read_text());c=duckdb.connect();c.execute("SET threads=4;SET memory_limit='6GB';SET preserve_insertion_order=false")
 c.execute("SET temp_directory='"+str(r/'quality_tmp').replace("'","''")+"'")
 for name in ('opportunities','demand_decisions','company_matches','company_work_matches'):
  c.read_parquet(m['tables'][name]['path']).create_view('prior_'+name)
 if 'opportunity_quality_reviews' in m['tables']:
  c.read_parquet(m['tables']['opportunity_quality_reviews']['path']).create_view('prior_quality_reviews')
  c.execute('CREATE TEMP TABLE decision_baselines AS SELECT episode_id,pre_quality_decision_json decision_json FROM prior_quality_reviews')
 else:c.execute('CREATE TEMP TABLE decision_baselines AS SELECT episode_id,decision_json FROM prior_demand_decisions')
 def output(name,sql):
  c.execute('CREATE OR REPLACE TEMP VIEW result AS '+sql);p=r/(name+'.parquet');tmp=r/(name+'.quality.parquet');c.execute("COPY result TO '"+str(tmp).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)");tmp.replace(p)
  columns=[dict(Name=n,Type='boolean' if t=='BOOLEAN' else 'double' if t in ('FLOAT','DOUBLE') else 'bigint' if t in ('BIGINT','INTEGER') else 'string') for n,t,*_ in c.execute('DESCRIBE result').fetchall()]
  count=c.execute('SELECT count(*) FROM read_parquet(?)',[str(p)]).fetchone()[0];m['tables'][name]=dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),rows=count,columns=columns);c.read_parquet(str(p)).create_view('quality_'+name,replace=True);print('Quality output',name,count,flush=True)
 # Assess each source once; do not reparse long descriptions for each company.
 p=r/'source_quality.jsonl'
 op_columns={n for n,*_ in c.execute('DESCRIBE prior_opportunities').fetchall()}
 psc_expression='psc' if 'psc' in op_columns else 'NULL::VARCHAR'
 rows=c.execute('SELECT episode_id,record_hash,title,scope_text,window_end,source_name,source_evidence_id,source_url,'+psc_expression+' requirement_psc FROM prior_opportunities').fetchall()
 with p.open('w') as f:
  for eid,rh,title,body,end,source,evidence,url,psc in rows:
   v=q.notice_quality(title,body,end,m['as_of'],source)
   mode,mode_basis=required_mode(title,body,psc,source=='sam_notices')
   v.update(required_delivery_mode=mode,delivery_mode_basis=mode_basis,requirement_psc=psc,psc_reference_url=PSC_REFERENCE)
   v.update(episode_id=eid,record_hash=rh,source_evidence_id=evidence,source_url=url,nomenclature=q.nomenclature(body),restricted_item=q.restricted_item(body),qualification_invitation=q.explicit_qualification(body),source_cages_json=json.dumps(sorted(q.listed_sources(body))),required_parts_json=json.dumps(sorted(q.required_parts(title,body))),policy_hash=POLICY_HASH)
   f.write(json.dumps(v)+'\n')
 c.read_json(str(p),format='newline_delimited',sample_size=-1).create_view('raw_source_quality')
 c.execute('CREATE TEMP VIEW source_quality AS SELECT * REPLACE(cast(body_deadline AS VARCHAR) AS body_deadline) FROM raw_source_quality')
 output('opportunity_quality_reviews','SELECT q.*,d.decision_json pre_quality_decision_json FROM source_quality q JOIN decision_baselines d USING(episode_id)')
 # Preserve concrete reference codes and row provenance; authorization is not
 # ownership of manufacturing data or a transfer of qualification.
 c.create_function('required_nsns',nsns,['VARCHAR'],duckdb.list_type('VARCHAR'))
 c.execute("CREATE TEMP TABLE source_keys AS SELECT DISTINCT cw.company_id,o.episode_id,right(n.nsn,9) niin,q.required_parts_json FROM prior_company_work_matches cw JOIN prior_opportunities o USING(episode_id) JOIN source_quality q USING(episode_id) CROSS JOIN unnest(required_nsns(concat_ws(' ',o.title,o.scope_text))) n(nsn) WHERE q.restricted_item AND NOT q.qualification_invitation")
 source=m['sources'].get('item_reference_facts')
 if source and all(f.get('path') and Path(f['path']).is_file() for f in source['files']):
  c.read_parquet([f['path'] for f in source['files']]).create_view('catalogue')
  output('company_notice_source_evidence',"""SELECT DISTINCT k.company_id,k.episode_id,i.evidence_id,i.niin,i.part_number,i.rncc_codes,i.rnvc_codes,i.rnsc_codes,i.source_snapshot_sha256,i.source_row,
   'Exact notice NIIN, specified part reference, authorized procurement code A and item-identifying design/reference codes. Manufacturing data rights and current notice eligibility remain separate.' interpretation
   FROM source_keys k JOIN catalogue i ON i.cage_code=substr(k.company_id,6) AND i.niin=k.niin
   WHERE trim(upper(i.rnsc_codes))='A' AND trim(i.rnvc_codes)='2' AND regexp_full_match(trim(i.rncc_codes),'[35 ,;|/]+')
   AND contains(from_json(k.required_parts_json,'["VARCHAR"]'),regexp_replace(upper(i.part_number),'[^A-Z0-9]','','g'))""")
 else:
  output('company_notice_source_evidence',"SELECT NULL::VARCHAR company_id,NULL::VARCHAR episode_id,NULL::VARCHAR evidence_id,NULL::VARCHAR niin,NULL::VARCHAR part_number,NULL::VARCHAR rncc_codes,NULL::VARCHAR rnvc_codes,NULL::VARCHAR rnsc_codes,NULL::VARCHAR source_snapshot_sha256,NULL::BIGINT source_row,NULL::VARCHAR interpretation WHERE false")
 def decision(raw,verdict,reason,passages):
  d=json.loads(raw)
  if verdict!='usable' and d['stage']!='resolved_or_elapsed':
   d.update(stage='evidence_gap',participation_route='source_revalidation_required',response_window_verified=False,next_action='Check the agency notice and its amendments to resolve the conflicting source details before treating this as available work.')
   d.setdefault('negative_evidence',[]).append(dict(reason=reason,source_passages=json.loads(passages),policy_hash=POLICY_HASH))
   d.setdefault('unresolved',[]).append(reason)
  d['source_quality_policy_hash']=POLICY_HASH;d.pop('decision_hash',None);d['decision_hash']=hashlib.sha256(json.dumps(d,sort_keys=True,default=str).encode()).hexdigest()
  return json.dumps(d,sort_keys=True)
 c.create_function('quality_decision',decision,['VARCHAR']*4,'VARCHAR')
 c.execute('CREATE TEMP TABLE reviewed_decisions AS SELECT d.* EXCLUDE(stage,decision_hash,decision_json),quality_decision(b.decision_json,q.verdict,q.reason,q.passages_json) decision_json FROM prior_demand_decisions d JOIN source_quality q USING(episode_id) JOIN decision_baselines b USING(episode_id)')
 output('demand_decisions',"SELECT *,json_extract_string(decision_json,'$.stage') stage,json_extract_string(decision_json,'$.decision_hash') decision_hash FROM reviewed_decisions")
 # Selected document coding is supporting evidence for delivery mode, not a
 # substitute for the named scope. Do not inherit a prime's PSC into a component.
 cols={n for n,*_ in c.execute('DESCRIBE prior_company_work_matches').fetchall()}
 if 'company_scope_features' in m['tables']:
  c.read_parquet(m['tables']['company_scope_features']['path']).create_view('scope_features')
  c.execute("""CREATE TEMP TABLE selected_doc_codings AS SELECT f.company_id,f.doc_id,
   CASE WHEN count(DISTINCT nullif(trim(f.psc),''))=1 THEN min(nullif(trim(f.psc),'')) END observed_work_psc,
   to_json(list(DISTINCT struct_pack(psc:=f.psc,naics:=f.naics))) work_classification_evidence_json
   FROM scope_features f SEMI JOIN prior_company_work_matches w ON w.company_id=f.company_id AND w.doc_id=f.doc_id GROUP BY f.company_id,f.doc_id""")
 else:c.execute("CREATE TEMP TABLE selected_doc_codings AS SELECT NULL::VARCHAR company_id,NULL::VARCHAR doc_id,NULL::VARCHAR observed_work_psc,NULL::VARCHAR work_classification_evidence_json WHERE false")
 def fit(work,title,named,entry,source,cage,restricted,invitation,cages,authority,delivery,work_psc):
  verdict,reason=q.fit_fields(work,title,named,entry,source,cage,restricted,invitation,json.loads(cages),authority,delivery,work_psc)
  _,observed,mode_reason=mode_fit(work,delivery,work_psc)
  target=q.terms(__import__('re').split(r'(?i)_End_Item_|\bNSN\b',title or '')[0])|q.terms(named)
  return json.dumps(dict(verdict=verdict,reason=reason,observed_delivery_mode=observed,mode_reason=mode_reason,shared_scope_terms=sorted(q.terms(work)&target)))
 c.create_function('assess_fit',fit,['VARCHAR']*6+['BOOLEAN','BOOLEAN','VARCHAR','BOOLEAN','VARCHAR','VARCHAR'],'VARCHAR',null_handling='special')
 old_mode_columns=cols&set('required_delivery_mode delivery_mode_basis observed_work_psc observed_delivery_mode work_classification_evidence_json mode_reason shared_scope_terms_json'.split())
 cw_selection='cw.*'+(' EXCLUDE('+','.join(sorted(old_mode_columns))+')' if old_mode_columns else '')
 c.execute("""CREATE TEMP TABLE assessed AS SELECT """+cw_selection+""",q.verdict source_verdict,q.required_delivery_mode,q.delivery_mode_basis,f.observed_work_psc,f.work_classification_evidence_json,
 assess_fit(cw.work_text,o.title,q.nomenclature,cw.company_entry_status,o.source_name,substr(cw.company_id,6),q.restricted_item,q.qualification_invitation,q.source_cages_json,coalesce(a.authorized,false),q.required_delivery_mode,f.observed_work_psc) fit_json
 FROM prior_company_work_matches cw JOIN prior_opportunities o USING(episode_id) JOIN source_quality q USING(episode_id)
 LEFT JOIN selected_doc_codings f ON f.company_id=cw.company_id AND f.doc_id=cw.doc_id
 LEFT JOIN (SELECT DISTINCT company_id,episode_id,true authorized FROM quality_company_notice_source_evidence) a ON a.company_id=cw.company_id AND a.episode_id=cw.episode_id""")
 original_caveat='pre_quality_company_evidence_caveat' if 'pre_quality_company_evidence_caveat' in cols else 'company_evidence_caveat'
 output('company_match_assessments',"SELECT match_id,company_id,episode_id,opportunity_hash,profile_hash,doc_id,work_text,sample_evidence_id,source_verdict,company_entry_status,required_delivery_mode,delivery_mode_basis,observed_work_psc,work_classification_evidence_json,"+original_caveat+" original_company_evidence_caveat,json_extract_string(fit_json,'$.observed_delivery_mode') observed_delivery_mode,json_extract_string(fit_json,'$.mode_reason') mode_reason,cast(json_extract(fit_json,'$.shared_scope_terms') AS VARCHAR) shared_scope_terms_json,json_extract_string(fit_json,'$.verdict') verdict,json_extract_string(fit_json,'$.reason') reason,'"+POLICY_HASH+"' policy_hash FROM assessed")
 # All candidates and their prior caveats remain auditable. Serving caveats
 # must not claim proven delivery capability for review-only relationships.
 excluded='review_category,match_hash,company_evidence_caveat'+(',fit_verdict,fit_reason' if 'fit_verdict' in cols else '')+(',pre_quality_company_evidence_caveat' if 'pre_quality_company_evidence_caveat' in cols else '')
 output('company_work_matches',"SELECT * EXCLUDE(fit_json,source_verdict,"+excluded+"),"+original_caveat+" pre_quality_company_evidence_caveat,CASE WHEN json_extract_string(fit_json,'$.verdict')<>'accepted' THEN json_extract_string(fit_json,'$.reason') ELSE "+original_caveat+" END company_evidence_caveat,CASE WHEN json_extract_string(fit_json,'$.verdict')='review' THEN 'market_context' WHEN json_extract_string(fit_json,'$.verdict')='rejected' THEN 'not_recommended' ELSE review_category END review_category,sha256(concat_ws('|',match_id,opportunity_hash,profile_hash,doc_id,work_text,'"+POLICY_HASH+"',fit_json)) match_hash,json_extract_string(fit_json,'$.verdict') fit_verdict,json_extract_string(fit_json,'$.reason') fit_reason,json_extract_string(fit_json,'$.observed_delivery_mode') observed_delivery_mode,json_extract_string(fit_json,'$.mode_reason') mode_reason,cast(json_extract(fit_json,'$.shared_scope_terms') AS VARCHAR) shared_scope_terms_json FROM assessed")
 output('company_matches',"""SELECT m.* REPLACE(coalesce(w.match_hash,m.match_hash) AS match_hash,coalesce(w.company_evidence_caveat,m.company_evidence_caveat) AS company_evidence_caveat)
 FROM prior_company_matches m LEFT JOIN quality_company_work_matches w ON m.company_id=w.company_id AND m.episode_id=w.episode_id
 WHERE w.fit_verdict IS NULL OR w.fit_verdict<>'rejected'""")
 m['source_quality_logic_sha256']=POLICY_HASH;m['quality_policy_version']=q.VERSION
 m['logic_sha256']=hashlib.sha256((m['logic_sha256']+'|'+POLICY_HASH).encode()).hexdigest()
 (r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close();return m


def review_current(previous,root,aws):
 """Re-review immutable market-wide outputs without re-extracting native data."""
 import copy
 r=Path(root)/'runs'/(datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')+'_'+uuid.uuid4().hex[:6]);r.mkdir(parents=True)
 m=copy.deepcopy(previous);m.update(run_id=r.name,reuse_parent_release=previous['run_id'],refresh_mode='quality_review_of_current_sources')
 for name,t in m['tables'].items():
  t['reuse_parent_binding']=True
  if t.get('remote_binding'):
   # Cohort gates inspect every comparator summary locally; the 29m awards
   # and 16m raw observations remain immutable remote bindings.
   if name=='contract_value_cohorts':
    for index,f in enumerate(m['sources'][t['source_binding']]['files']):
     path=r/('cohort_source_'+str(index)+'.parquet');aws.s3.download_file(BUCKET,f['s3_uri'].split('/',3)[3],str(path));f['path']=str(path)
     if hashlib.sha256(path.read_bytes()).hexdigest()!=f['sha256']:raise ValueError('Value-cohort source checksum changed')
   continue
  uri=t['s3_uri']+'part-00000.parquet'
  if t.get('source_binding'):
   uri=next(f['s3_uri'] for f in m['sources'][t['source_binding']]['files'] if f['sha256']==t['sha256'])
  path=r/(name+'.parquet');aws.s3.download_file(BUCKET,uri.split('/',3)[3],str(path));t['path']=str(path)
  if hashlib.sha256(path.read_bytes()).hexdigest()!=t['sha256']:raise ValueError('Published table content changed: '+name)
  print('Loaded immutable table',name,t['rows'],flush=True)
 # Only source evidence needed for the precision gates is fetched.
 s=m['sources'].get('item_reference_facts')
 if s:
  for f in s['files']:
   path=r/('reference_'+f['sha256']+'.parquet');aws.s3.download_file(BUCKET,f['s3_uri'].split('/',3)[3],str(path));f['path']=str(path)
   if hashlib.sha256(path.read_bytes()).hexdigest()!=f['sha256']:raise ValueError('Catalogue evidence checksum changed')
 (r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');apply(r);return r
