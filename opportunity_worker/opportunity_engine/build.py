"""Shared evidence materialisation. Inputs are versioned sources, never application data writes."""
import datetime,hashlib,json,re,uuid
from pathlib import Path
import duckdb
from . import rules
from .matching import match_topics
from .demand_history import scope_key,work_mode

def quote(s):return "'"+str(s).replace("'","''")+"'"
def normalise_phase(s):
 return {'I':'I','II':'II','III':'III','1':'I','2':'II','3':'III'}.get(re.sub(r'(?i)phase|[^A-Z0-9]','',str(s or '').upper()))

def build(seed_manifest,sbir_manifest,output,as_of,shared=None,additional_manifests=(),skip_matching=False):
 out=Path(output);out.mkdir(parents=True,exist_ok=True)
 c=duckdb.connect(str(out/'build.duckdb'));c.execute("SET threads=1;SET memory_limit='1800MB';SET preserve_insertion_order=false")
 c.execute('SET temp_directory='+quote(out/'tmp'));c.execute("SET max_temp_directory_size='3GB'")
 sources=json.loads(Path(seed_manifest).read_text());sbir=json.loads(Path(sbir_manifest).read_text());tables={}
 for name,s in sources.items():
  p=Path(s['path'])
  if hashlib.sha256(p.read_bytes()).hexdigest()!=s['sha256']:raise ValueError('Source hash mismatch: '+name)
  c.execute('CREATE VIEW '+name+' AS SELECT * FROM read_parquet('+quote(p)+')')
 for extra in ([shared] if shared else [])+list(additional_manifests):
  for name,s in json.loads(Path(extra).read_text())['sources'].items():
   if s.get('remote_only'):
    sources[name]=s;continue
   paths=[x['path'] for x in s['files']]
   if not paths:raise ValueError('Empty live source '+name)
   for f in s['files']:
    if hashlib.sha256(Path(f['path']).read_bytes()).hexdigest()!=f['sha256']:raise ValueError('Source hash mismatch: '+name)
   # Native refreshes need not share the old research-only columns.
   c.execute('CREATE OR REPLACE VIEW '+name+' AS SELECT * FROM read_parquet(['+','.join(quote(p) for p in paths)+'],union_by_name=true)');sources[name]=s
 c.execute("CREATE MACRO nid(s) AS regexp_replace(upper(coalesce(cast(s AS VARCHAR),'')),'[^A-Z0-9]','','g')")
 c.execute("CREATE MACRO dt(s) AS coalesce(try_cast(substr(cast(s AS VARCHAR),1,10) AS DATE),try_strptime(cast(s AS VARCHAR),'%m/%d/%Y')::DATE)")
 c.create_function('technical_scope_key',lambda s:scope_key(re.split(r'(?i)_End_Item_|\bNSN\b',s or '')[0]),['VARCHAR'],'VARCHAR')
 c.create_function('work_mode',work_mode,['VARCHAR','VARCHAR'],'VARCHAR')
 def materialise(name,sql):
  c.execute('CREATE TABLE oi_'+name+' AS '+sql)
  for col,typ,*_ in c.execute('DESCRIBE oi_'+name).fetchall():
   target='VARCHAR' if typ in ('DATE','TIMESTAMP','TIMESTAMP WITH TIME ZONE','JSON') else 'BIGINT' if typ in ('HUGEINT','UBIGINT','UINTEGER') else 'DOUBLE' if typ.startswith('DECIMAL') else None
   if target:c.execute('ALTER TABLE oi_'+name+' ALTER COLUMN '+chr(34)+col+chr(34)+' TYPE '+target)
  path=out/(name+'.parquet');c.execute('COPY oi_'+name+' TO '+quote(path)+' (FORMAT PARQUET,COMPRESSION ZSTD)')
  rows=c.execute('SELECT count(*) FROM oi_'+name).fetchone()[0]
  cols=[]
  for col,typ,*_ in c.execute('DESCRIBE oi_'+name).fetchall():
   glue='bigint' if typ in ('BIGINT','INTEGER','UBIGINT') else 'double' if typ in ('DOUBLE','FLOAT') or typ.startswith('DECIMAL') else 'boolean' if typ=='BOOLEAN' else 'string'
   if glue=='string' and typ not in ('VARCHAR',):raise ValueError('Nonportable output type '+typ+' in '+name+'.'+col)
   cols.append(dict(Name=col,Type=glue))
  tables[name]=dict(path=str(path),rows=rows,sha256=hashlib.sha256(path.read_bytes()).hexdigest(),columns=cols);print(name,rows,flush=True)
 def bind_source(name):
  source=sources[name]
  if source.get('remote_only'):
   tables[name]=dict(rows=source['rows'],sha256=source['sha256'],columns=source['columns'],remote_binding=True,table=source['table'],s3_uri=source['s3_uri'],source_binding=name,profileable_rows=source.get('profileable_rows'),profile_observed_net_total=source.get('profile_observed_net_total'))
   print('Bound complete distributed source',name,source['rows'],flush=True);return
  if len(source['files'])!=1:raise ValueError('Derived serving source must be consolidated: '+name)
  f=source['files'][0];c.execute('CREATE VIEW oi_'+name+' AS SELECT * FROM '+name)
  types={'VARCHAR':'string','BIGINT':'bigint','INTEGER':'bigint','DOUBLE':'double','FLOAT':'double','BOOLEAN':'boolean'}
  columns=[]
  for col,typ,*_ in c.execute('DESCRIBE oi_'+name).fetchall():
   if typ not in types:raise ValueError('Nonportable source binding '+name+'.'+col+': '+typ)
   columns.append(dict(Name=col,Type=types[typ]))
  tables[name]=dict(path=f['path'],rows=source['rows'],sha256=f['sha256'],columns=columns,source_binding=name)
  print('Bound complete serving source',name,source['rows'],flush=True)
 c.execute("CREATE TABLE raw_research AS SELECT row_number() OVER() source_csv_record_number,* FROM read_csv(?,header=true,all_varchar=true,sample_size=1000,max_line_size=2000000)",[sbir['path']])
 c.execute("CREATE TEMP VIEW research_cage_identity AS SELECT uei,min(cage_code) cage_code,list(DISTINCT evidence_id) identity_evidence FROM research_identity GROUP BY uei HAVING count(DISTINCT cage_code)=1")
 # No raw contact details are included in the derived research layer.
 materialise('research_awards',"""WITH r AS(SELECT sha256(concat_ws('|',Agency,Branch,Program,Phase,coalesce(nullif(trim(UEI),''),upper(trim(Company))),\"Agency Tracking Number\",Contract,\"Topic Code\",\"Proposal Award Date\")) award_id,
 sha256(to_json(struct_pack(company:=Company,title:=\"Award Title\",agency:=Agency,branch:=Branch,phase:=Phase,program_type:=Program,uei:=UEI,tracking:=\"Agency Tracking Number\",contract:=Contract,topic:=\"Topic Code\",award_date:=\"Proposal Award Date\",end_date:=\"Contract End Date\",amount:=\"Award Amount\",abstract:=Abstract))) record_hash,
 Company company_name,upper(nullif(trim(UEI),'')) uei,\"Award Title\" title,Agency agency,Branch branch,Phase phase,Program program_type,\"Agency Tracking Number\" tracking_number,Contract contract,nid(Contract) contract_key,\"Topic Code\" topic_code,nid(\"Topic Code\") topic_key,
 cast(dt(\"Proposal Award Date\") AS VARCHAR) award_date,cast(dt(\"Contract End Date\") AS VARCHAR) end_date,
 try_cast(regexp_replace(\"Award Amount\",'[$,]','','g') AS DOUBLE) observed_research_award_usd,Abstract abstract,source_csv_record_number,
 \"Solicitation Number\" solicitation_number FROM raw_research WHERE lower(Agency) IN ('department of defense','department of war')),
 uniq AS(SELECT *,count(DISTINCT record_hash) OVER(PARTITION BY award_id) variants,row_number() OVER(PARTITION BY award_id ORDER BY record_hash,source_csv_record_number) rn FROM r)
 SELECT r.* EXCLUDE(rn),i.cage_code,
 CASE WHEN i.cage_code IS NOT NULL THEN 'CAGE:'||i.cage_code WHEN r.uei IS NOT NULL THEN 'UEI:'||r.uei ELSE 'SBIR:'||sha256(r.company_name) END company_id,
 CASE WHEN i.cage_code IS NOT NULL THEN 'exact_uei_unique_retained_cage' WHEN r.uei IS NOT NULL THEN 'uei_without_unique_site' ELSE 'source_company_only_unresolved' END company_link_basis,
 to_json(i.identity_evidence) identity_evidence_json,
 '"""+sbir['source_url'].replace("'","''")+"""' source_url,'"""+sbir['source_sha256']+"""' source_sha256,'"""+sbir['retrieved_at']+"""' source_retrieved_at,
 'Observed research award; non-additive to USAspending, not a forecast Phase III' value_interpretation,
 CASE WHEN variants>1 THEN 'conflicting_native_award' WHEN dt(r.award_date)>DATE '"""+as_of+"""' THEN 'future_dated_award' WHEN dt(r.award_date) IS NULL THEN 'missing_award_date' ELSE 'observed_research_activity' END evidence_state
 FROM uniq r LEFT JOIN research_cage_identity i USING(uei) WHERE rn=1""")
 materialise('research_phase_history',"""SELECT b.award_id phase_ii_award_id,b.company_id,b.topic_code,b.branch,b.program_type,b.award_date phase_ii_date,
 min(a.award_date) earliest_phase_i_date,count(*) prior_phase_i_awards,to_json(list(DISTINCT a.award_id ORDER BY a.award_id)) prior_award_ids_json,
 'Same UEI, topic, branch and programme with earlier Phase I; project continuity requires source review' relationship,
 'Observed phase history; neither a Phase III prediction nor an open teaming agreement' limitation
 FROM oi_research_awards b JOIN oi_research_awards a ON a.uei=b.uei AND a.topic_key=b.topic_key AND a.branch=b.branch AND a.program_type=b.program_type
 AND a.phase='Phase I' AND b.phase='Phase II' AND a.topic_key<>'' AND dt(a.award_date)<dt(b.award_date) AND date_diff('day',dt(a.award_date),dt(b.award_date))<=1826
 WHERE a.evidence_state='observed_research_activity' AND b.evidence_state='observed_research_activity' GROUP BY ALL""")
 # Latest notice per native solicitation + buying office chain. Awards/cancellations
 # stay in the ledger, and outrank older positive messages by chronology.
 c.execute("CREATE TABLE native_notice_heads AS WITH keys AS(SELECT *,CASE WHEN nullif(nid(solicitation_id),'') IS NOT NULL THEN coalesce(nullif(aac_code,''),nullif(office,''),nullif(sub_tier,''),notice_id)||'|'||nid(solicitation_id) ELSE 'notice|'||notice_id END chain_key FROM sam_notices) SELECT *,sha256(concat_ws('|',notice_id,selected_notice_version_id,title,description,cast(response_deadline AS VARCHAR))) notice_hash FROM keys QUALIFY row_number() OVER(PARTITION BY chain_key ORDER BY dt(posted_at) DESC,posted_at DESC,notice_id DESC)=1")
 if 'contract_value_cohorts' in sources:
  c.execute("""CREATE TEMP VIEW notice_comparators AS SELECT n.notice_id,v.* FROM native_notice_heads n JOIN contract_value_cohorts v ON technical_scope_key(n.title)=v.scope_key
  AND work_mode(n.title,coalesce(n.psc_code,''))=v.work_mode AND nid(n.office)=nid(v.buyer_office)
  AND (nullif(n.psc_code,'') IS NULL OR starts_with(v.psc,n.psc_code))
  QUALIFY row_number() OVER(PARTITION BY n.notice_id ORDER BY v.awards DESC,v.psc,v.buyer_office_code)=1""")
 else:
  c.execute("CREATE TEMP VIEW notice_comparators AS SELECT NULL::VARCHAR notice_id,NULL::DOUBLE observed_min_usd,NULL::DOUBLE observed_median_usd,NULL::DOUBLE observed_max_usd,NULL::VARCHAR comparators_json,NULL::VARCHAR cohort_id,NULL::BIGINT awards WHERE false")
 materialise('published_calls',"""SELECT sha256('sam|'||CASE WHEN nullif(nid(solicitation_id),'') IS NOT NULL THEN coalesce(nullif(aac_code,''),nullif(office,''),nullif(sub_tier,''),notice_id)||'|'||nid(solicitation_id) ELSE 'notice|'||notice_id END) episode_id,
 sha256(concat_ws('|',notice_hash,v.comparators_json)) record_hash,'published_procurement' lane,title,description scope_text,concat_ws(' / ',sub_tier,office) customer_name,NULL::VARCHAR programme,psc_code psc,naics_code naics,NULL::VARCHAR window_start,cast(dt(response_deadline) AS VARCHAR) window_end,
 'Native response date; posting date is separate, not an award forecast' date_basis,v.observed_min_usd value_low,v.observed_median_usd value_median,v.observed_max_usd value_high,
 CASE WHEN v.comparators_json IS NOT NULL THEN 'historical_same_scope_work_mode_and_buyer' ELSE 'no_scoped_comparator_found' END value_basis,
 CASE WHEN v.comparators_json IS NOT NULL THEN 'Observed comparable contract range; current quantity, duration and company workshare remain unpriced' ELSE 'No matching historical scope/work-mode/buyer cohort; no value is assumed' END value_scope,
 CASE WHEN v.comparators_json IS NOT NULL THEN to_json(struct_pack(method:='Historical contract net obligations, exact normalized technical scope, same work mode and buyer office',cohort_id:=v.cohort_id,comparable_awards:=v.awards,preview_maximum:=22,comparators:=json(v.comparators_json),future_demand_estimate:=NULL::DOUBLE,company_addressable_value:=NULL::DOUBLE)) END value_inputs_json,
 'sam_notices' source_name,source_url,sha256('sam_native|'||notice_id||'|'||notice_hash) source_evidence_id,notice_id source_locator,
 CASE WHEN lower(notice_type) LIKE '%award%' THEN 'awarded_or_ordered' WHEN lower(notice_type) LIKE '%cancel%' THEN 'cancelled' ELSE 'procurement_published' END procurement_state,
 CASE WHEN lower(notice_type) LIKE '%award%' THEN 'awarded_or_ordered' WHEN lower(notice_type) LIKE '%cancel%' THEN 'cancelled' ELSE 'outcome_not_established' END reconciled_state,
 'Notice-specific access and eligibility unverified' access_state,cast(posted_at AS VARCHAR) notice_posted_date,notice_type,solicitation_id native_identifier,
 'Latest retained native notice; an announcement alone does not establish applicant eligibility' rationale,
 'Validate technical configuration, phase/vehicle entry and source restrictions; no win probability is assigned' limitations,
 source_s3_uri,source_s3_version_id,source_sha256,selected_notice_version_id
 FROM native_notice_heads n LEFT JOIN notice_comparators v USING(notice_id)""")
 if 'sbir_topics' in sources:
  c.execute("""CREATE TABLE native_research_topic_calls AS SELECT sha256('sbir_topic|'||topic_id) episode_id,source_sha256 record_hash,'published_procurement' lane,title,description scope_text,agency||' / '||branch customer_name,NULL::VARCHAR programme,NULL::VARCHAR psc,NULL::VARCHAR naics,open_date window_start,close_date window_end,
 'SBA topic copy release/open/close dates; agency amendments must be verified' date_basis,NULL::DOUBLE value_low,NULL::DOUBLE value_median,NULL::DOUBLE value_high,'unavailable_pending_scoped_comparators' value_basis,'No topic price or company share is inferred' value_scope,NULL::VARCHAR value_inputs_json,'sbir_topics' source_name,source_url,source_sha256 source_evidence_id,topic_id source_locator,
 CASE WHEN source_status='Open' AND dt(close_date)>=DATE '"""+as_of+"""' THEN 'procurement_published' ELSE 'response_window_elapsed' END procurement_state,'outcome_not_established' reconciled_state,
 'Phase, small-business and research-rights eligibility require notice verification' access_state,release_date notice_posted_date,topic_code native_identifier,
 'Published research topic; no automatic progression from a previous award is assumed' rationale,source_authority limitations FROM sbir_topics""")
 else:
  c.execute('CREATE TABLE native_research_topic_calls AS SELECT * EXCLUDE(notice_type,source_s3_uri,source_s3_version_id,source_sha256,selected_notice_version_id) FROM oi_published_calls WHERE false')
 # Replace older published notices by the freshly ingested native chain. Keep
 # negative heads in published_calls, and only current/recent demand in the feed.
 c.execute("""CREATE VIEW current_demand AS SELECT episode_id,record_hash,lane,title,scope_text,customer_name,programme,psc,naics,cast(window_start AS VARCHAR) window_start,cast(window_end AS VARCHAR) window_end,date_basis,cast(value_low AS DOUBLE) value_low,cast(value_median AS DOUBLE) value_median,cast(value_high AS DOUBLE) value_high,value_basis,value_scope,value_inputs_json,source_name,source_url,source_evidence_id,source_locator,procurement_state,reconciled_state,access_state,NULL::VARCHAR notice_posted_date,native_identifier,rationale,limitations,NULL::VARCHAR notice_type
 FROM demand_candidates WHERE lane<>'published_procurement' UNION ALL
 SELECT episode_id,record_hash,lane,title,scope_text,customer_name,programme,psc,naics,window_start,window_end,date_basis,value_low,value_median,value_high,value_basis,value_scope,value_inputs_json,source_name,source_url,source_evidence_id,source_locator,procurement_state,reconciled_state,access_state,notice_posted_date,native_identifier,rationale,limitations,notice_type
 FROM oi_published_calls WHERE procurement_state='procurement_published' AND (dt(window_end)>=DATE '"""+as_of+"""' OR dt(notice_posted_date)>=DATE '"""+as_of+"""'-INTERVAL 90 DAY)""")
 c.execute('CREATE VIEW all_current_demand AS SELECT * FROM current_demand UNION ALL BY NAME SELECT * FROM native_research_topic_calls')
 c.execute("CREATE TABLE research_topics AS SELECT DISTINCT topic_key,topic_code FROM oi_research_awards WHERE topic_key<>'' AND length(topic_key)>=5")
 materialise('research_notice_links',"""WITH tokens AS(SELECT notice_id,notice_hash,sub_tier,office,title,description,source_url,posted_at,response_deadline,notice_type,
 unnest(regexp_extract_all(upper(concat_ws(' ',title,description)),'[A-Z]{1,5}[0-9]{2,4}[-.][A-Z0-9][A-Z0-9.-]{1,18}')) token FROM native_notice_heads)
 SELECT DISTINCT t.notice_id,t.notice_hash,r.topic_code,r.topic_key,t.title,t.sub_tier customer,t.office,t.source_url,cast(t.posted_at AS VARCHAR) posted_at,cast(t.response_deadline AS VARCHAR) response_deadline,t.notice_type,
 'Explicit topic code in latest notice; topic overlap does not establish a particular firm, project or phase eligibility' relationship,
 CASE WHEN lower(t.notice_type) LIKE '%award%' OR lower(t.notice_type) LIKE '%cancel%' THEN 'resolved_or_negative_notice' WHEN dt(t.response_deadline)>=DATE '"""+as_of+"""' THEN 'dated_published_call' ELSE 'availability_unverified' END entry_state
 FROM tokens t JOIN research_topics r ON nid(t.token)=r.topic_key""")
 # All held funding observations are preserved; budgets are not supplier values.
 if 'fydp_previous' in sources:
  c.execute('CREATE VIEW fydp_combined AS SELECT * FROM fydp_facts UNION ALL BY NAME SELECT * FROM fydp_previous')
 else:c.execute('CREATE VIEW fydp_combined AS SELECT * FROM fydp_facts')
 materialise('programme_funding',"""SELECT sha256(concat_ws('|',source_id,source_locator,measure_type,value_kind,period_key,cast(fiscal_year AS VARCHAR))) funding_id,cast(submission_fiscal_year AS BIGINT) submission_fiscal_year,component,appropriation_category,budget_line_item,budget_line_item_title,cast(fiscal_year AS BIGINT) fiscal_year,funding_status,measure_type,value_kind,cast(amount_usd AS DOUBLE) amount_usd,cast(quantity AS DOUBLE) quantity,to_json(program_elements) program_elements_json,description_excerpt,source_id,source_locator,source_download_url source_url,source_sha256,
 'Source funding/quantity position; not a contract value or supplier addressable value' value_interpretation FROM fydp_combined""")
 materialise('budget_revision_signals',"""WITH revisions AS(SELECT *,lag(amount_usd) OVER w previous_amount_usd,lag(funding_id) OVER w previous_funding_id,lag(funding_status) OVER w previous_funding_status,lag(submission_fiscal_year) OVER w previous_submission_fy
 FROM oi_programme_funding WINDOW w AS(PARTITION BY component,appropriation_category,budget_line_item,fiscal_year,measure_type,value_kind ORDER BY submission_fiscal_year,funding_id))
 SELECT funding_id,previous_funding_id,component,budget_line_item,fiscal_year,measure_type,value_kind,funding_status,previous_funding_status,submission_fiscal_year,previous_submission_fy,amount_usd,previous_amount_usd,amount_usd-previous_amount_usd change_usd,
 'Source plan revision across budget editions; funding-stage differences and external work allocation require review' interpretation FROM revisions WHERE previous_submission_fy<submission_fiscal_year AND amount_usd IS NOT NULL AND previous_amount_usd IS NOT NULL AND amount_usd<>previous_amount_usd""")
 materialise('planning_narratives',"""SELECT sha256(concat_ws('|',source_id,cast(source_page_number AS VARCHAR),page_text_sha256)) narrative_id,cast(submission_fiscal_year AS BIGINT) submission_fiscal_year,component,budget_line_item,budget_line_item_title,cast(p1_line_number AS BIGINT) p1_line_number,to_json(program_elements) program_elements_json,page_text_excerpt,source_id,source_locator,source_download_url source_url,source_sha256,page_text_sha256,
 'Retained page excerpt, not a complete document; future clauses require task and timing review' scope_limit FROM budget_pages""")
 materialise('research_programme_links',"""WITH r AS(SELECT award_id,branch,unnest(regexp_extract_all(upper(abstract),'PE\\s*([0-9]{6,7}[A-Z])',1)) pe FROM oi_research_awards WHERE evidence_state='observed_research_activity'),p AS(SELECT funding_id,component,unnest(regexp_extract_all(upper(program_elements_json),'[0-9]{6,7}[A-Z]')) pe FROM oi_programme_funding)
 SELECT DISTINCT r.award_id,p.funding_id,r.pe,'Explicit programme-element identifier; budget supports context, not task allocation or follow-on award' relationship FROM r JOIN p USING(pe) WHERE r.pe<>'' AND (lower(replace(r.branch,' ',''))=lower(replace(p.component,' ','')) OR (p.component='Defense-Wide' AND r.branch NOT IN ('Army','Navy','Air Force')))""")
 materialise('force_plans',"SELECT force_row_id,platform_source_label,source_json,'Source force changes, not installed fleet or a supplier demand quantity' interpretation FROM force_plans")
 materialise('force_programme_links',"""WITH f AS(SELECT force_row_id,platform_source_label,unnest(regexp_extract_all(upper(platform_source_label),'(?:AH|UH|HH|CH|MH|SH|KC|RC|EC|MC|AC|WC|HC|C|F|B|T|P|E|V)[ -]?[0-9]{1,3}[A-Z]{0,2}')) variant FROM force_plans)
 SELECT DISTINCT f.force_row_id,p.funding_id,f.platform_source_label,f.variant,
 'Explicit variant text association, not a proven external work package or an allocation of funding' relationship
 FROM f JOIN oi_programme_funding p ON contains(regexp_replace(upper(concat_ws(' ',p.budget_line_item_title,p.description_excerpt)),'[^A-Z0-9]','','g'),nid(f.variant)) WHERE length(nid(f.variant))>=3""")
 for extra_name in ('research_prime_links','research_platform_links'):
  if extra_name in sources:materialise(extra_name,'SELECT * FROM '+extra_name)
 if 'sbir_topics' in sources:
  materialise('research_topics_current','SELECT * FROM sbir_topics')
 materialise('technical_evidence','SELECT * FROM technical_facts')
 # Keep the established company-demand population intact. No generic text tags
 # are silently added to its counts as validated opportunities.
 materialise('company_profiles','SELECT * FROM company_profiles')
 for name in ('company_award_facts','company_subcontract_facts','company_observed_features','company_history_coverage','contract_value_cohorts','contract_value_observations'):
  if name in sources and not skip_matching:bind_source(name)
 materialise('company_matches','SELECT match_id,company_id,episode_id,score,match_route,matched_terms,company_evidence_caveat,sample_evidence_id,match_hash,opportunity_hash,profile_hash FROM company_matches')
 rows=c.execute('SELECT episode_id,record_hash,lane,title,scope_text,customer_name,window_start,window_end,date_basis,value_inputs_json,procurement_state,reconciled_state,source_name,source_evidence_id,source_url,source_locator,notice_posted_date,notice_type FROM all_current_demand ORDER BY episode_id').fetchall();names=[x[0] for x in c.description]
 decisions=out/'decision_rows.jsonl'
 with decisions.open('w') as f:
  for row in rows:
   o=dict(zip(names,row));m=rules.evaluate(o,as_of)
   f.write(json.dumps(dict(episode_id=o['episode_id'],source_record_hash=o['record_hash'],rule_id=m['rule_id'],stage=m['stage'],decision_hash=m['decision_hash'],decision_json=json.dumps(m,sort_keys=True,default=str)),default=str)+'\n')
 materialise('demand_decisions','SELECT * FROM read_json_auto('+quote(decisions)+')')
 materialise('opportunities',"SELECT episode_id,record_hash,lane,title,scope_text,customer_name,programme,psc,naics,cast(window_start AS VARCHAR) window_start,cast(window_end AS VARCHAR) window_end,date_basis,cast(value_low AS DOUBLE) value_low,cast(value_median AS DOUBLE) value_median,cast(value_high AS DOUBLE) value_high,value_basis,value_scope,source_name,source_url,source_evidence_id,source_locator,procurement_state,reconciled_state,access_state,value_inputs_json,native_identifier,rationale,limitations,notice_posted_date,notice_type FROM all_current_demand")
 matching_summary={}
 if not skip_matching and 'sbir_topics' in sources and 'company_work' in sources:
  query=c.execute('SELECT topic_id,title FROM sbir_topics');topics=[dict(zip(['topic_id','title'],r)) for r in query.fetchall()]
  matches,matching_summary=match_topics(c,topics,out,as_of)
  materialise('company_topic_matches',"SELECT * FROM read_json("+quote(matches)+",format='newline_delimited',columns={topic_id:'VARCHAR',company_id:'VARCHAR',score:'DOUBLE',evidence_json:'VARCHAR',match_basis:'VARCHAR',applicant_eligibility:'VARCHAR',technical_qualification:'VARCHAR',company_addressable_value:'DOUBLE'})")
  materialise('topic_historical_benchmarks',"""WITH e AS(SELECT m.topic_id,json_extract_string(j.value,'$.evidence_id') award_id FROM oi_company_topic_matches m,json_each(m.evidence_json) j WHERE json_extract_string(j.value,'$.source_kind')='sbir_award'),a AS(SELECT DISTINCT e.topic_id,r.award_id,r.phase,r.observed_research_award_usd,r.branch FROM e JOIN oi_research_awards r USING(award_id) WHERE r.evidence_state='observed_research_activity' AND r.observed_research_award_usd>0)
  SELECT topic_id,phase,count(*) comparable_awards,median(observed_research_award_usd) historical_award_median_usd,min(observed_research_award_usd) historical_award_min_usd,max(observed_research_award_usd) historical_award_max_usd,to_json(list(award_id ORDER BY award_id)) award_ids_json,
  'Historical research awards with retrieved scope similarity, separated by phase. Not an announced topic budget or estimated future award.' value_interpretation,NULL::DOUBLE estimated_future_award_value FROM a GROUP BY topic_id,phase""")
 # An exact topic match produces an evidence edge to a research company, not
 # a finding that this company meets a Phase II or small-business condition.
 materialise('company_research_calls',"""SELECT DISTINCT r.company_id,l.notice_id,l.topic_code,l.entry_state,l.notice_hash,
 'Prior research on the exact topic; applicant/phase eligibility and new task fit require review' match_basis,
 'not_established' applicant_eligibility,NULL::DOUBLE company_addressable_value FROM oi_research_awards r JOIN oi_research_notice_links l USING(topic_key) WHERE r.evidence_state='observed_research_activity'""")
 manifest=dict(run_id=out.name,as_of=as_of,created_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),logic_sha256=rules.LOGIC_HASH,rules=rules.catalogue(),sources=sources,sbir_source=sbir,tables=tables,publication_state='authenticated_service',build_logic_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),matching=matching_summary,matching_logic_sha256=hashlib.sha256((Path(__file__).parent/'matching.py').read_bytes()).hexdigest(),public_release_approved=False,source_mutations=[],source_scope='Full declared sources; research/company/topic edges do not establish technical eligibility or a future award')
 (out/'manifest.json').write_text(json.dumps(manifest,indent=2,default=str)+'\n');c.close();(out/'build.duckdb').unlink();decisions.unlink();return manifest
