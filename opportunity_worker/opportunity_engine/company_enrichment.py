"""Portable company-first retrieval and item valuation, with evidence on every edge.

Relational rare-term retrieval replaces the omitted request-time prototype step.
It never transfers technical approval or turns a historical package into an RFQ.
"""
import datetime,hashlib,html,json,re
from pathlib import Path
import duckdb
STOP=set(json.loads((Path(__file__).parent/'scope_stopwords.json').read_text()))
ALIASES={'assy':'assembly','assys':'assembly','assemblies':'assembly','pumps':'pump','indicators':'indicator','clutches':'clutch','detectors':'detector','valves':'valve','sensors':'sensor','actuators':'actuator','elec':'electric','elect':'electric','mfg':'manufacturing'}
VERSION='company-first-portable-v3'
LOGIC_HASH=hashlib.sha256(Path(__file__).read_bytes()+(Path(__file__).parent/'scope_stopwords.json').read_bytes()).hexdigest()
def words(s):return [ALIASES.get(w,w) for w in re.findall(r'[a-z][a-z0-9]*',html.unescape(re.sub('<[^>]+>',' ',str(s or ''))).lower())]
def scope_terms(s):
 from .quality_core import administrative_scope,NOISE
 if administrative_scope(s):return []
 w=words(s)
 if len(w)>24 or len(str(s or ''))>260:return []
 if re.search(r'awards subcontracts for|may or may not be billed|see footnote',str(s or ''),re.I):return []
 # Classification labels are not delivered work. The presence of the word
 # manufacturing in an otherwise specific statement is not grounds to drop it.
 if re.search(r'(?i)^(?:search, detection, navigation|instrument manufacturing for|other aircraft parts and auxiliary|aircraft manufacturing$)',str(s or '').strip()):return []
 t=sorted(set(x for x in w if x not in STOP and x not in NOISE and len(x)>=3 and not x.isdigit()))
 if set(t)<=set('article first test testing contractor government environmental qual qualification option'.split()):return []
 return t if 2<=len(t)<=12 else []
def scope_fragments(s):
 raw=html.unescape(re.sub('<[^>]+>','\n',str(s or '')))
 if re.search(r'awards subcontracts for|may or may not be billed|see footnote',raw,re.I):return []
 if scope_terms(raw):return [raw]
 if len(words(raw))<=24:return []
 fragments=[]
 for sentence in re.split(r'[;\n]|(?<=[.!?])\s+',raw):
  w=sentence.split()
  if len(w)<=24:
   if scope_terms(sentence):fragments.append(sentence.strip())
  else:
   for start in range(0,len(w),12):
    fragment=' '.join(w[start:start+20])
    if scope_terms(fragment):fragments.append(fragment)
 return sorted(set(fragments))
def nsns(s):return sorted(set(re.sub(r'\D','',x) for x in re.findall(r'(?<!\d)\d{4}[- ]?\d{2}[- ]?\d{3}[- ]?\d{4}(?!\d)',str(s or ''))))
def platforms(s):return set(re.findall(r'\b(?:AH|UH|HH|CH|MH|SH|KC|RC|EC|MC|AC|WC|HC|C|F|B|T|P|E|V)[ -]?[0-9]{1,3}[A-Z]{0,2}\b',str(s or '').upper()))
def canonical_platforms(s):return {re.sub('[ -]','',v) for v in platforms(s)}
def admissible(work,opportunity,stage,work_context="",prior_reported_supplier=False):
 if stage in ('resolved_or_elapsed','evidence_gap','planning_context'):return False
 terms=scope_terms(work)
 if not terms:return False
 if ('supplier' in str(opportunity.get('lane')) or stage=='emerging_demand') and len(terms)<3 and not prior_reported_supplier:return False
 a=canonical_platforms(str(work)+' '+str(work_context or ''));b=canonical_platforms(str(opportunity.get('title'))+' '+str(opportunity.get('scope_text')))
 if a and b and a.isdisjoint(b):return False
 target=str(opportunity.get('title') or '')+' '+str(opportunity.get('scope_text') or '')
 if re.search(r'\b(?:repair|overhaul|remanufactur\w*)\b',str(work),re.I) and not re.search(r'\b(?:repair|overhaul|remanufactur\w*)\b',target,re.I):return False
 if stage=='qualification_opening':return True
 if nsns(opportunity.get('scope_text')) and opportunity.get('lane')=='published_procurement':return False
 return True

def company_entry_route(work,title,scope,lane,prior_reported_supplier=False):
 if lane!='new_prime_historical_supplier_flow':return 'source_specific_route'
 if prior_reported_supplier:return 'reported_supplier_relationship'
 expected=str(title or '').lower()
 if re.search(r'\b(software|cyber|logistics|training|simulation|analysis|test services|testing services|professional services|technical services|engineering services|systems engineering)\b',expected):return 'defined_services_scope'
 current=str(scope or '').split('Current funded scope example:',1)[-1]
 opening=re.search(r'\b(prototyp\w*|research|development|redesign\w*|new design|new source|alternate source|source approval|supplier on.ramp|open architecture)\b',current,re.I)
 if opening and set(scope_terms(work)).issubset(set(words(current))):return 'development_or_source_opening_context'
 return 'entry_route_unestablished'

def augment(directory,skip_work=False,reuse_features=False):
 r=Path(directory);m=json.loads((r/'manifest.json').read_text());c=duckdb.connect();c.execute("SET threads=4;SET memory_limit='8GB';SET preserve_insertion_order=false");c.execute("SET temp_directory='"+str(r/'company_tmp').replace("'","''")+"'");c.execute("SET max_temp_directory_size='40GB'")
 for n in ['company_profiles','opportunities','demand_decisions','company_matches','published_calls','dla_forecasts']:
  if n in m['tables']:c.read_parquet(m['tables'][n]['path']).create_view('base_'+n)
 c.execute('CREATE TEMP TABLE original_match_snapshot AS SELECT m.* FROM base_company_matches m JOIN base_opportunities o ON m.episode_id=o.episode_id AND m.opportunity_hash=o.record_hash JOIN base_company_profiles p ON p.company_id=m.company_id AND p.profile_hash=m.profile_hash');c.execute('DROP VIEW base_company_matches');c.execute('CREATE TEMP VIEW base_company_matches AS SELECT * FROM original_match_snapshot')
 c.read_parquet([f['path'] for f in m['sources']['company_work']['files']]).create_view('work')
 c.create_function('company_entry_route',company_entry_route,['VARCHAR']*4+['BOOLEAN'],'VARCHAR')
 c.create_function('scope_terms',scope_terms,['VARCHAR'],duckdb.list_type('VARCHAR'))
 c.create_function('scope_fragments',scope_fragments,['VARCHAR'],duckdb.list_type('VARCHAR'))
 c.create_function('scope_words',words,['VARCHAR'],duckdb.list_type('VARCHAR'))
 from .quality_core import title_score
 c.create_function('scope_title_score',title_score,['VARCHAR','VARCHAR'],'BIGINT')
 c.create_function('scope_nsns',nsns,['VARCHAR'],duckdb.list_type('VARCHAR'))
 c.create_function('scope_admissible',lambda w,t,s,l,stage,ctx,inc:admissible(w,{'title':t,'scope_text':s,'lane':l},stage,ctx,inc),['VARCHAR']*6+['BOOLEAN'],'BOOLEAN')
 work_columns={n for n,*_ in c.execute('DESCRIBE work').fetchall()}
 c.execute('CREATE TEMP VIEW retrieval_work AS SELECT *'+(",NULL::VARCHAR programme_context_json" if 'programme_context_json' not in work_columns else '')+' FROM work')
 def output(name,sql):
  c.execute('CREATE OR REPLACE TEMP VIEW result AS '+sql);p=r/(name+'.parquet');temporary=r/(name+'.writing.parquet');c.execute("COPY result TO '"+str(temporary).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)");temporary.replace(p)
  cols=[dict(Name=n,Type='bigint' if t in ('BIGINT','INTEGER') else 'double' if t in ('FLOAT','DOUBLE') else 'boolean' if t=='BOOLEAN' else 'string') for n,t,*_ in c.execute('DESCRIBE result').fetchall()];n=c.execute('SELECT count(*) FROM read_parquet(?)',[str(p)]).fetchone()[0];m['tables'][name]=dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),rows=n,columns=cols);c.read_parquet(str(p)).create_view('oi_'+name,replace=True);print(name,n,flush=True)
 estimate=m.get('company_matching_join_estimate')
 if not skip_work:
  # All held company work participates, rather than only a frozen saved match set.
  if reuse_features:
   c.read_parquet(str(r/'company_scope_features.parquet')).create_view('saved_features')
   c.execute("CREATE TEMP TABLE scopes AS SELECT * EXCLUDE(terms_json,profile_hash),from_json(terms_json,'[\"VARCHAR\"]') terms FROM saved_features")
  else:
   c.execute("CREATE TEMP TABLE scopes AS SELECT w.* EXCLUDE(work_text),f.fragment work_text,sha256(w.doc_id||'|'||f.fragment) scope_id,scope_terms(f.fragment) terms FROM retrieval_work w,unnest(scope_fragments(w.work_text)) f(fragment) WHERE substr(cast(last_observed AS VARCHAR),1,10)<='"+m['as_of']+"'")
   c.execute("DELETE FROM scopes WHERE len(terms)<2")
  target_query="""CREATE TEMP TABLE targets AS SELECT o.episode_id,o.record_hash,o.lane,o.title,o.customer_name,coalesce(from_json(json_extract(o.value_inputs_json,'$.prior_supplier_company_ids'),'["VARCHAR"]'),list_transform(from_json(json_extract(o.value_inputs_json,'$.preview'),'[{"supplier_company_id":"VARCHAR"}]'),x->x.supplier_company_id),[]::VARCHAR[]) historical_supplier_ids,CASE WHEN o.lane='published_procurement' THEN len(scope_nsns(coalesce(o.scope_text,''))) ELSE 0 END full_nsn_count,substr(o.scope_text,1,12000) scope_text,d.stage,scope_words(concat_ws(' ',o.title,substr(o.scope_text,1,12000))) tokens,scope_words(o.title) title_tokens FROM base_opportunities o JOIN base_demand_decisions d USING(episode_id) WHERE d.stage NOT IN ('resolved_or_elapsed','evidence_gap','planning_context') AND o.lane NOT IN ('source_item_forecasts','published_item_procurement')"""
  if any(n=='programme' for n,*_ in c.execute('DESCRIBE base_opportunities').fetchall()):target_query=target_query.replace('substr(o.scope_text,1,12000) scope_text',"concat_ws(' ',o.programme,substr(o.scope_text,1,12000)) scope_text")
  c.execute(target_query)
  c.execute('CREATE TEMP TABLE target_tokens AS SELECT DISTINCT episode_id,unnest(tokens) token FROM targets')
  c.execute('CREATE TEMP TABLE frequency AS SELECT token,count(*) docs FROM target_tokens GROUP BY token')
  # Persist reusable company scope features; no company-name or classification-only fit.
  if not reuse_features:output('company_scope_features',"SELECT s.company_id,s.doc_id,s.scope_id,s.work_text,s.source_kind,s.psc,s.naics,s.last_observed,s.sample_evidence_id,s.evidence_digest,s.programme_context_json,to_json(s.terms) terms_json,p.profile_hash FROM scopes s JOIN base_company_profiles p USING(company_id)")
  c.execute('CREATE TEMP TABLE anchor_counts AS SELECT token,count(*) n FROM target_tokens GROUP BY token')
  # Anchor is the least frequent present term; all terms must still occur together.
  c.execute("CREATE TEMP TABLE chosen_terms AS SELECT s.scope_id,arg_min(u.token,struct_pack(n:=f.docs,t:=u.token)) anchor_token,min(f.docs) docs FROM scopes s,unnest(s.terms) u(token) JOIN frequency f USING(token) GROUP BY s.scope_id")
  c.execute("CREATE TEMP TABLE required AS SELECT s.*,t.anchor_token,t.docs FROM scopes s JOIN chosen_terms t USING(scope_id)")
  estimate=c.execute('SELECT sum(w.n*d.n) FROM (SELECT anchor_token token,count(*) n FROM required WHERE docs<=200 OR (len(terms)>=3 AND docs<=2000) GROUP BY anchor_token) w JOIN anchor_counts d USING(token)').fetchone()[0] or 0
  partitions=max(1,(int(estimate)+3999999)//4000000)
  print('Company retrieval join estimate',estimate,'partitions',partitions,flush=True)
  for partition in range(partitions):
   # Keep the potentially broad anchor join narrow. Whole target descriptions
   # are attached only after every scope term and the proximity test pass.
   c.execute("""CREATE OR REPLACE TEMP TABLE pre_hits AS SELECT w.scope_id,o.episode_id,
   list_contains(o.historical_supplier_ids,w.company_id) prior_reported_supplier,
   list_max(list_transform(w.terms,t->list_position(o.tokens,t)))-list_min(list_transform(w.terms,t->list_position(o.tokens,t))) span
   FROM required w JOIN target_tokens tok ON tok.token=w.anchor_token JOIN targets o USING(episode_id)
   WHERE (w.docs<=200 OR (len(w.terms)>=3 AND w.docs<=2000)) AND list_has_all(o.tokens,w.terms)
   AND (o.lane<>'published_procurement' OR o.stage='qualification_opening' OR o.full_nsn_count=0)
   AND (o.lane NOT LIKE '%supplier%' OR len(w.terms)>=3 OR list_contains(o.historical_supplier_ids,w.company_id))
   AND list_max(list_transform(w.terms,t->list_position(o.tokens,t)))-list_min(list_transform(w.terms,t->list_position(o.tokens,t)))<=len(w.terms)+5
   AND hash(w.company_id)%"""+str(partitions)+'='+str(partition))
   c.execute("""CREATE OR REPLACE TEMP VIEW accepted_hits AS SELECT w.company_id,w.doc_id,w.work_text,w.source_kind,w.sample_evidence_id,w.evidence_digest,w.last_observed,w.terms,coalesce(w.programme_context_json,'') work_context,
   o.episode_id,o.record_hash,o.lane,o.stage,o.customer_name,k.prior_reported_supplier,scope_title_score(w.work_text,o.title) title_fit_score,
   company_entry_route(w.work_text,o.title,o.scope_text,o.lane,k.prior_reported_supplier) company_entry_status,k.span
   FROM pre_hits k JOIN required w ON w.scope_id=k.scope_id JOIN targets o ON o.episode_id=k.episode_id
   WHERE scope_admissible(w.work_text,o.title,coalesce(o.scope_text,''),o.lane,o.stage,coalesce(w.programme_context_json,''),k.prior_reported_supplier)""")
   if partition==0:c.execute('CREATE TEMP TABLE hits AS SELECT * FROM accepted_hits WHERE false')
   c.execute("INSERT INTO hits SELECT * FROM accepted_hits QUALIFY row_number() OVER(PARTITION BY company_id,episode_id ORDER BY title_fit_score DESC,len(terms) DESC,last_observed DESC,doc_id)=1")
   c.execute('DROP VIEW accepted_hits');c.execute('DROP TABLE pre_hits')
   print('Company retrieval partition',partition+1,'/',partitions,flush=True)
  print('Company retrieval join estimate',estimate,flush=True)
  for unused in ('scopes','required','target_tokens','frequency','anchor_counts','chosen_terms','targets'):c.execute('DROP TABLE '+unused)
  print('Admissible work links',c.execute('SELECT count(*) FROM hits').fetchone()[0],flush=True)
  work_sql="""WITH winners AS(SELECT company_id,episode_id,arg_max(doc_id,struct_pack(e:=CASE WHEN company_entry_status='entry_route_unestablished' THEN 0 ELSE 1 END,t:=title_fit_score,n:=len(terms),d:=last_observed,id:=doc_id)) doc_id FROM hits GROUP BY company_id,episode_id) SELECT sha256('"""+VERSION+"""|'||h.company_id||'|'||h.episode_id) match_id,h.company_id,h.episode_id,
   least(95.0,65.0+len(h.terms)*3)::DOUBLE score,'"""+VERSION+"""' match_route,array_to_string(h.terms,', ') matched_terms,
   CASE WHEN h.prior_reported_supplier THEN 'This site is named in the linked historical supplier reports for the same parent vehicle or reported program. New package availability, configuration, capacity and qualification remain checks.' ELSE 'Observed site work matches the defined functional scope. Configuration, technical access, qualification and unplaced work remain separate checks.' END company_evidence_caveat,h.sample_evidence_id,
   sha256(concat_ws('|',h.company_id,h.episode_id,h.record_hash,h.evidence_digest,p.profile_hash,h.company_entry_status,h.doc_id,h.work_text)) match_hash,h.record_hash opportunity_hash,p.profile_hash,h.doc_id,h.work_text,h.source_kind,h.last_observed,h.work_context,
   CASE WHEN h.stage='historical_watch' THEN 'account_context' WHEN h.stage='repeat_demand_inference' THEN 'historical_watch' WHEN h.company_entry_status='entry_route_unestablished' THEN 'market_context' ELSE 'work_lead' END review_category,h.company_entry_status
   FROM hits h JOIN winners z USING(company_id,episode_id,doc_id) JOIN base_company_profiles p USING(company_id)
   WHERE h.span<=len(h.terms)+5 AND regexp_replace(lower(coalesce(h.customer_name,'')),'[^a-z0-9]','','g')<>regexp_replace(lower(p.company_name),'[^a-z0-9]','','g')"""
  c.execute('CREATE TEMP VIEW generic_work_matches AS '+work_sql)
  report_source=m['sources'].get('supplier_pull_report_evidence')
  if report_source and all(f.get('path') and Path(f['path']).exists() for f in report_source['files']):
   c.read_parquet([f['path'] for f in report_source['files']]).create_view('reported_supplier_history')
   c.execute("""CREATE TEMP VIEW incumbent_matches AS SELECT sha256('reported-supplier-lineage-v1|'||r.supplier_company_id||'|'||r.episode_id) match_id,r.supplier_company_id company_id,r.episode_id,95.0::DOUBLE score,'reported-supplier-lineage-v1' match_route,
   'Named supplier in the linked historical outsourcing reports' matched_terms,'This site is a reported prior supplier in the evidenced prime work stream. Current package availability, configuration, technical access and capacity remain checks.' company_evidence_caveat,r.evidence_id sample_evidence_id,
   sha256(concat_ws('|',r.supplier_company_id,r.episode_id,o.record_hash,w.evidence_digest,p.profile_hash)) match_hash,o.record_hash opportunity_hash,p.profile_hash,w.doc_id,w.work_text,w.source_kind,r.event_date last_observed,coalesce(w.programme_context_json,'') work_context,
   'work_lead' review_category,'reported_supplier_relationship' company_entry_status
   FROM reported_supplier_history r JOIN base_opportunities o USING(episode_id) JOIN base_demand_decisions d USING(episode_id) JOIN base_company_profiles p ON p.company_id=r.supplier_company_id
   JOIN retrieval_work w ON w.company_id=r.supplier_company_id AND w.doc_id=sha256(concat_ws('|',r.supplier_company_id,'subaward_report',regexp_replace(upper(regexp_replace(coalesce(r.work_text,''),'^[0-9]{8,12}!\\s*','')),'[^A-Z0-9]','','g')))
   WHERE o.lane='new_prime_historical_supplier_flow' AND d.stage NOT IN ('resolved_or_elapsed','evidence_gap','planning_context')
   QUALIFY row_number() OVER(PARTITION BY r.supplier_company_id,r.episode_id ORDER BY r.event_date DESC,r.evidence_id)=1""")
   output('company_work_matches','SELECT g.* FROM generic_work_matches g WHERE NOT EXISTS(SELECT 1 FROM incumbent_matches i WHERE i.company_id=g.company_id AND i.episode_id=g.episode_id) UNION ALL SELECT * FROM incumbent_matches')
  else:output('company_work_matches','SELECT * FROM generic_work_matches')
  # The same edge contract feeds the authenticated API and hosted export.
  output('company_matches',"""SELECT * FROM base_company_matches WHERE match_route<>'specific_work_text' AND match_route NOT LIKE 'company-first-portable-%' UNION ALL SELECT match_id,company_id,episode_id,score,match_route,matched_terms,company_evidence_caveat,sample_evidence_id,match_hash,opportunity_hash,profile_hash FROM oi_company_work_matches n WHERE NOT EXISTS(SELECT 1 FROM base_company_matches b WHERE b.company_id=n.company_id AND b.episode_id=n.episode_id AND b.opportunity_hash=n.opportunity_hash AND b.match_route<>'specific_work_text' AND b.match_route NOT LIKE 'company-first-portable-%')""")
 if not skip_work:c.execute('DROP TABLE hits')
 # Product references keep catalogue prices and averages separate from transactions.
 source=m['sources'].get('item_reference_facts')
 if source:
  c.read_parquet([f['path'] for f in source['files']]).create_view('raw_item_reference_facts')
  c.execute("CREATE TEMP VIEW item_reference_facts AS SELECT * REPLACE(CASE WHEN govt_estimated_price>0 THEN govt_estimated_price END AS govt_estimated_price,CASE WHEN supplier_average_price>0 AND (last_sold_date IS NULL OR substr(cast(last_sold_date AS VARCHAR),1,10)<='"+m['as_of']+"') THEN supplier_average_price END AS supplier_average_price) FROM raw_item_reference_facts")
  # Exact company/notice item references expose unit evidence even where a
  # capability enquiry publishes no order quantity. No total is inferred.
  c.execute("""CREATE TEMP TABLE notice_item_keys AS SELECT DISTINCT o.episode_id,o.record_hash,n.nsn,right(n.nsn,9) niin
  FROM base_opportunities o JOIN base_demand_decisions d USING(episode_id),unnest(scope_nsns(concat_ws(' ',o.title,o.scope_text))) n(nsn)
  WHERE o.lane='published_procurement' AND d.stage IN ('qualification_opening','published_requirement')""")
  current_matches='oi_company_matches' if not skip_work else 'base_company_matches'
  c.execute("""CREATE TEMP TABLE notice_item_references AS SELECT DISTINCT cm.company_id,k.episode_id,k.record_hash,k.nsn,i.*
  FROM """+current_matches+""" cm JOIN notice_item_keys k ON k.episode_id=cm.episode_id AND k.record_hash=cm.opportunity_hash
  JOIN item_reference_facts i ON i.cage_code=substr(cm.company_id,6) AND i.niin=k.niin""")
  c.execute("""CREATE TEMP TABLE notice_item_prices AS SELECT company_id,episode_id,record_hash,nsn,
  CASE WHEN count(DISTINCT unit_of_issue)=1 THEN min(unit_of_issue) END catalogue_unit,
  CASE WHEN count(DISTINCT unit_of_issue)=1 AND count(DISTINCT govt_estimated_price)=1 THEN min(govt_estimated_price)::DOUBLE END government_reference_unit_price,
  CASE WHEN count(DISTINCT unit_of_issue)=1 AND count(DISTINCT supplier_average_price)=1 THEN min(supplier_average_price)::DOUBLE END supplier_average_unit_price,
  max(last_sold_date) FILTER(WHERE supplier_average_price IS NOT NULL) supplier_average_last_sale,count(*) reference_rows,
  to_json(list(DISTINCT struct_pack(evidence_id:=evidence_id,part_number:=part_number,description:=description,source_snapshot_sha256:=source_snapshot_sha256,source_row:=source_row,government_reference_price:=govt_estimated_price,supplier_average_price:=supplier_average_price,unit:=unit_of_issue,last_sale:=last_sold_date))) catalogue_evidence_json
  FROM notice_item_references GROUP BY company_id,episode_id,record_hash,nsn""")
  output('company_notice_item_prices',"""SELECT company_id,episode_id,record_hash,to_json(list(struct_pack(nsn:=nsn,catalogue_unit:=catalogue_unit,government_reference_unit_price:=government_reference_unit_price,supplier_average_unit_price:=supplier_average_unit_price,supplier_average_last_sale:=supplier_average_last_sale,reference_rows:=reference_rows,catalogue_evidence:=json(catalogue_evidence_json)) ORDER BY nsn)) items_json,
  'Exact notice NIIN and company catalogue reference. Unit-price evidence only; quantity, configuration, current source access and total opportunity value remain separate checks.' interpretation
  FROM notice_item_prices GROUP BY company_id,episode_id,record_hash""")
  for unused in ('notice_item_keys','notice_item_references','notice_item_prices'):c.execute('DROP TABLE '+unused)
  # Regenerate exact catalogue-to-demand links for every held profile, rather than
  # retaining the prototype's ten-item-per-company seed. A reference is not approval.
  c.execute("""CREATE TEMP TABLE item_demand_keys AS SELECT o.episode_id,o.record_hash,o.lane,
   coalesce(json_extract_string(o.value_inputs_json,'$.niin'),right(json_extract_string(o.value_inputs_json,'$.nsn'),9),right(nullif(regexp_extract(o.title,'NSN ([0-9]{13})',1),''),9)) niin
   FROM base_opportunities o JOIN base_demand_decisions d USING(episode_id)
   WHERE o.lane IN ('source_item_forecasts','published_item_procurement') AND d.stage IN ('source_forecast','published_requirement')""")
  c.execute("""CREATE TEMP TABLE exact_item_keys AS SELECT 'CAGE:'||i.cage_code company_id,k.episode_id,k.record_hash,min(i.evidence_id) evidence_id,arg_min(i.description,i.evidence_id) reference_description,arg_min(i.part_number,i.evidence_id) reference_part_number,p.profile_hash,k.niin
   FROM raw_item_reference_facts i JOIN item_demand_keys k USING(niin) JOIN base_company_profiles p ON p.company_id='CAGE:'||i.cage_code
   GROUP BY i.cage_code,k.episode_id,k.record_hash,p.profile_hash,k.niin""")
  output('company_item_matches',"""SELECT sha256('catalogue-demand-v1|'||company_id||'|'||episode_id) match_id,company_id,episode_id,85.0::DOUBLE score,
   'exact_catalogue_reference-v1' match_route,'exact NIIN '||niin matched_terms,
   'Exact company/item catalogue reference. This does not establish current manufacturing source, configuration, data rights, technical approval or capacity.' company_evidence_caveat,
   evidence_id sample_evidence_id,sha256(concat_ws('|',company_id,episode_id,record_hash,evidence_id,profile_hash)) match_hash,record_hash opportunity_hash,profile_hash,niin,reference_description,reference_part_number FROM exact_item_keys""")
  prior='oi_company_matches' if not skip_work else 'base_company_matches'
  c.execute('CREATE TEMP TABLE work_and_prior_matches AS SELECT * FROM '+prior)
  output('company_matches',"""SELECT * FROM work_and_prior_matches UNION ALL SELECT n.match_id,n.company_id,n.episode_id,n.score,n.match_route,n.matched_terms,n.company_evidence_caveat,n.sample_evidence_id,n.match_hash,n.opportunity_hash,n.profile_hash FROM oi_company_item_matches n WHERE NOT EXISTS(SELECT 1 FROM work_and_prior_matches b WHERE b.company_id=n.company_id AND b.episode_id=n.episode_id AND b.opportunity_hash=n.opportunity_hash)""")
  c.execute('DROP VIEW base_company_matches');c.execute('CREATE TEMP VIEW base_company_matches AS SELECT * FROM oi_company_matches')
  for unused in ('work_and_prior_matches','original_match_snapshot','exact_item_keys','item_demand_keys'):c.execute('DROP TABLE '+unused)
  # Materialize the exact forecast/company keys before scanning the all-company catalogue.
  # This prevents the optimizer expanding every catalogue reference across forecast episodes.
  c.execute("CREATE TEMP TABLE eligible_forecast_matches AS SELECT DISTINCT cm.company_id,cm.episode_id,cm.opportunity_hash,json_extract_string(o.value_inputs_json,'$.niin') niin FROM base_company_matches cm JOIN base_opportunities o ON cm.episode_id=o.episode_id AND cm.opportunity_hash=o.record_hash WHERE o.lane='source_item_forecasts'")
  c.execute("CREATE TEMP TABLE matched_item_references AS SELECT i.* FROM item_reference_facts i SEMI JOIN eligible_forecast_matches e ON i.cage_code=substr(e.company_id,6) AND i.niin=e.niin")
  catalogue_parts=r/'company_catalogue_parts';catalogue_parts.mkdir(exist_ok=True)
  for bucket in range(16):
   sql="""SELECT cage_code,niin,min(unit_of_issue) catalogue_unit,
   CASE WHEN count(DISTINCT govt_estimated_price)=1 THEN min(govt_estimated_price)::DOUBLE END government_reference_unit_price,
   CASE WHEN count(DISTINCT supplier_average_price)=1 THEN min(supplier_average_price)::DOUBLE END supplier_average_unit_price,
   max(last_sold_date) FILTER(WHERE supplier_average_price IS NOT NULL) supplier_average_last_sale,
   to_json(list(DISTINCT struct_pack(evidence_id:=evidence_id,part_number:=part_number,description:=description,source_snapshot_sha256:=source_snapshot_sha256,source_row:=source_row,government_reference_price:=govt_estimated_price,supplier_average_price:=supplier_average_price,unit:=unit_of_issue,last_sale:=last_sold_date))) catalogue_evidence_json
   FROM matched_item_references WHERE unit_of_issue IS NOT NULL AND hash(cage_code,niin)%16={bucket} GROUP BY cage_code,niin HAVING count(DISTINCT unit_of_issue)=1""".format(bucket=bucket);p=catalogue_parts/(str(bucket)+'.parquet');c.execute("COPY ("+sql+") TO '"+str(p).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)")
  c.read_parquet([str(catalogue_parts/(str(bucket)+'.parquet')) for bucket in range(16)]).create_view('catalogue_prices')
  c.execute('DROP TABLE matched_item_references')
  parts=r/'company_forecast_parts';parts.mkdir(exist_ok=True)
  # Bounded partitions avoid a single non-spillable list aggregate across the market.
  for bucket in range(16):
   sql="""WITH f AS(SELECT o.episode_id,o.record_hash,json_extract_string(o.value_inputs_json,'$.niin') niin,o.window_start,o.window_end FROM base_opportunities o SEMI JOIN eligible_forecast_matches e USING(episode_id) WHERE o.lane='source_item_forecasts' AND hash(o.episode_id)%16={bucket})
   SELECT f.episode_id,f.record_hash,f.niin,count(*) month_count,count(DISTINCT d.forecast_month) distinct_months,sum(d.forecast_qty)::BIGINT quantity,min(d.forecast_month) first_month,max(d.forecast_month) last_month,
   to_json(list(struct_pack(month:=d.forecast_month,quantity:=d.forecast_qty,source_release:=d.source_release,source_file:=d.source_file,source_item_id:=d.source_item_id) ORDER BY d.forecast_month)) monthly_evidence_json
   FROM f JOIN base_dla_forecasts d ON d.niin=f.niin AND try_cast(substr(cast(d.forecast_month AS VARCHAR),1,10) AS DATE)>=try_cast(substr(f.window_start,1,10) AS DATE) AND try_cast(substr(cast(d.forecast_month AS VARCHAR),1,10) AS DATE)<=try_cast(substr(f.window_end,1,10) AS DATE) GROUP BY f.episode_id,f.record_hash,f.niin""".format(bucket=bucket)
   p=parts/(str(bucket)+'.parquet');c.execute("COPY ("+sql+") TO '"+str(p).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)")
  c.read_parquet([str(parts/(str(bucket)+'.parquet')) for bucket in range(16)]).create_view('forecast_quantities')
  valuation_parts=r/'company_valuation_parts';valuation_parts.mkdir(exist_ok=True)
  for bucket in range(16):
   sql="""SELECT e.company_id,q.episode_id,q.record_hash,q.niin,q.quantity,i.catalogue_unit,q.first_month,q.last_month,q.month_count,q.monthly_evidence_json,
   i.government_reference_unit_price,i.supplier_average_unit_price,
   CASE WHEN q.quantity>=0 THEN q.quantity*i.government_reference_unit_price END government_reference_lot_scenario,
   CASE WHEN q.quantity>=0 THEN q.quantity*i.supplier_average_unit_price END supplier_average_lot_scenario,
   i.supplier_average_last_sale,i.catalogue_evidence_json,
   'Forecast quantity uses the unique catalogue unit; the forecast itself does not state a unit. These are alternative historical/reference-price scenarios, not orders or company-addressable value.' interpretation
   FROM eligible_forecast_matches e JOIN forecast_quantities q ON q.episode_id=e.episode_id AND q.record_hash=e.opportunity_hash JOIN catalogue_prices i ON i.cage_code=substr(e.company_id,6) AND i.niin=q.niin WHERE q.month_count=q.distinct_months AND hash(e.company_id)%16={bucket} AND hash('CAGE:'||i.cage_code)%16={bucket}""".format(bucket=bucket);p=valuation_parts/(str(bucket)+'.parquet');c.execute("COPY ("+sql+") TO '"+str(p).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)")
  c.read_parquet([str(valuation_parts/(str(bucket)+'.parquet')) for bucket in range(16)]).create_view('valuation_rows')
  output('company_item_valuations','SELECT * FROM valuation_rows')
 m['company_enrichment_logic_sha256']=LOGIC_HASH;m['company_matching_join_estimate']=estimate;(r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close();return m
