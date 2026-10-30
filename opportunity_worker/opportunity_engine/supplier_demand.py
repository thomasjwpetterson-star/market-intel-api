"""Group funded orders into defensible, source-backed supplier work packages.

A vehicle is evidence of an account relationship, not proof that every order
needs every historical component. Keep all lineage in normalized remote rows.
"""
from .company_enrichment import ALIASES,STOP

WHOLE_PLATFORM_PSCS=('1510','1520','1540','1550','1905','1910','1920','1930','1940','2350')
INTEGRATED_SCOPE=r'\b(integrat(e|ed|ion|ing)|systems? engineering|full[- ]rate production|low[- ]rate initial production|production lot|development and demonstration|engineering and manufacturing development|aircraft production|platform sustainment|fleet sustainment)\b'

def platform_designations_sql(expression):
    from .distributed_history import lit
    prefix=r'(?:AH|UH|HH|CH|MH|SH|KC|RC|EC|MC|AC|WC|HC|C|F|B|T|P|E|V|DDG|CG|CVN|LHA|LHD|LCS|LPD|SSN|SSBN|FFG)'
    combined="upper(coalesce("+expression+",''))"
    strict="regexp_extract_all("+combined+","+lit(r'\b'+prefix+r'[ -]+[0-9]{1,3}[A-Z]{0,2}\b')+")"
    flexible="CASE WHEN regexp_like("+combined+",'AIRCRAFT|HELICOPTER|AIRFRAME|AVIONIC|SUBMARINE|DESTROYER|CARRIER|FRIGATE') THEN regexp_extract_all("+combined+","+lit(r'\b'+prefix+r'[ -]?[0-9]{1,3}[A-Z]{0,2}\b')+") ELSE cast(ARRAY[] AS ARRAY(varchar)) END"
    return "array_sort(array_distinct(transform(concat("+strict+","+flexible+"),v->regexp_replace(v,'[ -]',''))))"


def refresh(emit,p,sub,as_of,actions=None):
    from .distributed_history import lit,terms,mode,norm
    def valid_programme(x):return "(nullif(trim("+x+"),'') IS NOT NULL AND upper(trim("+x+")) NOT IN ('NONE','-NONE-','NOT APPLICABLE','NOT_APPLICABLE','UNKNOWN','N/A','NA') AND NOT regexp_like(trim("+x+"),'^0+$'))"
    def platforms(a):return platform_designations_sql("concat_ws(' ',"+a+".programme_name,"+a+".work_text)")
    def vehicle(a):return "CASE WHEN nullif("+a+".parent_piid,'') IS NOT NULL AND upper("+a+".parent_piid) NOT IN ('-NONE-','NONE') THEN concat_ws('|','1vehicle',"+a+".uei,"+a+".parent_agency,"+a+".parent_piid) END"
    def programme(a):return "CASE WHEN "+valid_programme(a+'.programme_code')+" THEN concat_ws('|','2programme',"+a+".uei,"+a+".programme_code) END"
    def family_keys(a):return "filter(concat(ARRAY["+vehicle(a)+","+programme(a)+"],transform("+platforms(a)+",v->concat_ws('|','3platform',"+a+".uei,v))),x->x IS NOT NULL)"
    def context(a):return "coalesce("+programme(a)+",CASE WHEN cardinality("+platforms(a)+")=1 THEN concat_ws('|','3platform',"+a+".uei,element_at("+platforms(a)+",1)) END,"+vehicle(a)+")"
    aliases='MAP(ARRAY['+','.join(lit(k) for k in ALIASES)+'],ARRAY['+','.join(lit(v) for v in ALIASES.values())+'])'
    def tokenize(x):return "transform(regexp_extract_all(lower(coalesce("+x+",'')),'[a-z][a-z0-9]*'),w->coalesce(element_at("+aliases+",w),w))"
    tokens=tokenize('n.work_text')
    stop="split("+lit('|'.join(sorted(STOP)))+",'|')"
    def package_key(x):
        meaningful="array_distinct(filter("+tokenize(x)+",w->length(w)>=3 AND NOT contains("+stop+",w)))"
        return "CASE WHEN "+terms(x)+"<>'' THEN "+terms(x)+" WHEN length("+norm(x)+")>=40 AND cardinality("+meaningful+")>=3 AND NOT regexp_like(lower(coalesce("+x+",'')),'awards subcontracts for|may or may not be billed|see footnote') THEN 'full:'||lower(to_hex(sha256(to_utf8("+norm(x)+")))) ELSE '' END"
    def workmode(x,psc):return "CASE WHEN regexp_like(lower(coalesce("+x+",'')),'\\br\\s*[&/]\\s*o\\b') THEN 'repair' ELSE "+mode(x,psc)+" END"
    historical_mode=workmode('s.work_text',"''")
    current_mode=workmode('n.work_text','n.psc')
    ts="split(s.scope_key,'|')"
    functional="(all_match("+ts+",t->contains(n.scope_tokens,t)) AND array_max(transform("+ts+",t->array_position(n.scope_tokens,t)))-array_min(transform("+ts+",t->array_position(n.scope_tokens,t)))<=cardinality("+ts+")+8)"
    whole="(n.psc IN ("+','.join(lit(x) for x in WHOLE_PLATFORM_PSCS)+") AND regexp_like(lower(coalesce(n.work_text,'')),'\\b(aircraft|helicopter|unmanned|ship|destroyer|submarine|combat vehicle)\\b'))"
    same_programme=valid_programme('s.parent_programme_code')+' AND s.parent_programme_code=n.programme_code'
    integrated='('+'('+same_programme+' OR cardinality(filter(array_intersect(n.link_keys,s.link_keys),x->starts_with(x,\'3platform|\')))>0)'+' AND regexp_like(lower(coalesce(n.work_text,\'\')),'+lit(INTEGRATED_SCOPE)+'))'
    episode="lower(to_hex(sha256(to_utf8('native-prime-pull-v2|'||n.family_key||'|'||s.scope_key||'|'||s.historical_work_mode))))"
    canonical="SELECT * FROM (SELECT *,row_number() OVER(PARTITION BY parent_award_key,coalesce(nullif(sub_uei,''),sub_name),coalesce(nullif(subaward_number,''),report_key) ORDER BY modified_date DESC NULLS LAST,event_date DESC,evidence_id) revision FROM "+sub+") WHERE revision=1"
    event_sql="SELECT award_key,first_action_date signal_date,evidence_id signal_evidence_id,work_text signal_scope,'New funded prime award' signal_basis,CAST(NULL AS DOUBLE) signal_obligation FROM "+p+" WHERE try_cast(substr(first_action_date,1,10) AS date)>=DATE "+lit(as_of)+"-INTERVAL '365' DAY AND observed_net_obligations>0"
    if actions:event_sql+=" UNION ALL SELECT award_key,event_date,evidence_id,work_text,event_basis,observed_action_obligation FROM "+actions
    fields='award_key,piid,parent_piid,parent_agency,uei,company_name,first_action_date,period_end,programme_name,programme_code,psc,naics,source_url,observed_net_obligations,buyer_office,performance_city,performance_region'
    projection=','.join('n.'+x for x in fields.split(','))
    lineage=f"""WITH canonical_sub AS({canonical}),
    placements AS(SELECT parent_award_key,{package_key('work_text')} scope_key,{workmode('work_text',chr(39)+chr(39))} work_mode,count(*) placed_reports,max(event_date) last_placement_date FROM canonical_sub WHERE reported_amount>0 GROUP BY parent_award_key,{package_key('work_text')},{workmode('work_text',chr(39)+chr(39))}),
    hist_base AS(SELECT s.*,p.evidence_id parent_evidence,{family_keys('p')} link_keys,{package_key('s.work_text')} scope_key,{historical_mode} historical_work_mode FROM canonical_sub s JOIN {p} p ON p.award_key=s.parent_award_key WHERE s.reported_amount>0),
    hist AS(SELECT h.*,g.link_key matched_link_key FROM hist_base h CROSS JOIN UNNEST(h.link_keys) g(link_key)),
    work_events AS({event_sql}),event_heads AS(SELECT * FROM (SELECT *,row_number() OVER(PARTITION BY award_key ORDER BY signal_date DESC,signal_evidence_id) event_revision FROM work_events) WHERE event_revision=1),
    current_awards AS(SELECT {projection},n.evidence_id contract_evidence_id,e.signal_evidence_id evidence_id,e.signal_date,e.signal_basis,e.signal_obligation,CASE WHEN coalesce(e.signal_scope,'')='' OR e.signal_scope=n.work_text THEN n.work_text ELSE concat_ws(' ',e.signal_scope,'Contract scope:',n.work_text) END work_text FROM {p} n JOIN event_heads e ON e.award_key=n.award_key WHERE try_cast(substr(n.period_end,1,10) AS date)>=DATE {lit(as_of)} AND n.observed_net_obligations>0),
    new_awards AS(SELECT n.*,{context('n')} family_key,{family_keys('n')} link_keys,{tokens} scope_tokens FROM current_awards n)
    SELECT DISTINCT {episode} episode_id,n.family_key,s.scope_key,s.historical_work_mode,{current_mode} current_work_mode,n.award_key new_award_key,n.evidence_id new_prime_evidence,n.company_name customer_name,n.first_action_date,n.signal_date,n.signal_basis,n.signal_obligation,n.contract_evidence_id,n.period_end,n.programme_name,n.psc,n.naics,n.source_url new_source_url,n.piid,n.observed_net_obligations new_prime_observed_net,n.work_text new_prime_scope,n.buyer_office government_contracting_office,n.performance_city reported_performance_city,n.performance_region reported_performance_region,
    s.evidence_id report_evidence,s.parent_evidence,s.parent_award_key,s.report_key,s.sub_name supplier,s.company_id supplier_company_id,s.reported_amount,s.event_date,s.source_url report_source_url,s.work_text historical_work,s.performance_city historical_performance_city,s.performance_region historical_performance_region,CASE WHEN z.last_placement_date>=n.signal_date THEN coalesce(z.placed_reports,0) ELSE 0 END placed_reports,
    CASE WHEN {functional} THEN 'Same functional scope in the current funded award' WHEN {whole} THEN 'Whole-platform award with same-vehicle or program outsourcing history' ELSE 'Explicit integrated program work with prior same-program outsourcing' END||CASE WHEN starts_with(f.link_key,'1vehicle|') THEN '; exact parent vehicle' WHEN starts_with(f.link_key,'2programme|') THEN '; exact reported program code' ELSE '; explicit platform designation in parent award text' END demand_scope_basis
    FROM new_awards n CROSS JOIN UNNEST(n.link_keys) f(link_key) JOIN hist s ON s.matched_link_key=f.link_key AND s.prime_uei=n.uei
    LEFT JOIN placements z ON z.parent_award_key=n.award_key AND z.scope_key=s.scope_key AND z.work_mode=s.historical_work_mode
    WHERE s.scope_key<>'' AND n.family_key IS NOT NULL AND f.link_key=array_min(array_intersect(n.link_keys,s.link_keys)) AND (s.parent_psc=n.psc OR ({same_programme}) OR starts_with(f.link_key,'3platform|'))
    AND try_cast(substr(s.event_date,1,10) AS date)<try_cast(substr(n.signal_date,1,10) AS date) AND try_cast(substr(s.event_date,1,10) AS date)>=try_cast(substr(n.signal_date,1,10) AS date)-INTERVAL '5' YEAR
    AND (s.historical_work_mode<>'repair' OR {current_mode}='repair' OR regexp_like(lower(coalesce(n.work_text,'')),'\\b(sustainment|maintenance|fleet support)\\b'))
    AND (s.historical_work_mode<>'research_development' OR {current_mode}='research_development')
    AND ({functional} OR {whole} OR {integrated})"""
    l=emit('native_supplier_lineage',lineage,download=False)
    primes=emit('supplier_pull_prime_evidence',f"SELECT DISTINCT episode_id,family_key,scope_key,historical_work_mode work_mode,new_award_key award_key,new_prime_evidence evidence_id,customer_name,first_action_date,signal_date,signal_basis,signal_obligation,contract_evidence_id,period_end,programme_name,psc,naics,new_source_url source_url,piid,new_prime_observed_net observed_net_obligations,new_prime_scope work_text,government_contracting_office,reported_performance_city,reported_performance_region,placed_reports,demand_scope_basis FROM {l}",download=False)
    reports=emit('supplier_pull_report_evidence',f"SELECT DISTINCT episode_id,family_key,scope_key,historical_work_mode work_mode,report_evidence evidence_id,parent_evidence,parent_award_key,report_key,supplier,supplier_company_id,reported_amount,event_date,report_source_url source_url,historical_work work_text,historical_performance_city,historical_performance_region FROM {l}",download=False)
    placements=emit('supplier_pull_placement_evidence',f"SELECT DISTINCT n.episode_id,s.evidence_id,s.parent_award_key,s.report_key,s.sub_name supplier,s.company_id supplier_company_id,s.reported_amount,s.event_date,s.source_url,s.work_text FROM {primes} n JOIN ({canonical}) s ON s.parent_award_key=n.award_key AND {package_key('s.work_text')}=n.scope_key AND {historical_mode}=n.work_mode WHERE s.reported_amount>0 AND s.event_date>=n.signal_date",download=False)
    report_row="CAST(ROW(evidence_id,parent_evidence,parent_award_key,report_key,supplier,supplier_company_id,reported_amount,event_date,source_url,work_text) AS ROW(report_evidence varchar,parent_evidence varchar,parent_award_key varchar,report_key varchar,supplier varchar,supplier_company_id varchar,reported_amount double,event_date varchar,source_url varchar,work_text varchar))"
    prime_row="CAST(ROW(award_key,evidence_id,source_url,piid,work_text,observed_net_obligations,first_action_date,period_end,government_contracting_office,reported_performance_city,reported_performance_region,demand_scope_basis,signal_date,signal_basis,signal_obligation,contract_evidence_id) AS ROW(award_key varchar,evidence_id varchar,source_url varchar,piid varchar,work_text varchar,observed_net_obligations double,first_action_date varchar,period_end varchar,government_contracting_office varchar,reported_performance_city varchar,reported_performance_region varchar,demand_scope_basis varchar,signal_date varchar,signal_basis varchar,signal_obligation double,contract_evidence_id varchar))"
    rpreview="json_format(cast(array_distinct(concat(max_by("+report_row+",concat_ws('|',event_date,evidence_id),20),ARRAY[min_by("+report_row+",reported_amount),max_by("+report_row+",reported_amount)])) AS JSON))"
    ppreview="json_format(cast(max_by("+prime_row+",concat_ws('|',signal_date,award_key),20) AS JSON))"
    inputs="CAST(ROW(new_prime_digest,new_prime_count,new_prime_observed_net,prior_awards,source_reports,support_digest,first_action_date,json_parse(preview),json_parse(new_prime_preview),json_parse(supplier_company_ids_json),latest_work_event_date,placed_awards,coalesce(json_parse(placement_evidence_ids),json_parse('[]')),episode_id) AS ROW(new_prime_evidence varchar,new_prime_count bigint,new_prime_observed_net double,prior_awards bigint,source_reports bigint,support_digest varchar,first_award_date varchar,preview JSON,new_prime_preview JSON,prior_supplier_company_ids JSON,latest_work_event_date varchar,placed_awards bigint,outcome_evidence_ids JSON,lineage_episode_id varchar))"
    pull=f"""WITH h AS(SELECT episode_id,min(family_key) family_key,min(scope_key) scope_key,min(work_mode) work_mode,min(work_text) work_text,count(DISTINCT parent_award_key) prior_awards,count(*) source_reports,min(reported_amount) value_low,approx_percentile(reported_amount,0.5) value_median,max(reported_amount) value_high,to_hex(sha256(checksum(json_format(cast(ROW(evidence_id,parent_evidence,supplier,supplier_company_id,work_text,reported_amount,event_date,source_url) AS JSON))))) support_digest,{rpreview} preview,json_format(cast(array_agg(DISTINCT supplier_company_id) FILTER(WHERE supplier_company_id IS NOT NULL) AS JSON)) supplier_company_ids_json FROM {reports} GROUP BY episode_id),
    n AS(SELECT episode_id,max_by(customer_name,first_action_date) customer_name,min(first_action_date) first_action_date,max(signal_date) latest_work_event_date,max(period_end) period_end,max_by(programme_name,first_action_date) programme_name,max_by(psc,first_action_date) psc,max_by(naics,first_action_date) naics,max_by(source_url,first_action_date) source_url,max_by(piid,first_action_date) piid,max_by(work_text,first_action_date) new_prime_scope,count(*) new_prime_count,count_if(placed_reports>0) placed_awards,sum(observed_net_obligations) new_prime_observed_net,to_hex(sha256(checksum(json_format(cast(ROW(award_key,evidence_id,work_text,observed_net_obligations,first_action_date,signal_date,signal_basis,signal_obligation,contract_evidence_id,period_end,government_contracting_office,reported_performance_city,reported_performance_region,source_url,customer_name,programme_name,psc,naics,demand_scope_basis,placed_reports) AS JSON))))) new_prime_digest,{ppreview} new_prime_preview FROM {primes} GROUP BY episode_id),
    z AS(SELECT episode_id,json_format(cast(max_by(evidence_id,concat_ws('|',event_date,evidence_id),20) AS JSON)) placement_evidence_ids FROM {placements} GROUP BY episode_id)
    SELECT episode_id,lower(to_hex(sha256(to_utf8(concat_ws('|',new_prime_digest,support_digest,period_end,cast(value_median AS varchar)))))) record_hash,
    'new_prime_historical_supplier_flow' lane,CASE WHEN work_mode='repair' THEN 'Potential repair package: ' ELSE 'Potential supplier package: ' END||work_text title,work_text||' Current funded scope example: '||coalesce(new_prime_scope,'') scope_text,customer_name,nullif(programme_name,'NONE') programme,psc,naics,{lit(as_of)} window_start,period_end window_end,
    'New award or explicit funded work phase; contract performance period is not a supplier RFQ date' date_basis,value_low,value_median,value_high,'historical_same_vehicle_or_programme_subcontract_packages' value_basis,'Observed historical outsourced-package range; current prime funding is context, not supplier workshare' value_scope,json_format(cast({inputs} AS JSON)) value_inputs_json,
    'native_awards_and_subcontract_reports' source_name,source_url,new_prime_digest source_evidence_id,family_key source_locator,piid native_identifier,'new_prime_funded_prior_outsourcing_observed' procurement_state,
    CASE WHEN placed_awards=new_prime_count THEN 'source_confirmed_award' WHEN placed_awards>0 THEN 'some_supplier_placements_observed' ELSE 'supplier_placement_not_established' END reconciled_state,
    'Prime supplier entry, configuration and available work require confirmation' access_state,
    'New awards and explicit funded work phases on existing contracts are grouped into one work-package signal per prime vehicle or reported program. Each order passes a functional-scope or whole-platform/integrated-program test, with earlier reported outsourcing. Complete current awards and historical supplier reports remain linked.' rationale,
    'This is inferred downstream demand, not a published supplier RFQ. Some or all work may already be placed. Historical package amounts do not establish current quantity, technical approval or company-addressable revenue. Reported performance locations are not assumed to be procurement or production sites.' limitations
    FROM h JOIN n USING(episode_id) LEFT JOIN z USING(episode_id)"""
    return emit('native_supplier_pull',pull)
