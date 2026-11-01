"""Whole-market work histories and recurring buying, independent of company matching.

Only actual award/report rows form cohorts. Classifications constrain cohorts;
they never create capability. All financial projections are conditional gross
buying scenarios, never supplier revenue, qualification or win probabilities.
"""
import datetime, hashlib, json, re, uuid
from pathlib import Path
from .distributed_history import remote, lit, mode
from .aws import DATABASE, BUCKET

POLICY='market-work-demand-v1'
GENERIC=('contract','contracts','award','awards','order','orders','delivery','purchase','procurement','modification','mod','services','service','support','provide','providing','requirement','requirements','for','the','and','of','to','a','an','in','with','under','this','is','on','by','as','at','or','from','supplies','equipment','miscellaneous','general','other','unspecified','inc','llc','corp','corporation','funding','funds','obligation','obligations','exercise','option','period','base','year')

def scope_sql(text):
    # Keep platform numbers and technical words. Remove HTML and administrative
    # prefixes; retain full normalized scope, rather than truncating descriptions.
    words="regexp_extract_all(lower(regexp_replace(regexp_replace(coalesce("+text+",''),'^[0-9]{8,12}!\\s*',''),'<[^>]+>',' ')),'[a-z0-9][a-z0-9-]*')"
    stops='ARRAY['+','.join(lit(x) for x in GENERIC)+']'
    return "array_join(array_sort(array_distinct(filter("+words+",w->length(w)>=3 AND NOT contains("+stops+",w)))),'|')"

def history_sql(prime,sub,as_of):
    end="DATE "+lit(as_of)
    defence=f"(SELECT DISTINCT contract_award_unique_key defence_award_key FROM market_intel_silver.dataset_prime_contracts WHERE awarding_agency_code='097' AND try_cast(action_date AS date)<=DATE {lit(as_of)})"
    raw=f"""SELECT award_key record_id,company_id,company_name,'prime_award' source_kind,
    coalesce(nullif(buyer_office_code,''),concat_ws('|',buyer_name,buyer_office)) customer_key,
    concat_ws(' / ',buyer_name,buyer_office) customer_name,work_text,psc,naics,programme_name programme,programme_code,
    first_action_date event_date,latest_action_date last_observed,period_start,period_end,potential_end,
    observed_net_obligations amount,'observed_net_award_obligations' amount_basis,
    source_url,evidence_id,parent_piid,parent_agency,uei,cast(NULL AS varchar) parent_award_key,
    extent_competed,set_aside,offer_count,performance_city,performance_region,d.defence_award_key IS NOT NULL defence_customer
    FROM {prime} p LEFT JOIN {defence} d ON d.defence_award_key=p.award_key WHERE company_id IS NOT NULL AND observed_net_obligations>0
    AND try_cast(substr(latest_action_date,1,10) AS date) BETWEEN {end}-INTERVAL '5' YEAR AND {end}
    AND try_cast(substr(first_action_date,1,10) AS date)<={end}
    UNION ALL
    SELECT report_key,company_id,sub_name,'subcontract_report',coalesce(nullif(prime_uei,''),prime_name),prime_name,
    work_text,cast(NULL AS varchar),parent_naics,parent_programme_name,parent_programme_code,event_date,
    coalesce(modified_date,event_date),cast(NULL AS varchar),cast(NULL AS varchar),cast(NULL AS varchar),
    reported_amount,'reported_subcontract_package_amount',source_url,evidence_id,parent_piid,cast(NULL AS varchar),sub_uei,parent_award_key,
    cast(NULL AS varchar),cast(NULL AS varchar),cast(NULL AS varchar),performance_city,performance_region,d.defence_award_key IS NOT NULL
    FROM {sub} s LEFT JOIN {defence} d ON d.defence_award_key=s.parent_award_key WHERE company_id IS NOT NULL AND reported_amount>0
    AND try_cast(substr(event_date,1,10) AS date) BETWEEN {end}-INTERVAL '5' YEAR AND {end}"""
    key=scope_sql('work_text');wm=mode('work_text','psc').replace("ELSE 'supply_or_unspecified'","WHEN regexp_like(coalesce(psc,''),'^[A-Z]') THEN 'services' ELSE 'supply_or_unspecified'")
    return f"""WITH raw AS({raw}),keyed AS(SELECT *,{key} scope_key,{wm} work_mode FROM raw)
    SELECT *,lower(to_hex(sha256(to_utf8(concat_ws('|',source_kind,customer_key,scope_key,coalesce(psc,''),work_mode))))) cohort_id,
    lower(to_hex(sha256(to_utf8(concat_ws('|',source_kind,scope_key,coalesce(psc,''),work_mode))))) capability_id,
    cardinality(split(scope_key,'|')) technical_terms,
    regexp_like(coalesce(psc,''),'^[A-Z]') non_item_psc
    FROM keyed WHERE nullif(scope_key,'') IS NOT NULL AND nullif(customer_key,'') IS NOT NULL"""

def recurrence_sql(history,as_of):
    # Delayed DoD reporting must not be mistaken for a demand collapse.
    cutoff="date_add('day',-90,DATE "+lit(as_of)+")"
    preview="CAST(ROW(record_id,company_id,company_name,evidence_id,source_url,work_text,event_date,period_start,period_end,amount,amount_basis,parent_award_key,parent_piid) AS ROW(record_id varchar,company_id varchar,company_name varchar,evidence_id varchar,source_url varchar,work_text varchar,event_date varchar,period_start varchar,period_end varchar,amount double,amount_basis varchar,parent_award_key varchar,parent_piid varchar))"
    return f"""WITH h AS(SELECT * FROM {history} WHERE defence_customer AND technical_terms>=2
    AND try_cast(substr(event_date,1,10) AS date) BETWEEN {cutoff}-INTERVAL '3' YEAR AND {cutoff}),
    dates AS(SELECT DISTINCT cohort_id,try_cast(substr(event_date,1,10) AS date) event_day FROM h),
    gaps AS(SELECT cohort_id,date_diff('day',lag(event_day) OVER(PARTITION BY cohort_id ORDER BY event_day),event_day) gap_days FROM dates),
    cadence AS(SELECT cohort_id,approx_percentile(gap_days,0.25) gap_p25,approx_percentile(gap_days,0.5) gap_median,approx_percentile(gap_days,0.75) gap_p75 FROM gaps WHERE gap_days>0 GROUP BY cohort_id),
    grouped AS(SELECT cohort_id,min(source_kind) source_kind,min(customer_key) customer_key,min(customer_name) customer_name,
    min(scope_key) scope_key,min(work_mode) work_mode,min(capability_id) capability_id,min(psc) psc,min(naics) naics,
    min_by(work_text,event_date) work_text,max_by(programme,event_date) programme,min(event_date) first_event,max(event_date) last_event,
    count(*) awards,count(DISTINCT substr(event_date,1,7)) buying_months,count(DISTINCT company_id) suppliers,
    count(DISTINCT parent_award_key) parent_awards,
    count_if(source_kind='prime_award' AND try_cast(substr(period_end,1,10) AS date)<={cutoff}) completed_awards,
    approx_percentile(amount,0.1) observed_p10,approx_percentile(amount,0.5) observed_median,approx_percentile(amount,0.9) observed_p90,
    approx_percentile(amount,0.1) FILTER(WHERE source_kind='prime_award' AND try_cast(substr(period_end,1,10) AS date)<={cutoff}) completed_p10,
    approx_percentile(amount,0.5) FILTER(WHERE source_kind='prime_award' AND try_cast(substr(period_end,1,10) AS date)<={cutoff}) completed_median,
    approx_percentile(amount,0.9) FILTER(WHERE source_kind='prime_award' AND try_cast(substr(period_end,1,10) AS date)<={cutoff}) completed_p90,
    max(event_date) FILTER(WHERE source_kind='prime_award' AND try_cast(substr(period_end,1,10) AS date)<={cutoff}) latest_completed_award,
    sum(amount) observed_amounts,lower(to_hex(sha256(checksum(evidence_id)))) evidence_digest,
    json_format(cast(max_by({preview},event_date,10) AS JSON)) comparators_json,
    json_format(cast(array_sort(array_agg(DISTINCT company_id)) AS JSON)) supplier_ids_json
    FROM h GROUP BY cohort_id HAVING count(*)>=3 AND count(DISTINCT substr(event_date,1,7))>=3),
    result AS(SELECT g.*,c.gap_p25,c.gap_median,c.gap_p75,
    greatest(365,date_diff('day',try_cast(substr(first_event,1,10) AS date),{cutoff})+1) history_days,
    cast({cutoff} AS varchar) history_cutoff,
    cast(date_diff('day',try_cast(substr(last_event,1,10) AS date),{cutoff}) AS double) since_last
    FROM grouped g JOIN cadence c ON c.cohort_id=g.cohort_id)
    SELECT *,365.25*awards/history_days annual_award_rate FROM result
    WHERE gap_median>0 AND gap_p75<=greatest(365,4*gap_median)
    AND date_diff('day',try_cast(substr(first_event,1,10) AS date),{cutoff})>=365 AND since_last<=greatest(180,2*gap_median)
    AND (source_kind<>'subcontract_report' OR parent_awards>=2)"""

def continuation_sql(history,recurrence,as_of):
    # Existing funded work with a stated end is a follow-on planning prospect,
    # not a claim that a replacement competition will be held or remain open.
    return f"""SELECT h.* FROM {history} h WHERE defence_customer AND source_kind='prime_award' AND technical_terms>=3
    AND amount>=100000 AND try_cast(substr(last_observed,1,10) AS date)>=DATE {lit(as_of)}-INTERVAL '15' MONTH
    AND try_cast(substr(coalesce(nullif(potential_end,''),period_end),1,10) AS date) BETWEEN DATE {lit(as_of)}+INTERVAL '90' DAY AND DATE {lit(as_of)}+INTERVAL '10' YEAR
    AND try_cast(substr(period_start,1,10) AS date)<=DATE {lit(as_of)}
    AND NOT EXISTS(SELECT 1 FROM {recurrence} r WHERE r.cohort_id=h.cohort_id)
    AND NOT regexp_like(lower(work_text),'^(?:funding|administrative|modification|incremental funding|deobligation)\\b')
    """

def capability_sql(history):
    return f"""SELECT company_id,capability_id,min(source_kind) source_kind,min(scope_key) scope_key,min(work_mode) work_mode,min(psc) psc,min(naics) naics,
    max_by(work_text,event_date) work_text,count(*) observed_records,count(DISTINCT customer_key) customers,
    min(event_date) first_observed,max(last_observed) last_observed,max_by(evidence_id,event_date) sample_evidence_id,
    lower(to_hex(sha256(checksum(evidence_id)))) evidence_digest,
    json_format(cast(array_sort(array_agg(DISTINCT programme) FILTER(WHERE nullif(programme,'') IS NOT NULL AND upper(programme) NOT IN ('NONE','NOT APPLICABLE','UNKNOWN'))) AS JSON)) programmes_json
    FROM {history} GROUP BY company_id,capability_id"""

def refresh(aws,manifest,root):
    root=Path(root);root.mkdir(parents=True,exist_ok=True);as_of=manifest['as_of'];run='market_demand_'+as_of.replace('-','')+'_'+uuid.uuid4().hex[:8];sources={}
    def table(name):
        t=manifest['tables'].get(name)
        if t and t.get('table'):return DATABASE+'.'+t['table']
        source=manifest['sources'][{'company_award_facts':'native_company_awards','company_subcontract_facts':'native_company_subcontracts'}[name]]
        return DATABASE+'.'+source['table']
    def emit(name,sql,download=False):
        source=remote(aws,name,sql,run,as_of);sources[name]=source
        if download:
            dest=root/name;dest.mkdir(exist_ok=True)
            for i,f in enumerate(source['files']):
                path=dest/(str(i)+'.parquet');aws.s3.download_file(BUCKET,f['s3_uri'].split('/',3)[3],str(path))
                if hashlib.sha256(path.read_bytes()).hexdigest()!=f['sha256']:raise ValueError('Market source checksum mismatch')
                f['path']=str(path)
            source['remote_only']=False;source['path']=source['files'][0]['path']
        aws.put_json('governance/opportunity_intelligence/sources/'+run+'/manifest.json',dict(as_of=as_of,parent_release=manifest['run_id'],parent_facts_sha256={n:manifest['tables'][n]['sha256'] for n in ('company_award_facts','company_subcontract_facts')},sources=sources,phase='market_input_checkpoint'))
        return DATABASE+'.'+source['table']
    history=emit('market_work_history',history_sql(table('company_award_facts'),table('company_subcontract_facts'),as_of))
    capabilities=emit('company_capability_history',capability_sql(history))
    rec=emit('market_recurring_demand',recurrence_sql(history,as_of),True)
    emit('market_continuation_work',continuation_sql(history,rec,as_of),True)
    emit('market_supplier_history',f"""SELECT cohort_id,company_id,min(capability_id) capability_id,min(source_kind) source_kind,
    max_by(work_text,event_date) work_text,max(event_date) last_observed,count(*) observed_records,
    min(scope_key) scope_key,min(work_mode) work_mode,min(psc) psc,max_by(evidence_id,event_date) sample_evidence_id,max_by(record_id,event_date) record_id,
    lower(to_hex(sha256(checksum(evidence_id)))) evidence_digest
    FROM {history} WHERE try_cast(substr(event_date,1,10) AS date)>=DATE {lit(as_of)}-INTERVAL '3' YEAR GROUP BY cohort_id,company_id""",True)
    training_date=str(datetime.date.fromisoformat(as_of)-datetime.timedelta(days=365))
    training=emit('market_recurrence_training',recurrence_sql(history,training_date))
    emit('market_recurrence_validation',f"""WITH outcomes AS(SELECT t.cohort_id,count(h.record_id) subsequent_packages,count(DISTINCT h.company_id) subsequent_suppliers
    FROM {training} t LEFT JOIN {history} h ON h.cohort_id=t.cohort_id AND try_cast(substr(h.event_date,1,10) AS date)>try_cast(t.history_cutoff AS date)
    AND try_cast(substr(h.event_date,1,10) AS date)<=date_add('day',365,try_cast(t.history_cutoff AS date)) GROUP BY t.cohort_id)
    SELECT t.source_kind,t.work_mode,regexp_like(coalesce(t.psc,''),'^[A-Z]') non_item_psc,count(*) training_streams,count_if(o.subsequent_packages>0) with_subsequent_buying,
    sum(o.subsequent_packages) subsequent_packages,'Retrospective demand-persistence screen using current reconstructed records; not point-in-time forecast accuracy or company win probability' interpretation
    FROM {training} t JOIN outcomes o ON o.cohort_id=t.cohort_id GROUP BY t.source_kind,t.work_mode,regexp_like(coalesce(t.psc,''),'^[A-Z]')""",True)
    stats,query=aws.result(f"SELECT (SELECT count(*) FROM {history}) history_records,(SELECT count(DISTINCT company_id) FROM {history}) companies_with_history,(SELECT count(*) FROM {capabilities}) capability_records,(SELECT count(*) FROM {rec}) recurring_streams")
    manifest.setdefault('sources',{}).update(sources)
    manifest['market_demand_policy']=dict(version=POLICY,as_of=as_of,history_years=5,recurrence_years=3,reporting_lag_days=90,coverage=stats[0],coverage_query_id=query['QueryExecutionId'],qualification='Historical supply is evidence of prior work, not proof of current configuration, rights, eligibility or an unplaced package.',financial='Recurring buying scenarios, historical package benchmarks and reported subcontract amounts remain separate. No company revenue or win probability is inferred.')
    return sources


def capability_customer_routes(aws,manifest,root):
    """Exact observed service capabilities can identify another customer.

    Hardware/repair source qualification is not transferred to a different
    customer or item. This is a capability-fit demand prospect, not a bid gate.
    """
    c=DATABASE+'.'+manifest['sources']['company_capability_history']['table'];r=DATABASE+'.'+manifest['sources']['market_recurring_demand']['table'];h=DATABASE+'.'+manifest['sources']['market_work_history']['table'];as_of=manifest['as_of']
    sql=f"""SELECT r.cohort_id,c.company_id,c.capability_id,c.source_kind,c.scope_key,c.work_mode,c.psc,c.work_text,c.last_observed,c.observed_records,c.sample_evidence_id,c.evidence_digest,
    max(h.source_url) matching_source_url
    FROM {r} r JOIN {c} c ON c.capability_id=r.capability_id
    LEFT JOIN {h} h ON h.company_id=c.company_id AND h.evidence_id=c.sample_evidence_id
    WHERE c.source_kind='prime_award' AND c.observed_records>=2
    AND regexp_like(coalesce(r.psc,''),'^[ADFRSTUV]') AND c.work_mode NOT IN ('repair','supply_or_unspecified')
    AND cardinality(split(r.scope_key,'|'))>=3
    AND try_cast(substr(c.last_observed,1,10) AS date)>=DATE {lit(as_of)}-INTERVAL '3' YEAR
    AND NOT contains(cast(json_parse(r.supplier_ids_json) AS array(varchar)),c.company_id)
    GROUP BY r.cohort_id,c.company_id,c.capability_id,c.source_kind,c.scope_key,c.work_mode,c.psc,c.work_text,c.last_observed,c.observed_records,c.sample_evidence_id,c.evidence_digest"""
    source=remote(aws,'market_capability_customer_links',sql,'market_customer_routes_'+uuid.uuid4().hex[:8],as_of)
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    for i,f in enumerate(source['files']):
        dest=root/(str(i)+'.parquet');aws.s3.download_file(BUCKET,f['s3_uri'].split('/',3)[3],str(dest))
        if hashlib.sha256(dest.read_bytes()).hexdigest()!=f['sha256']:raise ValueError('Customer-capability source changed')
        f['path']=str(dest)
    source.update(remote_only=False,path=source['files'][0]['path']);manifest['sources']['market_capability_customer_links']=source
    return source
