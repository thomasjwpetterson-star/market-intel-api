"""Current prime pull and historical comparator cohorts with inspectable lineage."""
import datetime,hashlib,html,json,re
from pathlib import Path
import duckdb
from .company_enrichment import words,scope_terms

def clean_scope(value):
    text=html.unescape(re.sub('<[^>]+>',' ',str(value or '')))
    text=re.sub(r'^\d{8,12}!\s*','',text)
    text=re.sub(r'(?i)\b(?:s/?n|serial number)\s*[:#]?\s*[\w-]+','',text)
    return re.sub(r'\s+',' ',text).strip(' ,;')

def scope_key(value):
    t=scope_terms(clean_scope(value))
    if len(t)<2:return ''
    if set(t)<=set('article first test testing contractor government qualification environmental qual option'.split()):return ''
    return '|'.join(t)

def work_mode(text,psc=''):
    text=str(text or '').lower()
    if str(psc or '').startswith('J') or re.search(r'\b(?:repair|overhaul|remanufactur\w*|refurbish\w*)\b',text):return 'repair'
    if re.search(r'\b(?:research|prototype|prototyping|development|r&d)\b',text):return 'research_development'
    if re.search(r'\b(?:integration|integrating)\b',text):return 'integration'
    if re.search(r'\b(?:testing|test services|qualification services)\b',text):return 'test_services'
    return 'supply_or_unspecified'

def rebuild(sources,folder,as_of):
    today=datetime.date.fromisoformat(as_of);folder=Path(folder);folder.mkdir(parents=True,exist_ok=True)
    c=duckdb.connect();c.execute("SET threads=2;SET memory_limit='1800MB';SET preserve_insertion_order=false")
    c.execute("SET temp_directory='"+str(folder/'tmp').replace("'","''")+"'");c.execute("SET max_temp_directory_size='20GB'")
    for name in ('company_award_facts','company_subcontract_facts','demand_candidates'):
        c.read_parquet([f['path'] for f in sources[name]['files']]).create_view(name)
    c.create_function('scope_key',scope_key,['VARCHAR'],'VARCHAR');c.create_function('clean_scope',clean_scope,['VARCHAR'],'VARCHAR');c.create_function('work_mode',work_mode,['VARCHAR','VARCHAR'],'VARCHAR')
    c.execute("CREATE TEMP TABLE awards AS SELECT *,scope_key(coalesce(work_text,'')) scope_key,work_mode(coalesce(work_text,''),coalesce(psc,'')) work_mode FROM company_award_facts")
    # Revisions of a subcontract number are not separate spend to add together.
    c.execute("""CREATE TEMP TABLE packages AS SELECT *,scope_key(coalesce(work_text,'')) scope_key FROM company_subcontract_facts
    QUALIFY row_number() OVER(PARTITION BY parent_award_key,sub_uei,subaward_number ORDER BY modified_date DESC NULLS LAST,event_date DESC,evidence_id)=1""")
    c.execute("""CREATE TEMP TABLE piid_bridge AS SELECT piid,uei,min(award_key) award_key FROM awards GROUP BY piid,uei HAVING count(DISTINCT award_key)=1""")
    c.execute("""CREATE TEMP TABLE linked_packages AS SELECT s.*,p.psc,p.naics,p.programme_name,p.programme_code,p.parent_piid award_vehicle,p.parent_agency,p.uei parent_recipient_uei,p.evidence_id parent_evidence,
    CASE WHEN nullif(p.parent_piid,'') IS NOT NULL AND upper(p.parent_piid) NOT IN ('-NONE-','NONE') THEN concat_ws('|','vehicle',p.uei,p.parent_agency,p.parent_piid)
    WHEN nullif(p.programme_code,'') IS NOT NULL AND upper(p.programme_code) NOT IN ('000','0000','NONE','-NONE-','NOT APPLICABLE') THEN concat_ws('|','programme',p.uei,p.programme_code) END family_key
    FROM packages s LEFT JOIN piid_bridge b ON b.piid=s.parent_piid AND b.uei=s.prime_uei
    JOIN awards p ON p.award_key=coalesce(nullif(s.parent_award_key,''),b.award_key)
    WHERE s.scope_key<>'' AND s.reported_amount>0""")
    c.execute("""CREATE TEMP TABLE new_awards AS SELECT *,CASE WHEN nullif(parent_piid,'') IS NOT NULL AND upper(parent_piid) NOT IN ('-NONE-','NONE') THEN concat_ws('|','vehicle',uei,parent_agency,parent_piid)
    WHEN nullif(programme_code,'') IS NOT NULL AND upper(programme_code) NOT IN ('000','0000','NONE','-NONE-','NOT APPLICABLE') THEN concat_ws('|','programme',uei,programme_code) END family_key
    FROM awards WHERE try_cast(substr(first_action_date,1,10) AS DATE)>=DATE '"""+as_of+"""'-INTERVAL 365 DAY
    AND observed_net_obligations>0 AND (try_cast(substr(period_end,1,10) AS DATE)>=DATE '"""+as_of+"""' OR try_cast(substr(potential_end,1,10) AS DATE)>=DATE '"""+as_of+"""')""")
    # Require an exact parent vehicle or reported programme, same prime UEI and
    # an earlier outsourced package. PSC/NAICS overlap alone cannot form an edge.
    c.execute("""CREATE TEMP TABLE pull AS SELECT n.award_key,n.evidence_id new_prime_evidence,n.company_name customer_name,n.first_action_date,n.period_end,n.potential_end,n.programme_name,n.psc,n.naics,n.source_url,n.piid,n.observed_net_obligations new_prime_observed_net,
    s.scope_key,min(clean_scope(s.work_text)) work_text,count(DISTINCT s.parent_award_key) prior_awards,count(*) source_reports,
    min(s.reported_amount) value_low,median(s.reported_amount) value_median,max(s.reported_amount) value_high,
    sha256(string_agg(s.evidence_id,'|' ORDER BY s.evidence_id)) support_digest,
    to_json(list(struct_pack(report_evidence:=s.evidence_id,parent_evidence:=s.parent_evidence,parent_award_key:=s.parent_award_key,report_key:=s.report_key,supplier:=s.sub_name,supplier_company_id:=s.company_id,reported_amount:=s.reported_amount,event_date:=s.event_date,source_url:=s.source_url,work_text:=s.work_text))) preview
    FROM new_awards n JOIN linked_packages s ON s.family_key=n.family_key AND s.prime_uei=n.uei AND s.parent_award_key<>n.award_key
    AND try_cast(substr(s.event_date,1,10) AS DATE)<try_cast(substr(n.first_action_date,1,10) AS DATE)
    AND try_cast(substr(s.event_date,1,10) AS DATE)>=try_cast(substr(n.first_action_date,1,10) AS DATE)-INTERVAL 5 YEAR
    GROUP BY n.award_key,n.evidence_id,n.company_name,n.first_action_date,n.period_end,n.potential_end,n.programme_name,n.psc,n.naics,n.source_url,n.piid,n.observed_net_obligations,s.scope_key""")
    columns=[(n,t) for n,t,*_ in c.execute('DESCRIBE demand_candidates').fetchall()]
    mapping={
    'episode_id':"sha256('native-prime-pull-v1|'||award_key||'|'||scope_key)",
    'record_hash':"sha256(concat_ws('|',new_prime_evidence,support_digest))",'lane':"'new_prime_historical_supplier_flow'",
    'title':"'Potential supplier package: '||work_text",'scope_text':'work_text','customer_name':'customer_name','programme':"nullif(programme_name,'NONE')",'psc':'psc','naics':'naics',
    'window_start':"'"+as_of+"'",'window_end':"coalesce(nullif(period_end,''),potential_end)",
    'date_basis':"'Current prime performance period. Supplier RFQ date is not announced.'",'value_low':'value_low','value_median':'value_median','value_high':'value_high',
    'value_basis':"'historical_same_vehicle_or_programme_subcontract_packages'",'value_scope':"'Historical outsourced package range; potential package size, not the prime award value or company workshare'",
    'value_inputs_json':"to_json(struct_pack(new_prime_evidence:=new_prime_evidence,new_prime_award_key:=award_key,new_prime_observed_net:=new_prime_observed_net,prior_awards:=prior_awards,source_reports:=source_reports,support_digest:=support_digest,first_award_date:=first_action_date,scope_key:=scope_key,preview:=json(preview)))",
    'source_name':"'native_awards_and_subcontract_reports'",'source_url':'source_url','source_evidence_id':'new_prime_evidence','source_locator':'award_key','native_identifier':'piid',
    'procurement_state':"'new_prime_funded_prior_outsourcing_observed'",'reconciled_state':"'supplier_placement_not_established'",'access_state':"'Prime supplier entry, configuration and package availability require confirmation'",
    'rationale':"'A newly awarded, funded prime contract shares an exact parent vehicle or reported programme with earlier contracts where this prime outsourced the stated work. The linked reports show suppliers and historical package amounts.'",
    'limitations':"'Historical outsourced scope does not prove the new package is unplaced. Prime funding is context, never the supplier opportunity value. Confirm work configuration, procurement site, qualification and sourcing timetable.'"}
    projection=','.join('cast('+mapping.get(n,'NULL')+' AS '+t+') "'+n+'"' for n,t in columns)
    output={}
    def emit(name,sql):
        path=folder/(name+'.parquet');c.execute("COPY ("+sql+") TO '"+str(path).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)")
        sha=hashlib.sha256(path.read_bytes()).hexdigest();count=c.execute('SELECT count(*) FROM read_parquet(?)',[str(path)]).fetchone()[0]
        output[name]=dict(path=str(path),files=[dict(path=str(path),sha256=sha)],sha256=sha,rows=count,as_of=as_of,kind='native_warehouse_derived_demand',status='SUCCEEDED');print(name,count,flush=True)
    emit('demand_candidates',"SELECT * FROM demand_candidates WHERE lane<>'new_prime_historical_supplier_flow' UNION ALL SELECT "+projection+' FROM pull')
    # Published requirements can use these as observed comparators. The join
    # remains exact normalised technical scope + work mode + buyer, not NAICS.
    emit('contract_value_cohorts',"""SELECT scope_key,work_mode,buyer_office_code,buyer_name,buyer_office,psc,count(*) awards,
    min(observed_net_obligations) observed_min_usd,median(observed_net_obligations) observed_median_usd,max(observed_net_obligations) observed_max_usd,
    to_json(list(struct_pack(award_key:=award_key,piid:=piid,parent_piid:=parent_piid,evidence_id:=evidence_id,source_url:=source_url,work_text:=work_text,observed_net_obligations:=observed_net_obligations,first_action_date:=first_action_date,latest_action_date:=latest_action_date,period_start:=period_start,period_end:=period_end,action_count:=action_count))) comparators_json
    FROM awards WHERE scope_key<>'' AND observed_net_obligations>0 AND try_cast(substr(latest_action_date,1,10) AS DATE)>=DATE '"""+as_of+"""'-INTERVAL 5 YEAR
    GROUP BY scope_key,work_mode,buyer_office_code,buyer_name,buyer_office,psc""")
    c.close();return output
