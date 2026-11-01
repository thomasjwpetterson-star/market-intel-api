"""Materialise whole-market recurring and follow-on demand into the live contract.

The market is generated first. Company links require observed work in that
specific customer/scope stream. No source opening or technical approval is
invented, and benchmarks never become orders or company-addressable revenue.
"""
import copy,datetime,hashlib,json,math,re,uuid
from pathlib import Path
import duckdb
from .aws import BUCKET,PREFIX,DATABASE
from . import rules
from .market_history import POLICY,refresh as refresh_history

LANES=('recurring_government_work','recurring_subcontract_work','funded_contract_follow_on')

def digest(x):return hashlib.sha256(json.dumps(x,sort_keys=True,default=str).encode()).hexdigest()
def clean(x):
    import html
    return re.sub(r'\s+',' ',html.unescape(re.sub('<[^>]+>',' ',str(x or '')))).strip()
def number(x):
    try:v=float(x);return v if math.isfinite(v) else None
    except (ValueError,TypeError):return None

def scenario(row,as_of):
    """Timing and values are explicit steady-buying assumptions, not calibrated probabilities."""
    today=datetime.date.fromisoformat(as_of).replace(day=1);gap=max(1,round(number(row.get('gap_median')) or 365));last=datetime.date.fromisoformat(str(row['last_event'])[:10])
    cycles=max(1,math.ceil((today-last).days/gap));next_day=last+datetime.timedelta(days=cycles*gap)
    if next_day<today:next_day=today
    rate=number(row.get('annual_award_rate'));completed=int(row.get('completed_awards') or 0)
    # Reported subcontract package amounts lack a dependable performance period:
    # retain their historical size range, without treating report cadence as an
    # annual subcontract-revenue forecast.
    eligible=row['source_kind']=='prime_award' and completed>=3 and rate and rate>0
    values={k:number(row.get('completed_'+k)) for k in ('p10','median','p90')}
    eligible=bool(eligible and all(v and v>0 for v in values.values()))
    return dict(forecast_reference_month=today.isoformat()[:7],expected_next_purchase_date=str(next_day),window_start=str(max(today,next_day-datetime.timedelta(days=max(30,gap//2)))),window_end=str(today+datetime.timedelta(days=1096)),
                expected_annual_awards=rate,expected_purchase_interval_days=gap,
                next_12_month_low=rate*values['p10'] if eligible else None,next_12_month_median=rate*values['median'] if eligible else None,next_12_month_high=rate*values['p90'] if eligible else None,
                next_36_month_low=3*rate*values['p10'] if eligible else None,next_36_month_median=3*rate*values['median'] if eligible else None,next_36_month_high=3*rate*values['p90'] if eligible else None,
                value_estimate_supported=eligible,company_addressable_value=None,solicitation_date_estimate=None,
                assumptions=['Buying frequency continues at the observed rate over the stated horizon.','Future package mix and completed historical package sizes remain comparable.','No inflation, growth, win probability or supplier workshare is assumed.','Existing contracts, published requirements and this buying stream may overlap; values must not be added until reconciled.'],
                limitations=['The forecast concerns future customer buying, not a promised solicitation or an award to any company.','Historical amounts use the latest held award revisions; they are not reconstructed point-in-time estimates.','Quantity/configuration and contract duration may differ.'])

def make_opportunity(row,as_of,continuation=False):
    cohort=row['cohort_id'];scope=clean(row['work_text']);kind=row.get('source_kind');preview=json.loads(row.get('comparators_json') or '[]')
    # Athena CAST(ROW AS JSON) serialises named ROWs as arrays in some engines.
    if preview and isinstance(preview[0],list):
        keys=('record_id','company_id','company_name','evidence_id','source_url','work_text','event_date','period_start','period_end','amount','amount_basis','parent_award_key','parent_piid')
        preview=[dict(zip(keys,p)) for p in preview]
    if continuation:
        lane='funded_contract_follow_on';inputs=dict(cohort_id=cohort,record_id=row['record_id'],incumbent_company_id=row.get('company_id'),historical_package_benchmark=number(row.get('amount')),amount_basis=row.get('amount_basis'),period_start=row.get('period_start'),period_end=row.get('period_end'),potential_end=row.get('potential_end'),comparators=[row],company_addressable_value=None,solicitation_date_estimate=None)
        end=str(row.get('potential_end') or row.get('period_end'))[:10];start=str(max(datetime.date.fromisoformat(as_of).replace(day=1),datetime.date.fromisoformat(end)-datetime.timedelta(days=730)))
        title='Potential follow-on: '+scope[:145];basis='historical_funded_package_benchmark';lo=mid=hi=number(row.get('amount'))
        rationale='This customer has funded the stated work at an identified supplier, and the held contract records a future final performance/option end. This supports account planning for possible follow-on work; a replacement procurement is not confirmed.'
        timing='Planning window up to the stated potential contract end; not a solicitation date. Option exercise, extension, cancellation and successor awards can change it.'
    else:
        lane='recurring_subcontract_work' if kind=='subcontract_report' else 'recurring_government_work';s=scenario(row,as_of)
        inputs=dict(s,cohort_id=cohort,customer_key=row.get('customer_key'),capability_id=row.get('capability_id'),history_cutoff=row['history_cutoff'],first_observed=row['first_event'],last_observed=row['last_event'],historical_awards=int(row['awards']),historical_buying_months=int(row['buying_months']),historical_suppliers=int(row['suppliers']),completed_historical_awards=int(row.get('completed_awards') or 0),evidence_digest=row['evidence_digest'],comparators=preview,historical_package_low=number(row.get('observed_p10')),historical_package_median=number(row.get('observed_median')),historical_package_high=number(row.get('observed_p90')))
        title=('Recurring subcontract demand: ' if kind=='subcontract_report' else 'Expected repeat buying: ')+scope[:145];start=s['window_start'];end=s['window_end'];basis='estimated_recurring_buying_flow_36m' if s['value_estimate_supported'] else 'historical_recurring_package_benchmark'
        lo=s['next_36_month_low'] if s['value_estimate_supported'] else number(row.get('observed_p10'));mid=s['next_36_month_median'] if s['value_estimate_supported'] else number(row.get('observed_median'));hi=s['next_36_month_high'] if s['value_estimate_supported'] else number(row.get('observed_p90'))
        rationale=f"The same customer bought this evidenced work across {row['awards']} historical packages and {row['buying_months']} buying months. Timing uses the observed purchase intervals. Prior suppliers establish company relevance; future work remains conditional."
        timing='Estimated repeat-buying window from observed award cadence, with a 90-day reporting-lag allowance. No solicitation date is announced.'
    eid=digest({'model':POLICY,'lane':lane,'cohort':cohort});source_evidence=row.get('evidence_digest') or row.get('evidence_id');url=row.get('source_url') or (preview[0].get('source_url') if preview else None)
    hardware_family=bool(re.fullmatch(r'[0-9]{4}',str(row.get('psc') or ''))) and not continuation and int(row.get('suppliers') or 0)>1
    exact_item=bool(re.search(r'\b[0-9]{13}\b|\b(?:NSN|NIIN|P/?N|PART(?: NUMBER| NO\.?)?)\s*[:#]?\s*(?=[A-Z0-9/-]*[0-9])[A-Z0-9][A-Z0-9/-]{3,}',scope,re.I))
    inputs['hardware_family_configuration_unresolved']=hardware_family and not exact_item
    inputs.update(source_policy=POLICY,source_kind=kind,capability_id=row.get('capability_id'),scope_key=row.get('scope_key'),work_mode=row.get('work_mode'),overlap_status='requires_notice_and_contract_reconciliation',funded_business_not_unplaced_work=True)
    base=dict(episode_id=eid,lane=lane,title=title,scope_text=scope,customer_name=row['customer_name'],programme=row.get('programme'),psc=row.get('psc'),naics=row.get('naics'),window_start=start,window_end=end,date_basis=timing,value_low=lo,value_median=mid,value_high=hi,value_basis=basis,value_scope='Conditional gross buying flow over 36 months' if basis=='estimated_recurring_buying_flow_36m' else 'Historical package size guide; future contract amount and company workshare are not established',value_inputs_json=json.dumps(inputs,sort_keys=True,default=str),source_name='market_work_history',source_url=url,source_evidence_id=source_evidence,source_locator=cohort,native_identifier=cohort,procurement_state='future_buying_inferred_from_observed_work',reconciled_state='future_procurement_not_confirmed',access_state='Observed prior supplier relationship; current scope, eligibility, configuration and availability require confirmation',notice_posted_date=None,notice_type=None,rationale=rationale,limitations='Historical demand does not establish unplaced work, a new-source qualification route or a future award to the incumbent. Inspect the linked packages, timing assumptions and successor evidence.')
    base['record_hash']=digest(base);return base

def augment(directory,aws,prepared_key=None):
    r=Path(directory);m=json.loads((r/'manifest.json').read_text());as_of=m['as_of']
    if prepared_key:
        if not re.fullmatch(re.escape(PREFIX)+r'/sources/[A-Za-z0-9_/-]+/manifest\.json',prepared_key):raise ValueError('Invalid prepared market binding')
        p=json.loads(aws.s3.get_object(Bucket=BUCKET,Key=prepared_key)['Body'].read())
        if p['as_of']!=as_of or p['parent_facts_sha256']!={n:m['tables'][n]['sha256'] for n in ('company_award_facts','company_subcontract_facts')}:raise ValueError('Prepared market inputs do not match the current award/report population')
        m['sources'].update(p['sources']);m['market_demand_policy']=p['policy']
        # Reuse source bytes only when the current transformation is identical.
        # Small changed projections are rebuilt against the same pinned full history.
        from .market_history import recurrence_sql,continuation_sql
        from .distributed_history import remote
        history=DATABASE+'.'+m['sources']['market_work_history']['table']
        for name,sql in (('market_recurring_demand',recurrence_sql(history,as_of)),('market_continuation_work',continuation_sql(history,DATABASE+'.'+m['sources']['market_recurring_demand']['table'],as_of))):
            if m['sources'][name].get('query_sha256')!=hashlib.sha256(sql.encode()).hexdigest():
                m['sources'][name]=remote(aws,name,sql,'market_prepared_refresh_'+uuid.uuid4().hex[:8],as_of)
        for name in ('market_recurring_demand','market_continuation_work','market_supplier_history'):
            for i,f in enumerate(m['sources'][name]['files']):
                dest=r/(name+'_'+str(i)+'.parquet');aws.s3.download_file(BUCKET,f['s3_uri'].split('/',3)[3],str(dest))
                if hashlib.sha256(dest.read_bytes()).hexdigest()!=f['sha256']:raise ValueError('Prepared market file content changed')
                f['path']=str(dest)
    else:refresh_history(aws,m,r/'market_sources')
    from .market_history import capability_customer_routes
    if all(m['sources'].get(n,{}).get('table') for n in ('market_work_history','company_capability_history','market_recurring_demand')) and aws is not None:
        capability_customer_routes(aws,m,r/'market_customer_routes')
    c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='5GB';SET preserve_insertion_order=false");c.execute("SET temp_directory='"+str(r/'market_tmp').replace("'","''")+"'")
    names=('opportunities','demand_decisions','company_profiles','company_registry','company_matches','company_work_matches','company_match_assessments','opportunity_quality_reviews')
    for n in names:
        if n in m['tables']:c.read_parquet(m['tables'][n]['path']).create_view('old_'+n)
    for name in ('market_recurring_demand','market_continuation_work','market_supplier_history'):
        c.read_parquet([f['path'] for f in m['sources'][name]['files']]).create_view(name)
    if 'market_capability_customer_links' in m['sources']:
        c.read_parquet([f['path'] for f in m['sources']['market_capability_customer_links']['files']]).create_view('market_capability_customer_links')
    # One planning stream per buyer/scope, with every underlying contract kept
    # in normalized history. Avoid presenting ten active orders as ten recompetes.
    c.execute("CREATE TEMP VIEW continuations AS SELECT * FROM market_continuation_work QUALIFY row_number() OVER(PARTITION BY cohort_id ORDER BY coalesce(nullif(potential_end,''),period_end),last_observed DESC,record_id)=1")
    path=r/'market_opportunities.jsonl';count=0
    with path.open('w') as f:
        for table,continuation in (('market_recurring_demand',False),('continuations',True)):
            cur=c.execute('SELECT * FROM '+table);keys=[d[0] for d in cur.description]
            while True:
                rows=cur.fetchmany(1000)
                if not rows:break
                for values in rows:
                    row=dict(zip(keys,values))
                    if re.match(r'^[A-Z]',str(row.get('psc') or '')) and len(str(row.get('scope_key') or '').split('|'))<3:continue
                    o=make_opportunity(row,as_of,continuation);f.write(json.dumps(o,default=str)+'\n');count+=1
    if count==0:raise ValueError('The full-market history produced no demand records')
    c.read_json(str(path),format='newline_delimited',sample_size=-1).create_view('market_opportunities')
    def output(name,sql):
        c.execute('CREATE OR REPLACE TEMP TABLE output_result AS '+sql);p=r/(name+'.market.parquet')
        cols=[]
        for n,t,*_ in c.execute('DESCRIBE output_result').fetchall():
            typ='VARCHAR' if t=='JSON' else 'BIGINT' if t in ('HUGEINT','UBIGINT') else 'DOUBLE' if t.startswith('DECIMAL') else t
            if typ!=t:c.execute('ALTER TABLE output_result ALTER COLUMN "'+n+'" TYPE '+typ)
            cols.append(dict(Name=n,Type={'VARCHAR':'string','BIGINT':'bigint','INTEGER':'bigint','DOUBLE':'double','FLOAT':'double','BOOLEAN':'boolean'}.get(typ,'string')))
        c.execute("COPY output_result TO '"+str(p)+"' (FORMAT PARQUET,COMPRESSION ZSTD)");n=c.execute('SELECT count(*) FROM output_result').fetchone()[0]
        m['tables'][name]=dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),rows=n,columns=cols);c.read_parquet(str(p)).create_view('new_'+name,replace=True);print('Market demand output',name,n,flush=True)
    def project(table,expressions):
        return ','.join('cast('+expressions.get(n,'NULL')+' AS '+t+') "'+n+'"' for n,t,*_ in c.execute('DESCRIBE '+table).fetchall())
    from .demand_progression import prepare as prepare_progression
    path=prepare_progression(c,m,r,aws,path,output)
    lane_list=','.join("'"+x+"'" for x in LANES)
    cols={n:'x."'+n+'"' for n,*_ in c.execute('DESCRIBE market_opportunities').fetchall()}
    output('opportunities','SELECT * FROM old_opportunities WHERE lane NOT IN ('+lane_list+') UNION ALL SELECT '+project('old_opportunities',cols)+' FROM market_opportunities x')
    # Use the versioned rule registry. Market stages explicitly preserve the
    # distinction between inferred future buying and published participation.
    dec=r/'market_decisions.jsonl'
    with dec.open('w') as f:
        for line in path.open():
            o=json.loads(line);d=rules.evaluate(o,as_of);f.write(json.dumps(dict(episode_id=o['episode_id'],source_record_hash=o['record_hash'],rule_id=d['rule_id'],stage=d['stage'],decision_hash=d['decision_hash'],decision_json=json.dumps(d,sort_keys=True)))+'\n')
    c.read_json(str(dec),format='newline_delimited',sample_size=-1).create_view('market_decisions')
    output('demand_decisions','SELECT * FROM old_demand_decisions WHERE episode_id NOT IN (SELECT episode_id FROM old_opportunities WHERE lane IN ('+lane_list+')) UNION ALL SELECT '+project('old_demand_decisions',{n:'x."'+n+'"' for n,*_ in c.execute('DESCRIBE market_decisions').fetchall()})+' FROM market_decisions x')
    # Historical supply establishes a specific incumbent/account relationship.
    # It is not a new-source approval and is never inherited by sister sites.
    c.execute("""CREATE TEMP TABLE market_links AS SELECT s.*,o.episode_id,o.record_hash,p.profile_hash,p.company_name,'prior_customer_work' relationship_kind,
    sha256('market-observed-supplier-v1|'||s.company_id||'|'||o.episode_id) match_id,
    sha256(concat_ws('|',s.company_id,o.record_hash,s.evidence_digest,p.profile_hash)) match_hash
    FROM market_supplier_history s JOIN market_opportunities o ON json_extract_string(o.value_inputs_json,'$.cohort_id')=s.cohort_id
    JOIN old_company_profiles p ON p.company_id=s.company_id
    WHERE (o.lane<>'funded_contract_follow_on' OR s.company_id=json_extract_string(o.value_inputs_json,'$.incumbent_company_id')) AND s.observed_records>=CASE WHEN o.lane='funded_contract_follow_on' THEN 1 ELSE 2 END
    AND length(trim(s.work_text))>=8""")
    if 'market_capability_customer_links' in m['sources']:
        alt=dict(cohort_id='a.cohort_id',company_id='a.company_id',capability_id='a.capability_id',source_kind='a.source_kind',scope_key='a.scope_key',work_mode='a.work_mode',psc='a.psc',work_text='a.work_text',last_observed='a.last_observed',observed_records='a.observed_records',sample_evidence_id='a.sample_evidence_id',record_id="sha256('market-capability-doc-v1|'||a.company_id||'|'||a.capability_id)",evidence_digest='a.evidence_digest',episode_id='o.episode_id',record_hash='o.record_hash',profile_hash='p.profile_hash',company_name='p.company_name',relationship_kind="'same_capability_other_customer'",match_id="sha256('market-capability-customer-v1|'||a.company_id||'|'||o.episode_id)",match_hash="sha256(concat_ws('|',a.company_id,o.record_hash,a.evidence_digest,p.profile_hash))")
        c.execute('INSERT INTO market_links SELECT '+project('market_links',alt)+" FROM market_capability_customer_links a JOIN market_opportunities o ON json_extract_string(o.value_inputs_json,'$.cohort_id')=a.cohort_id JOIN old_company_profiles p ON p.company_id=a.company_id WHERE NOT EXISTS(SELECT 1 FROM market_links own WHERE own.company_id=a.company_id AND own.episode_id=o.episode_id)")
    common=dict(match_id='x.match_id',company_id='x.company_id',episode_id='x.episode_id',score='90.0',match_route="CASE WHEN x.relationship_kind='same_capability_other_customer' THEN 'market-capability-customer-v1' ELSE 'market-observed-supplier-v1' END",matched_terms="'Observed supplier in this exact customer and normalized work stream'",company_evidence_caveat="CASE WHEN x.relationship_kind='same_capability_other_customer' THEN 'This site has delivered the same normalized service scope and work mode to another customer. This is a capability-fit buying prospect; future procurement, eligibility and available work are not confirmed.' ELSE 'This site has delivered the evidenced work to this customer. This is a conditional continuation prospect, not proof of current qualification, an open competition or available supplier work.' END",sample_evidence_id='x.sample_evidence_id',match_hash='x.match_hash',opportunity_hash='x.record_hash',profile_hash='x.profile_hash')
    retain=' WHERE episode_id NOT IN (SELECT episode_id FROM old_opportunities WHERE lane IN ('+lane_list+'))'
    output('company_matches','SELECT * FROM old_company_matches'+retain+' UNION ALL SELECT '+project('old_company_matches',common)+' FROM market_links x')
    work=dict(common,doc_id='x.record_id',work_text='x.work_text',source_kind='x.source_kind',last_observed='x.last_observed',work_context="'Exact historical customer/scope; underlying awards and reports are linked'",company_entry_status="CASE WHEN x.relationship_kind='same_capability_other_customer' THEN 'observed_capability_new_customer' ELSE 'observed_same_scope_supplier' END",review_category="CASE WHEN coalesce(try_cast(json_extract_string(o.value_inputs_json,'$.hardware_family_configuration_unresolved') AS BOOLEAN),false) THEN 'market_context' ELSE 'work_lead' END",fit_verdict="CASE WHEN coalesce(try_cast(json_extract_string(o.value_inputs_json,'$.hardware_family_configuration_unresolved') AS BOOLEAN),false) THEN 'review' ELSE 'accepted' END",fit_reason="CASE WHEN x.relationship_kind='same_capability_other_customer' THEN 'Observed service deliveries match the same normalized scope and work mode at another customer. Future buying and notice-specific eligibility are unconfirmed.' ELSE 'Exact historical supplier/customer/work relationship supports retention and account planning. New procurement and current access remain unconfirmed.' END",observed_work_psc="NULL",required_delivery_mode="'not_established'",observed_delivery_mode="'not_established'",shared_scope_terms_json="'[\"observed customer\",\"observed work\"]'")
    output('company_work_matches','SELECT * FROM old_company_work_matches'+retain+' UNION ALL SELECT '+project('old_company_work_matches',work)+' FROM market_links x JOIN market_opportunities o USING(episode_id)')
    assessments=dict(work,verdict=work['fit_verdict'],reason=work['fit_reason'],source_verdict="'usable'",policy_hash="'"+POLICY+"'",original_company_evidence_caveat=common['company_evidence_caveat'])
    output('company_match_assessments','SELECT * FROM old_company_match_assessments'+retain+' UNION ALL SELECT '+project('old_company_match_assessments',assessments)+' FROM market_links x JOIN market_opportunities o USING(episode_id)')
    quality=dict(episode_id='x.episode_id',record_hash='x.record_hash',source_evidence_id='x.source_evidence_id',source_url='x.source_url',verdict="'usable'",reason="'Source-backed historical demand inference; future procurement is not announced'",passages_json="'[]'",source_entry_kind="'historical_customer_work'",category_override="'work'",scope_has_item_identifier='false',restricted_item='false',qualification_invitation='false',source_cages_json="'[]'",required_parts_json="'[]'",required_delivery_mode="'not_established'",policy_hash="'"+POLICY+"'",pre_quality_decision_json='d.decision_json')
    output('opportunity_quality_reviews','SELECT * FROM old_opportunity_quality_reviews'+retain+' UNION ALL SELECT '+project('old_opportunity_quality_reviews',quality)+' FROM market_opportunities x JOIN market_decisions d USING(episode_id)')
    # Every held CAGE identity gets a matching profile with explicit readiness;
    # classification-only identities never become claimed delivered capabilities.
    registry_columns={n for n,*_ in c.execute('DESCRIBE old_company_registry').fetchall()}
    if 'parent_cage' in registry_columns:
        c.execute("""CREATE TEMP TABLE parent_edges AS SELECT cage_code,upper(trim(parent_cage)) parent_cage,dla_last_update
        FROM old_company_registry WHERE regexp_full_match(upper(trim(parent_cage)),'[A-Z0-9]{5}')
        AND upper(trim(parent_cage)) NOT IN ('00000','99999','XXXXX','ZZZZZ') AND upper(trim(parent_cage))<>cage_code""")
        output('company_parent_resolution',"""WITH RECURSIVE chain AS(
        SELECT cage_code, parent_cage current_cage,[cage_code,parent_cage] chain_path,1 depth,false cycle_detected,dla_last_update
        FROM parent_edges
        UNION ALL SELECT c.cage_code,e.parent_cage,list_append(c.chain_path,e.parent_cage),c.depth+1,list_contains(c.chain_path,e.parent_cage),c.dla_last_update
        FROM chain c JOIN parent_edges e ON e.cage_code=c.current_cage WHERE c.depth<16 AND NOT c.cycle_detected)
        SELECT 'CAGE:'||c.cage_code company_id,CASE WHEN c.cycle_detected OR c.depth=16 AND n.cage_code IS NOT NULL THEN NULL ELSE c.current_cage END resolved_registry_parent_cage,
        CASE WHEN c.cycle_detected THEN 'cycle_requires_review' WHEN c.depth=16 AND n.cage_code IS NOT NULL THEN 'depth_limit_requires_review' WHEN r.cage_code IS NULL THEN 'chain_ends_at_unheld_reference' ELSE 'held_registered_parent_chain' END resolution_status,
        c.depth,to_json(c.chain_path) chain_path_json,c.dla_last_update,
        'Current registered parent-CAGE chain. Source dates remain attached. This is an organizational reference, not inherited site capability, qualification or retrospective ownership.' interpretation
        FROM chain c LEFT JOIN parent_edges n ON n.cage_code=c.current_cage LEFT JOIN old_company_registry r ON r.cage_code=c.current_cage
        QUALIFY row_number() OVER(PARTITION BY c.cage_code ORDER BY c.depth DESC,c.current_cage)=1""")
    else:
        c.execute("CREATE TEMP VIEW new_company_parent_resolution AS SELECT NULL::VARCHAR company_id,NULL::VARCHAR resolved_registry_parent_cage,NULL::VARCHAR resolution_status,NULL::VARCHAR chain_path_json WHERE false")
    output('company_matching_profiles',"""SELECT r.*,p.profile_hash,parent.resolved_registry_parent_cage,parent.resolution_status parent_resolution_status,parent.chain_path_json parent_chain_path_json,
    CASE WHEN p.company_id IS NOT NULL THEN 'observed_work_product_or_research' WHEN r.coverage_state='declared_classifications_only' THEN 'declared_only_requires_delivery_evidence' ELSE 'identity_only_requires_enrichment' END matching_readiness,
    'company_capability_history' capability_evidence_table,'"""+as_of+"""' assessed_as_of
    FROM old_company_registry r LEFT JOIN old_company_profiles p ON p.company_id=r.company_id LEFT JOIN new_company_parent_resolution parent ON parent.company_id=r.company_id""")
    for name in ('market_work_history','company_capability_history','market_recurring_demand','market_continuation_work','market_supplier_history','market_recurrence_training','market_recurrence_validation','market_capability_customer_links'):
        if name not in m['sources']:continue
        source=m['sources'][name];m['tables'][name]=dict(rows=source['rows'],sha256=source['sha256'],columns=source['columns'],remote_binding=True,table=source['table'],s3_uri=source['s3_uri'],source_binding=name)
    m['rules']=rules.catalogue();m['market_demand_policy']['generated_opportunities']=count;m['market_demand_policy']['company_links']=c.execute('SELECT count(*) FROM market_links').fetchone()[0]
    (r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close();return m


def refresh_current(previous,root,aws,prepared_key=None):
    root=Path(root);r=root/'runs'/(datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')+'_'+uuid.uuid4().hex[:6]);r.mkdir(parents=True);m=copy.deepcopy(previous)
    m.update(run_id=r.name,reuse_parent_release=previous['run_id'],refresh_mode='whole_market_work_demand')
    for name,t in m['tables'].items():
        t['reuse_parent_binding']=True
        if t.get('remote_binding'):continue
        uri=next(f['s3_uri'] for f in m['sources'][t['source_binding']]['files'] if f['sha256']==t['sha256']) if t.get('source_binding') else t['s3_uri']+'part-00000.parquet'
        path=r/(name+'.parquet');aws.s3.download_file(BUCKET,uri.split('/',3)[3],str(path))
        if hashlib.sha256(path.read_bytes()).hexdigest()!=t['sha256']:raise ValueError('Parent market table changed: '+name)
        t['path']=str(path)
    for name in ('contract_value_cohorts',):
        for i,f in enumerate(m['sources'].get(name,{}).get('files',[])):
            path=r/(name+'_source_'+str(i)+'.parquet');aws.s3.download_file(BUCKET,f['s3_uri'].split('/',3)[3],str(path))
            if hashlib.sha256(path.read_bytes()).hexdigest()!=f['sha256']:raise ValueError('Comparator hash mismatch')
            f['path']=str(path)
    (r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n')
    augment(r,aws,prepared_key)
    from .pipeline_coverage import build
    build(r)
    from .logic_provenance import record
    record(r)
    from .events import track
    track(r,previous,aws)
    from .validate import validate,validate_demand_integrity
    validate_demand_integrity(r);validate(r)
    return r
