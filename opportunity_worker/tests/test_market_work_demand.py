import datetime,json
from opportunity_engine.market_demand import scenario,make_opportunity
from opportunity_engine.rules import evaluate
from opportunity_engine.market_history import history_sql,recurrence_sql,capability_sql

def example(**overrides):
    r=dict(cohort_id='a'*64,source_kind='prime_award',customer_name='Navy / Naval Research Laboratory',customer_key='NRL',work_text='Airborne radar signal processing software engineering',scope_key='airborne|radar|signal|processing|software|engineering',work_mode='services',capability_id='b'*64,psc='D302',naics='541330',programme='Radar',first_event='2023-08-01',last_event='2026-07-01',history_cutoff='2026-08-03',awards=9,buying_months=9,suppliers=3,completed_awards=6,observed_p10=1000000,observed_median=2000000,observed_p90=4000000,completed_p10=1000000,completed_median=2000000,completed_p90=3000000,evidence_digest='c'*64,comparators_json=json.dumps([dict(evidence_id='d'*64,source_url='https://www.usaspending.gov/award/one')]),gap_median=120,annual_award_rate=3)
    r.update(overrides);return r

def test_recurring_value_is_bounded_horizon_not_company_share():
    v=scenario(example(),'2026-11-01')
    assert v['next_12_month_median']==6000000
    assert v['next_36_month_median']==18000000
    assert v['next_36_month_low']<=v['next_36_month_median']<=v['next_36_month_high']
    assert v['company_addressable_value'] is None and v['solicitation_date_estimate'] is None
    assert datetime.date.fromisoformat(v['expected_next_purchase_date'])>=datetime.date(2026,11,1)

def test_no_future_value_from_incomplete_contracts_or_sub_report_cadence():
    for row in (example(completed_awards=2),example(source_kind='subcontract_report'),example(completed_median=None)):
        v=scenario(row,'2026-11-01');assert not v['value_estimate_supported'];assert v['next_36_month_median'] is None

def test_recurrence_has_stable_identity_and_versioned_inputs():
    a=make_opportunity(example(),'2026-11-01');b=make_opportunity(example(awards=10),'2026-11-01')
    assert a['episode_id']==b['episode_id'] and a['record_hash']!=b['record_hash']
    assert a['value_median']==18000000
    d=evaluate(a,'2026-11-01');assert d['stage']=='expected_demand' and d['rule_id']=='PR12'
    assert not d['response_window_verified'] and d['company_addressable_value'] is None
    assert d['source_basis']['comparator_evidence_ids']==['d'*64]

def test_subcontract_values_stay_package_benchmarks():
    x=make_opportunity(example(source_kind='subcontract_report'),'2026-11-01')
    assert x['lane']=='recurring_subcontract_work'
    assert x['value_basis']=='historical_recurring_package_benchmark' and x['value_median']==2000000
    assert json.loads(x['value_inputs_json'])['next_36_month_median'] is None

def test_continuation_is_not_announced_recompete_or_future_contract_value():
    r=example(record_id='old-contract',amount=27000000,amount_basis='observed_net_award_obligations',period_start='2024-01-01',period_end='2027-01-01',potential_end='2030-01-01',evidence_id='d'*64,source_url='https://www.usaspending.gov/award/one')
    x=make_opportunity(r,'2026-11-01',True);d=evaluate(x,'2026-11-01')
    assert x['window_end']=='2030-01-01' and x['value_basis']=='historical_funded_package_benchmark'
    assert d['stage']=='expected_demand' and not d['response_window_verified']
    assert d['value_interpretation']=='historical_package_benchmark'

def test_athena_history_queries_cover_prime_and_subcontract_without_company_sample():
    s=history_sql('db.awards','db.subs','2026-11-01')
    assert 'db.awards' in s and 'db.subs' in s and 'reported_subcontract_package_amount' in s
    assert 'reported_ceiling' not in s and 'LIMIT' not in s.upper()
    s=recurrence_sql('db.history','2026-11-01')
    assert "INTERVAL '3' YEAR" in s and '>=365' in s and 'completed_awards' in s
    assert 'LIMIT' not in s.upper()

def test_full_augmentation_links_only_evidenced_sites_and_profiles_all_identities(tmp_path,monkeypatch):
    import duckdb,hashlib
    import opportunity_engine.market_demand as mod
    c=duckdb.connect();tables={};sources={}
    schemas={
      'opportunities':dict(episode_id='VARCHAR',record_hash='VARCHAR',lane='VARCHAR',title='VARCHAR',scope_text='VARCHAR',customer_name='VARCHAR',programme='VARCHAR',psc='VARCHAR',naics='VARCHAR',window_start='VARCHAR',window_end='VARCHAR',date_basis='VARCHAR',value_low='DOUBLE',value_median='DOUBLE',value_high='DOUBLE',value_basis='VARCHAR',value_scope='VARCHAR',value_inputs_json='VARCHAR',source_name='VARCHAR',source_url='VARCHAR',source_evidence_id='VARCHAR',source_locator='VARCHAR',native_identifier='VARCHAR',procurement_state='VARCHAR',reconciled_state='VARCHAR',access_state='VARCHAR',rationale='VARCHAR',limitations='VARCHAR'),
      'demand_decisions':{k:'VARCHAR' for k in ('episode_id','source_record_hash','rule_id','decision_json','stage','decision_hash')},
      'company_profiles':{k:'VARCHAR' for k in ('company_id','company_name','profile_hash')},
      'company_registry':{k:'VARCHAR' for k in ('company_id','company_name','coverage_state','cage_code','parent_cage','dla_last_update')},
      'company_matches':dict(**{k:'VARCHAR' for k in ('match_id','company_id','episode_id','match_route','matched_terms','company_evidence_caveat','sample_evidence_id','match_hash','opportunity_hash','profile_hash')},score='DOUBLE'),
      'company_work_matches':dict(**{k:'VARCHAR' for k in ('match_id','company_id','episode_id','match_route','matched_terms','sample_evidence_id','match_hash','opportunity_hash','profile_hash','doc_id','work_text','source_kind','last_observed','company_entry_status','review_category','fit_verdict','fit_reason','company_evidence_caveat')},score='DOUBLE'),
      'company_match_assessments':{k:'VARCHAR' for k in ('match_id','company_id','episode_id','opportunity_hash','profile_hash','doc_id','work_text','sample_evidence_id','source_verdict','company_entry_status','verdict','reason','policy_hash')},
      'opportunity_quality_reviews':{k:'VARCHAR' for k in ('episode_id','record_hash','verdict','reason','source_evidence_id','pre_quality_decision_json')},
    }
    for name,cols in schemas.items():
        c.execute('CREATE TABLE '+name+'('+','.join(n+' '+t for n,t in cols.items())+')')
    c.execute("INSERT INTO company_profiles VALUES ('CAGE:12345','A supplier','profilehash'),('CAGE:55555','Another service provider','otherhash')")
    c.execute("INSERT INTO company_registry VALUES ('CAGE:12345','A supplier','observed_evidence_profile','12345','54321','2026-10-01'),('CAGE:54321','Declared site','declared_classifications_only','54321','88888','2026-10-01'),('CAGE:88888','Identity site','registered_identity_only','88888',NULL,'2026-10-01'),('CAGE:99997','Cycle one','registered_identity_only','99997','99998','2026-10-01'),('CAGE:99998','Cycle two','registered_identity_only','99998','99997','2026-10-01'),('CAGE:55555','Another service provider','observed_evidence_profile','55555',NULL,'2026-10-01')")
    for name,cols in schemas.items():
        path=tmp_path/(name+'.parquet');c.execute('COPY '+name+' TO ? (FORMAT PARQUET)',[str(path)]);tables[name]=dict(path=str(path),rows=c.execute('SELECT count(*) FROM '+name).fetchone()[0],sha256=hashlib.sha256(path.read_bytes()).hexdigest(),columns=[dict(Name=k,Type='double' if v=='DOUBLE' else 'string') for k,v in cols.items()])
    for name,rows in [('market_recurring_demand',[example()]),('market_continuation_work',[]),('market_supplier_history',[dict(cohort_id='a'*64,company_id='CAGE:12345',capability_id='b'*64,source_kind='prime_award',work_text=example()['work_text'],last_observed='2026-07-01',observed_records=3,sample_evidence_id='d'*64,record_id='one',evidence_digest='e'*64)])]:
        jp=tmp_path/(name+'.json');jp.write_text(json.dumps(rows or [dict(cohort_id=None,source_kind=None,technical_terms=None,potential_end=None,period_end=None,last_observed=None,record_id=None)]));path=tmp_path/(name+'.parquet')
        c.execute('CREATE OR REPLACE TEMP TABLE fixture_source AS SELECT * FROM read_json_auto(?)'+(' WHERE false' if not rows else ''),[str(jp)]);c.execute('COPY fixture_source TO ? (FORMAT PARQUET)',[str(path)])
        sources[name]=dict(path=str(path),files=[dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest())],rows=len(rows),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),columns=[dict(Name=n,Type='string') for n,*_ in c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[str(path)]).fetchall()],table='oi_test_'+name,s3_uri='s3://fake/'+name)
    jp=tmp_path/'routes.json';jp.write_text(json.dumps([dict(cohort_id='a'*64,company_id='CAGE:55555',capability_id='b'*64,source_kind='prime_award',scope_key=example()['scope_key'],work_mode='services',psc='D302',work_text=example()['work_text'],last_observed='2026-06-01',observed_records=3,sample_evidence_id='9'*64,evidence_digest='8'*64)]));path=tmp_path/'routes.parquet'
    c.execute('CREATE TEMP TABLE route_fixture AS SELECT * FROM read_json_auto(?)',[str(jp)]);c.execute('COPY route_fixture TO ? (FORMAT PARQUET)',[str(path)])
    sources['market_capability_customer_links']=dict(path=str(path),files=[dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest())],rows=1,sha256=hashlib.sha256(path.read_bytes()).hexdigest(),columns=[dict(Name=n,Type='string') for n,*_ in c.execute('DESCRIBE route_fixture').fetchall()],table='oi_test_routes',s3_uri='s3://fake/routes')
    for n in ('market_work_history','company_capability_history'):sources[n]=dict(rows=1,sha256='f'*64,columns=[dict(Name='company_id',Type='string')],table='oi_test_'+n,s3_uri='s3://fake/'+n,files=[])
    c.close();m=dict(run_id='test_market',as_of='2026-11-01',tables=tables,sources={},rules={});(tmp_path/'manifest.json').write_text(json.dumps(m))
    def fake_refresh(aws,manifest,root):manifest['sources'].update(sources);manifest['market_demand_policy']={}
    monkeypatch.setattr(mod,'refresh_history',fake_refresh)
    out=mod.augment(tmp_path,None)
    c=duckdb.connect();assert c.execute('SELECT count(*) FROM read_parquet(?)',[out['tables']['company_matching_profiles']['path']]).fetchone()[0]==6
    assert c.execute('SELECT count(*) FROM read_parquet(?)',[out['tables']['company_matches']['path']]).fetchone()[0]==2
    assert c.execute('SELECT stage FROM read_parquet(?)',[out['tables']['demand_decisions']['path']]).fetchone()[0]=='expected_demand'
    assert c.execute('SELECT pre_quality_decision_json IS NOT NULL FROM read_parquet(?)',[out['tables']['opportunity_quality_reviews']['path']]).fetchone()[0]
    assert c.execute("SELECT resolved_registry_parent_cage FROM read_parquet(?) WHERE company_id='CAGE:12345'",[out['tables']['company_parent_resolution']['path']]).fetchone()[0]=='88888'
    assert c.execute("SELECT count(*) FROM read_parquet(?) WHERE resolution_status='cycle_requires_review' AND resolved_registry_parent_cage IS NULL",[out['tables']['company_parent_resolution']['path']]).fetchone()[0]==2
    assert c.execute("SELECT company_entry_status FROM read_parquet(?) WHERE company_id='CAGE:55555'",[out['tables']['company_work_matches']['path']]).fetchone()[0]=='observed_capability_new_customer'
    c.close()

def test_multi_supplier_hardware_family_keeps_configuration_unresolved():
    r=example(psc='2915',work_text='Aircraft fuel pump',scope_key='aircraft|fuel|pump',suppliers=4)
    x=make_opportunity(r,'2026-11-01');assert json.loads(x['value_inputs_json'])['hardware_family_configuration_unresolved']
    x=make_opportunity(dict(r,work_text='Aircraft fuel pump NSN 2915003538074'),'2026-11-01')
    assert not json.loads(x['value_inputs_json'])['hardware_family_configuration_unresolved']


def test_clock_only_daily_rebuilds_do_not_rewrite_monthly_forecasts():
    a=make_opportunity(example(),'2026-11-01');b=make_opportunity(example(),'2026-11-20')
    assert a['episode_id']==b['episode_id'] and a['record_hash']==b['record_hash']
    assert scenario(example(),'2026-11-01')['next_36_month_median']==scenario(example(),'2026-11-20')['next_36_month_median']
    assert recurrence_sql('db.history','2026-11-01')==recurrence_sql('db.history','2026-11-20')
    assert make_opportunity(example(),'2026-12-01')['record_hash']!=a['record_hash']


def test_missing_labels_and_permalink_use_exact_held_identifiers():
    r=example(customer_name='',customer_key='W912HQ',comparators_json=json.dumps([dict(record_id='CONT_AWD_W912HQ24C0001_9700_NONE_NONE',evidence_id='d'*64)]))
    o=make_opportunity(r,'2026-11-01')
    assert o['customer_name']=='Defense buying office W912HQ'
    assert o['source_url']=='https://www.usaspending.gov/award/CONT_AWD_W912HQ24C0001_9700_NONE_NONE'
    assert evaluate(o,'2026-11-01')['stage']=='expected_demand'


def test_active_funded_performance_is_not_removed_by_action_recency():
    from opportunity_engine.market_history import continuation_sql
    s=continuation_sql('db.history','db.recurring','2026-11-01')
    assert "try_cast(substr(period_end,1,10) AS date)>=DATE '2026-11-01' OR" in s
    h=history_sql('db.awards','db.subs','2026-11-01')
    assert "OR try_cast(substr(period_end,1,10) AS date)>=DATE '2026-11-01'" in h
