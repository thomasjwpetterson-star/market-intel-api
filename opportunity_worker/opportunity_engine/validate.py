"""Release gates cover provenance, joins, financial semantics and negative states."""
import hashlib,json
from pathlib import Path
import duckdb

def validate_demand_integrity(directory):
 r=Path(directory);m=json.loads((r/'manifest.json').read_text());c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='1200MB'")
 for name in ('opportunities','demand_decisions'):c.read_parquet(m['tables'][name]['path']).create_view(name)
 checks={}
 for name in ('opportunities','demand_decisions'):
  checks[name+'_invalid_identity']=c.execute("SELECT count(*) FROM "+name+" WHERE NOT coalesce(regexp_full_match(episode_id,'[a-f0-9]{64}'),false)").fetchone()[0]
  checks[name+'_duplicates']=c.execute('SELECT count(*)-count(DISTINCT episode_id) FROM '+name).fetchone()[0]
 checks['missing_decisions']=c.execute('SELECT count(*) FROM opportunities ANTI JOIN demand_decisions USING(episode_id)').fetchone()[0]
 checks['orphan_decisions']=c.execute('SELECT count(*) FROM demand_decisions ANTI JOIN opportunities USING(episode_id)').fetchone()[0]
 checks['decision_version_mismatch']=c.execute('SELECT count(*) FROM opportunities o JOIN demand_decisions d USING(episode_id) WHERE o.record_hash IS DISTINCT FROM d.source_record_hash').fetchone()[0];c.close()
 (r/'demand_integrity.json').write_text(json.dumps(checks,indent=2)+'\n');print('Demand integrity before company matching:',json.dumps(checks),flush=True)
 failed={k:v for k,v in checks.items() if v}
 if failed:raise ValueError('Demand integrity failed before company matching: '+json.dumps(failed))
 return checks

def validate(directory):
 r=Path(directory);m=json.loads((r/'manifest.json').read_text());c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='1400MB';SET preserve_insertion_order=false");checks=[]
 def check(name,ok,detail=None):checks.append(dict(name=name,passed=bool(ok),detail=detail))
 for n,t in m['tables'].items():
  if t.get('remote_binding'):
   check(n+'_distributed_source_integrity',bool(t.get('sha256') and isinstance(t.get('rows'),int) and t['rows']>=0 and t.get('table') and t.get('columns')))
   source=m['sources'][t['source_binding']];files=source.get('files',[])
   if files and all(f.get('path') and Path(f['path']).is_file() for f in files):
    check(n+'_local_source_hashes',all(hashlib.sha256(Path(f['path']).read_bytes()).hexdigest()==f['sha256'] for f in files));c.read_parquet([f['path'] for f in files]).create_view(n);check(n+'_local_source_count',c.execute('SELECT count(*) FROM '+n).fetchone()[0]==t['rows'])
   continue
  p=Path(t['path']);check(n+'_content_hash',hashlib.sha256(p.read_bytes()).hexdigest()==t['sha256']);c.read_parquet(str(p)).create_view(n);check(n+'_count',c.execute('SELECT count(*) FROM '+n).fetchone()[0]==t['rows'])
 for n,key in [('research_awards','award_id'),('opportunities','episode_id'),('demand_decisions','episode_id'),('company_profiles','company_id')]:check(n+'_unique',c.execute('SELECT count(*)-count(DISTINCT '+key+') FROM '+n).fetchone()[0]==0)
 check('opportunity_ids_routable',c.execute("SELECT count(*) FROM opportunities WHERE NOT coalesce(regexp_full_match(episode_id,'[a-f0-9]{64}'),false)").fetchone()[0]==0)
 check('decision_population',c.execute('SELECT count(*) FROM opportunities o ANTI JOIN demand_decisions d USING(episode_id)').fetchone()[0]==0)
 check('decision_record_version',c.execute('SELECT count(*) FROM opportunities o JOIN demand_decisions d USING(episode_id) WHERE o.record_hash<>d.source_record_hash').fetchone()[0]==0)
 check('no_invented_company_value',c.execute("SELECT count(*) FROM demand_decisions WHERE json_extract_string(decision_json,'$.company_addressable_value') IS NOT NULL").fetchone()[0]==0)
 check('elapsed_not_actionable',c.execute("SELECT count(*) FROM opportunities o JOIN demand_decisions d USING(episode_id) WHERE try_cast(o.window_end AS DATE)<cast(? AS DATE) AND d.stage<>'resolved_or_elapsed'",[m['as_of']]).fetchone()[0]==0)
 check('rule_registry',len(m['rules']['rules'])==23)
 for n in ('research_prime_links','research_platform_links'):
  if n in m['tables']:check(n+'_award_integrity',c.execute('SELECT count(*) FROM '+n+' x ANTI JOIN research_awards r USING(award_id)').fetchone()[0]==0)
 if 'company_topic_matches' in m['tables']:
  check('topic_match_integrity',c.execute('SELECT count(*) FROM company_topic_matches m ANTI JOIN research_topics_current t USING(topic_id)').fetchone()[0]==0)
  check('match_is_not_qualification',c.execute("SELECT count(*) FROM company_topic_matches WHERE applicant_eligibility<>'not_established' OR technical_qualification<>'not_established' OR company_addressable_value IS NOT NULL").fetchone()[0]==0)
  check('match_has_evidence',c.execute('SELECT count(*) FROM company_topic_matches WHERE json_array_length(evidence_json)<1').fetchone()[0]==0)
  check('topic_benchmark_not_future_value',c.execute('SELECT count(*) FROM topic_historical_benchmarks WHERE estimated_future_award_value IS NOT NULL').fetchone()[0]==0)
 if 'research_topics_current' in m['tables']:
  check('topics_declared_coverage',m['tables']['research_topics_current']['rows']==m['sources']['sbir_topics']['rows'])
 if 'company_registry' in m['tables']:
  check('registry_one_row_per_cage',c.execute('SELECT count(*)-count(DISTINCT cage_code) FROM company_registry').fetchone()[0]==0)
  check('registry_valid_codes',c.execute("SELECT count(*) FROM company_registry WHERE NOT regexp_full_match(cage_code,'[A-Z0-9]{5}')").fetchone()[0]==0)
  check('registry_universe_not_shrunk_to_profile_cohort',m['tables']['company_registry']['rows']>=m['tables']['company_profiles']['rows'])
  check('identity_only_not_capability_claim',c.execute("SELECT count(*) FROM company_registry WHERE coverage_state='registered_identity_only' AND (prime_awards>0 OR subcontract_reports>0 OR research_awards>0 OR referenced_items>0)").fetchone()[0]==0)
 if 'company_award_facts' in m['tables'] and m['tables']['company_award_facts'].get('remote_binding'):
  t=m['tables']['company_award_facts'];check('native_company_award_population',c.execute('SELECT sum(prime_awards) FROM company_profiles').fetchone()[0]==t['profileable_rows'])
  observed=c.execute('SELECT sum(observed_prime_net_obligations) FROM company_profiles').fetchone()[0];check('native_company_obligations_reconcile',abs((observed or 0)-(t['profile_observed_net_total'] or 0))<1.0)
 elif 'company_award_facts' in m['tables']:
  check('native_awards_unique',c.execute('SELECT count(*)-count(DISTINCT award_key) FROM company_award_facts').fetchone()[0]==0)
  check('native_company_award_population',c.execute('SELECT sum(prime_awards) FROM company_profiles').fetchone()[0]==m['tables']['company_award_facts']['rows'])
  totals=c.execute('SELECT (SELECT sum(observed_net_obligations) FROM company_award_facts)-(SELECT sum(observed_prime_net_obligations) FROM company_profiles)').fetchone()[0]
  check('native_company_obligations_reconcile',abs(totals or 0)<1.0)
  check('native_awards_no_future_actions',c.execute("SELECT count(*) FROM company_award_facts WHERE substr(latest_action_date,1,10)>?",[m['as_of']]).fetchone()[0]==0)
 if 'contract_value_observations' in m['tables']:
  check('value_cohort_full_observation_coverage',c.execute('SELECT sum(awards) FROM contract_value_cohorts').fetchone()[0]==m['tables']['contract_value_observations']['rows'])
 if 'company_subcontract_facts' in m['tables']:
  check('native_subreports_unique',c.execute('SELECT count(*)-count(DISTINCT report_key) FROM company_subcontract_facts').fetchone()[0]==0)
  check('ambiguous_supplier_not_inherited_by_site',c.execute("SELECT count(*) FROM company_subcontract_facts WHERE site_resolution='multiple_sites_uei_only' AND starts_with(company_id,'CAGE:')").fetchone()[0]==0)
 if 'contract_value_cohorts' in m['tables']:
  check('contract_values_have_source_comparators',c.execute("SELECT count(*) FROM contract_value_cohorts WHERE json_array_length(comparators_json)>awards OR json_array_length(comparators_json)>22 OR json_array_length(comparators_json)=0 OR observed_min_usd<=0 OR observed_min_usd>observed_median_usd OR observed_median_usd>observed_max_usd").fetchone()[0]==0)
 if 'company_work_matches' in m['tables']:
  check('work_match_company_episode_unique',c.execute('SELECT count(*)-count(DISTINCT (company_id,episode_id)) FROM company_work_matches').fetchone()[0]==0)
  check('work_matches_bind_current_requirement',c.execute('SELECT count(*) FROM company_work_matches x LEFT JOIN opportunities o USING(episode_id) WHERE o.record_hash IS NULL OR x.opportunity_hash<>o.record_hash').fetchone()[0]==0)
  check('work_matches_have_delivery_evidence',c.execute("SELECT count(*) FROM company_work_matches WHERE nullif(sample_evidence_id,'') IS NULL OR nullif(work_text,'') IS NULL OR nullif(doc_id,'') IS NULL").fetchone()[0]==0)
 if 'company_item_matches' in m['tables']:
  check('catalogue_match_company_episode_unique',c.execute('SELECT count(*)-count(DISTINCT (company_id,episode_id)) FROM company_item_matches').fetchone()[0]==0)
  check('catalogue_matches_bind_current_requirement',c.execute('SELECT count(*) FROM company_item_matches x LEFT JOIN opportunities o USING(episode_id) WHERE o.record_hash IS NULL OR x.opportunity_hash<>o.record_hash').fetchone()[0]==0)
  check('catalogue_matches_have_reference',c.execute("SELECT count(*) FROM company_item_matches WHERE nullif(sample_evidence_id,'') IS NULL").fetchone()[0]==0)
 if 'company_item_valuations' in m['tables']:
  check('forecast_scenarios_unique',c.execute('SELECT count(*)-count(DISTINCT (company_id,episode_id)) FROM company_item_valuations').fetchone()[0]==0)
  check('forecast_scenarios_have_all_months',c.execute('SELECT count(*) FROM company_item_valuations WHERE json_array_length(monthly_evidence_json)<>month_count').fetchone()[0]==0)
  check('forecast_scenario_arithmetic',c.execute('SELECT count(*) FROM company_item_valuations WHERE abs(government_reference_lot_scenario-quantity*government_reference_unit_price)>0.005 OR abs(supplier_average_lot_scenario-quantity*supplier_average_unit_price)>0.005').fetchone()[0]==0)
  check('no_future_supplier_average',c.execute("SELECT count(*) FROM company_item_valuations WHERE supplier_average_lot_scenario IS NOT NULL AND substr(supplier_average_last_sale,1,10)>?",[m['as_of']]).fetchone()[0]==0)
 if 'company_opportunity_events' in m['tables']:
  check('company_events_unique',c.execute('SELECT count(*)-count(DISTINCT event_id) FROM company_opportunity_events').fetchone()[0]==0)
 check('public_not_silently_released',m['public_release_approved'] is False)
 v=dict(passed=sum(x['passed'] for x in checks),failed=sum(not x['passed'] for x in checks),checks=checks,logic_sha256=m['logic_sha256']);(r/'validation.json').write_text(json.dumps(v,indent=2)+'\n');c.close()
 if v['failed']:raise ValueError('Failed release gates: '+', '.join(x['name'] for x in checks if not x['passed']))
 return v
