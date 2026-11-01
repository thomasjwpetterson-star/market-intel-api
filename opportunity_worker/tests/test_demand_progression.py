import json,hashlib
from pathlib import Path
import duckdb
from opportunity_engine.demand_progression import contract_refs,scope_key,prepare

def test_contract_references_are_exact_tokens_not_substrings_or_nsns():
 assert contract_refs('Incumbent W56HZV-24-C-0001; NSN 6115012704801')==['W56HZV24C0001']
 assert contract_refs('XW56HZV24C0001X')==[]

def test_actual_notice_links_keep_candidate_scope_and_explicit_incumbent_distinct(tmp_path):
 c=duckdb.connect();cols='episode_id VARCHAR,record_hash VARCHAR,source_name VARCHAR,source_locator VARCHAR,title VARCHAR,scope_text VARCHAR,source_url VARCHAR,source_evidence_id VARCHAR,procurement_state VARCHAR,window_end VARCHAR'
 c.execute('CREATE TABLE calls('+cols+')');c.execute("INSERT INTO calls VALUES ('"+'b'*64+"','notice-version','sam_notices','notice1','Airborne radar signal processing software engineering','Previous contract W56HZV-24-C-0001 and W56HZV24C0001','https://sam.gov/opp/one','notice-evidence','procurement_published','2027-01-01')")
 calls=tmp_path/'calls.parquet';c.execute('COPY calls TO ? (FORMAT PARQUET)',[str(calls)])
 c.execute("CREATE TABLE sam AS SELECT 'notice1' notice_id,'NRL' aac_code");sam=tmp_path/'sam.parquet';c.execute('COPY sam TO ? (FORMAT PARQUET)',[str(sam)])
 c.execute("CREATE TABLE ref AS SELECT 'cohort' cohort_id,'old-award' record_id,'W56HZV24C0001' piid,'award-evidence' evidence_id,'https://usaspending.gov/award/old' source_url");ref=tmp_path/'reference.parquet';c.execute('COPY ref TO ? (FORMAT PARQUET)',[str(ref)])
 scope=scope_key('Airborne radar signal processing software engineering');rows=[dict(episode_id='a'*64,record_hash='before-one',lane='recurring_government_work',value_inputs_json=json.dumps(dict(scope_key=scope,customer_key='NRL'))),dict(episode_id='c'*64,record_hash='before-two',lane='funded_contract_follow_on',value_inputs_json=json.dumps(dict(record_id='old-award')))]
 path=tmp_path/'opps.jsonl';path.write_text('\n'.join(json.dumps(x) for x in rows));c.read_json(str(path),format='newline_delimited').create_view('market_opportunities')
 m=dict(run_id='test',as_of='2026-11-01',tables={'published_calls':{'path':str(calls)}},sources={'sam_notices':{'files':[dict(path=str(sam),sha256=hashlib.sha256(sam.read_bytes()).hexdigest())]},'native_company_awards':{'table':'oi_test_prime'},'market_continuation_work':{'table':'oi_test_current'}})
 class AWS:
  def extract(self,*args):return dict(files=[dict(path=str(ref),sha256=hashlib.sha256(ref.read_bytes()).hexdigest())])
 results={}
 def output(name,sql):
  c.execute('CREATE OR REPLACE TEMP TABLE result_'+name+' AS '+sql);results[name]=c.execute('SELECT * FROM result_'+name).fetchall()
 out=prepare(c,m,tmp_path,AWS(),path,output)
 links=c.execute('SELECT episode_id,relationship_kind,forecast_source_hash FROM result_market_demand_progression ORDER BY episode_id').fetchall()
 assert len(links)==2 and links[0][1]=='candidate_same_buyer_scope' and links[1][1]=='explicit_incumbent_contract_reference'
 assert all(x[2] not in ('before-one','before-two') for x in links)
 assert all(json.loads(json.loads(line)['value_inputs_json'])['related_procurement_events'] for line in out.read_text().splitlines())
 c.close()
