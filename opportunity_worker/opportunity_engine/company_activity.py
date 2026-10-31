"""Rolling business-volume evidence. A coverage benchmark is not created demand."""
import datetime,hashlib,json,re
from pathlib import Path
from .aws import DATABASE
from .distributed_history import remote

POLICY='annual-obligations-coverage-v1'

def action_query(as_of,identity_table):
 datetime.date.fromisoformat(as_of)
 if not re.fullmatch(r'market_intel_platform_candidate\.oi_[a-z0-9_]+',identity_table):raise ValueError('Invalid identity table')
 return """WITH revisions AS(
 SELECT contract_transaction_unique_key transaction_key,contract_award_unique_key award_key,award_id_piid piid,upper(trim(cage_code)) cage_code,upper(trim(recipient_uei)) uei,
 recipient_name company_name,substr(action_date,1,10) action_date,try_cast(federal_action_obligation AS double) action_obligation,awarding_agency_code,awarding_sub_agency_name customer,transaction_description work_text,usaspending_permalink source_url,
 row_number() OVER(PARTITION BY contract_transaction_unique_key ORDER BY last_modified_date DESC,ingest_ts DESC) revision
 FROM market_intel_silver.dataset_prime_contracts WHERE contract_transaction_unique_key IS NOT NULL AND contract_award_unique_key IS NOT NULL),
 actions AS(SELECT r.*,CASE WHEN regexp_like(r.cage_code,'^[A-Z0-9]{5}$') THEN 'CAGE:'||r.cage_code WHEN i.cage_code IS NOT NULL THEN 'CAGE:'||i.cage_code WHEN nullif(r.uei,'') IS NOT NULL THEN 'UEI:'||r.uei END company_id
 FROM revisions r LEFT JOIN IDENTITY_TABLE i ON i.uei=r.uei WHERE revision=1 AND try_cast(action_date AS date)>date_add('year',-1,DATE 'ASOF') AND try_cast(action_date AS date)<=DATE 'ASOF')
 SELECT transaction_key,award_key,piid,cage_code,uei,company_name,company_id,action_date,action_obligation,awarding_agency_code,customer,work_text,source_url,
 lower(to_hex(sha256(to_utf8(json_format(cast(row(transaction_key,award_key,company_id,action_date,action_obligation,work_text) AS json)))))) evidence_id FROM actions""".replace('IDENTITY_TABLE',identity_table).replace('ASOF',as_of)

def refresh(directory,aws):
 r=Path(directory);m=json.loads((r/'manifest.json').read_text());identity=m['sources'].get('native_company_identity')
 if not identity or not identity.get('table'):return
 asof=m['as_of'];source=remote(aws,'company_activity_actions',action_query(asof,DATABASE+'.'+identity['table']),m['run_id']+'_activity',asof)
 m['sources']['company_activity_actions']=source
 m['tables']['company_activity_actions']=dict(**{k:source[k] for k in ('rows','sha256','columns','table','s3_uri')},source_binding='company_activity_actions',remote_binding=True)
 table=DATABASE+'.'+source['table']
 sql="""WITH source_period AS(SELECT max(action_date) source_latest_action FROM TABLE), awards AS(SELECT company_id,award_key,sum(coalesce(action_obligation,0)) net FROM TABLE WHERE company_id IS NOT NULL GROUP BY company_id,award_key),
 sizes AS(SELECT company_id,approx_percentile(net,0.5) annual_award_net_median,max(net) annual_award_net_max FROM awards WHERE net>0 GROUP BY company_id)
 SELECT a.company_id,max_by(company_name,action_date) company_name,count(*) annual_action_count,count(DISTINCT a.award_key) annual_award_count,
 sum(coalesce(action_obligation,0)) annual_net_obligations,sum(greatest(coalesce(action_obligation,0),0)) annual_positive_obligations,
 sum(CASE WHEN awarding_agency_code='097' THEN coalesce(action_obligation,0) ELSE 0 END) annual_dod_net_obligations,
 count_if(awarding_agency_code='097') annual_dod_action_count,(SELECT source_latest_action FROM source_period) source_latest_action,date_diff('day',try_cast((SELECT source_latest_action FROM source_period) AS date),DATE 'ASOF') source_lag_days,min(action_date) first_observed_action,max(action_date) last_observed_action,
 max(s.annual_award_net_median) annual_award_net_median,max(s.annual_award_net_max) annual_award_net_max,
 cast(date_add('day',1,date_add('year',-1,DATE 'ASOF')) AS varchar) period_start,'ASOF' period_end,
 lower(to_hex(sha256(checksum(evidence_id)))) evidence_digest,
 'Rolling twelve months, latest transaction revision. Net obligations include deobligations; positive obligations are separate. All federal buyers, with DoD shown separately. Not company revenue or lifetime award value.' methodology
 FROM TABLE a LEFT JOIN sizes s ON s.company_id=a.company_id WHERE a.company_id IS NOT NULL GROUP BY a.company_id""".replace('TABLE',table).replace('ASOF',asof)
 out=aws.extract('company_activity_baselines',sql,r/'activity',m['run_id']);files=out['files']
 import duckdb
 c=duckdb.connect();c.read_parquet([f['path'] for f in files]).create_view('baselines')
 p=r/'company_activity_baselines.parquet';c.execute("COPY baselines TO '"+str(p).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)")
 types={'VARCHAR':'string','DOUBLE':'double','BIGINT':'bigint'};cols=[dict(Name=n,Type=types[t]) for n,t,*_ in c.execute('DESCRIBE baselines').fetchall()];count=c.execute('SELECT count(*) FROM baselines').fetchone()[0]
 # Remote uniqueness/period gates and exact reconciliation are release prerequisites.
 gates,execution=aws.result("SELECT count(*)-count(DISTINCT transaction_key) duplicate_keys,count_if(try_cast(action_date AS date)<=date_add('year',-1,DATE '"+asof+"') OR action_date>'"+asof+"') outside_period,sum(CASE WHEN company_id IS NOT NULL THEN coalesce(action_obligation,0) ELSE 0 END) net FROM "+table)
 g=gates[0];local=c.execute('SELECT sum(annual_net_obligations) FROM baselines').fetchone()[0] or 0
 if int(g['duplicate_keys']) or int(g['outside_period']) or abs(float(g.get('net') or 0)-local)>1:raise ValueError('Annual activity evidence failed integrity/reconciliation')
 c.close();m['sources']['company_activity_baselines']=out
 m['tables']['company_activity_baselines']=dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),rows=count,columns=cols)
 m['annual_activity_policy']=dict(version=POLICY,target_multiple=3,interpretation='Commercial coverage benchmark only; no opportunity is created or inflated to reach it.',validation_query_id=execution['QueryExecutionId'],validation=g,source_query_id=source['query_id'],baseline_query_id=out['query_id'])
 (r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');print('Annual business baselines',count,'actions',source['rows'],flush=True)
