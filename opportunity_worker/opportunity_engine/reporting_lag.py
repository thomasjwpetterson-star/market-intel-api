"""Observed annual comparisons that respect Defense public reporting latency."""
import datetime,hashlib,json
from pathlib import Path
from .aws import DATABASE
from .company_activity import action_query
from .distributed_history import remote
POLICY='defense-public-reporting-lag-year-v1'
SOURCE='https://www.usaspending.gov/data/about-the-data-download.pdf'

def period(as_of):
 day=datetime.date.fromisoformat(as_of);cutoff=day-datetime.timedelta(days=90)
 end=cutoff.replace(day=1)-datetime.timedelta(days=1)
 start=(end.replace(year=end.year-1,day=1)+datetime.timedelta(days=32)).replace(day=1)
 return start.isoformat(),end.isoformat()

def refresh(directory,aws):
 r=Path(directory);m=json.loads((r/'manifest.json').read_text());identity=m['sources'].get('native_company_identity')
 if not identity or not identity.get('table'):return
 start,end=period(m['as_of']);query=action_query(end,DATABASE+'.'+identity['table']).replace("try_cast(action_date AS date)>date_add('year',-1,DATE '"+end+"')","try_cast(action_date AS date)>=DATE '"+start+"'")
 source=remote(aws,'company_reporting_lag_actions',query,m['run_id']+'_reporting_lag',end)
 m['sources']['company_reporting_lag_actions']=source
 m['tables']['company_reporting_lag_actions']=dict(**{k:source[k] for k in ('rows','sha256','columns','table','s3_uri')},source_binding='company_reporting_lag_actions',remote_binding=True)
 table=DATABASE+'.'+source['table']
 sql="""SELECT company_id,sum(coalesce(action_obligation,0)) lag_adjusted_annual_net_obligations,
 sum(greatest(coalesce(action_obligation,0),0)) lag_adjusted_annual_positive_obligations,
 sum(CASE WHEN awarding_agency_code='097' THEN coalesce(action_obligation,0) ELSE 0 END) lag_adjusted_annual_dod_net_obligations,
 count(*) lag_adjusted_annual_action_count,count(DISTINCT award_key) lag_adjusted_annual_award_count,
 min(action_date) lag_adjusted_first_observed_action,max(action_date) lag_adjusted_last_observed_action,
 '__START__' lag_adjusted_period_start,'__END__' lag_adjusted_period_end,cast(90 AS bigint) lag_adjusted_nominal_reporting_delay_days,
 lower(to_hex(sha256(checksum(evidence_id)))) lag_adjusted_evidence_digest,
 '__SOURCE__' lag_adjusted_methodology_source,
 'Observed twelve-month period ending with the last calendar month before the 90-day Defense publication-delay window. Latest held transaction revisions; no extrapolation or certification that every report is complete. The recent trailing-year view remains separate.' lag_adjusted_methodology
 FROM TABLE WHERE company_id IS NOT NULL GROUP BY company_id""".replace('TABLE',table).replace('__START__',start).replace('__END__',end).replace('__SOURCE__',SOURCE)
 out=aws.extract('company_reporting_lag_baselines',sql,r/'reporting_lag',m['run_id'])
 import duckdb
 c=duckdb.connect();c.read_parquet([f['path'] for f in out['files']]).create_view('baselines');p=r/'company_reporting_lag_baselines.parquet';c.execute('COPY baselines TO ? (FORMAT PARQUET,COMPRESSION ZSTD)',[str(p)])
 types={'VARCHAR':'string','DOUBLE':'double','BIGINT':'bigint','INTEGER':'bigint'};cols=[dict(Name=n,Type=types[t]) for n,t,*_ in c.execute('DESCRIBE baselines').fetchall()];count=c.execute('SELECT count(*) FROM baselines').fetchone()[0]
 gates,e=aws.result("SELECT count(*)-count(DISTINCT transaction_key) duplicates,count_if(action_date<'"+start+"' OR action_date>'"+end+"') outside_period,sum(CASE WHEN company_id IS NOT NULL THEN coalesce(action_obligation,0) ELSE 0 END) net FROM "+table)
 g=gates[0];net=c.execute('SELECT sum(lag_adjusted_annual_net_obligations) FROM baselines').fetchone()[0] or 0
 if int(g['duplicates']) or int(g['outside_period']) or abs(float(g.get('net') or 0)-net)>1:raise ValueError('Lag-adjusted annual evidence failed reconciliation')
 m['tables']['company_reporting_lag_baselines']=dict(path=str(p),rows=count,sha256=hashlib.sha256(p.read_bytes()).hexdigest(),columns=cols);m['sources']['company_reporting_lag_baselines']=out;m['reporting_lag_policy']=dict(version=POLICY,nominal_delay_days=90,period_start=start,period_end=end,source_url=SOURCE,validation_query_id=e['QueryExecutionId'],checks=g)
 (r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close();print('Reporting-lag annual comparison',count,'companies',start,end,flush=True)
