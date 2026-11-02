"""Build a full release-bound serving sidecar using existing validated evidence.

No source updates, new catalogue tables, identity inference or personal writes.
Run with the existing worker identity. ready.json is written only after parity.
"""
import argparse,concurrent.futures,datetime as dt,gzip,hashlib,json,re,tempfile,time
from pathlib import Path
import duckdb
from .api import Repository
from .company_index import VERSION,binding,contract,encode,eligible,overview,MAX_COMPRESSED,MAX_EXPANDED
from .aws import BUCKET,PREFIX
from opportunity_worker.opportunity_engine.aws import AWS


def strings(row):
    return {k:None if v is None else str(v).lower() if isinstance(v,bool) else str(v) for k,v in row.items()}


def read_rows(con,files):
    return con.execute('SELECT * FROM read_parquet(?)',[[f['path'] for f in files]])


def company_batches(con,root):
    # Order keys in a disk B-tree; never sort or partition the wide evidence
    # payload in DuckDB. SQLite's page cache is bounded independently of input.
    import sqlite3
    folder=Path(tempfile.mkdtemp(prefix='company-order-',dir=root))
    database=sqlite3.connect(str(folder/'records.sqlite'))
    database.execute('PRAGMA journal_mode=OFF');database.execute('PRAGMA synchronous=OFF')
    database.execute('PRAGMA temp_store=FILE');database.execute('PRAGMA cache_size=-65536')
    database.execute('CREATE TABLE records(company_id TEXT,episode_id TEXT,payload TEXT,PRIMARY KEY(company_id,episode_id)) WITHOUT ROWID')
    try:
        cursor=con.execute('SELECT * FROM candidates');names=[c[0] for c in cursor.description]
        while True:
            batch=cursor.fetchmany(128)
            if not batch:break
            encoded=[]
            for values in batch:
                row=strings(dict(zip(names,values)))
                encoded.append((row['serving_company_id'],row['episode_id'],encode(row).decode()))
            database.executemany('INSERT INTO records VALUES(?,?,?)',encoded)
        database.commit()
        cursor=database.execute('SELECT payload FROM records ORDER BY company_id,episode_id')
        while True:
            batch=cursor.fetchmany(128)
            if not batch:break
            decoded=[json.loads(raw) for (raw,) in batch]
            yield names,[[row.get(name) for name in names] for row in decoded]
    finally:database.close()


def build(manifest,root,companies=None,publish=False,advertise=True):
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    aws=AWS();repo=Repository(aws);repo._manifest=manifest;repo._expires=float('inf')
    release=manifest['run_id'];source_binding=binding(manifest);query_contract=contract(repo)
    base=PREFIX+'/releases/'+release+'/company-index/'+VERSION+'/'
    if not re.fullmatch('[a-zA-Z0-9_]+',release):raise ValueError('Unsafe release identity')
    if companies and any(not re.fullmatch('[A-Z0-9]{5}',c) for c in companies):raise ValueError('Invalid bounded CAGE list')
    sql=repo.opportunities('CAGE:00000',_query=True,_source_groups=False,_materialize=True)
    if companies:sql="SELECT * FROM ("+sql+") WHERE serving_company_id IN ("+','.join("'CAGE:"+c+"'" for c in companies)+')'
    extraction=aws.extract('company_serving_candidates',sql,root,'company_serving_'+release)
    con=duckdb.connect(str(root/'build.duckdb'));con.execute("SET memory_limit='4GB'");con.execute('SET threads=1');con.execute('SET preserve_insertion_order=false');con.execute('SET partitioned_write_max_open_files=8');con.execute("SET temp_directory='"+str(root/'spill').replace("'","''")+"'")
    paths=[f['path'] for f in extraction['files']]
    con.from_parquet(paths).create_view('candidates',replace=True)
    duplicates=con.execute('SELECT count(*) FROM (SELECT serving_company_id,episode_id,count(*) n FROM candidates GROUP BY 1,2 HAVING count(*)>1)').fetchone()[0]
    if duplicates:raise ValueError('Duplicate company/episode facts in serving projection')
    companies_meta={}
    for name in ('company_activity_baselines','company_catalogue_sales','company_reporting_lag_baselines','company_product_market_coverage','company_history_coverage'):
        if name not in manifest['tables']:continue
        auxsql='SELECT * FROM '+repo.table(name)
        if companies:auxsql+=" WHERE company_id IN ("+','.join("'CAGE:"+c+"'" for c in companies)+')'
        aux=aws.extract(name,auxsql,root,'company_serving_'+release)
        cursor=read_rows(con,aux['files']);names=[c[0] for c in cursor.description]
        while True:
            batch=cursor.fetchmany(1000)
            if not batch:break
            for values in batch:
                row=strings(dict(zip(names,values)));company=row.pop('company_id');row.pop('company_name',None)
                if not re.fullmatch('CAGE:[A-Z0-9]{5}',company or ''):continue
                item=companies_meta.setdefault(company,dict(business={},plans=[]))
                if name=='company_history_coverage':item['observed_prime_net_obligations']=row.get('observed_prime_net_obligations')
                else:item['business'].update(row)
    if 'agency_forecast_incumbents' in manifest['tables']:
        plansql=repo.opportunities('CAGE:00000',_query=True,_source_groups=False,_materialize=True,_plan_links=True)
        if companies:plansql="SELECT * FROM ("+plansql+") WHERE serving_company_id IN ("+','.join("'CAGE:"+c+"'" for c in companies)+')'
        aux=aws.extract('plan_precedence',plansql,root,'company_serving_'+release)
        cursor=read_rows(con,aux['files']);names=[c[0] for c in cursor.description]
        for values in cursor.fetchall():
            row=strings(dict(zip(names,values)));company=row.pop('serving_company_id')
            companies_meta.setdefault(company,dict(business={},plans=[]))['plans'].append(row)
    registry_sql='SELECT * FROM '+repo.table('company_registry')
    if companies:registry_sql+=" WHERE cage_code IN ("+','.join("'"+c+"'" for c in companies)+')'
    else:registry_sql+=' WHERE company_id IN (SELECT DISTINCT company_id FROM '+repo.table('company_matches')+')'
    aux=aws.extract('registry_identity',registry_sql,root,'company_serving_'+release)
    cursor=read_rows(con,aux['files']);names=[c[0] for c in cursor.description]
    while True:
        batch=cursor.fetchmany(1000)
        if not batch:break
        for values in batch:
            row=strings(dict(zip(names,values)));company=row['company_id']
            if re.fullmatch('CAGE:[A-Z0-9]{5}',company or ''):companies_meta.setdefault(company,dict(business={},plans=[]))['company']=row
    from .company_markets import query as markets_query
    markets_sql=markets_query(repo,None,limit=8)
    if companies:markets_sql="SELECT * FROM ("+markets_sql+") WHERE company_id IN ("+','.join("'CAGE:"+c+"'" for c in companies)+')'
    aux=aws.extract('company_market_footprints',markets_sql,root,'company_serving_'+release)
    cursor=read_rows(con,aux['files']);names=[c[0] for c in cursor.description]
    while True:
        batch=cursor.fetchmany(1000)
        if not batch:break
        for values in batch:
            row=strings(dict(zip(names,values)));company=row['company_id']
            companies_meta.setdefault(company,dict(business={},plans=[])).setdefault('markets',[]).append(row)
    for item in companies_meta.values():item['markets']=sorted(item.get('markets',[]),key=lambda row:int(row['market_rank']))
    from .item_notice_activity import query as notice_query
    aux=aws.extract('exact_item_notice_activity',notice_query(repo),root,'company_serving_'+release)
    notice_activity={}
    cursor=read_rows(con,aux['files']);names=[c[0] for c in cursor.description]
    while True:
        batch=cursor.fetchmany(1000)
        if not batch:break
        for values in batch:
            row=strings(dict(zip(names,values)));eid=row.pop('forecast_episode_id')
            notice_activity.setdefault(eid,[]).append(row)
    snapshot_time=dt.datetime.now(dt.timezone.utc);shards={};proof=[];pending=[];total=0;max_size=0
    pool=concurrent.futures.ThreadPoolExecutor(max_workers=12)
    def put(key,data):
        aws.s3.put_object(Bucket=BUCKET,Key=key,Body=data,ServerSideEncryption='AES256',ContentType='application/gzip' if key.endswith('.gz') else 'application/json')
    def emit(company,rows):
        nonlocal total,max_size
        meta=companies_meta.pop(company,dict(business={},plans=[]))
        for row in rows:
            linked=notice_activity.get(row['episode_id'])
            if linked:
                row['related_notice_activity_total']=str(len(linked))
                linked=sorted(linked,key=lambda r:(str(r.get('notice_posted_date') or ''),r['notice_episode_id']),reverse=True)
                row['related_notice_activity_json']=json.dumps(linked[:20])
        payload=dict(company_id=company,release=release,binding=source_binding,records=rows,**meta)
        raw=encode(payload);data=gzip.compress(raw,compresslevel=6,mtime=0)
        if len(data)>MAX_COMPRESSED or len(raw)>MAX_EXPANDED:raise ValueError('Company serving object requires paging: '+company)
        digest=hashlib.sha256(data).hexdigest();cage=company[5:];key=base+'objects/'+digest+'.json.gz'
        shards.setdefault(cage[:2],{})[cage]=dict(key=key,sha256=digest,bytes=len(data),rows=len(rows))
        if not rows:shards[cage[:2]][cage]=dict(empty_payload=payload)
        if rows or companies:(root/(cage+'.json.gz')).write_bytes(data)
        if publish and rows:pending.append(pool.submit(put,key,data))
        if len(pending)>=48:
            for task in pending:task.result()
            pending.clear()
        total+=len(rows);max_size=max(max_size,len(data))
        return payload
    current=None;rows=[];sample_payloads={}
    for names,batch in company_batches(con,root):
        for values in batch:
            row=strings(dict(zip(names,values)));company=row.pop('serving_company_id')
            if current and company!=current:
                payload=emit(current,rows)
                if len(sample_payloads)<6 or current in ('CAGE:45934','CAGE:07639','CAGE:81755','CAGE:98897'):sample_payloads[current]=payload
                if not any(p.get('_sample_role')=='subcontract_led' for p in sample_payloads.values()) and any(r.get('lane')=='recurring_subcontract_work' for r in rows) and not float(payload.get('business',{}).get('lag_adjusted_annual_dod_net_obligations') or 0):
                    payload['_sample_role']='subcontract_led';sample_payloads[current]=payload
                if not any(p.get('_sample_role')=='wholesale_classification' for p in sample_payloads.values()) and str(payload.get('company',{}).get('primary_naics') or '').startswith('42'):
                    payload['_sample_role']='wholesale_classification';sample_payloads[current]=payload
                current=company;rows=[]
            if current is None:current=company
            rows.append(row)
    if current:
        payload=emit(current,rows);sample_payloads.setdefault(current,payload)
    for company in list(companies_meta):
        payload=emit(company,[])
        if companies:sample_payloads.setdefault(company,payload)
    for company in ['CAGE:'+c for c in companies or []]:
        if company[5:] not in shards.get(company[5:7],{}):sample_payloads[company]=emit(company,[])
    expected=con.execute('SELECT count(*) FROM candidates').fetchone()[0]
    if total!=expected:raise ValueError('Serialized full population differs from extraction')
    # Validate original SQL family membership and canonical money on different
    # kinds, not just counts from the same serializer. No ready marker on failure.
    def parity(item):
        company,payload=item
        for kind in ('all','work','items'):
            q=repo.opportunities(company,kind=kind,_query=True)
            authoritative,_=aws.result('SELECT episode_id,record_hash,commercial_value,commercial_value_kind,item_demand FROM ('+q+')')
            actual=eligible(payload,kind)
            def keys(records):return sorted((r['episode_id'],r['record_hash'],r.get('commercial_value_kind'),None if r.get('commercial_value') is None else round(float(r['commercial_value']),6),str(r.get('item_demand')).lower()) for r in records)
            if keys(authoritative)!=keys(actual):raise ValueError('Warehouse/index parity failed: '+company+'/'+kind)
        authoritative=repo.overview(company[5:]);computed=overview(payload,manifest)
        # Components contain unordered source aggregates. Compare cardinality,
        # disjoint totals and historical coverage separately from that ordering.
        checks=['overall','categories','forecast','planned_packages','replenishment','work_forecast','valuation_coverage','business_coverage','materiality_floor']
        def stable(value):
            if isinstance(value,dict):return {k:stable(v) for k,v in value.items() if k not in ('value_components','related_requirements')}
            if isinstance(value,list):return [stable(x) for x in value]
            if isinstance(value,str) and re.fullmatch(r'-?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[Ee][+-]?[0-9]+)?',value):
                from decimal import Decimal
                return str(Decimal(value).normalize())
            return value
        if any(stable(authoritative.get(k))!=stable(computed.get(k)) for k in checks):
            (root/('overview-mismatch-'+company[5:]+'.json')).write_bytes(encode(dict(company=company,authoritative=authoritative,computed=computed)))
            raise ValueError('Overview parity failed: '+company)
        print('Company point-read parity passed',company,len(payload['records']),flush=True)
        return dict(company_id=company,kinds=['all','work','items'],overview=True,records=len(payload['records']))
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as validation_pool:
        proof=list(validation_pool.map(parity,sample_payloads.items()))
    for task in pending:task.result()
    pool.shutdown(wait=True)
    descriptor=dict(version=VERSION,release=release,binding=source_binding,contract=query_contract,complete=companies is None,created_at=snapshot_time.isoformat(),source_query_id=extraction['query_id'],source_query_sha256=extraction['query_sha256'],candidate_rows=total,companies=sum(len(s) for s in shards.values()),maximum_company_compressed_bytes=max_size,parity=proof,shards={})
    for shard,entries in shards.items():
        data=gzip.compress(encode(entries),compresslevel=6,mtime=0);digest=hashlib.sha256(data).hexdigest();key=base+'shards/'+digest+'.json.gz'
        descriptor['shards'][shard]=dict(key=key,sha256=digest,bytes=len(data),compression='gzip')
        if publish:put(key,data)
    (root/'ready.json').write_bytes(encode(descriptor))
    if publish:
        if companies:raise ValueError('Bounded samples cannot publish an authoritative universe index')
        put(base+'ready.json',encode(descriptor))
    if publish and advertise:
        response=aws.s3.get_object(Bucket=BUCKET,Key=PREFIX+'/current.json')
        pointer=json.loads(response['Body'].read())
        if pointer['run_id']!=release:raise ValueError('Serving release advanced; do not advertise a mixed generation')
        pointer['company_index']=dict(version=VERSION,sha256=hashlib.sha256(encode(descriptor)).hexdigest())
        aws.s3.put_object(Bucket=BUCKET,Key=PREFIX+'/current.json',Body=encode(pointer),IfMatch=response['ETag'],ServerSideEncryption='AES256',ContentType='application/json')
    return descriptor

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--manifest',required=True);parser.add_argument('--output',required=True);parser.add_argument('--companies');parser.add_argument('--publish',action='store_true');args=parser.parse_args()
    print(json.dumps(build(json.loads(Path(args.manifest).read_text()),args.output,args.companies.split(',') if args.companies else None,args.publish),indent=2))
