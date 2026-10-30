"""Distributed warehouse aggregation; large evidence stays in immutable S3 tables."""
import hashlib,json,re,uuid
from pathlib import Path
from .aws import AWS,BUCKET,PREFIX,DATABASE
from .company_enrichment import STOP,ALIASES

def lit(value):return "'"+str(value).replace("'","''")+"'"
def digest(text):return 'to_hex(sha256(to_utf8('+text+')))'
def ident():return "CASE WHEN regexp_like(upper(trim(cage_code)),'^[A-Z0-9]{5}$') THEN 'CAGE:'||upper(trim(cage_code)) WHEN nullif(trim(uei),'') IS NOT NULL THEN 'UEI:'||upper(trim(uei)) END"
def norm(expr):return "regexp_replace(upper(regexp_replace(coalesce("+expr+",''),'^[0-9]{8,12}!\\s*','')),'[^A-Z0-9]','')"
def terms(expr):
    aliases='MAP(ARRAY['+','.join(lit(k) for k in ALIASES)+'],ARRAY['+','.join(lit(v) for v in ALIASES.values())+'])'
    stop='ARRAY['+','.join(lit(s) for s in sorted(STOP))+']'
    words="regexp_extract_all(lower(regexp_replace(coalesce("+expr+",''),'<[^>]+>',' ')),'[a-z][a-z0-9]*')"
    ts="array_sort(array_distinct(filter(transform("+words+",w->coalesce(element_at("+aliases+",w),w)),w->length(w)>=3 AND NOT contains("+stop+",w))))"
    return "CASE WHEN length(coalesce("+expr+",''))<=260 AND cardinality("+words+")<=24 AND cardinality("+ts+") BETWEEN 2 AND 12 AND NOT regexp_like(lower(coalesce("+expr+",'')),'awards subcontracts for|may or may not be billed|first article test') THEN array_join("+ts+",'|') ELSE '' END"
def mode(text,psc):return "CASE WHEN starts_with(coalesce("+psc+",''),'J') OR regexp_like(lower(coalesce("+text+",'')),'\\b(repair|overhaul|remanufactur\\w*|refurbish\\w*)\\b') THEN 'repair' WHEN regexp_like(lower(coalesce("+text+",'')),'\\b(research|prototype|prototyping|development|r&d)\\b') THEN 'research_development' WHEN regexp_like(lower(coalesce("+text+",'')),'\\b(integration|integrating)\\b') THEN 'integration' WHEN regexp_like(lower(coalesce("+text+",'')),'\\b(testing|test services|qualification services)\\b') THEN 'test_services' ELSE 'supply_or_unspecified' END"

def columns(aws,sql):
    e=aws.query('SELECT * FROM ('+sql+') column_probe WHERE false')
    result=aws.athena.get_query_results(QueryExecutionId=e['QueryExecutionId'],MaxResults=1)
    mapping={'varchar':'string','char':'string','bigint':'bigint','integer':'bigint','double':'double','real':'double','boolean':'boolean'}
    out=[]
    for col in result['ResultSet']['ResultSetMetadata']['ColumnInfo']:
        typ=col['Type'].split('(')[0]
        if typ not in mapping:raise ValueError('Nonportable remote source type '+col['Type'])
        out.append({'Name':col['Name'],'Type':mapping[typ]})
    if len({v['Name'] for v in out})!=len(out):raise ValueError('Duplicate distributed projection columns')
    return out

def remote(aws,name,sql,run,as_of,recovered=None):
    logical_sql=sql.replace('__SHARD__','true')
    if recovered:
        source=dict(recovered);source['remote_only']=True
        cols=source.get('columns') or columns(aws,logical_sql)
        prefix=source['files'][0]['s3_uri'].rsplit('/',1)[0]+'/'
    else:
        prefix='s3://'+BUCKET+'/'+PREFIX+'/sources/'+run+'/'+name+'/'+uuid.uuid4().hex[:10]+'/data/'
        shards=list('0123456789abcdef') if '__SHARD__' in sql else [None]
        files=[];query_ids=[];scanned=0
        for shard in shards:
            target=prefix if shard is None else prefix.replace('/data/','/shards/'+shard+'/')
            statement=sql.replace('__SHARD__',"substr(lower(to_hex(sha256(to_utf8(company_id)))),1,1)='"+str(shard)+"'")
            e=aws.query("UNLOAD ("+statement+") TO '"+target+"' WITH (format='PARQUET',compression='ZSTD')")
            query_ids.append(e['QueryExecutionId']);scanned+=e['Statistics'].get('DataScannedInBytes',0)
            for page in aws.s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET,Prefix=target.split('/',3)[3]):
                for obj in page.get('Contents',[]):
                    key=obj['Key']
                    if shard is not None:
                        key=prefix.split('/',3)[3]+shard+'_'+key.rsplit('/',1)[-1]
                        aws.s3.copy_object(Bucket=BUCKET,Key=key,CopySource={'Bucket':BUCKET,'Key':obj['Key']},ServerSideEncryption='AES256')
                    body=aws.s3.get_object(Bucket=BUCKET,Key=key)['Body'];sha=hashlib.sha256()
                    for chunk in iter(lambda:body.read(8*1024*1024),b''):sha.update(chunk)
                    files.append(dict(s3_uri='s3://'+BUCKET+'/'+key,sha256=sha.hexdigest(),bytes=obj['Size']))
            if shard is not None:print('Complete work-history shard',shard,flush=True)
        cols=columns(aws,logical_sql)
        if not files:raise ValueError('Remote source empty: '+name)
        source=dict(files=files,query_id=e['QueryExecutionId'],query_ids=query_ids,query_sha256=hashlib.sha256(logical_sql.encode()).hexdigest(),scanned_bytes=scanned,sql=logical_sql)
    signature=hashlib.sha256(json.dumps([(f['s3_uri'],f['sha256']) for f in source['files']],sort_keys=True).encode()).hexdigest()
    table='oi_source_'+signature[:20]+'_'+name
    aws.register(table,prefix,cols)
    stats,_=aws.result('SELECT count(*) records FROM '+DATABASE+'.'+table)
    source.update(table=table,s3_uri=prefix,columns=cols,rows=int(stats[0]['records']),sha256=signature,as_of=as_of,status='SUCCEEDED',kind='complete_native_warehouse_projection',remote_only=True)
    print('Distributed source',name,source['rows'],flush=True)
    return source

def refresh(aws,root,as_of,native_queries,previous=None):
    run='distributed_history_'+as_of.replace('-','')+'_'+uuid.uuid4().hex[:8]
    root=Path(root);folder=root/'inputs'/run;folder.mkdir(parents=True,exist_ok=True);sources={}
    for name,sql in native_queries.items():
        prior=(previous or {}).get(name)
        sources[name]=remote(aws,name,sql,run,as_of,prior if prior and prior.get('as_of')==as_of else None)
    p=DATABASE+'.'+sources['native_prime_awards']['table'];s=DATABASE+'.'+sources['native_subcontract_reports']['table']
    def emit(name,sql,download=True):
        prior=(previous or {}).get(name)
        same=prior and prior.get('as_of')==as_of and prior.get('query_sha256')==hashlib.sha256(sql.replace('__SHARD__','true').encode()).hexdigest()
        source=remote(aws,name,sql,run,as_of,prior if same else None);sources[name]=source
        if download:
            files=[];dest=folder/name;dest.mkdir()
            for i,f in enumerate(source['files']):
                path=dest/(str(i)+'.parquet');aws.s3.download_file(BUCKET,f['s3_uri'].split('/',3)[3],str(path))
                if hashlib.sha256(path.read_bytes()).hexdigest()!=f['sha256']:raise ValueError('Source hash mismatch')
                files.append(dict(f,path=str(path)))
            source.update(files=files,path=files[0]['path'],remote_only=False)
        aws.put_json(PREFIX+'/sources/'+run+'/manifest.json',dict(run_id=run,as_of=as_of,sources=sources,phase='checkpoint'))
        return DATABASE+'.'+source['table']
    identity=emit('native_company_identity',"""SELECT uei,CASE WHEN count(DISTINCT cage_code)=1 THEN min(cage_code) END cage_code,count(DISTINCT cage_code) candidate_sites FROM (
    SELECT upper(trim(uei)) uei,upper(trim(cage_code)) cage_code FROM P
    UNION SELECT upper(trim(unique_entity_id)),upper(trim(cage_code)) FROM market_intel_silver.ref_sam_entities)
    WHERE nullif(uei,'') IS NOT NULL AND regexp_like(cage_code,'^[A-Z0-9]{5}$') GROUP BY uei""".replace(' P',' '+p))
    sub=emit('native_company_subcontracts',"""SELECT s.*,CASE WHEN i.cage_code IS NOT NULL THEN 'CAGE:'||i.cage_code WHEN nullif(s.sub_uei,'') IS NOT NULL THEN 'UEI:'||s.sub_uei END company_id,
    i.cage_code,CASE WHEN i.cage_code IS NOT NULL THEN 'unique_observed_uei_cage' WHEN i.candidate_sites>1 THEN 'multiple_sites_uei_only' ELSE 'unresolved_uei_only' END site_resolution,
    pi.cage_code prime_cage,pi.candidate_sites prime_site_candidates,p.programme_name parent_programme_name,p.programme_code parent_programme_code,p.psc parent_psc,p.naics linked_parent_naics,p.work_text parent_award_scope FROM S s LEFT JOIN I i ON i.uei=s.sub_uei LEFT JOIN I pi ON pi.uei=s.prime_uei LEFT JOIN P p ON p.award_key=s.parent_award_key""".replace(' S ', ' '+s+' ').replace(' I ', ' '+identity+' ').replace(' P ', ' '+p+' '))
    resolved_id="CASE WHEN regexp_like(upper(trim(p.cage_code)),'^[A-Z0-9]{5}$') THEN 'CAGE:'||upper(trim(p.cage_code)) WHEN i.cage_code IS NOT NULL THEN 'CAGE:'||i.cage_code WHEN nullif(trim(p.uei),'') IS NOT NULL THEN 'UEI:'||upper(trim(p.uei)) END"
    resolved_prime=emit('native_company_awards','SELECT p.*,'+resolved_id+' company_id FROM '+p+' p LEFT JOIN '+identity+' i ON i.uei=p.uei',download=False)
    prime=resolved_prime
    emit('native_company_prime_stats',"""SELECT company_id,max_by(company_name,latest_action_date) company_name,max_by(cage_code,latest_action_date) cage_code,max_by(uei,latest_action_date) uei,
    count(*) prime_awards,sum(observed_net_obligations) observed_prime_net_obligations,min(first_action_date) first_prime_action,max(latest_action_date) last_prime_action,
    count_if(lower(manufacturer_of_goods) IN ('t','true','y','yes')) manufacturer_declared_awards,count_if(substr(naics,1,2) IN ('31','32','33')) manufacturing_naics_awards,count_if(substr(naics,1,2)='42') wholesale_naics_awards,
    max_by(parent_name,latest_action_date) parent_name,max_by(parent_uei,latest_action_date) parent_uei,
    to_hex(sha256(to_utf8(array_join(array_sort(array_agg(evidence_id)),'|')))) prime_evidence_digest FROM P WHERE company_id IS NOT NULL GROUP BY company_id""".replace(' P ', ' '+prime+' '))
    emit('native_company_sub_stats',"""SELECT company_id,max_by(sub_name,event_date) company_name,count(*) subcontract_reports,count(DISTINCT coalesce(nullif(prime_uei,''),prime_name)) commercial_customers,max(event_date) last_subcontract_report,
    to_hex(sha256(to_utf8(array_join(array_sort(array_agg(evidence_id)),'|')))) sub_evidence_digest FROM S WHERE company_id IS NOT NULL GROUP BY company_id""".replace(' S ', ' '+sub+' '))
    raw="""SELECT company_id,'prime_award' source_kind,work_text,psc,naics,latest_action_date last_observed,evidence_id,programme_name FROM P WHERE company_id IS NOT NULL
    UNION ALL SELECT company_id,'latest_prime_action',latest_description,psc,naics,latest_action_date,evidence_id,programme_name FROM P WHERE company_id IS NOT NULL AND N1<>N2
    UNION ALL SELECT company_id,'subaward_report',work_text,cast(NULL AS varchar),cast(NULL AS varchar),event_date,evidence_id,parent_programme_name FROM S WHERE company_id IS NOT NULL""".replace(' P ', ' '+prime+' ').replace(' S ', ' '+sub+' ').replace('N1',norm('latest_description')).replace('N2',norm('work_text'))
    # Bounded aggregate fingerprint; full source bytes retain SHA-256 provenance.
    # Aggregating every duplicate identifier into an array exceeds Athena row limits.
    work="""WITH raw AS(RAW),g AS(SELECT company_id,source_kind,NORM scope_key,psc,naics,min(work_text) work_text,max(last_observed) last_observed,min(evidence_id) sample_evidence_id,
    to_hex(sha256(checksum(evidence_id))) evidence_digest,count(*) source_records,json_format(cast(array_sort(array_agg(DISTINCT programme_name) FILTER(WHERE programme_name IS NOT NULL AND programme_name NOT IN ('NONE','NOT APPLICABLE','UNKNOWN',''))) AS JSON)) programme_context_json
    FROM raw WHERE length(NORM)>=8 AND __SHARD__ GROUP BY company_id,source_kind,NORM,psc,naics)
    SELECT lower(to_hex(sha256(to_utf8(concat_ws('|',company_id,source_kind,scope_key,psc,naics))))) doc_id,company_id,source_kind,work_text,psc,naics,last_observed,sample_evidence_id,evidence_digest,source_records,programme_context_json FROM g""".replace('RAW',raw).replace('NORM',norm('work_text'))
    work_table=emit('native_company_work',work)
    expected,qe=aws.result('SELECT count(*) records FROM ('+raw+') raw WHERE length('+norm('work_text')+')>=8')
    observed,qo=aws.result('SELECT sum(source_records) records FROM '+work_table)
    if expected[0]['records']!=observed[0]['records']:raise ValueError('Full work-history partition coverage failed')
    sources['native_company_work']['coverage_verification']=dict(source_rows=int(expected[0]['records']),retained_rows=int(observed[0]['records']),query_ids=[qe['QueryExecutionId'],qo['QueryExecutionId']],complete=True,partitions=16)
    features="""SELECT company_id,'psc' feature_type,psc feature_value,count(*) supporting_records,min(evidence_id) sample_evidence_id FROM P WHERE company_id IS NOT NULL AND nullif(psc,'') IS NOT NULL GROUP BY company_id,psc
    UNION ALL SELECT company_id,'naics',naics,count(*),min(evidence_id) FROM P WHERE company_id IS NOT NULL AND nullif(naics,'') IS NOT NULL GROUP BY company_id,naics
    UNION ALL SELECT company_id,'government_customer',concat_ws(' / ',buyer_name,buyer_office),count(*),min(evidence_id) FROM P WHERE company_id IS NOT NULL GROUP BY company_id,buyer_name,buyer_office
    UNION ALL SELECT company_id,'programme',programme_name,count(*),min(evidence_id) FROM P WHERE company_id IS NOT NULL AND nullif(programme_name,'') IS NOT NULL AND upper(programme_name) NOT IN ('NONE','NOT APPLICABLE','UNKNOWN') GROUP BY company_id,programme_name
    UNION ALL SELECT company_id,'commercial_customer',prime_name,count(*),min(evidence_id) FROM S WHERE company_id IS NOT NULL GROUP BY company_id,prime_name""".replace(' P ', ' '+prime+' ').replace(' S ', ' '+sub+' ')
    emit('native_company_features',features)
    keyed="(SELECT *,"+terms("regexp_replace(work_text,'^[0-9]{8,12}!\\s*','')")+" scope_key,"+mode('work_text','psc')+" work_mode FROM "+p+")"
    comparator_row="CAST(ROW(award_key,piid,parent_piid,evidence_id,company_name,cage_code,source_url,work_text,observed_net_obligations,first_action_date,latest_action_date,period_start,period_end,action_count) AS ROW(award_key varchar,piid varchar,parent_piid varchar,evidence_id varchar,company_name varchar,cage_code varchar,source_url varchar,work_text varchar,observed_net_obligations double,first_action_date varchar,latest_action_date varchar,period_start varchar,period_end varchar,action_count bigint))"
    emit('contract_value_cohorts',"SELECT scope_key,work_mode,buyer_office_code,buyer_name,buyer_office,psc,count(*) awards,min(observed_net_obligations) observed_min_usd,approx_percentile(observed_net_obligations,0.5) observed_median_usd,max(observed_net_obligations) observed_max_usd,json_format(cast(array_agg("+comparator_row+" ORDER BY award_key) AS JSON)) comparators_json FROM "+keyed+" WHERE scope_key<>'' AND observed_net_obligations>0 AND try_cast(substr(latest_action_date,1,10) AS date)>=DATE "+lit(as_of)+"-INTERVAL '5' YEAR GROUP BY scope_key,work_mode,buyer_office_code,buyer_name,buyer_office,psc")
    family=lambda alias:"CASE WHEN nullif("+alias+".parent_piid,'') IS NOT NULL AND upper("+alias+".parent_piid) NOT IN ('-NONE-','NONE') THEN concat_ws('|','vehicle',"+alias+".uei,"+alias+".parent_agency,"+alias+".parent_piid) WHEN nullif("+alias+".programme_code,'') IS NOT NULL AND upper("+alias+".programme_code) NOT IN ('000','0000','NONE','-NONE-','NOT APPLICABLE') THEN concat_ws('|','programme',"+alias+".uei,"+alias+".programme_code) END"
    preview="CAST(ROW(s.evidence_id,p.evidence_id,s.parent_award_key,s.report_key,s.sub_name,s.company_id,s.reported_amount,s.event_date,s.source_url,s.work_text) AS ROW(report_evidence varchar,parent_evidence varchar,parent_award_key varchar,report_key varchar,supplier varchar,supplier_company_id varchar,reported_amount double,event_date varchar,source_url varchar,work_text varchar))"
    pull="""WITH canonical_sub AS(SELECT * FROM S QUALIFY_PLACEHOLDER),
    placements AS(SELECT parent_award_key,PLACEMENT_KEY scope_key,count(*) placed_reports,json_format(cast(array_agg(evidence_id ORDER BY evidence_id) AS JSON)) placement_evidence_ids FROM canonical_sub GROUP BY parent_award_key,PLACEMENT_KEY),
    hist AS(SELECT s.*,p.evidence_id parent_evidence,FAMILY_HIST family_key,KEY scope_key FROM canonical_sub s JOIN P p ON p.award_key=s.parent_award_key WHERE s.reported_amount>0),
    new_awards AS(SELECT n.*,FAMILY_NEW family_key FROM P n WHERE try_cast(substr(n.first_action_date,1,10) AS date)>=DATE ASOF-INTERVAL '365' DAY AND try_cast(substr(n.period_end,1,10) AS date)>=DATE ASOF AND n.observed_net_obligations>0),
    packages AS(SELECT n.award_key,n.evidence_id new_prime_evidence,n.company_name customer_name,n.first_action_date,n.period_end,n.programme_name,n.psc,n.naics,n.source_url,n.piid,n.observed_net_obligations new_prime_observed_net,n.work_text new_prime_scope,
    s.scope_key,min(s.work_text) work_text,coalesce(max(z.placed_reports),0) placed_reports,max(z.placement_evidence_ids) placement_evidence_ids,count(DISTINCT s.parent_award_key) prior_awards,count(*) source_reports,min(s.reported_amount) value_low,approx_percentile(s.reported_amount,0.5) value_median,max(s.reported_amount) value_high,
    to_hex(sha256(to_utf8(array_join(array_sort(array_agg(s.evidence_id)),'|')))) support_digest,
    json_format(cast(array_agg(PREVIEW_ROW ORDER BY s.evidence_id) AS JSON)) preview
    FROM new_awards n JOIN hist s ON s.family_key=n.family_key AND s.prime_uei=n.uei AND s.parent_award_key<>n.award_key
    JOIN P p ON p.award_key=s.parent_award_key LEFT JOIN placements z ON z.parent_award_key=n.award_key AND z.scope_key=s.scope_key
    WHERE s.scope_key<>'' AND (s.parent_psc=n.psc OR (nullif(s.parent_programme_code,'') IS NOT NULL AND s.parent_programme_code=n.programme_code AND s.parent_programme_code NOT IN ('000','0000','NONE','-NONE-'))) AND try_cast(substr(s.event_date,1,10) AS date)<try_cast(substr(n.first_action_date,1,10) AS date) AND try_cast(substr(s.event_date,1,10) AS date)>=try_cast(substr(n.first_action_date,1,10) AS date)-INTERVAL '5' YEAR
    GROUP BY n.award_key,n.evidence_id,n.company_name,n.first_action_date,n.period_end,n.programme_name,n.psc,n.naics,n.source_url,n.piid,n.observed_net_obligations,n.work_text,s.scope_key)
    SELECT lower(to_hex(sha256(to_utf8('native-prime-pull-v1|'||award_key||'|'||scope_key)))) episode_id,lower(to_hex(sha256(to_utf8(concat_ws('|',new_prime_evidence,support_digest))))) record_hash,
    'new_prime_historical_supplier_flow' lane,'Potential supplier package: '||work_text title,work_text||' New prime scope: '||coalesce(new_prime_scope,'') scope_text,customer_name,nullif(programme_name,'NONE') programme,psc,naics,ASOF window_start,period_end window_end,
    'Current prime performance period; supplier RFQ date is not announced' date_basis,value_low,value_median,value_high,
    'historical_same_vehicle_or_programme_subcontract_packages' value_basis,'Historical outsourced package range; potential package size, not prime value or company workshare' value_scope,
    json_format(cast(ROW_INPUTS AS JSON)) value_inputs_json,
    'native_awards_and_subcontract_reports' source_name,source_url,new_prime_evidence source_evidence_id,award_key source_locator,piid native_identifier,
    'new_prime_funded_prior_outsourcing_observed' procurement_state,CASE WHEN placed_reports>0 THEN 'source_confirmed_award' ELSE 'supplier_placement_not_established' END reconciled_state,'Prime supplier entry, configuration and availability require confirmation' access_state,
    'A newly funded prime award shares an exact parent vehicle or reported program with earlier contracts where this prime outsourced the stated work. Linked reports identify historical suppliers and package amounts.' rationale,
    'The package may already be placed. Confirm scope, buying site, technical access, qualifications and sourcing timetable. Prime funding is context, not supplier opportunity value.' limitations FROM packages"""
    # Athena has no QUALIFY; revisions are resolved inside a bounded window.
    canonical="SELECT * FROM (SELECT *,row_number() OVER(PARTITION BY parent_award_key,sub_uei,subaward_number ORDER BY modified_date DESC NULLS LAST,event_date DESC,evidence_id) revision FROM "+sub+") WHERE revision=1"
    pull=pull.replace('SELECT * FROM S QUALIFY_PLACEHOLDER',canonical).replace(' P ', ' '+p+' ').replace('FAMILY_HIST',family('p')).replace('FAMILY_NEW',family('n')).replace('PLACEMENT_KEY',terms('work_text')).replace('KEY',terms('s.work_text')).replace('PREVIEW_ROW',preview).replace('ASOF',lit(as_of))
    inputs="CAST(ROW(new_prime_evidence,award_key,new_prime_observed_net,prior_awards,source_reports,support_digest,first_action_date,json_parse(preview),placed_reports,coalesce(json_parse(placement_evidence_ids),json_parse('[]'))) AS ROW(new_prime_evidence varchar,new_prime_award_key varchar,new_prime_observed_net double,prior_awards bigint,source_reports bigint,support_digest varchar,first_award_date varchar,preview JSON,placed_reports bigint,outcome_evidence_ids JSON))"
    emit('native_supplier_pull',pull.replace('ROW_INPUTS',inputs))
    # Only a compact serving projection is brought into the worker. Raw evidence
    # remains available in the registered complete award and report tables.
    result=dict(run_id=run,as_of=as_of,sources=sources)
    path=folder/'manifest.json';path.write_text(json.dumps(result,indent=2)+'\n');aws.put_json(PREFIX+'/sources/'+run+'/manifest.json',result)
    return result
