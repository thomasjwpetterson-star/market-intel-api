"""Whole-market vocabulary, evidence hygiene and additive serving projections."""
import copy,datetime,hashlib,json,uuid
from pathlib import Path
import duckdb
from .aws import BUCKET,PREFIX,DATABASE
from .distributed_history import remote
from .work_vocabulary import annotate,codebook,evidence_verdict,VERSION,POLICY_HASH,SPEC

def scope_id(text,psc):
    return "sha256(coalesce("+psc+",'')||'|'||trim(regexp_replace(upper(coalesce("+text+",'')),'\\s+',' ','g')))"

def source_facts_sql(manifest,keys_table=None):
    t=lambda name:DATABASE+'.'+manifest['tables'][name]['table']
    keys='SELECT DISTINCT company_id,sample_evidence_id FROM '+(keys_table or t('company_work_matches'))
    return """WITH keys AS(KEYS), facts AS(
    SELECT k.company_id,k.sample_evidence_id,p.evidence_id,'prime_award' history_kind,p.award_key record_id,p.work_text,
    p.observed_net_obligations net_obligations,p.observed_positive_obligations positive_obligations,
    cast(NULL AS double) reported_subcontract_amount,p.psc,p.naics,p.source_url,p.first_action_date,p.latest_action_date,p.period_start,p.period_end
    FROM keys k JOIN PRIME p ON p.company_id=k.company_id AND p.evidence_id=k.sample_evidence_id
    UNION ALL SELECT k.company_id,k.sample_evidence_id,s.evidence_id,'subcontract_report',s.report_key,s.work_text,
    cast(NULL AS double),cast(NULL AS double),s.reported_amount,cast(NULL AS varchar),s.parent_naics,s.source_url,s.event_date,s.modified_date,cast(NULL AS varchar),cast(NULL AS varchar)
    FROM keys k JOIN SUBS s ON s.company_id=k.company_id AND s.evidence_id=k.sample_evidence_id)
    SELECT * FROM facts WHERE evidence_id IS NOT NULL""".replace('KEYS',keys).replace('PRIME',t('company_award_facts')).replace('SUBS',t('company_subcontract_facts'))

def augment(directory,aws):
    r=Path(directory);m=json.loads((r/'manifest.json').read_text())
    if not all(n in m['tables'] for n in ('company_capability_history','company_work_matches','company_award_facts','company_subcontract_facts')):return m
    run='vocabulary_'+m['run_id'];c=duckdb.connect();c.execute("SET threads=4;SET memory_limit='6GB';SET preserve_insertion_order=false")
    c.execute("SET temp_directory='"+str(r/'vocabulary_tmp').replace("'","''")+"'")
    for name in ('opportunities','company_work_matches','company_matches','company_match_assessments'):
        c.read_parquet(m['tables'][name]['path']).create_view('old_'+name)
    def download_source(name,source):
        paths=[]
        for i,f in enumerate(source['files']):
            path=r/(name+'_input_'+str(i)+'.parquet')
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=f['sha256']:aws.s3.download_file(BUCKET,f['s3_uri'].split('/',3)[3],str(path))
            if hashlib.sha256(path.read_bytes()).hexdigest()!=f['sha256']:raise ValueError('Vocabulary source checksum mismatch: '+name)
            paths.append(str(path))
        c.read_parquet(paths).create_view(name)
    download_source('company_capability_history',m['sources']['company_capability_history'])
    # Native builds have fresh work edges before publication. Register their
    # exact keys instead of querying the preceding release's company matches.
    kp=r/'vocabulary_evidence_keys.parquet';c.execute('COPY (SELECT DISTINCT company_id,sample_evidence_id FROM old_company_work_matches) TO ? (FORMAT PARQUET,COMPRESSION ZSTD)',[str(kp)])
    key_sha=hashlib.sha256(kp.read_bytes()).hexdigest();key=PREFIX+'/sources/'+run+'/evidence_keys/data/part-00000.parquet'
    key_table='oi_source_'+hashlib.sha256((key+key_sha).encode()).hexdigest()[:20]+'_vocabulary_evidence_keys'
    aws.upload(kp,key);aws.register(key_table,'s3://'+BUCKET+'/'+key.rsplit('/',1)[0]+'/',[dict(Name='company_id',Type='string'),dict(Name='sample_evidence_id',Type='string')])
    m['sources']['work_match_evidence_keys']=dict(files=[dict(s3_uri='s3://'+BUCKET+'/'+key,sha256=key_sha,path=str(kp))],remote_only=True,table=key_table)
    proof=remote(aws,'work_match_source_facts',source_facts_sql(m,DATABASE+'.'+key_table),run,m['as_of'])
    m['sources']['work_match_source_facts']=proof
    download_source('work_match_source_facts',proof)
    def output(name,sql):
        c.execute('CREATE OR REPLACE TEMP VIEW vocabulary_result AS '+sql)
        p=r/(name+'.parquet');temporary=r/(name+'.vocabulary.parquet')
        c.execute('COPY vocabulary_result TO ? (FORMAT PARQUET,COMPRESSION ZSTD)',[str(temporary)]);temporary.replace(p)
        types={'VARCHAR':'string','JSON':'string','BIGINT':'bigint','INTEGER':'bigint','UBIGINT':'bigint','DOUBLE':'double','BOOLEAN':'boolean'}
        columns=[dict(Name=n,Type=types[t]) for n,t,*_ in c.execute('DESCRIBE vocabulary_result').fetchall()]
        count=c.execute('SELECT count(*) FROM read_parquet(?)',[str(p)]).fetchone()[0]
        m['tables'][name]=dict(path=str(p),rows=count,sha256=hashlib.sha256(p.read_bytes()).hexdigest(),columns=columns)
        c.read_parquet(str(p)).create_view('new_'+name,replace=True);print('Vocabulary output',name,count,flush=True)
    ref=codebook(m['as_of']);reference={x['psc']:x for x in ref}
    rp=r/'psc_reference.jsonl';rp.write_text('\n'.join(json.dumps(x) for x in ref)+'\n')
    c.read_json(str(rp),format='newline_delimited',sample_size=-1).create_view('reference_input')
    output('work_vocabulary_reference','SELECT * REPLACE(cast(effective_start AS VARCHAR) AS effective_start,cast(effective_end AS VARCHAR) AS effective_end) FROM reference_input')
    topic_rows=[dict(topic_id=x['id'],label=x['label'],parent_id=x.get('parent'),facet=x.get('facet','navigation'),vocabulary_version=VERSION,policy_hash=POLICY_HASH,pattern=x.get('pattern','')) for x in SPEC['roots']+SPEC['topics']]
    tp=r/'work_topics.jsonl';tp.write_text('\n'.join(json.dumps(x) for x in topic_rows)+'\n')
    c.read_json(str(tp),format='newline_delimited',sample_size=-1).create_view('topic_input')
    output('work_vocabulary_topics','SELECT * FROM topic_input')
    # Preserve the actual supplier work package as the source of a supply-chain
    # label. The whole prime description is not this component's capability.
    c.execute("CREATE TEMP TABLE vocabulary_opportunity_inputs AS SELECT episode_id,record_hash,psc,lane,CASE WHEN lane='new_prime_historical_supplier_flow' THEN title WHEN lane IN ('source_item_forecasts','stock_supported_item_demand','published_item_procurement') THEN concat_ws(' ',title,substr(scope_text,1,500)) ELSE concat_ws(' ',title,substr(scope_text,1,3500)) END work_text FROM old_opportunities")
    cw_columns={x[0] for x in c.execute('DESCRIBE old_company_work_matches').fetchall()}
    cw_psc='observed_work_psc' if 'observed_work_psc' in cw_columns else 'NULL::VARCHAR'
    c.execute('CREATE TEMP TABLE vocabulary_scope_inputs AS SELECT '+scope_id('work_text','psc')+' scope_id,min(work_text) work_text,min(psc) psc FROM (SELECT work_text,psc FROM company_capability_history UNION ALL SELECT work_text,psc FROM vocabulary_opportunity_inputs UNION ALL SELECT work_text,'+cw_psc+' psc FROM old_company_work_matches) GROUP BY 1')
    cursor=c.execute('SELECT scope_id,work_text,psc FROM vocabulary_scope_inputs')
    ap=r/'vocabulary_annotations.jsonl';n=0
    with ap.open('w') as stream:
        while True:
            batch=cursor.fetchmany(2000)
            if not batch:break
            for sid,text,psc in batch:
                a=annotate(text,psc,m['as_of'],reference)
                stream.write(json.dumps(dict(scope_id=sid,work_text=text,reported_psc=psc,normalized_scope_label=a['normalized_scope_label'],scope_status=a['scope_status'],topic_ids_json=json.dumps(a['topic_ids']),work_scope_topic_ids_json=json.dumps(a['work_scope_topic_ids']),context_topic_ids_json=json.dumps(a['context_topic_ids']),work_types_json=json.dumps([x['work_type'] for x in a['work_types']]),annotation_json=json.dumps(a),policy_hash=POLICY_HASH,vocabulary_version=VERSION))+'\n');n+=1
            if n%100000==0:print('Annotated distinct work scopes',n,flush=True)
    c.read_json(str(ap),format='newline_delimited',sample_size=-1,maximum_object_size=16777216).create_view('annotation_input')
    output('work_vocabulary_scopes','SELECT * FROM annotation_input');ap.unlink()
    output('company_capability_vocabulary','SELECT h.*,v.scope_id,v.normalized_scope_label,v.scope_status,v.topic_ids_json,v.work_scope_topic_ids_json,v.context_topic_ids_json,v.work_types_json,v.vocabulary_version,v.policy_hash FROM company_capability_history h JOIN new_work_vocabulary_scopes v ON v.scope_id='+scope_id('h.work_text','h.psc'))
    from .vocabulary_reprojection import opportunity_sql,match_sql,POLICY as PSC_SCOPE_POLICY
    c.execute('CREATE TEMP VIEW vocabulary_opportunity_keys AS SELECT o.*,'+scope_id('o.work_text','o.psc')+' scope_id FROM vocabulary_opportunity_inputs o')
    output('opportunity_vocabulary',opportunity_sql())
    c.execute("CREATE TEMP TABLE selected_source_facts AS SELECT * FROM work_match_source_facts QUALIFY row_number() OVER(PARTITION BY company_id,sample_evidence_id ORDER BY coalesce(latest_action_date,first_action_date) DESC,record_id)=1")
    c.create_function('work_evidence_verdict',lambda text,kind,net,pos,rid:json.dumps(dict(zip(('verdict','reason'),evidence_verdict(text,kind,net,pos,rid)))),['VARCHAR','VARCHAR','DOUBLE','DOUBLE','VARCHAR'],'VARCHAR',null_handling='special')
    c.execute('CREATE TEMP TABLE evidence_assessment AS SELECT w.match_id,w.company_id,w.episode_id,w.opportunity_hash,w.profile_hash,w.sample_evidence_id,w.work_text,w.doc_id,w.fit_verdict original_fit_verdict,w.fit_reason original_fit_reason,f.history_kind,f.record_id,f.net_obligations,f.positive_obligations,f.reported_subcontract_amount,f.source_url,f.psc observed_source_psc,work_evidence_verdict(w.work_text,f.history_kind,f.net_obligations,f.positive_obligations,f.record_id) evaluation_json FROM old_company_work_matches w LEFT JOIN selected_source_facts f ON f.company_id=w.company_id AND f.sample_evidence_id=w.sample_evidence_id')
    output('company_match_evidence_reviews',"SELECT *,json_extract_string(evaluation_json,'$.verdict') evidence_verdict,json_extract_string(evaluation_json,'$.reason') evidence_reason,'"+POLICY_HASH+"' evidence_policy_hash FROM evidence_assessment")
    # Keep candidate rows and their prior verdicts; only serving links change.
    output('company_work_matches',"SELECT w.* REPLACE(CASE WHEN e.evidence_verdict='rejected' THEN 'rejected' WHEN e.evidence_verdict='review' AND w.fit_verdict<>'rejected' THEN 'review' ELSE w.fit_verdict END AS fit_verdict,CASE WHEN e.evidence_verdict IN ('rejected','review') THEN e.evidence_reason ELSE w.fit_reason END AS fit_reason,CASE WHEN e.evidence_verdict='rejected' THEN 'not_recommended' WHEN e.evidence_verdict='review' AND w.fit_verdict<>'rejected' THEN 'market_context' ELSE w.review_category END AS review_category,CASE WHEN e.evidence_verdict IN ('rejected','review') THEN e.evidence_reason ELSE w.company_evidence_caveat END AS company_evidence_caveat,CASE WHEN e.evidence_verdict IN ('rejected','review') THEN sha256(concat_ws('|',w.match_id,w.opportunity_hash,w.profile_hash,w.doc_id,w.work_text,'"+POLICY_HASH+"',e.evaluation_json)) ELSE w.match_hash END AS match_hash) FROM old_company_work_matches w JOIN new_company_match_evidence_reviews e USING(match_id)")
    output('company_matches',"SELECT m.* REPLACE(coalesce(w.match_hash,m.match_hash) AS match_hash,coalesce(w.company_evidence_caveat,m.company_evidence_caveat) AS company_evidence_caveat) FROM old_company_matches m LEFT JOIN new_company_work_matches w ON w.company_id=m.company_id AND w.episode_id=m.episode_id WHERE coalesce(w.fit_verdict,'')<>'rejected'")
    output('company_match_assessments',"SELECT a.* REPLACE(w.fit_verdict AS verdict,w.fit_reason AS reason) FROM old_company_match_assessments a JOIN new_company_work_matches w ON w.company_id=a.company_id AND w.episode_id=a.episode_id")
    output('company_match_vocabulary',match_sql('v.scope_id='+scope_id('w.work_text','w.'+cw_psc if cw_psc!='NULL::VARCHAR' else cw_psc),'source_kind' in cw_columns))
    output('work_vocabulary_coverage',"SELECT 'company_capability' population,scope_status,count(*) records,count(DISTINCT company_id) companies FROM new_company_capability_vocabulary GROUP BY scope_status UNION ALL SELECT 'opportunity',scope_status,count(*),cast(NULL AS BIGINT) FROM new_opportunity_vocabulary GROUP BY scope_status")
    m['work_vocabulary_policy']=dict(version=VERSION,policy_hash=POLICY_HASH,topics=len(SPEC['topics']),work_types=len(SPEC['work_types']),psc_nodes=len(ref),psc_edition='GSA PSC April 2025',scope_label_source='Explicit work wording supplements reported PSC; source passages, raw scope and original codes remain retained.',facet_policy='Work scope, platform context and mission context remain separate. Context does not establish the delivered scope.',work_scope_topics=sum(x.get('facet')=='work_scope' for x in SPEC['topics']),context_topics=sum(x.get('facet')!='work_scope' for x in SPEC['topics']),no_equivalence_or_qualification=True,psc_scope_policy=PSC_SCOPE_POLICY,source_checks_query_ids=proof['query_ids'],evidence_effects=dict(c.execute('SELECT evidence_verdict,count(*) FROM new_company_match_evidence_reviews GROUP BY evidence_verdict').fetchall()))
    (r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close();return m

def refresh_current(previous,root,aws,projection_only=False):
    root=Path(root);r=root/'runs'/(datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')+'_'+uuid.uuid4().hex[:6]);r.mkdir(parents=True)
    m=copy.deepcopy(previous);m.update(run_id=r.name,reuse_parent_release=previous['run_id'],refresh_mode='work_vocabulary_source_projection' if projection_only else 'whole_market_work_vocabulary')
    for name,t in m['tables'].items():
        t['reuse_parent_binding']=True
        if t.get('remote_binding'):continue
        uri=next(f['s3_uri'] for f in m['sources'][t['source_binding']]['files'] if f['sha256']==t['sha256']) if t.get('source_binding') else t['s3_uri']+'part-00000.parquet'
        path=r/(name+'.parquet');aws.s3.download_file(BUCKET,uri.split('/',3)[3],str(path))
        if hashlib.sha256(path.read_bytes()).hexdigest()!=t['sha256']:raise ValueError('Immutable vocabulary input changed: '+name)
        t['path']=str(path)
    for name in ('contract_value_cohorts',):
        for i,f in enumerate(m['sources'].get(name,{}).get('files',[])):
            path=r/(name+'_source_'+str(i)+'.parquet');aws.s3.download_file(BUCKET,f['s3_uri'].split('/',3)[3],str(path));f['path']=str(path)
            if hashlib.sha256(path.read_bytes()).hexdigest()!=f['sha256']:raise ValueError('Comparator input changed')
    (r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n')
    try:
        if projection_only:
            from .vocabulary_reprojection import apply
            from .logic_provenance import record
            from .validate import validate,validate_demand_integrity
            apply(r);record(r);validate_demand_integrity(r);validate(r,aws);return r
        augment(r,aws)
        from .pipeline_coverage import build
        from .logic_provenance import record
        from .events import track
        from .validate import validate,validate_demand_integrity
        build(r);record(r);track(r,previous,aws);validate_demand_integrity(r);validate(r);return r
    except Exception:
        from .checkpoint import archive_failed_attempt
        try:archive_failed_attempt(r,aws,previous['run_id'])
        except Exception as error:print('Could not archive vocabulary derivation:',str(error),flush=True)
        raise
