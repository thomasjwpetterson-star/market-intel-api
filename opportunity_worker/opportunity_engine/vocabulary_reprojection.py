"""Keep linked-prime classification separate from supplier-delivered work."""
import hashlib,json
from pathlib import Path
import duckdb

POLICY='supplier-psc-context-v1'
SUPPLIER_LANES=('new_prime_historical_supplier_flow','funded_prime_supplier_packages','recurring_subcontract_work')

def opportunity_sql():
    supplier="o.lane IN ("+','.join("'"+x+"'" for x in SUPPLIER_LANES)+")"
    contextual=supplier+" AND json_array_length(v.work_scope_topic_ids_json)=0 AND v.scope_status<>'administrative_only'"
    label="CASE WHEN "+contextual+" THEN 'Supplier work scope needs classification' ELSE v.normalized_scope_label END"
    status="CASE WHEN "+contextual+" THEN CASE WHEN nullif(v.reported_psc,'') IS NOT NULL OR json_array_length(v.context_topic_ids_json)>0 THEN 'context_only' ELSE 'unclassified_scope' END ELSE v.scope_status END"
    basis="CASE WHEN "+supplier+" THEN 'linked_prime_context' ELSE 'reported_work_classification' END"
    work_psc="CASE WHEN NOT ("+supplier+") THEN v.reported_psc END"
    parent_psc="CASE WHEN "+supplier+" THEN o.psc END"
    annotation="json_merge_patch(v.annotation_json,json_object('psc_scope',"+basis+",'work_psc',"+work_psc+",'parent_prime_psc',"+parent_psc+",'normalized_scope_label',"+label+",'scope_status',"+status+"))"
    return "SELECT o.episode_id,o.record_hash,v.* EXCLUDE(work_text) REPLACE("+label+" AS normalized_scope_label,"+status+" AS scope_status,"+annotation+" AS annotation_json),"+basis+" psc_scope,"+work_psc+" work_psc,"+parent_psc+" parent_prime_psc,CASE WHEN "+supplier+" THEN json_extract_string(v.annotation_json,'$.psc_label') END parent_prime_psc_label FROM vocabulary_opportunity_keys o JOIN new_work_vocabulary_scopes v USING(scope_id)"

def match_sql(company_scope_join,has_source_kind=True):
    supplier="w.source_kind='subcontract_report'" if has_source_kind else 'false'
    label="CASE WHEN "+supplier+" AND json_array_length(v.work_scope_topic_ids_json)=0 AND nullif(v.reported_psc,'') IS NOT NULL THEN 'Supplier work scope needs classification' ELSE v.normalized_scope_label END"
    return """SELECT w.match_id,w.company_id,w.episode_id,w.opportunity_hash,w.sample_evidence_id,v.scope_id company_scope_id,o.scope_id opportunity_scope_id,
    LABEL company_scope_label,o.normalized_scope_label opportunity_scope_label,
    to_json(list_intersect(from_json(v.topic_ids_json,'["VARCHAR"]'),from_json(o.topic_ids_json,'["VARCHAR"]'))) shared_topic_ids_json,
    to_json(list_intersect(from_json(v.work_scope_topic_ids_json,'["VARCHAR"]'),from_json(o.work_scope_topic_ids_json,'["VARCHAR"]'))) shared_work_scope_topic_ids_json,
    to_json(list_intersect(from_json(v.context_topic_ids_json,'["VARCHAR"]'),from_json(o.context_topic_ids_json,'["VARCHAR"]'))) shared_context_topic_ids_json,
    v.work_types_json company_work_types_json,o.work_types_json opportunity_work_types_json,w.fit_verdict,w.review_category,
    o.psc_scope opportunity_psc_scope,o.work_psc opportunity_work_psc,o.parent_prime_psc,
    CASE WHEN SUPPLIER THEN 'linked_prime_context' ELSE 'reported_work_classification' END company_psc_scope,
    v.policy_hash vocabulary_policy_hash
    FROM new_company_work_matches w JOIN new_work_vocabulary_scopes v ON SCOPE_JOIN
    JOIN new_opportunity_vocabulary o ON o.episode_id=w.episode_id AND o.record_hash=w.opportunity_hash""".replace('LABEL',label).replace('SCOPE_JOIN',company_scope_join).replace('SUPPLIER',supplier)

def apply(directory):
    """Reproject unchanged, validated scope annotations; no matching rerun."""
    from .work_vocabulary import POLICY_HASH
    from .vocabulary_refresh import scope_id
    r=Path(directory);m=json.loads((r/'manifest.json').read_text())
    if m.get('work_vocabulary_policy',{}).get('policy_hash')!=POLICY_HASH:raise ValueError('Source projection requires the exact current annotation policy')
    c=duckdb.connect();c.execute("SET threads=2;SET memory_limit='3GB';SET preserve_insertion_order=false")
    for name,alias in [('opportunities','old_opportunities'),('company_work_matches','new_company_work_matches'),('work_vocabulary_scopes','new_work_vocabulary_scopes'),('opportunity_vocabulary','old_opportunity_vocabulary'),('company_capability_vocabulary','new_company_capability_vocabulary')]:
        t=m['tables'][name];p=Path(t['path'])
        if hashlib.sha256(p.read_bytes()).hexdigest()!=t['sha256']:raise ValueError('Projection input changed: '+name)
        c.read_parquet(str(p)).create_view(alias)
    c.execute('CREATE TEMP TABLE vocabulary_opportunity_keys AS SELECT o.episode_id,o.record_hash,o.lane,o.psc,v.scope_id FROM old_opportunities o JOIN old_opportunity_vocabulary v ON v.episode_id=o.episode_id AND v.record_hash=o.record_hash')
    def output(name,sql):
        c.execute('CREATE OR REPLACE TEMP VIEW projection_result AS '+sql)
        p=r/(name+'.parquet');tmp=r/(name+'.projection.parquet')
        types={'VARCHAR':'string','JSON':'string','BIGINT':'bigint','INTEGER':'bigint','UBIGINT':'bigint','DOUBLE':'double','BOOLEAN':'boolean'}
        cols=[dict(Name=n,Type=types[t]) for n,t,*_ in c.execute('DESCRIBE projection_result').fetchall()]
        c.execute('COPY projection_result TO ? (FORMAT PARQUET,COMPRESSION ZSTD)',[str(tmp)]);tmp.replace(p)
        rows=c.execute('SELECT count(*) FROM read_parquet(?)',[str(p)]).fetchone()[0]
        m['tables'][name]=dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),rows=rows,columns=cols)
        c.read_parquet(str(p)).create_view('new_'+name,replace=True);print('Vocabulary source projection',name,rows,flush=True)
    output('opportunity_vocabulary',opportunity_sql())
    columns={n for n,*_ in c.execute('DESCRIBE new_company_work_matches').fetchall()}
    psc='w.observed_work_psc' if 'observed_work_psc' in columns else 'NULL::VARCHAR'
    output('company_match_vocabulary',match_sql('v.scope_id='+scope_id('w.work_text',psc),'source_kind' in columns))
    output('work_vocabulary_coverage',"SELECT 'company_capability' population,scope_status,count(*) records,count(DISTINCT company_id) companies FROM new_company_capability_vocabulary GROUP BY scope_status UNION ALL SELECT 'opportunity',scope_status,count(*),cast(NULL AS BIGINT) FROM new_opportunity_vocabulary GROUP BY scope_status")
    m['work_vocabulary_policy']['psc_scope_policy']=POLICY
    m['vocabulary_projection_refresh']=dict(annotation_policy_unchanged=True,company_matches_unchanged=True,source_opportunities_unchanged=True)
    (r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close();return m
