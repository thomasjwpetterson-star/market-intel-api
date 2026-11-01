"""Source-versioned links from inferred work streams to actual SAM notices.

A reference to an incumbent contract is stronger than scope similarity. Neither
closes a multi-year buying stream or establishes applicant eligibility by itself.
"""
import hashlib,json,re
from pathlib import Path
from .aws import BUCKET,PREFIX,DATABASE
from .market_history import GENERIC
from .market_demand import clean,digest


def scope_key(text):
    stops=set(GENERIC)|{'request','quote','quotes','quotation','rfq','rfi','solicitation','notice'}
    return '|'.join(sorted({w for w in re.findall(r'[a-z0-9][a-z0-9-]*',clean(text).lower()) if len(w)>=3 and w not in stops}))

def contract_refs(text):
    return sorted({re.sub('[^A-Z0-9]','',x.upper()) for x in re.findall(r'(?i)\b[A-Z0-9]{6}[- ]?[0-9]{2}[- ]?[A-Z][- ]?[0-9]{4}\b',str(text or ''))})


def prepare(c,m,r,aws,path,output):
    r=Path(r)
    empty="SELECT NULL::VARCHAR relationship_id,NULL::VARCHAR episode_id,NULL::VARCHAR forecast_source_hash,NULL::VARCHAR notice_episode_id,NULL::VARCHAR notice_record_hash,NULL::VARCHAR relationship_kind,NULL::VARCHAR referenced_contract,NULL::VARCHAR notice_title,NULL::VARCHAR notice_source_url,NULL::VARCHAR notice_source_evidence_id,NULL::VARCHAR notice_state,NULL::VARCHAR notice_deadline,NULL::VARCHAR explanation WHERE false"
    if aws is None or not m['sources'].get('sam_notices') or 'published_calls' not in m['tables']:
        output('market_demand_progression',empty);return path
    c.read_parquet(m['tables']['published_calls']['path']).create_view('market_published_calls')
    raw=m['sources']['sam_notices']
    for i,f in enumerate(raw['files']):
        if not f.get('path') or not Path(f['path']).exists():
            dest=r/('market_sam_'+str(i)+'.parquet');aws.s3.download_file(BUCKET,f['s3_uri'].split('/',3)[3],str(dest));f['path']=str(dest)
        if hashlib.sha256(Path(f['path']).read_bytes()).hexdigest()!=f['sha256']:raise ValueError('SAM progression input content changed')
    c.read_parquet([f['path'] for f in raw['files']]).create_view('market_raw_sam')
    c.create_function('market_scope_key',scope_key,['VARCHAR'],'VARCHAR')
    c.create_function('market_contract_refs',contract_refs,['VARCHAR'],duck_list_type())
    c.execute("""CREATE TEMP TABLE market_notice_offices AS SELECT notice_id,CASE WHEN count(DISTINCT nullif(aac_code,''))=1 THEN min(nullif(aac_code,'')) END aac_code FROM market_raw_sam GROUP BY notice_id""")
    c.execute("""CREATE TEMP TABLE market_notice_keys AS SELECT n.*,b.aac_code,market_scope_key(n.title) normalized_scope,
    market_contract_refs(concat_ws(' ',n.title,n.scope_text)) referenced_contracts
    FROM market_published_calls n LEFT JOIN market_notice_offices b ON b.notice_id=n.source_locator WHERE n.source_name='sam_notices'""")
    c.execute("""CREATE TEMP TABLE market_scope_notice_links AS SELECT o.episode_id,n.episode_id notice_episode_id,n.record_hash notice_record_hash,
    'candidate_same_buyer_scope' relationship_kind,NULL::VARCHAR referenced_contract,n.title notice_title,n.source_url notice_source_url,n.source_evidence_id notice_source_evidence_id,n.procurement_state notice_state,n.window_end notice_deadline,
    'The exact buying-office code and normalized work scope match. This is a possible procurement in the buying stream; configuration, quantities, successor identity and eligibility remain checks.' explanation
    FROM market_opportunities o JOIN market_notice_keys n ON json_extract_string(o.value_inputs_json,'$.scope_key')=n.normalized_scope
    WHERE nullif(n.aac_code,'') IS NOT NULL AND regexp_replace(upper(n.aac_code),'[^A-Z0-9]','','g')=regexp_replace(upper(json_extract_string(o.value_inputs_json,'$.customer_key')),'[^A-Z0-9]','','g')""")
    # Expand the small set of explicit references once, so the whole-market
    # join uses equality instead of scanning every notice for every contract.
    c.execute("""CREATE TEMP TABLE market_notice_contracts AS SELECT DISTINCT n.* EXCLUDE(referenced_contracts),r.contract_reference
    FROM market_notice_keys n,UNNEST(n.referenced_contracts) r(contract_reference)""")
    prime=m['sources'].get('native_company_awards',{}).get('table')
    current=m['sources'].get('market_continuation_work',{}).get('table')
    if prime and current:
        sql='SELECT DISTINCT f.cohort_id,f.record_id,a.piid,a.evidence_id,a.source_url FROM '+DATABASE+'.'+current+' f JOIN '+DATABASE+'.'+prime+' a ON a.award_key=f.record_id WHERE nullif(a.piid,\'\') IS NOT NULL'
        src=aws.extract('market_contract_reference_index',sql,r/'market_contract_reference_sources',m['run_id']+'_reference_index');src.update(as_of=m['as_of'],kind='exact_held_award_reference_projection')
        c.read_parquet([f['path'] for f in src['files']]).create_view('market_contract_reference_source');src['rows']=c.execute('SELECT count(*) FROM market_contract_reference_source').fetchone()[0];m['sources']['market_contract_reference_index']=src
        output('market_contract_reference_index','SELECT * FROM market_contract_reference_source')
        c.execute("""CREATE TEMP TABLE market_explicit_notice_links AS SELECT o.episode_id,n.episode_id notice_episode_id,n.record_hash notice_record_hash,
        'explicit_incumbent_contract_reference' relationship_kind,r.piid referenced_contract,n.title notice_title,n.source_url notice_source_url,n.source_evidence_id notice_source_evidence_id,n.procurement_state notice_state,n.window_end notice_deadline,
        'The agency notice explicitly references the exact incumbent contract behind this planning record. Inspect the notice type and current scope; a contract reference alone does not prove a replacement award or eligibility.' explanation
        FROM market_opportunities o JOIN market_contract_reference_source r ON r.record_id=json_extract_string(o.value_inputs_json,'$.record_id')
        JOIN market_notice_contracts n ON n.contract_reference=regexp_replace(upper(r.piid),'[^A-Z0-9]','','g') WHERE o.lane='funded_contract_follow_on'""")
    else:c.execute('CREATE TEMP TABLE market_explicit_notice_links AS SELECT * FROM market_scope_notice_links WHERE false')
    c.execute("""CREATE TEMP TABLE market_notice_links AS SELECT * FROM market_explicit_notice_links UNION ALL SELECT s.* FROM market_scope_notice_links s WHERE NOT EXISTS(SELECT 1 FROM market_explicit_notice_links e WHERE e.episode_id=s.episode_id AND e.notice_episode_id=s.notice_episode_id)""")
    rows=c.execute('SELECT * FROM market_notice_links').fetchall();keys=[d[0] for d in c.description];by={}
    for values in rows:
        row=dict(zip(keys,values));by.setdefault(row['episode_id'],[]).append(row)
    updated=r/'market_opportunities_with_notices.jsonl'
    with updated.open('w') as out:
        for line in Path(path).open():
            o=json.loads(line);links=sorted(by.get(o['episode_id'],[]),key=lambda x:(x['notice_episode_id'],x['relationship_kind']))
            if links:
                inputs=json.loads(o['value_inputs_json']);inputs['related_procurement_events']=sorted(links,key=lambda x:x['notice_episode_id']);inputs['procurement_relationships_digest']=digest(links)
                o['value_inputs_json']=json.dumps(inputs,sort_keys=True,default=str);o.pop('record_hash',None);o['record_hash']=digest(o)
            out.write(json.dumps(o,default=str)+'\n')
    c.read_json(str(updated),format='newline_delimited',sample_size=-1).create_view('market_opportunities',replace=True)
    output('market_demand_progression',"""SELECT sha256(concat_ws('|',l.episode_id,l.notice_episode_id,l.relationship_kind)) relationship_id,l.episode_id,o.record_hash forecast_source_hash,l.* EXCLUDE(episode_id)
    FROM market_notice_links l JOIN market_opportunities o USING(episode_id) QUALIFY row_number() OVER(PARTITION BY l.episode_id,l.notice_episode_id ORDER BY l.relationship_kind)=1""")
    return updated


def duck_list_type():
    import duckdb
    return duckdb.list_type('VARCHAR')
