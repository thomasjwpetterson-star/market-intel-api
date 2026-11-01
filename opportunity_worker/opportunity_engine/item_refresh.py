"""Refresh monthly item evidence while preserving the validated non-item graph."""
import copy,datetime,hashlib,json,uuid
from pathlib import Path
import duckdb
from .aws import BUCKET,PREFIX
from . import rules

ITEMS=('published_item_procurement','source_item_forecasts','stock_supported_item_demand')


def preserve_nonitem(c,parent_opportunities,parent_decisions,new_demand,as_of):
    """Build only item decisions; retain every non-item source version exactly."""
    c.read_parquet(str(parent_opportunities)).create_view('parent_opportunities')
    c.read_parquet(str(parent_decisions)).create_view('parent_decisions')
    c.read_parquet(str(new_demand)).create_view('item_demand_input')
    lanes=','.join("'"+x+"'" for x in ITEMS)
    c.execute('CREATE TEMP VIEW updated_opportunities AS SELECT * FROM parent_opportunities WHERE lane NOT IN ('+lanes+') UNION ALL BY NAME SELECT * FROM item_demand_input WHERE lane IN ('+lanes+')')
    q=c.execute('SELECT * FROM updated_opportunities WHERE lane IN ('+lanes+') ORDER BY episode_id');names=[x[0] for x in q.description]
    decisions=[]
    for row in q.fetchall():
        o=dict(zip(names,row));d=rules.evaluate(o,as_of);decisions.append(dict(episode_id=o['episode_id'],source_record_hash=o['record_hash'],rule_id=d['rule_id'],stage=d['stage'],decision_hash=d['decision_hash'],decision_json=json.dumps(d,sort_keys=True)))
    return decisions


def refresh(previous,root,aws):
    from .ingestion.refresh_dla_monthly import refresh as refresh_monthly
    from .company_references import refresh as refresh_references
    from .historical_purchases import refresh as refresh_history
    from .demand_history import refresh_forecast_episodes
    from .item_demand import prepare as prepare_items,augment as augment_items
    from .replenishment import prepare as prepare_stock,augment as augment_stock
    from .company_enrichment import augment as augment_companies
    from .quality import apply as apply_quality
    from .item_presence import hold
    from .item_requests import link
    from .item_sources import augment as augment_sources
    from .catalogue_sales import augment as augment_sales
    from .product_market import build as build_market
    from .pipeline_coverage import build as build_coverage
    from .logic_provenance import record
    from .events import track
    from .validate import validate,validate_demand_integrity
    root=Path(root);m=copy.deepcopy(previous)
    # Keep the validated assessment date. This operation updates published
    # source evidence, rather than re-dating the untouched annual/prime graph.
    r=root/'runs'/(datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')+'_'+uuid.uuid4().hex[:6]);r.mkdir(parents=True)
    m.update(run_id=r.name,reuse_parent_release=previous['run_id'],refresh_mode='monthly_item_evidence_update')
    def download(uri,sha,label):
        if not uri.startswith('s3://'+BUCKET+'/'+PREFIX+'/'):raise ValueError('Invalid item refresh source URI')
        p=root/'sources'/(sha+'.parquet');p.parent.mkdir(exist_ok=True)
        if not p.exists():aws.s3.download_file(BUCKET,uri.split('/',3)[3],str(p))
        if hashlib.sha256(p.read_bytes()).hexdigest()!=sha:raise ValueError('Item refresh checksum differs: '+label)
        return str(p)
    for name,t in m['tables'].items():
        t['reuse_parent_binding']=True
        if t.get('remote_binding'):continue
        uri=next(f['s3_uri'] for f in m['sources'][t['source_binding']]['files'] if f['sha256']==t['sha256']) if t.get('source_binding') else t['s3_uri']+'part-00000.parquet'
        t['path']=download(uri,t['sha256'],name)
    for name in ('company_work','item_reference_facts','dla_solicitations','contract_value_cohorts'):
        for f in m['sources'].get(name,{}).get('files',[]):f['path']=download(f['s3_uri'],f['sha256'],name)
    # Source-specific raw archives and snapshots remain immutable. A failed HTTP
    # check retains the last complete monthly source with its original dates.
    m['sources']['item_reference_facts']=refresh_references(m['sources']['item_reference_facts'],root,aws)
    m['sources']['historical_item_purchase_lines']=refresh_history(m,root,aws,m['as_of'])
    monthly=refresh_monthly(root);m['sources'].update(monthly['sources'])
    current_opportunities=m['tables']['opportunities']['path'];current_decisions=m['tables']['demand_decisions']['path']
    m['sources']['demand_candidates']=dict(files=[dict(path=current_opportunities,sha256=m['tables']['opportunities']['sha256'])])
    s=m['sources'];s.update(refresh_forecast_episodes(s,r/'forecast_inputs',m['as_of']));s.update(prepare_items(s,r/'item_inputs',m['as_of']));s.update(prepare_stock(s,r/'stock_inputs',m['as_of']))
    c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='3GB';SET preserve_insertion_order=false");c.execute("SET temp_directory='"+str(r/'incremental_tmp').replace("'","''")+"'")
    def emit(name,sql):
        c.execute('CREATE OR REPLACE TEMP VIEW raw_result AS '+sql)
        fields=c.execute('DESCRIBE raw_result').fetchall();cast=[]
        for n,t,*_ in fields:
            target='VARCHAR' if t in ('DATE','TIMESTAMP','JSON') else 'DOUBLE' if t.startswith('DECIMAL') else 'BIGINT' if t=='HUGEINT' else t
            cast.append('cast("'+n+'" AS '+target+') "'+n+'"')
        c.execute('CREATE OR REPLACE TEMP VIEW result AS SELECT '+','.join(cast)+' FROM raw_result');p=r/(name+'.parquet');c.execute('COPY result TO ? (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 2048)',[str(p)])
        cols=[dict(Name=n,Type={'VARCHAR':'string','DOUBLE':'double','BIGINT':'bigint','INTEGER':'bigint','BOOLEAN':'boolean'}[t]) for n,t,*_ in c.execute('DESCRIBE result').fetchall()]
        m['tables'][name]=dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),rows=c.execute('SELECT count(*) FROM read_parquet(?)',[str(p)]).fetchone()[0],columns=cols);print('Incremental item output',name,m['tables'][name]['rows'],flush=True)
    decisions=preserve_nonitem(c,current_opportunities,current_decisions,s['demand_candidates']['path'],m['as_of']);raw=r/'item_decisions.jsonl';raw.write_text(''.join(json.dumps(x)+'\n' for x in decisions));del decisions;c.read_json(str(raw),columns={n:'VARCHAR' for n in ('episode_id','source_record_hash','rule_id','stage','decision_hash','decision_json')}).create_view('new_item_decisions')
    emit('opportunities','SELECT * FROM updated_opportunities')
    lanes=','.join("'"+x+"'" for x in ITEMS)
    emit('demand_decisions','SELECT d.* FROM parent_decisions d JOIN parent_opportunities o USING(episode_id) WHERE o.lane NOT IN ('+lanes+') UNION ALL BY NAME SELECT * FROM new_item_decisions')
    for name in ('dla_forecasts','dla_inventory','item_price_references','item_market_price_evidence','item_replenishment_context','item_reference_facts'):
        c.read_parquet([f['path'] for f in s[name]['files']]).create_view('source_'+name);emit(name,'SELECT * FROM source_'+name)
        if name=='item_reference_facts':m['tables'][name]['source_binding']='item_reference_facts'
    c.read_parquet(m['tables']['company_matches']['path']).create_view('parent_matches')
    emit('company_matches','SELECT x.* FROM parent_matches x JOIN parent_opportunities o USING(episode_id) WHERE o.lane NOT IN ('+lanes+')')
    m['item_refresh_parent']=previous['run_id'];m['item_refresh_nonitem_graph_preserved']=True
    (r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close();validate_demand_integrity(r)
    try:
        augment_companies(r,skip_work=True)
        from .sam_approved import augment as approved
        if approved(r):
            from .notice_prices import refresh as prices
            prices(r)
        augment_items(r);augment_stock(r);apply_quality(r);hold(r);link(r);augment_sources(r);augment_sales(r);build_market(r);build_coverage(r);record(r);track(r,previous,aws);validate(r)
        return r
    except Exception:
        from .checkpoint import archive_failed_attempt
        archive_failed_attempt(r,aws,previous['run_id']);raise
