"""Recover held annual DLA purchases and retain already observed price history.

This source is historical market context. It never creates or prices a future
opportunity, establishes delivered manufacturer, or changes upstream datasets.
"""
import copy,datetime,hashlib,json,re
from pathlib import Path
import duckdb
from .aws import BUCKET,PREFIX

KEY='bronze/dla/contract_history/2025ContractHistory.txt'
FIELDS='pricing_record_id source_record_id niin nsn part_number item_name supplier_cage supplier_name contract_number transaction_date record_amount_usd quantity quantity_unit unit_price_usd source_url source_s3_uri source_row_locator source_sha256'.split()
NUMERIC={'record_amount_usd','quantity','unit_price_usd'}


def compile_history(path,folder,binding,as_of):
    folder=Path(folder);folder.mkdir(parents=True,exist_ok=True);c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='2GB';SET preserve_insertion_order=false")
    c.execute("SET temp_directory='"+str(folder/'tmp').replace("'","''")+"'")
    c.read_csv(str(path),delimiter='|',header=True,all_varchar=True,strict_mode=True).create_view('raw')
    required=set('NIIN SECURITY_CLASSIFICATION FSC UNIT CAGE CONTRACT_NUMBER ORDER_QTY AWARD_DATE NETPRICE PO_NUM PO_ITMNO ITEM_NAME PART_NUMBER STD_U_PRICE NSN'.split())
    if not required<={n.upper() for n,*_ in c.execute('DESCRIBE raw').fetchall()}:raise ValueError('Held annual purchase schema differs')
    c.execute("CREATE TEMP TABLE public AS SELECT * FROM raw WHERE SECURITY_CLASSIFICATION='U'")
    invalid=c.execute("""SELECT count(*) FROM public WHERE NOT coalesce(regexp_full_match(NIIN,'[0-9]{9}') AND regexp_full_match(NSN,'[0-9]{13}') AND right(NSN,9)=NIIN AND regexp_full_match(CAGE,'[A-Z0-9]{5}') AND nullif(trim(UNIT),'') IS NOT NULL AND try_strptime(AWARD_DATE,'%Y%m%d') IS NOT NULL AND substr(AWARD_DATE,1,4)='2025' AND cast(try_strptime(AWARD_DATE,'%Y%m%d') AS DATE)<=cast(? AS DATE),false)""",[as_of]).fetchone()[0]
    if invalid:raise ValueError('Malformed public annual purchase rows: '+str(invalid))
    key="sha256(concat_ws(chr(31),"+','.join("coalesce(trim(\""+n+"\"),'<NULL>')" for n in 'NIIN FSC UNIT CAGE CONTRACT_NUMBER ORDER_QTY AWARD_DATE NETPRICE PO_NUM PO_ITMNO'.split())+'))'
    c.execute('CREATE TEMP TABLE keyed AS SELECT *,'+key+' financial_id FROM public')
    # Conflicting quantities/prices/units for a purchase identity are not summed.
    conflicts=c.execute("SELECT count(*) FROM (SELECT NIIN,CAGE,CONTRACT_NUMBER,PO_NUM,PO_ITMNO,AWARD_DATE FROM keyed GROUP BY ALL HAVING count(DISTINCT (UNIT,try_cast(ORDER_QTY AS DOUBLE),try_cast(NETPRICE AS DOUBLE)))>1)").fetchone()[0]
    if conflicts:raise ValueError('Conflicting annual financial versions: '+str(conflicts))
    c.execute("""CREATE TEMP TABLE lines AS SELECT financial_id,min(NIIN) NIIN,CASE WHEN count(DISTINCT NSN)=1 THEN min(NSN) END NSN,min(FSC) FSC,min(UNIT) UNIT,min(CAGE) CAGE,min(CONTRACT_NUMBER) CONTRACT_NUMBER,min(ORDER_QTY) ORDER_QTY,min(AWARD_DATE) AWARD_DATE,min(NETPRICE) NETPRICE,min(PO_NUM) PO_NUM,min(PO_ITMNO) PO_ITMNO,
    CASE WHEN count(DISTINCT nullif(trim(PART_NUMBER),''))=1 THEN min(nullif(trim(PART_NUMBER),'')) END part_number,
    CASE WHEN count(DISTINCT nullif(trim(ITEM_NAME),''))=1 THEN min(nullif(trim(ITEM_NAME),'')) END item_name,
    count(DISTINCT nullif(trim(PART_NUMBER),'')) part_count FROM keyed GROUP BY financial_id""")
    p=folder/'annual_purchases.parquet';sha=binding['sha256'];uri='s3://'+BUCKET+'/'+binding['key']
    # A stored source is not given an invented original government download URL.
    c.execute("""CREATE TEMP TABLE projected AS SELECT sha256('dla-monthly-line-v1|'||financial_id) pricing_record_id,financial_id source_record_id,NIIN niin,NSN nsn,part_number,item_name,CAGE supplier_cage,NULL::VARCHAR supplier_name,CONTRACT_NUMBER contract_number,
    cast(cast(strptime(AWARD_DATE,'%Y%m%d') AS DATE) AS VARCHAR) transaction_date,try_cast(ORDER_QTY AS DOUBLE)*try_cast(NETPRICE AS DOUBLE) record_amount_usd,try_cast(ORDER_QTY AS DOUBLE) quantity,UNIT quantity_unit,try_cast(NETPRICE AS DOUBLE) unit_price_usd,
    NULL::VARCHAR source_url,?::VARCHAR source_s3_uri,PO_NUM||' | '||PO_ITMNO source_row_locator,?::VARCHAR source_sha256
    FROM lines WHERE try_cast(ORDER_QTY AS DOUBLE)>0 AND try_cast(NETPRICE AS DOUBLE)>0 AND isfinite(try_cast(ORDER_QTY AS DOUBLE)) AND isfinite(try_cast(NETPRICE AS DOUBLE))""",[uri,sha])
    c.execute("COPY projected TO ? (FORMAT PARQUET,COMPRESSION ZSTD)",[str(p)])
    rows=c.execute('SELECT count(*) FROM read_parquet(?)',[str(p)]).fetchone()[0]
    stats=dict(raw_rows=c.execute('SELECT count(*) FROM raw').fetchone()[0],public_rows=c.execute('SELECT count(*) FROM public').fetchone()[0],physical_financial_lines=c.execute('SELECT count(*) FROM lines').fetchone()[0],usable_positive_lines=rows,ambiguous_part_lines=c.execute('SELECT count(*) FROM lines WHERE part_count>1').fetchone()[0],financial_version_conflicts=conflicts,malformed_reported_fsc_rows=c.execute("SELECT count(*) FROM public WHERE NOT coalesce(regexp_full_match(FSC,'[0-9]{4}'),false)").fetchone()[0],fsc_nsn_classification_differences=c.execute("SELECT count(*) FROM public WHERE left(NSN,4)<>FSC").fetchone()[0])
    if not rows:raise ValueError('Held annual purchase projection is empty')
    c.close();return p,stats


def load_files(source,root,aws):
    paths=[]
    for f in source.get('files',[]):
        path=Path(f.get('path') or root/(f['sha256']+'.parquet'))
        if not path.exists():
            uri=f['s3_uri']
            if not uri.startswith('s3://'+BUCKET+'/'+PREFIX+'/sources/'):raise ValueError('History cache outside owned source namespace')
            path=root/(f['sha256']+'.parquet');aws.s3.download_file(BUCKET,uri.split('/',3)[3],str(path))
        if hashlib.sha256(path.read_bytes()).hexdigest()!=f['sha256']:raise ValueError('Historical purchase cache checksum differs')
        f['path']=str(path);paths.append(str(path))
    return paths


def merge(paths,folder,as_of,replaced_source=None):
    """First sources take precedence on the same physical order/line/date/item."""
    folder=Path(folder);folder.mkdir(parents=True,exist_ok=True);c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='2GB';SET preserve_insertion_order=false")
    c.execute("SET temp_directory='"+str(folder/'tmp').replace("'","''")+"'")
    parts=[]
    for index,path in enumerate(paths):
        name='input_'+str(index);c.read_parquet(path).create_view(name)
        fields={n for n,*_ in c.execute('DESCRIBE '+name).fetchall()}
        required=set(FIELDS)-{'contract_number'}
        if not required<=fields:raise ValueError('Historical purchase cache schema differs')
        columns=','.join(('cast("'+n+'" AS '+('DOUBLE' if n in NUMERIC else 'VARCHAR')+')' if n in fields else 'NULL::VARCHAR')+' "'+n+'"' for n in FIELDS)
        parts.append('SELECT '+columns+','+str(index)+' source_priority FROM '+name)
    if not parts:raise ValueError('No historical purchase inputs')
    c.execute('CREATE TEMP VIEW input_rows AS '+' UNION ALL '.join(parts))
    if replaced_source:
        c.execute('CREATE TEMP TABLE inputs AS SELECT * FROM input_rows WHERE NOT (source_s3_uri=? AND source_sha256<>?)',list(replaced_source))
    else:c.execute('CREATE TEMP VIEW inputs AS SELECT * FROM input_rows')
    # The original PO/line locator remains usable; blank locators use the source
    # physical ID. Preference is explicit, so overlapping snapshots never add cash.
    sql="""SELECT * EXCLUDE(source_priority,rn) FROM (SELECT *,row_number() OVER(PARTITION BY niin,supplier_cage,substr(transaction_date,1,10),coalesce(nullif(trim(source_row_locator),''),source_record_id) ORDER BY source_priority,source_record_id,pricing_record_id) rn FROM inputs WHERE record_amount_usd>0 AND quantity>0 AND unit_price_usd>0 AND isfinite(record_amount_usd) AND isfinite(quantity) AND isfinite(unit_price_usd) AND try_cast(substr(transaction_date,1,10) AS DATE)>cast(? AS DATE)-INTERVAL 16 MONTH AND try_cast(substr(transaction_date,1,10) AS DATE)<=cast(? AS DATE)) WHERE rn=1"""
    p=folder/'historical_item_purchase_lines.parquet';c.execute('CREATE TEMP TABLE selected AS '+sql,[as_of,as_of]);c.execute('COPY selected TO ? (FORMAT PARQUET,COMPRESSION ZSTD)',[str(p)]);rows=c.execute('SELECT count(*) FROM read_parquet(?)',[str(p)]).fetchone()[0];c.close();return p,rows


def refresh(previous,root,aws,as_of):
    root=Path(root);now=datetime.datetime.now(datetime.timezone.utc).isoformat();out=root/'inputs'/('historical_purchases_'+now.replace(':','').replace('-',''));out.mkdir(parents=True,exist_ok=True)
    prior=copy.deepcopy(previous.get('sources',{}).get('historical_item_purchase_lines'));result=None
    earlier=load_files(prior,out,aws) if prior else []
    try:
        head=aws.s3.head_object(Bucket=BUCKET,Key=KEY);version=head.get('VersionId')
        if not version or not 1<=head['ContentLength']<=256*1024**2:raise ValueError('Historical purchase source is not a bounded versioned object')
        same=prior and prior.get('upstream_binding',{}).get('version_id')==version
        archives=list(prior.get('raw_archives',[])) if prior else []
        if same:binding=prior['upstream_binding'];stats=prior.get('annual_source_validation',{});annual=[]
        else:
            raw=out/'2025ContractHistory.txt';aws.s3.download_file(BUCKET,KEY,str(raw),ExtraArgs={'VersionId':version})
            if raw.stat().st_size!=head['ContentLength']:raise ValueError('Historical source size changed')
            sha=hashlib.sha256(raw.read_bytes()).hexdigest();binding=dict(key=KEY,version_id=version,sha256=sha,bytes=head['ContentLength'],last_modified=str(head['LastModified']))
            compiled,stats=compile_history(raw,out/'annual',binding,as_of);annual=[str(compiled)];archive_key=PREFIX+'/sources/content/sha256='+sha+'/source.archive';aws.upload(raw,archive_key)
            archives=[x for x in archives if x.get('source_role')!='Held annual DLA public purchase history']+[dict(path=str(raw),sha256=sha,s3_uri='s3://'+BUCKET+'/'+archive_key,original_s3_uri='s3://'+BUCKET+'/'+KEY,original_s3_version_id=version,source_role='Held annual DLA public purchase history')]
        # Preserve previous dated physical purchases when a monthly/yearly source
        # rolls forward. Their own source hashes and raw archives remain attached.
        previous_prices=previous.get('sources',{}).get('item_market_price_evidence',{})
        recent=load_files(previous_prices,out,aws)
        for raw in previous_prices.get('raw_archives',[])+previous.get('sources',{}).get('item_market_price_lines',{}).get('raw_archives',[]):
            if re.search(r'_(?:prochist|contracthist)\.zip(?:\?|$)',raw.get('url',''),re.I) and not any(x['sha256']==raw['sha256'] for x in archives):archives.append(raw)
        paths=recent+annual+earlier;p,rows=merge(paths,out/'merged',as_of,('s3://'+BUCKET+'/'+KEY,binding['sha256']));sha=hashlib.sha256(p.read_bytes()).hexdigest();key=PREFIX+'/sources/content/sha256='+sha+'/source.parquet';aws.upload(p,key)
        result=dict(path=str(p),files=[dict(path=str(p),sha256=sha,s3_uri='s3://'+BUCKET+'/'+key)],sha256=sha,rows=rows,as_of=as_of,kind='retained_dated_physical_purchase_history',upstream_binding=binding,annual_source_validation=stats,raw_archives=archives,interpretation='Sixteen-month retained window of held annual and already observed dated physical purchases. Snapshot overlaps are retained once with explicit source precedence. Exact parts remain null when ambiguous. Historical market context only, not future demand, manufacturer revenue or technical qualification.',refresh_health=dict(status='held_annual_history_checked_and_observed_purchases_retained',attempted_at=now,source_release='2025:'+version))
    except Exception as error:
        if not prior:raise
        result=dict(prior,refresh_health=dict(status='retained_historical_purchases_source_check_failed',attempted_at=now,reason=type(error).__name__,interpretation='Last complete purchase history and original transaction dates retained.'))
    (out/'manifest.json').write_text(json.dumps({'sources':{'historical_item_purchase_lines':result}},indent=2)+'\n');return result
