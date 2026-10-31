"""Strict public DLA monthly snapshots in the owned opportunity namespace.

Procurement lines are physical spending; contract-part mappings are attributes.
Rolling stock files are snapshots, never additive demand or annual revenue.
"""
import datetime,hashlib,json,re,shutil,zipfile
from pathlib import Path
import duckdb

PAGE='https://www.dla.mil/FOIA/Electronic-Reading-Room/'
HEADERS={
'forecast':'niin|FCST_QTY|FCST_DT|SECURITY_CLASSIFICATION',
'inventory':'NSN|TOTAL_STOCK|SECURITY_CLASSIFICATION|date_stamp|cond_cd|bo_qty|adq',
'reorder':'NSN|total_stock|rop|SECURITY_CLASSIFICATION|date_stamp|cond_cd',
'procurement':'NIIN|SECURITY_CLASSIFICATION|FSC|CLIN|UNIT|CAGE|CONTRACT_NUMBER|REFERENCED_PIID|ORDER_QTY|AWARD_DATE|NETPRICE|STD_U_PRICE|PO_NUM|PO_ITMNO|PIINSPIINMod|SOLIC_AMENDMENT_NUMBER',
'parts':'NIIN|SECURITY_CLASSIFICATION|FSC|UNIT|CAGE|CONTRACT_NUMBER|ORDER_QTY|AWARD_DATE|NETPRICE|PO_NUM|PO_ITMNO|ITEM_NAME|PART_NUMBER|STD_U_PRICE|NSN'}


def compile_sources(archives,folder,release,as_of):
 if not re.fullmatch(r'20\d{2}-\d{2}-\d{2}-\d{4}',release) or release[:10]>as_of:raise ValueError('Invalid or future DLA source release')
 required=set(HEADERS)
 if not required.issubset(archives):raise ValueError('Incomplete monthly source family')
 r=Path(folder);r.mkdir(parents=True,exist_ok=True);c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='3GB';SET preserve_insertion_order=false");c.execute("SET temp_directory='"+str(r/'tmp').replace("'","''")+"'")
 raw=[];stats={};results={}
 for family in sorted(required):
  meta=archives[family];path=Path(meta['path']);sha=hashlib.sha256(path.read_bytes()).hexdigest()
  if meta.get('sha256') and meta['sha256']!=sha:raise ValueError('Archive hash changed: '+family)
  raw.append(dict(path=str(path),sha256=sha,url=meta['url']));paths=[]
  with zipfile.ZipFile(path) as z:
   members=[x for x in z.infolist() if not x.is_dir()]
   if not members or sum(x.file_size for x in members)>2_000_000_000:raise ValueError('Archive population outside bounds: '+family)
   for i,member in enumerate(members):
    if not member.filename.lower().endswith('.txt') or '..' in Path(member.filename).parts or Path(member.filename).is_absolute():raise ValueError('Unexpected archive member')
    dest=r/family/Path(member.filename).name;dest.parent.mkdir(exist_ok=True)
    with z.open(member) as source:
     header=source.readline().decode('utf-8-sig').strip()
     if header.lower()!=HEADERS[family].lower():raise ValueError('Unrecognized '+family+' layout')
    with z.open(member) as source,dest.open('wb') as target:shutil.copyfileobj(source,target,1024*1024)
    paths.append(str(dest))
  path_sql='['+','.join("'"+p.replace("'","''")+"'" for p in paths)+']'
  c.execute("CREATE TEMP VIEW raw_"+family+" AS SELECT * FROM read_csv("+path_sql+",delim='|',header=true,all_varchar=true,strict_mode=true,filename=true,quote='',escape='',nullstr='')")
  stats[family]={'raw_rows':c.execute('SELECT count(*) FROM raw_'+family).fetchone()[0],'archive_sha256':sha,'members':len(paths)}
  if not stats[family]['raw_rows']:raise ValueError('Empty monthly dataset: '+family)
 c.execute("CREATE MACRO dt(s) AS cast(try_strptime(substr(s,1,9),'%d%b%Y') AS DATE)")
 def emit(name,sql):
  c.execute('CREATE OR REPLACE TEMP VIEW result AS '+sql);p=r/(name+'.parquet');c.execute('COPY result TO ? (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 8192)',[str(p)]);sha=hashlib.sha256(p.read_bytes()).hexdigest();n=c.execute('SELECT count(*) FROM read_parquet(?)',[str(p)]).fetchone()[0]
  results[name]=dict(path=str(p),files=[dict(path=str(p),sha256=sha)],sha256=sha,rows=n,as_of=as_of,source_release=release,kind='validated_public_dla_monthly_snapshot',raw_archives=raw,status='validated_monthly_source',coverage=stats);print('DLA monthly source',name,n,flush=True)
  if not n:raise ValueError('No usable public rows; retaining the previous snapshot: '+name)
 # Reject malformed public identifiers/dates instead of silently turning a bad
 # parse into an apparent reduction in demand.
 invalid=c.execute("SELECT count(*) FROM raw_forecast WHERE SECURITY_CLASSIFICATION='U' AND (NOT coalesce(regexp_full_match(FCST_QTY,'-?[0-9]+'),false) OR try_strptime(FCST_DT,'%Y%m') IS NULL)").fetchone()[0]
 if invalid:raise ValueError('Malformed public forecast rows: '+str(invalid))
 stats['forecast']['unmapped_source_item_rows']=c.execute("SELECT count(*) FROM raw_forecast WHERE SECURITY_CLASSIFICATION='U' AND NOT coalesce(regexp_full_match(niin,'[0-9]{9}'),false)").fetchone()[0]
 emit('dla_forecasts',"SELECT sha256('"+release+"|'||filename||'|'||cast(row_number() OVER() AS VARCHAR)) source_item_id,niin,CASE WHEN regexp_full_match(niin,'[0-9]{9}') THEN 'NIIN' ELSE 'unmapped_source_item_identifier' END identifier_type,cast(FCST_QTY AS BIGINT) forecast_qty,cast(cast(strptime(FCST_DT,'%Y%m') AS DATE) AS VARCHAR) forecast_month,SECURITY_CLASSIFICATION security_classification,true public_release_eligible,'"+as_of+"' retrieval_date,'"+release+"' source_release,regexp_extract(filename,'[^/]+$') source_file FROM raw_forecast WHERE SECURITY_CLASSIFICATION='U'")
 for family,name in [('inventory','dla_inventory'),('reorder','dla_reorder_points')]:
  invalid=c.execute('SELECT count(*) FROM raw_'+family+" WHERE SECURITY_CLASSIFICATION='U' AND (NOT coalesce(regexp_full_match(NSN,'[0-9]{13}'),false) OR dt(date_stamp) IS NULL OR dt(date_stamp)>cast(? AS DATE))",[as_of]).fetchone()[0]
  if invalid:raise ValueError('Malformed public stock rows: '+str(invalid))
  prefix="SELECT NSN nsn,substr(NSN,1,4) fsc,right(NSN,9) niin,TOTAL_STOCK total_stock,SECURITY_CLASSIFICATION security_classification,cond_cd condition_code,"
  if family=='inventory':prefix+='bo_qty backorder_qty,adq annual_demand_quantity,'
  else:prefix+='try_cast(rop AS DOUBLE) reorder_point,'
  sql=prefix+"true public_release_eligible,'"+as_of+"' retrieval_date,'"+release+"' source_release,regexp_extract(filename,'[^/]+$') source_file,cast(dt(date_stamp) AS VARCHAR) snapshot_date FROM raw_"+family+" WHERE SECURITY_CLASSIFICATION='U'"
  if family=='reorder':sql="SELECT nsn,niin,try_cast(total_stock AS DOUBLE) total_stock,reorder_point,condition_code,public_release_eligible,snapshot_date,source_release,source_file,retrieval_date FROM ("+sql+") WHERE snapshot_date IN (SELECT DISTINCT cast(dt(date_stamp) AS VARCHAR) FROM raw_reorder WHERE SECURITY_CLASSIFICATION='U' ORDER BY 1 DESC LIMIT 2)"
  emit(name,sql)
 # Keep the established physical financial key. Multiple part mappings never
 # multiply the spending and never turn an arbitrary part into an exact match.
 key="sha256(concat_ws(chr(31),"+','.join("coalesce(trim(\""+n+"\"),'<NULL>')" for n in 'NIIN FSC UNIT CAGE CONTRACT_NUMBER ORDER_QTY AWARD_DATE NETPRICE PO_NUM PO_ITMNO'.split())+'))'
 for family in ('procurement','parts'):c.execute('CREATE TEMP TABLE '+family+' AS SELECT *,'+key+' financial_line_id FROM raw_'+family+" WHERE SECURITY_CLASSIFICATION='U'")
 conflicts=c.execute('SELECT count(*) FROM (SELECT financial_line_id FROM procurement GROUP BY financial_line_id HAVING count(DISTINCT (CLIN,REFERENCED_PIID,STD_U_PRICE,PIINSPIINMOD,SOLIC_AMENDMENT_NUMBER))>1)').fetchone()[0]
 if conflicts:raise ValueError('Conflicting procurement lineage: '+str(conflicts))
 c.execute('CREATE TEMP TABLE proc AS SELECT * EXCLUDE(rn) FROM (SELECT *,row_number() OVER(PARTITION BY financial_line_id ORDER BY filename) rn FROM procurement) WHERE rn=1')
 c.execute('CREATE TEMP TABLE part AS SELECT financial_line_id,count(DISTINCT PART_NUMBER) part_count,CASE WHEN count(DISTINCT PART_NUMBER)=1 THEN min(PART_NUMBER) END part_number,CASE WHEN count(DISTINCT ITEM_NAME)=1 THEN min(ITEM_NAME) END item_name FROM parts GROUP BY financial_line_id')
 orphan=c.execute('SELECT (SELECT count(*) FROM proc ANTI JOIN part USING(financial_line_id))+(SELECT count(*) FROM part ANTI JOIN proc USING(financial_line_id))').fetchone()[0]
 if orphan:raise ValueError('Procurement and part files do not reconcile: '+str(orphan))
 proc_sha=stats['procurement']['archive_sha256'];proc_url=archives['procurement']['url'];source_uri='s3://a-and-d-intel-lake-newaccount/governance/opportunity_intelligence/sources/content/sha256='+proc_sha+'/source.archive'
 stats['procurement']['physical_lines']=c.execute('SELECT count(*) FROM proc').fetchone()[0];stats['procurement']['ambiguous_part_lines']=c.execute('SELECT count(*) FROM part WHERE part_count<>1').fetchone()[0]
 emit('item_market_price_lines',"""SELECT sha256('dla-monthly-line-v1|'||p.financial_line_id) pricing_record_id,p.NIIN niin,p.FSC||p.NIIN nsn,x.part_number,x.item_name,p.UNIT quantity_unit,try_cast(p.ORDER_QTY AS DOUBLE) quantity,try_cast(p.NETPRICE AS DOUBLE) unit_price_usd,try_cast(p.ORDER_QTY AS DOUBLE)*try_cast(p.NETPRICE AS DOUBLE) record_amount_usd,cast(cast(try_strptime(p.AWARD_DATE,'%Y%m%d') AS DATE) AS VARCHAR) transaction_date,p.CAGE supplier_cage,NULL::VARCHAR supplier_name,'DLA' source_system,'Observed procurement-line unit price; not a quote' price_basis,p.financial_line_id source_record_id,p.PO_NUM||' | '||p.PO_ITMNO source_row_locator,'URL' source_url,'URI' source_s3_uri,'SHA' source_sha256,'raw_archive_and_physical_purchase_line_verified' provenance_status,true include_in_mimir_range
 FROM proc p JOIN part x USING(financial_line_id) WHERE regexp_full_match(p.NIIN,'[0-9]{9}') AND regexp_full_match(p.FSC,'[0-9]{4}') AND regexp_full_match(p.CAGE,'[A-Z0-9]{5}') AND nullif(p.UNIT,'') IS NOT NULL AND try_cast(p.ORDER_QTY AS DOUBLE)>0 AND try_cast(p.NETPRICE AS DOUBLE)>0 AND cast(try_strptime(p.AWARD_DATE,'%Y%m%d') AS DATE)<=DATE 'ASOF'""".replace('URL',proc_url).replace('URI',source_uri).replace('SHA',proc_sha).replace('ASOF',as_of))
 stats['procurement']['excluded_or_unpriceable_lines']=stats['procurement']['physical_lines']-results['item_market_price_lines']['rows']
 (r/'source-validation.json').write_text(json.dumps(stats,indent=2)+'\n');c.close();return results
