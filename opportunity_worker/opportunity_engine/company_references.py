"""Consume the promoted Main catalogue by exact manifest and object versions.

This connector never promotes Main or changes source permissions. A denied or
failed upstream check retains the previous complete reference with truthful health.
"""
import datetime,hashlib,json,re
from pathlib import Path
import duckdb
from .aws import BUCKET,PREFIX

ROOT='mimir/platform/current_manifest.json'
MAIN_PATTERN=r'mimir/releases/mimir-main-platform-[A-Za-z0-9_-]+/manifest\.json'
PART_PATTERN=r'governance/platform_reference/candidates/[^\x00]+/output/nsn_cage_reference/[^/]+(?:\.parquet)?'


def resolve(s3):
    response=s3.get_object(Bucket=BUCKET,Key=ROOT);root_bytes=response['Body'].read();root=json.loads(root_bytes)
    if root.get('schema_version')!=2 or root.get('promotion_ready') is not True:raise ValueError('Main root is not a promoted schema-2 release')
    main=root.get('components',{}).get('main',{});key=main.get('immutable_manifest_key','');version=main.get('immutable_manifest_version_id');digest=main.get('immutable_manifest_sha256','')
    if not re.fullmatch(MAIN_PATTERN,key) or not version or not re.fullmatch('[a-f0-9]{64}',digest):raise ValueError('Main manifest is not version/hash pinned')
    raw=s3.get_object(Bucket=BUCKET,Key=key,VersionId=version)['Body'].read()
    if hashlib.sha256(raw).hexdigest()!=digest:raise ValueError('Main manifest hash differs')
    m=json.loads(raw)
    if m.get('release_id')!=main.get('release_id') or m.get('platform_reference_release_id')!=root.get('components',{}).get('platform_reference',{}).get('release_id'):raise ValueError('Mixed Main/platform source versions')
    files=[x for x in m.get('files',[]) if x.get('local_path')=='nsn_cage_reference.parquet']
    if len(files)!=1:raise ValueError('Company/item reference entry is not unique')
    entry=files[0]
    if entry.get('platform_reference_release_id')!=m['platform_reference_release_id']:raise ValueError('Company reference belongs to a different platform release')
    parts=entry.get('parts') or [entry]
    if not 1<=len(parts)<=16 or sum(int(p.get('size',0)) for p in parts)>4*1024**3:raise ValueError('Company source exceeds declared bounds')
    for p in parts:
        if not re.fullmatch(PART_PATTERN,p.get('s3_key','')) or not p.get('s3_version_id') or not p.get('etag') or int(p.get('size',0))<8:raise ValueError('Company source object is not an allowed pinned catalogue part')
    fingerprint=hashlib.sha256(json.dumps(parts,sort_keys=True).encode()).hexdigest()
    return dict(main_release_id=m['release_id'],platform_reference_release_id=m['platform_reference_release_id'],root_key=ROOT,root_version_id=response.get('VersionId'),root_sha256=hashlib.sha256(root_bytes).hexdigest(),main_manifest_key=key,main_manifest_version_id=version,main_manifest_sha256=digest,parts=parts,fingerprint=fingerprint)


def project(parts,folder,binding):
    folder=Path(folder);folder.mkdir(parents=True,exist_ok=True);c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='3GB';SET preserve_insertion_order=false")
    c.execute("SET temp_directory='"+str(folder/'tmp').replace("'","''")+"'")
    paths=[str(p['path']) for p in parts];c.read_parquet(paths,file_row_number=True,filename=True).create_view('raw')
    fields={n for n,*_ in c.execute('DESCRIBE raw').fetchall()}
    required={'cage','niin','nsn','description','part_number','unit_of_issue','govt_estimated_price','avg_unit_price','last_sold_date','rncc_codes','rnvc_codes','rnsc_codes'}
    if not required<=fields:raise ValueError('Promoted company catalogue schema differs: '+','.join(sorted(required-fields)))
    c.execute('CREATE TEMP TABLE source_parts(filename VARCHAR,snapshot_sha VARCHAR)');c.executemany('INSERT INTO source_parts VALUES (?,?)',[(str(p['path']),p['sha256']) for p in parts])
    interpretation='Version-pinned promoted Mimir Main company/item projection. Original per-part reference codes and unit-price observations are retained. These are not a transfer of manufacturing rights, source qualification or current quote eligibility.'
    sql="""SELECT upper(trim(cast(r.cage AS VARCHAR))) cage_code,trim(cast(r.niin AS VARCHAR)) niin,cast(r.nsn AS VARCHAR) nsn,cast(description AS VARCHAR) description,cast(part_number AS VARCHAR) part_number,upper(trim(cast(unit_of_issue AS VARCHAR))) unit_of_issue,
    CASE WHEN try_cast(govt_estimated_price AS DOUBLE)>0 THEN try_cast(govt_estimated_price AS DOUBLE) END govt_estimated_price,
    CASE WHEN try_cast(avg_unit_price AS DOUBLE)>0 THEN try_cast(avg_unit_price AS DOUBLE) END supplier_average_price,cast(last_sold_date AS VARCHAR) last_sold_date,
    cast(rncc_codes AS VARCHAR) rncc_codes,cast(rnvc_codes AS VARCHAR) rnvc_codes,cast(rnsc_codes AS VARCHAR) rnsc_codes,
    sha256(concat_ws('|','promoted-main-item-reference-v1',p.snapshot_sha,cast(r.file_row_number+1 AS VARCHAR))) evidence_id,p.snapshot_sha source_snapshot_sha256,cast(r.file_row_number+1 AS BIGINT) source_row,
    'INTERPRETATION' interpretation FROM raw r JOIN source_parts p USING(filename)
    WHERE regexp_full_match(upper(trim(cast(r.cage AS VARCHAR))),'[A-Z0-9]{5}') AND regexp_full_match(trim(cast(r.niin AS VARCHAR)),'[0-9]{9}')""".replace('INTERPRETATION',interpretation)
    # Preserve useful existing Main facts without turning them into blanket
    # eligibility or single-platform claims. Absent optional fields remain null.
    optional={n:'VARCHAR' for n in ('cage_status_codes','supplier_status','supplier_status_detail','platform_family','platform_families','platform_attribution_status','reported_end_item_context')}
    optional.update(is_procurement_authorized='BOOLEAN',is_active_authorized_source='BOOLEAN')
    extra=','.join(('cast(r."'+n+'" AS '+typ+')' if n in fields else 'NULL::'+typ)+' "'+n+'"' for n,typ in optional.items())
    sql=sql.replace(' FROM raw r',','+extra+' FROM raw r')
    path=folder/'item_reference_facts.parquet';c.execute('COPY ('+sql+') TO ? (FORMAT PARQUET,COMPRESSION ZSTD)',[str(path)])
    rows=c.execute('SELECT count(*) FROM read_parquet(?)',[str(path)]).fetchone()[0]
    if not rows:raise ValueError('Promoted company reference projection is empty')
    raw_rows=c.execute('SELECT count(*) FROM raw').fetchone()[0];c.close();sha=hashlib.sha256(path.read_bytes()).hexdigest()
    return dict(path=str(path),files=[dict(path=str(path),sha256=sha)],sha256=sha,rows=rows,as_of='promoted_main_release:'+binding['main_release_id'],source_release=binding['main_release_id'],kind='version_pinned_promoted_main_item_references',status='validated_promoted_main_projection',upstream_binding=binding,projection_counts=dict(raw_rows=raw_rows,valid_identity_rows=rows,excluded_invalid_identity_rows=raw_rows-rows),interpretation=interpretation)


def refresh(previous,root,aws):
    root=Path(root);now=datetime.datetime.now(datetime.timezone.utc).isoformat();out=root/'inputs'/('company_references_'+now.replace(':','').replace('-',''));out.mkdir(parents=True,exist_ok=True)
    try:
        binding=resolve(aws.s3)
        if previous.get('upstream_binding',{}).get('fingerprint')==binding['fingerprint']:
            result=dict(previous);status='promoted_catalogue_checked_unchanged'
        else:
            parts=[];archives=[]
            for index,p in enumerate(binding['parts']):
                h=aws.s3.head_object(Bucket=BUCKET,Key=p['s3_key'],VersionId=p['s3_version_id'])
                if h['ContentLength']!=int(p['size']) or h['ETag'].strip('"')!=p['etag'].strip('"'):raise ValueError('Pinned company catalogue metadata differs')
                path=out/('part-'+str(index)+'.parquet');aws.s3.download_file(BUCKET,p['s3_key'],str(path),ExtraArgs={'VersionId':p['s3_version_id']})
                if path.stat().st_size!=int(p['size']):raise ValueError('Pinned company catalogue size differs')
                sha=hashlib.sha256(path.read_bytes()).hexdigest();parts.append(dict(path=str(path),sha256=sha))
                key=PREFIX+'/sources/content/sha256='+sha+'/source.archive';aws.upload(path,key)
                archives.append(dict(path=str(path),sha256=sha,s3_uri='s3://'+BUCKET+'/'+key,original_s3_uri='s3://'+BUCKET+'/'+p['s3_key'],original_s3_version_id=p['s3_version_id'],source_role='Promoted Mimir source projection, not an original government notice'))
            result=project(parts,out/'compiled',binding);result['raw_archives']=archives
            key=PREFIX+'/sources/content/sha256='+result['sha256']+'/source.parquet';aws.upload(Path(result['path']),key);result['files'][0]['s3_uri']='s3://'+BUCKET+'/'+key
            status='promoted_catalogue_refreshed'
        result['refresh_health']=dict(status=status,attempted_at=now,source_release=binding['main_release_id'],interpretation='Promoted Main object versions were checked; source-price dates and reference-code meaning are not advanced by a processing timestamp.')
    except Exception as error:
        if not previous:raise
        result=dict(previous,refresh_health=dict(status='retained_company_catalogue_upstream_check_failed',attempted_at=now,source_release=previous.get('source_release') or 'retained_reference_snapshot',reason=type(error).__name__,interpretation='The previous complete company reference is retained with its original source provenance; the current promoted Main catalogue has not been verified.'))
    manifest=out/'manifest.json';manifest.write_text(json.dumps({'sources':{'item_reference_facts':result}},indent=2)+'\n')
    print('Company reference refresh',json.dumps(result['refresh_health']),flush=True);return result
