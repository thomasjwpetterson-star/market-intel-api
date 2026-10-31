"""Requirement-level agency plans, with original bands and fiscal precision.

Plans are neither solicitations nor company revenue. Native identifiers survive
document revisions; ambiguous identities and unrecognised layouts stay in review.
"""
import calendar
import datetime
import hashlib
import io
import json
import re
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

VERSION = 'agency-requirement-forecasts-v1'
INDEX = 'https://www.navair.navy.mil/LRAE'
WORKBOOK = 'https://www.navair.navy.mil/sites/g/files/jejdrs536/files/document/%5Bfilename%5D/Consolidated%20Dec%202025%20LRAE%20Report%2002_05_26%20Final.xlsx'
WEAPONS = 'https://www.navair.navy.mil/nawcwd/sites/g/files/jejdrs601/files/document/%5Bfilename%5D/NAWCWD%20LRAF%20FY%202026.pdf'
AIRCRAFT = 'https://www.navair.navy.mil/osbp/sites/g/files/jejdrs551/files/document/%5Bfilename%5D/NAWCAD%20LRAE%20FY26Q4.pdf'
NS = {'s': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
FIELDS = ('title description programme value_band procurement_method contract_type instrument buying_office solicitation_fy solicitation_quarter award_fy award_quarter period_months follow_on prior_contract incumbent performance_site naics psc contact_name contact_route secondary_contact secondary_route facility_clearance personnel_clearance palt_code sub_palt_code comments native_plan_id detail_url').split()


def clean(value):
    return re.sub(r'\s+', ' ', str(value or '')).strip()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def fiscal_period(year, quarter):
    """US federal fiscal quarters; an absent quarter retains a whole FY range."""
    y = re.fullmatch(r'(?:FY\s*)?(\d{2}|20\d{2})', clean(year), re.I)
    if not y:
        return None, None, 'unresolved'
    fy = int(y[1]); fy = fy + 2000 if fy < 100 else fy
    q = re.fullmatch(r'(?:Q(?:TR|UARTER)?\s*)?([1-4])', clean(quarter), re.I)
    if not q:
        return f'{fy-1}-10-01', f'{fy}-09-30', 'fiscal_year'
    start_month = (10, 1, 4, 7)[int(q[1])-1]
    start_year = fy-1 if start_month == 10 else fy
    end_month = start_month+2
    return f'{start_year}-{start_month:02d}-01', f'{start_year}-{end_month:02d}-{calendar.monthrange(start_year,end_month)[1]}', 'fiscal_quarter'


def value_band(raw):
    """Do not invent a midpoint or replace a one-sided band with a point value."""
    text = clean(raw).upper().replace(',', '')
    matches = list(re.finditer(r'(?<![A-Z0-9.])\$?\s*(\d+(?:\.\d+)?)\s*([KMB])\b', text))
    values = [float(x[1])*{'K':1e3,'M':1e6,'B':1e9}[x[2]] for x in matches]
    if len(values) == 2 and values[0] < values[1] and not text.lstrip().startswith('<'):
        return values[0], values[1], None, 'source_range'
    if len(values) == 1:
        if '<' in text or re.search(r'\bLESS THAN\b', text):
            return None, values[0], None, 'source_upper_bound'
        if '>' in text or '+' in text or re.search(r'\bOVER\b', text):
            return values[0], None, None, 'source_lower_bound'
        if re.fullmatch(r'\$?\s*\d+(?:\.\d+)?\s*[KMB]', text):
            return values[0], values[0], values[0], 'source_stated_amount'
    return None, None, None, 'unresolved'


def _column(reference):
    result = 0
    for char in re.match(r'[A-Z]+', reference)[0]:
        result = result*26+ord(char)-64
    return result-1


def workbook_rows(raw):
    """Read bounded XML cells, ignoring Excel's enormous styled empty range."""
    with zipfile.ZipFile(io.BytesIO(raw)) as book:
        if sum(x.file_size for x in book.infolist()) > 100*1024*1024:
            raise ValueError('Forecast workbook exceeds uncompressed size bound')
        shared = []
        if 'xl/sharedStrings.xml' in book.namelist():
            shared = [''.join(n.itertext()) for n in ET.fromstring(book.read('xl/sharedStrings.xml')).findall('s:si',NS)]
        found = False
        for filename in sorted(n for n in book.namelist() if re.fullmatch(r'xl/worksheets/sheet\d+\.xml',n)):
            header_seen = False
            for row in ET.fromstring(book.read(filename)).findall('s:sheetData/s:row',NS):
                values = [None]*30
                for cell in row.findall('s:c',NS):
                    col = _column(cell.attrib['r'])
                    if col >= 30:
                        continue
                    value = cell.find('s:v',NS)
                    if cell.attrib.get('t') == 's' and value is not None:
                        values[col] = shared[int(value.text)]
                    elif cell.attrib.get('t') == 'inlineStr':
                        values[col] = ''.join(cell.find('s:is',NS).itertext())
                    elif value is not None:
                        values[col] = value.text
                if clean(values[0]) == 'Requirement Title':
                    if clean(values[3]) != 'Anticipated Total Value (Including Options)' or clean(values[28]) != 'PID Number':
                        raise ValueError('NAVAIR forecast column layout changed')
                    header_seen = found = True
                    continue
                if header_seen and clean(values[0]) and clean(values[1]) and clean(values[7]):
                    yield dict(zip(FIELDS, map(clean,values))), filename+':row '+row.attrib['r']
        if not found:
            raise ValueError('NAVAIR requirement table header missing')


def weapons_rows(raw):
    import pdfplumber
    mapping = {'title':1,'description':2,'programme':3,'value_band':5,'procurement_method':6,'contract_type':7,'instrument':8,'native_plan_id':9,'solicitation_fy':10,'solicitation_quarter':11,'award_fy':12,'award_quarter':13,'period_months':14,'follow_on':15,'prior_contract':16,'incumbent':17,'performance_site':18,'naics':19,'psc':20,'facility_clearance':21,'personnel_clearance':22,'buying_office':23,'contact_name':24,'contact_route':25,'secondary_contact':26,'secondary_route':27,'comments':29}
    seen = []
    with pdfplumber.open(io.BytesIO(raw)) as document:
        if len(document.pages) > 30:
            raise ValueError('Unexpected NAWCWD document length')
        for number,page in enumerate(document.pages,1):
            for table in page.extract_tables():
                if not table or len(table[0]) != 30:
                    continue
                for row in table:
                    if not re.fullmatch(r'\d+',clean(row[0])):
                        continue
                    seen.append(int(row[0])); record = {name:clean(row[index]) for name,index in mapping.items()}
                    # TBD is not an enduring procurement identifier.
                    if not re.fullmatch(r'[A-Z][A-Z0-9-]{8,}',record['native_plan_id']):
                        record['native_plan_id'] = ''
                    record['buying_office'] = 'N68936 / '+record['buying_office']
                    yield record,f'page {number}, row {row[0]}'
    if not seen or sorted(seen) != list(range(1,max(seen)+1)):
        raise ValueError('NAWCWD forecast row population is incomplete or duplicated')


def normalize(record, url, sha, locator, observed_at, source_family):
    record = {name:clean(record.get(name)) for name in FIELDS}
    start,end,precision = fiscal_period(record['solicitation_fy'],record['solicitation_quarter'])
    award_start,award_end,award_precision = fiscal_period(record['award_fy'],record['award_quarter'])
    low,high,amount,band_kind = value_band(record['value_band'])
    office = re.match(r'N\d{5}',record['buying_office'])
    office = office[0] if office else 'NAVAIR'
    prior = sorted(set(re.findall(r'\b[A-Z]\d{5}[A-Z0-9]{7,12}\b',record['prior_contract'].upper())))
    identifier = record['native_plan_id']
    # Workbook PID and named procurement URL are stronger than row numbers.
    if identifier and identifier.upper() not in ('TBD','N/A','NA','NEW'):
        identity = office+'|id|'+re.sub(r'[^A-Z0-9]','',identifier.upper())
    elif record['detail_url'].startswith('https://ams.navair.navy.mil/procurement/'):
        identity = office+'|url|'+record['detail_url'].rstrip('/').rsplit('/',1)[-1]
    else:
        identity = office+'|'+('|'.join(prior) if prior else 'new')+'|'+re.sub(r'[^A-Z0-9]','',record['title'].upper())
    status = 'source_plan' if award_end or end else 'timing_unresolved'
    if start and award_end and start>award_end:
        status = 'conflicting_planned_dates_review'
    return dict(**record,forecast_id=digest({'agency':'NAVAIR','key':identity}),source_family=source_family,source_url=url,source_sha256=sha,source_locator=locator,source_observed_at=observed_at,
        raw_fields_json=json.dumps(record,sort_keys=True),record_hash=digest({'fields':record,'source_sha256':sha,'locator':locator}),source_evidence_id=digest({'sha256':sha,'locator':locator}),
        solicitation_start=start,solicitation_end=end,solicitation_precision=precision,award_start=award_start,award_end=award_end,award_precision=award_precision,
        planned_value_low=low,planned_value_high=high,planned_value_amount=amount,planned_value_kind=band_kind,includes_options=True,prior_contracts_json=json.dumps(prior),
        status=status,policy_version=VERSION)


def deduplicate(records):
    """Conflicting same identities remain evidence, without arbitrary winners."""
    counts = Counter(r['forecast_id'] for r in records)
    for row in records:
        if counts[row['forecast_id']] > 1:
            row['status'] = 'duplicate_identity_review'
    return records


def write_source(records, destination, health, archives, observed_at):
    import duckdb
    destination = Path(destination); destination.mkdir(parents=True,exist_ok=True)
    raw = destination/'requirements.jsonl'
    if not records:
        raise ValueError('No complete agency forecast source retrieved')
    raw.write_text(''.join(json.dumps(row)+'\n' for row in deduplicate(records)))
    path=destination/'requirements.parquet';c=duckdb.connect()
    # Explicit types prevent an all-null optional field becoming an integer.
    types={name:('DOUBLE' if name.startswith('planned_value_') and name not in ('planned_value_kind',) else 'BOOLEAN' if name=='includes_options' else 'VARCHAR') for name in records[0]}
    c.read_json(str(raw),format='newline_delimited',columns=types).create_view('result')
    c.execute('COPY result TO ? (FORMAT PARQUET,COMPRESSION ZSTD)',[str(path)]);c.close()
    sha=hashlib.sha256(path.read_bytes()).hexdigest()
    return dict(files=[dict(path=str(path),sha256=sha)],rows=len(records),as_of=observed_at,kind='official_requirement_level_acquisition_forecasts',health=health,raw_archives=archives,status='source_specific_coverage_recorded',interpretation='Agency planning rows, including options. Source bands and fiscal precision are preserved; no midpoint, procurement commitment or company workshare is inferred.')


def prepare(sources, folder, as_of, aws):
    """Append plans to demand using exact prior-contract and notice bridges."""
    if 'agency_acquisition_forecasts' not in sources:
        return {}
    import duckdb
    from .aws import DATABASE
    folder=Path(folder);folder.mkdir(parents=True,exist_ok=True)
    c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='1500MB'")
    source=sources['agency_acquisition_forecasts']
    query=c.read_parquet([f['path'] for f in source['files']]);names=query.columns
    rows=[dict(zip(names,values)) for values in query.fetchall()]
    contracts=sorted(set(k for row in rows for k in json.loads(row['prior_contracts_json'])))
    award_source=sources.get('company_award_facts',{})
    award_table=award_source.get('table')
    incumbents=[];query_id=None
    if award_table and contracts:
        if not re.fullmatch(r'oi_[a-z0-9_]+',award_table):raise ValueError('Invalid incumbent source table')
        literals=','.join("'"+k+"'" for k in contracts)
        sql='SELECT award_key,piid,cage_code,uei,company_name,work_text,psc,naics,latest_action_date,evidence_id,source_url FROM '+DATABASE+'.'+award_table+' WHERE piid IN ('+literals+") AND regexp_like(cage_code,'^[A-Z0-9]{5}$')"
        incumbents,e=aws.result(sql);query_id=e.get('QueryExecutionId')
    elif award_source and award_source.get('files'):
        c.read_parquet([f['path'] for f in award_source['files']]).create_view('awards')
        q=c.execute("SELECT award_key,piid,cage_code,uei,company_name,work_text,psc,naics,latest_action_date,evidence_id,source_url FROM awards WHERE piid IN (SELECT unnest(?))",[contracts]);names=[x[0] for x in q.description];incumbents=[dict(zip(names,v)) for v in q.fetchall()]
    by_contract={}
    for award in incumbents:by_contract.setdefault(award['piid'],[]).append(award)
    notice_heads={}
    if 'sam_notices' in sources:
        c.read_parquet([f['path'] for f in sources['sam_notices']['files']]).create_view('notices')
        q=c.execute("""SELECT solicitation_id,notice_id,notice_type,cast(posted_at AS VARCHAR) posted_at,source_url,selected_notice_version_id FROM notices
        WHERE regexp_matches(regexp_replace(upper(solicitation_id),'[^A-Z0-9]','','g'),'^N[0-9]{7}[RQT][0-9]{4}$')
        QUALIFY row_number() OVER(PARTITION BY regexp_replace(upper(solicitation_id),'[^A-Z0-9]','','g') ORDER BY posted_at DESC,notice_id DESC)=1""")
        for values in q.fetchall():
            item=dict(zip([x[0] for x in q.description],values));notice_heads[re.sub('[^A-Z0-9]','',item['solicitation_id'].upper())]=item
    records=[];links=[]
    newer_pax=any(h['source']=='nawcad_current' and h['status']=='document_archived_requirement_OCR_pending' for h in source.get('health',[]))
    for row in rows:
        if row['status']=='duplicate_identity_review':
            continue
        episode=hashlib.sha256(('agency-plan|'+row['forecast_id']).encode()).hexdigest()
        known=[a for k in json.loads(row['prior_contracts_json']) for a in by_contract.get(k,[])]
        # Same PIID with conflicting recipients cannot silently select a site.
        cages={a['cage_code'] for a in known}
        exact_incumbents=known if len(cages)==1 else []
        for a in exact_incumbents:
            links.append(dict(episode_id=episode,forecast_id=row['forecast_id'],company_id='CAGE:'+a['cage_code'],**a,link_basis='exact_named_prior_contract_unique_site',forecast_source_evidence_id=row['source_evidence_id'],forecast_source_url=row['source_url']))
        native=re.sub('[^A-Z0-9]','',row['native_plan_id'].upper())
        successor=notice_heads.get(native) if re.fullmatch(r'N\d{7}[RQT]\d{4}',native) else None
        state='agency_planned_requirement'
        if row['status']!='source_plan':state='agency_plan_evidence_review'
        elif newer_pax and row['source_family']=='navair_consolidated' and row['buying_office'].startswith('N00421'):state='agency_plan_newer_document_reconciliation_required'
        elif successor:state='agency_plan_resolved_to_published_notice'
        elif row['solicitation_end'] and row['solicitation_end']<as_of:state='agency_plan_schedule_revalidation_required'
        inputs=dict(forecast_id=row['forecast_id'],source_forecast_row=row,planned_solicitation=dict(start=row['solicitation_start'],end=row['solicitation_end'],precision=row['solicitation_precision'],source_year=row['solicitation_fy'],source_quarter=row['solicitation_quarter']),planned_award=dict(start=row['award_start'],end=row['award_end'],precision=row['award_precision'],source_year=row['award_fy'],source_quarter=row['award_quarter']),planned_value_band=row['value_band'],planned_value_kind=row['planned_value_kind'],includes_options=True,company_addressable_value=None,incumbent_company_ids=sorted('CAGE:'+x for x in cages) if len(cages)==1 else [],incumbent_contracts=exact_incumbents,incumbent_query_id=query_id,successor_notice=successor)
        scope=row['description']+'\nAnticipated procurement method: '+row['procurement_method']+'. Contract instrument: '+row['instrument']+'. Facility clearance: '+row['facility_clearance']+'. Personnel clearance: '+row['personnel_clearance']+'. '+row['comments']
        content=dict(inputs);content.pop('incumbent_query_id',None);content['source_forecast_row']=dict(row);content['source_forecast_row'].pop('source_observed_at',None)
        records.append(dict(episode_id=episode,record_hash=digest(dict(inputs=content,state=state)),lane='explicit_work_package_plan',title=row['title'],scope_text=scope,customer_name='Navy / NAVAIR / '+row['buying_office'],programme=row['programme'],psc=re.split(r'\s+-\s+',row['psc'])[0],naics=re.split(r'\s+-\s+',row['naics'])[0],window_start=row['solicitation_start'] or row['award_start'],window_end=row['award_end'] or row['solicitation_end'],date_basis='Agency planned solicitation and award fiscal periods; these are not response or delivery deadlines',value_low=row['planned_value_low'],value_median=row['planned_value_amount'],value_high=row['planned_value_high'],value_basis='source_planned_package_band_including_options',value_scope='Agency estimated whole-package band including options; no midpoint, company workshare or obligated spending is inferred',value_inputs_json=json.dumps(inputs,sort_keys=True),source_name='agency_acquisition_forecasts',source_url=row['source_url'],source_evidence_id=row['source_evidence_id'],source_locator=row['source_locator'],native_identifier=row['native_plan_id'] or row['forecast_id'],procurement_state=state,reconciled_state='later_notice_linked' if successor else 'plan_outcome_unresolved',access_state='Incumbent, competition method, set-aside, vehicle and clearance conditions remain explicit; similarity does not transfer qualifications',rationale='The agency publishes this specific planned work package, its acquisition timing and estimated value band. The source record and any exact incumbent-contract or later notice bridge remain linked.',limitations='Plans and estimated bands can change. A named incumbent is not an award commitment. Hardware qualification, vehicle access, set-aside eligibility and capacity require their own evidence.'))
    def source_file(name, values, columns=None):
        raw=folder/(name+'.jsonl');raw.write_text(''.join(json.dumps(x)+'\n' for x in values));path=folder/(name+'.parquet')
        c.read_json(str(raw),sample_size=-1).create_view('result',replace=True)
        if columns:
            selected=','.join('cast('+('"'+n+'"' if n in {x[0] for x in c.execute('DESCRIBE result').fetchall()} else 'NULL')+' AS '+typ+') "'+n+'"' for n,typ in columns)
            c.execute('CREATE OR REPLACE TEMP VIEW portable_result AS SELECT '+selected+' FROM result')
        else:c.execute('CREATE OR REPLACE TEMP VIEW portable_result AS SELECT * FROM result')
        c.execute('COPY portable_result TO ? (FORMAT PARQUET,COMPRESSION ZSTD)',[str(path)])
        sha=hashlib.sha256(path.read_bytes()).hexdigest();return dict(files=[dict(path=str(path),sha256=sha)],path=str(path),sha256=sha,rows=len(values),as_of=as_of,kind='official_requirement_level_forecast_derivation',status='SUCCEEDED')
    raw=folder/'plans.jsonl';raw.write_text(''.join(json.dumps(x)+'\n' for x in records));c.read_json(str(raw),sample_size=-1).create_view('plans')
    c.read_parquet([f['path'] for f in sources['demand_candidates']['files']]).create_view('prior_demand')
    cols=c.execute('DESCRIBE prior_demand').fetchall();projection=','.join('cast('+('"'+n+'"' if n in records[0] else 'NULL')+' AS '+typ+') "'+n+'"' for n,typ,*_ in cols)
    path=folder/'demand_candidates.parquet';c.execute("COPY (SELECT * FROM prior_demand WHERE coalesce(source_name,'')<>'agency_acquisition_forecasts' UNION ALL SELECT "+projection+' FROM plans) TO ? (FORMAT PARQUET,COMPRESSION ZSTD)',[str(path)]);sha=hashlib.sha256(path.read_bytes()).hexdigest();count=c.execute('SELECT count(*) FROM read_parquet(?)',[str(path)]).fetchone()[0]
    out={'demand_candidates':dict(files=[dict(path=str(path),sha256=sha)],path=str(path),sha256=sha,rows=count,as_of=as_of,kind='source_backed_demand_with_agency_forecasts',status='SUCCEEDED')}
    # Explicit schema supports a genuine empty incumbent result.
    schema=[(n,'VARCHAR') for n in 'episode_id forecast_id company_id award_key piid cage_code uei company_name work_text psc naics latest_action_date evidence_id source_url link_basis forecast_source_evidence_id forecast_source_url'.split()]
    if links:out['agency_forecast_incumbents']=source_file('agency_forecast_incumbents',links,schema)
    else:
        path=folder/'agency_forecast_incumbents.parquet';c.execute('COPY (SELECT '+','.join('NULL::VARCHAR '+n for n,_ in schema)+' WHERE false) TO ? (FORMAT PARQUET)',[str(path)]);sha=hashlib.sha256(path.read_bytes()).hexdigest();out['agency_forecast_incumbents']=dict(files=[dict(path=str(path),sha256=sha)],rows=0,as_of=as_of,kind='exact_named_prior_contract_bridges',status='SUCCEEDED')
    c.close();print('Agency planned demand',len(records),'exact incumbent links',len(links),flush=True);return out


def link_incumbents(directory):
    """Exact source-named incumbent contracts form an auditable retention route."""
    import duckdb
    r=Path(directory);m=json.loads((r/'manifest.json').read_text())
    if 'agency_forecast_incumbents' not in m['tables']:return
    c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='2GB'")
    for name in ('company_work_matches','company_matches','company_profiles','opportunities','demand_decisions','agency_forecast_incumbents'):
        c.read_parquet(m['tables'][name]['path']).create_view('prior_'+name)
    c.execute("""CREATE TEMP VIEW exact_incumbents AS SELECT
    sha256('agency-incumbent-v1|'||i.company_id||'|'||o.episode_id) match_id,i.company_id,o.episode_id,95.0::DOUBLE score,'exact_agency_incumbent_contract-v1' match_route,
    'Agency names prior contract '||i.piid||'; native award resolves to this CAGE' matched_terms,
    i.evidence_id sample_evidence_id,o.record_hash opportunity_hash,p.profile_hash,i.award_key doc_id,i.work_text,'exact_incumbent_contract' source_kind,
    i.latest_action_date last_observed,o.programme work_context,'documented_incumbent_contract' company_entry_status,
    'The agency identifies a follow-on to this site''s exact prior contract. This is an incumbent-retention prospect, not a promised award or transferable qualification. Current acquisition eligibility, scope, capacity and schedule remain checks.' company_evidence_caveat,
    'work_lead' review_category,sha256(concat_ws('|',i.evidence_id,o.record_hash,p.profile_hash,'agency-incumbent-v1')) match_hash
    FROM prior_agency_forecast_incumbents i JOIN prior_company_profiles p USING(company_id) JOIN prior_opportunities o USING(episode_id)
    JOIN prior_demand_decisions d USING(episode_id) WHERE o.source_name='agency_acquisition_forecasts' AND d.stage='emerging_demand'
    QUALIFY row_number() OVER(PARTITION BY i.company_id,o.episode_id ORDER BY i.latest_action_date DESC,i.evidence_id)=1""")
    def emit(name,sql):
        path=r/(name+'.parquet');temp=r/(name+'.agency.parquet');c.execute('CREATE OR REPLACE TEMP VIEW result AS '+sql)
        columns=[dict(Name=n,Type={'VARCHAR':'string','DOUBLE':'double','BIGINT':'bigint','BOOLEAN':'boolean'}[t]) for n,t,*_ in c.execute('DESCRIBE result').fetchall()]
        c.execute('COPY result TO ? (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 2048)',[str(temp)]);count=c.execute('SELECT count(*) FROM read_parquet(?)',[str(temp)]).fetchone()[0];temp.replace(path)
        m['tables'][name]=dict(path=str(path),rows=count,columns=columns,sha256=hashlib.sha256(path.read_bytes()).hexdigest());c.read_parquet(str(path)).create_view('new_'+name)
    emit('company_work_matches',"SELECT w.* FROM prior_company_work_matches w WHERE NOT EXISTS(SELECT 1 FROM exact_incumbents i WHERE i.company_id=w.company_id AND i.episode_id=w.episode_id) UNION ALL BY NAME SELECT * FROM exact_incumbents")
    fields='match_id,company_id,episode_id,score,match_route,matched_terms,company_evidence_caveat,sample_evidence_id,match_hash,opportunity_hash,profile_hash'
    emit('company_matches','SELECT w.* FROM prior_company_matches w WHERE NOT EXISTS(SELECT 1 FROM exact_incumbents i WHERE i.company_id=w.company_id AND i.episode_id=w.episode_id) UNION ALL SELECT '+fields+' FROM exact_incumbents')
    m['agency_forecast_policy']=VERSION;(r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close()
