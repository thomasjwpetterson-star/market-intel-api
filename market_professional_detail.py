"""Read one page of an immutable, reconciled Market Explorer fact export."""
import base64
import hashlib
import hmac
import json
import re
import time

VIEWS = {
    'contracts': 'PRIME_AWARD', 'customers': 'PRIME_AWARD', 'contractors': 'PRIME_AWARD',
    'subcontracts': 'FIRST_TIER_REPORT', 'subcontractors': 'FIRST_TIER_REPORT',
    'parts': 'DLA_AWARD', 'award-nsns': 'PRIME_AWARD',
}
PAGE_SIZE = 25
DB = 'market_intel_platform_candidate'
KEY = r'[A-Z0-9_]{1,128}'
TABLE = r'market_professional_detail_([0-9a-f]{20})'
QID = r'[0-9a-f-]{36}'


def query_for(payload):
    view = payload['view']
    source = VIEWS.get(view)
    years = payload['years']
    nodes = payload['nodes']
    table = payload['table']
    match = re.fullmatch(TABLE, table)
    if (not source or not match or not isinstance(years, list) or not 1 <= len(years) <= 8
        or any(type(y) is not int or not 2019 <= y <= 2026 for y in years)
        or not isinstance(nodes, list) or not 1 <= len(nodes) <= 512
        or any(not isinstance(n, str) or not re.fullmatch(KEY, n) for n in nodes)):
        raise ValueError('Invalid market detail filters.')
    selected = ','.join(map(str, sorted(set(years))))
    scope = ','.join(f"'{n}'" for n in sorted(set(nodes)))
    fields = ('contract_id', 'parent_award_key', 'company', 'cage', 'uei', 'prime_company',
              'description', 'customer', 'nsn', 'report_year')
    # Metadata is one verified row (or null) per native ID in the immutable fact table.
    metadata = ','.join(f'arbitrary({f}) AS {f}' for f in fields)
    platforms = '' if view in ('customers','contractors','subcontractors') else (
        ',json_format(CAST(array_sort(array_distinct(flatten(array_agg(platform_ids)))) AS JSON)) AS platforms')
    sql = f"""WITH records AS (
      SELECT source_record_id AS id, SUM(net_usd) AS net, {metadata}{platforms}
      FROM "{DB}"."{table}"
      WHERE source_kind='{source}' AND fiscal_year IN ({selected}) AND market_node IN ({scope})
      GROUP BY 1
    )"""
    if view == 'customers':
        sql += """, grouped AS (
          SELECT COALESCE(NULLIF(TRIM(customer),''),'Awarding customer not reported') AS id,
                 SUM(net) AS net, COUNT(*) AS records
          FROM records GROUP BY 1
        ) SELECT * FROM grouped"""
    elif view in ('contractors', 'subcontractors'):
        sql += """, grouped AS (
          SELECT COALESCE(
            CASE WHEN regexp_like(cage,'^[A-Z0-9]{5}$') THEN 'CAGE:'||cage END,
            CASE WHEN regexp_like(uei,'^[A-Z0-9]{12}$') THEN 'UEI:'||uei END,
            'RECORD:'||id) AS id,
            MAX(company) AS name, MAX(cage) AS cage, SUM(net) AS net, COUNT(*) AS records
          FROM records GROUP BY 1
        ) SELECT * FROM grouped"""
    else:
        sql += ' SELECT * FROM records'
        if view == 'award-nsns':
            sql += (
                " WHERE regexp_like(replace(COALESCE(nsn,''),'-',''),'^[0-9]{13}$')"
                r" OR regexp_like(COALESCE(description,''),'(?i)\bNSN\b[\s:)]*[0-9]{4}[- ]?[0-9]{2}[- ]?[0-9]{3}[- ]?[0-9]{4}(?![0-9])')"
            )
    return sql + ' ORDER BY ABS(net) DESC, id ASC', match[1]


def token(secret, context, qid, next_token=None, page=1):
    message = base64.urlsafe_b64encode(json.dumps([context, qid, next_token, page, int(time.time())],
        separators=(',', ':')).encode()).decode().rstrip('=')
    signature = hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()
    return message + '.' + signature


def read_token(value, secret, context):
    if not isinstance(value, str) or len(value) > 8192:
        raise ValueError('Invalid market page cursor.')
    message, sig = value.rsplit('.', 1)
    expected = hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        raise ValueError('Invalid market page cursor.')
    bound, qid, position, page, issued = json.loads(base64.urlsafe_b64decode(message+'='*(-len(message)%4)))
    if (bound != context or not re.fullmatch(QID, qid) or page < 1
        or not 0 <= time.time()-issued <= 86400):
        raise ValueError('Market page expired or its filters changed.')
    return qid, position, page


def read_page(athena, payload, secret, bucket, owner, workgroup='primary'):
    sql, table_id = query_for(payload)
    context = hashlib.sha256(json.dumps([payload['mainRelease'], payload['candidateId'], payload['market'],
        payload['view'], PAGE_SIZE, sql], separators=(',', ':')).encode()).hexdigest()
    cursor = payload.get('cursor')
    if cursor:
        qid, position, page = read_token(cursor, secret, context)
    else:
        output = (f's3://{bucket}/governance/market_intelligence/candidates/'
                  f'professional-market-detail-{table_id}/page-results/')
        qid = athena.start_query_execution(
            QueryString=sql, WorkGroup=workgroup,
            ClientRequestToken=hashlib.sha256(f'{context}/{time.time()//3600:.0f}'.encode()).hexdigest(),
            ResultConfiguration={'OutputLocation': output, 'ExpectedBucketOwner': owner,
                                 'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}},
            ResultReuseConfiguration={'ResultReuseByAgeConfiguration': {'Enabled': True, 'MaxAgeInMinutes': 60}},
        )['QueryExecutionId']
        position, page = None, 1
    deadline = time.monotonic()+5
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']['State']
        if status not in ('QUEUED', 'RUNNING'):
            break
        if time.monotonic() >= deadline:
            return {'status':'pending', 'retry':token(secret, context, qid, position, page)}
        time.sleep(.4)
    if status != 'SUCCEEDED':
        raise RuntimeError(f'Market detail query {qid} ended with state {status}.')
    args = {'QueryExecutionId': qid, 'MaxResults': PAGE_SIZE if position else PAGE_SIZE+1}
    if position:
        args['NextToken'] = position
    result = athena.get_query_results(**args)
    names = [item['Name'] for item in result['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    rows = result['ResultSet']['Rows'] if position else result['ResultSet']['Rows'][1:]
    rows = [dict(zip(names,(cell.get('VarCharValue') for cell in row['Data']))) for row in rows]
    next_token = result.get('NextToken')
    return {'status':'ready', 'rows':rows, 'page':page,
            'next':token(secret, context, qid, next_token, page+1) if next_token else None}
