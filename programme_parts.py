"""Buyer-scoped item funding from recorded actions, without multiplying item links."""
import math
import re
import json


def buyer_name(value):
    if not value or not str(value).strip():
        return None
    text = str(value).strip().upper()
    key = re.sub('[^A-Z0-9]', '', text)
    return {
        'NAVY':'DEPARTMENT OF THE NAVY', 'USNAVY':'DEPARTMENT OF THE NAVY',
        'AIRFORCE':'DEPARTMENT OF THE AIR FORCE', 'USAIRFORCE':'DEPARTMENT OF THE AIR FORCE',
        'ARMY':'DEPARTMENT OF THE ARMY', 'USARMY':'DEPARTMENT OF THE ARMY',
        'DLA':'DEFENSE LOGISTICS AGENCY', 'DOD':'DEPARTMENT OF DEFENSE',
        'USDOD':'DEPARTMENT OF DEFENSE', 'DEFENSE':'DEPARTMENT OF DEFENSE',
    }.get(key, text)


def build_parts_query(name, years=None, agency=None, domain=None, include_zero=True,
                      min_spend=0, limit=100, offset=0, count_only=False, reviewed_items=None):
    programme = str(name or '').strip().upper()
    if not programme:
        raise ValueError('Choose a programme.')
    ys = sorted(set(years or []))
    if any(type(y) is not int or not 1900 <= y <= 2200 for y in ys):
        raise ValueError('Use valid fiscal years.')
    buyer = buyer_name(agency)
    minimum = float(min_spend)
    if not math.isfinite(minimum):
        raise ValueError('Use a finite minimum value.')
    clauses = []
    params = [programme]
    reviewed=[]
    for row in reviewed_items or []:
        if not re.fullmatch(r'[0-9]{9}',row.get('niin','')) or type(row.get('evidence_count'))is not int or row['evidence_count']<1:
            raise ValueError('Invalid reviewed item evidence.')
        nsn=row.get('nsn')
        if nsn is not None and (not re.fullmatch(r'[0-9]{13}',nsn)or not nsn.endswith(row['niin'])):
            raise ValueError('Reviewed stock number does not match its item identifier.')
        reviewed.append({'niin':row['niin'],'evidence_count':row['evidence_count'],'nsn':nsn,'item_name':row.get('item_name')})
    if len({r['niin']for r in reviewed})!=len(reviewed):raise ValueError('Duplicate reviewed item evidence.')
    reviewed_cte=''
    reviewed_union=''
    reviewed_join=''
    reviewed_select=''
    item_amount='COALESCE(t.amount,0)'
    financial_eligibility='financial_record_count > 0'
    item_nsn='COALESCE(p.nsn,b.niin)'
    item_description='p.description'
    if reviewed:
        params.append(json.dumps(reviewed,separators=(',',':')))
        reviewed_cte=""", reviewed_items AS (
            SELECT json_extract_string(value,'$.niin') AS niin,
                   CAST(json_extract(value,'$.evidence_count') AS BIGINT) AS evidence_count,
                   json_extract_string(value,'$.nsn') AS nsn,
                   json_extract_string(value,'$.item_name') AS item_name
            FROM json_each(?)
        )"""
        reviewed_union=' UNION SELECT niin FROM reviewed_items'
        reviewed_join=' LEFT JOIN reviewed_items e ON b.niin=e.niin'
        reviewed_select=',COALESCE(e.evidence_count,0) AS reviewed_award_item_evidence_count'
        item_amount='CASE WHEN COALESCE(t.financial_record_count,0)=0 AND COALESCE(e.evidence_count,0)>0 THEN NULL ELSE COALESCE(t.amount,0) END'
        financial_eligibility='(financial_record_count > 0 OR reviewed_award_item_evidence_count > 0)'
        item_nsn='COALESCE(p.nsn,e.nsn,b.niin)'
        item_description='COALESCE(p.description,e.item_name)'
    if ys:
        clauses.append('f.fiscal_year IN (' + ','.join('?' for _ in ys) + ')')
        params.extend(ys)
    if buyer:
        clauses.append('(f.sub_agency = ? OR f.parent_agency = ?)')
        params.extend([buyer, buyer])
    if domain:
        clauses.append('UPPER(TRIM(f.market_segment)) = ?')
        params.append(str(domain).strip().upper())
    where = ' AND '.join(clauses) or '1=1'
    sql = f"""
    WITH reference_bom AS (
        SELECT DISTINCT LPAD(TRIM(CAST(niin AS VARCHAR)),9,'0') AS niin
        FROM v_platform_bom WHERE UPPER(TRIM(platform_family)) = ?
    ){reviewed_cte}, bom AS (
        SELECT niin FROM reference_bom{reviewed_union}
    ), cache_floor AS (
        SELECT MIN(TRY_CAST(year AS INTEGER)) AS first_fy FROM v_transactions
    ), financial AS (
        SELECT LPAD(COALESCE(NULLIF(TRIM(CAST(t.niin AS VARCHAR)),''),
                   RIGHT(REGEXP_REPLACE(COALESCE(t.nsn,''),'[^0-9]','','g'),9)),9,'0') AS niin,
               TRY_CAST(t.year AS INTEGER) AS fiscal_year,
               UPPER(TRIM(t.vendor_cage)) AS cage, t.vendor_name AS vendor,
               UPPER(TRIM(t.sub_agency)) AS sub_agency,
               UPPER(TRIM(t.parent_agency)) AS parent_agency,
               t.market_segment, TRY_CAST(t.spend_amount AS DOUBLE) AS line_value,
               CAST(NULL AS DOUBLE) AS line_units, TRY_CAST(t.action_date AS DATE) AS last_sold,
               COALESCE(TRY_CAST(t.platform_count AS INTEGER),0) AS platform_count
        FROM v_transactions t
        WHERE NULLIF(TRIM(CAST(t.niin AS VARCHAR)),'') IS NOT NULL
           OR NULLIF(TRIM(CAST(t.nsn AS VARCHAR)),'') IS NOT NULL
        UNION ALL
        SELECT LPAD(TRIM(CAST(s.niin AS VARCHAR)),9,'0'), TRY_CAST(s.year AS INTEGER),
               UPPER(TRIM(s.cage)), s.vendor, UPPER(TRIM(s.sub_agency)),
               UPPER(TRIM(s.parent_agency)), s.market_segment,
               TRY_CAST(s.total_revenue AS DOUBLE), TRY_CAST(s.total_units_sold AS DOUBLE),
               TRY_CAST(s.last_sold AS DATE), COALESCE(TRY_CAST(s.platform_count AS INTEGER),0)
        FROM v_nsn_supplier_lookup s CROSS JOIN cache_floor c
        WHERE s.year < COALESCE(c.first_fy,2201)
    ), scoped AS (
        SELECT f.* FROM financial f INNER JOIN bom b ON f.niin=b.niin
        WHERE {where}
    ), annual AS (
        SELECT niin, fiscal_year, SUM(COALESCE(line_value,0)) AS amount
        FROM scoped GROUP BY 1,2
    ), trends AS (
        SELECT niin, STRING_AGG(CAST(fiscal_year AS VARCHAR)||':'||CAST(amount AS VARCHAR),
                               '|' ORDER BY fiscal_year) AS annual_revenue_trend
        FROM annual GROUP BY 1
    ), totals AS (
        SELECT niin, SUM(COALESCE(line_value,0)) AS amount, COUNT(*) AS financial_record_count,
               CASE WHEN COUNT(line_units)=COUNT(*) THEN SUM(line_units) END AS total_units_sold,
               MAX(last_sold) AS last_sold, MAX(platform_count) AS platform_count
        FROM scoped GROUP BY 1
    ), vendor_totals AS (
        SELECT niin,cage,MAX(vendor) AS vendor,SUM(COALESCE(line_value,0)) AS amount
        FROM scoped GROUP BY 1,2
    ), vendors AS (
        SELECT *,ROW_NUMBER() OVER(PARTITION BY niin ORDER BY amount DESC,cage) AS rank
        FROM vendor_totals
    ), profiles AS (
        SELECT LPAD(TRIM(CAST(niin AS VARCHAR)),9,'0') AS niin,
               MAX(nsn) AS nsn, MAX(item_name) AS description, MAX(fsc_code) AS fsc_code
        FROM v_nsn_profile_lookup GROUP BY 1
    ), item_rows AS (
        SELECT b.niin,{item_nsn} AS nsn,{item_nsn} AS item_id,
               {item_description} AS description,p.fsc_code,{item_amount} AS amount,
               t.total_units_sold,COALESCE(t.financial_record_count,0) AS financial_record_count,
               COALESCE(r.annual_revenue_trend,'') AS annual_revenue_trend,t.last_sold,
               v.cage AS top_vendor,v.vendor AS top_vendor_name,
               COALESCE(t.platform_count,0) AS platform_count{reviewed_select}
        FROM bom b LEFT JOIN totals t ON b.niin=t.niin
        LEFT JOIN trends r ON b.niin=r.niin LEFT JOIN profiles p ON b.niin=p.niin
        LEFT JOIN vendors v ON b.niin=v.niin AND v.rank=1{reviewed_join}
    )
    """
    conditions = ['(? OR amount <> 0)', '(? = 0 OR amount >= ?)', '(? = FALSE OR '+financial_eligibility+')']
    params.extend([bool(include_zero), minimum, minimum, bool(buyer or ys or domain)])
    select = ('COUNT(*) AS count, (SELECT first_fy FROM cache_floor) AS buyer_history_start_fy' if count_only
              else '*, COUNT(*) OVER() AS matching_item_count, (SELECT first_fy FROM cache_floor) AS buyer_history_start_fy')
    sql += f" SELECT {select} FROM item_rows WHERE " + ' AND '.join(conditions)
    if not count_only:
        sql += ' ORDER BY amount DESC,niin LIMIT ? OFFSET ?'
        params.extend([max(1,min(int(limit),5000)),max(0,int(offset))])
    return sql,params
