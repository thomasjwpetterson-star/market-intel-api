"""Explain missing item matches without treating missing identifiers as no demand."""
import math
from programme_parts import buyer_name
from programme_context import aliases_for, context_for
from research_descriptions import description_predicate


def build_coverage_query(payload, columns):
    if not isinstance(payload, dict) or set(payload)-{'program', 'years', 'agency', 'domain'}:
        raise ValueError('Choose programme, fiscal years and recorded agency.')
    programme = payload.get('program')
    if not isinstance(programme, str) or not 2 <= len(programme.strip()) <= 128:
        raise ValueError('Choose a programme name.')
    programme = programme.strip().upper()
    years = payload.get('years', [])
    if not isinstance(years, list) or len(years)>10 or any(type(y) is not int or not 1900<=y<=2200 for y in years):
        raise ValueError('Use up to ten valid fiscal years.')
    agency = payload.get('agency')
    if agency is not None and (not isinstance(agency,str) or not 2<=len(agency.strip())<=128):
        raise ValueError('Choose a recorded agency.')
    agency = buyer_name(agency)
    required = {'niin','nsn','year','platform_family','sub_agency','parent_agency','spend_amount'}
    if not required.issubset(columns):
        raise ValueError('Programme coverage columns are unavailable in this source.')
    aliases = aliases_for(programme)
    text_sql, text_params = description_predicate(aliases, columns)
    raw_item = "COALESCE(NULLIF(TRIM(CAST(niin AS VARCHAR)),''),NULLIF(RIGHT(REGEXP_REPLACE(COALESCE(nsn,''),'[^0-9]','','g'),9),''))"
    item = f"CASE WHEN REGEXP_FULL_MATCH({raw_item},'[0-9]{{1,9}}') AND TRY_CAST({raw_item} AS BIGINT)>0 THEN LPAD({raw_item},9,'0') END"
    where, params = [], [programme, programme, *text_params]
    if years:
        where.append('year IN ('+','.join('?' for _ in sorted(set(years)))+')')
        params.extend(sorted(set(years)))
    if payload.get('domain'):
        if not isinstance(payload['domain'],str) or len(payload['domain'])>128 or 'market_segment' not in columns:
            raise ValueError('Choose an available market segment.')
        where.append('UPPER(TRIM(market_segment))=?');params.append(payload['domain'].strip().upper())
    scope = ' AND '.join(where) or '1=1'
    source = "COALESCE(NULLIF(UPPER(TRIM(source_system)),''),'UNKNOWN')" if 'source_system' in columns else "'UNKNOWN'"
    sql = f"""
    WITH bom AS (
        SELECT DISTINCT LPAD(TRIM(CAST(niin AS VARCHAR)),9,'0') AS item
        FROM v_platform_bom WHERE UPPER(TRIM(platform_family))=?
    ), base AS (
        SELECT UPPER(TRIM(sub_agency)) AS buyer, UPPER(TRIM(parent_agency)) AS department,
               {source} AS source_system, {item} AS item,
               COALESCE(TRY_CAST(spend_amount AS DOUBLE),0) AS amount,
               COALESCE(UPPER(TRIM(platform_family))=?,FALSE) AS programme_match,
               NULLIF(TRIM(platform_family),'') IS NULL AS unmapped,
               {text_sql} AS description_match
        FROM v_transactions WHERE {scope}
    )
    SELECT buyer,department,source_system,GROUPING(buyer) AS all_buyers,
           (SELECT MIN(year) FROM v_transactions) AS action_history_start_fy,
           (SELECT COUNT(*) FROM bom) AS reference_item_count,
           COUNT(*) AS buyer_action_count,
           COUNT(item) AS buyer_actions_with_item_number,
           COUNT(*) FILTER(WHERE programme_match) AS programme_action_count,
           COUNT(item) FILTER(WHERE programme_match) AS programme_actions_with_item_number,
           COUNT(*) FILTER(WHERE programme_match AND bom.item IS NOT NULL) AS programme_actions_with_reference_item,
           COALESCE(SUM(amount) FILTER(WHERE programme_match),0) AS programme_net_obligations_usd,
           COUNT(*) FILTER(WHERE description_match AND NOT programme_match) AS description_candidate_action_count,
           COUNT(*) FILTER(WHERE description_match AND unmapped) AS unmapped_description_action_count,
           COALESCE(SUM(amount) FILTER(WHERE description_match AND NOT programme_match),0) AS description_candidate_net_obligations_usd,
           COUNT(DISTINCT base.item) FILTER(WHERE bom.item IS NOT NULL) AS reference_linked_item_count
    FROM base LEFT JOIN bom ON base.item=bom.item
    GROUP BY GROUPING SETS ((buyer,department,source_system),())
    """
    # Qualify the only ambiguous column after the item crosswalk join.
    sql=sql.replace('COUNT(item)', 'COUNT(base.item)')
    return sql, params, {'program':programme, **({'years':sorted(set(years))} if years else {}), **({'agency':agency} if agency else {})}, aliases


def run_coverage(payload, columns, read):
    sql,params,filters,aliases=build_coverage_query(payload, columns)
    rows=read(sql,params)
    total=next((r for r in rows if r['all_buyers']==1),None)
    if total is None:
        raise ValueError('The source did not establish programme coverage.')
    selected=[r for r in rows if not r['all_buyers'] and filters['agency'] in (r['buyer'],r['department'])] if filters.get('agency') else [total]
    integers=['buyer_action_count','buyer_actions_with_item_number','programme_action_count','programme_actions_with_item_number','programme_actions_with_reference_item','description_candidate_action_count','unmapped_description_action_count','reference_linked_item_count']
    summary={k:sum(int(r.get(k) or 0) for r in selected) for k in integers}
    for k in ['programme_net_obligations_usd','description_candidate_net_obligations_usd']:
        value=sum(float(r.get(k) or 0) for r in selected)
        if not math.isfinite(value):raise ValueError('The source returned an invalid funding amount.')
        summary[k]=round(value,2)
    # Distinct item counts cannot be summed across buyers/sources.
    if len(selected)>1:
        summary.pop('reference_linked_item_count')
    first_fy=total['action_history_start_fy']
    full_window=bool(first_fy is not None and (not filters.get('years') or min(filters['years'])>=first_fy))
    if not full_window:reason='action_history_does_not_cover_full_requested_window'
    elif summary['programme_action_count'] and not summary['programme_actions_with_item_number']:reason='programme_actions_have_no_item_numbers'
    elif summary['programme_actions_with_item_number'] and not summary['programme_actions_with_reference_item']:reason='programme_item_numbers_not_in_reference_crosswalk'
    elif not summary['programme_action_count'] and summary['description_candidate_action_count']:reason='description_matches_outside_primary_programme_mapping'
    elif not summary['programme_action_count']:reason='no_primary_programme_actions_for_recorded_agency'
    else:reason='programme_item_identifiers_available'
    buyers=sorted([{'agency':r['buyer'],'source_system':r['source_system'],'action_count':int(r['programme_action_count']),'net_obligations_usd':round(float(r['programme_net_obligations_usd']),2)} for r in rows if not r['all_buyers'] and r['programme_action_count']],key=lambda r:r['net_obligations_usd'],reverse=True)
    return {'schema_version':'1.0','filters':filters,'reason':reason,'selected_scope':summary,
        'action_history_start_fy':first_fy,'full_requested_action_window_available':full_window,
        'reference_item_count':int(total['reference_item_count']),
        'programme_recorded_agencies':buyers[:10],'programme_recorded_agencies_scope':'Same fiscal years and market segment; all recorded agencies.',
        'agency_interpretation':'Recorded contracting agency, not a complete funding-service or operating-service breakdown.',
        'description_search_terms':aliases,
        'description_candidate_interpretation':'Description matches outside the primary programme mapping are research evidence. Their funding is not added to confirmed programme totals.',
        'programme_context':context_for(filters['program'])}
