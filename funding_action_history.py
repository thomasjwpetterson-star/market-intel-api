"""Balanced, whole-scope funding context with bounded native-action pages."""
import json
from private_research_query import build_queries
from private_research_context import effective_award_key


def action_history(payload, read):
    if not isinstance(payload,dict)or set(payload)-{'filters','limit','offset'}:
        raise ValueError('Use the documented action-history fields.')
    filters=payload.get('filters',{})
    if not isinstance(filters,dict)or not(filters.get('cage')or filters.get('contract_id')):
        raise ValueError('Choose an exact supplier site or contract.')
    if set(filters)-{'cage','contract_id','years','psc','agency','department'}:
        raise ValueError('Choose documented action-history filters.')
    limit,offset=payload.get('limit',6),payload.get('offset',0)
    if type(limit)is not int or not 1<=limit<=11:raise ValueError('Choose an action page of one to eleven rows.')
    q=build_queries({'dataset':'contract_actions','filters':filters,'group_by':['year'],'limit':limit,'offset':offset})
    sql="""WITH selected AS MATERIALIZED (
        SELECT award_key,transaction_key,source_system,contract_id,action_date,year,
               vendor_cage,vendor_name,parent_agency,sub_agency,psc,naics_code,
               description,platform_family,spend_amount,
               """+effective_award_key()+""" AS native_identity
        FROM v_transactions WHERE """+q['where_sql']+"""
    ), totals AS (
        SELECT COUNT(*) AS action_count,COUNT(DISTINCT native_identity) AS contract_count,
               SUM(spend_amount) AS net_obligations_usd,
               SUM(CASE WHEN spend_amount>0 THEN spend_amount ELSE 0 END) AS increases_usd,
               SUM(CASE WHEN spend_amount<0 THEN spend_amount ELSE 0 END) AS reductions_usd,
               COUNT(*) FILTER(WHERE spend_amount>0) AS increase_action_count,
               COUNT(*) FILTER(WHERE spend_amount<0) AS reduction_action_count,
               COUNT(*) FILTER(WHERE spend_amount=0) AS zero_value_action_count
        FROM selected
    ), annual AS (
        SELECT year,SUM(spend_amount) AS net_obligations_usd FROM selected GROUP BY year ORDER BY year
    ), ranked_awards AS (
        SELECT native_identity AS award_key,contract_id,SUM(spend_amount) AS net_obligations_usd,
               COUNT(*) AS action_count FROM selected GROUP BY 1,2
        ORDER BY ABS(net_obligations_usd) DESC,award_key LIMIT 5
    ), increases AS (
        SELECT * EXCLUDE(native_identity) FROM selected WHERE spend_amount>0
        ORDER BY spend_amount DESC,award_key,transaction_key LIMIT 3
    ), reductions AS (
        SELECT * EXCLUDE(native_identity) FROM selected WHERE spend_amount<0
        ORDER BY spend_amount ASC,award_key,transaction_key LIMIT 3
    ), page AS (
        SELECT * EXCLUDE(native_identity) FROM selected
        ORDER BY action_date DESC NULLS LAST,award_key,transaction_key LIMIT ? OFFSET ?
    )
    SELECT 'summary' AS kind,to_json(t) AS payload FROM totals t
    UNION ALL SELECT 'annual',to_json(t) FROM annual t
    UNION ALL SELECT 'award',to_json(t) FROM ranked_awards t
    UNION ALL SELECT 'increase',to_json(t) FROM increases t
    UNION ALL SELECT 'reduction',to_json(t) FROM reductions t
    UNION ALL SELECT 'page',to_json(t) FROM page t"""
    buckets={k:[]for k in ['summary','annual','award','increase','reduction','page']}
    for row in read(sql,[*q['where_params'],limit,offset]):
        buckets[row['kind']].append(json.loads(row['payload']))
    # Preserve advertised order even if the UNION branches run concurrently.
    buckets['page'].sort(key=lambda r:(r['award_key']or'',r['transaction_key']or''))
    buckets['page'].sort(key=lambda r:r['action_date']or'',reverse=True)
    buckets['annual'].sort(key=lambda r:r['year'])
    if len(buckets['summary'])!=1:raise ValueError('The complete available-action summary was not established.')
    summary=buckets['summary'][0]
    if not summary['action_count']:
        summary.update(net_obligations_usd=None,increases_usd=None,reductions_usd=None)
    return {'schema_version':'1.0','filters':q['filters'],'scope_summary':summary,
            'annual_summary':buckets['annual'],'leading_awards_by_absolute_net_funding':buckets['award'],
            'largest_increases':buckets['increase'],'largest_reductions':buckets['reduction'],
            'rows':buckets['page'],'source_grain':'AVAILABLE_CONTRACT_ACTIONS',
            'ordering':'action_date_desc_then_native_award_and_transaction_keys',
            'summary_scope':'ALL_AVAILABLE_ACTIONS_MATCHING_FILTERS',
            'interpretation':'Net funding includes increases and reductions. A positive action can be offset by another action; read the whole-scope summary before interpreting a page.'}
