"""Exact item references across current and earlier retained notices.

An association does not establish that a request consumes a forecast quantity.
Amendment identifiers are source-mentioned identifiers, not inferred aliases.
"""
import re

def query(repo,episode_id=None):
    if episode_id is not None and not re.fullmatch('[a-f0-9]{64}',episode_id):raise ValueError('Invalid item episode')
    opportunities=repo.table('opportunities');decisions=repo.table('demand_decisions')
    target=" AND f.episode_id='"+episode_id+"'" if episode_id else ''
    return r"""WITH forecast_items AS (
 SELECT f.episode_id forecast_episode_id,f.record_hash forecast_record_hash,json_extract_scalar(f.value_inputs_json,'$.niin') niin
 FROM OPPORTUNITIES f WHERE f.lane IN ('source_item_forecasts','stock_supported_item_demand') TARGET
 ),notice_items AS (
 SELECT n.*,d.stage recorded_stage,exact_niin,
 json_format(cast(array_distinct(regexp_extract_all(upper(coalesce(n.scope_text,'')),'\b[A-Z0-9]{6}[0-9]{2}[A-Z][0-9]{4}\b')) AS json)) source_mentioned_identifiers_json
 FROM OPPORTUNITIES n JOIN DECISIONS d ON d.episode_id=n.episode_id
 CROSS JOIN UNNEST(array_distinct(filter(concat(ARRAY[json_extract_scalar(n.value_inputs_json,'$.niin'),substr(json_extract_scalar(n.value_inputs_json,'$.nsn'),5,9)],
 transform(regexp_extract_all(coalesce(n.scope_text,''),'(?i)\bNSN[ :#-]*([0-9]{4}[- ]?[0-9]{2}[- ]?[0-9]{3}[- ]?[0-9]{4})\b',1),x->substr(regexp_replace(x,'[^0-9]',''),5,9))),x->regexp_like(x,'^[0-9]{9}$')))) AS u(exact_niin)
 WHERE n.source_name IN ('sam_notices','dla_solicitations')
 ) SELECT f.forecast_episode_id,f.forecast_record_hash,f.niin,n.episode_id notice_episode_id,n.record_hash notice_record_hash,n.native_identifier,n.title,n.lane,n.recorded_stage,n.source_url,n.source_evidence_id,n.source_locator,n.notice_posted_date,n.window_end response_deadline,
 try_cast(json_extract_scalar(n.value_inputs_json,'$.quantity') AS double) source_quantity,json_extract_scalar(n.value_inputs_json,'$.request_unit') source_unit,n.source_mentioned_identifiers_json,
 'exact_item_reference_context' relationship_type,
 'A retained notice references this exact item. Its quantity and status are separate from the source forecast; fulfilment, award and quantity subtraction are not established.' interpretation
 FROM forecast_items f JOIN notice_items n ON n.exact_niin=f.niin
 """.replace('OPPORTUNITIES',opportunities).replace('DECISIONS',decisions).replace('TARGET',target)
