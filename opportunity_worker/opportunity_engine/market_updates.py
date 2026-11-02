"""Small, source-linked budget revisions for explicit work interests.

These are changes between editions for the SAME fiscal-year budget line, not
changes in supplier revenue, awarded spend, or a forecast contract value.
"""
import hashlib
import json
import math
import re
from urllib.parse import urlparse
from .discovery_tags import parse_tags
from .work_vocabulary import SPEC,plain,annotate


def quoted(value):
    return "'"+str(value).replace("'","''")+"'"


def url(value):
    text=str(value or '')
    parsed=urlparse(text)
    return text if parsed.scheme=='https' and parsed.netloc and not parsed.username and not parsed.password else None


def budget_query(revisions,funding,patterns,limit):
    text="coalesce(p.budget_line_item_title,'')"
    conditions=[]
    for rule in patterns:
        condition='regexp_like('+text+','+quoted('(?i)'+rule['pattern'])+')'
        if rule.get('exclude'):condition+=' AND NOT regexp_like('+text+','+quoted('(?i)'+rule['exclude'])+')'
        conditions.append('('+condition+')')
    if not conditions:raise ValueError('Explicit work interests required')
    return """WITH candidates AS(SELECT r.*,p.budget_line_item_title,p.description_excerpt,p.source_url current_source_url,
    p.source_locator current_source_locator,old.source_url previous_source_url,old.source_locator previous_source_locator,
    row_number() OVER(PARTITION BY r.component,r.budget_line_item,r.fiscal_year,r.measure_type,r.value_kind
    ORDER BY r.submission_fiscal_year DESC,r.funding_id) revision_rank
    FROM REVISIONS r JOIN FUNDING p ON p.funding_id=r.funding_id JOIN FUNDING old ON old.funding_id=r.previous_funding_id
    WHERE r.funding_status=r.previous_funding_status AND nullif(trim(r.funding_status),'') IS NOT NULL
    AND r.amount_usd>=0 AND r.previous_amount_usd>0 AND r.submission_fiscal_year>r.previous_submission_fy
    AND p.fiscal_year=old.fiscal_year AND p.measure_type=old.measure_type AND p.value_kind=old.value_kind
    AND p.component=old.component AND p.budget_line_item=old.budget_line_item AND p.appropriation_category=old.appropriation_category
    AND abs(r.amount_usd-r.previous_amount_usd)>=1000000 AND abs((r.amount_usd-r.previous_amount_usd)/r.previous_amount_usd)>=0.05
    AND (CONDITIONS)) SELECT * FROM candidates WHERE revision_rank=1
    ORDER BY submission_fiscal_year DESC,abs(change_usd) DESC,funding_id LIMIT LIMITVALUE""".replace('REVISIONS',revisions).replace('FUNDING',funding).replace('CONDITIONS',' OR '.join(conditions)).replace('LIMITVALUE',str(limit))


def future_plan_query(funding,patterns,as_of,limit):
    # Compare adjacent future FYs in the same book/status/measure. Do not turn
    # enacted-versus-requested differences into a like-for-like growth claim.
    text="coalesce(p.budget_line_item_title,'')"
    conditions=[]
    for rule in patterns:
        condition='regexp_like('+text+','+quoted('(?i)'+rule['pattern'])+')'
        if rule.get('exclude'):condition+=' AND NOT regexp_like('+text+','+quoted('(?i)'+rule['exclude'])+')'
        conditions.append('('+condition+')')
    return """WITH latest AS(SELECT max(submission_fiscal_year) edition FROM FUNDING), pairs AS(
    SELECT p.funding_id,old.funding_id previous_funding_id,p.component,p.budget_line_item,p.budget_line_item_title,p.description_excerpt,
    p.fiscal_year,old.fiscal_year previous_fiscal_year,p.submission_fiscal_year,old.submission_fiscal_year previous_submission_fy,
    p.funding_status,p.measure_type,p.amount_usd,old.amount_usd previous_amount_usd,p.amount_usd-old.amount_usd change_usd,
    p.source_url current_source_url,p.source_locator current_source_locator,old.source_url previous_source_url,old.source_locator previous_source_locator,
    'future_plan' comparison_kind,
    row_number() OVER(PARTITION BY p.component,p.budget_line_item ORDER BY CASE p.measure_type WHEN 'net_procurement_p1' THEN 0 ELSE 1 END,old.fiscal_year,p.funding_id) pair_rank
    FROM FUNDING p JOIN FUNDING old ON p.component=old.component AND p.appropriation_category=old.appropriation_category
    AND p.budget_line_item=old.budget_line_item AND p.submission_fiscal_year=old.submission_fiscal_year
    AND p.fiscal_year=old.fiscal_year+1 AND p.measure_type=old.measure_type AND p.value_kind=old.value_kind
    WHERE p.submission_fiscal_year=(SELECT edition FROM latest) AND p.funding_status='projected' AND old.funding_status='projected'
    AND p.measure_type IN ('net_procurement_p1','total_obligation_authority') AND p.value_kind='amount_usd'
    AND p.amount_usd>=0 AND old.amount_usd>0 AND p.amount_usd<>old.amount_usd
    AND abs(p.amount_usd-old.amount_usd)>=1000000 AND abs((p.amount_usd-old.amount_usd)/old.amount_usd)>=0.05
    AND old.fiscal_year>year(DATE ASOF) AND (CONDITIONS))
    SELECT * FROM pairs WHERE pair_rank=1 ORDER BY abs(change_usd) DESC,funding_id LIMIT LIMITVALUE""".replace('FUNDING',funding).replace('ASOF',quoted(as_of)).replace('CONDITIONS',' OR '.join(conditions)).replace('LIMITVALUE',str(limit))


def render(row,selected_topics):
    try:
        before=float(row['previous_amount_usd']);after=float(row['amount_usd'])
        year=int(row['fiscal_year']);edition=int(row['submission_fiscal_year']);previous=int(row['previous_submission_fy'])
    except (ValueError,TypeError,KeyError):return None
    if not all(math.isfinite(x) for x in (before,after)) or before<=0 or after<0 or before==after:return None
    planned=row.get('comparison_kind')=='future_plan'
    if planned:
        try:previous_year=int(row['previous_fiscal_year'])
        except (KeyError,TypeError,ValueError):return None
        if previous_year+1!=year or edition!=previous:return None
    sources=[dict(label=f'FY{edition} budget book',url=url(row.get('current_source_url')),location=plain(row.get('current_source_locator'))),
             dict(label=f'FY{previous} budget book',url=url(row.get('previous_source_url')),location=plain(row.get('previous_source_locator')))]
    if not all(s['url'] for s in sources):return None
    # Incidental mentions of another system in a narrative must not make
    # this programme a trend for that technology (e.g. AIM-9X mentioning
    # complementary radar-guided AMRAAM). Start with explicit line titles.
    annotation=annotate(row.get('budget_line_item_title'),reference={})
    matches=[t for t in annotation['topics'] if t['topic_id'] in selected_topics]
    if not matches:return None
    title=plain(row.get('budget_line_item_title'))
    if not title:return None
    percent=(after-before)/before*100
    if abs(percent)<5 or abs(after-before)<1000000:return None
    # Disambiguate named military systems from generic component nouns.
    if re.search(r'(?i)\brolling airframe missile\b|\bship[ -]to[ -]shore connector\b',title):return None
    direction='increased' if after>before else 'reduced'
    headline=(f'{title}: planned funding {direction} {abs(percent):.0f}% in FY{year}' if planned else f'{title}: FY{year} budget {direction} {abs(percent):.0f}%')
    summary=(f'{plain(row.get("component"))} projects ${before:,.0f} in FY{previous_year} and ${after:,.0f} in FY{year} in its FY{edition} budget plan.' if planned else f'{plain(row.get("component"))} revised this FY{year} budget line from ${before:,.0f} to ${after:,.0f} between its FY{previous} and FY{edition} budget books.')
    return dict(update_id=hashlib.sha256(('budget_revision|'+str(row['funding_id'])+'|'+str(row['previous_funding_id'])).encode()).hexdigest(),
        category='future_budget_plan' if planned else 'budget_plan_change',title=headline,summary=summary,
        fiscal_year=year,budget_edition=edition,previous_budget_edition=previous,
        current_budget=after,previous_budget=before,change_percent=round(percent,2),
        value_kind='programme_budget_context',relevance_basis='explicit_budget_line_title',funding_status=plain(row.get('funding_status')),
        matched_interests=[dict(tag_id=('scope:' if t['facet']=='work_scope' else 'context:')+t['topic_id'],label=t['label'],passage=t['passage']) for t in matches],
        evidence=sources,note='A budget-plan change, not an awarded contract or value available to one supplier.')


class MarketUpdatesMixin:
    def market_updates(self,tags='',company=None,limit=3):
        if type(limit) is not int or not 1<=limit<=3:raise ValueError('Use one to three market updates')
        if company and not re.fullmatch('CAGE:[A-Z0-9]{5}',company):raise ValueError('Invalid company identifier')
        selected=parse_tags(tags);m=self.manifest()
        result=dict(records=[],release=m['run_id'],as_of=m['as_of'],availability='available',selection_basis='selected_interests',selected_tags=[])
        required=('budget_revision_signals','programme_funding','work_vocabulary_topics','opportunity_vocabulary')
        if not all(n in m['tables'] for n in required):return dict(result,availability='not_published')
        catalogue=self._discovery_catalogue(m)
        if not selected and company:
            # Only explicit source-backed work topics; a platform mention or
            # NAICS code does not become demonstrated manufacturing capability.
            if 'company_capability_vocabulary' in m['tables'] and 'work_vocabulary_scopes' in m['tables']:
                for row in self.capability_vocabulary(company[5:],25,0):
                    try:a=json.loads(row.get('annotation_json') or '{}')
                    except (ValueError,TypeError):continue
                    selected.extend('scope:'+t for t in a.get('work_scope_topic_ids',[]) if 'scope:'+t in catalogue.records)
            selected=list(dict.fromkeys(selected))[:10]
            result['selection_basis']='observed_company_work_topics'
        catalogue.validate(selected);result['selected_tags']=selected
        topics={t['legacy_topic_id'] for key in selected for t in catalogue.expansion(key) if t.get('legacy_topic_id') and t['facet'] in ('work_scope','context')}
        patterns=[r for r in SPEC['topics'] if r['id'] in topics]
        if not patterns:return result
        query=budget_query(self._discovery_table(m,'budget_revision_signals'),self._discovery_table(m,'programme_funding'),patterns,30)
        # Extra rows allow local positive/negative-context checks and removal of
        # duplicate FY slices of one line, without inventing a market trend.
        used=set()
        rows=self.rows(query)
        if len(rows)<30:
            rows+=self.rows(future_plan_query(self._discovery_table(m,'programme_funding'),patterns,m['as_of'],30))
        for row in rows:
            item=render(row,topics)
            key=(row.get('component'),row.get('budget_line_item'))
            if item and key not in used:
                used.add(key);result['records'].append(item)
                if len(result['records'])==limit:break
        return result
