"""Fixed, parameterised read-only aggregates for the authenticated research proxy.

Only analytical views and dimensions declared here can be queried. This module
does not accept SQL, identifiers, joins or expressions supplied by a caller.
"""
import math
import re

SOURCES = {
    'funding_summary': {'table':'v_summary','amount':'total_spend','cage':'cage_code','company':'clean_parent','psc':'psc_code'},
    'contract_actions': {'table':'v_transactions','amount':'spend_amount','cage':'vendor_cage','company':'vendor_name','psc':'psc'},
}
DIMENSIONS = {'year':'year','company':None,'agency':'sub_agency','psc':None,'program':'platform_family','naics':'naics_code'}
FILTERS = {'years','cage','parent','agency','department','psc','naics','program','contract_id','search','description_terms'}

def _text(value, maximum=128):
    if not isinstance(value,str) or not 1<=len(value.strip())<=maximum or any(ord(c)<32 for c in value):
        raise ValueError('Use a nonempty research filter within the length limit.')
    return value.strip().upper()

def build_queries(payload):
    if not isinstance(payload,dict) or set(payload)-{'dataset','group_by','filters','limit','offset'}:
        raise ValueError('Choose the documented research query fields.')
    dataset=payload.get('dataset','funding_summary')
    if not isinstance(dataset,str) or dataset not in SOURCES:raise ValueError('Choose funding_summary or contract_actions.')
    spec=SOURCES[dataset]
    groups=payload.get('group_by',['year'])
    if not isinstance(groups,list) or not 1<=len(groups)<=3 or any(not isinstance(g,str) or g not in DIMENSIONS for g in groups) or len(set(groups))!=len(groups):
        raise ValueError('Choose one to three distinct research grouping dimensions.')
    limit=payload.get('limit',10)
    if type(limit) is not int or not 1<=limit<=50:raise ValueError('Use a result limit between 1 and 50.')
    offset=payload.get('offset',0)
    if type(offset) is not int or not 0<=offset<=100000:raise ValueError('Use a valid research page offset.')
    filters=payload.get('filters',{})
    if not isinstance(filters,dict) or not filters or set(filters)-FILTERS:raise ValueError('Supply at least one documented research filter.')
    clauses=[];params=[];normal={};prefix=''
    for key,value in filters.items():
        if key in {'search','description_terms'}:
            if dataset!='contract_actions':raise ValueError('Description searches use contract_actions.')
            from research_descriptions import description_predicate, description_terms
            terms=description_terms(value)
            if key=='search' and not isinstance(value,str):raise ValueError('Use a single search string.')
            normal[key]=terms[0] if key=='search' else terms
            text_sql,text_params=description_predicate(terms)
            clauses.append(text_sql);params.extend(text_params)
            continue
        if key=='years':
            if not isinstance(value,list) or not 1<=len(value)<=10 or any(type(y) is not int or not 1900<=y<=2200 for y in value):raise ValueError('Use one to ten financial years.')
            years=sorted(set(value));normal[key]=years
            clauses.append('year IN ('+','.join('?' for _ in years)+')');params.extend(years)
            continue
        clean=_text(value)
        if key=='cage' and not re.fullmatch(r'[A-Z0-9]{5}',clean):raise ValueError('Use a five-character supplier CAGE.')
        if key=='psc' and not re.fullmatch(r'[A-Z0-9]{4}',clean):raise ValueError('Use a four-character buying category.')
        if key=='naics' and not re.fullmatch(r'[0-9]{6}',clean):raise ValueError('Use a six-digit industry category.')
        normal[key]=clean
        if key=='department':
            if clean in {'DOD','US DOD','U.S. DOD','DEFENSE'}:clean='DEPARTMENT OF DEFENSE'
            normal[key]=clean
            if dataset=='contract_actions':clauses.append('UPPER(TRIM(parent_agency)) = ?');params.append(clean)
            else:
                # Preserve the dashboard's financial grain, resolving hierarchy
                # through recorded agency relationships rather than guessing
                # that one sub-agency name represents the whole department.
                prefix='WITH department_members AS (SELECT DISTINCT UPPER(TRIM(sub_agency)) AS agency FROM v_transactions WHERE UPPER(TRIM(parent_agency)) = ? UNION SELECT DISTINCT UPPER(TRIM(sub_agency)) AS agency FROM v_contracts_rolled WHERE UPPER(TRIM(parent_agency)) = ?) '
                clauses.append('UPPER(TRIM(sub_agency)) IN (SELECT agency FROM department_members)')
            continue
        if key=='parent':
            if dataset!='funding_summary':raise ValueError('Parent-company filters use funding_summary; action rows retain their recorded vendor.')
            column='clean_parent'
        elif key=='contract_id':
            if dataset!='contract_actions':raise ValueError('Contract identifiers use contract_actions.')
            if not re.fullmatch(r'[A-Z0-9_-]{2,128}',clean):raise ValueError('Use a contract identifier.')
            column='contract_id'
        else:column={'cage':spec['cage'],'agency':'sub_agency','psc':spec['psc'],'program':'platform_family','naics':'naics_code','naics':'naics_code'}[key]
        clauses.append(f'UPPER(TRIM(CAST({column} AS VARCHAR))) = ?');params.append(clean)
    if prefix:params=[normal['department'],normal['department']]+params
    where=' AND '.join(clauses)
    base=f" FROM {spec['table']} WHERE {where}"
    metric=f"COALESCE(SUM(TRY_CAST({spec['amount']} AS DOUBLE)),0)"
    dimensions=[(spec['company'] if g=='company' else spec['psc'] if g=='psc' else DIMENSIONS[g],g) for g in groups]
    select=', '.join(f'{column} AS {alias}' for column,alias in dimensions)
    group=', '.join(str(i+1) for i in range(len(groups)))
    order='year ASC' if groups==['year'] else 'observed_obligations_usd DESC, '+', '.join(g+' ASC NULLS LAST' for g in groups)
    total_sql=prefix+f'SELECT {metric} AS observed_obligations_usd, COUNT(*) AS source_row_count'+base
    grouped_sql=prefix+f'SELECT {select}, {metric} AS observed_obligations_usd'+base+f' GROUP BY {group} ORDER BY {order} LIMIT ? OFFSET ?'
    return {'dataset':dataset,'filters':normal,'group_by':groups,'limit':limit,'offset':offset,'total_sql':total_sql,'total_params':params,'grouped_sql':grouped_sql,'grouped_params':params+[limit+1,offset]}

def run_aggregate(payload, read):
    query=build_queries(payload)
    total_rows=read(query['total_sql'],query['total_params'])
    rows=read(query['grouped_sql'],query['grouped_params'])
    if len(total_rows)!=1 or len(rows)>query['limit']+1:raise ValueError('The research source returned an inconsistent aggregate.')
    total=total_rows[0];value=total['observed_obligations_usd']
    if not isinstance(value,(int,float)) or not math.isfinite(value):raise ValueError('The research source returned an invalid amount.')
    for row in rows:
        if not isinstance(row.get('observed_obligations_usd'),(int,float)) or not math.isfinite(row['observed_obligations_usd']):raise ValueError('The research source returned an invalid amount.')
    count=int(total['source_row_count'])
    return {'schema_version':'1.0','dataset':query['dataset'],'filters':query['filters'],'group_by':query['group_by'],
            'observed_obligations_usd':round(value,2) if count else None,'source_row_count':count,
            'groups':[{**row,'observed_obligations_usd':round(row['observed_obligations_usd'],2)} for row in rows[:query['limit']]],
            'groups_truncated':len(rows)>query['limit'],'measure':'recorded_net_contract_obligations',
            'match_basis':'description_search_not_confirmed_programme_attribution' if set(query['filters']) & {'search','description_terms'} else 'structured_source_filters',
            'company_grouping':'recorded_parent' if query['dataset']=='funding_summary' else 'recorded_vendor',
            'department_membership_basis':'recorded_parent_and_sub_agency_relationships' if 'department' in query['filters'] else None}
