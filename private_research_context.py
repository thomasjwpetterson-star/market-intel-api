"""Read-only current-v7 adapters for the authenticated MCP research routes."""
import json
import re

META_BASIS='CURRENT_V7'


def effective_award_key(alias=''):
    p=alias+'.'if alias else''
    return "COALESCE(NULLIF(TRIM("+p+"award_key),''),CONCAT(COALESCE(NULLIF(TRIM("+p+"source_system),''),'UNKNOWN'),'|PIID|',COALESCE(NULLIF(TRIM("+p+"contract_id),''),'<NULL>'),'|CAGE|',COALESCE(NULLIF(TRIM("+p+"vendor_cage),''),'<NULL>')))"


def metadata(reference,program=None):
    entity=reference.resolve(program)if program else None
    if program and entity is None:raise ValueError('Select a unique registered v7 programme name.')
    return {'mapping_basis':META_BASIS,'mapping_release_id':reference.manifest['release_id'],
        'requested_program':program,'canonical_entity_id':entity['canonical_entity_id']if entity else None,
        'canonical_programme_name':entity['canonical_label']if entity else None,
        'buyer_filter_basis':'RECORDED_ACTION_AGENCY','award_identity_basis':'AGENCY_QUALIFIED_NATIVE_AWARD_KEY',
        'financial_allocation_eligible':False}


def prepend(sql,ctes):
    stripped=sql.lstrip()
    return 'WITH '+ctes+', '+stripped[5:]if stripped.upper().startswith('WITH ')else'WITH '+ctes+' '+stripped


def action_relation(reference,program=None):
    """Override reviewed prime awards; preserve source-native associations otherwise."""
    names={r['canonical_entity_id']:r['canonical_label']for r in reference.registry}
    decisions=[{'award_key':r['award_key'],'label':' | '.join(names[e]for e in reference.targets(r))or None}for r in reference.rows]
    decision_json=json.dumps(decisions,separators=(',',':'))
    if program:
        entity,where,params=reference.predicate({'program':program})
        sql="mimir_v7_actions AS (SELECT t.* EXCLUDE(platform_family),?::VARCHAR AS platform_family,t.platform_family AS recorded_source_primary_label FROM v_transactions t WHERE "+where+')'
        return sql,[entity['canonical_label'],*params]
    sql="mimir_v7_decisions AS (SELECT json_extract_string(value,'$.award_key') AS native_award_key,json_extract_string(value,'$.label') AS programme_label FROM json_each(?)), mimir_v7_actions AS (SELECT t.* EXCLUDE(platform_family),CASE WHEN d.native_award_key IS NOT NULL THEN d.programme_label ELSE t.platform_family END AS platform_family,t.platform_family AS recorded_source_primary_label FROM v_transactions t LEFT JOIN mimir_v7_decisions d ON t.award_key=d.native_award_key)"
    return sql,[decision_json]


def summary_relation():
    # The same site/year parent mapping as the platform's financial summary,
    # using a parent only when that site/year resolves unambiguously.
    return """mimir_v7_parents AS (
        SELECT UPPER(TRIM(cage_code)) AS cage,year,
               CASE WHEN COUNT(DISTINCT NULLIF(TRIM(clean_parent),''))=1 THEN MAX(clean_parent) END AS parent
        FROM v_summary s WHERE EXISTS(SELECT 1 FROM mimir_v7_actions a WHERE UPPER(TRIM(a.vendor_cage))=UPPER(TRIM(s.cage_code)) AND a.year=s.year) GROUP BY 1,2
    ), mimir_v7_summary AS (
        SELECT t.year,t.vendor_name,t.vendor_cage AS cage_code,COALESCE(p.parent,t.vendor_name) AS clean_parent,
               t.sub_agency,t.psc AS psc_code,CAST(NULL AS VARCHAR) AS psc_description,t.naics_code,
               t.platform_family,t.market_segment,SUM(t.spend_amount) AS total_spend,
               COUNT(DISTINCT """+effective_award_key('t')+""") AS contract_count
        FROM mimir_v7_actions t LEFT JOIN mimir_v7_parents p ON UPPER(TRIM(t.vendor_cage))=p.cage AND t.year=p.year
        GROUP BY 1,2,3,4,5,6,7,8,9,10
    )"""


def wrap_action_read(reference,read,program=None,summary=False):
    ctes,extra=action_relation(reference,program)
    if summary:ctes+=', '+summary_relation()
    def scoped(sql,params):
        # SQL comes only from fixed server builders; caller text is parameterised.
        text=re.sub(r'\bv_transactions\b','mimir_v7_actions',sql)
        if summary:text=re.sub(r'\bv_summary\b','mimir_v7_summary',text)
        return read(prepend(text,ctes),[*extra,*params])
    return scoped


def aggregate(payload,reference,read):
    from private_research_query import run_aggregate
    program=payload.get('filters',{}).get('program')
    meta=metadata(reference,program)
    scope=bool(program or 'program'in payload.get('group_by',[]))
    normalized={**payload,'filters':dict(payload.get('filters',{}))}
    if program:normalized['filters']['program']=meta['canonical_programme_name']
    reader=wrap_action_read(reference,read,program,summary=payload.get('dataset','funding_summary')=='funding_summary')if scope else read
    result=run_aggregate(normalized,reader)
    if program:result['filters']['program']=program.strip().upper()
    if scope:
        result.update(meta);result['source_grain']='AVAILABLE_CONTRACT_ACTIONS';result['company_grouping']='unambiguous_recorded_site_year_parent_else_recorded_vendor'if payload.get('dataset','funding_summary')=='funding_summary'else'recorded_vendor'
        result['measure']='v7_programme_associated_net_contract_obligations'
        result['programme_groups_additive']=not bool(program)
        result['shared_support_grouping']='This programme-associated total can overlap other programme totals; shared support is not allocated exclusively.'if program else'Shared programmes retain one combined label; amounts are counted once.'
    return result


def reference_context(reference,kind,identifier):
    if kind=='item':
        digits=re.sub('[^0-9]','',str(identifier))
        if len(digits)not in{9,13}:raise ValueError('Use a valid stock item identifier.')
        items=[r for r in reference.items if r['niin']==digits[-9:]]
    elif kind=='item_batch':
        if not isinstance(identifier,list)or not 1<=len(identifier)<=11 or any(not re.fullmatch('[0-9]{9}',str(v))for v in identifier):raise ValueError('Use up to eleven exact NIINs.')
        items=[r for r in reference.items if r['niin']in identifier]
    elif kind=='award':
        if not re.fullmatch('[A-Za-z0-9_-]{2,128}',str(identifier)):raise ValueError('Use a contract identifier.')
        items=[r for r in reference.items if r['contract_id'].upper()==str(identifier).upper()]
    else:raise ValueError('Choose item or award context.')
    labels={r['canonical_entity_id']:r['canonical_label']for r in reference.registry}
    decisions=[]
    if kind=='award':
        selected={k for k,v in reference.by_key.items()if str(v.get('contract_id','')).upper()==str(identifier).upper()}
        decisions=[{'award_key':r['award_key'],'canonical_entity_ids':reference.targets(r),'canonical_programme_names':[labels[e]for e in reference.targets(r)],'association_scope':r.get('association_scope'),'financial_allocation_eligible':False}for r in reference.rows if r['award_key']in selected]
    return {**metadata(reference),'kind':kind,'identifier':identifier,'reviewed_item_links':[{**r,'canonical_programme_name':labels[r['canonical_entity_id']]}for r in items],'reviewed_award_decisions':decisions}


def wrap_item_read(reference,read,program,columns):
    entity=reference.resolve(program)
    if entity is None:raise ValueError('Select a unique registered v7 programme.')
    niins=sorted({r['niin']for r in reference.items if r['canonical_entity_id']==entity['canonical_entity_id']})
    ctes="mimir_v7_item_scope AS (SELECT DISTINCT LPAD(TRIM(CAST(niin AS VARCHAR)),9,'0') AS niin FROM v_platform_bom WHERE UPPER(TRIM(platform_family))=? UNION SELECT json_extract_string(value,'$') AS niin FROM json_each(?))"
    params=[entity['canonical_label'].upper(),json.dumps(niins)]
    for source,target in [('v_nsn_cage_reference','mimir_v7_item_reference'),('v_nsn_cage_reference_explorer','mimir_v7_item_explorer')]:
        excluded=' EXCLUDE(platform_families)'if 'platform_families'in columns else''
        ctes+=", "+target+" AS (SELECT r.*"+excluded+",?::VARCHAR AS platform_families FROM "+source+" r WHERE LPAD(TRIM(CAST(r.niin AS VARCHAR)),9,'0') IN(SELECT niin FROM mimir_v7_item_scope))"
        params.append(entity['canonical_label'])
    def scoped(sql,values):
        text=re.sub(r'\bv_nsn_cage_reference_explorer\b','mimir_v7_item_explorer',sql)
        text=re.sub(r'\bv_nsn_cage_reference\b','mimir_v7_item_reference',text)
        return read(prepend(text,ctes),[*params,*values])
    return scoped


def search_programmes(reference,query,limit=25):
    if not isinstance(query,str)or not 2<=len(query.strip())<=128:raise ValueError('Use a programme search of 2 to 128 characters.')
    terms=re.findall('[A-Z0-9]+',query.upper());matches=[]
    for row in reference.registry:
        eid=row['canonical_entity_id'];label=row['canonical_label']
        aliases=[a for a in reference.aliases if(a.get('effective_canonical_entity_id')or a.get('canonical_entity_id'))==eid]
        texts=[label,eid,*[a['source_label']for a in aliases]]
        if any(all(t in str(s).upper()for t in terms)for s in texts):
            dispositions=sorted({a.get('disposition')for a in aliases if a.get('disposition')})
            matches.append({'entity_type':'platform','entity_id':eid,'display_name':label,'program_name':label,
                'discovery_source':'current_v7_canonical_registry','mapping_dispositions':dispositions,
                'financial_promotion_held':any('SPLIT_REQUIRED'in d or 'NO_FINANCIAL_PROMOTION'in d for d in dispositions)})
    matches.sort(key=lambda r:(r['display_name'].upper()!=query.strip().upper(),len(r['display_name']),r['entity_id']))
    return {**metadata(reference),'query':query,'entries':matches[:limit],'matching_identity_count':len(matches)}


def item_history(payload,read):
    """Use the same native item financial universe as programme parts."""
    from programme_parts import build_parts_query,buyer_name
    digits=re.sub('[^0-9]','',str(payload.get('nsn','')))
    if len(digits)not in{9,13}:raise ValueError('Use a stock number or nine-digit NIIN.')
    if set(payload)-{'nsn','years','agency','psc','limit','offset'}:raise ValueError('Use documented item history filters.')
    niin=digits[-9:];years=payload.get('years')or[];agency=buyer_name(payload.get('agency'))
    limit=payload.get('limit',51);offset=payload.get('offset',0)
    if type(limit)is not int or not 1<=limit<=501 or type(offset)is not int or not 0<=offset<=100000:raise ValueError('Choose a valid item page.')
    # A source identity is a lookup key, not a programme or price assertion.
    sql,params=build_parts_query('__EXACT_ITEM_HISTORY__',years=years,agency=agency,reviewed_items=[{'niin':niin,'nsn':digits if len(digits)==13 else None,'evidence_count':1}],limit=1)
    if payload.get('psc'):
        psc=str(payload['psc']).strip().upper()
        if not re.fullmatch('[A-Z0-9]{4}',psc):raise ValueError('Use a four-character buying category.')
        # The historical DLA rollup lacks the recorded action PSC; do not
        # silently treat an item's current stock class as its historical PSC.
        sql=sql.replace('FROM v_transactions t','FROM mimir_item_psc_actions t')
        sql=sql.replace('FROM v_nsn_supplier_lookup s','FROM (SELECT * FROM v_nsn_supplier_lookup WHERE FALSE) s')
        sql=prepend(sql,'mimir_item_psc_actions AS (SELECT * FROM v_transactions WHERE UPPER(TRIM(psc))=?)')
        params=[psc,*params]
    rows=read(sql,params);item=rows[0]if rows else{}
    annual=[]
    for pair in(item.get('annual_revenue_trend')or'').split('|'):
        if pair:
            year,value=pair.split(':',1);annual.append({'label':year,'spend':round(float(value),2)})
    return {'annual':annual,'niin':niin,'buyer_history_start_fy':item.get('buyer_history_start_fy'),'funding_basis':'RECORDED_ITEM_ACTIONS_WITH_DLA_HISTORY_BEFORE_ACTION_CACHE','recorded_quantity':item.get('total_units_sold')}
