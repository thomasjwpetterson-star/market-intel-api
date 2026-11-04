"""Private, pinned reads of the existing v7 mapping authority.

This reader is called only after research-proxy authentication. Canonical IDs,
reviewed holds and award keys come from the selected shared v7 bundle.
"""
import hashlib
import io
import json
import re
from pathlib import Path
import threading

from programme_parts import buyer_name

OWNER='868631722720'
BUCKET='a-and-d-intel-lake-newaccount'
PREFIX='governance/platform_reference/candidates/'
_lock=threading.Lock()
_cached=None


def read_pin(client,pin,limit=8*1024*1024):
    if pin.get('bucket')!=BUCKET or not str(pin.get('key','')).startswith(PREFIX) or not pin.get('s3_version_id'):
        raise ValueError('A version-pinned private v7 reference is required.')
    obj=client.get_object(Bucket=BUCKET,Key=pin['key'],VersionId=pin['s3_version_id'],ExpectedBucketOwner=OWNER)
    try: raw=obj['Body'].read(limit+1)
    finally: obj['Body'].close()
    if len(raw)>limit or obj.get('VersionId')!=pin['s3_version_id'] or hashlib.sha256(raw).hexdigest()!=pin['sha256']:
        raise ValueError('The private v7 reference version or hash changed.')
    return raw


class Reference:
    def __init__(self,client,binding):
        import pyarrow as pa
        import pyarrow.parquet as pq
        self.binding=binding
        self.manifest=json.loads(read_pin(client,binding['manifest']))
        m=self.manifest
        if (m.get('schema_version')!='platform-award-remap-cloud-candidate-v1'
            or m.get('accepted_main_release_id')!=binding['accepted_main_release_id']
            or m.get('base_platform_reference_release_id')!=binding['base_platform_reference_release_id']
            or m.get('customer_publication_eligible')is not False):
            raise ValueError('Private mapping authority does not match the selected v7 source.')
        def table(pin):return pq.read_table(pa.BufferReader(read_pin(client,pin))).to_pylist()
        self.rows=table(m['tables']['award_mapping_authority'])
        self.registry=table(m['tables']['canonical_registry'])
        self.metadata=table(m['files']['data/award_resolution_decisions.parquet'])
        self.items=table(m['tables']['award_item_associations'])if m['tables'].get('award_item_associations')else[]
        alias_pin=m['files'].get('data/canonical_label_mappings_reviewed_overlay.parquet')
        self.aliases=table(alias_pin)if alias_pin else[]
        if len({r['award_key']for r in self.rows})!=len(self.rows):raise ValueError('Duplicate v7 award identity.')
        self.by_key={r['award_key']:r for r in self.metadata}
        self.by_identity={r['canonical_entity_id']:r for r in self.registry}
        for r in self.rows:
            if r.get('mapping_decision_active')is not True:raise ValueError('Inactive private mapping authority.')
            if any(v not in self.by_identity for v in self.targets(r)):raise ValueError('Mapping target is outside the v7 registry.')

    @staticmethod
    def targets(row):return [v.strip()for v in (row.get('target_entity_ids')or row.get('canonical_entity_id')or'').split('|')if v.strip()]

    def resolve(self,program):
        program=program.strip().upper()
        exact=[r for r in self.registry if program in {str(r['canonical_entity_id']).upper(),str(r['canonical_label']).upper()}]
        if len(exact)==1:return exact[0]
        normal=' '.join(re.findall('[A-Z0-9]+',program))
        ids=set()
        for row in self.aliases:
            if any(t in str(row.get('disposition','')).upper()for t in ['AMBIGUOUS','HOLD','REJECT']):continue
            if program==str(row.get('source_label','')).upper()or normal==row.get('normalized_source_label'):
                eid=row.get('effective_canonical_entity_id')or row.get('canonical_entity_id')
                if eid in self.by_identity:ids.add(eid)
        return self.by_identity[next(iter(ids))]if len(ids)==1 else None

    def predicate(self,payload):
        entity=self.resolve(payload['program'])
        if entity is None:raise ValueError('No unique registered v7 programme identity matches this name.')
        positive=[r['award_key']for r in self.rows if entity['canonical_entity_id']in self.targets(r)]
        all_keys=[r['award_key']for r in self.rows]
        sql="(award_key IN (SELECT json_extract_string(value,'$') FROM json_each(?)) OR (COALESCE(award_key,'') NOT IN (SELECT json_extract_string(value,'$') FROM json_each(?)) AND UPPER(TRIM(platform_family))=?))"
        params=[json.dumps(sorted(positive)),json.dumps(sorted(all_keys)),entity['canonical_label'].upper()]
        years=sorted(set(payload.get('years')or[]));agency=buyer_name(payload.get('agency'))
        if payload.get('domain'):sql+=' AND UPPER(TRIM(market_segment))=?';params.append(payload['domain'].strip().upper())
        if years:sql+=' AND year IN ('+','.join('?'for _ in years)+')';params.extend(years)
        if agency:sql+=' AND (UPPER(TRIM(sub_agency))=? OR UPPER(TRIM(parent_agency))=?)';params.extend([agency,agency])
        return entity,sql,params

    def coverage(self,payload,read,columns):
        program=payload['program'].strip().upper()
        entity=self.resolve(program)
        if entity is None:
            return {'status':'NO_EXACT_CANONICAL_IDENTITY','base_reference_release_id':self.binding['base_platform_reference_release_id'],
                    'mapping_release_id':self.manifest['release_id'],'alias_inference_applied':False,'reviewed_awards':[],'reviewed_items':[]}
        eid=entity['canonical_entity_id']
        selected={r['award_key']:r for r in self.rows if eid in self.targets(r)}
        reviewed_by_key={r['award_key']:r for r in self.rows}
        item_keys={r['award_key']for r in self.items if r['canonical_entity_id']==eid and (r['award_key']not in reviewed_by_key or eid in self.targets(reviewed_by_key[r['award_key']]))}
        evidence_keys=set(selected)|item_keys
        if evidence_keys and 'award_key'not in columns:raise ValueError('The action source lacks agency-qualified award keys.')
        clauses=['award_key IN (SELECT json_extract_string(value,\'$\') FROM json_each(?))']
        params=[json.dumps(sorted(evidence_keys))]
        years=sorted(set(payload.get('years')or[]));agency=buyer_name(payload.get('agency'))
        if payload.get('domain'):clauses.append('UPPER(TRIM(market_segment))=?');params.append(payload['domain'].strip().upper())
        if years:clauses.append('year IN ('+','.join('?'for _ in years)+')');params.extend(years)
        if agency:clauses.append('(UPPER(TRIM(sub_agency))=? OR UPPER(TRIM(parent_agency))=?)');params.extend([agency,agency])
        usable="COALESCE(NULLIF(TRIM(CAST(niin AS VARCHAR)),''),NULLIF(TRIM(CAST(nsn AS VARCHAR)),''))"
        amounts=read('SELECT award_key, COUNT(*) AS action_count, COUNT('+usable+') AS actions_with_native_item_number, '
            'COALESCE(SUM(spend_amount),0) AS source_net_obligations_usd FROM v_transactions WHERE '+' AND '.join(clauses)+' GROUP BY award_key',params)if evidence_keys else[]
        scoped={r['award_key']:r for r in amounts};awards=[]
        for key,value in sorted(scoped.items()):
            if key not in selected:continue
            row=selected[key];meta=self.by_key.get(key,{})
            awards.append({'award_key':key,'contract_id':meta.get('contract_id')or key.split('_')[2],
                'canonical_entity_ids':self.targets(row),'association_scope':row['association_scope'],
                'action_count':int(value['action_count']),'actions_with_native_item_number':int(value['actions_with_native_item_number']),
                'source_net_obligations_usd':round(float(value['source_net_obligations_usd']),2),
                'financial_allocation_eligible':bool(row.get('financial_allocation_eligible')),
                'pricing_evidence_eligible':bool(row.get('pricing_evidence_eligible'))})
        items=[]
        for item in self.items:
            if item['award_key']in scoped and item['canonical_entity_id']==eid:
                items.append({**item,'evidence_count':int(scoped[item['award_key']]['action_count'])})
        effective=None
        if {'award_key','contract_id','platform_family','vendor_cage'}.issubset(columns):
            _,where,query_params=self.predicate(payload)
            summary=read("SELECT COUNT(*) AS action_count, COUNT(DISTINCT COALESCE(NULLIF(award_key,''),NULLIF(contract_id,''))) AS contract_count, COALESCE(SUM(spend_amount),0) AS source_net_obligations_usd, COUNT(DISTINCT NULLIF(vendor_cage,'')) AS supplier_site_count, COUNT("+usable+") AS actions_with_native_item_number FROM v_transactions WHERE "+where,query_params)
            if len(summary)!=1:raise ValueError('Current programme association count is unavailable.')
            s=summary[0];effective={'action_count':int(s['action_count']),'contract_count':int(s['contract_count']),
                'source_net_obligations_usd':round(float(s['source_net_obligations_usd']),2),'supplier_site_count':int(s['supplier_site_count']),
                'actions_with_native_item_number':int(s['actions_with_native_item_number']),
                'basis':'Accepted v7 primary source associations plus current reviewed award decisions; reviewed clears and holds override old labels.',
                'financial_allocation_eligible':False,'funding_interpretation':'Funding recorded on associated actions. Shared-support amounts are not exclusive programme funding.'}
        return {'status':'VERIFIED_CURRENT_V7_AUTHORITY','base_reference_release_id':self.binding['base_platform_reference_release_id'],
            'mapping_release_id':self.manifest['release_id'],'authority_manifest':self.binding['manifest'],
            'canonical_entity_id':eid,'canonical_label':entity['canonical_label'],'reviewed_award_count':len(awards),
            'reviewed_action_count':sum(r['action_count']for r in awards),'reviewed_awards':awards[:50],
            'reviewed_awards_truncated':len(awards)>50,'reviewed_items':items,
            'reviewed_decision_scope':'Reviewed overrides; not a complete list of all accepted-Main programme awards.',
            'effective_programme_scope':effective,
            'interpretation':'Current v7 reviewed associations are authoritative. Primary-label counts elsewhere describe the accepted Main snapshot. Associated award funding is not allocated to a programme or item unless separately eligible.',
            'accepted_main_cache_rebuilt':False,'public_publication_eligible':False}


def reference_for(main_release_id):
    global _cached
    path=Path(__file__).with_name('private_v7_binding.json')
    if not path.exists():return None
    binding=json.loads(path.read_text())
    if main_release_id!=binding['accepted_main_release_id']:
        raise RuntimeError('The private v7 mapping is bound to a different Main source release.')
    with _lock:
        if _cached is None or _cached.binding!=binding:
            import boto3
            try:_cached=Reference(boto3.client('s3',region_name='us-east-1'),binding)
            except Exception as error:raise RuntimeError('The selected private v7 authority could not be verified.')from error
        reference=_cached
    return reference


def programme_context(payload,read,columns,main_release_id):
    reference=reference_for(main_release_id)
    return reference.coverage(payload,read,columns)if reference else None


def programme_records(payload,read,columns,main_release_id):
    reference=reference_for(main_release_id)
    if reference is None:raise RuntimeError('Private v7 authority is not configured.')
    entity,where,params=reference.predicate(payload)
    limit,offset=payload.get('limit',21),payload.get('offset',0)
    if type(limit)is not int or not 1<=limit<=501 or type(offset)is not int or not 0<=offset<=100000:
        raise ValueError('Choose a valid programme page.')
    section=payload.get('section','contractors')
    if section=='contractors':
        # Resolve spelling/history variants at the exact supplier site before
        # ranking and pagination. Missing/placeholder CAGEs remain name-specific.
        site="CASE WHEN REGEXP_FULL_MATCH(UPPER(TRIM(COALESCE(vendor_cage,''))),'[A-Z0-9]{5}') AND UPPER(TRIM(vendor_cage)) NOT IN ('UNKNO','00000') THEN UPPER(TRIM(vendor_cage)) END"
        sql="WITH scoped AS (SELECT *,"+site+" AS site_cage FROM v_transactions WHERE "+where+") SELECT MAX_BY(vendor_name,STRUCT_PACK(date:=COALESCE(CAST(action_date AS VARCHAR),''),name:=COALESCE(vendor_name,''))) AS name,site_cage AS cage,COALESCE(SUM(spend_amount),0) AS total,'PRIME' AS role FROM scoped GROUP BY site_cage,CASE WHEN site_cage IS NULL THEN UPPER(TRIM(vendor_name)) END ORDER BY total DESC,name,cage LIMIT ? OFFSET ?"
    elif section=='awards':
        order=",transaction_key"if'transaction_key'in columns else''
        sql="SELECT contract_id,action_date,vendor_name,vendor_cage,sub_agency AS agency,description,spend_amount AS spend,naics_code AS naics,? AS platform_family,platform_family AS recorded_source_primary_label FROM v_transactions WHERE "+where+" ORDER BY spend DESC,action_date,contract_id"+order+" LIMIT ? OFFSET ?"
        params=[entity['canonical_label']]+params
    else:raise ValueError('Choose programme contractors or award actions.')
    return read(sql,params+[limit,offset])


def summary_coverage(payload,current):
    """Routine reads use exact v7 associations; description diagnostics stay explicit."""
    from programme_context import context_for
    entity=current['effective_programme_scope']
    actions=entity['action_count'];native=entity['actions_with_native_item_number']
    reason=('programme_actions_have_no_item_numbers' if actions and not native else
            'programme_item_identifiers_available' if native else 'no_current_v7_programme_actions_for_recorded_agency')
    return {'schema_version':'1.0','filters':{'program':payload['program'].strip().upper(),
        **({'years':sorted(set(payload['years']))}if payload.get('years')else{}),
        **({'agency':buyer_name(payload['agency'])}if payload.get('agency')else{})},
        'reason':reason,'selected_scope':{'programme_action_count':actions,
            'programme_actions_with_item_number':native,'programme_net_obligations_usd':entity['source_net_obligations_usd']},
        'count_basis':'CURRENT_V7_ASSOCIATED_ACTIONS','diagnostic_mode':'summary',
        'description_candidates_evaluated':False,
        'description_candidate_interpretation':'Description-only candidates are evaluated by the detailed coverage section.',
        'agency_interpretation':'Recorded contracting agency, not a complete funding-service or operating-service breakdown.',
        'programme_context':context_for(payload['program'].strip().upper())}
