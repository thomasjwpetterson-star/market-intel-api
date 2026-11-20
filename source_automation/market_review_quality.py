"""Native-action discovery and reviewed-coverage closure, independent of net totals.

Callers supply a pinned eligible population and reviewed controls. Description
matches create candidates only; they never supply an inclusion decision.
"""
from decimal import Decimal
import re

TEXT_FIELDS=('action_description','base_award_description','description','parent_description')


def native_key(row):
    for field in ('transaction_key','native_action_key','native_record_id'):
        if row.get(field):return str(row[field])
    raise ValueError('Missing agency-qualified native transaction key')


def cents(value):
    amount=Decimal(str(value))
    if not amount.is_finite()or amount!=amount.quantize(Decimal('.01')):
        raise ValueError('Action obligations must be exact signed cents')
    return amount


def discover_candidates(records,aliases,years):
    """Search every description; preserve unmapped, zero and negative actions."""
    needles={re.sub(r'[^a-z0-9]','',a.casefold()):a for a in aliases}
    if not needles or ''in needles:raise ValueError('Empty discovery alias')
    seen=set();out=[]
    for row in records:
        key=native_key(row)
        if key in seen:raise ValueError('Repeated native key: '+key)
        seen.add(key)
        year=int(row.get('fiscal_year',row.get('year')))
        if year not in years:continue
        matches=[]
        for field in TEXT_FIELDS:
            text=re.sub(r'[^a-z0-9]','',str(row.get(field)or'').casefold())
            matches += [{'field':field,'alias':alias}for needle,alias in needles.items()if needle in text]
        if matches:out.append({'record':row,'matches':matches,'review_status':'PENDING_REVIEW'})
    return out


def apply_reviewed_controls(source,scope,controls,recovered):
    """Apply exact reviewed controls to a local candidate; never infer approvals.

    Every absent record needs an independently pinned recovered source record.
    Existing approved decisions outside the supplied scope remain untouched.
    Held controls cannot erase an existing financial approval.
    """
    def index(records):
        result={}
        for row in records:
            key=native_key(row)
            if key in result:raise ValueError('Repeated native key: '+key)
            result[key]=dict(row)
        return result
    sources=index(source);scopes=index(scope);recovery=index(recovered);checked=set()
    for c in controls:
        key=native_key(c)
        if key in checked:raise ValueError('Repeated reviewed control: '+key)
        checked.add(key)
        if c['decision']not in ('INCLUDE','HOLD'):raise ValueError('Unsupported review decision')
        if key not in sources:
            if key not in recovery:raise ValueError('Missing independently recovered source: '+key)
            sources[key]=recovery[key]
        row=sources[key]
        expected=cents(c['source_amount_usd'])
        if cents(row['source_amount_usd'])!=expected or int(row['fiscal_year'])!=int(c['fiscal_year']):
            raise ValueError('Source cents/year mismatch: '+key)
        old=scopes.get(key)
        if c['decision']=='HOLD'and old and (old.get('approved_market_fact')or cents(old['reviewed_market_amount_usd'])!=0):
            raise ValueError('Hold would erase an existing approval: '+key)
        amount=expected if c['decision']=='INCLUDE'else Decimal('0.00')
        scopes[key]={**(old or {}),'market_id':row['market_id'],'main_release_id':row['main_release_id'],
            'transaction_key':key,'award_key':row['award_key'],'fiscal_year':int(c['fiscal_year']),
            'source_amount_usd':str(expected),'reviewed_market_amount_usd':str(amount),
            'scope_unallocated_amount_usd':str(expected-amount),'scope_basis':c['decision_basis'],
            'approved_market_fact':c['decision']=='INCLUDE'}
    if set(sources)!=set(scopes):raise ValueError('Source/scope native-key coverage differs')
    validate_review_closure(list(sources.values()),list(scopes.values()),controls)
    return list(sources.values()),list(scopes.values())


def validate_review_closure(source,scope,controls):
    """Per-native coverage catches omissions even when aggregate errors offset."""
    sm={native_key(r):r for r in source};qm={native_key(r):r for r in scope}
    if len(sm)!=len(source)or len(qm)!=len(scope):raise ValueError('Native-key fan-out')
    for c in controls:
        key=native_key(c)
        if key not in sm or key not in qm:raise ValueError('Reviewed record absent: '+key)
        s=sm[key];q=qm[key];expected=cents(c['source_amount_usd'])
        if cents(s['source_amount_usd'])!=expected or int(s['fiscal_year'])!=int(c['fiscal_year']):raise ValueError('Source drift: '+key)
        accepted=c['decision']=='INCLUDE'
        if bool(q['approved_market_fact'])!=accepted:raise ValueError('Review decision drift: '+key)
        if cents(q['reviewed_market_amount_usd'])!=(expected if accepted else 0):raise ValueError('Reviewed cents drift: '+key)
        if cents(q['reviewed_market_amount_usd'])+cents(q['scope_unallocated_amount_usd'])!=expected:raise ValueError('Signed allocation drift: '+key)
    return {'controls':len(controls),'status':'PASS'}
