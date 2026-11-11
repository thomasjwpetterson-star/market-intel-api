"""Release-bound company point reads. No personal state or warehouse queries.

The producer stores every pre-family eligible edge, not merely the first page.
Deadlines and incumbent-plan precedence are evaluated again at request time.
"""
import copy
from collections import OrderedDict
from concurrent.futures import Future
import datetime as dt
import gzip
import hashlib
import json
import math
import re
import threading
import time
from pathlib import Path
from .aws import BUCKET, PREFIX
from .overview import summary
from opportunity_worker.opportunity_engine.financial_readiness import guard, POLICY_HASH

VERSION = 'company-point-read-v1'
MAX_COMPRESSED = 32 * 1024 * 1024
MAX_EXPANDED = 192 * 1024 * 1024

class IndexUnavailable(RuntimeError):
    pass


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False, default=str).encode()


def binding(manifest):
    return hashlib.sha256(encode(dict(release=manifest['run_id'], as_of=manifest['as_of'],
        tables={k:{f:v.get(f) for f in ('sha256','rows','table','columns')} for k,v in manifest['tables'].items()},
        finance=manifest.get('subcontract_opportunity_finance'), finance_policy=POLICY_HASH))).hexdigest()


def contract(repo):
    sql=repo.opportunities('CAGE:00000',_query=True,_source_groups=False,_materialize=True)
    plans=repo.opportunities('CAGE:00000',_query=True,_source_groups=False,_materialize=True,_plan_links=True) if 'agency_forecast_incumbents' in repo.manifest()['tables'] else ''
    # Bind eligibility SQL, layout and monetary policy. Operational producer
    # changes (parallelism, spill layout) do not change the stored fact contract.
    return hashlib.sha256(VERSION.encode()+sql.encode()+plans.encode()+POLICY_HASH.encode()+Path(__file__).with_name('overview.py').read_bytes()).hexdigest()


def truth(value):
    return str(value).lower()=='true'


def number(value):
    try:
        n=float(value)
        return n if math.isfinite(n) else None
    except (TypeError,ValueError):return None


def inputs(row):
    try:return json.loads(row.get('value_inputs_json') or '{}')
    except (ValueError,TypeError):return {}


def open_at(row,now):
    if row.get('stage') not in ('published_requirement','qualification_opening','teaming_opening'):return True
    end=str(row.get('window_end') or '')[:10]
    try:
        if dt.date.fromisoformat(end)<now.date():return False
    except ValueError:pass
    info=inputs(row)
    if info.get('source_response_deadline_precision')=='timestamp_utc':
        try:
            deadline=dt.datetime.fromisoformat(info['source_response_deadline_utc'].replace('Z','+00:00'))
            if deadline.tzinfo and deadline<=now:return False
        except (ValueError,KeyError,TypeError):pass
    return True


def family(row):
    native=str(row.get('native_identifier') or '')
    if row.get('source_name')=='sam_notices' and len(native)>4:
        return ('sam_notices',str(row.get('customer_name') or '').upper(),re.sub('-00$','',native.upper()),re.sub('[^A-Z0-9]','',str(row.get('title') or '').upper()))
    return ('episode',row['episode_id'])


def eligible(payload,kind='all',now=None):
    now=now or dt.datetime.now(dt.timezone.utc)
    active_plans=set()
    for plan in payload.get('plans',[]):
        # The authoritative predecessor query requires a parseable end date.
        try:
            if dt.date.fromisoformat(str(plan.get('window_end') or '')[:10])>=now.date():active_plans.add(plan['award_key'])
        except ValueError:pass
    groups={}
    for original in payload['records']:
        if kind=='items' and not truth(original.get('item_demand')):continue
        if kind=='work' and truth(original.get('item_demand')):continue
        if not open_at(original,now):continue
        if original.get('lane')=='funded_contract_follow_on' and inputs(original).get('record_id') in active_plans:continue
        groups.setdefault(family(original),[]).append(original)
    records=[]
    for members in groups.values():
        # NULLS LAST, posted DESC then episode ASC; retain all source links.
        latest=max((str(r.get('notice_posted_date') or '') for r in members),default='')
        winner=min((r for r in members if str(r.get('notice_posted_date') or '')==latest),key=lambda r:r['episode_id'])
        row=copy.deepcopy(winner);row['source_family_rank']='1'
        row['related_source_records_json']=json.dumps([dict(episode_id=r['episode_id'],native_identifier=r.get('native_identifier'),source_url=r.get('source_url'),source_evidence_id=r.get('source_evidence_id'),posted_date=r.get('notice_posted_date')) for r in sorted(members,key=lambda r:r['episode_id'])])
        records.append(row)
    return records


def rank(row,sort='relevance',discovery=False):
    amount=number(row.get('commercial_value'));deadline=row.get('window_end') or '\uffff'
    if sort=='value':
        amount=amount if amount is not None and amount>0 else None
        return (amount is None,-(amount or 0),deadline,row['episode_id'])
    if sort=='deadline':return (deadline,row['episode_id'])
    stage={'published_requirement':0,'qualification_opening':1,'teaming_opening':1}.get(row.get('stage'),2)
    priority=number(row.get('priority_score'))
    base=(stage,0 if (amount or 0)>=1e6 else 1,priority is None,-(priority or 0))
    if discovery:return base+(deadline,row['episode_id'])
    old_stage={'qualification_opening':0,'teaming_opening':0,'published_requirement':1,'emerging_demand':2}.get(row.get('stage'),3)
    return base+(old_stage,row.get('lane')=='source_item_forecasts',deadline,row['episode_id'])


def overview(payload,manifest,now=None):
    rows=[guard(r,manifest) for r in eligible(payload,now=now)]
    counts={}
    for kind in ('work','items'):
        chosen=[r for r in rows if truth(r.get('item_demand'))==(kind=='items')]
        counts[kind+'_count']=len(chosen)
        counts[kind+'_larger_count']=sum((number(r.get('commercial_value')) or 0)>=1e6 for r in chosen)
    published=[r for r in rows if r.get('stage') in ('published_requirement','qualification_opening','teaming_opening')]
    counts.update(observed_prime_net_obligations=payload.get('observed_prime_net_obligations'),published_records_count=len(published),
        published_priced_records_count=sum(r.get('commercial_value_kind') in ('item_reference_estimate','historical_unit_price_estimate') and (number(r.get('commercial_value')) or 0)>0 for r in published),
        published_historical_guide_records_count=sum(r.get('commercial_value_kind') in ('historical_comparison','historical_context_source_limit') for r in published),
        published_first_deadline=min((r['window_end'] for r in published if r.get('window_end')),default=None),published_last_deadline=max((r['window_end'] for r in published if r.get('window_end')),default=None))
    kinds=('item_reference_estimate','historical_unit_price_estimate','forecast_scenario','annual_usage_scenario','planned_package_reference','recurring_demand_scenario','historical_package_benchmark')
    priced=[r for r in rows if r.get('commercial_value_kind') in kinds and ((number(r.get('commercial_value')) or 0)>0 or r.get('commercial_value_kind')=='planned_package_reference')]
    columns={name:[c['Name'] for c in manifest.get('tables',{}).get(name,{}).get('columns',[]) if c['Name'] not in ('company_id','company_name')] for name in ('company_activity_baselines','company_catalogue_sales','company_reporting_lag_baselines','company_product_market_coverage')}
    supplied=payload.get('business',{});present=payload.get('business_present_tables')
    if present is None:
        # Backwards-compatible source objects used distinct table-specific
        # fields as presence evidence; generic shared labels are insufficient.
        occurrences={col:sum(col in names for names in columns.values()) for names in columns.values() for col in names}
        present={name for name,names in columns.items() if any(col in supplied for col in names if occurrences[col]==1)}
    business={}
    for name,names in columns.items():business.update({col:supplied.get(col) if name in present else None for col in names})
    return dict(summary(counts,priced,manifest['run_id'],business),company_id=payload['company_id'],cage_code=payload['company_id'][5:])


class CompanyIndex:
    def __init__(self,repo):
        self.repo=repo;self._roots={};self._lock=threading.Lock();self._payloads=OrderedDict();self._pending={}
    def _get(self,key,maximum=MAX_COMPRESSED):
        response=self.repo.aws.s3.get_object(Bucket=BUCKET,Key=key)
        body=response['Body']
        try:
            if response.get('ContentLength',0)>maximum:raise IndexUnavailable('Company read object exceeds its limit')
            data=body.read(maximum+1)
            if len(data)>maximum:raise IndexUnavailable('Company read object exceeds its limit')
            return data
        finally:body.close()
    def load(self,company):
        if not re.fullmatch('CAGE:[A-Z0-9]{5}',company):return None
        m=self.repo.manifest()
        advertised=m.get('_company_index')
        if not advertised:return None
        release=m['run_id']
        if not re.fullmatch('[a-zA-Z0-9_]+',release):raise IndexUnavailable('Invalid source generation')
        key=PREFIX+'/releases/'+release+'/company-index/'+VERSION+'/ready.json'
        try:
            cache_key=(release,advertised.get('sha256'))
            with self._lock:cached=self._roots.get(cache_key)
            if cached and cached[0]>time.monotonic():root=cached[1]
            else:
                try:
                    raw=self._get(key,4*1024*1024)
                    if advertised.get('version')!=VERSION or hashlib.sha256(raw).hexdigest()!=advertised.get('sha256'):raise IndexUnavailable('Company index root content mismatch')
                    root=json.loads(raw)
                except Exception as error:
                    if getattr(error,'response',{}).get('Error',{}).get('Code') in ('NoSuchKey','404'):raise IndexUnavailable('Advertised company index is unavailable')
                    else:raise
                if root and (root.get('complete') is not True or root.get('binding')!=binding(m) or root.get('contract')!=contract(self.repo)):raise IndexUnavailable('Company index does not match this evidence release')
                with self._lock:self._roots={cache_key:(time.monotonic()+30,root)}
            if root is None:return None
            cage=company[5:];shard=root['shards'].get(cage[:2])
            empty=dict(company_id=company,release=release,binding=root['binding'],records=[],plans=[],business={})
            if not shard:return empty
            descriptor=self._checked(shard,root,compressed=shard.get('compression')=='gzip')
            entry=descriptor.get(cage)
            if not entry:return empty
            payload=entry.get('empty_payload') if 'empty_payload' in entry else self._payload(entry,root)
            if payload.get('company_id')!=company or payload.get('release')!=release or payload.get('binding')!=root['binding']:raise IndexUnavailable('Company index identity mismatch')
            return payload
        except IndexUnavailable:raise
        except Exception as error:raise IndexUnavailable('Company feed is temporarily unavailable') from error
    def _payload(self,entry,root):
        key=(root['binding'],entry['sha256'])
        with self._lock:
            cached=self._payloads.get(key)
            if cached and cached[0]>time.monotonic():return cached[1]
            future=self._pending.get(key);leader=future is None
            if leader:future=Future();self._pending[key]=future
        if not leader:return future.result(timeout=20)
        try:
            payload=self._checked(entry,root,compressed=True)
            weight=len(encode(payload))*3  # conservative bound for decoded object overhead
            with self._lock:
                if weight<=64*1024*1024:
                    self._payloads[key]=(time.monotonic()+30,payload,weight)
                    while sum(v[2] for v in self._payloads.values())>64*1024*1024:self._payloads.popitem(last=False)
            future.set_result(payload);return payload
        except BaseException as error:
            future.set_exception(error);raise
        finally:
            with self._lock:self._pending.pop(key,None)
    def _checked(self,entry,root,compressed=False):
        prefix=PREFIX+'/releases/'+root['release']+'/company-index/'+VERSION+'/'
        key=entry['key']
        if not key.startswith(prefix) or not re.fullmatch('[a-f0-9]{64}',entry['sha256']):raise IndexUnavailable('Invalid company index object')
        data=self._get(key)
        if len(data)!=entry['bytes'] or hashlib.sha256(data).hexdigest()!=entry['sha256']:raise IndexUnavailable('Company index content mismatch')
        if compressed:
            import io
            with gzip.GzipFile(fileobj=io.BytesIO(data)) as stream:data=stream.read(MAX_EXPANDED+1)
            if len(data)>MAX_EXPANDED:raise IndexUnavailable('Company read expansion exceeds its limit')
        return json.loads(data)
