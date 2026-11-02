"""Interest-only and company-plus-interest retrieval over existing release tables."""
import json
import math
import re
from .aws import DATABASE
from .discovery_tags import TagCatalogue, VERSION, parse_tags, fingerprint
from .financial_readiness import guard as guard_finance,status as finance_status,risk_sql as finance_risk_sql


class ReleaseChanged(RuntimeError):
    error_code = 'opportunity_release_changed'


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


from .market_updates import MarketUpdatesMixin
from .activity_feed import ActivityFeedMixin


class DiscoveryRepositoryMixin(MarketUpdatesMixin,ActivityFeedMixin):
    def _discovery_table(self, manifest, name):
        table = manifest['tables'][name]['table']
        if not re.fullmatch('oi_[a-z0-9_]+', table): raise ValueError('Invalid discovery table binding')
        return DATABASE + '.' + table

    def _discovery_catalogue(self, manifest):
        cached = getattr(self, '_cached_discovery_catalogue', None)
        if cached and cached[0] == manifest['run_id']: return cached[1]
        # The published policy hashes the complete scope rules and official PSC
        # reference. Reuse those exact bundled bytes when they match, avoiding
        # two warehouse reads merely to populate a small interest picker.
        from .work_vocabulary import SPEC,POLICY_HASH,codebook
        policy=manifest.get('work_vocabulary_policy',{})
        if policy.get('policy_hash')==POLICY_HASH:
            topics=[dict(topic_id=t['id'],label=t['label'],parent_id=t.get('parent'),facet=t.get('facet','navigation')) for t in SPEC['roots']+SPEC['topics']]
            references=codebook(manifest['as_of'])
            if len(topics)==manifest['tables']['work_vocabulary_topics'].get('rows') and len(references)==policy.get('psc_nodes'):
                catalogue=TagCatalogue(topics,references)
                self._cached_discovery_catalogue=(manifest['run_id'],catalogue)
                return catalogue
        table = manifest['tables']['work_vocabulary_topics']
        columns = {x['Name'] for x in table.get('columns', [])}
        facet = 'facet' if 'facet' in columns else 'cast(NULL AS varchar) facet'
        topics = self.rows('SELECT topic_id,label,parent_id,' + facet + ' FROM ' + self._discovery_table(manifest, 'work_vocabulary_topics'))
        classifications = self.rows('SELECT * FROM ' + self._discovery_table(manifest, 'work_vocabulary_reference')) if 'work_vocabulary_reference' in manifest['tables'] else []
        catalogue = TagCatalogue(topics, classifications)
        self._cached_discovery_catalogue = (manifest['run_id'], catalogue)
        return catalogue

    def work_tags(self, q='', facet='all', limit=100, offset=0):
        m = self.manifest()
        if not all(n in m['tables'] for n in ('work_vocabulary_topics', 'opportunity_vocabulary')):
            # Validate requests even when the current release predates vocabulary.
            result = TagCatalogue([]).search(q,facet,limit,offset)
            return dict(result, records=[], total=0, has_more=False, release=m['run_id'], availability='not_published', vocabulary_version=None)
        catalogue = self._discovery_catalogue(m)
        result = catalogue.search(q,facet,limit,offset)
        return dict(result, release=m['run_id'], availability='available', vocabulary_version=m.get('work_vocabulary_policy',{}).get('version'), tag_contract_version=VERSION, catalogue_hash=fingerprint(list(catalogue.records.values())))

    def discovery(self, mode='interests', tags='', match='any', company=None, kind='all', limit=24, offset=0, release=None, minimum_value=0.0, include_unpriced=True, sort='relevance'):
        if not math.isfinite(minimum_value) or not 0<=minimum_value<=1e12 or type(include_unpriced) is not bool or sort not in ('relevance','value','deadline'):
            raise ValueError('Invalid opportunity value filter or ordering')
        if mode not in ('interests','company','combined') or match not in ('any','all') or kind not in ('all','work','items'):
            raise ValueError('Invalid discovery mode, match rule or opportunity kind')
        if not 1 <= limit <= 100 or not 0 <= offset <= 1000000:
            raise ValueError('Invalid discovery pagination')
        if company and not re.fullmatch(r'(?:CAGE:[A-Z0-9]{5}|UEI:[A-Z0-9]{12})', company):
            raise ValueError('Invalid company identifier')
        if mode != 'interests' and not company: raise ValueError('Company selection required for this feed')
        if release and not re.fullmatch('[A-Za-z0-9._-]{1,120}', release): raise ValueError('Invalid evidence release')
        selected = parse_tags(tags)
        if mode == 'interests' and not selected: raise ValueError('Select at least one interest tag')
        if mode == 'company' and selected: raise ValueError('Use combined mode to add interests to company suggestions')
        m = self.manifest()
        if release and release != m['run_id']: raise ReleaseChanged('The evidence release changed. Reload the feed before continuing.')
        response = dict(records=[], release=m['run_id'], availability='not_published', mode=mode, match=match,
                        financial_readiness=finance_status(m),
                        selected_tags=[], has_more=False, next_offset=None, minimum_value=minimum_value, include_unpriced=include_unpriced, sort=sort,
                        interpretation='Company suggestions and chosen interests are distinct discovery routes. Neither establishes qualification or company revenue.')
        required = ['opportunities','demand_decisions','opportunity_vocabulary','work_vocabulary_topics']
        if mode != 'interests': required.append('company_matches')
        if not all(n in m['tables'] for n in required): return response
        if mode=='company':
            # The company query already carries vocabulary, pricing, source
            # grouping and eligibility. Avoid fetching the whole interest
            # catalogue and joining its annotations a second time.
            base=self.opportunities(company,kind=kind,_query=True)
            amount='q.commercial_value' if ' commercial_value,' in base else 'q.value_median'
            amount='CASE WHEN '+finance_risk_sql(m,'q')+' OR ('+amount+')<=0 THEN cast(NULL AS double) ELSE '+amount+' END'
            condition='true' if include_unpriced else '('+amount+') IS NOT NULL'
            if minimum_value>0:condition+=' AND (('+amount+')>='+str(float(minimum_value))+(' OR ('+amount+') IS NULL' if include_unpriced else '')+')'
            ordering=('('+amount+') DESC NULLS LAST,' if sort=='value' else 'q.window_end ASC NULLS LAST,' if sort=='deadline' else 'CASE WHEN ('+amount+')>=1000000 THEN 0 ELSE 1 END,q.priority_score DESC NULLS LAST,')
            query='SELECT q.* FROM ('+base+') q WHERE '+condition+' ORDER BY '+ordering+'q.window_end ASC NULLS LAST,q.episode_id OFFSET '+str(offset)+' LIMIT '+str(limit+1)
            if self.manifest()['run_id']!=m['run_id']:raise ReleaseChanged('The evidence release changed. Reload the feed before continuing.')
            rows=self.rows(query)
            response.update(availability='available',has_more=len(rows)>limit,next_offset=offset+limit if len(rows)>limit else None)
            response['records']=[guard_finance(dict(row,matched_interest_tags=[],discovery_routes=['company_match'],company_fit_established=True,fit_assessment='existing_engine_candidate',qualification='not_established'),m) for row in rows[:limit]]
            return response
        catalogue = self._discovery_catalogue(m)
        response['selected_tags'] = catalogue.validate(selected)
        cols = {x['Name'] for x in m['tables']['opportunity_vocabulary'].get('columns',[])}
        def field(name, path):
            return 'ov.'+name if name in cols else "json_format(json_extract(ov.annotation_json,'$."+path+"'))" if 'annotation_json' in cols else "cast(NULL AS varchar)"
        work = field('work_scope_topic_ids_json','work_scope_topic_ids')
        context = field('context_topic_ids_json','context_topic_ids')
        modes = 'ov.work_types_json' if 'work_types_json' in cols else "json_format(cast(transform(cast(json_extract(ov.annotation_json,'$.work_types') AS array(json)),x -> json_extract_scalar(x,'$.work_type')) AS JSON))" if 'annotation_json' in cols else "cast(NULL AS varchar)"
        psc = 'ov.work_psc' if 'work_psc' in cols else 'b.psc'
        # Exact JSON-array membership. No substring topic/part-name matching.
        def overlap(expression, values):
            if not values:return 'false'
            return 'coalesce(try(arrays_overlap(cast(json_parse('+expression+') AS array(varchar)),ARRAY['+','.join(literal(v) for v in sorted(set(values)))+'])),false)'
        predicates = []
        for key in selected:
            tag = catalogue.records[key]; expanded = catalogue.expansion(key)
            if tag['facet'] in ('area','work_scope','context'):
                predicate = overlap(context if tag['facet']=='context' else work,[t['legacy_topic_id'] for t in expanded])
            elif tag['facet']=='work_type':
                predicate = overlap(modes,[t['tag_id'].split(':',1)[1] for t in expanded])
            else:
                codes=[t['tag_id'].split(':',1)[1] for t in expanded]
                predicate=psc+' IN ('+','.join(literal(v) for v in codes)+')' if codes else 'false'
            predicates.append('('+predicate+')')
        interest = (' AND ' if match=='all' else ' OR ').join(predicates) or 'false'
        # Existing source-quality, maturity and source-family grouping remain
        # authoritative; this adds a retrieval route, not a looser demand engine.
        base = self.opportunities(company if mode=='company' else None, kind=kind, _query=True, _source_groups=mode!='combined')
        company_match = 'true' if mode=='company' else 'false'
        company_query = None
        company_join = ''
        company_cte = ''
        company_amount = 'b.commercial_value' if mode=='company' and ' commercial_value,' in base else 'b.value_median'
        if mode=='combined':
            # Reuse the complete company eligibility/quality query. In
            # particular, a latest generic repost must not hide an older source
            # that is the actual eligible company-family representative.
            company_query = self.opportunities(company,kind=kind,_query=True)
            company_amount_column='commercial_value' if ' commercial_value,' in company_query else 'value_median'
            company_cte = ', discovery_company_keys AS (SELECT episode_id,record_hash,priority_score,'+company_amount_column+' discovery_company_amount FROM ('+company_query+'))'
            company_amount='CASE WHEN dc.episode_id IS NOT NULL THEN dc.discovery_company_amount ELSE b.value_median END'
            company_join = ' LEFT JOIN discovery_company_keys dc ON dc.episode_id=b.episode_id AND dc.record_hash=b.record_hash'
            company_match = '(dc.episode_id IS NOT NULL)'
        condition = company_match if mode=='company' else '('+company_match+') OR ('+interest+')' if mode=='combined' else interest
        annotation = 'ov.annotation_json' if 'annotation_json' in cols else "cast(NULL AS varchar)"
        priority = 'b.priority_score' if mode=='company' else 'dc.priority_score' if mode=='combined' else 'cast(NULL AS double)'
        query = 'WITH discovery_base AS ('+base+')'+company_cte+', discovery_candidates AS (SELECT b.*,'+company_match+' discovery_company_match,'+work+' discovery_work_ids_json,'+context+' discovery_context_ids_json,'+modes+' discovery_modes_json,'+psc+' discovery_work_psc,'+annotation+' discovery_annotation_json,'+priority+' discovery_priority_score,'+company_amount+' discovery_size_amount FROM discovery_base b LEFT JOIN '+self._discovery_table(m,'opportunity_vocabulary')+' ov ON ov.episode_id=b.episode_id AND ov.record_hash=b.record_hash'+company_join+' WHERE '+condition+')'
        source_columns={x['Name'] for x in m['tables']['opportunities'].get('columns',[])}
        if mode=='combined' and {'source_name','native_identifier','customer_name','title','notice_posted_date','source_url','source_evidence_id'} <= source_columns:
            family="CASE WHEN c.source_name='sam_notices' AND length(c.native_identifier)>4 THEN concat_ws('|',c.source_name,upper(c.customer_name),regexp_replace(upper(c.native_identifier),'-00$',''),regexp_replace(upper(c.title),'[^A-Z0-9]','')) ELSE c.episode_id END"
            related="json_format(cast(array_agg(map(ARRAY['episode_id','native_identifier','source_url','source_evidence_id','posted_date'],ARRAY[c.episode_id,c.native_identifier,c.source_url,c.source_evidence_id,cast(c.notice_posted_date AS varchar)])) OVER (PARTITION BY "+family+") AS JSON))"
            query+=', discovery_families AS (SELECT c.*,row_number() OVER (PARTITION BY '+family+' ORDER BY discovery_company_match DESC,c.notice_posted_date DESC NULLS LAST,c.episode_id) discovery_source_rank,'+related+' related_source_records_json FROM discovery_candidates c) SELECT q.* FROM discovery_families q WHERE q.discovery_source_rank=1'
        else:query+=' SELECT q.* FROM discovery_candidates q'
        # A size guide is not expected revenue: preserve its original basis on
        # every card. Do not use an upper-only plan band as a minimum value or
        # allow quarantined subcontract amounts to affect filtering/ranking.
        amount='coalesce(q.discovery_size_amount,q.value_low)' if 'value_low' in source_columns else 'q.discovery_size_amount'
        risk=finance_risk_sql(m,'q') if 'lane' in source_columns else 'false'
        amount='CASE WHEN '+risk+' OR ('+amount+')<=0 THEN cast(NULL AS double) ELSE '+amount+' END'
        # Keep unknown values available by default, including substantial work
        # whose published notice has no usable estimate.
        query='SELECT q.* FROM ('+query+') q WHERE '+('true' if include_unpriced else '('+amount+') IS NOT NULL')
        if minimum_value>0:
            query+=' AND (('+amount+')>='+str(float(minimum_value))+(' OR ('+amount+') IS NULL' if include_unpriced else '')+')'
        maturity="CASE q.stage WHEN 'qualification_opening' THEN 0 WHEN 'teaming_opening' THEN 0 WHEN 'published_requirement' THEN 1 WHEN 'emerging_demand' THEN 2 ELSE 3 END"
        if sort=='value':ordering='('+amount+') DESC NULLS LAST,'+maturity+','
        elif sort=='deadline':ordering='q.window_end ASC NULLS LAST,'+maturity+','
        else:ordering='q.discovery_company_match DESC,q.discovery_priority_score DESC NULLS LAST,'+maturity+',('+amount+') DESC NULLS LAST,'
        query += ' ORDER BY '+ordering+'q.window_end ASC NULLS LAST,q.episode_id OFFSET '+str(offset)+' LIMIT '+str(limit+1)
        if self.manifest()['run_id'] != m['run_id']: raise ReleaseChanged('The evidence release changed. Reload the feed before continuing.')
        records = self.rows(query)
        response.update(availability='available',has_more=len(records)>limit,next_offset=offset+limit if len(records)>limit else None)
        # Interest discovery must not remove the company's exact item quantities,
        # unit prices, fit caveats or source/access evidence on combined cards.
        company_rows={}
        if company_query:
            ids=[r['episode_id'] for r in records[:limit] if r.get('discovery_company_match') is True or str(r.get('discovery_company_match')).lower()=='true']
            if ids:
                enriched=self.rows('SELECT q.* FROM ('+company_query+') q WHERE q.episode_id IN ('+','.join(literal(x) for x in ids)+')')
                company_rows={(r['episode_id'],r['record_hash']):r for r in enriched}
        for raw in records[:limit]:
            row = dict(raw)
            matched = catalogue.matched(row,selected)
            fit = raw.get('discovery_company_match') is True or str(raw.get('discovery_company_match')).lower()=='true'
            related=row.get('related_source_records_json')
            row.update(company_rows.get((raw['episode_id'],raw['record_hash']),{}))
            if related is not None:row['related_source_records_json']=related
            row.update(matched_interest_tags=matched, discovery_routes=(['company_match'] if fit else [])+(['selected_interest'] if matched else []),
                       company_fit_established=fit, fit_assessment='existing_engine_candidate' if fit else 'not_assessed_for_company',qualification='not_established')
            # Raw detail stays available via the stable episode evidence endpoint.
            for name in ('discovery_work_ids_json','discovery_context_ids_json','discovery_modes_json','discovery_annotation_json','discovery_company_match','discovery_priority_score','discovery_size_amount'): row.pop(name,None)
            response['records'].append(guard_finance(row,m))
        return response


def register_routes(router, repo, call):
    from fastapi import Query, HTTPException
    @router.get('/work-tags')
    def work_tags(q:str=Query('',max_length=100),facet:str='all',limit:int=Query(100,ge=1,le=100),offset:int=Query(0,ge=0,le=10000)):
        return call(repo.work_tags,q,facet,limit,offset)
    @router.get('/companies/{cage}/activity')
    def activity(cage:str,limit:int=Query(25,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000),since:str=None):
        return call(repo.activity_feed,cage,limit,offset,since)
    @router.get('/market-updates')
    def market_updates(tags:str=Query('',max_length=4096),company:str=None,limit:int=Query(3,ge=1,le=3)):
        return call(repo.market_updates,tags,company,limit)
    @router.get('/discovery')
    def discovery(mode:str='interests',tags:str=Query('',max_length=4096),match:str='any',company:str=None,kind:str='all',limit:int=Query(24,ge=1,le=100),offset:int=Query(0,ge=0,le=1000000),release:str=Query(None,max_length=120),minimum_value:float=Query(0,ge=0,le=1e12),include_unpriced:bool=True,sort:str='relevance'):
        try: return call(repo.discovery,mode,tags,match,company,kind,limit,offset,release,minimum_value,include_unpriced,sort)
        except RuntimeError as error:
            # Both supported package import paths preserve the domain error code.
            if getattr(error,'error_code',None) != 'opportunity_release_changed': raise
            raise HTTPException(status_code=409,detail=str(error))
