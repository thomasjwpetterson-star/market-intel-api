import json
import pytest
from opportunity_engine.discovery_tags import TagCatalogue, parse_tags
from opportunity_engine.discovery_read_model import DiscoveryRepositoryMixin, ReleaseChanged


TOPICS = [dict(topic_id='electronics',label='Defence electronics',parent_id=None,facet='navigation'),
          dict(topic_id='aircraft',label='Aircraft',parent_id=None,facet='navigation'),
          dict(topic_id='radar',label='Radar',parent_id='electronics',facet='work_scope'),
          dict(topic_id='radar.processing',label='Radar processing',parent_id='radar',facet='work_scope'),
          dict(topic_id='radar.processing.track',label='Track processing',parent_id='radar.processing',facet='work_scope'),
          dict(topic_id='radar.processing.track.fusion',label='Track fusion',parent_id='radar.processing.track',facet='work_scope'),
          dict(topic_id='rotorcraft',label='Rotorcraft platforms',parent_id='aircraft',facet='platform_context')]
PSC = [dict(psc='58',label='Communications',parent_code=None),dict(psc='5840',label='Radar equipment',parent_code='58')]


def test_search_offers_broad_groups_and_aliases_but_keeps_psc_explicit():
    c=TagCatalogue(TOPICS,PSC)
    assert c.search('defense electronics')['records'][0]['tag_id']=='area:electronics'
    assert c.search('AESA')['records'][0]['tag_id']=='scope:radar'
    assert not any(x['facet']=='classification' for x in c.search()['records'])
    assert c.search('5840')['records'][0]['tag_id']=='psc:5840'
    assert c.search(facet='classification')['total']==2
    assert c.search(limit=1)['has_more']
    assert c.search(offset=1000)['records']==[]


def test_expansion_handles_arbitrary_depth_and_keeps_work_separate_from_context():
    c=TagCatalogue(TOPICS,PSC)
    assert len(c.expansion('scope:radar'))==4
    assert all(x['facet']=='work_scope' for x in c.expansion('area:electronics'))
    assert c.expansion('area:aircraft')==[]  # platform context is not aircraft manufacturing.
    assert [x['tag_id'] for x in c.expansion('psc:58')]==['psc:58','psc:5840']
    with pytest.raises(ValueError,match='Cycle'):TagCatalogue([TOPICS[2]|dict(parent_id='radar')])


@pytest.mark.parametrize('bad',["scope:radar' OR 1=1",'radar','scope:radar,','scope:radar;context:rotorcraft', ['scope:radar'], 'scope:x,'*51])
def test_only_bounded_canonical_ids_can_be_selected(bad):
    with pytest.raises(ValueError):parse_tags(bad)


def test_exact_membership_and_evidence_dont_promote_a_platform_mention():
    c=TagCatalogue(TOPICS)
    row=dict(discovery_work_ids_json='["radar.processing.track.fusion"]',discovery_context_ids_json='["rotorcraft"]',
             discovery_annotation_json=json.dumps(dict(topics=[dict(topic_id='radar.processing.track.fusion',passage='Radar track fusion software')])))
    result=c.matched(row,['scope:radar','context:rotorcraft','area:aircraft'])
    assert [x['tag_id'] for x in result]==['scope:radar','context:rotorcraft']
    assert result[0]['passages']==['Radar track fusion software']
    assert c.matched(row|dict(discovery_work_ids_json='["radar-like"]'),['scope:radar'])==[]


@pytest.fixture
def repo():
    import duckdb,re
    c=duckdb.connect();c.execute('SET threads=1')
    c.execute('CREATE MACRO json_parse(x) AS cast(x AS JSON)')
    c.execute('CREATE MACRO json_extract_scalar(x,p) AS json_extract_string(x,p)')
    c.execute('CREATE MACRO json_format(x) AS cast(x AS VARCHAR)')
    c.execute('CREATE TABLE work_vocabulary_topics(topic_id VARCHAR,label VARCHAR,parent_id VARCHAR,facet VARCHAR)')
    c.executemany('INSERT INTO work_vocabulary_topics VALUES (?,?,?,?)',[(x['topic_id'],x['label'],x['parent_id'],x['facet']) for x in TOPICS])
    c.execute('CREATE TABLE work_vocabulary_reference(psc VARCHAR,label VARCHAR,parent_code VARCHAR)')
    c.executemany('INSERT INTO work_vocabulary_reference VALUES (?,?,?)',[(x['psc'],x['label'],x['parent_code']) for x in PSC])
    c.execute('CREATE TABLE opportunities(episode_id VARCHAR,record_hash VARCHAR,title VARCHAR,psc VARCHAR,stage VARCHAR,window_end VARCHAR,value_median DOUBLE,value_basis VARCHAR)')
    c.executemany('INSERT INTO opportunities VALUES (?,?,?,?,?,?,?,?)',[
        ('a'*64,'1'*64,'Radar fusion research','5840','published_requirement','2027-02-01',2000000.,'historical_comparison'),
        ('b'*64,'2'*64,'Established wiring work','5995','emerging_demand','2032-01-01',None,None),
        ('c'*64,'3'*64,'Work on rotorcraft','5840','published_requirement','2027-03-01',500000.,'source_planned_value'),
        ('d'*64,'4'*64,'Old radar notice','5840','resolved_or_elapsed','2020-01-01',2000000.,'historical_comparison')])
    c.execute("ALTER TABLE opportunities ADD COLUMN lane VARCHAR DEFAULT 'published_procurement'")
    c.execute('CREATE TABLE opportunity_vocabulary(episode_id VARCHAR,record_hash VARCHAR,work_scope_topic_ids_json VARCHAR,context_topic_ids_json VARCHAR,work_types_json VARCHAR,annotation_json VARCHAR,work_psc VARCHAR)')
    a=json.dumps(dict(topics=[dict(topic_id='radar.processing.track.fusion',passage='Radar fusion research')],work_types=[dict(work_type='design_development',passage='Research of radar fusion')]))
    c.executemany('INSERT INTO opportunity_vocabulary VALUES (?,?,?,?,?,?,?)',[
        ('a'*64,'1'*64,'["radar.processing.track.fusion"]','[]','["design_development"]',a,'5840'),
        ('b'*64,'2'*64,'[]','[]','[]','{}','5995'),
        ('c'*64,'3'*64,'[]','["rotorcraft"]','[]','{}',None),
        ('d'*64,'4'*64,'["radar"]','[]','[]','{}','5840')])
    c.execute('CREATE TABLE company_matches(company_id VARCHAR,episode_id VARCHAR,opportunity_hash VARCHAR)')
    c.executemany('INSERT INTO company_matches VALUES (?,?,?)',[('CAGE:ABCDE','a'*64,'1'*64),('CAGE:ABCDE','b'*64,'2'*64),('CAGE:ABCDE','c'*64,'3'*64)])
    c.execute('CREATE TABLE company_work_matches(company_id VARCHAR,episode_id VARCHAR,opportunity_hash VARCHAR,review_category VARCHAR)')
    c.execute("INSERT INTO company_work_matches VALUES ('CAGE:ABCDE',repeat('c',64),repeat('3',64),'market_context')")
    class Local(DiscoveryRepositoryMixin):
        def __init__(self):
            self.current=dict(run_id='release-one',tables={},work_vocabulary_policy={'version':'fixture'})
            for name in ['opportunities','demand_decisions','work_vocabulary_topics','work_vocabulary_reference','opportunity_vocabulary','company_matches','company_work_matches']:
                cols=c.execute('DESCRIBE '+name).fetchall() if name!='demand_decisions' else []
                self.current['tables'][name]={'columns':[{'Name':x[0]} for x in cols]}
            self.queries=[]
            self.connection=c
        def manifest(self):return self.current
        def _discovery_table(self,m,n):return n
        def opportunities(self,company=None,kind='all',_query=False,_source_groups=True):
            # Existing source eligibility remains authoritative outside tag logic.
            sql="SELECT *"+(",123.45 government_reference_unit_price,CASE WHEN value_median>1000000 THEN 90 ELSE 20 END priority_score" if company else '')+" FROM opportunities WHERE stage<>'resolved_or_elapsed'"
            if company:sql+=" AND episode_id IN (SELECT episode_id FROM company_matches WHERE company_id='"+company+"') AND episode_id NOT IN (SELECT episode_id FROM company_work_matches WHERE review_category='market_context')"
            return sql
        def rows(self,sql):
            self.queries.append(sql)
            sql=re.sub(r'array\(varchar\)','VARCHAR[]',sql,flags=re.I)
            sql=re.sub(r'array\(json\)','JSON[]',sql,flags=re.I).replace('transform(','list_transform(').replace('arrays_overlap(','list_has_any(')
            cur=c.execute(sql);return [dict(zip([x[0] for x in cur.description],r)) for r in cur.fetchall()]
    yield Local()
    c.close()


def test_interests_without_cage_and_multi_tag_all_preserve_values(repo):
    r=repo.discovery(tags='scope:radar,work:design_development',match='all')
    assert [x['episode_id'] for x in r['records']]==['a'*64]
    a=r['records'][0]
    assert a['discovery_routes']==['selected_interest'] and not a['company_fit_established']
    assert a['qualification']=='not_established' and a['value_median']==2000000 and a['value_basis']=='historical_comparison'
    assert len(a['matched_interest_tags'])==2 and a['matched_interest_tags'][0]['passages']
    assert repo.discovery(tags='scope:radar,context:rotorcraft',match='all')['records']==[]
    assert len(repo.discovery(tags='scope:radar,context:rotorcraft',match='any')['records'])==2


def test_combined_is_union_deduplicated_not_an_intersection_or_qualification_claim(repo):
    r=repo.discovery(mode='combined',tags='scope:radar,context:rotorcraft',company='CAGE:ABCDE')
    assert {x['episode_id'] for x in r['records']}=={x*64 for x in 'abc'}
    assert len(r['records'])==3
    rows={x['episode_id'][0]:x for x in r['records']}
    assert rows['a']['discovery_routes']==['company_match','selected_interest']
    assert rows['b']['discovery_routes']==['company_match']  # outside the chosen tags; distant year retained.
    assert rows['c']['discovery_routes']==['selected_interest'] and not rows['c']['company_fit_established']
    assert rows['b']['window_end']=='2032-01-01'
    assert float(rows['a']['government_reference_unit_price'])==123.45
    assert 'government_reference_unit_price' not in rows['c']


def test_work_classification_does_not_use_a_parent_only_psc(repo):
    assert [x['episode_id'] for x in repo.discovery(tags='psc:58')['records']]==['a'*64]
    assert repo.discovery(tags='area:aircraft')['records']==[]
    assert repo.discovery(tags='context:rotorcraft')['records'][0]['episode_id']=='c'*64


def test_pagination_has_no_duplicates_and_rejects_a_changed_release(repo):
    first=repo.discovery(mode='combined',company='CAGE:ABCDE',tags='context:rotorcraft',limit=1)
    assert first['has_more'] and first['next_offset']==1
    second=repo.discovery(mode='combined',company='CAGE:ABCDE',tags='context:rotorcraft',limit=1,offset=1,release=first['release'])
    assert first['records'][0]['episode_id']!=second['records'][0]['episode_id']
    with pytest.raises(ReleaseChanged):repo.discovery(tags='scope:radar',release='old-release')


@pytest.mark.parametrize('kwargs',[{'tags':'scope:fake'}, {'mode':'company','tags':'scope:radar','company':'CAGE:ABCDE'}, {'mode':'combined','tags':'scope:radar'}, {'tags':''}, {'tags':'scope:radar','company':"CAGE:ABCDE' OR 1=1"}, {'tags':'scope:radar','limit':101}])
def test_invalid_selection_never_reaches_opportunity_sql(repo,kwargs):
    with pytest.raises(ValueError):repo.discovery(**kwargs)
    assert not any('WITH discovery_base' in q for q in repo.queries)


def test_old_release_is_explicitly_unpublished_not_an_available_empty_feed(repo):
    repo.current['tables'].pop('opportunity_vocabulary')
    assert repo.discovery(tags='scope:radar')['availability']=='not_published'
    assert repo.work_tags()['availability']=='not_published'


def test_matching_bundled_policy_populates_picker_without_warehouse_reads(repo):
    from opportunity_engine.work_vocabulary import POLICY_HASH,SPEC,codebook
    repo.current.update(as_of='2026-11-01',work_vocabulary_policy={'policy_hash':POLICY_HASH,'psc_nodes':len(codebook('2026-11-01'))})
    repo.current['tables']['work_vocabulary_topics']['rows']=len(SPEC['roots'])+len(SPEC['topics'])
    assert repo.work_tags()['total']==96 and repo.queries==[]
    assert repo.work_tags(q='5840')['records'][0]['tag_id']=='psc:5840'


def test_combined_reposts_keep_company_representative_prices_and_all_sources(repo):
    c=repo.connection
    for name in ['source_name','native_identifier','customer_name','notice_posted_date','source_url','source_evidence_id']:
        c.execute('ALTER TABLE opportunities ADD COLUMN '+name+' VARCHAR')
        repo.current['tables']['opportunities']['columns'].append({'Name':name})
    c.execute("UPDATE opportunities SET source_name='sam_notices',native_identifier=episode_id,customer_name='Navy',notice_posted_date='2026-09-01',source_url='https://example.test/source',source_evidence_id=record_hash")
    c.execute("UPDATE opportunities SET native_identifier='RADAR-001' WHERE episode_id=repeat('a',64)")
    c.execute("INSERT INTO opportunities SELECT * REPLACE(repeat('e',64) AS episode_id,repeat('5',64) AS record_hash,'RADAR-001-00' AS native_identifier,'2026-10-01' AS notice_posted_date) FROM opportunities WHERE episode_id=repeat('a',64)")
    c.execute("INSERT INTO opportunity_vocabulary SELECT * REPLACE(repeat('e',64) AS episode_id,repeat('5',64) AS record_hash) FROM opportunity_vocabulary WHERE episode_id=repeat('a',64)")
    result=repo.discovery(mode='combined',company='CAGE:ABCDE',tags='scope:radar')
    assert {x['episode_id'] for x in result['records']}=={x*64 for x in 'ab'}
    a=next(x for x in result['records'] if x['episode_id']=='a'*64)
    assert float(a['government_reference_unit_price'])==123.45
    assert {x['episode_id'] for x in json.loads(a['related_source_records_json'])}=={x*64 for x in 'ae'}


def test_release_change_during_taxonomy_load_never_executes_opportunity_query(repo):
    original=repo._discovery_catalogue
    def changed(manifest):
        catalogue=original(manifest)
        repo.current=dict(repo.current,run_id='release-two')
        return catalogue
    repo._discovery_catalogue=changed
    with pytest.raises(ReleaseChanged):repo.discovery(tags='scope:radar')
    assert not any('WITH discovery_base' in q for q in repo.queries)


@pytest.mark.parametrize('module',['opportunity_engine.api','opportunity_intelligence.api'])
def test_both_api_adapters_keep_auth_limits_and_release_conflicts(repo,module):
    import importlib
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    api=importlib.import_module(module)
    app=FastAPI();app.include_router(api.create_router(lambda:None,repo));client=TestClient(app)
    assert client.get('/opportunity-intelligence/work-tags').status_code==401
    app=FastAPI();app.include_router(api.create_router(lambda:{'id':'verified-user'},repo));client=TestClient(app)
    assert client.get('/opportunity-intelligence/work-tags?q=AESA').json()['records'][0]['tag_id']=='scope:radar'
    response=client.get('/opportunity-intelligence/discovery?tags=scope:radar&minimum_value=1000000&include_unpriced=false&sort=value')
    assert response.status_code==200
    assert response.json()['minimum_value']==1000000 and response.json()['include_unpriced'] is False and response.json()['sort']=='value'
    assert client.get('/opportunity-intelligence/discovery?tags=scope:radar&minimum_value=-1').status_code==422
    assert client.get('/opportunity-intelligence/discovery?tags=scope:fake').status_code==400
    assert client.get('/opportunity-intelligence/discovery?tags=scope:radar&release=old').status_code==409
    assert client.get('/opportunity-intelligence/discovery?tags=scope:radar&limit=101').status_code==422


def test_interest_value_filter_keeps_unknown_work_optional_and_retains_value_basis(repo):
    result=repo.discovery(mode='combined',company='CAGE:ABCDE',tags='context:rotorcraft',minimum_value=1000000)
    assert {x['episode_id'] for x in result['records']}=={x*64 for x in 'ab'}
    result=repo.discovery(tags='scope:radar,context:rotorcraft',minimum_value=1000000,include_unpriced=False)
    assert [x['episode_id'] for x in result['records']]==['a'*64]
    assert result['records'][0]['value_basis']=='historical_comparison'
    assert repo.discovery(tags='context:rotorcraft',minimum_value=1000000,include_unpriced=False)['records']==[]


def test_size_ranking_and_deadline_ranking_are_separate(repo):
    repo.connection.execute("UPDATE opportunities SET window_end='2027-01-01' WHERE episode_id=repeat('c',64)")
    kwargs=dict(tags='scope:radar,context:rotorcraft')
    assert [x['episode_id'][0] for x in repo.discovery(**kwargs,sort='value')['records']]==['a','c']
    assert [x['episode_id'][0] for x in repo.discovery(**kwargs,sort='deadline')['records']]==['c','a']


def test_company_discovery_retains_existing_priority_instead_of_reordering_by_deadline(repo):
    repo.connection.execute("UPDATE opportunities SET window_end='2026-11-03' WHERE episode_id=repeat('b',64)")
    result=repo.discovery(mode='company',company='CAGE:ABCDE')
    assert [x['episode_id'][0] for x in result['records']]==['a','b']


@pytest.mark.parametrize('bad',[dict(minimum_value=float('nan')),dict(minimum_value=float('inf')),dict(minimum_value=-1),dict(minimum_value=1e13),dict(include_unpriced='false'),dict(sort='unsafe')])
def test_invalid_value_filters_are_rejected_before_query(repo,bad):
    with pytest.raises(ValueError):repo.discovery(tags='scope:radar',**bad)
    assert repo.queries==[]


def test_quarantined_subcontract_value_cannot_rank_or_pass_minimum(repo):
    c=repo.connection
    c.execute("UPDATE opportunities SET lane='recurring_subcontract_work' WHERE episode_id=repeat('a',64)")
    result=repo.discovery(tags='scope:radar,context:rotorcraft',minimum_value=1000000,include_unpriced=False)
    assert result['records']==[]
    result=repo.discovery(tags='scope:radar,context:rotorcraft',sort='value')
    assert [x['episode_id'][0] for x in result['records']]==['c','a']
    assert result['records'][1]['value_median'] is None


def test_company_only_feed_skips_interest_catalogue_and_repeated_vocabulary_join(repo):
    def fail(*args):raise AssertionError('Company-only feed must not fetch catalogue')
    repo._discovery_catalogue=fail
    result=repo.discovery(mode='company',company='CAGE:ABCDE')
    assert len(result['records'])==2 and len(repo.queries)==1
    assert 'JOIN opportunity_vocabulary' not in repo.queries[0]


def test_unknown_company_commercial_size_encoded_as_zero_remains_optional(repo):
    original=repo.opportunities
    def query(*args,**kwargs):
        return 'SELECT z.*,coalesce(value_median,0.0) commercial_value,1 extra FROM ('+original(*args,**kwargs)+') z'
    repo.opportunities=query
    result=repo.discovery(mode='company',company='CAGE:ABCDE',minimum_value=1000000)
    assert {x['episode_id'][0] for x in result['records']}=={'a','b'}
    result=repo.discovery(mode='company',company='CAGE:ABCDE',minimum_value=1000000,include_unpriced=False)
    assert [x['episode_id'][0] for x in result['records']]==['a']
