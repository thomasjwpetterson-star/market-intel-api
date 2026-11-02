import copy,json
import duckdb
import pytest
from opportunity_engine.market_updates import budget_query,future_plan_query,render,MarketUpdatesMixin
from opportunity_engine.discovery_tags import TagCatalogue
from opportunity_engine.work_vocabulary import SPEC


def row():
    return dict(funding_id='new',previous_funding_id='old',component='Air Force',budget_line_item='RADAR',
        budget_line_item_title='Radar modernization',description_excerpt='Radar processing upgrades',
        fiscal_year=2027,submission_fiscal_year=2027,previous_submission_fy=2026,
        measure_type='funding',value_kind='requested',funding_status='requested',previous_funding_status='requested',
        amount_usd=12000000.,previous_amount_usd=10000000.,change_usd=2000000.,
        current_source_url='https://budget.example/2027.pdf',previous_source_url='https://budget.example/2026.pdf',
        current_source_locator='Page 12',previous_source_locator='Page 9')


def test_budget_note_retains_same_year_editions_two_sources_and_no_pipeline_value():
    result=render(row(),{'radar'})
    assert 'FY2027 budget increased 20%' in result['title']
    assert result['current_budget']==12000000 and result['value_kind']=='programme_budget_context'
    assert 'commercial_value' not in result and len(result['evidence'])==2
    assert result['matched_interests'][0]['tag_id']=='scope:radar'
    assert 'awarded contract' in result['note']


@pytest.mark.parametrize('patch',[dict(previous_amount_usd=0),dict(amount_usd=float('inf')),dict(current_source_url='javascript:bad'),dict(previous_source_url=None),dict(description_excerpt='No radar included',budget_line_item_title='General upgrades')])
def test_invalid_and_negated_or_unsourced_notes_are_not_shown(patch):
    assert render(row()|patch,{'radar'}) is None


def test_reduction_is_a_change_in_plans_not_a_claim_of_a_new_contract():
    result=render(row()|dict(amount_usd=8000000.),{'radar'})
    assert 'reduced 20%' in result['title'] and result['change_percent']==-20


def test_real_query_excludes_funding_stage_changes_and_keeps_both_source_rows():
    c=duckdb.connect();c.execute('CREATE MACRO regexp_like(t,p) AS regexp_matches(t,p)')
    c.execute('CREATE TABLE revisions(funding_id VARCHAR,previous_funding_id VARCHAR,component VARCHAR,budget_line_item VARCHAR,fiscal_year BIGINT,measure_type VARCHAR,value_kind VARCHAR,funding_status VARCHAR,previous_funding_status VARCHAR,submission_fiscal_year BIGINT,previous_submission_fy BIGINT,amount_usd DOUBLE,previous_amount_usd DOUBLE,change_usd DOUBLE)')
    c.execute("INSERT INTO revisions VALUES ('new','old','Air Force','RADAR',2027,'funding','requested','requested','requested',2027,2026,12000000,10000000,2000000)")
    c.execute('CREATE TABLE funding(funding_id VARCHAR,budget_line_item_title VARCHAR,description_excerpt VARCHAR,source_url VARCHAR,source_locator VARCHAR,fiscal_year BIGINT,measure_type VARCHAR,value_kind VARCHAR,component VARCHAR,budget_line_item VARCHAR,appropriation_category VARCHAR)')
    c.execute("INSERT INTO funding VALUES ('old','Radar modernization','Radar processing','https://budget.example/2026.pdf','Page 9',2027,'funding','requested','Air Force','RADAR','Procurement'),('new','Radar modernization','Radar processing','https://budget.example/2027.pdf','Page 12',2027,'funding','requested','Air Force','RADAR','Procurement')")
    query=budget_query('revisions','funding',[r for r in SPEC['topics'] if r['id']=='radar'],3)
    assert len(c.execute(query).fetchall())==1
    c.execute("UPDATE revisions SET previous_funding_status='enacted'")
    assert c.execute(query).fetchall()==[]
    c.execute("UPDATE revisions SET previous_funding_status='requested'")
    c.execute("UPDATE funding SET fiscal_year=2026 WHERE funding_id='old'")
    assert c.execute(query).fetchall()==[]
    c.close()


class Repo(MarketUpdatesMixin):
    def __init__(self):
        self.queries=[]
        self.m=dict(run_id='one',as_of='2026-11-01',tables={name:{} for name in ('budget_revision_signals','programme_funding','work_vocabulary_topics','opportunity_vocabulary','company_capability_vocabulary','work_vocabulary_scopes')})
    def manifest(self):return self.m
    def _discovery_catalogue(self,m):return TagCatalogue([dict(topic_id=t['id'],label=t['label'],parent_id=t.get('parent'),facet=t.get('facet','navigation')) for t in SPEC['roots']+SPEC['topics']])
    def _discovery_table(self,m,n):return n
    def capability_vocabulary(self,*args):return [dict(annotation_json=json.dumps(dict(work_scope_topic_ids=['radar'],context_topic_ids=['rotorcraft'])))]
    def rows(self,sql):self.queries.append(sql);return [row(),row()|dict(funding_id='later',fiscal_year=2028)]


def test_company_topics_are_source_backed_and_duplicate_line_years_not_multiple_updates():
    r=Repo();result=r.market_updates(company='CAGE:ABCDE')
    assert result['selected_tags']==['scope:radar']
    assert len(result['records'])==1 and result['selection_basis']=='observed_company_work_topics'
    assert len(r.queries)==2


def test_manual_interests_require_no_company_and_psc_is_not_assumed_budget_scope():
    r=Repo();assert len(r.market_updates(tags='scope:radar')['records'])==1
    r=Repo();assert r.market_updates()['records']==[] and r.queries==[]
    r=Repo();r.m['tables'].pop('programme_funding')
    assert r.market_updates(tags='scope:radar')['availability']=='not_published'


@pytest.mark.parametrize('kwargs',[dict(tags='unsafe'),dict(company="CAGE:ABCDE' OR 1=1"),dict(tags='scope:radar',limit=4)])
def test_bad_inputs_never_reach_source_query(kwargs):
    r=Repo()
    with pytest.raises(ValueError):r.market_updates(**kwargs)
    assert r.queries==[]


def test_future_plan_names_both_future_years_and_does_not_claim_an_award():
    result=render(row()|dict(comparison_kind='future_plan',fiscal_year=2029,previous_fiscal_year=2028,previous_submission_fy=2027),{'radar'})
    assert result['category']=='future_budget_plan'
    assert 'projects $10,000,000 in FY2028 and $12,000,000 in FY2029' in result['summary']
    assert 'planned funding' in result['title'] and result['value_kind']=='programme_budget_context'
    assert render(row()|dict(comparison_kind='future_plan',fiscal_year=2031,previous_fiscal_year=2028),{'radar'}) is None


def test_future_plan_query_requires_latest_same_edition_and_adjacent_projected_years():
    query=future_plan_query('funding',[r for r in SPEC['topics'] if r['id']=='radar'],'2026-11-01',30)
    assert 'p.fiscal_year=old.fiscal_year+1' in query
    assert "old.funding_status='projected'" in query and "p.funding_status='projected'" in query
    assert 'p.submission_fiscal_year=old.submission_fiscal_year' in query
    assert "old.fiscal_year>year(DATE '2026-11-01')" in query
    assert "p.value_kind='amount_usd'" in query


def test_incidental_complementary_system_is_not_a_technology_market_trend():
    sample=row()|dict(budget_line_item_title='Sidewinder (AIM-9X)',description_excerpt='Complements the radar guided Advanced Medium Range Air-to-Air Missile (AMRAAM).')
    assert render(sample,{'radar'}) is None


@pytest.mark.parametrize('title,topic',[('Rolling Airframe Missile (RAM)','airframes-structures'),('Ship to Shore Connector','electronic-connectors')])
def test_named_systems_do_not_match_unrelated_component_word_senses(title,topic):
    assert render(row()|dict(budget_line_item_title=title),{topic}) is None


def test_immaterial_plan_change_is_not_a_headline_market_update():
    assert render(row()|dict(amount_usd=10010000.),{'radar'}) is None
