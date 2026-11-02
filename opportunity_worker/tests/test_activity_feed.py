import duckdb,pytest
from opportunity_engine.activity_feed import ActivityFeedMixin


class Repo(ActivityFeedMixin):
    def __init__(self):
        self.c=duckdb.connect();self.queries=[]
        self.m=dict(run_id='r1',as_of='2026-11-01',tables={'company_activity_actions':{},'company_subcontract_facts':{}})
        self.c.execute('CREATE TABLE company_activity_actions(company_id VARCHAR,transaction_key VARCHAR,action_date VARCHAR,work_text VARCHAR,customer VARCHAR,action_obligation DOUBLE,source_url VARCHAR,evidence_id VARCHAR,award_key VARCHAR)')
        self.c.execute("INSERT INTO company_activity_actions VALUES ('CAGE:ABCDE','a','2026-10-20','<p>Avionics integration</p>','Navy',-100,'https://source/award','p','AWARD'),('CAGE:OTHER','b','2026-10-21','Unrelated','Army',100,'https://source/other','o','OTHER')")
        self.c.execute('CREATE TABLE company_subcontract_facts(company_id VARCHAR,report_key VARCHAR,event_date VARCHAR,work_text VARCHAR,prime_name VARCHAR,reported_amount DOUBLE,source_url VARCHAR,evidence_id VARCHAR,parent_award_key VARCHAR)')
        self.c.execute("INSERT INTO company_subcontract_facts VALUES ('CAGE:ABCDE','s','2026-10-22','Test equipment','Prime',1000000000000,'https://source/report','s','PARENT')")
    def manifest(self):return self.m
    def table(self,name):return name
    def rows(self,sql):
        self.queries.append(sql);cur=self.c.execute(sql);return [dict(zip([d[0] for d in cur.description],r)) for r in cur.fetchall()]


def test_watch_activity_is_actual_company_history_and_quarantines_unvalidated_subcontract_value():
    r=Repo();result=r.activity_feed('ABCDE')
    assert [x['source_record_id'] for x in result['records']]==['s','a']
    sub,prime=result['records'];assert sub['observed_amount'] is None
    assert prime['observed_amount']==-100 and prime['title']=='Avionics integration'
    assert sub['parent_award_key']=='PARENT' and prime['award_key']=='AWARD'
    assert all('reported_amount_raw' not in x for x in result['records'])


def test_activity_pagination_since_and_stable_identity_across_releases():
    r=Repo();a=r.activity_feed('ABCDE',limit=1)
    assert a['has_more'] and a['next_offset']==1
    b=r.activity_feed('ABCDE',limit=1,offset=1)
    assert b['records'][0]['activity_id']!=a['records'][0]['activity_id'] and not b['has_more']
    r.m['run_id']='r2';assert r.activity_feed('ABCDE',since='2026-10-21')['records'][0]['activity_id']==a['records'][0]['activity_id']


@pytest.mark.parametrize('kwargs',[dict(cage="ABC' OR 1=1"),dict(cage='ABCDE',since='2026-02-30'),dict(cage='ABCDE',since="2026-01-01'"),dict(cage='ABCDE',limit=1000)])
def test_invalid_inputs_never_execute_a_query(kwargs):
    r=Repo()
    with pytest.raises(ValueError):r.activity_feed(**kwargs)
    assert r.queries==[]
