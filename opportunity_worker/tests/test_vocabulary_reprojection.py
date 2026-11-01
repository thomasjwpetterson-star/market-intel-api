import json
import duckdb
from opportunity_engine.work_vocabulary import annotate,codebook
from opportunity_engine.vocabulary_reprojection import opportunity_sql

def test_prime_study_psc_does_not_classify_a_supplier_component():
    c=duckdb.connect();ref={r['psc']:r for r in codebook('2026-11-01')}
    c.execute('CREATE TABLE new_work_vocabulary_scopes(scope_id VARCHAR,work_text VARCHAR,reported_psc VARCHAR,normalized_scope_label VARCHAR,scope_status VARCHAR,work_scope_topic_ids_json VARCHAR,context_topic_ids_json VARCHAR,annotation_json VARCHAR)')
    for sid,text in [('known','Potential supplier package: FUEL BOOST PUMP'),('unclear','Potential supplier package: SPECIAL ASSEMBLY')]:
        a=annotate(text,'B541',reference=ref)
        c.execute('INSERT INTO new_work_vocabulary_scopes VALUES (?,?,?,?,?,?,?,?)',[sid,text,'B541',a['normalized_scope_label'],a['scope_status'],json.dumps(a['work_scope_topic_ids']),json.dumps(a['context_topic_ids']),json.dumps(a)])
    c.execute("CREATE TABLE vocabulary_opportunity_keys AS SELECT * FROM (VALUES ('e1','h1','new_prime_historical_supplier_flow','B541','known'),('e2','h2','new_prime_historical_supplier_flow','B541','unclear'),('e3','h3','published_procurement','B541','unclear')) t(episode_id,record_hash,lane,psc,scope_id)")
    c.execute('CREATE VIEW result AS '+opportunity_sql())
    assert c.execute("SELECT work_psc,parent_prime_psc,psc_scope FROM result WHERE episode_id='e1'").fetchone()==(None,'B541','linked_prime_context')
    assert c.execute("SELECT normalized_scope_label FROM result WHERE episode_id='e1'").fetchone()[0].startswith('Fuel systems')
    assert c.execute("SELECT normalized_scope_label,scope_status FROM result WHERE episode_id='e2'").fetchone()==('Supplier work scope needs classification','context_only')
    assert c.execute("SELECT work_psc,parent_prime_psc FROM result WHERE episode_id='e3'").fetchone()==('B541',None)
    assert c.execute("SELECT record_hash FROM result WHERE episode_id='e2'").fetchone()[0]=='h2'
