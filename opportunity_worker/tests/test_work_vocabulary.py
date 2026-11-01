import hashlib,json,shutil
from pathlib import Path
import duckdb
from opportunity_engine.work_vocabulary import annotate,codebook,evidence_verdict,administrative_only

REFERENCE={r['psc']:r for r in codebook('2026-11-01')}

def test_reported_psc_is_not_a_technical_capability_or_equivalence_claim():
    a=annotate('Supply copper wire','5841',reference=REFERENCE)
    assert a['reported_psc']=='5841' and a['psc_label']
    assert a['topic_ids']==[] and a['scope_status']=='reported_psc_only'
    assert a['qualification']==a['technical_equivalence']=='not_established'
    b=annotate('NGJ-MB jamming system integration','5841',reference=REFERENCE)
    assert 'electronic-warfare' in b['topic_ids'] and 'radar' not in b['topic_ids']
    assert any(x['work_type']=='integration' for x in b['work_types'])

def test_specific_work_modes_and_source_passages_survive_normalization():
    repair=annotate('Repair of F-16 radar antennas','J584',reference=REFERENCE)
    production=annotate('Production of AESA radar arrays','5841',reference=REFERENCE)
    assert {'radar','radar.antennas'}<=set(repair['topic_ids'])
    assert repair['work_types']!=production['work_types']
    assert all(t['matched_phrase'] in t['passage'] for t in repair['topics'])
    assert annotate('We do not provide radar equipment','',reference=REFERENCE)['topic_ids']==[]
    assert 'aircraft-electrical-power' not in annotate('Diesel generator maintenance','6115',reference=REFERENCE)['topic_ids']

def test_meetings_and_negative_adjustments_do_not_prove_delivery():
    from opportunity_engine.company_enrichment import scope_terms
    assert administrative_only('MEUAS IV KICK-OFF MEETING')
    assert scope_terms('KICK-OFF MEETING')==[]
    assert annotate('MEUAS IV KICK-OFF MEETING','L015',reference=REFERENCE)['topic_ids']==[]
    assert evidence_verdict('KICK-OFF MEETING')[0]=='rejected'
    assert evidence_verdict('CABLE, FIBER OPTIC','prime_award',-1593,0,'CONT_AWD_OLD')[0]=='review'
    assert evidence_verdict('Contract vehicle scope','prime_award',0,0,'CONT_IDV_VALID')[0]=='unchanged'
    assert evidence_verdict('Radar repair','prime_award',100,150,'CONT_AWD_POSITIVE')[0]=='unchanged'

def test_dated_official_psc_reference_keeps_retired_nodes_and_source():
    assert len(REFERENCE)>3000
    assert all(len(r['source_sha256'])==64 and r['source_url'].startswith('https://www.acquisition.gov/') for r in REFERENCE.values())
    assert any(not r['active_in_reference'] for r in REFERENCE.values())

def test_market_research_and_business_development_are_not_engineering_deliverables():
    inquiry=annotate('This is market research to identify suppliers. Contact our business development office to procure a radar system.','5840',reference=REFERENCE)
    assert not any(m['work_type']=='design_development' for m in inquiry['work_types'])
    assert 'radar' in inquiry['topic_ids']
    technical=annotate('Engineering development and prototype radar integration','5840',reference=REFERENCE)
    assert any(m['work_type']=='design_development' for m in technical['work_types'])
    assert all(m['matched_phrase'] in m['passage'] for m in technical['work_types'])
    assert annotate('We do not provide repair services. Radar integration is required.','5840',reference=REFERENCE)['work_types'][0]['work_type']=='integration'

def test_component_scope_is_distinct_from_platform_and_mission_context():
    switch=annotate('SWITCH,PROXIMITY for use on Navy submarines. Requirements for manufacture.','5930',reference=REFERENCE)
    assert 'electronic-sensors.proximity-switches' in switch['work_scope_topic_ids']
    assert 'naval-platforms' in switch['context_topic_ids']
    assert 'shipbuilding' not in switch['topic_ids']
    assert switch['normalized_scope_label'].startswith('Proximity switches')
    context=annotate('Black Hawk helicopter programme','',reference=REFERENCE)
    assert context['scope_status']=='context_only' and not context['work_scope_topic_ids']
    detection=annotate('Radar detection of small unmanned aircraft systems','5840',reference=REFERENCE)
    assert 'uncrewed-aircraft' in detection['context_topic_ids']
    assert 'autonomy-robotics' not in detection['topic_ids']
    assert 'cloud-data-centres' not in annotate('Radar measurement of cloud cover','5840',reference=REFERENCE)['topic_ids']
    assert 'cloud-data-centres' in annotate('Cloud infrastructure migration services','D302',reference=REFERENCE)['topic_ids']

def test_whole_projection_filters_evidence_and_preserves_original_sources(tmp_path,monkeypatch):
    from opportunity_engine import vocabulary_refresh as vr
    c=duckdb.connect();tables={}
    def parquet(name,sql):
        p=tmp_path/(name+'.input.parquet');c.execute('COPY ('+sql+') TO ? (FORMAT PARQUET)',[str(p)])
        return p
    p=parquet('company_work_matches',"SELECT * FROM (VALUES ('m1','CAGE:02FP7','opp1','oh1','ph','e1','KICK-OFF MEETING','doc1','accepted','old reason','work_lead','old caveat','mh1',NULL::VARCHAR),('m2','CAGE:02FP7','opp2','oh2','ph','e2','CABLE FIBER OPTIC','doc2','accepted','old reason','work_lead','old caveat','mh2','5810'),('m3','CAGE:45934','opp3','oh3','ph','e3','FUEL BOOST PUMP','doc3','accepted','old reason','work_lead','old caveat','mh3','2915')) t(match_id,company_id,episode_id,opportunity_hash,profile_hash,sample_evidence_id,work_text,doc_id,fit_verdict,fit_reason,review_category,company_evidence_caveat,match_hash,observed_work_psc)")
    tables['company_work_matches']={'path':str(p),'table':'oi_test_work'}
    tables['company_matches']={'path':str(parquet('company_matches',"SELECT match_id,company_id,episode_id,opportunity_hash,match_hash,company_evidence_caveat FROM read_parquet('"+str(p)+"')"))}
    tables['company_match_assessments']={'path':str(parquet('assessments',"SELECT company_id,episode_id,fit_verdict verdict,fit_reason reason FROM read_parquet('"+str(p)+"')"))}
    tables['opportunities']={'path':str(parquet('opportunities',"SELECT * FROM (VALUES ('opp1','oh1','published_procurement','F-35 depot tooling','F-35 depot tooling','1510'),('opp2','oh2','published_procurement','Fiber optic cable','Fiber optic cable','5810'),('opp3','oh3','new_prime_historical_supplier_flow','FUEL BOOST PUMP','FUEL BOOST PUMP Current funded scope example: radar and avionics integration','2915')) t(episode_id,record_hash,lane,title,scope_text,psc)"))}
    cap=parquet('capabilities',"SELECT company_id,doc_id capability_id,work_text,observed_work_psc psc FROM read_parquet('"+str(p)+"')")
    facts=parquet('facts',"SELECT * REPLACE(cast(net_obligations AS DOUBLE) AS net_obligations,cast(positive_obligations AS DOUBLE) AS positive_obligations,cast(reported_subcontract_amount AS DOUBLE) AS reported_subcontract_amount),NULL::VARCHAR first_action_date,NULL::VARCHAR latest_action_date,'https://www.usaspending.gov/award/test/'::VARCHAR source_url FROM (VALUES ('CAGE:02FP7','e1','e1','prime_award','CONT_AWD_MEETING','MEUAS IV KICK-OFF MEETING',1023.72,1500.,NULL::DOUBLE,'L015'),('CAGE:02FP7','e2','e2','prime_award','CONT_AWD_CABLE','CABLE FIBER OPTIC',-1593.,0.,NULL::DOUBLE,'5810'),('CAGE:45934','e3','e3','subcontract_report','REPORT_PUMP','FUEL BOOST PUMP',NULL::DOUBLE,NULL::DOUBLE,250000.,NULL::VARCHAR)) t(company_id,sample_evidence_id,evidence_id,history_kind,record_id,work_text,net_obligations,positive_obligations,reported_subcontract_amount,psc)")
    c.close()
    def source(p):return {'files':[dict(s3_uri='s3://'+vr.BUCKET+'/'+vr.PREFIX+'/'+p.name,sha256=hashlib.sha256(p.read_bytes()).hexdigest())],'query_ids':['source-proof']}
    tables.update(company_capability_history={},company_award_facts={'table':'oi_prime'},company_subcontract_facts={'table':'oi_sub'})
    (tmp_path/'manifest.json').write_text(json.dumps(dict(run_id='test',as_of='2026-11-01',tables=tables,sources={'company_capability_history':source(cap)})))
    class S3:
        def download_file(self,bucket,key,dest):shutil.copy(tmp_path/key.rsplit('/',1)[-1],dest)
    class AWS:
        s3=S3()
        def upload(self,*args):pass
        def register(self,*args):pass
    monkeypatch.setattr(vr,'remote',lambda *args,**kwargs:source(facts))
    vr.augment(tmp_path,AWS());c=duckdb.connect()
    assert c.execute('SELECT match_id FROM read_parquet(?) ORDER BY match_id',[str(tmp_path/'company_matches.parquet')]).fetchall()==[('m2',),('m3',)]
    assert c.execute("SELECT fit_verdict,review_category FROM read_parquet(?) WHERE match_id='m2'",[str(tmp_path/'company_work_matches.parquet')]).fetchone()==('review','market_context')
    assert c.execute('SELECT count(*) FROM read_parquet(?)',[str(tmp_path/'company_match_vocabulary.parquet')]).fetchone()[0]==3
    label=c.execute("SELECT topic_ids_json FROM read_parquet(?) WHERE episode_id='opp3'",[str(tmp_path/'opportunity_vocabulary.parquet')]).fetchone()[0]
    assert 'radar' not in json.loads(label) and 'fuel-systems' in json.loads(label)
    assert c.execute('SELECT count(*) FROM read_parquet(?)',[str(tmp_path/'company_match_evidence_reviews.parquet')]).fetchone()[0]==3
    from opportunity_engine.vocabulary_gates import validate
    m=json.loads((tmp_path/'manifest.json').read_text());m['tables']['company_capability_history']['rows']=3;m['tables']['opportunities']['rows']=3
    for name,t in m['tables'].items():
        if t.get('path'):c.read_parquet(t['path']).create_view(name,replace=True)
    checks=[];validate(c,m,lambda name,ok:checks.append((name,bool(ok))))
    assert all(ok for _,ok in checks),checks
