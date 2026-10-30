import hashlib,json
import duckdb
from opportunity_engine import quality_core as q
from opportunity_engine.quality import apply


def test_rejects_actual_false_positive_history_but_retains_specific_work():
 cases=[('BRAGG, NORTH CAROLINA.','Medical training at Fort Bragg North Carolina'),('AND IT EDUCATION AND TRAINING.','Architectural repairs and fire alarm training facilities'),('RESEARCH AND DEVELOPMENT','New optical radar research and development'),('PURCHASE OF MOBILE COMPUTERS','Digital Forensics Examiner Services'),('MODIFICATION TO REDUCE COST','Reusable Illumination System'),('CUSTOM COMPUTER PROGRAMMING SERVICES','Depot Maintenance Applications Consolidated3'),('FORM FIT','M4 Trainer Weapons'),('SUPPLIES/SERVICES, COST REIMBURSEMENT','Coverity Renewal'),('SOFTWARE UPGRADES','Blood bank disease testing'),('PROCUREMENT OF WEST COAST IT SERVICES','Jones Act Tanker Time Charter')]
 for work,title in cases:
  assert q.fit_review(work,title,work,'published_procurement','source_specific_route','sam_notices','45934')[0]=='rejected'
 assert not q.administrative_scope('WASHER, FL')
 assert q.administrative_scope('ALEXANDRIA, VA 22314')
 assert q.fit_review('FUEL BOOST PUMP','Repair aircraft fuel boost pump','Seeking sources with pump repair capability','published_procurement','source_specific_route','sam_notices','45934')[0]=='review'
 assert q.fit_review('FUEL BOOST PUMP REPAIR','Repair aircraft fuel boost pump','Seeking sources with pump repair capability','published_procurement','source_specific_route','sam_notices','45934')[0]=='accepted'
 assert q.fit_review('C130 mounting base','Potential supplier package: Mounting base','Current funded scope: C130 mounting base','new_prime_historical_supplier_flow','reported_supplier_relationship','supplier_flow','45934')[0]=='accepted'


def test_notice_coherence_and_body_deadline_do_not_inherit_fresh_posted_date():
 body='NSN: 7HH 5990 017271903 NOM: CIRCUIT CARD ASSEMB P/N: 802-05-0309-02/EB4143-H-02 Approved Design Cage Code: Lockheed Martin Corporation (CAGE 12050). Non-OEM sources must provide traceability information tracing material back to the OEM. Return the completed market survey via email by 2:00 PM EST on 18 August 2025.'
 v=q.notice_quality('59 - ENCODER,SHAFT ANGLE',body,None,'2026-10-30','sam_notices')
 assert v['verdict']=='source_review_required'
 assert 'past response deadline' in v['reason'] and 'different product families' in v['reason']
 assert v['body_deadline']=='2025-08-18'
 assert v['category_override']=='items'
 assert q.fit_review('CIRCUIT CARD ASSEMBLY','Circuit card assembly',body,'published_procurement','source_specific_route','sam_notices','45934')[0]=='review'
 assert q.fit_review('CIRCUIT CARD ASSEMBLY','Circuit card assembly',body,'published_procurement','source_specific_route','sam_notices','12050')[0]=='accepted'
 assert '80205030902' in q.required_parts('',body)
 assert q.notice_quality('Circuit card assembly',body,'2026-12-01','2026-10-30','sam_notices')['body_deadline']=='2025-08-18'


def test_sbir_relevance_is_not_eligibility_and_source_inquiry_is_not_new_qualification():
 assert q.fit_review('Optical laser illumination','Reusable laser illumination system','Research topic','published_procurement','source_specific_route','sbir_topics','45934')[0]=='review'
 body='No substitutes permitted. Source approval requests for alternate manufacturers will be considered.'
 assert q.explicit_qualification(body)
 assert q.notice_quality('Fuel boost pump',body,'2027-01-01','2026-10-30','sam_notices')['source_entry_kind']=='new_source_qualification'


def test_quality_pipeline_keeps_evidence_and_filters_serving_edges(tmp_path):
 c=duckdb.connect();tables={}
 def put(name,sql):
  path=tmp_path/(name+'.parquet');c.execute("COPY ("+sql+") TO '"+str(path)+"' (FORMAT PARQUET)");tables[name]={'path':str(path)}
 put('opportunities',"""SELECT 'good' episode_id,'h1' record_hash,'Fuel boost pump repair' title,'Seeking fuel pump repair capability' scope_text,NULL::VARCHAR window_end,'sam_notices' source_name,'e1' source_evidence_id,'https://sam.gov/good' source_url UNION ALL SELECT 'bad','h2','Digital Forensics Examiner Services','Purchase of mobile computers',NULL,'sam_notices','e2','https://sam.gov/bad' UNION ALL SELECT 'stale','h3','Circuit card assembly','Return response by 18 August 2025',NULL,'sam_notices','e3','https://sam.gov/stale'""")
 put('demand_decisions',"SELECT episode_id,record_hash source_record_hash,'PR13' rule_id,'qualification_opening' stage,'oldhash' decision_hash,'{\"stage\":\"qualification_opening\",\"unresolved\":[],\"negative_evidence\":[]}' decision_json FROM read_parquet('"+str(tmp_path/'opportunities.parquet')+"')")
 put('company_matches',"""SELECT episode_id match_id,'CAGE:45934' company_id,episode_id,90.0 score,'work-route' match_route,'terms' matched_terms,'caveat' company_evidence_caveat,'evidence' sample_evidence_id,'matchhash' match_hash,record_hash opportunity_hash,'profilehash' profile_hash FROM read_parquet('"""+str(tmp_path/'opportunities.parquet')+"')")
 put('company_work_matches',"""SELECT *,episode_id doc_id,CASE episode_id WHEN 'good' THEN 'FUEL BOOST PUMP' WHEN 'bad' THEN 'PURCHASE OF MOBILE COMPUTERS' ELSE 'CIRCUIT CARD ASSEMBLY' END work_text,'prime_award' source_kind,'2026-01-01' last_observed,'' work_context,'work_lead' review_category,'source_specific_route' company_entry_status FROM read_parquet('"""+str(tmp_path/'company_matches.parquet')+"')")
 put('company_scope_features',"SELECT 'CAGE:45934' company_id,'good' doc_id,'J043' psc,'811310' naics")
 c.close();(tmp_path/'manifest.json').write_text(json.dumps(dict(tables=tables,sources={},as_of='2026-10-30',logic_sha256='old')));apply(tmp_path);c=duckdb.connect()
 assert c.execute('SELECT episode_id FROM read_parquet(?) ORDER BY episode_id',[str(tmp_path/'company_matches.parquet')]).fetchall()==[('good',),('stale',)]
 assert c.execute("SELECT stage FROM read_parquet(?) WHERE episode_id='stale'",[str(tmp_path/'demand_decisions.parquet')]).fetchone()[0]=='evidence_gap'
 assert c.execute("SELECT work_text,verdict FROM read_parquet(?) WHERE episode_id='bad'",[str(tmp_path/'company_match_assessments.parquet')]).fetchone()==('PURCHASE OF MOBILE COMPUTERS','rejected')
 assert c.execute('SELECT count(*) FROM read_parquet(?)',[str(tmp_path/'opportunity_quality_reviews.parquet')]).fetchone()[0]==3
 assert c.execute("SELECT observed_work_psc,observed_delivery_mode FROM read_parquet(?) WHERE episode_id='good'",[str(tmp_path/'company_match_assessments.parquet')]).fetchone()==('J043','maintenance_repair')
 assert c.execute("DESCRIBE SELECT body_deadline FROM read_parquet(?)",[str(tmp_path/'opportunity_quality_reviews.parquet')]).fetchone()[1]=='VARCHAR'
 c.close();apply(tmp_path);c=duckdb.connect()
 d=json.loads(c.execute("SELECT decision_json FROM read_parquet(?) WHERE episode_id='stale'",[str(tmp_path/'demand_decisions.parquet')]).fetchone()[0]);assert len(d['negative_evidence'])==1


def test_exact_source_codes_require_named_part_and_nonconflicting_authorization(tmp_path):
 c=duckdb.connect();tables={}
 def put(name,sql):
  p=tmp_path/(name+'.parquet');c.execute("COPY ("+sql+") TO '"+str(p)+"' (FORMAT PARQUET)");tables[name]={'path':str(p)}
 put('opportunities',"SELECT 'inquiry' episode_id,'requirement-hash' record_hash,'Circuit card assembly' title,'NSN 5990017271903 NOM: CIRCUIT CARD ASSEMB P/N: AA-12 Approved source required. Non-OEM suppliers must provide traceability to OEM.' scope_text,'2027-01-01' window_end,'sam_notices' source_name,'e' source_evidence_id,'https://sam.gov/notice' source_url")
 put('demand_decisions',"SELECT 'inquiry' episode_id,'requirement-hash' source_record_hash,'PR13' rule_id,'qualification_opening' stage,'dh' decision_hash,'{\"stage\":\"qualification_opening\"}' decision_json")
 put('company_matches',"SELECT cage match_id,'CAGE:'||cage company_id,'inquiry' episode_id,90.0 score,'work-route' match_route,'circuit,card' matched_terms,'caveat' company_evidence_caveat,'evidence' sample_evidence_id,'mh' match_hash,'requirement-hash' opportunity_hash,'ph' profile_hash FROM (VALUES ('45934'),('07639'),('12050')) n(cage)")
 put('company_work_matches',"SELECT *,match_id doc_id,'CIRCUIT CARD ASSEMBLY' work_text,'prime_award' source_kind,'2026-01-01' last_observed,'' work_context,'work_lead' review_category,'source_specific_route' company_entry_status FROM read_parquet('"+str(tmp_path/'company_matches.parquet')+"')")
 put('catalogue',"SELECT cage_code,'017271903' niin,part_number,'3' rncc_codes,rnvc_codes,'A' rnsc_codes,cage_code evidence_id,'snapshot' source_snapshot_sha256,1::BIGINT source_row FROM (VALUES ('45934','AA-12','2'),('07639','AA-12','2,9'),('12050','DIFFERENT','2')) v(cage_code,part_number,rnvc_codes)")
 catalogue=tables.pop('catalogue');c.close();(tmp_path/'manifest.json').write_text(json.dumps(dict(tables=tables,sources={'item_reference_facts':{'files':[catalogue]}},as_of='2026-10-30',logic_sha256='old')));apply(tmp_path);c=duckdb.connect()
 assert c.execute('SELECT company_id FROM read_parquet(?)',[str(tmp_path/'company_notice_source_evidence.parquet')]).fetchall()==[('CAGE:45934',)]
 result=dict(c.execute('SELECT company_id,review_category FROM read_parquet(?)',[str(tmp_path/'company_work_matches.parquet')]).fetchall())
 assert result=={'CAGE:45934':'work_lead','CAGE:07639':'market_context','CAGE:12050':'market_context'}


def test_portable_quality_review_fetches_cohort_files_for_full_release_gates(tmp_path,monkeypatch):
 from opportunity_engine import quality
 source=tmp_path/'cohort.parquet';c=duckdb.connect();c.execute("COPY (SELECT 1 awards) TO '"+str(source)+"' (FORMAT PARQUET)");c.close();sha=hashlib.sha256(source.read_bytes()).hexdigest()
 previous={'run_id':'prior','sources':{'contract_value_cohorts':{'files':[{'s3_uri':'s3://a-and-d-intel-lake-newaccount/governance/opportunity_intelligence/cohort.parquet','sha256':sha}]}},'tables':{'contract_value_cohorts':{'remote_binding':True,'source_binding':'contract_value_cohorts'}}}
 class S3:
  def download_file(self,bucket,key,path):
   Path=__import__('pathlib').Path;Path(path).write_bytes(source.read_bytes())
 class AWS:s3=S3()
 monkeypatch.setattr(quality,'apply',lambda directory:None)
 out=quality.review_current(previous,tmp_path/'review',AWS());m=json.loads((out/'manifest.json').read_text());f=m['sources']['contract_value_cohorts']['files'][0]
 assert __import__('pathlib').Path(f['path']).is_file()
 assert hashlib.sha256(__import__('pathlib').Path(f['path']).read_bytes()).hexdigest()==sha
 assert m['reuse_parent_release']=='prior'


def test_component_supply_does_not_establish_facility_or_shipyard_service_capability():
 from opportunity_engine.delivery_modes import required_mode,mode_fit
 rejected=q.fit_review('4610038458!TOILET ASSEMBLY,AIR','(AFDW/CCP) Toilet Partition Replacement','A construction requirement to remove old partitions and install new toilet partitions at a conference center.','published_procurement','source_specific_route','sam_notices','45934',work_psc='4610',target_psc='Z2AB')
 assert rejected[0]=='rejected'
 body='This contract shall provide on-site wire rope replacement services for a naval shipyard.'
 for title in ['Wire Rope Replacement Services at PSNS','Wire Rope Replacement Sources Sought Bangor']:
  verdict,reason=q.fit_review('8510338769!WIRE ROPE ASSEMBLY,',title,body,'published_procurement','source_specific_route','sam_notices','45934',work_psc='4010',target_psc='J040')
  assert verdict=='review' and 'onsite services' in reason and 'component supply' in reason
  assert q.fit_review('ON-SITE WIRE ROPE REPLACEMENT SERVICES',title,body,'published_procurement','source_specific_route','sam_notices','OTHER',work_psc='J040',target_psc='J040')[0]=='accepted'
  assert q.fit_review('WIRE ROPE ASSEMBLY',title,body,'published_procurement','reported_supplier_relationship','sam_notices','OTHER',work_psc='4010',target_psc='J040')[0]=='review'
 assert required_mode('Installation kit for fuel boost pump','','2915',True)[0]=='component_supply'
 assert mode_fit('FUEL BOOST PUMP','maintenance_repair','J043')[0] is True
 assert q.fit_review('TOILET PARTITION INSTALLATION','Toilet Partition Replacement','Install new partitions in a building.','published_procurement','source_specific_route','sam_notices','OTHER',work_psc='Z2AB',target_psc='Z2AB')[0]=='accepted'

def test_single_product_noun_and_reverse_service_to_manufacture_are_not_direct_matches():
 assert q.fit_review('VALVE, PRESSURE','Hydraulic control valve','Generic valve family','published_procurement','source_specific_route','sam_notices','OTHER')[0]=='rejected'
 assert q.fit_review('REPAIR FUEL BOOST PUMP','Fuel boost pump','New manufacturing of pumps','published_procurement','source_specific_route','sam_notices','OTHER',work_psc='J043',target_psc='2915')[0]=='review'
 assert q.fit_review('FUEL BOOST PUMP','Research prototype for fuel boost pump','New research design','published_procurement','source_specific_route','sam_notices','OTHER',work_psc='2915',target_psc='AC12')[0]=='accepted'
