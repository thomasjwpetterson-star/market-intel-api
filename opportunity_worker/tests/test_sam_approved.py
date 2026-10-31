import hashlib,json
import duckdb
from opportunity_engine.sam_approved import approvals,augment
from opportunity_engine.notice_quantity import quantity_fact

def row(**kw):return dict(source_name='sam_notices',title='29--LEVER ASSEMBLY,FUEL CO',psc='2915',scope_text='Proposed procurement for NSN 2915005864023 LEVER ASSEMBLY: Line 0001 Qty 307 UI EA Deliver To: Depot. Approved sources are 45934 519296; 73030 519296. The solicitation is an RFQ.',window_end='2026-11-04',**kw)

def test_current_agency_approval_is_a_tuple_not_a_bare_company_mention():
 out=approvals(row(),'2026-10-31');assert [(x['company_id'],x['part_key']) for x in out]==[('CAGE:45934','519296'),('CAGE:73030','519296')]
 assert out[0]['niin']=='005864023' and 'Approved sources are' in out[0]['approval_quote']
 r=row();r['scope_text']=r['scope_text'].replace('Approved sources are','Contact information:');assert approvals(r,'2026-10-31')==[]
 r=row();r['title']='Repair lever assembly';assert approvals(r,'2026-10-31')==[]
 r=row();r['scope_text']+=' Another item NSN 2915003538074';assert approvals(r,'2026-10-31')==[]

def test_structured_dla_line_quantity_is_not_the_idc_minimum_or_ceiling():
 r=row();r['scope_text']+=' Aggregate total of orders reach 350000.00. Guaranteed Minimum quantity will be 4.'
 q=quantity_fact(r);assert q['notice_quantity']==307 and q['notice_quantity_unit']=='EA'
 assert q['notice_quantity_evidence']=='Line 0001 Qty 307 UI EA'
 r['scope_text']+=' Line 0002 Qty 5 UI EA';assert quantity_fact(r)=={}

def test_approved_notice_links_require_the_same_company_niin_and_part(tmp_path):
 c=duckdb.connect();tables={}
 def put(n,q):
  p=tmp_path/(n+'.parquet');c.execute('COPY ('+q+') TO ? (FORMAT PARQUET)',[str(p)]);tables[n]={'path':str(p)}
 r=row();body=r['scope_text'].replace("'","''")
 put('opportunities',"SELECT 'episode' episode_id,'hash' record_hash,'sam_notices' source_name,'29--LEVER ASSEMBLY,FUEL CO' title,'2915' psc,'"+body+"' scope_text,'2026-11-04' window_end,'notice-evidence' source_evidence_id,'https://sam.gov/notice' source_url,'published_procurement' lane,'SPE4A727Q0117' native_identifier,'{}' value_inputs_json")
 put('demand_decisions',"SELECT 'episode' episode_id,'published_requirement' stage")
 put('company_profiles',"SELECT 'CAGE:45934' company_id,'profile' profile_hash UNION ALL SELECT 'CAGE:73030','other'")
 put('company_matches',"SELECT NULL::VARCHAR match_id,NULL::VARCHAR company_id,NULL::VARCHAR episode_id,NULL::DOUBLE score,NULL::VARCHAR match_route,NULL::VARCHAR matched_terms,NULL::VARCHAR company_evidence_caveat,NULL::VARCHAR sample_evidence_id,NULL::VARCHAR match_hash,NULL::VARCHAR opportunity_hash,NULL::VARCHAR profile_hash WHERE false")
 put('company_item_matches','SELECT *,NULL::VARCHAR niin,NULL::VARCHAR reference_description,NULL::VARCHAR reference_part_number FROM read_parquet(\''+tables['company_matches']['path']+'\')')
 put('company_work_matches','SELECT company_id,episode_id,opportunity_hash FROM read_parquet(\''+tables['company_matches']['path']+'\')')
 put('refs',"SELECT '45934' cage_code,'005864023' niin,'519296' part_number,'Lever assembly' description,'reference' evidence_id,'3' rncc_codes,'2' rnvc_codes,'A' rnsc_codes,'snapshot' source_snapshot_sha256,1::BIGINT source_row UNION ALL SELECT '73030','005864023','WRONG-PART','Other','wrong','3','2','A','snapshot',2")
 m=dict(as_of='2026-10-31',tables={n:t for n,t in tables.items() if n!='refs'},sources={'item_reference_facts':{'files':[{'path':tables['refs']['path']}]}});(tmp_path/'manifest.json').write_text(json.dumps(m));assert augment(tmp_path)==1
 m=json.loads((tmp_path/'manifest.json').read_text());out=c.execute('SELECT company_id,match_route,agency_approval_part_number FROM read_parquet(?)',[m['tables']['company_item_matches']['path']]).fetchall();assert out==[('CAGE:45934','agency_named_approved_source-v1','519296')]
 assert augment(tmp_path)==1
 m=json.loads((tmp_path/'manifest.json').read_text());assert m['tables']['company_matches']['rows']==1
 # A SAM/DIBBS copy and a trailing -00 notice remain one company prospect,
 # while every current agency approval remains in the evidence ledger.
 c.execute('CREATE TEMP TABLE op_snapshot AS SELECT * FROM read_parquet(?)',[tables['opportunities']['path']])
 c.execute('CREATE TEMP TABLE match_snapshot AS SELECT * FROM read_parquet(?)',[m['tables']['company_matches']['path']])
 put('opportunities',"SELECT * FROM op_snapshot UNION ALL SELECT * REPLACE('dibbs' AS episode_id,'dibbs-hash' AS record_hash,'dla_solicitations' AS source_name,'published_item_procurement' AS lane,'{\"niin\":\"005864023\"}' AS value_inputs_json) FROM op_snapshot UNION ALL SELECT * REPLACE('sam2' AS episode_id,'sam2-hash' AS record_hash,'SPE4A727Q0117-00' AS native_identifier) FROM op_snapshot")
 put('demand_decisions',"SELECT id episode_id,'published_requirement' stage FROM (VALUES ('episode'),('dibbs'),('sam2')) t(id)")
 put('company_matches',"SELECT * FROM match_snapshot UNION ALL SELECT * REPLACE('dibbs' AS episode_id,'dibbs-hash' AS opportunity_hash,'old-dibbs-match' AS match_id) FROM match_snapshot")
 for name in ('opportunities','demand_decisions','company_matches'):m['tables'][name]=tables[name]
 (tmp_path/'manifest.json').write_text(json.dumps(m));augment(tmp_path);m=json.loads((tmp_path/'manifest.json').read_text())
 assert c.execute('SELECT company_id,episode_id,opportunity_hash FROM read_parquet(?)',[m['tables']['company_matches']['path']]).fetchall()==[('CAGE:45934','dibbs','dibbs-hash')]
 assert m['tables']['company_approved_notice_sources']['rows']==2
