import json
import duckdb
import pytest
from opportunity_engine.notice_prices import build
from opportunity_engine.company_enrichment import nsns

@pytest.mark.parametrize('extra,government,supplier,unit',[
 ("",84079.48,250.0,'EA'),
 (" UNION ALL SELECT * REPLACE('12345' AS cage_code,'other-supplier' AS evidence_id) FROM seed",84079.48,None,'EA'),
 (" UNION ALL SELECT * REPLACE('BX' AS unit_of_issue,'box' AS evidence_id) FROM seed",None,None,None),
 (" UNION ALL SELECT * REPLACE(NULL::VARCHAR AS unit_of_issue,'missing-unit' AS evidence_id) FROM seed",None,None,None),
 (" UNION ALL SELECT * REPLACE(90000.0 AS govt_estimated_price,'different-price' AS evidence_id) FROM seed",None,250.0,'EA'),
])
def test_global_notice_prices_preserve_supplier_provenance_and_suppress_ambiguous_prices(extra,government,supplier,unit):
 c=duckdb.connect();c.create_function('scope_nsns',nsns,['VARCHAR'],duckdb.list_type('VARCHAR'))
 c.execute("CREATE TEMP TABLE base_opportunities AS SELECT 'op' episode_id,'hash' record_hash,'published_procurement' lane,'Generator' title,'NSN 6115-01-270-4801 Estimated Spares Requirement: 9EA' scope_text")
 c.execute("CREATE TEMP TABLE base_demand_decisions AS SELECT 'op' episode_id,'qualification_opening' stage")
 c.execute("CREATE TEMP TABLE matches AS SELECT 'CAGE:45934' company_id,'op' episode_id,'hash' opportunity_hash")
 c.execute("CREATE TEMP TABLE seed AS SELECT '62983' cage_code,'012704801' niin,'EA' unit_of_issue,84079.48::DOUBLE govt_estimated_price,250.0::DOUBLE supplier_average_price,'2026-01-01' last_sold_date,'source-row' evidence_id,'418446' part_number,'GENERATOR' description,'snapshot' source_snapshot_sha256,5::BIGINT source_row")
 c.execute('CREATE TEMP VIEW item_reference_facts AS SELECT * FROM seed'+extra)
 result={}
 def output(name,sql):result[name]=c.execute(sql).fetchall()
 build(c,output,'2026-10-30','matches');row=result['company_notice_item_prices'][0]
 assert row[0]=='CAGE:45934';item=json.loads(row[3])[0]
 assert item['government_reference_unit_price']==government
 assert item['supplier_average_unit_price']==supplier
 assert item['catalogue_unit']==unit
 assert item['government_reference_lot_scenario']==(round(9*government,2) if government is not None else None)
 assert item['source_quantity']==(9 if unit=='EA' else None)
 assert item['supplier_average_lot_scenario']==(round(9*supplier,2) if supplier is not None else None)
 assert any(row['reference_cage']=='62983' for row in item['catalogue_evidence'])
 assert 'qualification' in row[4] and 'current quotes' in row[4]
 c.close()

@pytest.mark.parametrize('title,psc,body,total',[
 ('Repair generator','J061','NSN 6115012704801 Quantity Required: 3 EA',None),
 ('Generator','J061','NSN 6115012704801 Quantity Required: 3 EA',None),
 ('Generator','6115','Proposed procurement for NSN 6115012704801 Generator: Line 0001 Qty 3 UI EA. Aggregate total of orders reach 250.00.',None),
 ('Generator','6115','Proposed procurement for NSN 6115012704801 Generator: Line 0001 Qty 3 UI EA.',300),
])
def test_new_item_prices_do_not_value_repairs_or_exceed_a_stated_ceiling(title,psc,body,total):
 c=duckdb.connect();c.create_function('scope_nsns',nsns,['VARCHAR'],duckdb.list_type('VARCHAR'))
 c.execute('CREATE TEMP TABLE base_opportunities AS SELECT ?::VARCHAR episode_id,?::VARCHAR record_hash,?::VARCHAR lane,?::VARCHAR title,?::VARCHAR psc,?::VARCHAR scope_text',['op','hash','published_procurement',title,psc,body])
 c.execute("CREATE TEMP TABLE base_demand_decisions AS SELECT 'op' episode_id,'published_requirement' stage")
 c.execute("CREATE TEMP TABLE matches AS SELECT 'CAGE:45934' company_id,'op' episode_id,'hash' opportunity_hash")
 c.execute("CREATE TEMP TABLE item_reference_facts AS SELECT '45934' cage_code,'012704801' niin,'EA' unit_of_issue,100.0::DOUBLE govt_estimated_price,NULL::DOUBLE supplier_average_price,NULL::VARCHAR last_sold_date,'source' evidence_id,'part' part_number,'Generator' description,'snapshot' source_snapshot_sha256,1::BIGINT source_row")
 out={}
 def output(name,sql):out[name]=c.execute(sql).fetchall()
 build(c,output,'2026-10-31','matches');item=json.loads(out['company_notice_item_prices'][0][3])[0]
 assert item['government_reference_unit_price']==100
 assert item['government_reference_lot_scenario']==total
 c.close()
