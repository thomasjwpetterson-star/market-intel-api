import json
from pathlib import Path
import duckdb
from opportunity_engine.replenishment import prepare,augment


def test_annual_outlook_never_overrides_source_forecast_or_conflicting_stale_inventory(tmp_path):
 c=duckdb.connect();sources={}
 def source(name,sql):
  p=tmp_path/(name+'.parquet');c.execute('COPY ('+sql+') TO ? (FORMAT PARQUET)',[str(p)]);sources[name]={'files':[{'path':str(p)}]}
 source('dla_inventory',"SELECT * FROM (VALUES ('000000001','2026-08-31',10.,0.,2.,true),('000000002','2026-08-31',10.,0.,2.,true),('000000003','2026-08-31',10.,0.,2.,true),('000000004','2026-06-01',10.,0.,2.,true),('000000005','2026-08-31',10.,0.,2.,true),('000000005','2026-08-31',20.,0.,2.,true),('000000006','2026-08-31',10.,20.,0.,true),('000000007','2026-08-31',10.,0.,2.,false),('000000008','2026-08-31',10.,0.,2.,true)) v(niin,snapshot_date,annual_demand_quantity,total_stock,backorder_qty,public_release_eligible) CROSS JOIN (SELECT '1234000000001' nsn,'A' condition_code,'release' source_release,'file' source_file)")
 source('dla_forecasts',"SELECT * FROM (VALUES ('000000002','2026-11-01',0.),('000000003','2026-11-01',-1.)) t(niin,forecast_month,forecast_qty)")
 source('item_price_references',"SELECT '00000000'||cast(i AS VARCHAR) niin,CASE WHEN i=8 THEN 'BX' ELSE 'EA' END unit,CASE WHEN i=8 THEN 'BX' ELSE 'EA' END catalogue_unit,100::DOUBLE government_reference_unit_price,80::DOUBLE market_price_median,70::DOUBLE market_price_low,90::DOUBLE market_price_high,2::BIGINT market_observations,1::BIGINT market_sellers,'2026-01-01' first_price_date,'2026-07-01' latest_price_date,'[]' price_preview_json,'reference' sample_reference_evidence,'sha' source_snapshot_sha256 FROM range(1,9) t(i)")
 source('item_reference_facts',"SELECT '00000000'||cast(i AS VARCHAR) niin,'Widget' description,'1234000000001' nsn,'2' rnvc_codes FROM range(1,9) t(i)")
 schemas=json.loads((Path(__file__).parent/'fixtures/validation_schemas.json').read_text());types={'string':'VARCHAR','double':'DOUBLE','bigint':'BIGINT','boolean':'BOOLEAN'}
 source('demand_candidates','SELECT '+','.join('NULL::'+types[x['Type']]+' "'+x['Name']+'"' for x in schemas['opportunities'])+' WHERE false')
 out=prepare(sources,tmp_path/'out','2026-10-31')
 assert out['item_replenishment_context']['rows']==1
 q=c.execute('SELECT * FROM read_parquet(?)',[out['item_replenishment_context']['path']]);row=dict(zip([x[0] for x in q.description],q.fetchone()))
 assert row['niin']=='000000001' and row['annual_demand_quantity']==10
 assert row['market_price_lot_scenario']==800 and row['government_reference_lot_scenario']==1000
 assert row['window_end']=='2027-10-30'
 q=c.execute('SELECT * FROM read_parquet(?)',[out['demand_candidates']['path']]);d=dict(zip([x[0] for x in q.description],q.fetchone()));inputs=json.loads(d['value_inputs_json'])
 assert d['value_median'] is None and inputs['company_addressable_value'] is None
 assert inputs['source_forecast_present'] is False and 'not a source forecast' in row['interpretation']
 # Compatible annual value evidence is separate from monthly source forecasts.
 tables={'item_replenishment_context':{'path':out['item_replenishment_context']['path']}}
 for name,sql in [('company_matches',"SELECT 'CAGE:45934' company_id,'"+row['episode_id']+"' episode_id,'"+row['record_hash']+"' opportunity_hash"),('company_item_valuations',"SELECT NULL::VARCHAR company_id,NULL::VARCHAR episode_id,NULL::VARCHAR record_hash,NULL::VARCHAR niin,NULL::DOUBLE quantity,NULL::VARCHAR valuation_status,NULL::DOUBLE market_price_lot_scenario,NULL::VARCHAR monthly_evidence_json,NULL::BIGINT month_count WHERE false")]:
  p=tmp_path/'out'/(name+'.parquet');c.execute('COPY ('+sql+') TO ? (FORMAT PARQUET)',[str(p)]);tables[name]={'path':str(p)}
 (tmp_path/'out/manifest.json').write_text(json.dumps({'tables':tables}));augment(tmp_path/'out')
 actual=c.execute('SELECT valuation_status,market_price_lot_scenario,monthly_evidence_json FROM read_parquet(?)',[str(tmp_path/'out/company_item_valuations.parquet')]).fetchone()
 assert actual[0]=='reported_annual_usage_EA_assumed_not_source_forecast' and actual[1]==800
 assert 'not a source monthly forecast' in actual[2]
