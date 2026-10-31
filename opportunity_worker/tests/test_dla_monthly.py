import json,zipfile
import duckdb
import pytest
from opportunity_engine.dla_monthly import HEADERS,compile_sources


def fixture(tmp_path,orphan=False):
 rows={
 'forecast':['000000001|4|202611|U','LLBF00006|3|202611|U','000000002|5|202611|J'],
 'inventory':['1234000000001|0|U|30SEP2026:00:00:00.000000|A|2|10'],
 'reorder':['1234000000001|0|2|U|30SEP2026:00:00:00.000000|A'],
 'procurement':['000000001|U|1234||EA|45934|CONTRACT|PARENT|2.000|20260910|10.00|12.00|PO1|00001|MOD|'],
 'parts':['000000001|U|1234|EA|45934|CONTRACT|2.000|20260910|10.00|PO1|00001|Widget|PART-A|12.00|1234000000001','000000001|U|1234|EA|45934|CONTRACT|2.000|20260910|10.00|PO1|00001|Widget|PART-B|12.00|1234000000001']}
 if orphan:rows['parts']=[x.replace('PO1','WRONG') for x in rows['parts']]
 archives={}
 for n,values in rows.items():
  p=tmp_path/(n+'.zip')
  with zipfile.ZipFile(p,'w') as z:z.writestr(n+'.txt',HEADERS[n]+'\n'+'\n'.join(values)+'\n')
  archives[n]={'path':str(p),'url':'https://www.dla.mil/'+n+'.zip'}
 return archives


def test_monthly_physical_prices_do_not_add_part_mappings_and_unknown_ids_stay_unmapped(tmp_path):
 out=compile_sources(fixture(tmp_path),tmp_path/'out','2026-10-19-0949','2026-10-31');c=duckdb.connect()
 assert out['dla_forecasts']['rows']==2
 assert c.execute('SELECT identifier_type FROM read_parquet(?) WHERE niin=?',[out['dla_forecasts']['path'],'LLBF00006']).fetchone()[0]=='unmapped_source_item_identifier'
 q=c.execute('SELECT quantity,unit_price_usd,record_amount_usd,part_number FROM read_parquet(?)',[out['item_market_price_lines']['path']]).fetchall();assert q==[(2,10,20,None)]
 assert c.execute('SELECT snapshot_date,annual_demand_quantity FROM read_parquet(?)',[out['dla_inventory']['path']]).fetchone()==('2026-09-30','10')
 assert json.loads((tmp_path/'out/source-validation.json').read_text())['forecast']['unmapped_source_item_rows']==1


def test_incoherent_procurement_and_parts_snapshot_is_not_published(tmp_path):
 with pytest.raises(ValueError,match='do not reconcile'):compile_sources(fixture(tmp_path,True),tmp_path/'out','2026-10-19-0949','2026-10-31')
