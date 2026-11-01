import json
import duckdb
from opportunity_engine.request_values import reference_request_value_sql


def test_reference_values_are_withheld_on_ceiling_conflict_in_both_readers():
 c=duckdb.connect();c.execute('CREATE MACRO json_extract_scalar(j,p) AS json_extract_string(j,p)')
 cases=[({'quantity':10,'observed_unit_price':300,'request_unit':'EA'},3000),
        ({'quantity':10,'observed_unit_price':300,'request_unit':'EA','source_contract_maximum_usd':2500},None),
        ({'quantity':10,'observed_unit_price':300,'request_unit':'EA','source_contract_maximum_usd':3000},3000),
        ({'quantity':10,'observed_unit_price':300,'request_unit':'EA','pricing_conflict':True},None),
        ({'quantity':10,'observed_unit_price':300,'request_unit':''},None),
        ({'quantity':0,'observed_unit_price':300,'request_unit':'EA'},None)]
 for extract in ('json_extract_scalar','json_extract_string'):
  sql='SELECT '+reference_request_value_sql(extract)+" FROM (SELECT 'published_item_procurement' lane,? value_inputs_json) o"
  for inputs,expected in cases:assert c.execute(sql,[json.dumps(inputs)]).fetchone()[0]==expected
 c.close()
