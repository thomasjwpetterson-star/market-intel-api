"""Compile every release gate against the retained production table schemas.

Empty fixtures exercise syntax/bindings; actual release data must still pass
the production gates before publication.
"""
import hashlib
import json
from pathlib import Path
import duckdb
from opportunity_engine.validate import validate


def test_all_release_queries_bind_to_production_schemas(tmp_path):
    schemas = json.loads((Path(__file__).parent / 'fixtures' / 'validation_schemas.json').read_text())
    connection = duckdb.connect()
    types = {'string': 'VARCHAR', 'bigint': 'BIGINT', 'double': 'DOUBLE', 'boolean': 'BOOLEAN', 'int': 'INTEGER'}
    tables = {}
    for name, columns in schemas.items():
        projection = ','.join('cast(NULL AS ' + types[column['Type']] + ') "' + column['Name'] + '"' for column in columns)
        path = tmp_path / (name + '.parquet')
        connection.execute("COPY (SELECT " + projection + " WHERE false) TO '" + str(path) + "' (FORMAT PARQUET)")
        tables[name] = dict(path=str(path), rows=0, columns=columns, sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    connection.close()
    manifest = dict(tables=tables, sources={'sbir_topics': {'rows': 0}}, as_of='2026-10-31', rules={'rules': list(range(23))}, public_release_approved=False, logic_sha256='schema-only-test', reporting_lag_policy={'validation_query_id': 'schema-only'}, annual_activity_policy={'validation_query_id': 'schema-only'})
    (tmp_path / 'manifest.json').write_text(json.dumps(manifest))
    try:
        validate(tmp_path)
    except ValueError as error:
        assert str(error).startswith('Failed release gates:'), str(error)
    result = json.loads((tmp_path / 'validation.json').read_text())
    assert len(result['checks']) > 150
    failures = {check['name'] for check in result['checks'] if not check['passed']}
    # Empty SUMs are NULL, so these two population reconciliations deliberately
    # fail. Every query must nevertheless parse, bind and execute.
    assert failures == {'native_company_award_population', 'value_cohort_full_observation_coverage'}
    assert next(check for check in result['checks'] if check['name'] == 'catalogue_sales_physical_line_once_per_company')['passed']
