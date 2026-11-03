import math
from pathlib import Path

import duckdb
import pytest

from contract_lookup_layout import sort_lookup_file,verify_layout


@pytest.mark.parametrize('window_rows',[1000,3])
def test_every_value_and_duplicate_first_selection_survives_sort(tmp_path,window_rows):
    source=tmp_path/'source.parquet';target=tmp_path/'candidate.parquet'
    c=duckdb.connect()
    c.execute('CREATE TABLE awards(contract_id VARCHAR, total_spend DOUBLE, n BIGINT, details VARCHAR)')
    c.executemany('INSERT INTO awards VALUES (?,?,?,?)',[
        ('Z',1.25,1,'old first'),('A',-0.0,2,None),('Z',-10,3,'second'),
        (None,float('nan'),4,'null key'),('B',0,5,'other'),('Z',1e20,6,'third'),
        ('A',float('inf'),7,'duplicate in later window')])
    c.execute(f"COPY awards TO '{source}' (FORMAT PARQUET)")
    before={key:c.execute(f"SELECT * FROM read_parquet('{source}') WHERE contract_id=? LIMIT 1",[key]).fetchall() for key in ['A','B','Z']}
    result=sort_lookup_file(source,target,memory_mb=128,window_rows=window_rows)
    assert result['all_values_equal'] and result['rows_compared']==7 and result['columns_compared']==4
    after={key:c.execute(f"SELECT * FROM read_parquet('{target}') WHERE contract_id=? LIMIT 1",[key]).fetchall() for key in ['A','B','Z']}
    assert before==after
    c.close()


def test_validation_rejects_changed_value_and_never_overwrites_source(tmp_path):
    c=duckdb.connect();source=tmp_path/'source.parquet';changed=tmp_path/'changed.parquet'
    c.execute(f"COPY (SELECT 'A' AS contract_id, 1.0 AS amount) TO '{source}' (FORMAT PARQUET)")
    c.execute(f"COPY (SELECT 'A' AS contract_id, 2.0 AS amount) TO '{changed}' (FORMAT PARQUET)")
    with pytest.raises(ValueError,match='values/order'):
        verify_layout(source,changed,memory_mb=128,window_rows=3,spill=tmp_path/'spill')
    with pytest.raises(ValueError,match='new path'):
        sort_lookup_file(source,source)
    c.close()
