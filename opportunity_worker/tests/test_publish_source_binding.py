import hashlib
import duckdb
import pytest
from opportunity_engine.publish import verify_serving_source


def test_missing_legacy_row_metadata_is_counted_from_identical_restored_bytes(tmp_path):
 p=tmp_path/'source.parquet';c=duckdb.connect();c.execute('COPY (SELECT * FROM range(3)) TO ? (FORMAT PARQUET)',[str(p)]);c.close();sha=hashlib.sha256(p.read_bytes()).hexdigest();source={'files':[{'path':'/expired-worker/source.parquet','sha256':sha}]};table={'path':str(p),'sha256':sha,'rows':3}
 verify_serving_source(source,table);assert source['rows']==3
 with pytest.raises(ValueError,match='count mismatch'):verify_serving_source({'files':source['files']},dict(table,rows=4))
 with pytest.raises(ValueError,match='hash mismatch'):verify_serving_source({'files':[{'sha256':'bad'}]},table)
