import time
import duckdb
import pytest
from fastapi import HTTPException
from analytical_admission import ANALYTICAL_DEADLINE
from analytical_deadline import query_deadline
from explorer_page_read import read_snapshot_page


def test_page_keeps_one_snapshot_when_a_release_replaces_the_table(tmp_path):
    path=str(tmp_path/'snapshot.duckdb')
    reader=duckdb.connect(path)
    writer=duckdb.connect(path)
    try:
        reader.execute("CREATE TABLE awards AS SELECT 'old native award' AS value")
        def followup(ranked):
            writer.execute("CREATE OR REPLACE TABLE awards AS SELECT 'new native award' AS value")
            return 'SELECT value FROM awards WHERE rowid=?',[int(ranked.iloc[0,0])]
        result=read_snapshot_page(reader,'SELECT rowid FROM awards',[],followup)
        assert result.iloc[0,0]=='old native award'
        assert writer.execute('SELECT value FROM awards').fetchone()[0]=='new native award'
    finally:
        writer.close()
        reader.close()


def test_empty_rank_skips_details_and_failure_rolls_back():
    db=duckdb.connect()
    try:
        def forbidden(_):
            raise AssertionError('An empty page must not read details')
        assert read_snapshot_page(db,'SELECT 1 AS id WHERE FALSE',[],forbidden).empty
        with pytest.raises(AssertionError):
            read_snapshot_page(db,'SELECT 1 AS id',[],forbidden)
        db.execute('BEGIN TRANSACTION')
        db.execute('ROLLBACK')
    finally:
        db.close()


def test_second_statement_shares_the_original_deadline_and_reader_remains_usable():
    db=duckdb.connect()
    token=ANALYTICAL_DEADLINE.set(time.monotonic()+0.03)
    try:
        with pytest.raises(HTTPException) as failure:
            with query_deadline(db):
                read_snapshot_page(db,'SELECT 1 AS id',[],lambda _: (
                    'SELECT SUM(a.i*b.i) FROM range(100000) a(i) CROSS JOIN range(100000) b(i)',[]))
        assert failure.value.status_code==504
        assert db.execute('SELECT 1').fetchone()[0]==1
        db.execute('BEGIN TRANSACTION')
        db.execute('ROLLBACK')
    finally:
        ANALYTICAL_DEADLINE.reset(token)
        db.close()
