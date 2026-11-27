"""Read a ranked award page within one snapshot and the caller's existing budget."""
import logging
import time


def read_snapshot_page(cursor,sql,params,followup):
    cursor.execute('BEGIN TRANSACTION')
    started = time.monotonic()
    rank_finished = None
    count = 0
    completed = False
    try:
        ranked = cursor.execute(sql,params).fetchdf()
        rank_finished = time.monotonic()
        count = len(ranked)
        if count:
            detail_sql,detail_params = followup(ranked)
            result = cursor.execute(detail_sql,detail_params).fetchdf()
            if len(result) < count:
                raise RuntimeError('Explorer details do not match the ranked page.')
        else:
            result = ranked
        cursor.execute('COMMIT')
        completed = True
        return result
    except BaseException:
        try:
            cursor.execute('ROLLBACK')
        except Exception:
            pass
        raise
    finally:
        finished = time.monotonic()
        logging.getLogger('mimir-api').info(
            'Explorer page stages rank_ms=%.1f detail_ms=%.1f page_rows=%s completed=%s',
            ((rank_finished or finished)-started)*1000,
            (finished-rank_finished)*1000 if rank_finished is not None else 0,
            count,completed)
