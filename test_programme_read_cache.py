from concurrent.futures import ThreadPoolExecutor
import threading

import duckdb
import pandas as pd
import pytest

from programme_read_cache import ProgrammeReadCache


def test_parallel_identical_reads_share_one_real_duckdb_query_and_isolate_results():
    cache = ProgrammeReadCache()
    entered, release = threading.Event(), threading.Event()
    calls = []
    def compute():
        calls.append(1); entered.set()
        assert release.wait(2)
        with duckdb.connect() as db:
            return db.execute('SELECT SUM(i) AS amount FROM range(10000) t(i)').fetchdf()
    with ThreadPoolExecutor(max_workers=6) as pool:
        tasks = [pool.submit(cache.read, 'generation-1', 'programme SQL', [2025, 'NAVY'], compute) for _ in range(6)]
        assert entered.wait(2)
        release.set()
        rows = [task.result(timeout=3) for task in tasks]
    assert len(calls) == 1
    assert all(row.iloc[0]['amount'] == 49995000 for row in rows)
    rows[0].iloc[0,0] = -1
    assert cache.read('generation-1', 'programme SQL', [2025, 'NAVY'], compute).iloc[0,0] == 49995000
    assert not cache.flights


def test_generation_scope_ttl_and_memory_limits_do_not_mix_results():
    now = [0]
    cache = ProgrammeReadCache(clock=lambda: now[0], ttl=2, max_entries=2, max_bytes=1024)
    calls = []
    def read(generation, scope):
        return cache.read(generation, 'SQL', [scope], lambda: (calls.append(scope), pd.DataFrame({'scope':[scope]}))[1])
    assert read(1,'NAVY').iloc[0,0] == 'NAVY'
    assert read(1,'AIR FORCE').iloc[0,0] == 'AIR FORCE'
    read(1,'NAVY'); assert len(calls) == 2
    read(2,'NAVY'); assert len(calls) == 3
    assert all(key[0] == '2' for key in cache.entries)
    now[0] = 3; read(2,'NAVY'); assert len(calls) == 4
    for i in range(20):
        read(2,str(i))
        assert len(cache.entries) <= 2 and cache.bytes <= 1024
    assert cache.bytes == sum(entry[2] for entry in cache.entries.values())


def test_failed_queries_are_not_cached_and_bounded_flights_are_released():
    cache = ProgrammeReadCache(max_flights=1)
    entered, release = threading.Event(), threading.Event()
    def fail():
        entered.set(); assert release.wait(2)
        raise ValueError('source failed')
    with ThreadPoolExecutor(max_workers=2) as pool:
        task = pool.submit(cache.read, 1, 'SQL', [], fail)
        assert entered.wait(2)
        with pytest.raises(RuntimeError, match='capacity'):
            cache.read(1, 'other SQL', [], lambda: pd.DataFrame())
        follower = pool.submit(cache.read, 1, 'SQL', [], fail)
        release.set()
        for future in [task, follower]:
            with pytest.raises(ValueError, match='source failed'): future.result(timeout=3)
    assert not cache.flights and not cache.entries
    assert cache.read(1, 'SQL', [], lambda: pd.DataFrame({'ok':[1]})).iloc[0,0] == 1
