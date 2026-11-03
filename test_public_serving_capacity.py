"""Regression for public DuckDB waiters exhausting the AnyIO serving pool."""
import ast
import asyncio
import queue
import threading
import time
from pathlib import Path

import anyio
import duckdb
import pandas as pd
import pytest
from fastapi import FastAPI, Response, HTTPException
from analytical_deadline import query_deadline, remaining_query_budget
from httpx import ASGITransport, AsyncClient

from public_serving_capacity import PublicServingCapacityMiddleware
from public_lookup_context import is_reserved_public_lookup, public_reader_memory_ok, public_analytical_slot
from contextlib import nullcontext


def production_functions():
    names = {'duck_fetch_df', 'get_readiness_state', 'ready_check'}
    nodes = []
    for node in ast.parse(Path('main.py').read_text()).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names:
            node.decorator_list = []
            nodes.append(node)
    ns = dict(Optional=__import__('typing').Optional, List=list, Any=object, Dict=dict,
              time=time, hashlib=__import__('hashlib'), logger=__import__('logging').getLogger('capacity-test'),
              pd=pd, Response=Response, HTTPException=HTTPException, Empty=queue.Empty,
              query_deadline=query_deadline, remaining_query_budget=remaining_query_budget, os=__import__('os'),
              GLOBAL_CACHE={'profiles_df': pd.DataFrame({'cage':['ABCDE']}),
                            'last_loaded':1, 'is_loading':False},
              DUCK_CONN=object(), ensure_duck_conn=lambda: None,
              _DUCK_PUBLIC_POOL=None, is_reserved_public_lookup=is_reserved_public_lookup,
              public_reader_memory_ok=public_reader_memory_ok,
              public_analytical_slot=public_analytical_slot, nullcontext=nullcontext,
              df_sanitize_for_json=lambda df: df)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'main.py', 'exec'), ns)
    return ns


@pytest.mark.asyncio
async def test_real_duckdb_waiters_no_longer_starve_customer_or_readiness():
    ns = production_functions()
    pool = queue.Queue(maxsize=2)
    db1, db2 = duckdb.connect(), duckdb.connect()
    pool.put(db1)
    pool.put(db2)
    ns['_DUCK_POOL'] = pool
    original_get = pool.get
    entered = threading.Event()
    release = threading.Event()

    # A real public query holds one reader. Simulate its long scan with a latch
    # at lease acquisition so timing is deterministic across CI machines.
    def held_get(*args, **kwargs):
        conn = original_get(*args, **kwargs)
        if threading.current_thread().name and not entered.is_set():
            entered.set()
            assert release.wait(5)
        return conn
    pool.get = held_get
    app = FastAPI()
    app.add_middleware(PublicServingCapacityMiddleware)
    app.get('/ready')(ns['ready_check'])

    @app.get('/api/public/intelligence/awards/{record}')
    def public(record: str):
        return ns['duck_fetch_df']('SELECT sum(i) AS total FROM range(10000) t(i)').to_dict('records')

    @app.get('/customer')
    def customer():
        return ns['duck_fetch_df']('SELECT 42 AS value').to_dict('records')

    limiter = anyio.to_thread.current_default_thread_limiter()
    previous = limiter.total_tokens
    limiter.total_tokens = 12
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as client:
            first = asyncio.create_task(client.get('/api/public/intelligence/awards/cold'))
            for _ in range(100):
                if entered.is_set():
                    break
                await asyncio.sleep(.01)
            assert entered.is_set()
            started = time.perf_counter()
            responses = await asyncio.wait_for(asyncio.gather(*[
                client.get(f'/api/public/intelligence/awards/{i}', headers={'X-Mimir-Export-Key':'trusted'})
                for i in range(40)]), 1)
            assert all(r.status_code == 503 for r in responses)
            assert all(r.headers['retry-after'] == '2' and r.headers['cache-control'] == 'no-store' for r in responses)
            ready, customer_response = await asyncio.wait_for(asyncio.gather(client.get('/ready'), client.get('/customer')), 1)
            assert ready.json()['ready'] is True
            assert customer_response.json() == [{'value':42}]
            assert time.perf_counter() - started < 1
            release.set()
            assert (await first).status_code == 200
            assert (await client.get('/api/public/intelligence/awards/warm')).status_code == 200
            ns['GLOBAL_CACHE']['is_loading'] = True
            assert (await client.get('/ready')).status_code == 503
    finally:
        release.set()
        limiter.total_tokens = previous
        db1.close()
        db2.close()


@pytest.mark.asyncio
async def test_capacity_released_after_downstream_failure():
    async def downstream(scope, receive, send):
        raise RuntimeError('query failed')
    middleware = PublicServingCapacityMiddleware(downstream)
    scope = {'type':'http','method':'GET','path':'/api/public/intelligence/nsn/123'}
    with pytest.raises(RuntimeError, match='query failed'):
        await middleware(scope, None, None)
    assert middleware.active == 0
    assert not middleware.inflight


@pytest.mark.asyncio
async def test_memory_reduction_drains_two_slots_before_granting_next_waiter():
    memory_ok = [True]
    middleware = PublicServingCapacityMiddleware(None, limit=2, pending_limit=2,
                                                capacity_ok=lambda: memory_ok[0])
    assert await middleware._admit()
    assert await middleware._admit()
    pending = asyncio.create_task(middleware._admit())
    await asyncio.sleep(0)
    memory_ok[0] = False
    middleware._release()
    await asyncio.sleep(0)
    assert middleware.active == 1 and not pending.done()
    middleware._release()
    assert await asyncio.wait_for(pending, 1)
    assert middleware.active == 1
    middleware._release()
    assert middleware.active == 0


@pytest.mark.asyncio
async def test_previous_sync_readiness_queues_behind_real_duck_pool_waiters():
    ns = production_functions()
    ns['_DUCK_POOL'] = queue.Queue(maxsize=2)
    app = FastAPI()
    # The incident version used a sync endpoint and therefore needed an AnyIO
    # token even though readiness itself does not query the database.
    @app.get('/ready')
    def previous_ready():
        return ns['get_readiness_state']()

    @app.get('/public')
    def public():
        return ns['duck_fetch_df']('SELECT 1').to_dict('records')

    limiter = anyio.to_thread.current_default_thread_limiter()
    old_limit = limiter.total_tokens
    limiter.total_tokens = 12
    dbs = [duckdb.connect(), duckdb.connect()]
    tasks = []
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as client:
            tasks = [asyncio.create_task(client.get('/public')) for _ in range(12)]
            for _ in range(100):
                if limiter.borrowed_tokens == 12:
                    break
                await asyncio.sleep(.01)
            assert limiter.borrowed_tokens == 12
            ready = asyncio.create_task(client.get('/ready'))
            tasks.append(ready)
            await asyncio.sleep(.15)
            assert not ready.done(), 'Old readiness must reproduce thread-pool starvation'
            for db in dbs:
                ns['_DUCK_POOL'].put(db)
            assert (await asyncio.wait_for(ready, 2)).status_code == 200
            await asyncio.gather(*tasks)
    finally:
        limiter.total_tokens = old_limit
        for db in dbs:
            db.close()


@pytest.mark.asyncio
async def test_cancelled_http_request_keeps_capacity_until_sync_worker_finishes():
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    app = FastAPI()
    @app.get('/api/public/intelligence/awards/{record}')
    def public(record: str):
        entered.set()
        try:
            release.wait(5)
            return {'record': record}
        finally:
            finished.set()
    middleware = PublicServingCapacityMiddleware(app)
    try:
        async with AsyncClient(transport=ASGITransport(app=middleware), base_url='http://test') as client:
            request = asyncio.create_task(client.get('/api/public/intelligence/awards/first'))
            for _ in range(100):
                if entered.is_set():break
                await asyncio.sleep(.01)
            assert entered.is_set()
            request.cancel()
            with pytest.raises(asyncio.CancelledError):await request
            assert not finished.is_set()
            assert middleware.active == 1, 'Cancelled client must not release a still-running sync query slot'
            rejected = await client.get('/api/public/intelligence/awards/second')
            assert rejected.status_code == 503
            release.set()
            for _ in range(100):
                if finished.is_set() and middleware.active == 0:break
                await asyncio.sleep(.01)
            assert finished.is_set() and middleware.active == 0
    finally:
        release.set()


@pytest.mark.asyncio
async def test_capacity_cannot_be_bypassed_by_trusted_ssr_and_cors_is_retained(monkeypatch):
    from api_traffic_protection import ApiTrafficProtectionMiddleware, VerifiedCrawlers
    from fastapi.middleware.cors import CORSMiddleware
    entered, release = threading.Event(), threading.Event()
    app = FastAPI()
    @app.get('/api/public/intelligence/companies/{record}')
    def public(record: str):
        entered.set()
        release.wait(5)
        return {'record':record}
    app.add_middleware(ApiTrafficProtectionMiddleware, crawlers=VerifiedCrawlers(refresh=False))
    app.add_middleware(PublicServingCapacityMiddleware)
    app.add_middleware(CORSMiddleware, allow_origins=['https://www.mimiradvisors.org'])
    monkeypatch.setenv('MIMIR_EXPORT_PROXY_SECRET','trusted-test-key')
    task=None
    try:
        async with AsyncClient(transport=ASGITransport(app=app),base_url='http://test') as client:
            task=asyncio.create_task(client.get('/api/public/intelligence/companies/FIRST',headers={'X-Mimir-Export-Key':'trusted-test-key'}))
            for _ in range(100):
                if entered.is_set():break
                await asyncio.sleep(.01)
            assert entered.is_set()
            response=await client.get('/api/public/intelligence/companies/NEXT',headers={'X-Mimir-Export-Key':'trusted-test-key','Origin':'https://www.mimiradvisors.org'})
            assert response.status_code==503
            assert response.headers['access-control-allow-origin']=='https://www.mimiradvisors.org'
            assert response.headers['cache-control']=='no-store'
            release.set()
            assert (await task).status_code==200
    finally:
        release.set()
        if task is not None:await task


@pytest.mark.asyncio
async def test_readiness_stays_unready_for_missing_data_and_duckdb_errors_return_pool_lease():
    ns=production_functions()
    app=FastAPI();app.get('/ready')(ns['ready_check'])
    async with AsyncClient(transport=ASGITransport(app=app),base_url='http://test') as client:
        assert (await client.get('/ready')).status_code==200
        ns['DUCK_CONN']=None
        assert (await client.get('/ready')).status_code==503
        ns['DUCK_CONN']=object();ns['GLOBAL_CACHE']['profiles_df']=pd.DataFrame()
        assert (await client.get('/ready')).status_code==503
    db=duckdb.connect();pool=queue.Queue(maxsize=1);pool.put(db);ns['_DUCK_POOL']=pool
    try:
        with pytest.raises(duckdb.Error):ns['duck_fetch_df']('SELECT absent_column')
        assert pool.qsize()==1
        assert ns['duck_fetch_df']('SELECT 1 AS value').iloc[0]['value']==1
    finally:db.close()
