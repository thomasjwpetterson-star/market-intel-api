import asyncio
import threading
import time

import duckdb
import pytest
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient

from analytical_admission import AnalyticalAdmissionMiddleware, ANALYTICAL_DEADLINE
from analytical_deadline import query_deadline
from public_serving_capacity import PublicServingCapacityMiddleware
from test_public_reader_reservation import readers


def test_deadline_interrupts_real_cursor_and_does_not_poison_next_query(readers):
    ns, pool, _ = readers
    token = ANALYTICAL_DEADLINE.set(time.monotonic()+.1)
    started = time.monotonic()
    try:
        with pytest.raises(HTTPException) as failure:
            ns['duck_fetch_df']('SELECT SUM(i::DOUBLE * SIN(i)) FROM range(10000000000) t(i)')
        assert failure.value.status_code == 504
        assert time.monotonic()-started < 2
    finally:
        ANALYTICAL_DEADLINE.reset(token)
    assert pool.qsize() == 2
    assert ns['duck_fetch_df']('SELECT 42 AS value').iloc[0]['value'] == 42
    time.sleep(.15)
    assert ns['duck_fetch_df']('SELECT 43 AS value').iloc[0]['value'] == 43


def test_expired_deadline_does_not_lease_reader(readers):
    ns, pool, _ = readers
    token = ANALYTICAL_DEADLINE.set(time.monotonic()-1)
    try:
        with pytest.raises(HTTPException) as failure:
            ns['duck_fetch_df']('SELECT 42')
        assert failure.value.status_code == 504
        assert pool.qsize() == 2
    finally:
        ANALYTICAL_DEADLINE.reset(token)


@pytest.mark.asyncio
async def test_queue_is_bounded_and_cancelled_caller_keeps_running_slot(monkeypatch):
    monkeypatch.setenv('MIMIR_EXPORT_PROXY_SECRET','fixture')
    release, entered = asyncio.Event(), asyncio.Event()
    executions = []
    async def downstream(scope,receive,send):
        if dict(scope.get('headers',[])).get(b'x-mimir-export-key') != b'fixture':
            await send({'type':'http.response.start','status':401,'headers':[]})
        else:
            executions.append(ANALYTICAL_DEADLINE.get())
            entered.set();await release.wait()
            await send({'type':'http.response.start','status':200,'headers':[]})
        await send({'type':'http.response.body','body':b''})
    middleware=AnalyticalAdmissionMiddleware(downstream,pending_limit=1,pending_timeout=.08)
    scope={'type':'http','method':'POST','path':'/api/research/aggregate','headers':[(b'x-mimir-export-key',b'fixture')]}
    messages=[]
    async def send(message):messages.append(message)
    first=asyncio.create_task(middleware(scope,None,send));await entered.wait()
    first.cancel()
    with pytest.raises(asyncio.CancelledError):await first
    assert middleware.active==1
    invalid={**scope,'headers':[]}
    await middleware(invalid,None,send)
    assert messages[-2]['status']==401 and middleware.active==1
    queued=asyncio.create_task(middleware(scope,None,send));await asyncio.sleep(.01)
    await middleware(scope,None,send)
    assert messages[-2]['status']==503
    await queued
    assert messages[-2]['status']==503 and len(executions)==1 and middleware.waiting==0
    release.set();await asyncio.gather(*list(middleware.inflight))
    assert middleware.active==0
    await middleware(scope,None,send)
    assert messages[-2]['status']==200 and len(executions)==2
    assert ANALYTICAL_DEADLINE.get() is None


@pytest.mark.asyncio
async def test_long_analysis_and_parallel_crawl_share_two_readers_without_new_connections(readers,monkeypatch):
    """Keep a lease occupied beyond the old eight-second public queue window."""
    monkeypatch.setenv('MIMIR_EXPORT_PROXY_SECRET','fixture')
    ns,pool,_=readers
    original_get=pool.get
    entered,release=threading.Event(),threading.Event()
    leased=0;peak=0;lock=threading.Lock()
    original_put=pool.put
    def get(*args,**kwargs):
        nonlocal leased,peak
        connection=original_get(*args,**kwargs)
        with lock:leased+=1;peak=max(peak,leased)
        if ANALYTICAL_DEADLINE.get() is not None:
            entered.set();assert release.wait(30)
        return connection
    def put(connection,*args,**kwargs):
        nonlocal leased
        with lock:leased-=1
        return original_put(connection,*args,**kwargs)
    pool.get,pool.put=get,put
    app=FastAPI()
    app.add_middleware(AnalyticalAdmissionMiddleware)
    app.add_middleware(PublicServingCapacityMiddleware,limit=2,pending_limit=12,pending_timeout=12)
    @app.post('/api/research/aggregate')
    def analysis():return ns['duck_fetch_df']('SELECT 42 AS value').to_dict('records')
    @app.get('/api/public/intelligence/awards/{record}')
    def fallback(record:str):
        time.sleep(.03)
        return ns['duck_fetch_df']('SELECT SUM(i) AS value FROM range(100000) t(i)').to_dict('records')
    @app.get('/ready')
    async def ready():return {'ready':True}
    task=None
    try:
        async with AsyncClient(transport=ASGITransport(app=app),base_url='http://test') as client:
            task=asyncio.create_task(client.post('/api/research/aggregate',headers={'X-Mimir-Export-Key':'fixture'}))
            while not entered.is_set():await asyncio.sleep(.01)
            started=time.monotonic()
            # Eight simultaneous distinct legacy lookups, then one new page/sec.
            burst=await asyncio.gather(*[client.get(f'/api/public/intelligence/awards/B{i}') for i in range(8)])
            assert all(r.status_code==200 for r in burst)
            for i in range(20):
                response=await client.get(f'/api/public/intelligence/awards/S{i}')
                assert response.status_code==200
                assert (await client.get('/ready')).json()['ready']
                await asyncio.sleep(1)
            assert time.monotonic()-started>=20 and not task.done()
            assert peak==2 and leased==1
            release.set();assert (await task).status_code==200
            assert pool.qsize()==2
    finally:
        release.set()
        if task is not None:await task
