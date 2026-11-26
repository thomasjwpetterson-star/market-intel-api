"""Browsing remains serviceable while real background readers are occupied."""
import asyncio
import threading
import time

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from analytical_admission import AnalyticalAdmissionMiddleware
from dashboard_capacity import DashboardCapacityMiddleware
from dashboard_routes import BACKGROUND_ANALYSIS, DASHBOARD_REQUEST, READERS
from public_lookup_context import PUBLIC_LOOKUP_REQUEST
from public_serving_capacity import PublicServingCapacityMiddleware
from test_public_reader_reservation import readers


@pytest.mark.asyncio
async def test_seven_dashboard_sections_succeed_during_a_held_research_scan(readers,monkeypatch):
    monkeypatch.setenv('MIMIR_EXPORT_PROXY_SECRET','fixture')
    ns,pool,_=readers
    entered,release=threading.Event(),threading.Event()
    original_get,original_put=pool.get,pool.put
    leased=peak=0
    lock=threading.Lock()
    def get(*args,**kwargs):
        nonlocal leased,peak
        connection=original_get(*args,**kwargs)
        with lock:leased+=1;peak=max(peak,leased)
        if BACKGROUND_ANALYSIS.get() and not PUBLIC_LOOKUP_REQUEST.get():
            entered.set();assert release.wait(5)
        return connection
    def put(connection,*args,**kwargs):
        nonlocal leased
        with lock:leased-=1
        return original_put(connection,*args,**kwargs)
    pool.get,pool.put=get,put
    app=FastAPI()
    app.add_middleware(AnalyticalAdmissionMiddleware)
    app.add_middleware(DashboardCapacityMiddleware)
    app.add_middleware(PublicServingCapacityMiddleware)
    @app.post('/api/research/aggregate')
    def research():return ns['duck_fetch_df']('SELECT 10 AS value').to_dict('records')
    @app.get('/api/platform/{section:path}')
    def dashboard(section:str):
        assert DASHBOARD_REQUEST.get()
        return ns['duck_fetch_df']('SELECT 42 AS value').to_dict('records')
    task=None
    try:
        async with AsyncClient(transport=ASGITransport(app=app),base_url='http://test') as client:
            task=asyncio.create_task(client.post('/api/research/aggregate',headers={'X-Mimir-Export-Key':'fixture'}))
            for _ in range(100):
                if entered.is_set():break
                await asyncio.sleep(.01)
            assert entered.is_set()
            began=time.monotonic()
            responses=await asyncio.wait_for(asyncio.gather(*[client.get('/api/platform/'+section) for section in
                ['profile','contractors','parts','parts/count','awards','subcontracts','announcements']]),2)
            assert all(response.status_code==200 and response.json()==[{'value':42}] for response in responses)
            assert time.monotonic()-began<2 and not task.done()
            assert peak==2 and leased==1 and pool.qsize()==1
            release.set();assert (await task).status_code==200
            assert pool.qsize()==2
    finally:
        release.set()
        if task is not None:await task
    assert READERS.demand==READERS.background==READERS.foreground==0


@pytest.mark.asyncio
async def test_dashboard_queue_cancellation_cannot_release_a_running_query():
    entered,release=asyncio.Event(),asyncio.Event()
    async def downstream(scope,receive,send):
        assert DASHBOARD_REQUEST.get();entered.set();await release.wait()
        await send({'type':'http.response.start','status':200,'headers':[]})
        await send({'type':'http.response.body','body':b'[]'})
    middleware=DashboardCapacityMiddleware(downstream)
    messages=[]
    async def send(message):messages.append(message)
    scope={'type':'http','method':'GET','path':'/api/platform/awards','headers':[]}
    task=asyncio.create_task(middleware(scope,None,send));await entered.wait();task.cancel()
    with pytest.raises(asyncio.CancelledError):await task
    assert middleware.gate.active==1 and READERS.demand>=1
    release.set();await asyncio.gather(*list(middleware.gate.inflight))
    assert middleware.gate.active==middleware.gate.waiting==0 and READERS.demand==0
