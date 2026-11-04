"""Real ASGI/reader tests for bounded research waiting and caller deadlines."""
import asyncio
import threading
import time

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from analytical_admission import AnalyticalAdmissionMiddleware, ANALYTICAL_DEADLINE, ANALYTICAL_TRACE_ID
from test_public_reader_reservation import readers

PATH = '/api/research/query'
TRACE = '11111111-2222-4333-8444-555555555555'

def headers(ms=800):
    return {'X-Mimir-Export-Key':'fixture', 'X-Mimir-Research-Budget-Ms':str(ms),
            'X-Mimir-Research-Trace':TRACE}

async def entered(event):
    async with asyncio.timeout(2):
        while not event.is_set():
            await asyncio.sleep(.002)


@pytest.mark.asyncio
async def test_five_distinct_research_reads_wait_fifo_without_extra_readers_or_retries(readers, monkeypatch):
    monkeypatch.setenv('MIMIR_EXPORT_PROXY_SECRET','fixture')
    ns,pool,public=readers
    start,release=threading.Event(),threading.Event()
    order=[]
    app=FastAPI()
    @app.post(PATH)
    def query(payload:dict):
        order.append(payload['id'])
        if payload['id']==0:
            start.set(); assert release.wait(2)
        time.sleep(.015)
        return ns['duck_fetch_df']('SELECT ? AS value',[payload['id']]).to_dict('records')
    guard=AnalyticalAdmissionMiddleware(app,pending_timeout=.04,execution_reserve=.02,budgets={('POST',PATH):.8})
    tasks=[]
    try:
        async with AsyncClient(transport=ASGITransport(app=guard),base_url='http://test') as client:
            tasks.append(asyncio.create_task(client.post(PATH,json={'id':0},headers=headers())))
            await entered(start)
            for i in range(1,5):
                tasks.append(asyncio.create_task(client.post(PATH,json={'id':i},headers=headers())))
                await asyncio.sleep(.003)
            await asyncio.sleep(.08)  # Beyond the legacy queue timeout.
            assert guard.active==1 and guard.waiting==4 and order==[0]
            overflow=await client.post(PATH,json={'id':99},headers=headers())
            assert overflow.status_code==503
            assert overflow.headers['x-mimir-analytical-capacity']=='busy'
            release.set()
            responses=await asyncio.gather(*tasks)
            assert [r.status_code for r in responses]==[200]*5
            assert [r.json() for r in responses]==[[{'value':i}]for i in range(5)]
            assert order==list(range(5))
            assert pool.qsize()==2 and public.qsize()==1
            assert guard.active==guard.waiting==0
    finally:
        release.set()
        await asyncio.gather(*tasks,return_exceptions=True)


@pytest.mark.asyncio
async def test_remaining_budget_includes_wait_and_interrupts_real_sql_before_reader_reuse(readers,monkeypatch,caplog):
    monkeypatch.setenv('MIMIR_EXPORT_PROXY_SECRET','fixture')
    ns,pool,_=readers
    start,release=threading.Event(),threading.Event()
    app=FastAPI()
    @app.post(PATH)
    def query(payload:dict):
        if payload['id']==0:
            start.set(); assert release.wait(2)
            return {'value':0}
        if payload['id']==1:
            assert ANALYTICAL_TRACE_ID.get()==TRACE
            return ns['duck_fetch_df']('SELECT SUM(i::DOUBLE*SIN(i)) FROM range(10000000000) t(i)').to_dict('records')
        return ns['duck_fetch_df']('SELECT 42 AS value').to_dict('records')
    guard=AnalyticalAdmissionMiddleware(app,execution_reserve=.02,budgets={('POST',PATH):.8})
    async with AsyncClient(transport=ASGITransport(app=guard),base_url='http://test') as client:
        leader=asyncio.create_task(client.post(PATH,json={'id':0},headers=headers()))
        await entered(start)
        began=time.monotonic()
        follower=asyncio.create_task(client.post(PATH,json={'id':1},headers=headers(220)))
        try:
            await asyncio.sleep(.10); release.set()
            assert (await leader).status_code==200
            response=await follower
            assert response.status_code==504
            assert time.monotonic()-began<.6
            assert pool.qsize()==2 and guard.active==guard.waiting==0
            assert TRACE in caplog.text
            await asyncio.sleep(.03)
            assert (await client.post(PATH,json={'id':2},headers=headers())).json()==[{'value':42}]
        finally:
            release.set(); await asyncio.gather(leader,follower,return_exceptions=True)
    assert ANALYTICAL_DEADLINE.get() is None and ANALYTICAL_TRACE_ID.get() is None


@pytest.mark.asyncio
async def test_only_authenticated_caller_can_shorten_not_extend_budget(monkeypatch):
    monkeypatch.setenv('MIMIR_EXPORT_PROXY_SECRET','fixture')
    seen=[]
    async def app(scope,receive,send):
        authorised=dict(scope['headers']).get(b'x-mimir-export-key')==b'fixture'
        if authorised:
            seen.append((ANALYTICAL_DEADLINE.get()-time.monotonic(),ANALYTICAL_TRACE_ID.get()))
        await send({'type':'http.response.start','status':200 if authorised else 401,'headers':[]})
        await send({'type':'http.response.body','body':b'{}'})
    guard=AnalyticalAdmissionMiddleware(app,budgets={('POST',PATH):.4})
    async with AsyncClient(transport=ASGITransport(app=guard),base_url='http://test') as client:
        assert (await client.post(PATH,headers=headers(999999))).status_code==200
        assert 0<seen[-1][0]<=.4 and seen[-1][1]==TRACE
        assert (await client.post(PATH,headers=headers(75))).status_code==200
        assert 0<seen[-1][0]<=.075
        delayed={**headers(400),'X-Mimir-Research-Deadline-Ms':str(int(time.time()*1000+50))}
        assert (await client.post(PATH,headers=delayed)).status_code==200
        assert 0<seen[-1][0]<=.05
        for changed,status in [({'X-Mimir-Research-Budget-Ms':'0'},504),
                               ({'X-Mimir-Research-Budget-Ms':'NaN'},400),
                               ({'X-Mimir-Research-Budget-Ms':'-1'},400),
                               ({'X-Mimir-Research-Deadline-Ms':'0'},504),
                               ({'X-Mimir-Research-Deadline-Ms':'invalid'},400)]:
            count=len(seen)
            assert (await client.post(PATH,headers={**headers(),**changed})).status_code==status
            assert len(seen)==count
        bad={**headers(),'X-Mimir-Export-Key':'wrong','X-Mimir-Research-Budget-Ms':'0'}
        assert (await client.post(PATH,headers=bad)).status_code==401
        assert (await client.post(PATH,headers={**headers(),'X-Mimir-Research-Trace':'injected-private-data'})).status_code==200
        assert seen[-1][1] is None
    assert guard.active==guard.waiting==0


@pytest.mark.asyncio
async def test_cancelled_waiter_leaves_queue_and_cancelled_running_caller_keeps_lease(monkeypatch):
    monkeypatch.setenv('MIMIR_EXPORT_PROXY_SECRET','fixture')
    start,release=asyncio.Event(),asyncio.Event()
    async def app(scope,receive,send):
        start.set(); await release.wait()
    guard=AnalyticalAdmissionMiddleware(app,budgets={('POST',PATH):1},execution_reserve=.02)
    scope={'type':'http','method':'POST','path':PATH,'headers':[(k.lower().encode(),v.encode())for k,v in headers(800).items()]}
    async def send(message):pass
    leader=asyncio.create_task(guard(scope,None,send)); await start.wait()
    waiter=asyncio.create_task(guard(scope,None,send)); await asyncio.sleep(.01)
    assert guard.active==guard.waiting==1
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):await waiter
    assert guard.waiting==0 and guard.active==1
    leader.cancel()
    with pytest.raises(asyncio.CancelledError):await leader
    assert guard.active==1
    release.set(); await asyncio.gather(*list(guard.inflight))
    assert guard.active==guard.waiting==0
