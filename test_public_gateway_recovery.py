"""Gateway-loss recovery must not start duplicate public database work."""
import asyncio
import json
import time

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from public_serving_capacity import PublicServingCapacityMiddleware
from test_public_reader_reservation import readers

PATH='/api/public/intelligence/awards/SPE4A621V352V'
SCOPE={'type':'http','method':'GET','path':PATH,'query_string':b'public_schema=3','headers':[]}

async def response(send,body=b'{"found":true}'):
    await send({'type':'http.response.start','status':200,'headers':[(b'cache-control',b'public, max-age=300')]})
    await send({'type':'http.response.body','body':body})


@pytest.mark.asyncio
@pytest.mark.parametrize('max_waiters',[0,16])
async def test_coalescing_timeout_or_full_waiters_never_start_duplicate_work(max_waiters):
    entered,release=asyncio.Event(),asyncio.Event()
    calls=0
    async def app(scope,receive,send):
        nonlocal calls
        calls+=1; entered.set(); await release.wait(); await response(send)
    guard=PublicServingCapacityMiddleware(app,limit=2,cache_generation=lambda:1,max_waiters=max_waiters,wait_timeout=.025)
    first_messages=[]
    async def first_send(message):first_messages.append(message)
    first=asyncio.create_task(guard(SCOPE,None,first_send)); await entered.wait()
    duplicate_messages=[]
    async def duplicate_send(message):duplicate_messages.append(message)
    duplicate=asyncio.create_task(guard(SCOPE,None,duplicate_send))
    try:
        await asyncio.sleep(.06)
        assert calls==1, 'same-key wait expiry must not admit another database query'
        assert duplicate.done()
        assert duplicate_messages[0]['status']==503
        assert guard.active==1 and guard.waiters==0
    finally:
        release.set(); await asyncio.gather(first,duplicate)
    assert guard.active==0


@pytest.mark.asyncio
async def test_disconnected_response_is_reusable_without_reexecuting_real_read(readers):
    ns,pool,public=readers
    calls=0
    app=FastAPI()
    @app.get(PATH)
    def award():
        nonlocal calls
        calls+=1
        return ns['duck_fetch_df']('SELECT 42 AS value').to_dict('records')
    # Use the public cache policy of the real award endpoint.
    async def public_app(scope,receive,send):
        async def public_send(message):
            if message['type']=='http.response.start':
                message={**message,'headers':[*message['headers'],(b'cache-control',b'public, max-age=300')]}
            await send(message)
        await app(scope,receive,public_send)
    guard=PublicServingCapacityMiddleware(public_app,limit=2,cache_generation=lambda:1)
    failed=False
    class LostGateway:
        async def __call__(self,scope,receive,send):
            nonlocal failed
            if not failed:
                failed=True
                async def broken_send(message):raise OSError('fixture downstream connection closed')
                await guard(scope,receive,broken_send)
                await send({'type':'http.response.start','status':502,'headers':[]})
                await send({'type':'http.response.body','body':b'Bad gateway'})
            else:
                await guard(scope,receive,send)
    async with AsyncClient(transport=ASGITransport(app=LostGateway()),base_url='http://test') as client:
        assert (await client.get(PATH+'?public_schema=3')).status_code==502
        recovered=await client.get(PATH+'?public_schema=3')
        assert recovered.status_code==200 and recovered.json()==[{'value':42}]
        assert recovered.headers['x-mimir-public-cache']=='hit'
    assert calls==1 and pool.qsize()==2 and public.qsize()==1
    assert guard.active==0 and guard.cache_bytes<=guard.max_bytes


@pytest.mark.asyncio
async def test_same_key_queued_before_flight_creation_rechecks_shared_work():
    release={k:asyncio.Event()for k in ['A','B','C']}
    entered={k:asyncio.Event()for k in release}
    calls=[]
    async def app(scope,receive,send):
        key=scope['path'].rsplit('/',1)[-1]
        calls.append(key); entered[key].set(); await release[key].wait(); await response(send)
    guard=PublicServingCapacityMiddleware(app,limit=2,cache_generation=lambda:1,pending_limit=4,pending_timeout=1,wait_timeout=.3)
    async def send(message):pass
    def request(key):return asyncio.create_task(guard({**SCOPE,'path':PATH.rsplit('/',1)[0]+'/'+key},None,send))
    a,b=request('A'),request('B'); await entered['A'].wait();await entered['B'].wait()
    c1,c2=request('C'),request('C');await asyncio.sleep(.01)
    try:
        release['A'].set();await entered['C'].wait()
        release['B'].set();await asyncio.sleep(.025)
        assert calls.count('C')==1
    finally:
        release['C'].set();release['A'].set();release['B'].set()
        await asyncio.gather(a,b,c1,c2)
    assert guard.active==0 and not guard.pending


@pytest.mark.asyncio
async def test_authenticated_gateway_retry_waits_for_the_original_work(monkeypatch):
    monkeypatch.setenv('MIMIR_EXPORT_PROXY_SECRET','fixture')
    entered,release=asyncio.Event(),asyncio.Event()
    calls=0
    async def app(scope,receive,send):
        nonlocal calls
        calls+=1;entered.set();await release.wait();await response(send,b'{"net":-12.5,"actions":22}')
    guard=PublicServingCapacityMiddleware(app,limit=2,cache_generation=lambda:1,wait_timeout=.02)
    async def send(message):pass
    first=asyncio.create_task(guard(SCOPE,None,send));await entered.wait()
    deadline=str(int(time.time()*1000+500)).encode()
    retry={**SCOPE,'headers':[(b'x-mimir-export-key',b'fixture'),(b'x-mimir-public-retry',b'1'),
        (b'x-mimir-public-deadline-ms',deadline),(b'x-mimir-public-trace',b'11111111-2222-4333-8444-555555555555')]}
    messages=[]
    async def retry_send(message):messages.append(message)
    follower=asyncio.create_task(guard(retry,None,retry_send))
    try:
        await asyncio.sleep(.08)
        assert calls==1 and not follower.done()
        release.set();await asyncio.gather(first,follower)
        assert messages[0]['status']==200
        assert json.loads(messages[1]['body'])=={'net':-12.5,'actions':22}
        assert dict(messages[0]['headers'])[b'x-mimir-public-cache']==b'coalesced'
    finally:
        release.set();await asyncio.gather(first,follower)


def test_retry_deadline_requires_server_auth_and_is_clamped(monkeypatch):
    monkeypatch.setenv('MIMIR_EXPORT_PROXY_SECRET','fixture')
    guard=PublicServingCapacityMiddleware(None)
    headers=[(b'x-mimir-export-key',b'fixture'),(b'x-mimir-public-retry',b'1'),
        (b'x-mimir-public-deadline-ms',str(int(time.time()*1000+60000)).encode()),
        (b'x-mimir-public-trace',b'unsafe\ntrace')]
    assert guard._retry_context({**SCOPE,'headers':headers})==(12,'-')
    assert guard._retry_context({**SCOPE,'headers':headers[1:]}) is None
    assert guard._retry_context({**SCOPE,'path':'/api/public/intelligence/companies/X','headers':headers}) is None
    assert guard._retry_context({**SCOPE,'headers':headers[:2]+[(b'x-mimir-public-deadline-ms',b'NaN')]}) is None
    assert guard._retry_context({**SCOPE,'headers':headers[:2]+[(b'x-mimir-public-deadline-ms',b'1')]})==(0,'-')


@pytest.mark.asyncio
async def test_expired_retry_after_queued_admission_does_not_start_database_work(monkeypatch):
    monkeypatch.setenv('MIMIR_EXPORT_PROXY_SECRET','fixture')
    entered,release=asyncio.Event(),asyncio.Event()
    calls=0
    async def app(scope,receive,send):
        nonlocal calls
        calls+=1;entered.set();await release.wait();await response(send)
    guard=PublicServingCapacityMiddleware(app,limit=1,cache_generation=lambda:1,pending_limit=2,pending_timeout=1)
    async def send(message):pass
    first=asyncio.create_task(guard(SCOPE,None,send));await entered.wait()
    retry={**SCOPE,'path':PATH+'OTHER','headers':[(b'x-mimir-export-key',b'fixture'),
        (b'x-mimir-public-retry',b'1'),(b'x-mimir-public-deadline-ms',str(int(time.time()*1000+40)).encode())]}
    messages=[]
    async def retry_send(message):messages.append(message)
    follower=asyncio.create_task(guard(retry,None,retry_send))
    try:
        await asyncio.sleep(.07);release.set();await asyncio.gather(first,follower)
        assert calls==1 and messages[0]['status']==503
        assert guard.active==0 and not guard.pending
    finally:
        release.set();await asyncio.gather(first,follower)

