import asyncio
import threading

import pytest
from fastapi import FastAPI, Response
from httpx import ASGITransport, AsyncClient

from public_serving_capacity import PublicServingCapacityMiddleware


def fixture_app(**options):
    state = {'generation': 1, 'calls': 0, 'now': 0}
    entered, release = threading.Event(), threading.Event()
    app = FastAPI()

    @app.get('/api/public/intelligence/companies/{record}')
    def public(record: str, response: Response):
        state['calls'] += 1
        if record == 'HELD':
            entered.set()
            assert release.wait(5)
        response.headers['Cache-Control'] = 'public, max-age=300'
        if record == 'PRIVATE':
            response.headers['Cache-Control'] = 'private, no-store'
        if record == 'COOKIE':
            response.set_cookie('session', 'test')
        if record == 'VARY':
            response.headers['Vary'] = 'Authorization'
        if record == 'ERROR':
            response.status_code = 503
        return {'record': record, 'generation': state['generation']}

    @app.get('/api/customer')
    async def customer():
        return {'customer': 'responsive'}

    middleware = PublicServingCapacityMiddleware(
        app, cache_generation=lambda: state['generation'], clock=lambda: state['now'], **options)
    return middleware, state, entered, release


@pytest.mark.asyncio
async def test_only_declared_public_results_are_reused_and_generation_invalidates():
    middleware, state, _, _ = fixture_app()
    async with AsyncClient(transport=ASGITransport(app=middleware), base_url='http://test') as client:
        path = '/api/public/intelligence/companies/SAFE'
        assert (await client.get(path)).status_code == 200
        assert (await client.get(path)).headers['x-mimir-public-cache'] == 'hit'
        assert state['calls'] == 1
        state['generation'] = 2
        assert (await client.get(path)).json()['generation'] == 2
        assert state['calls'] == 2
        for record in ['PRIVATE', 'COOKIE', 'VARY', 'ERROR']:
            for _ in range(2):
                result = await client.get('/api/public/intelligence/companies/' + record)
                assert 'x-mimir-public-cache' not in result.headers
            client.cookies.clear()
        for headers in [{'Authorization': 'Bearer test'}, {'Cookie': 'session=test'}]:
            assert 'x-mimir-public-cache' not in (await client.get(path, headers=headers)).headers
        assert state['calls'] == 12


@pytest.mark.asyncio
async def test_duplicate_work_waits_but_waiter_count_and_customer_latency_are_bounded():
    middleware, state, entered, release = fixture_app(max_waiters=2, wait_timeout=.2)
    async with AsyncClient(transport=ASGITransport(app=middleware), base_url='http://test') as client:
        first = asyncio.create_task(client.get('/api/public/intelligence/companies/HELD'))
        try:
            for _ in range(100):
                if entered.is_set(): break
                await asyncio.sleep(.005)
            assert entered.is_set()
            followers = [asyncio.create_task(client.get('/api/public/intelligence/companies/HELD')) for _ in range(5)]
            await asyncio.sleep(.02)
            assert middleware.waiters == 2
            assert middleware.active == 1
            assert (await asyncio.wait_for(client.get('/api/customer'), .5)).status_code == 200
            assert (await client.get('/api/public/intelligence/companies/OTHER')).status_code == 503
            release.set()
            results = await asyncio.gather(*followers)
            assert sorted(r.status_code for r in results) == [200, 200, 503, 503, 503]
            assert state['calls'] == 1
            assert middleware.waiters == 0
            assert (await first).status_code == 200
            assert len(middleware.cache) == 1
        finally:
            release.set()
            await first


@pytest.mark.asyncio
async def test_bounded_stale_public_result_is_available_while_different_query_runs():
    middleware, state, entered, release = fixture_app(ttl=10, stale_ttl=20)
    async with AsyncClient(transport=ASGITransport(app=middleware), base_url='http://test') as client:
        path = '/api/public/intelligence/companies/SAFE'
        await client.get(path)
        state['now'] = 15
        first = asyncio.create_task(client.get('/api/public/intelligence/companies/HELD'))
        try:
            for _ in range(100):
                if entered.is_set(): break
                await asyncio.sleep(.005)
            assert entered.is_set()
            result = await client.get(path, headers={'User-Agent': 'Googlebot'})
            assert result.status_code == 200
            assert result.headers['x-mimir-public-cache'] == 'stale'
            assert result.headers['age'] == '15'
            state['now'] = 21
            assert (await client.get(path)).status_code == 503
            state['generation'] = None
            assert (await client.get(path)).status_code == 503
            assert not middleware.cache
        finally:
            release.set()
            await first


@pytest.mark.asyncio
async def test_cache_memory_and_entry_limits_hold_across_refreshes():
    middleware, state, _, _ = fixture_app(max_entries=2, max_bytes=1000, ttl=1)
    async with AsyncClient(transport=ASGITransport(app=middleware), base_url='http://test') as client:
        for i in range(20):
            state['now'] = i * 2
            await client.get('/api/public/intelligence/companies/' + str(i % 3))
            assert len(middleware.cache) <= 2
            assert middleware.cache_bytes <= 1000
            assert middleware.cache_bytes == sum(entry[3] for entry in middleware.cache.values())
        for i in range(10):
            state['now'] += 2
            await client.get('/api/public/intelligence/companies/SAME')
            assert middleware.cache_bytes == sum(entry[3] for entry in middleware.cache.values())
        assert middleware.active == 0
        assert not middleware.flights


@pytest.mark.asyncio
async def test_cancelled_leader_keeps_one_worker_and_followers_receive_public_result():
    middleware, state, entered, release = fixture_app()
    async with AsyncClient(transport=ASGITransport(app=middleware), base_url='http://test') as client:
        path = '/api/public/intelligence/companies/HELD'
        leader = asyncio.create_task(client.get(path))
        try:
            for _ in range(100):
                if entered.is_set(): break
                await asyncio.sleep(.005)
            assert entered.is_set()
            leader.cancel()
            with pytest.raises(asyncio.CancelledError): await leader
            follower = asyncio.create_task(client.get(path))
            await asyncio.sleep(.02)
            assert middleware.active == 1 and state['calls'] == 1
            release.set()
            result = await follower
            assert result.status_code == 200
            assert result.headers['x-mimir-public-cache'] == 'coalesced'
            assert middleware.active == 0
            assert state['calls'] == 1
        finally:
            release.set()
