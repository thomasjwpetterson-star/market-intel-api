"""Exercise reader isolation through ASGI and the real DuckDB query wrapper."""
import ast
import asyncio
import logging
import os
import queue
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from public_lookup_context import PUBLIC_LOOKUP_REQUEST, is_reserved_public_lookup
from public_serving_capacity import PublicServingCapacityMiddleware
from test_public_serving_capacity import production_functions
import public_lookup_context


LOOKUP = "SELECT * FROM public_award_profile WHERE contract_id = ? LIMIT 1"


@pytest.fixture
def readers(tmp_path):
    database = str(tmp_path / "readers.duckdb")
    connections = [duckdb.connect(database) for _ in range(3)]
    connections[0].execute("CREATE TABLE public_award_profile AS SELECT 'A'||i AS contract_id, i AS value FROM range(100) t(i)")
    connections[0].execute("CREATE INDEX award_id ON public_award_profile(contract_id)")
    analytical, public = queue.Queue(maxsize=2), queue.Queue(maxsize=1)
    for connection in connections[:2]:
        analytical.put(connection)
    public.put(connections[2])
    namespace = production_functions()
    namespace.update(_DUCK_POOL=analytical, _DUCK_PUBLIC_POOL=public)
    yield namespace, analytical, public
    for connection in connections:
        connection.close()


def test_only_bounded_projection_queries_are_reserved():
    assert not is_reserved_public_lookup(LOOKUP)
    token = PUBLIC_LOOKUP_REQUEST.set(True)
    try:
        assert is_reserved_public_lookup(LOOKUP)
        assert is_reserved_public_lookup("SELECT payload_json FROM public_nsn_profile WHERE niin = ? LIMIT 1")
        assert not is_reserved_public_lookup("SELECT * FROM v_transactions LIMIT 1")
        assert not is_reserved_public_lookup("SELECT * FROM public_award_profile")
        assert not is_reserved_public_lookup(LOOKUP + "; SELECT * FROM v_transactions")
    finally:
        PUBLIC_LOOKUP_REQUEST.reset(token)


@pytest.mark.asyncio
async def test_distinct_public_reads_succeed_while_both_analytical_readers_are_occupied(readers):
    namespace, analytical, public = readers
    held = [analytical.get_nowait(), analytical.get_nowait()]
    app = FastAPI()
    app.add_middleware(PublicServingCapacityMiddleware, pending_limit=4, pending_timeout=8)

    @app.get("/api/public/intelligence/awards/{record}")
    def award(record: str):
        assert PUBLIC_LOOKUP_REQUEST.get(), "ASGI context must reach the sync worker"
        return namespace["duck_fetch_df"](LOOKUP, [record]).to_dict("records")

    @app.get("/customer")
    def customer():
        assert not PUBLIC_LOOKUP_REQUEST.get(), "Public routing must not leak to other requests"
        return namespace["duck_fetch_df"]("SELECT 42 AS value").to_dict("records")

    @app.get("/ready")
    async def ready():
        return {"ready": True}

    private_task = None
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            private_task = asyncio.create_task(client.get("/customer"))
            await asyncio.sleep(.05)
            assert not private_task.done()
            for index in range(20):
                response = await asyncio.wait_for(client.get(f"/api/public/intelligence/awards/A{index}"), 1)
                assert response.status_code == 200
                assert response.json() == [{"contract_id": f"A{index}", "value": index}]
            assert not private_task.done(), "Analytical work cannot borrow the public connection"
            assert public.qsize() == 1 and analytical.qsize() == 0
            assert (await asyncio.wait_for(client.get("/ready"), 1)).json()["ready"]
            analytical.put(held.pop())
            assert (await asyncio.wait_for(private_task, 1)).json() == [{"value": 42}]
        assert not PUBLIC_LOOKUP_REQUEST.get()
    finally:
        for connection in held:
            analytical.put(connection)
        if private_task is not None:
            await private_task


@pytest.mark.asyncio
async def test_rollback_to_no_reserved_reader_restores_shared_pool(readers):
    namespace, analytical, public = readers
    namespace["_DUCK_PUBLIC_POOL"] = None
    held = [analytical.get_nowait(), analytical.get_nowait()]
    app = FastAPI()
    app.add_middleware(PublicServingCapacityMiddleware)

    @app.get("/api/public/intelligence/awards/{record}")
    def award(record: str):
        return namespace["duck_fetch_df"](LOOKUP, [record]).to_dict("records")

    task = None
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            task = asyncio.create_task(client.get("/api/public/intelligence/awards/A1"))
            await asyncio.sleep(.1)
            assert not task.done()
            analytical.put(held.pop())
            assert (await asyncio.wait_for(task, 1)).status_code == 200
            assert public.qsize() == 1
    finally:
        for connection in held:
            analytical.put(connection)
        if task is not None:
            await task


def test_failed_reserved_query_returns_its_lease(readers):
    namespace, _, public = readers
    token = PUBLIC_LOOKUP_REQUEST.set(True)
    try:
        with pytest.raises(duckdb.Error):
            namespace["duck_fetch_df"](LOOKUP, [])
        assert public.qsize() == 1
        assert namespace["duck_fetch_df"](LOOKUP, ["A3"]).iloc[0]["value"] == 3
    finally:
        PUBLIC_LOOKUP_REQUEST.reset(token)


def test_memory_ceiling_latches_off_reserved_work(monkeypatch, caplog):
    monkeypatch.setattr(public_lookup_context, "_MEMORY_SUSPENDED", False)
    monkeypatch.setattr(public_lookup_context, "_MEMORY_CEILING_BYTES", 100 * 1024**2)
    monkeypatch.setattr(public_lookup_context.sys, "platform", "linux")
    peak = [99 * 1024]
    monkeypatch.setattr(public_lookup_context.resource, "getrusage", lambda _: SimpleNamespace(ru_maxrss=peak[0]))
    assert public_lookup_context.public_reader_memory_ok()
    peak[0] = 100 * 1024
    assert not public_lookup_context.public_reader_memory_ok()
    peak[0] = 1
    assert not public_lookup_context.public_reader_memory_ok(), "Do not re-enable while a process is under pressure"
    assert caplog.text.count("Reserved public reader disabled") == 1


def test_memory_tripwire_routes_back_to_analytical_pool(readers):
    namespace, analytical, public = readers
    namespace["public_reader_memory_ok"] = lambda: False
    token = PUBLIC_LOOKUP_REQUEST.set(True)
    public_connection = public.get_nowait()
    try:
        assert namespace["duck_fetch_df"](LOOKUP, ["A4"]).iloc[0]["value"] == 4
        assert analytical.qsize() == 2 and public.qsize() == 0
    finally:
        public.put(public_connection)
        PUBLIC_LOOKUP_REQUEST.reset(token)


@pytest.mark.parametrize("reserved", [0, 1])
def test_real_pool_initialization_and_shutdown_support_rollback(tmp_path, reserved):
    wanted = {"ensure_duck_conn", "_close_all_duck_read_conns"}
    nodes = [node for node in ast.parse(Path("main.py").read_text()).body
             if isinstance(node, ast.FunctionDef) and node.name in wanted]
    namespace = dict(DUCK_CONN=None, _DUCK_POOL=None, _DUCK_PUBLIC_POOL=None,
                     DUCK_POOL_SIZE=2, DUCK_PUBLIC_POOL_SIZE=reserved,
                     DUCKDB_PATH=tmp_path / "init.duckdb", DUCK_INIT_LOCK=threading.RLock(),
                     duckdb=duckdb, os=os, logger=logging.getLogger("pool-test"),
                     _apply_duck_pragmas=lambda connection: connection.execute("SET threads=1"))
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "main.py", "exec"), namespace)
    writer = namespace["ensure_duck_conn"]()
    try:
        first = namespace["_DUCK_POOL"]
        assert first.qsize() == 2
        if reserved:
            assert namespace["_DUCK_PUBLIC_POOL"].qsize() == 1
        else:
            assert namespace["_DUCK_PUBLIC_POOL"] is None
        assert namespace["ensure_duck_conn"]() is writer
        assert namespace["_DUCK_POOL"] is first
        namespace["_close_all_duck_read_conns"]()
        assert namespace["_DUCK_POOL"] is None and namespace["_DUCK_PUBLIC_POOL"] is None
    finally:
        namespace["_close_all_duck_read_conns"]()
        writer.close()
