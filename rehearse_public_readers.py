"""Bounded local mixed-load rehearsal; synthetic data, no production requests.

Run once per policy: python rehearse_public_readers.py --reserved 0|1.
Uses real public handlers, 100,000 distinct indexed award records, two analytical
workers and fresh public requests at one per second. No response cache is used.
This is a mechanism/memory comparison, not production-data capacity certification.
"""
import argparse
import asyncio
import json
import queue
import statistics
import tempfile
import threading
import time
from pathlib import Path

import duckdb
import psutil
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from public_serving_capacity import PublicServingCapacityMiddleware
from test_public_award_serving import route_namespace
from test_public_serving_capacity import production_functions


async def run(reserved, duration):
    process = psutil.Process()
    baseline = process.memory_info().rss
    peak = baseline
    stop = threading.Event()
    stopped_for_memory = False
    connections = []

    def sample():
        nonlocal peak, stopped_for_memory
        while not stop.wait(.02):
            peak = max(peak, process.memory_info().rss)
            if peak > 600 * 1024**2:
                stopped_for_memory = True
                stop.set()
                for connection in connections:
                    connection.interrupt()

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    with tempfile.TemporaryDirectory(prefix="mimir-reader-rehearsal-") as directory:
        database = str(Path(directory) / "serving.duckdb")
        writer = duckdb.connect(database)
        connections.append(writer)
        writer.execute("SET threads=1")
        writer.execute("SET memory_limit='128MB'")
        writer.execute("CREATE TABLE public_award_profile AS SELECT 'LOCAL'||i AS contract_id, 'VENDOR'||i AS vendor_name, 'ABCDE' AS vendor_cage, 'Example public award '||i AS base_award_description, i::DOUBLE AS total_spend, 2 AS action_count FROM range(100000) t(i)")
        writer.execute("CREATE INDEX award_id ON public_award_profile(contract_id)")
        analytical, public = queue.Queue(maxsize=2), queue.Queue(maxsize=1)
        for _ in range(2 + reserved):
            connection = duckdb.connect(database)
            connections.append(connection)
            if analytical.qsize() < 2:
                analytical.put(connection)
            else:
                public.put(connection)
        query = production_functions()
        query.update(_DUCK_POOL=analytical, _DUCK_PUBLIC_POOL=public if reserved else None)
        route = route_namespace()
        route["duck_fetch_df"] = query["duck_fetch_df"]
        app = FastAPI()
        app.add_middleware(PublicServingCapacityMiddleware, pending_limit=4, pending_timeout=8)
        app.get("/api/public/intelligence/awards/{contract_id}")(route["get_public_award_page_snapshot"])
        counts = [0, 0]
        errors = []
        analytical_deadline = time.monotonic() + duration
        def analytics(index):
            while not stop.is_set() and time.monotonic() < analytical_deadline:
                try:
                    query["duck_fetch_df"]("SELECT SUM(s) AS total FROM (SELECT i%50000 AS k, SUM(SIN(i::DOUBLE)) AS s FROM range(3000000) t(i) GROUP BY k)")
                    counts[index] += 1
                except Exception as error:
                    if not stopped_for_memory:
                        errors.append(type(error).__name__)
                    break
        workers = [threading.Thread(target=analytics, args=(index,), daemon=True) for index in range(2)]
        for worker in workers:
            worker.start()
        results = []
        try:
            async with AsyncClient(transport=ASGITransport(app=app), base_url="http://local") as client:
                async def lookup(index):
                    await asyncio.sleep(index)
                    started = time.perf_counter()
                    response = await client.get(f"/api/public/intelligence/awards/LOCAL{index}")
                    results.append({"status": response.status_code, "ms": round((time.perf_counter()-started)*1000, 2)})
                await asyncio.gather(*(lookup(index) for index in range(duration)))
        finally:
            stop.set()
            for worker in workers:
                worker.join(10)
            sampler.join(1)
        live_workers = sum(worker.is_alive() for worker in workers)
        assert not live_workers
        for connection in connections:
            connection.close()
    latencies = sorted(result["ms"] for result in results)
    return {"duckdb": duckdb.__version__, "reserved_readers": reserved,
            "analytical_readers": 2, "synthetic_records": 100000,
            "public_requests": len(results), "request_rate_per_second": 1,
            "response_cache": False, "rehearsal_duckdb_memory_cap_mb": 128,
            "baseline_rss_mib": round(baseline/1024**2, 1),
            "peak_rss_mib": round(peak/1024**2, 1),
            "public_p50_ms": statistics.median(latencies),
            "public_p95_ms": latencies[int(.95*(len(latencies)-1))],
            "public_max_ms": max(latencies), "results": results,
            "analytical_queries_completed": counts, "analytical_errors": errors,
            "stopped_for_memory": stopped_for_memory,
            "limitations": "Synthetic data and a lower local memory cap; production RSS and mixed traffic require post-deploy monitoring."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reserved", type=int, choices=[0, 1], required=True)
    parser.add_argument("--duration", type=int, default=20)
    arguments = parser.parse_args()
    print(json.dumps(asyncio.run(run(arguments.reserved, arguments.duration)), indent=2))
