"""Bounded, generation-scoped reuse of read-only programme query results.

Called after route authentication, never used for account or arbitrary SQL APIs.
Followers share an existing scan instead of occupying another DuckDB reader.
"""
from collections import OrderedDict
from concurrent.futures import Future
import threading
import time


class ProgrammeReadCache:
    def __init__(self, ttl=60, max_entries=64, max_bytes=8*1024*1024,
                 max_flights=16, wait_timeout=40, clock=time.monotonic):
        self.ttl, self.max_entries, self.max_bytes = ttl, max_entries, max_bytes
        self.max_flights, self.wait_timeout, self.clock = max_flights, wait_timeout, clock
        self.lock = threading.Lock()
        self.entries, self.flights = OrderedDict(), {}
        self.bytes = 0

    def read(self, generation, sql, params, compute):
        if not generation:
            return compute()
        key = (str(generation), sql, tuple(params))
        with self.lock:
            for old, (expires, _, size) in list(self.entries.items()):
                if expires <= self.clock() or old[0] != key[0]:
                    self.entries.pop(old); self.bytes -= size
            if key in self.entries:
                self.entries.move_to_end(key)
                return self.entries[key][1].copy(deep=True)
            flight = self.flights.get(key)
            leader = flight is None
            if leader:
                if len(self.flights) >= self.max_flights:
                    raise RuntimeError('Programme read capacity is temporarily full.')
                flight = self.flights[key] = Future()
        if not leader:
            return flight.result(timeout=self.wait_timeout).copy(deep=True)
        try:
            value = compute()
            stored = value.copy(deep=True)
            size = int(stored.memory_usage(index=True, deep=True).sum())
            with self.lock:
                if size <= self.max_bytes:
                    self.entries[key] = (self.clock()+self.ttl, stored, size)
                    self.bytes += size
                    while len(self.entries) > self.max_entries or self.bytes > self.max_bytes:
                        _, (_, _, removed) = self.entries.popitem(last=False)
                        self.bytes -= removed
                flight.set_result(stored)
            return value
        except BaseException as error:
            flight.set_exception(error)
            raise
        finally:
            with self.lock:
                self.flights.pop(key, None)
