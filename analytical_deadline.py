"""Interrupt only the leased query cursor, and retire its timer before reuse."""
from contextlib import contextmanager
import logging
import threading
import time

import duckdb
from fastapi import HTTPException
from analytical_admission import ANALYTICAL_DEADLINE, ANALYTICAL_TRACE_ID


def remaining_query_budget():
    deadline = ANALYTICAL_DEADLINE.get()
    if deadline is None:
        return None
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise HTTPException(status_code=504, detail="The analysis exceeded its execution limit. Narrow the query and retry.")
    return remaining


@contextmanager
def query_deadline(cursor):
    remaining = remaining_query_budget()
    expired = threading.Event()
    timer = None
    if remaining is not None:
        def interrupt():
            expired.set()
            cursor.interrupt()
        timer = threading.Timer(remaining, interrupt)
        timer.daemon = True
        timer.start()
    try:
        yield
    except duckdb.InterruptException as error:
        if not expired.is_set():
            raise
        logging.getLogger("mimir-api").warning("Analytical query interrupted at execution deadline trace=%s", ANALYTICAL_TRACE_ID.get() or "-")
        raise HTTPException(status_code=504, detail="The analysis exceeded its execution limit. Narrow the query and retry.") from error
    finally:
        if timer is not None:
            timer.cancel()
            timer.join()
