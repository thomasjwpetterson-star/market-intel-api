from fastapi import FastAPI, HTTPException, Query, Depends, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, date
import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
import os
import pandas as pd
from io import BytesIO
from typing import Optional, List, Dict, Any, Tuple
import threading
import time
from functools import lru_cache 
import re
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from html import unescape
from difflib import SequenceMatcher
import concurrent.futures
import gc 
import duckdb # ✅ NEW: Disk-based query engine
import asyncio  # <--- Add this
from pathlib import Path
import logging
import random
import anyio 
import math
import numpy as np
from pydantic import BaseModel
import uuid
import hmac
import csv
from fastapi import APIRouter
from dotenv import load_dotenv

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("mimir-api")

def is_authenticated_user(request: Request) -> bool:
    """If the frontend sends an Authorization header, they bypass the public limit."""
    return "Authorization" in request.headers

# ✅ GLOBAL MEMORY STORE (Initialized with empty defaults)
# We will swap this entire dictionary reference atomically.
GLOBAL_CACHE = {
    "df": pd.DataFrame(),
    "geo_df": pd.DataFrame(),
    "profiles_df": pd.DataFrame(),
    "risk_df": pd.DataFrame(),
    "df_opportunities": pd.DataFrame(),
    "kpis_path": None,
    "options": {},
    "search_index": [],
    "is_loading": True, # Start as loading
    "last_loaded": 0,
    "cage_name_map": {},
    "location_map": {},
    "naics_map": {}
}

# NOTE: global_data is no longer needed as we use DuckDB for heavy data
# CACHE_LOCK is removed because we use atomic pointer swapping

LOCAL_CACHE_DIR = Path(os.getenv("LOCAL_CACHE_DIR", "./local_data"))
LOCAL_CACHE_DIR.mkdir(parents=True, exist_ok=True)

DUCKDB_PATH = LOCAL_CACHE_DIR / "mimir.duckdb"
DUCK_CONN = None
DUCK_LOCK = threading.RLock()

DUCKDB_PATH = (LOCAL_CACHE_DIR / "mimir.duckdb").resolve()

# ✅ Keep a single base connection to minimize RAM use
DUCK_CONN = None
DUCK_INIT_LOCK = threading.RLock()

# ✅ Tiny execution pool to allow a little concurrency without opening many connections
# Two readers keep the dashboard responsive without allowing three large
# aggregations to compete for a small Render instance at the same time.
DUCK_POOL_SIZE = int(os.getenv("DUCK_POOL_SIZE", "2"))
_DUCK_POOL = None  # will be a queue.Queue of connections

def _apply_duck_pragmas(conn: duckdb.DuckDBPyConnection):
    duck_tmp = str((LOCAL_CACHE_DIR / "duckdb_tmp").resolve())
    Path(duck_tmp).mkdir(parents=True, exist_ok=True)

    # Parquet is built-in; LOAD is harmless if missing
    try:
        conn.execute("LOAD parquet;")
    except Exception:
        pass

    # ✅ Render/OOM-friendly defaults (tune via env vars)
    conn.execute(f"PRAGMA temp_directory='{duck_tmp}';")
    conn.execute(f"PRAGMA threads={int(os.getenv('DUCKDB_THREADS', '1'))};")
    conn.execute(f"PRAGMA memory_limit='{os.getenv('DUCKDB_MEM', '650MB')}';")
    conn.execute("SET preserve_insertion_order=false;")
    conn.execute("PRAGMA enable_object_cache=false;")

def ensure_duck_conn() -> duckdb.DuckDBPyConnection:
    """
    Backwards-compatible initializer. Returns a connection.
    Also initializes a small pool used for query execution.
    """
    global DUCK_CONN, _DUCK_POOL

    if DUCK_CONN is not None and _DUCK_POOL is not None:
        return DUCK_CONN

    with DUCK_INIT_LOCK:
        if DUCK_CONN is None:
            DUCK_CONN = duckdb.connect(str(DUCKDB_PATH), read_only=False)
            _apply_duck_pragmas(DUCK_CONN)

        if _DUCK_POOL is None:
            import queue
            _DUCK_POOL = queue.Queue(maxsize=DUCK_POOL_SIZE)

            # Pool connections: keep them read_only=False so VIEW refresh works without re-opening.
            # If you want strict separation, set these to read_only=True and keep DUCK_CONN for writes.
            for _ in range(DUCK_POOL_SIZE):
                c = duckdb.connect(str(DUCKDB_PATH), read_only=False)
                _apply_duck_pragmas(c)
                _DUCK_POOL.put(c)

    return DUCK_CONN

# ✅ New names you referenced in lifespan/reload — now they exist.
def ensure_duck_write_conn() -> duckdb.DuckDBPyConnection:
    # For low-memory mode, we reuse DUCK_CONN as the writer.
    return ensure_duck_conn()

def _close_all_duck_read_conns():
    # In low-memory mode, pool conns are the "read conns".
    global _DUCK_POOL, DUCK_CONN
    if _DUCK_POOL is not None:
        try:
            while True:
                c = _DUCK_POOL.get_nowait()
                try:
                    c.close()
                except Exception:
                    pass
        except Exception:
            pass
        _DUCK_POOL = None

def duck_fetch_df(sql: str, params: Optional[List[Any]] = None, use_writer: bool = False) -> pd.DataFrame:
    """
    Executes a DuckDB query using a pooled connection.
    This avoids opening many connections (OOM) while still preventing one slow query from blocking *all* code paths.
    """
    if params is None:
        params = []

    ensure_duck_conn()

    # Writer mode: serialize through init lock (DDL during reload)
    if use_writer:
        with DUCK_INIT_LOCK:
            df = DUCK_CONN.execute(sql, params).fetchdf()
            return df_sanitize_for_json(df)

    # Read mode: use pool
    c = None
    try:
        c = _DUCK_POOL.get(timeout=float(os.getenv("DUCK_POOL_TIMEOUT", "60")))
        # Use a cursor for isolation of statement state
        df = c.cursor().execute(sql, params).fetchdf()
        return df_sanitize_for_json(df)
    finally:
        if c is not None:
            try:
                _DUCK_POOL.put(c)
            except Exception:
                pass




# ✅ FIX: SINGLE SOURCE OF TRUTH STARTUP
# ✅ FIX: SINGLE SOURCE OF TRUTH STARTUP
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("API starting up (Hybrid V5 Mode)...")

    # ✅ OOM-friendly thread limiter (don’t set too high on small Render instances)
    try:
        limiter = anyio.to_thread.current_default_thread_limiter()
        limiter.total_tokens = int(os.getenv("ANYIO_THREADS", "12"))
        logger.info("AnyIO thread limiter set to %s", limiter.total_tokens)
    except Exception:
        logger.exception("Failed to set AnyIO thread limiter")

    # ✅ Initialize DuckDB + pool
    try:
        ensure_duck_conn()
        logger.info("DuckDB connected successfully: %s", str(DUCKDB_PATH))
    except Exception:
        logger.exception("DuckDB init failed")

    # ✅ Background reload
    async def safe_reload():
        try:
            logger.info("Triggering background reload...")
            await asyncio.to_thread(reload_all_data)
        except Exception:
            logger.exception("Background reload failed")

    asyncio.create_task(safe_reload())

    yield

    logger.info("API shutting down...")
    gc.collect()

    # Close pool conns first, then base conn
    _close_all_duck_read_conns()

    global DUCK_CONN
    if DUCK_CONN is not None:
        try:
            DUCK_CONN.close()
        except Exception:
            pass
        DUCK_CONN = None

    # Best-effort close of any thread-local read conns we created
    _close_all_duck_read_conns()


SAFE_IDENT_RE = re.compile(r"^[A-Z0-9_ \-./]{1,200}$")

def safe_int(v: Any, default: int = 0, min_v: int = 0, max_v: int = 10_000_000) -> int:
    try:
        n = int(v)
    except Exception:
        n = int(default)
    if n < min_v:
        return min_v
    if n > max_v:
        return max_v
    return n

MAX_JSON_ROWS = safe_int(os.getenv("MAX_JSON_ROWS", 2000), 2000, 100, 200_000)

def df_sanitize_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make a DataFrame safe for JSON serialization:
    - NaN/NaT -> None
    - +inf/-inf -> None
    - keep types as object so None is preserved
    """
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()

    # Replace inf/-inf first
    df = df.replace([np.inf, -np.inf], None)

    # Convert to object so None can exist
    df = df.astype(object)

    # Replace NaN/NaT with None
    df = df.where(pd.notnull(df), None)

    return df



def safe_years(years: Optional[List[int]], min_year: int = 1900, max_year: int = 2100, max_len: int = 50) -> List[int]:
    if not years:
        return []
    out: List[int] = []
    for y in years:
        try:
            yi = int(y)
        except Exception:
            continue
        if min_year <= yi <= max_year:
            out.append(yi)
    out = list(dict.fromkeys(out))[:max_len]
    return out

def sql_literal(s: Any) -> str:
    # Athena/Presto/Trino style: escape single quotes by doubling.
    if s is None:
        return "''"
    t = str(s)
    return "'" + t.replace("'", "''") + "'"

def sql_like_contains(s: Any) -> str:
    """
    Sanitizes a string for use in a LIKE clause.
    We escape the special characters % and _ using '#'.
    """
    if s is None:
        raw = ""
    else:
        raw = str(s)
    
    # 1. Escape the escape char itself first (# -> ##)
    # 2. Then escape the wildcards (% -> #%, _ -> #_)
    # 3. Finally escape single quotes for the SQL literal (' -> '')
    safe_str = raw.replace("#", "##").replace("%", "#%").replace("_", "#_").replace("'", "''")
    
    return f" '%{safe_str}%' "

def safe_ident(s: Any) -> str:
    t = ("" if s is None else str(s)).strip().upper()
    if not t:
        return ""
    if not SAFE_IDENT_RE.match(t):
        raise HTTPException(status_code=400, detail="Invalid identifier")
    return t

def safe_contains_upper(field_sql: str, user_value: Any) -> str:
    """
    Generates: upper(field) LIKE '%VALUE%' ESCAPE '#'
    """
    v = "" if user_value is None else str(user_value).upper()
    # ✅ FIX: Use '#' as escape char to avoid Python backslash issues
    return f"upper({field_sql}) LIKE {sql_like_contains(v)} ESCAPE '#'"

def safe_equals_upper(field_sql: str, user_value: Any) -> str:
    v = "" if user_value is None else str(user_value)
    return f"upper({field_sql}) = {sql_literal(v.upper())}"

class TTLQueryCache:
    def __init__(self, maxsize: int = 256, ttl_seconds: int = 60):
        self.maxsize = maxsize
        self.ttl = ttl_seconds
        self._lock = threading.RLock()
        self._data: Dict[str, Tuple[float, Any]] = {}

    def get(self, key: str) -> Optional[Any]:
        now = time.time()
        with self._lock:
            item = self._data.get(key)
            if not item:
                return None
            exp, val = item
            if exp < now:
                self._data.pop(key, None)
                return None
            return val

    def set(self, key: str, val: Any):
        now = time.time()
        with self._lock:
            if len(self._data) >= self.maxsize:
                # drop random key to keep O(1)
                k = next(iter(self._data.keys()), None)
                if k is not None:
                    self._data.pop(k, None)
            self._data[key] = (now + self.ttl, val)

ATHENA_CACHE = TTLQueryCache(maxsize=safe_int(os.getenv("ATHENA_CACHE_MAX", 256), 256, 16, 5000),
                            ttl_seconds=safe_int(os.getenv("ATHENA_CACHE_TTL", 60), 60, 1, 3600))
NEWS_CACHE = TTLQueryCache(
    maxsize=safe_int(os.getenv("NEWS_CACHE_MAX", 512), 512, 16, 5000),
    ttl_seconds=safe_int(os.getenv("NEWS_CACHE_TTL", 600), 600, 5, 86400)
)



app = FastAPI(
    title="Mimir Hybrid Intelligence API - Instant Mode V5",
    default_response_class=JSONResponse,
    lifespan=lifespan 
)

# ✅ FIX: Add a middleware to catch NaNs in responses or handle it in your dataframes
# The safest surgical fix without rewriting every endpoint is to enforce simplejson or handle it in the response class.
# However, for FastAPI/Starlette, the easiest fix is to patch the dataframe conversion or use a custom JSON encoder.

# Better yet, let's fix it at the source in `reload_all_data` and your query functions.
# But since you asked for a SURGICAL fix for the error log, here is a patch you can add 
# right after `app = FastAPI(...)`

from fastapi.encoders import jsonable_encoder

@app.middleware("http")
async def sanitize_nan_responses(request: Request, call_next):
    try:
        response = await call_next(request)
        return response
    except ValueError as e:
        msg = str(e).lower()
        if ("nan" in msg) or ("out of range float values" in msg) or ("inf" in msg):
            logger.error(f"NaN Detection: {request.url.path} returned NaNs. Please fix data source.")
            # We can't easily fix the stream here, but it prevents the hard crash log loop.
            return JSONResponse(
                status_code=500, 
                content={"error": "Data formatting error (NaN values detected). Please reload."}
            )
        raise e

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    try:
        response = await call_next(request)
        return response
    finally:
        dur_ms = (time.time() - start) * 1000.0
        try:
            logger.info("%s %s -> %s (%.1fms)",
                        request.method,
                        request.url.path,
                        getattr(locals().get("response", None), "status_code", "NA"),
                        dur_ms)
        except Exception:
            pass

# ==========================================
# CORS CONFIGURATION (Updated)
# ==========================================

# 1. Define your trusted domains (Hardcoded Fallback)
default_origins = [
    "https://market-intel-mc87mey5f-tom-pettersons-projects.vercel.app", # Your specific deployment
    "https://market-intel-ui.vercel.app",                                # Your production alias
    "https://market-intel-ui-git-main-tom-pettersons-projects.vercel.app", # Your git branch alias
    "https://mimiradvisors.org",                                         # Your main domain
    "https://www.mimiradvisors.org",
    "http://localhost:3000",                                               # Local development
    "http://127.0.0.1:3000",                                               # Local development
    "http://localhost:5173"                                                # Local development (Vite)
]

# 2. Check for Environment Variable override (Optional)
cors_env = os.getenv("CORS_ORIGINS", "")
if cors_env:
    # If you set CORS_ORIGINS in Render, use that instead
    origins = [o.strip() for o in cors_env.split(",") if o.strip()]
else:
    # Otherwise, use the hardcoded list above
    origins = default_origins

# 3. Optional escape hatch for quick testing:
allow_all = os.getenv("CORS_ALLOW_ALL", "false").lower() == "true"
if allow_all:
    origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    # Never allow credentials with wildcard origin
    allow_credentials=False if origins == ["*"] else True,
    allow_methods=["GET", "POST", "HEAD", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)


# --- HEALTH CHECK ---
@app.get("/live")
def live_check():
    return {"status": "ok"}

def get_readiness_state() -> Dict[str, Any]:
    cache = GLOBAL_CACHE

    is_loading = bool(cache.get("is_loading", False))
    last_loaded = cache.get("last_loaded", 0)

    profiles_ok = not cache.get("profiles_df", pd.DataFrame()).empty
    duck_ok = DUCK_CONN is not None

    # geo_df is now handled by DuckDB, so we remove it from the strict readiness check
    ready = bool(duck_ok and profiles_ok and (not is_loading) and (last_loaded and last_loaded > 0))

    return {
        "ready": ready,
        "duck_ok": bool(duck_ok),
        "geo_ok": True, # Hardcoded to true for backwards compatibility with UI
        "profiles_ok": bool(profiles_ok),
        "is_loading": bool(is_loading),
        "last_loaded": last_loaded,
    }


@app.get("/ready")
def ready_check():
    s = get_readiness_state()
    return {
        "status": "ok" if s["ready"] else "starting",
        "ready": bool(s["ready"]),
        "is_loading": bool(s["is_loading"]),
        "last_loaded": s["last_loaded"],
        "duck_ok": bool(s["duck_ok"]),
        "geo_ok": bool(s["geo_ok"]),
        "profiles_ok": bool(s["profiles_ok"]),
    }


@app.get("/")
def health_check():
    return {"status": "ok", "message": "Mimir V5 is Live", "ready_endpoint": "/ready"}

@app.head("/")
def health_check_head():
    return {"status": "ok"}


# Bucket & Athena Config
raw_bucket = os.getenv('ATHENA_OUTPUT_BUCKET', 'a-and-d-intel-lake-newaccount')
BUCKET_NAME = raw_bucket.replace('s3://', '').split('/')[0]
CACHE_PREFIX = "app_cache/"
DATABASE = 'market_intel_gold'
SUMMARY_PARQUET_CLEAN = "summary_clean.parquet"

# Detect Environment
IS_PRODUCTION = os.getenv('RENDER') or os.getenv('IS_PROD')

# AWS Clients
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
session = boto3.Session(region_name=AWS_REGION)

BOTO_CFG = BotoConfig(
    retries={"max_attempts": safe_int(os.getenv("AWS_MAX_ATTEMPTS", 10), 10, 1, 50), "mode": "standard"},
    connect_timeout=safe_int(os.getenv("AWS_CONNECT_TIMEOUT", 5), 5, 1, 60),
    read_timeout=safe_int(os.getenv("AWS_READ_TIMEOUT", 60), 60, 1, 600),
)

s3 = session.client("s3", config=BOTO_CFG)
athena = session.client("athena", config=BOTO_CFG)


RELOAD_LOCK = threading.Lock()

def get_cache_snapshot() -> Dict[str, Any]:
    # Atomic pointer swap means reading GLOBAL_CACHE is safe without a lock.
    cache = GLOBAL_CACHE

    # DUCK_CONN is protected by DUCK_LOCK
    with DUCK_LOCK:
        conn = DUCK_CONN

    # return references; handlers should not mutate
    return {
        "GLOBAL_CACHE": cache,
        "DUCK_CONN": conn,
    }




# --- HELPER: Sanitize Inputs ---
def sanitize(input_str: Optional[str]) -> str:
    if not input_str: return ""
    return input_str.replace("'", "").replace(";", "").replace("--", "").strip().upper()

def cached_athena_query(query: str):
    key = query.strip()
    hit = ATHENA_CACHE.get(key)
    if hit is not None:
        return hit
    res = run_athena_query(key)
    ATHENA_CACHE.set(key, res)
    return res

def _cancel_athena_query(qid: Optional[str]):
    if not qid:
        return
    try:
        athena.stop_query_execution(QueryExecutionId=qid)
        logger.warning("Athena query cancelled qid=%s", qid)
    except Exception:
        # best effort
        pass


def run_athena_query(query: str):
    if not query or not str(query).strip():
        return []

    start_ts = time.time()
    qid: Optional[str] = None

    try:
        resp = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": DATABASE},
            ResultConfiguration={"OutputLocation": f"s3://{BUCKET_NAME}/temp_api_queries/"},
        )
        qid = resp["QueryExecutionId"]

        final_state = "UNKNOWN"
        status = None

        max_wait_s = safe_int(os.getenv("ATHENA_MAX_WAIT_SECONDS", 60), 60, 5, 600)
        poll_interval_s = float(os.getenv("ATHENA_POLL_INTERVAL", "0.5") or "0.5")
        deadline = time.time() + max_wait_s

        while time.time() < deadline:
            try:
                status = athena.get_query_execution(QueryExecutionId=qid)
            except ClientError as e:
                code = (e.response.get("Error", {}) or {}).get("Code", "")
                if code in {"ThrottlingException", "TooManyRequestsException"}:
                    time.sleep(min(2.0, poll_interval_s) + random.random() * 0.25)
                    continue
                raise

            final_state = status["QueryExecution"]["Status"]["State"]
            if final_state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
            time.sleep(poll_interval_s)

        if final_state not in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            final_state = "TIMED_OUT"
            _cancel_athena_query(qid)
            logger.error("Athena query timed out qid=%s wait_s=%s", qid, max_wait_s)
            return []

        if final_state != "SUCCEEDED":
            reason = ""
            try:
                reason = (status["QueryExecution"]["Status"] or {}).get("StateChangeReason", "") if status else ""
            except Exception:
                reason = ""
            _cancel_athena_query(qid)
            logger.error("Athena query failed state=%s qid=%s reason=%s", final_state, qid, reason)
            return []

        # ✅ SUCCESS PATH
        outloc = status["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
        key = outloc.replace(f"s3://{BUCKET_NAME}/", "")
        
        # 1. Get object from S3
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        
        # 2. Read into DataFrame (This line was likely missing/skipped causing your error)
        df = pd.read_csv(BytesIO(obj["Body"].read()))
        
        # 3. Clean NaN values (The Fix)
        # We replace NaN with None so JSON conversion works
        df = df.replace([np.inf, -np.inf], None)
        df = df.astype(object).where(pd.notnull(df), None)
        
        # 4. Convert to Dict
        out = df.to_dict(orient="records")

        logger.info("Athena query ok qid=%s rows=%s dur_ms=%.1f",
                    qid, len(out), (time.time() - start_ts) * 1000.0)
        return out

    except Exception:
        _cancel_athena_query(qid)
        logger.exception("Athena query exception qid=%s", qid)
        return []


ALLOWED_ORDER_BY = {
    "action_date", "year", "total_spend", "spend_amount", "total_revenue", "subaward_value"
}

def get_subset_from_disk(
    filename: str,
    where_clause: str = "1=1",
    params: tuple = (),
    columns_sql: str = "*",
    order_by_sql: str = "",
    limit: int = 0,
    offset: int = 0,
) -> pd.DataFrame:
    # 1. Strict SQL Injection Check on columns
    if columns_sql != "*":
        cols = [c.strip() for c in columns_sql.split(",")]
        for c in cols:
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", c):
                logger.error(f"Security Alert: Invalid column detected {c}")
                raise HTTPException(status_code=400, detail="Invalid columns selection")

    # 2. Strict SQL Injection Check on WHERE clause
    if ";" in where_clause or "--" in where_clause:
         logger.error(f"Security Alert: Injection attempt in WHERE: {where_clause}")
         raise HTTPException(status_code=400, detail="Invalid query format")

    global DUCK_CONN
    try:
        path = LOCAL_CACHE_DIR / filename
        if not path.exists():
            return pd.DataFrame()

        limit = max(0, int(limit or 0))
        offset = max(0, int(offset or 0))
        
        # Hard cap: Prevent massive JSON serialization payloads
        if limit > MAX_JSON_ROWS:
            limit = MAX_JSON_ROWS

        # Validate ORDER BY
        order_clause = ""
        if order_by_sql:
            parts = order_by_sql.strip().split()
            col = parts[0]
            if col not in ALLOWED_ORDER_BY:
                 col = "action_date"
            
            direction = parts[1].upper() if len(parts) > 1 else "DESC"
            if direction not in {"ASC", "DESC"}: 
                direction = "DESC"
            
            order_clause = f" ORDER BY {col} {direction}"

        # 3. Construct Query
        sql = f"SELECT {columns_sql} FROM read_parquet(?) WHERE {where_clause}{order_clause}"
        
        local_params = list(params)
        if limit > 0:
            sql += " LIMIT ?"
            local_params.append(limit)
        if offset > 0:
            sql += " OFFSET ?"
            local_params.append(offset)

        # 4. Execute using Global Connection (Thread-Locked)
        with DUCK_LOCK:
            ensure_duck_conn()
            all_params = (str(path),) + tuple(local_params)
            df = DUCK_CONN.execute(sql, all_params).fetchdf()
            
            # ✅ FIX: Sanitize NaN values to ensure valid JSON
            return df_sanitize_for_json(df)

    except Exception:
        logger.exception(f"DuckDB Query Failed: {filename}")
        return pd.DataFrame()
    

def _like_param_contains(val: str) -> str:
    """
    Returns a parameter value for: upper(col) LIKE ? ESCAPE '\\'
    Uses your existing sql_like_contains() which returns a SQL literal string.
    We want the raw param, so we build it ourselves.
    """
    raw = "" if val is None else str(val)
    raw = raw.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{raw.upper()}%"

def build_summary_where(
    years: Optional[List[int]], 
    filters: Dict[str, Optional[Any]],
    use_fy_logic: bool = False  # Kept for compatibility, but no longer needed
) -> Tuple[str, List[Any]]:
    """
    Returns (where_sql, params) for summary.parquet queries.
    All comparisons are done in UPPER space.
    """
    where_parts: List[str] = ["1=1"]
    params: List[Any] = []

    # --- YEAR / FY FILTERING ---
    if years and len(years) > 0:
        ys = safe_years(years, min_year=1900, max_year=2200, max_len=50)
        if ys:
            placeholders = ','.join(['?' for _ in ys])
            # ✅ The 'year' column is now permanently Fiscal Year in the data!
            where_parts.append(f"year IN ({placeholders})")
            params.extend(ys)

    threshold_m = filters.get("threshold_m")
    if threshold_m is not None:
        try:
            thresh_val = float(threshold_m) * 1_000_000
            if thresh_val > 1_000_000: # Only filter if they moved the slider up
                where_parts.append("total_spend >= ?")
                params.append(thresh_val)
        except (ValueError, TypeError):
            pass

    # --- HELPERS ---
    def eq(col: str, v: str):
        where_parts.append(f"upper({col}) = ?")
        params.append(str(v).strip().upper())

    def contains(col: str, v: str):
        where_parts.append(f"upper({col}) LIKE ? ESCAPE '\\'")
        params.append(_like_param_contains(v))

    # --- STANDARD FILTERS ---
    vendor = filters.get("vendor")
    parent = filters.get("parent")
    cage = filters.get("cage")
    domain = filters.get("domain")
    agency = filters.get("agency")
    platform = filters.get("platform")
    psc = filters.get("psc")

    if vendor:
        contains("vendor_name", vendor)

    if parent:
        eq("clean_parent", parent)

    if cage:
        eq("cage_code", cage)

    if domain:
        eq("market_segment", domain)

    if agency:
        eq("sub_agency", agency)

    if platform:
        eq("platform_family", platform)

    if psc:
        where_parts.append("(upper(psc_code) LIKE ? ESCAPE '\\' OR upper(psc_description) LIKE ? ESCAPE '\\')")
        p = _like_param_contains(psc)
        params.extend([p, p])

    return " AND ".join(where_parts), params


def query_summary_df(
    where_sql: str,
    params: List[Any],
    select_sql: str,
    group_by_sql: str = "", 
    order_by_sql: str = "",
    limit: int = 0,
    offset: int = 0,
    ignore_cap: bool = False # ✅ NEW PARAMETER
) -> pd.DataFrame:
    """
    Runs a DuckDB query against summary.parquet without loading the full file into RAM.
    """
    global DUCK_CONN
    path = LOCAL_CACHE_DIR / SUMMARY_PARQUET_CLEAN
    if not path.exists():
        return pd.DataFrame()

    limit = max(0, int(limit or 0))
    offset = max(0, int(offset or 0))
    
    # ✅ FIX: Only enforce the global cap if ignore_cap is False
    if not ignore_cap and limit > MAX_JSON_ROWS:
        limit = MAX_JSON_ROWS

    # ✅ OPTIMIZED: Query the pre-built memory view instead of reading the parquet file
    sql = f"SELECT {select_sql} FROM v_summary WHERE {where_sql}"
    
    if group_by_sql:
        sql += f" GROUP BY {group_by_sql}"
        
    if order_by_sql:
        sql += f" ORDER BY {order_by_sql}"
        
    local_params = list(params)

    if limit > 0:
        sql += " LIMIT ?"
        local_params.append(limit)
    if offset > 0:
        sql += " OFFSET ?"
        local_params.append(offset)

    # ✅ OPTIMIZED: Use the connection pool (no locks!) so queries run simultaneously
    df = duck_fetch_df(sql, local_params)
    return df



# --- FILTER ENGINE (Optimized / Vectorized) ---
class FilterEngine:
    @staticmethod
    def apply_pandas(
        df: pd.DataFrame,
        years: Optional[List[int]],
        filters: Dict[str, Optional[str]]
    ) -> pd.DataFrame:
        if df.empty: return df

        # 1) Base year filter (Vectorized isin is fast)
        if years and len(years) > 0:
            mask = df["year"].isin(years)
        else:
            mask = pd.Series(True, index=df.index)

        # 2) Dynamic filters
        # We iterate only if the filter has a value
        for param, val in (filters or {}).items():
            if not val: continue
            
            # Sanitize once.
            clean_upper = str(val).strip().upper()
            if clean_upper == "": continue

            # --- PARENT LOGIC ---
            if param == "parent":
                col = 'clean_parent' if 'clean_parent' in df.columns else 'ultimate_parent_name'
                if col in df.columns:
                    # Exact match is 100x faster than contains
                    mask &= (df[col] == clean_upper)
                continue

            # --- VENDOR LOGIC ---
            if param == "vendor":
                if "vendor_name" in df.columns:
                    # Optimized: ETL guarantees string/upper. Use vectorized string search.
                    # Note: We use contains() because users might search "BOEING" to find "BOEING CO"
                    mask &= df["vendor_name"].str.contains(clean_upper, regex=False, na=False)
                continue
            
            # --- CAGE LOGIC ---
            if param == "cage":
                if "cage_code" in df.columns:
                    # Exact match for CAGE is preferred and much faster
                    mask &= (df["cage_code"] == clean_upper)
                continue

            # --- PSC LOGIC ---
            if param == "psc":
                psc_mask = pd.Series(False, index=df.index)
                if "psc_code" in df.columns:
                    psc_mask |= df["psc_code"].str.contains(clean_upper, regex=False, na=False)
                if "psc_description" in df.columns:
                    psc_mask |= df["psc_description"].str.contains(clean_upper, regex=False, na=False)
                mask &= psc_mask
                continue

            # --- CATEGORICAL EXACT MATCHES ---
            # Used for: agency, platform, domain (market_segment)
            col_map = {
                "domain": "market_segment",
                "agency": "sub_agency",
                "platform": "platform_family"
            }
            col_name = col_map.get(param)
            
            if col_name and col_name in df.columns:
                # Direct equality check is optimized for Categories
                mask &= (df[col_name] == clean_upper)

        return df[mask]
    
# --- HELPER: Parent Aggregation Logic ---
# --- HELPER: Parent Aggregation Logic ---
# [Find and Replace get_parent_aggregate_stats in api.py]

# [Find and Replace in api.py]

def get_parent_aggregate_stats(parent_name: str, years: Optional[List[int]] = None):
    if not parent_name:
        return None

    clean = parent_name.strip().upper().replace("'", "")

    filters = {"parent": clean}
    where_sql, params = build_summary_where(years, filters)

    totals = query_summary_df(
        where_sql, params,
        select_sql="sum(total_spend) as total_spend, sum(contract_count) as contract_count, max(year) as last_active",
        limit=1
    )
    if totals.empty:
        return None

    # Safely extract and check for NaN before casting
    ts_val = totals["total_spend"].iloc[0] if "total_spend" in totals.columns else 0.0
    total_spend = 0.0 if pd.isna(ts_val) else float(ts_val)

    tc_val = totals["contract_count"].iloc[0] if "contract_count" in totals.columns else 0
    total_contracts = 0 if pd.isna(tc_val) else int(tc_val)
    last_active = int(totals["last_active"].iloc[0]) if "last_active" in totals.columns and pd.notna(totals["last_active"].iloc[0]) else 0

    naics = query_summary_df(
        where_sql, params,
        select_sql="naics_code, naics_description, count(*) as n",
        group_by_sql="naics_code, naics_description",
        order_by_sql="n DESC",
        limit=5
    )
    top_naics: List[str] = []
    if not naics.empty and "naics_code" in naics.columns:
        # Vectorized string formatting
        naics["naics_code"] = naics["naics_code"].astype(str).str.strip()
        
        if "naics_description" in naics.columns:
            naics["naics_description"] = naics["naics_description"].astype(str).str.strip()
            
            # Create boolean mask for valid descriptions
            valid_desc = (naics["naics_description"] != "") & (naics["naics_description"].str.lower() != "nan")
            
            # Apply formatting conditionally without loops
            naics.loc[valid_desc, "formatted"] = naics["naics_code"] + " - " + naics["naics_description"]
            naics.loc[~valid_desc, "formatted"] = naics["naics_code"]
            
            top_naics = naics["formatted"].tolist()
        else:
            top_naics = naics["naics_code"].tolist()

    plats = query_summary_df(
        where_sql, params,
        select_sql="platform_family, sum(total_spend) as spend",
        group_by_sql="platform_family",
        order_by_sql="spend DESC",
        limit=5
    )
    top_platforms = plats["platform_family"].dropna().astype(str).tolist() if ("platform_family" in plats.columns and not plats.empty) else []

    if total_spend <= 0:
        return None

    return {
        "total_obligations": float(total_spend),
        "total_contracts": int(total_contracts),
        "last_active": int(last_active),
        "top_naics": top_naics,
        "top_platforms": top_platforms
    }

def _calc_child_kpis_from_kpis_disk(cage_code: str, years: Optional[List[int]] = None) -> Dict[str, Any]:
    cage_code = (cage_code or "").strip().upper()
    ys = safe_years(years, min_year=1900, max_year=2200, max_len=50)

    kpis_path = (GLOBAL_CACHE.get("kpis_path") or "").strip()
    if not cage_code or not kpis_path or not os.path.exists(kpis_path):
        return {"has_kpis": False}

    # Prefer querying v_kpis if you created it; otherwise read_parquet(kpis_path).
    # This query never loads the full parquet into RAM.
    where_year_sql = ""
    params: List[Any] = [cage_code]

    if ys:
        placeholders = ",".join(["?"] * len(ys))
        where_year_sql = f" AND year IN ({placeholders})"
        params.extend(ys)

    try:
        df = duck_fetch_df(
            f"""
            SELECT
                SUM(total_spend) AS total_obligations,
                SUM(contract_count) AS total_contracts,
                MAX(year) AS last_active
            FROM v_kpis
            WHERE cage_code = ? {where_year_sql}
            """,
            params=params,
        )

        if df.empty:
            # view exists but no rows
            return {"has_kpis": True, "total_obligations": 0.0, "total_contracts": 0, "last_active": 0}

        # Safely extract and check for NaN before casting
        ob_val = df["total_obligations"].iloc[0] if "total_obligations" in df.columns else 0.0
        total_ob = 0.0 if pd.isna(ob_val) else float(ob_val)

        ct_val = df["total_contracts"].iloc[0] if "total_contracts" in df.columns else 0
        total_ct = 0 if pd.isna(ct_val) else int(ct_val)

        last_active = 0
        if "last_active" in df.columns and pd.notna(df["last_active"].iloc[0]):
            try:
                last_active = int(df["last_active"].iloc[0])
            except Exception:
                last_active = 0

        return {
            "has_kpis": True,
            "total_obligations": total_ob,
            "total_contracts": total_ct,
            "last_active": last_active,
        }

    except Exception:
        logger.exception("kpis disk calc failed cage=%s", cage_code)
        return {"has_kpis": False}


def reload_all_data():
    # Lock is the single source of truth for in-progress reloads
    if not RELOAD_LOCK.acquire(blocking=False):
        logger.info("Reload already in progress, skipping.")
        return

    # ✅ SURGICAL FIX 1: Declare global immediately so we can read from it safely
    global GLOBAL_CACHE 

    try:
        # Update loading state immediately
        GLOBAL_CACHE = {**GLOBAL_CACHE, "is_loading": True}
    except Exception:
        pass

    try:
        logger.info("STARTING DATA LOAD (Chunked + RAM Optimized)...")

        # 1. PREPARE TEMPORARY STATE
        new_global_cache = {
            "is_loading": True,
            "last_loaded": GLOBAL_CACHE.get("last_loaded", 0),
            "options": {},
            "cage_name_map": {},
            "location_map": {},
            "search_index": [],
            "naics_map": {},
            "df": pd.DataFrame(),
            "geo_df": pd.DataFrame(),
            "profiles_df": pd.DataFrame(),
            "risk_df": pd.DataFrame(),
            "kpis_path": None,
            "df_opportunities": pd.DataFrame()
        }

        # 2. DOWNLOAD FILES
        LOCAL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        files = [
            "products.parquet", "summary.parquet", "geo.parquet", "cage_locations.parquet",
            "profiles.parquet", "risk.parquet", "network.parquet",
            "subcontract_descriptions.parquet",
            "transactions.parquet", "opportunities.parquet", "kpis.parquet",
            "nsn_summary.parquet",
            "nsn_profile_lookup.parquet",
            "nsn_supplier_lookup.parquet",
            "nsn_cage_reference.parquet",
            "platform_bom.parquet" # unrelated to NSN/CAGE reference, leave only if another feature uses it
        ]

        def fetch_file(filename: str) -> str:
            final_path = (LOCAL_CACHE_DIR / filename).resolve()

            # ✅ ADD THIS BLOCK: Skip download if we are in dev mode and file exists
            if os.getenv("SKIP_DOWNLOADS") == "1" and final_path.exists():
                logger.info(f"⏭️ DEV MODE: Skipping {filename} download (already exists locally).")
                return filename
            
            tmp_path = (LOCAL_CACHE_DIR / f"{filename}.tmp").resolve()

            logger.info("Downloading %s...", filename)
            try:
                if tmp_path.exists():
                    try:
                        tmp_path.unlink()
                    except Exception:
                        pass

                s3_local = boto3.client("s3", region_name=AWS_REGION, config=BOTO_CFG)
                s3_local.download_file(BUCKET_NAME, f"{CACHE_PREFIX}{filename}", str(tmp_path))
                tmp_path.replace(final_path)

            except Exception as e:
                logger.error(f"Download failed for {filename}: {e}")
                try:
                    if tmp_path.exists():
                        tmp_path.unlink()
                except Exception:
                    pass
            return filename

        def fetch_prefix(prefix: str, local_dir: Path) -> str:
            """
            Downloads all parquet parts under a prefix into a local folder.
            Used for contracts_rolled dataset.
            """

            # ✅ ADD THIS BLOCK: Skip download if folder has files and we are in dev mode
            if os.getenv("SKIP_DOWNLOADS") == "1" and local_dir.exists() and any(local_dir.iterdir()):
                logger.info(f"⏭️ DEV MODE: Skipping {prefix} download (already exists locally).")
                return prefix
            
            logger.info("Downloading prefix %s -> %s ...", prefix, str(local_dir))
            local_dir.mkdir(parents=True, exist_ok=True)

            # ✅ Clear old parts so we don't mix stale/new parquet files
            try:
                for p in local_dir.glob("*.parquet"):
                    try:
                        p.unlink()
                    except Exception:
                        pass
            except Exception:
                pass

            s3_local = boto3.client("s3", region_name=AWS_REGION, config=BOTO_CFG)
            paginator = s3_local.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)

            downloaded = 0
            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj.get("Key")
                    if not key or not key.endswith(".parquet"):
                        continue

                    basename = os.path.basename(key)
                    final_path = (local_dir / basename).resolve()
                    tmp_path = (local_dir / f"{basename}.tmp").resolve()

                    try:
                        if tmp_path.exists():
                            try:
                                tmp_path.unlink()
                            except Exception:
                                pass

                        s3_local.download_file(BUCKET_NAME, key, str(tmp_path))
                        tmp_path.replace(final_path)
                        downloaded += 1
                    except Exception as e:
                        logger.error("Prefix download failed for %s: %s", key, e)

            logger.info("Downloaded %d parquet parts from %s", downloaded, prefix)
            return prefix

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # Download all the single-file cache artifacts
            list(executor.map(fetch_file, files))

        # contracts_rolled can be either:
        #  A) single file: app_cache/contracts_rolled.parquet
        #  B) dataset folder: app_cache/contracts_rolled/*.parquet
        #
        # Try A first; if missing, download B.
        try:
            fetch_file("contracts_rolled.parquet")
        except Exception:
            pass

        local_single = (LOCAL_CACHE_DIR / "contracts_rolled.parquet").resolve()
        local_folder = (LOCAL_CACHE_DIR / "contracts_rolled").resolve()

        if not local_single.exists():
            fetch_prefix(f"{CACHE_PREFIX}contracts_rolled/", local_folder)

        kpis_local = (LOCAL_CACHE_DIR / "kpis.parquet").resolve()
        if kpis_local.exists():
            new_global_cache["kpis_path"] = str(kpis_local)
        else:
            new_global_cache["kpis_path"] = None
        
        # 3. REFRESH DUCKDB VIEWS (Using writer connection)
        with DUCK_INIT_LOCK:
            conn = ensure_duck_conn()

            # ✅ NEW: Define all files that should be handled by DuckDB instead of RAM
            # ✅ NEW: Define all files that should be handled by DuckDB instead of RAM
            views_to_create = [
                ("v_products", "products.parquet"),
                ("v_profiles", "profiles.parquet"),
                ("v_transactions", "transactions.parquet"),
                # v_contracts_rolled handled specially below (file OR folder)
                ("v_network", "network.parquet"),
                ("v_geo", "geo.parquet"),
                ("v_cage_locations", "cage_locations.parquet"),
                ("v_risk", "risk.parquet"),
                ("v_opportunities", "opportunities.parquet"),
                ("v_nsn_summary", "nsn_summary.parquet"),
                ("v_nsn_profile_lookup", "nsn_profile_lookup.parquet"),
                ("v_nsn_supplier_lookup", "nsn_supplier_lookup.parquet"),
                ("v_nsn_cage_reference", "nsn_cage_reference.parquet"),
                ("v_platform_bom", "platform_bom.parquet") # ✅ Added here
            ]
            
            for view_name, file_name in views_to_create:
                file_path = str((LOCAL_CACHE_DIR / file_name).resolve())
                if os.path.exists(file_path):
                    conn.execute(f"DROP VIEW IF EXISTS {view_name};")
                    conn.execute(f"DROP TABLE IF EXISTS {view_name};")
                    conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{file_path}');")

            description_path = str(
                (LOCAL_CACHE_DIR / "subcontract_descriptions.parquet").resolve()
            )
            conn.execute("DROP VIEW IF EXISTS v_subcontract_descriptions;")
            conn.execute("DROP TABLE IF EXISTS v_subcontract_descriptions;")
            if os.path.exists(description_path):
                description_columns = {
                    str(row[0]).strip().lower()
                    for row in conn.execute(
                        f"DESCRIBE SELECT * FROM read_parquet('{description_path}')"
                    ).fetchall()
                }

                def description_col_or_null(column_name: str, sql_type: str = "VARCHAR") -> str:
                    if column_name in description_columns:
                        return f"TRY_CAST({column_name} AS {sql_type})"
                    return f"CAST(NULL AS {sql_type})"

                conn.execute(f"""
                    CREATE OR REPLACE VIEW v_subcontract_descriptions AS
                    SELECT
                        CAST(source_report_id AS VARCHAR) AS source_report_id,
                        CAST(source_dedup_key AS VARCHAR) AS source_dedup_key,
                        CAST(description_lookup_key AS VARCHAR) AS description_lookup_key,
                        TRY_CAST(reported_description_count AS INTEGER) AS reported_description_count,
                        {description_col_or_null("equal_value_report_count", "INTEGER")} AS equal_value_report_count,
                        {description_col_or_null("source_record_count", "INTEGER")} AS source_record_count,
                        {description_col_or_null("superseded_source_version_count", "INTEGER")} AS superseded_source_version_count,
                        {description_col_or_null("earliest_reported_action_date", "DATE")} AS earliest_reported_action_date,
                        {description_col_or_null("latest_reported_action_date", "DATE")} AS latest_reported_action_date,
                        {description_col_or_null("report_id")} AS report_id,
                        {description_col_or_null("report_dedup_key")} AS report_dedup_key,
                        {description_col_or_null("report_last_modified_date")} AS report_last_modified_date,
                        {description_col_or_null("report_action_date")} AS report_action_date,
                        {description_col_or_null("report_amount", "DOUBLE")} AS report_amount,
                        {description_col_or_null("report_description")} AS report_description,
                        {description_col_or_null("is_current_source_version", "BOOLEAN")} AS is_current_source_version
                    FROM read_parquet('{description_path}');
                """)
            else:
                conn.execute("""
                    CREATE OR REPLACE VIEW v_subcontract_descriptions AS
                    SELECT
                        CAST(NULL AS VARCHAR) AS source_report_id,
                        CAST(NULL AS VARCHAR) AS source_dedup_key,
                        CAST(NULL AS VARCHAR) AS description_lookup_key,
                        CAST(NULL AS INTEGER) AS reported_description_count,
                        CAST(NULL AS INTEGER) AS equal_value_report_count,
                        CAST(NULL AS INTEGER) AS source_record_count,
                        CAST(NULL AS INTEGER) AS superseded_source_version_count,
                        CAST(NULL AS DATE) AS earliest_reported_action_date,
                        CAST(NULL AS DATE) AS latest_reported_action_date,
                        CAST(NULL AS VARCHAR) AS report_id,
                        CAST(NULL AS VARCHAR) AS report_dedup_key,
                        CAST(NULL AS VARCHAR) AS report_last_modified_date,
                        CAST(NULL AS VARCHAR) AS report_action_date,
                        CAST(NULL AS DOUBLE) AS report_amount,
                        CAST(NULL AS VARCHAR) AS report_description,
                        CAST(NULL AS BOOLEAN) AS is_current_source_version
                    WHERE 1=0;
                """)

            # ✅ Special handling: contracts_rolled -> MATERIALIZED TABLE + INDEX (award speed win)
            # ✅ Revert: contracts_rolled back to a VIEW (no table build, no index build)
            conn.execute("DROP VIEW IF EXISTS v_contracts_rolled;")
            conn.execute("DROP TABLE IF EXISTS contracts_rolled;")  # cleanup from experiment (safe if absent)

            contracts_single = (LOCAL_CACHE_DIR / "contracts_rolled.parquet").resolve()
            contracts_folder = (LOCAL_CACHE_DIR / "contracts_rolled").resolve()

            if contracts_single.exists():
                conn.execute(
                    f"CREATE OR REPLACE VIEW v_contracts_rolled AS "
                    f"SELECT * FROM read_parquet('{str(contracts_single)}');"
                )
            else:
                glob_path = str(contracts_folder / "*.parquet")
                if os.path.exists(str(contracts_folder)) and len(list(contracts_folder.glob("*.parquet"))) > 0:
                    conn.execute(
                        f"CREATE OR REPLACE VIEW v_contracts_rolled AS "
                        f"SELECT * FROM read_parquet('{glob_path}');"
                    )
                else:
                    # Empty view fallback
                    conn.execute("""
                        CREATE OR REPLACE VIEW v_contracts_rolled AS
                        SELECT
                            CAST(NULL AS VARCHAR) AS contract_id,
                            CAST(NULL AS VARCHAR) AS award_key,
                            CAST(NULL AS DATE) AS last_action_date,
                            CAST(0.0 AS DOUBLE) AS total_spend,
                            CAST(NULL AS VARCHAR) AS vendor_name,
                            CAST(NULL AS VARCHAR) AS vendor_cage,
                            CAST(NULL AS VARCHAR) AS sub_agency,
                            CAST(NULL AS VARCHAR) AS parent_agency,
                            CAST(NULL AS VARCHAR) AS description,
                            CAST(NULL AS VARCHAR) AS platform_family,
                            CAST(NULL AS VARCHAR) AS naics_code,
                            CAST(NULL AS VARCHAR) AS psc,
                            CAST(NULL AS VARCHAR) AS nsn,
                            CAST(NULL AS VARCHAR) AS niin,
                            CAST(NULL AS VARCHAR) AS nsn_source_system,
                            CAST(NULL AS VARCHAR) AS nsn_derivation_method,
                            CAST(NULL AS VARCHAR) AS nsn_resolution_status,
                            CAST(NULL AS VARCHAR) AS location_quality,
                            CAST(NULL AS INTEGER) AS year
                        WHERE 1=0;
                    """)

            conn.execute("DROP VIEW IF EXISTS v_subcontracts;")
            network_columns = {
                str(row[0]).strip().lower()
                for row in conn.execute("DESCRIBE v_network").fetchall()
            }
            award_key_expr = (
                "CAST(award_key AS VARCHAR)"
                if "award_key" in network_columns
                else "CAST(NULL AS VARCHAR)"
            )
            source_report_id_expr = (
                "CAST(source_report_id AS VARCHAR)"
                if "source_report_id" in network_columns
                else "CAST(NULL AS VARCHAR)"
            )
            source_report_modified_expr = (
                "CAST(source_report_last_modified_date AS VARCHAR)"
                if "source_report_last_modified_date" in network_columns
                else "CAST(NULL AS VARCHAR)"
            )
            source_dedup_key_expr = (
                "CAST(source_dedup_key AS VARCHAR)"
                if "source_dedup_key" in network_columns
                else "CAST(NULL AS VARCHAR)"
            )
            def optional_network_column(name: str, sql_type: str = "VARCHAR") -> str:
                if name in network_columns:
                    return f"TRY_CAST({name} AS {sql_type})"
                return f"CAST(NULL AS {sql_type})"

            conn.execute(f"""
                CREATE OR REPLACE VIEW v_subcontracts AS
                SELECT
                    {award_key_expr} AS award_key,
                    {source_report_id_expr} AS source_report_id,
                    {source_report_modified_expr} AS source_report_last_modified_date,
                    {source_dedup_key_expr} AS source_dedup_key,
                    {optional_network_column("internal_value_treatment")} AS subcontract_value_treatment,
                    {optional_network_column("included_in_adjusted_total", "BOOLEAN")} AS included_in_adjusted_total,
                    {optional_network_column("prime_award_control_value", "DOUBLE")} AS prime_award_control_value,
                    {optional_network_column("source_report_version_count", "INTEGER")} AS source_report_version_count,
                    {optional_network_column("exact_repeat_count", "INTEGER")} AS exact_repeat_count,
                    {optional_network_column("reported_action_version_count", "INTEGER")} AS reported_action_version_count,
                    {optional_network_column("same_date_description_version_count", "INTEGER")} AS same_date_description_version_count,
                    {optional_network_column("equal_value_description_report_count", "INTEGER")} AS equal_value_description_report_count,
                    'USAspending.gov first-tier subaward reports' AS subcontract_data_source,
                    'Reported subcontract value v3' AS subcontract_methodology,
                    {optional_network_column("sub_cage_resolution")} AS sub_cage_resolution,
                    {optional_network_column("sub_cage_source_period")} AS sub_cage_source_period,
                    {optional_network_column("sub_cage_candidate_count", "INTEGER")} AS sub_cage_candidate_count,
                    {optional_network_column("sub_cage_candidates")} AS sub_cage_candidates,
                    {optional_network_column("subawardee_uei")} AS subcontractor_uei,
                    {optional_network_column("prime_award_description")} AS prime_award_description,
                    CAST(prime_name AS VARCHAR) AS prime_name,
                    CAST(prime_cage AS VARCHAR) AS prime_cage,
                    CAST(contract_id AS VARCHAR) AS prime_award_id,
                    CAST(sub_name AS VARCHAR) AS subcontractor_name,
                    CAST(sub_cage AS VARCHAR) AS subcontractor_cage,
                    CAST(invoice_id AS VARCHAR) AS subcontract_id,
                    CAST(description AS VARCHAR) AS subcontract_description,
                    CAST(action_date AS VARCHAR) AS subcontract_action_date,
                    TRY_CAST(year AS INTEGER) AS year,
                    TRY_CAST(subaward_value AS DOUBLE) AS subcontract_value_usd,
                    TRY_CAST(subaward_value_raw AS DOUBLE) AS subcontract_value_raw_usd,
                    CAST(sub_city AS VARCHAR) AS subcontractor_city,
                    CAST(sub_state AS VARCHAR) AS subcontractor_state,
                    {optional_network_column("sub_country")} AS subcontractor_country,
                    {optional_network_column("sub_zip")} AS subcontractor_zip,
                    CAST(platform_family AS VARCHAR) AS platform_family,
                    CAST(psc AS VARCHAR) AS psc_code,
                    CAST(NULL AS VARCHAR) AS psc_description,
                    CAST(NULL AS VARCHAR) AS naics_code,
                    CAST(NULL AS VARCHAR) AS naics_description,
                    CAST(market_segment AS VARCHAR) AS market_segment
                FROM v_network;
            """)

            # ✅ NEW: KPIs as a Native Table
            # Restore KPIs as a View
            kpis_path = str((LOCAL_CACHE_DIR / "kpis.parquet").resolve())
            conn.execute("DROP VIEW IF EXISTS v_kpis;")
            conn.execute("DROP TABLE IF EXISTS v_kpis;")
            if os.path.exists(kpis_path):
                conn.execute(f"""
                    CREATE OR REPLACE VIEW v_kpis AS
                    SELECT
                        upper(trim(CAST(cage_code AS VARCHAR))) AS cage_code,
                        try_cast(year AS INTEGER) AS year,
                        coalesce(try_cast(total_spend AS DOUBLE), 0.0) AS total_spend,
                        coalesce(try_cast(contract_count AS BIGINT), 0) AS contract_count
                    FROM read_parquet('{kpis_path}');
                """)
            else:
                conn.execute("""
                    CREATE OR REPLACE VIEW v_kpis AS
                    SELECT CAST(NULL AS VARCHAR) AS cage_code, CAST(NULL AS INTEGER) AS year, CAST(0.0 AS DOUBLE) AS total_spend, CAST(0 AS BIGINT) AS contract_count WHERE 1=0;
                """)

        # 4. FETCH MAPPINGS (Athena)
        cage_map: Dict[str, str] = {}
        naics_map: Dict[str, str] = {}
        try:
            parents_list = run_athena_query("SELECT child_cage, parent_name FROM ref_parent_child")
            if parents_list:
                p_df = pd.DataFrame(parents_list)
                cage_map = dict(zip(p_df.child_cage, p_df.parent_name))
            
            naics_list = run_athena_query('SELECT code, title FROM "market_intel_silver"."ref_naics"')
            if naics_list:
                naics_map = {str(i["code"]).strip(): str(i["title"]).strip() for i in naics_list}
        except Exception:
            logger.exception("Mapping Error")
        
        new_global_cache["naics_map"] = naics_map

        # 5. LOAD SMALL FILES
        # ✅ FIX: Removed geo, risk, and opportunities from RAM. They are strictly DuckDB now.
        ram_files = ["profiles.parquet"]
        
        for file in ram_files:
            local_path = str(LOCAL_CACHE_DIR / file)
            try:
                df = pd.read_parquet(local_path, engine="pyarrow", dtype_backend="pyarrow")
                
                # Basic string cleanup
                for col in ["vendor_name", "cage_code"]:
                    if col in df.columns:
                        if isinstance(df[col].dtype, pd.ArrowDtype):
                            df[col] = df[col].astype(str)
                        df[col] = df[col].astype(str).str.upper().str.strip()

                # Fix for profiles
                if file == "profiles.parquet" and "top_platforms" in df.columns:
                    df["top_platforms"] = (
                        df["top_platforms"].astype(str)
                        .str.replace(r"\bNAN\b,?", "", regex=True)
                        .str.replace(r",+$", "", regex=True)
                        .str.replace(r"^,+", "", regex=True)
                    )
                
                # ✅ SAFE NULL HANDLING for pyarrow-backed frames
                # 1) Convert to object first so we can safely place None
                df = df.astype(object)

                # 2) Set missing values to None across the board
                df = df.where(pd.notnull(df), None)

                # 3) Only fill numeric columns with 0 (do NOT touch strings)
                num_cols = df.select_dtypes(include=["number"]).columns
                if len(num_cols) > 0:
                    df.loc[:, num_cols] = (
                        pd.DataFrame(df[num_cols])
                        .apply(pd.to_numeric, errors="coerce")
                        .fillna(0)
                    )

                if file == "geo.parquet": new_global_cache["geo_df"] = df
                if file == "profiles.parquet": new_global_cache["profiles_df"] = df
                if file == "risk.parquet": new_global_cache["risk_df"] = df
                if file == "opportunities.parquet": new_global_cache["df_opportunities"] = df

            except Exception:
                logger.exception(f"Failed to load {file}")

# 6. LOAD SUMMARY (Atomic Swap Logic + Clean NaN via DuckDB)
        try:
            summary_in_path = str((LOCAL_CACHE_DIR / "summary.parquet").resolve())
            summary_final_path = str((LOCAL_CACHE_DIR / SUMMARY_PARQUET_CLEAN).resolve())
            summary_temp_path = str((LOCAL_CACHE_DIR / f"{SUMMARY_PARQUET_CLEAN}.tmp").resolve())

            if os.path.exists(summary_temp_path):
                try: os.remove(summary_temp_path)
                except Exception: pass

            with DUCK_INIT_LOCK:
                conn = ensure_duck_conn()
                
                # 1. Register the Athena cage_map as a temporary table so DuckDB can use it
                map_df = pd.DataFrame(list(cage_map.items()), columns=['cage', 'parent']) if cage_map else pd.DataFrame(columns=['cage', 'parent'])
                conn.register('temp_cage_map', map_df)

                logger.info("Cleaning summary.parquet using DuckDB streaming (OOM-Safe)...")

                # 2. Let DuckDB read, clean, map, and write the parquet file entirely out-of-core
                conn.execute(f"""
                    COPY (
                        SELECT 
                            -- Keep all other columns untouched
                            s.* EXCLUDE (vendor_name, cage_code, platform_family, market_segment, sub_agency, psc_description),

                            -- Apply Name Corrections
                            CASE 
                                WHEN upper(trim(s.vendor_name)) IN ('THE BOEING', 'BOEING', 'BOEING CO') THEN 'THE BOEING COMPANY'
                                ELSE upper(trim(s.vendor_name))
                            END AS vendor_name,

                            -- Clean cage_code
                            CASE 
                                WHEN upper(trim(s.cage_code)) IN ('NAN', 'NONE', 'NULL', '') THEN NULL 
                                ELSE upper(trim(s.cage_code)) 
                            END AS cage_code,

                            -- Clean categorical text to NULLs
                            CASE WHEN upper(trim(s.platform_family)) IN ('NAN', 'NAN.0', 'NONE', '', 'UNKNOWN') THEN NULL ELSE upper(trim(s.platform_family)) END AS platform_family,
                            CASE WHEN upper(trim(s.market_segment)) IN ('NAN', 'NAN.0', 'NONE', '', 'UNKNOWN') THEN NULL ELSE upper(trim(s.market_segment)) END AS market_segment,
                            CASE WHEN upper(trim(s.sub_agency)) IN ('NAN', 'NAN.0', 'NONE', '', 'UNKNOWN') THEN NULL ELSE upper(trim(s.sub_agency)) END AS sub_agency,
                            CASE WHEN upper(trim(s.psc_description)) IN ('NAN', 'NAN.0', 'NONE', '', 'UNKNOWN') THEN NULL ELSE upper(trim(s.psc_description)) END AS psc_description,

                            -- Map the parent dynamically
                            CASE 
                                WHEN upper(trim(COALESCE(m.parent, s.vendor_name))) IN ('THE BOEING', 'BOEING', 'BOEING CO') THEN 'THE BOEING COMPANY'
                                ELSE upper(trim(COALESCE(m.parent, s.vendor_name)))
                            END AS clean_parent

                        FROM read_parquet('{summary_in_path}') s
                        LEFT JOIN temp_cage_map m ON upper(trim(s.cage_code)) = m.cage
                    ) TO '{summary_temp_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
                """)

                # Cleanup the virtual table mapping
                conn.unregister('temp_cage_map')

                # 3. Atomic Swap
                if os.path.exists(summary_temp_path):
                    os.replace(summary_temp_path, summary_final_path) 
                
                # 4. Re-create the memory-efficient View
                conn.execute("DROP VIEW IF EXISTS v_summary;")
                conn.execute("DROP TABLE IF EXISTS v_summary;")
                conn.execute(f"CREATE OR REPLACE VIEW v_summary AS SELECT * FROM read_parquet('{summary_final_path}');")
            
            logger.info("Summary updated safely via DuckDB swap (View).")
            new_global_cache["df"] = pd.DataFrame()

            # Re-compute options
            # NOTE: We use use_writer=True here to query during the reload lock
            years_df = duck_fetch_df("SELECT DISTINCT year FROM v_summary WHERE year IS NOT NULL ORDER BY year ASC;", use_writer=True)
            agencies_df = duck_fetch_df("SELECT DISTINCT sub_agency FROM v_summary WHERE sub_agency IS NOT NULL AND TRIM(CAST(sub_agency AS VARCHAR)) <> '' ORDER BY sub_agency ASC LIMIT 5000;", use_writer=True)
            domains_df = duck_fetch_df("SELECT DISTINCT market_segment FROM v_summary WHERE market_segment IS NOT NULL AND TRIM(CAST(market_segment AS VARCHAR)) <> '' ORDER BY market_segment ASC LIMIT 5000;", use_writer=True)
            platforms_df = duck_fetch_df("SELECT DISTINCT platform_family FROM v_summary WHERE platform_family IS NOT NULL AND TRIM(CAST(platform_family AS VARCHAR)) <> '' ORDER BY platform_family ASC LIMIT 5000;", use_writer=True)

            new_global_cache["options"] = {
                "years": years_df["year"].dropna().astype(int).tolist() if "year" in years_df.columns else [],
                "agencies": agencies_df["sub_agency"].dropna().astype(str).tolist() if "sub_agency" in agencies_df.columns else [],
                "domains": domains_df["market_segment"].dropna().astype(str).tolist() if "market_segment" in domains_df.columns else [],
                "platforms": platforms_df["platform_family"].dropna().astype(str).tolist() if "platform_family" in platforms_df.columns else [],
            }

        except Exception:
            logger.exception("Summary clean-to-disk failed")

        # 7. BUILD MAPS
        if not new_global_cache["profiles_df"].empty:
             p_df = new_global_cache["profiles_df"]
             if {"cage_code", "vendor_name"}.issubset(p_df.columns):
                new_global_cache["cage_name_map"] = p_df.set_index("cage_code")["vendor_name"].to_dict()
        
        # ✅ FIX: Build location_map using DuckDB instead of RAM
        try:
            try:
                geo_mapping_df = duck_fetch_df(
                    "SELECT cage_code, city, state FROM v_cage_locations WHERE cage_code IS NOT NULL",
                    use_writer=True,
                )
            except Exception:
                geo_mapping_df = duck_fetch_df(
                    "SELECT cage_code, city, state FROM v_geo WHERE cage_code IS NOT NULL",
                    use_writer=True,
                )
            if not geo_mapping_df.empty:
                new_global_cache["location_map"] = geo_mapping_df.set_index("cage_code")[["city", "state"]].to_dict(orient="index")
            else:
                new_global_cache["location_map"] = {}
        except Exception:
            logger.exception("Failed to build location_map from DuckDB")
            new_global_cache["location_map"] = {}

        # 8. BUILD SEARCH INDEX (Native DuckDB - RAM Safe)
        try:
            logger.info("Building native search index in DuckDB...")
            conn.execute("DROP TABLE IF EXISTS search_index;")
            
            # This perfectly mirrors your 4 DataFrames, their limits, and the final sort
            conn.execute("""
                CREATE TABLE search_index AS 
                SELECT * FROM (
                    -- 1. PARENTS (Preserves >1 cage or >1B spend, Limit 5000)
                    SELECT 
                        clean_parent AS label, 
                        clean_parent AS value, 
                        'PARENT' AS type, 
                        SUM(total_spend) AS score, 
                        'AGGREGATE' AS cage,
                        '' AS city,
                        '' AS state
                    FROM v_summary 
                    WHERE clean_parent IS NOT NULL AND TRIM(CAST(clean_parent AS VARCHAR)) <> '' 
                    GROUP BY clean_parent 
                    HAVING SUM(total_spend) > 0 
                       AND (COUNT(DISTINCT cage_code) > 1 OR SUM(total_spend) > 1e9)
                    ORDER BY score DESC
                    LIMIT 5000
                )
                UNION ALL
                SELECT * FROM (
                    -- 2. CHILDREN (Preserves NAN filter, location join, Limit 20000)
                    SELECT 
                        s.vendor_name AS label, 
                        s.vendor_name AS value, 
                        'CHILD' AS type, 
                        SUM(s.total_spend) AS score, 
                        s.cage_code AS cage,
                        COALESCE(ANY_VALUE(g.city), '') AS city,
                        COALESCE(ANY_VALUE(g.state), '') AS state
                    FROM v_summary s
                    LEFT JOIN v_geo g ON s.cage_code = g.cage_code
                    WHERE s.vendor_name IS NOT NULL AND TRIM(CAST(s.vendor_name AS VARCHAR)) <> '' 
                      AND s.cage_code IS NOT NULL AND TRIM(CAST(s.cage_code AS VARCHAR)) NOT IN ('NAN', 'NONE', 'NULL', '')
                    GROUP BY s.vendor_name, s.cage_code
                    HAVING SUM(s.total_spend) > 0
                    ORDER BY score DESC
                    LIMIT 20000
                )
                UNION ALL
                SELECT * FROM (
                    -- 3. PROFILE-BACKED COMPANIES (includes low-spend and network-only sites)
                    SELECT
                        p.vendor_name AS label,
                        p.vendor_name AS value,
                        'CHILD' AS type,
                        GREATEST(
                            COALESCE(TRY_CAST(p.total_lifetime_spend AS DOUBLE), 0.0),
                            COALESCE(TRY_CAST(p.network_flow_total AS DOUBLE), 0.0),
                            1.0
                        ) AS score,
                        p.cage_code AS cage,
                        COALESCE(ANY_VALUE(g.city), '') AS city,
                        COALESCE(ANY_VALUE(g.state), '') AS state
                    FROM v_profiles p
                    LEFT JOIN v_geo g ON UPPER(TRIM(CAST(p.cage_code AS VARCHAR))) = UPPER(TRIM(CAST(g.cage_code AS VARCHAR)))
                    WHERE p.vendor_name IS NOT NULL
                      AND TRIM(CAST(p.vendor_name AS VARCHAR)) <> ''
                      AND p.cage_code IS NOT NULL
                      AND TRIM(CAST(p.cage_code AS VARCHAR)) NOT IN ('NAN', 'NONE', 'NULL', '')
                    GROUP BY p.vendor_name, p.cage_code, p.total_lifetime_spend, p.network_flow_total
                    ORDER BY score DESC
                    LIMIT 50000
                )
                UNION ALL
                SELECT * FROM (
                    -- 4. PLATFORMS (Limit 2000)
                    SELECT 
                        platform_family AS label, 
                        platform_family AS value, 
                        'PLATFORM' AS type, 
                        SUM(total_spend) AS score, 
                        '' AS cage, 
                        '' AS city, 
                        '' AS state
                    FROM v_summary 
                    WHERE platform_family IS NOT NULL AND TRIM(CAST(platform_family AS VARCHAR)) <> '' 
                    GROUP BY platform_family
                    HAVING SUM(total_spend) > 0
                    ORDER BY score DESC
                    LIMIT 2000
                )
                UNION ALL
                SELECT * FROM (
                    -- 4. AGENCIES (Limit 2000)
                    SELECT 
                        sub_agency AS label, 
                        sub_agency AS value, 
                        'AGENCY' AS type, 
                        SUM(total_spend) AS score, 
                        '' AS cage, 
                        '' AS city, 
                        '' AS state
                    FROM v_summary 
                    WHERE sub_agency IS NOT NULL AND TRIM(CAST(sub_agency AS VARCHAR)) <> '' 
                    GROUP BY sub_agency
                    HAVING SUM(total_spend) > 0
                    ORDER BY score DESC
                    LIMIT 2000
                )
                UNION ALL
                SELECT * FROM (
                    -- 5. OPPORTUNITIES (No Limit, Instant Pipeline Search)
                    SELECT 
                        title AS label, 
                        sol_num AS value, 
                        'OPPORTUNITY' AS type, 
                        999999999.0 AS score, -- High artificial score so active bids show at the top
                        '' AS cage, 
                        agency AS city, -- Stashing the agency name in 'city' so we can display it in the UI
                        '' AS state
                    FROM v_opportunities 
                    WHERE title IS NOT NULL AND TRIM(CAST(title AS VARCHAR)) <> '' 
                )
                -- 5. FINAL COMBINED SORT (Mirrors your python: search_list.sort(...))
                ORDER BY score DESC;
            """)
            
            # Leave the Python list empty to save RAM
            new_global_cache["search_index"] = []
            logger.info("Native search index built successfully.")

        except Exception:
            logger.exception("Search index build failed")
            new_global_cache["search_index"] = []

        gc.collect()

        # 9. ATOMIC POINTER SWAP
        new_global_cache["is_loading"] = False
        new_global_cache["last_loaded"] = time.time()

        # ✅ SURGICAL FIX 2: Do NOT declare global here again. Just assign.
        GLOBAL_CACHE = new_global_cache

        # Get the new native count for the log
        idx_count = 0
        try:
            idx_count = conn.execute("SELECT COUNT(*) FROM search_index;").fetchone()[0]
        except Exception:
            pass

        logger.info("RELOAD COMPLETE (Atomic Swap). search_index=%d (Native)", idx_count)
        
        new_global_cache = None
        gc.collect()

    except Exception:
        logger.exception("Reload crash")
        # Restore safe state on crash so API doesn't hang
        GLOBAL_CACHE = {**GLOBAL_CACHE, "is_loading": False}
    finally:
        RELOAD_LOCK.release()

# ==========================================
# CONSTANTS / HELPERS
# ==========================================

import re

GENERIC_AWARD_DESC_REGEX = re.compile(
    r"(LUBRICATING OIL|CLEANING COMPOUND|CORROSION PREVENTIV|"
    r"\bADHESIVE\b|REMOVER,SEALANT|SEALING COMPOUND|"
    r"\bDETECTOR,GAS\b|MILK OF MAGNESIA|PRIMER COATING|"
    r"BRUSH PLATING SOLUT|THREADLOCKER ADHESI|"
    r"GREASE,GENERAL PURP|SEALER,CHEMICAL|RESIN COATING|"
    r"INSPECTION PENETRAN|PLUG,PROTECTIVE,DUS)",
    re.IGNORECASE,
)

GENERIC_VARIANT_REGEX = re.compile(
    r"GENERAL USE CONSUMABLES|GUC",
    re.IGNORECASE,
)

@app.get("/api/public/f35-supply-chain")
def get_public_f35_supply_chain():
    # ✅ FIX: Stripped out PSCs. Pure Prime-to-Sub flow-down.
    sql = f"""
        SELECT 
            sub_name as sub_vendor,
            sub_cage as sub_cage,
            sub_city,
            sub_state,
            prime_name as prime_vendor,
            array_to_string((array_agg(DISTINCT description))[1:3], ' | ') as description,
            COUNT(*) as contract_count,
            SUM(subaward_value) as subaward_value
        FROM read_parquet(?)
        WHERE UPPER(TRIM(platform_family)) = 'F-35'
        GROUP BY sub_name, sub_cage, sub_city, sub_state, prime_name
        ORDER BY subaward_value DESC
    """
    
    path = LOCAL_CACHE_DIR / "network.parquet"
    if not path.exists():
        return []
        
    global DUCK_CONN
    try:
        with DUCK_LOCK:
            ensure_duck_conn()
            df = DUCK_CONN.execute(sql, [str(path)]).fetchdf()
            df = df_sanitize_for_json(df)
            return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Public F-35 API Error: {e}")
        return []
    
# ==========================================
# DATA EXPLORER & EXPORT API
# ==========================================

class ExplorerRequest(BaseModel):
    table: str = "v_contracts_rolled"
    columns: List[str] = []
    filters: Dict[str, Any] = {}
    subscription_status: str = "free"
    limit: Optional[int] = None
    offset: Optional[int] = 0


class SubcontractDescriptionsRequest(BaseModel):
    source_report_id: Optional[str] = None
    source_dedup_key: Optional[str] = None
    primary_description: Optional[str] = None


NSN_REF_TABLE = "v_nsn_cage_reference"
CONTRACT_AWARD_EXPLORER_TABLE = "v_contract_awards_enriched"
SUBCONTRACT_EXPLORER_TABLE = "v_subcontracts"

CONTRACT_AWARD_SYNTHETIC_COLUMNS = {
    "award_key", "contract_id", "vendor_name", "vendor_cage", "parent_agency", "sub_agency",
    "city", "state", "country", "market_segment", "platform_family",
    "place_of_performance_city", "place_of_performance_state",
    "place_of_performance_country", "place_of_performance_zip",
    "nsn", "niin",
    "location_quality",
    "psc_code", "psc", "psc_description", "naics_code", "naics_description",
    "base_award_description", "latest_action_description", "source_of_supply",
    "obligations_in_selected_period_usd",
    "number_of_actions_in_selected_period",
    "earliest_action_date_in_selected_period", "latest_action_date_in_selected_period",
}

ALLOWED_EXPLORER_TABLES = {
    CONTRACT_AWARD_EXPLORER_TABLE,
    SUBCONTRACT_EXPLORER_TABLE,
    "v_contracts_rolled",
    "v_transactions",
    "v_summary",
    NSN_REF_TABLE,
}

ALLOWED_EXPLORER_COLUMNS = {
    # Existing revenue-backed / award-backed fields
    "award_key", "transaction_key", "source_system", "modification_number", "awarding_agency_code",
    "po_number", "po_item_number", "source_reference_rows", "reference_part_number_count",
    "part_number_reference_status", "source_report_id", "source_dedup_key", "location_quality",
    "sub_cage_resolution", "sub_cage_source_period", "sub_cage_candidate_count", "sub_cage_candidates",
    "contract_id", "year", "action_date", "last_action_date", "total_spend", "spend_amount", "contract_count",
    "vendor_cage", "cage_code", "cage", "vendor_name", "sub_agency", "parent_agency", "clean_parent",
    "psc", "psc_code", "psc_description", "naics_code", "naics_description",
    "platform_family", "platform_families", "platform_count", "is_multi_platform_component",
    "platform_attributed_spend_amount", "platform_attributed_spend",
    "shared_use_exposure_amount", "shared_use_exposure", "market_segment", "description",
    "city", "state", "country", "piid", "idv_piid", "transaction_id",
    "place_of_performance_city", "place_of_performance_state",
    "place_of_performance_country", "place_of_performance_zip",
    "nsn", "niin", "part_number", "pricing_type", "set_aside_type", "competition_type", "offers_count",

    # Full NSN/CAGE reference fields
    "fsc", "fsc_code", "item_name", "nomenclature",
    "demil_code", "shelf_life_code", "mgmt_control_code", "unit_of_issue",
    "source_of_supply", "source_of_supply_codes", "management_organizations", "management_record_count",
    "govt_estimated_price", "acquisition_advice_code",
    "rncc_codes", "rnvc_codes", "rnsc_codes", "cage_status_codes",
    "is_procurement_authorized", "is_active_authorized_source",
    "supplier_status", "supplier_status_detail",

    # Contract-award explorer fields
    "base_award_description", "action_description", "latest_action_description",
    "obligations_in_selected_period_usd", "lifetime_obligations_usd",
    "number_of_actions_in_selected_period",
    "earliest_action_date_in_selected_period", "latest_action_date_in_selected_period",
    "earliest_action_date_lifetime", "latest_action_date_lifetime",
    "award_type_code", "award_type_description",

    # Subcontract explorer fields
    "prime_name", "prime_cage", "prime_award_id", "subcontractor_name", "subcontractor_cage",
    "subcontract_id", "subcontract_description", "subcontract_action_date",
    "subcontract_value_usd", "subcontract_value_raw_usd", "subcontractor_city", "subcontractor_state",
    "subcontractor_country", "subcontractor_zip", "subcontractor_uei", "prime_award_description",
    "prime_award_control_value",
}

SUBCONTRACT_INTERNAL_EXPORT_COLUMNS = {
    "award_key",
    "source_report_id",
    "source_report_last_modified_date",
    "source_dedup_key",
    "subcontract_value_treatment",
    "included_in_adjusted_total",
    "source_report_version_count",
    "exact_repeat_count",
    "reported_action_version_count",
    "same_date_description_version_count",
    "equal_value_description_report_count",
    "subcontract_data_source",
    "subcontract_methodology",
}

EXPLORER_DEFAULT_COLUMNS = {
    CONTRACT_AWARD_EXPLORER_TABLE: [
        "contract_id", "vendor_name", "vendor_cage",
        "obligations_in_selected_period_usd",
        "number_of_actions_in_selected_period", "earliest_action_date_in_selected_period",
        "latest_action_date_in_selected_period",
        "base_award_description", "latest_action_description",
        "place_of_performance_city", "place_of_performance_state",
    ],
    SUBCONTRACT_EXPLORER_TABLE: [
        "prime_name", "prime_cage", "prime_award_id", "subcontractor_name", "subcontractor_cage",
        "subcontract_value_usd", "subcontract_action_date", "platform_family", "psc_code",
        "subcontract_description",
    ],
    "v_contracts_rolled": [
        "contract_id", "last_action_date", "vendor_name", "vendor_cage", "total_spend", "description"
    ],
    "v_transactions": [
        "contract_id", "action_date", "vendor_name", "vendor_cage", "spend_amount", "nsn", "part_number", "description"
    ],
    "v_summary": [
        "vendor_name", "cage_code", "clean_parent", "total_spend", "contract_count", "market_segment", "platform_family"
    ],
    NSN_REF_TABLE: [
        "nsn", "niin", "cage_code", "vendor_name", "part_number", "description",
        "platform_families", "platform_count", "supplier_status"
    ],
}

# These are granular tables. Do not allow accidental full-table scans.
EXPLORER_FILTER_REQUIRED_TABLES = {
    CONTRACT_AWARD_EXPLORER_TABLE,
    SUBCONTRACT_EXPLORER_TABLE,
    "v_contracts_rolled",
    "v_transactions",
    NSN_REF_TABLE,
}

# Lets the frontend use stable names even if the parquet uses slightly different names.
EXPLORER_COLUMN_ALIASES = {
    SUBCONTRACT_EXPLORER_TABLE: {
        "vendor_cage": ["subcontractor_cage", "prime_cage"],
        "cage_code": ["subcontractor_cage", "prime_cage"],
        "cage": ["subcontractor_cage", "prime_cage"],
        "vendor_name": ["subcontractor_name", "prime_name"],
        "psc": ["psc_code"],
        "description": ["subcontract_description"],
        "action_date": ["subcontract_action_date"],
        "spend_amount": ["subcontract_value_usd"],
        "total_spend": ["subcontract_value_usd"],
    },
    NSN_REF_TABLE: {
        "cage": ["cage", "cage_code", "vendor_cage"],
        "cage_code": ["cage_code", "cage", "vendor_cage"],
        "vendor_cage": ["vendor_cage", "cage_code", "cage"],
        "vendor_name": ["vendor_name", "company_name", "manufacturer_name", "entity_name"],
        "description": ["description", "item_name", "nomenclature", "product_description", "item_description"],
        "item_name": ["item_name", "description", "nomenclature"],
        "fsc_code": ["fsc_code", "fsc"],
        "fsc": ["fsc", "fsc_code"],
        "source": ["source", "reference_source", "source_layer", "data_source"],
        "reference_source": ["reference_source", "source", "source_layer", "data_source"],
        "platform_family": ["platform_families", "platform_family"],
    }
}


def quote_ident(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def get_duck_table_columns(table: str) -> set:
    if table not in ALLOWED_EXPLORER_TABLES:
        return set()

    if table == CONTRACT_AWARD_EXPLORER_TABLE:
        return set(CONTRACT_AWARD_SYNTHETIC_COLUMNS)

    try:
        df = duck_fetch_df(f"DESCRIBE {table}")
        if df.empty or "column_name" not in df.columns:
            return set()
        return {str(c).strip().lower() for c in df["column_name"].dropna().tolist()}
    except Exception:
        logger.exception("Failed to describe explorer table: %s", table)
        return set()


def resolve_explorer_column(table: str, requested_col: str, actual_cols: set) -> Optional[str]:
    requested = str(requested_col).strip().lower()

    if requested in actual_cols:
        return requested

    aliases = EXPLORER_COLUMN_ALIASES.get(table, {}).get(requested, [])
    for alt in aliases:
        if alt.lower() in actual_cols:
            return alt.lower()

    return None


def multi_platform_component_expr(actual_cols: set) -> Optional[str]:
    """Customer-facing flag derived from the underlying platform relationship count."""
    if "platform_count" in actual_cols:
        return '(COALESCE(TRY_CAST("platform_count" AS INTEGER), 0) > 1)'
    return None


def normalised_niin_filter_expr(actual_col: str) -> str:
    col = quote_ident(actual_col)
    digits = f"REGEXP_REPLACE(CAST({col} AS VARCHAR), '[^0-9]', '', 'g')"
    return f"RIGHT(LPAD({digits}, 9, '0'), 9)"


def _null_expr(alias: str, sql_type: str = "VARCHAR") -> str:
    return f"CAST(NULL AS {sql_type}) AS {quote_ident(alias)}"


def _source_expr(actual_cols: set, col: str, alias: Optional[str] = None, sql_type: str = "VARCHAR") -> str:
    alias = alias or col
    if col in actual_cols:
        return f"{quote_ident(col)} AS {quote_ident(alias)}"
    return _null_expr(alias, sql_type)


def _contract_award_type_description_expr(code_expr: str) -> str:
    return f"""
        CASE
            WHEN {code_expr} IN ('A', 'B', 'C', 'D') THEN 'Definitive contract'
            WHEN {code_expr} IN ('DO', 'DELIVERY ORDER', 'DELIVERY_ORDER') THEN 'Delivery order'
            WHEN {code_expr} IN ('PO', 'PURCHASE ORDER', 'PURCHASE_ORDER') THEN 'Purchase order'
            WHEN {code_expr} IN ('BPA_CALL', 'BPA CALL') THEN 'BPA call'
            WHEN {code_expr} IS NULL OR TRIM(CAST({code_expr} AS VARCHAR)) = '' THEN NULL
            ELSE CAST({code_expr} AS VARCHAR)
        END
    """


def build_contract_awards_explorer_query_from_actions(
    payload: ExplorerRequest,
    row_limit: int,
    offset: int = 0,
    count_only: bool = False
):
    actual_cols = get_duck_table_columns("v_transactions")
    if not actual_cols:
        raise HTTPException(status_code=503, detail="v_transactions is not available yet. Reload may still be running.")

    requested_cols = payload.columns or EXPLORER_DEFAULT_COLUMNS[CONTRACT_AWARD_EXPLORER_TABLE]
    selected_fields = [str(c).strip().lower() for c in requested_cols if str(c).strip().lower() in CONTRACT_AWARD_SYNTHETIC_COLUMNS]
    if not selected_fields:
        selected_fields = EXPLORER_DEFAULT_COLUMNS[CONTRACT_AWARD_EXPLORER_TABLE]

    def col_or_null(col: str, alias: Optional[str] = None, sql_type: str = "VARCHAR") -> str:
        return _source_expr(actual_cols, col, alias, sql_type)

    selected_where = ["contract_id IS NOT NULL", "TRIM(CAST(contract_id AS VARCHAR)) <> ''"]
    params: List[Any] = []
    has_valid_filter = False
    filters = payload.filters or {}
    spend_min = None
    spend_max = None

    filter_column_map = {
        "contract_id": "contract_id",
        "year": "year",
        "sub_agency": "sub_agency",
        "parent_agency": "parent_agency",
        "market_segment": "market_segment",
        "platform_family": "platform_family",
        "country": "country",
        "state": "state",
        "city": "city",
        "place_of_performance_country": "place_of_performance_country",
        "place_of_performance_state": "place_of_performance_state",
        "place_of_performance_city": "place_of_performance_city",
        "place_of_performance_zip": "place_of_performance_zip",
        "vendor_name": "vendor_name",
        "vendor_cage": "vendor_cage",
        "cage_code": "vendor_cage",
        "cage": "vendor_cage",
        "nsn": "nsn",
        "niin": "niin",
        "part_number": "part_number",
        "source_of_supply": "source_of_supply",
        "naics_code": "naics_code",
        "psc": "psc",
        "psc_code": "psc",
    }

    for raw_col, val in filters.items():
        requested_col = str(raw_col).strip().lower()

        if requested_col == "min_spend":
            try:
                spend_min = float(val)
                has_valid_filter = True
            except (TypeError, ValueError):
                pass
            continue

        if requested_col == "max_spend":
            try:
                spend_max = float(val)
                has_valid_filter = True
            except (TypeError, ValueError):
                pass
            continue

        if requested_col in {"q", "search", "query"}:
            if val is None or str(val).strip() == "":
                continue
            search_cols = [c for c in [
                "contract_id", "vendor_name", "vendor_cage", "description",
                "base_award_description", "action_description", "nsn", "niin", "part_number"
            ] if c in actual_cols]
            if search_cols:
                p = _like_param_contains(str(val))
                selected_where.append("(" + " OR ".join([f"UPPER(CAST({quote_ident(c)} AS VARCHAR)) LIKE ? ESCAPE '\\\\'" for c in search_cols]) + ")")
                params.extend([p] * len(search_cols))
                has_valid_filter = True
            continue

        actual_col = filter_column_map.get(requested_col)
        if not actual_col or actual_col not in actual_cols:
            continue

        if val is None or str(val).strip() == "":
            continue

        has_valid_filter = True

        if requested_col in {"nsn", "niin"}:
            safe_niin = get_niin(str(val))
            selected_where.append(f"{normalised_niin_filter_expr(actual_col)} = ?")
            params.append(safe_niin)
            continue

        if requested_col in {"vendor_cage", "cage_code", "cage"}:
            selected_where.append(f"UPPER(TRIM(CAST({quote_ident(actual_col)} AS VARCHAR))) = ?")
            params.append(str(val).strip().upper())
            continue

        if isinstance(val, list) and len(val) > 0:
            placeholders = ",".join(["?"] * len(val))
            selected_where.append(f"UPPER(TRIM(CAST({quote_ident(actual_col)} AS VARCHAR))) IN ({placeholders})")
            params.extend([str(v).strip().upper() for v in val])
            continue

        if not isinstance(val, list):
            selected_where.append(f"UPPER(TRIM(CAST({quote_ident(actual_col)} AS VARCHAR))) = ?")
            params.append(str(val).strip().upper())

    if not has_valid_filter:
        raise HTTPException(
            status_code=400,
            detail="At least one filter is required for contract-award data. Use years, vendor, platform, agency, PSC, NAICS, NSN, or search."
        )

    selected_where_clause = " AND ".join(selected_where)

    selected_spend_filters = []
    if spend_min is not None:
        selected_spend_filters.append("COALESCE(SUM(spend_amount), 0) >= ?")
        params.append(spend_min)
    if spend_max is not None:
        selected_spend_filters.append("COALESCE(SUM(spend_amount), 0) <= ?")
        params.append(spend_max)
    selected_spend_having = "HAVING " + " AND ".join(selected_spend_filters) if selected_spend_filters else ""

    award_type_source_expr = "CAST(NULL AS VARCHAR)"
    for candidate in ["award_type_code", "award_type", "award_type_description"]:
        if candidate in actual_cols:
            award_type_source_expr = f"CAST({quote_ident(candidate)} AS VARCHAR)"
            break

    field_exprs = {
        "award_key": "f.award_key",
        "contract_id": "f.contract_id",
        "vendor_name": "f.vendor_name",
        "vendor_cage": "f.vendor_cage",
        "parent_agency": "f.parent_agency",
        "sub_agency": "f.sub_agency",
        "city": "f.city",
        "state": "f.state",
        "country": "f.country",
        "place_of_performance_city": "f.place_of_performance_city",
        "place_of_performance_state": "f.place_of_performance_state",
        "place_of_performance_country": "f.place_of_performance_country",
        "place_of_performance_zip": "f.place_of_performance_zip",
        "market_segment": "f.market_segment",
        "platform_family": "f.platform_family",
        "psc": "f.psc_code AS psc",
        "psc_code": "f.psc_code",
        "psc_description": "f.psc_description",
        "naics_code": "f.naics_code",
        "naics_description": "f.naics_description",
        "nsn": "f.nsn",
        "niin": "f.niin",
        "nsn_source_system": (
            "f.nsn_source_system" if "nsn_source_system" in actual_cols
            else "CAST(NULL AS VARCHAR) AS nsn_source_system"
        ),
        "nsn_derivation_method": (
            "f.nsn_derivation_method" if "nsn_derivation_method" in actual_cols
            else "CAST(NULL AS VARCHAR) AS nsn_derivation_method"
        ),
        "nsn_resolution_status": (
            "f.nsn_resolution_status" if "nsn_resolution_status" in actual_cols
            else "CAST(NULL AS VARCHAR) AS nsn_resolution_status"
        ),
        "location_quality": (
            "f.location_quality" if "location_quality" in actual_cols
            else "CAST(NULL AS VARCHAR) AS location_quality"
        ),
        "base_award_description": "f.base_award_description",
        "latest_action_description": "f.latest_action_description",
        "source_of_supply": "f.source_of_supply",
        "obligations_in_selected_period_usd": "f.obligations_in_selected_period_usd",
        "lifetime_obligations_usd": "f.lifetime_obligations_usd",
        "number_of_actions_in_selected_period": "f.number_of_actions_in_selected_period",
        "earliest_action_date_in_selected_period": "f.earliest_action_date_in_selected_period",
        "latest_action_date_in_selected_period": "f.latest_action_date_in_selected_period",
        "earliest_action_date_lifetime": "f.earliest_action_date_lifetime",
        "latest_action_date_lifetime": "f.latest_action_date_lifetime",
        "award_type_code": "f.award_type_code",
        "award_type_description": "f.award_type_description",
    }

    select_parts = []
    for field in selected_fields:
        expr = field_exprs.get(field)
        if not expr:
            continue
        if " AS " in expr:
            select_parts.append(expr)
        else:
            select_parts.append(f"{expr} AS {quote_ident(field)}")

    select_clause = ", ".join(select_parts) or "f.contract_id"

    source_ctes = f"""
        WITH source_actions AS NOT MATERIALIZED (
            SELECT
                {col_or_null("award_key")},
                {col_or_null("source_system")},
                {col_or_null("contract_id")},
                {col_or_null("action_date")},
                {col_or_null("vendor_name")},
                {col_or_null("vendor_cage")},
                {col_or_null("parent_agency")},
                {col_or_null("sub_agency")},
                {col_or_null("city")},
                {col_or_null("state")},
                {col_or_null("country")},
                {col_or_null("place_of_performance_city")},
                {col_or_null("place_of_performance_state")},
                {col_or_null("place_of_performance_country")},
                {col_or_null("place_of_performance_zip")},
                {col_or_null("market_segment")},
                {col_or_null("platform_family")},
                {col_or_null("psc")},
                {col_or_null("naics_code")},
                {col_or_null("description")},
                {col_or_null("base_award_description")},
                {col_or_null("action_description")},
                {col_or_null("source_of_supply")},
                {col_or_null("nsn")},
                {col_or_null("niin")},
                {col_or_null("nsn_source_system")},
                {col_or_null("nsn_derivation_method")},
                {col_or_null("nsn_resolution_status")},
                {col_or_null("location_quality")},
                {col_or_null("part_number")},
                {col_or_null("year", sql_type="INTEGER")},
                COALESCE(TRY_CAST({quote_ident("spend_amount")} AS DOUBLE), 0.0) AS spend_amount,
                {award_type_source_expr} AS award_type_code
            FROM v_transactions
        ),
        keyed_actions AS NOT MATERIALIZED (
            SELECT
                *,
                COALESCE(
                    NULLIF(TRIM(CAST(award_key AS VARCHAR)), ''),
                    CONCAT(
                        COALESCE(NULLIF(TRIM(CAST(source_system AS VARCHAR)), ''), 'UNKNOWN'),
                        '|PIID|',
                        COALESCE(NULLIF(TRIM(CAST(contract_id AS VARCHAR)), ''), '<NULL>'),
                        '|CAGE|',
                        COALESCE(NULLIF(TRIM(CAST(vendor_cage AS VARCHAR)), ''), '<NULL>')
                    )
                ) AS effective_award_key
            FROM source_actions
        ),
        selected_actions AS NOT MATERIALIZED (
            SELECT *
            FROM keyed_actions
            WHERE {selected_where_clause}
        ),
        selected_contracts AS (
            SELECT
                effective_award_key,
                MAX_BY(contract_id, TRY_CAST(action_date AS DATE)) AS contract_id,
                SUM(spend_amount) AS obligations_in_selected_period_usd,
                COUNT(*) AS number_of_actions_in_selected_period,
                MIN(TRY_CAST(action_date AS DATE)) AS earliest_action_date_in_selected_period,
                MAX(TRY_CAST(action_date AS DATE)) AS latest_action_date_in_selected_period
            FROM selected_actions
            GROUP BY effective_award_key
            {selected_spend_having}
        )
    """

    if count_only:
        return source_ctes + " SELECT COUNT(*) AS count FROM selected_contracts", params

    row_limit = safe_int(row_limit, 50, 1, 500_000)
    offset = safe_int(offset, 0, 0, 10_000_000)

    detail_ctes = f"""
        , candidate_contracts AS (
            SELECT *
            FROM selected_contracts
            ORDER BY obligations_in_selected_period_usd DESC NULLS LAST, contract_id
            LIMIT ? OFFSET ?
        ),
        selected_rollup AS (
            SELECT
                c.effective_award_key AS award_key,
                c.contract_id,
                MAX_BY(a.vendor_name, TRY_CAST(a.action_date AS DATE)) AS vendor_name,
                MAX_BY(a.vendor_cage, TRY_CAST(a.action_date AS DATE)) AS vendor_cage,
                MAX_BY(a.parent_agency, TRY_CAST(a.action_date AS DATE)) AS parent_agency,
                MAX_BY(a.sub_agency, TRY_CAST(a.action_date AS DATE)) AS sub_agency,
                MAX_BY(a.city, TRY_CAST(a.action_date AS DATE)) AS city,
                MAX_BY(a.state, TRY_CAST(a.action_date AS DATE)) AS state,
                MAX_BY(a.country, TRY_CAST(a.action_date AS DATE)) AS country,
                MAX_BY(a.place_of_performance_city, TRY_CAST(a.action_date AS DATE)) AS place_of_performance_city,
                MAX_BY(a.place_of_performance_state, TRY_CAST(a.action_date AS DATE)) AS place_of_performance_state,
                MAX_BY(a.place_of_performance_country, TRY_CAST(a.action_date AS DATE)) AS place_of_performance_country,
                MAX_BY(a.place_of_performance_zip, TRY_CAST(a.action_date AS DATE)) AS place_of_performance_zip,
                MAX_BY(a.market_segment, TRY_CAST(a.action_date AS DATE)) AS market_segment,
                MAX_BY(a.platform_family, TRY_CAST(a.action_date AS DATE)) AS platform_family,
                MAX_BY(a.psc, TRY_CAST(a.action_date AS DATE)) AS psc_code,
                MAX_BY(a.naics_code, TRY_CAST(a.action_date AS DATE)) AS naics_code,
                CASE
                    WHEN COUNT(DISTINCT NULLIF(TRIM(a.nsn), '')) = 1
                    THEN MIN(NULLIF(TRIM(a.nsn), ''))
                END AS nsn,
                CASE
                    WHEN COUNT(DISTINCT NULLIF(TRIM(a.niin), '')) = 1
                    THEN MIN(NULLIF(TRIM(a.niin), ''))
                END AS niin,
                MAX_BY(a.nsn_source_system, TRY_CAST(a.action_date AS DATE)) AS nsn_source_system,
                MAX_BY(a.nsn_derivation_method, TRY_CAST(a.action_date AS DATE)) AS nsn_derivation_method,
                MAX_BY(a.nsn_resolution_status, TRY_CAST(a.action_date AS DATE)) AS nsn_resolution_status,
                MAX_BY(a.location_quality, TRY_CAST(a.action_date AS DATE)) AS location_quality,
                COALESCE(
                    MAX_BY(NULLIF(a.action_description, ''), TRY_CAST(a.action_date AS DATE)),
                    MAX_BY(NULLIF(a.description, ''), TRY_CAST(a.action_date AS DATE))
                ) AS latest_action_description,
                MAX_BY(NULLIF(a.source_of_supply, ''), TRY_CAST(a.action_date AS DATE)) AS source_of_supply,
                MAX_BY(NULLIF(a.award_type_code, ''), TRY_CAST(a.action_date AS DATE)) AS award_type_code,
                c.obligations_in_selected_period_usd,
                c.number_of_actions_in_selected_period,
                c.earliest_action_date_in_selected_period,
                c.latest_action_date_in_selected_period
            FROM selected_actions a
            INNER JOIN candidate_contracts c
                ON a.effective_award_key = c.effective_award_key
            GROUP BY
                c.effective_award_key,
                c.contract_id,
                c.obligations_in_selected_period_usd,
                c.number_of_actions_in_selected_period,
                c.earliest_action_date_in_selected_period,
                c.latest_action_date_in_selected_period
        ),
        lifetime_rollup AS (
            SELECT
                a.effective_award_key AS award_key,
                SUM(a.spend_amount) AS lifetime_obligations_usd,
                MIN(TRY_CAST(a.action_date AS DATE)) AS earliest_action_date_lifetime,
                MAX(TRY_CAST(a.action_date AS DATE)) AS latest_action_date_lifetime,
                COALESCE(
                    MAX_BY(NULLIF(a.base_award_description, ''), TRY_CAST(a.action_date AS DATE)),
                    MIN_BY(NULLIF(a.description, ''), TRY_CAST(a.action_date AS DATE))
                ) AS base_award_description
            FROM keyed_actions a
            INNER JOIN candidate_contracts c
                ON a.effective_award_key = c.effective_award_key
            GROUP BY a.effective_award_key
        ),
        psc_map AS (
            SELECT psc_code, MAX(psc_description) AS psc_description
            FROM v_summary
            WHERE psc_code IS NOT NULL
            GROUP BY psc_code
        ),
        naics_map AS (
            SELECT naics_code, MAX(naics_description) AS naics_description
            FROM v_summary
            WHERE naics_code IS NOT NULL
            GROUP BY naics_code
        ),
        final_rows AS (
            SELECT
                s.*,
                l.lifetime_obligations_usd,
                l.earliest_action_date_lifetime,
                l.latest_action_date_lifetime,
                l.base_award_description,
                pm.psc_description,
                nm.naics_description,
                {_contract_award_type_description_expr("s.award_type_code")} AS award_type_description
            FROM selected_rollup s
            INNER JOIN lifetime_rollup l ON s.award_key = l.award_key
            LEFT JOIN psc_map pm ON TRIM(CAST(s.psc_code AS VARCHAR)) = TRIM(CAST(pm.psc_code AS VARCHAR))
            LEFT JOIN naics_map nm ON TRIM(CAST(s.naics_code AS VARCHAR)) = TRIM(CAST(nm.naics_code AS VARCHAR))
        )
    """

    sql = source_ctes + detail_ctes + f"""
        SELECT {select_clause}
        FROM final_rows f
        ORDER BY COALESCE(f.obligations_in_selected_period_usd, 0) DESC NULLS LAST, f.contract_id
    """
    params.extend([row_limit, offset])
    return sql, params


def _rolled_contract_years(actual_cols: set) -> List[int]:
    years = []
    for col in actual_cols:
        match = re.fullmatch(r"obligations_fy(\d{4})", str(col).lower())
        if match:
            years.append(int(match.group(1)))
    return sorted(set(years))


def build_contract_awards_explorer_query_from_rollup(
    payload: ExplorerRequest,
    row_limit: int,
    offset: int = 0,
    count_only: bool = False,
):
    actual_cols = get_duck_table_columns("v_contracts_rolled")
    available_years = _rolled_contract_years(actual_cols)
    if not actual_cols or not available_years:
        raise HTTPException(
            status_code=503,
            detail="The enriched contract-award cache is not available yet. Rebuild contracts_rolled.parquet and reload the API.",
        )

    requested_cols = payload.columns or EXPLORER_DEFAULT_COLUMNS[CONTRACT_AWARD_EXPLORER_TABLE]
    selected_fields = [
        str(c).strip().lower()
        for c in requested_cols
        if str(c).strip().lower() in CONTRACT_AWARD_SYNTHETIC_COLUMNS
    ]
    if not selected_fields:
        selected_fields = EXPLORER_DEFAULT_COLUMNS[CONTRACT_AWARD_EXPLORER_TABLE]

    filters = payload.filters or {}
    where_parts = ["r.contract_id IS NOT NULL", "TRIM(CAST(r.contract_id AS VARCHAR)) <> ''"]
    params: List[Any] = []
    has_valid_filter = False
    spend_min = None
    spend_max = None

    requested_year_values = filters.get("year")
    if requested_year_values is None:
        selected_years = available_years
    else:
        raw_years = requested_year_values if isinstance(requested_year_values, list) else [requested_year_values]
        parsed_years = {
            safe_int(value, -1, 1900, 2200)
            for value in raw_years
            if str(value).strip()
        }
        selected_years = [year for year in available_years if year in parsed_years]
        has_valid_filter = bool(parsed_years)
        if not selected_years:
            where_parts.append("1=0")

    obligations_terms = [
        f"COALESCE(TRY_CAST(r.{quote_ident(f'obligations_fy{year}')} AS DOUBLE), 0.0)"
        for year in selected_years
    ]
    action_terms = [
        f"COALESCE(TRY_CAST(r.{quote_ident(f'action_count_fy{year}')} AS BIGINT), 0)"
        for year in selected_years
        if f"action_count_fy{year}" in actual_cols
    ]
    obligations_expr = " + ".join(obligations_terms) if obligations_terms else "0.0"
    actions_expr = " + ".join(action_terms) if action_terms else "0"

    earliest_terms = [
        f"COALESCE(TRY_CAST(r.{quote_ident(f'earliest_action_date_fy{year}')} AS DATE), DATE '9999-12-31')"
        for year in selected_years
        if f"earliest_action_date_fy{year}" in actual_cols
    ]
    latest_terms = [
        f"COALESCE(TRY_CAST(r.{quote_ident(f'latest_action_date_fy{year}')} AS DATE), DATE '0001-01-01')"
        for year in selected_years
        if f"latest_action_date_fy{year}" in actual_cols
    ]
    earliest_expr = (
        f"CASE WHEN ({actions_expr}) > 0 THEN LEAST({', '.join(earliest_terms)}) END"
        if earliest_terms else "CAST(NULL AS DATE)"
    )
    latest_expr = (
        f"CASE WHEN ({actions_expr}) > 0 THEN GREATEST({', '.join(latest_terms)}) END"
        if latest_terms else "CAST(NULL AS DATE)"
    )

    # A selected year means the contract must have at least one action in that period.
    if requested_year_values is not None and selected_years:
        where_parts.append(f"({actions_expr}) > 0")

    filter_column_map = {
        "contract_id": "contract_id",
        "sub_agency": "sub_agency",
        "parent_agency": "parent_agency",
        "market_segment": "market_segment",
        "platform_family": "platform_family",
        "country": "country",
        "state": "state",
        "city": "city",
        "place_of_performance_country": "place_of_performance_country",
        "place_of_performance_state": "place_of_performance_state",
        "place_of_performance_city": "place_of_performance_city",
        "place_of_performance_zip": "place_of_performance_zip",
        "vendor_name": "vendor_name",
        "vendor_cage": "vendor_cage",
        "cage_code": "vendor_cage",
        "cage": "vendor_cage",
        "naics_code": "naics_code",
        "psc": "psc",
        "psc_code": "psc",
    }

    for raw_col, val in filters.items():
        requested_col = str(raw_col).strip().lower()
        if requested_col == "year":
            continue
        if requested_col == "min_spend":
            try:
                spend_min = float(val)
                has_valid_filter = True
            except (TypeError, ValueError):
                pass
            continue
        if requested_col == "max_spend":
            try:
                spend_max = float(val)
                has_valid_filter = True
            except (TypeError, ValueError):
                pass
            continue
        if requested_col in {"q", "search", "query"}:
            if val is None or str(val).strip() == "":
                continue
            search_cols = [
                col for col in [
                    "contract_id", "vendor_name", "vendor_cage", "description",
                    "base_award_description", "latest_action_description",
                ]
                if col in actual_cols
            ]
            if search_cols:
                p = _like_param_contains(str(val))
                where_parts.append(
                    "(" + " OR ".join([
                        f"UPPER(CAST(r.{quote_ident(col)} AS VARCHAR)) LIKE ? ESCAPE '\\\\'"
                        for col in search_cols
                    ]) + ")"
                )
                params.extend([p] * len(search_cols))
                has_valid_filter = True
            continue

        actual_col = filter_column_map.get(requested_col)
        if not actual_col or actual_col not in actual_cols:
            continue
        if val is None or str(val).strip() == "":
            continue

        has_valid_filter = True
        if requested_col in {"vendor_cage", "cage_code", "cage"}:
            where_parts.append(f"UPPER(TRIM(CAST(r.{quote_ident(actual_col)} AS VARCHAR))) = ?")
            params.append(str(val).strip().upper())
        elif isinstance(val, list) and val:
            placeholders = ",".join(["?"] * len(val))
            where_parts.append(
                f"UPPER(TRIM(CAST(r.{quote_ident(actual_col)} AS VARCHAR))) IN ({placeholders})"
            )
            params.extend([str(v).strip().upper() for v in val])
        elif not isinstance(val, list):
            where_parts.append(f"UPPER(TRIM(CAST(r.{quote_ident(actual_col)} AS VARCHAR))) = ?")
            params.append(str(val).strip().upper())

    if not has_valid_filter:
        raise HTTPException(
            status_code=400,
            detail="At least one filter is required for contract-award data. Use fiscal years, vendor, platform, agency, PSC, NAICS, or search.",
        )

    if spend_min is not None:
        where_parts.append(f"({obligations_expr}) >= ?")
        params.append(spend_min)
    if spend_max is not None:
        where_parts.append(f"({obligations_expr}) <= ?")
        params.append(spend_max)

    where_clause = " AND ".join(where_parts)
    if count_only:
        return f"SELECT COUNT(*) AS count FROM v_contracts_rolled r WHERE {where_clause}", params

    row_limit = safe_int(row_limit, 50, 1, 500_000)
    offset = safe_int(offset, 0, 0, 10_000_000)

    field_exprs = {
        "award_key": "f.award_key",
        "contract_id": "f.contract_id",
        "vendor_name": "f.vendor_name",
        "vendor_cage": "f.vendor_cage",
        "parent_agency": "f.parent_agency",
        "sub_agency": "f.sub_agency",
        "city": "f.city",
        "state": "f.state",
        "country": "f.country",
        "place_of_performance_city": "f.place_of_performance_city",
        "place_of_performance_state": "f.place_of_performance_state",
        "place_of_performance_country": "f.place_of_performance_country",
        "place_of_performance_zip": "f.place_of_performance_zip",
        "market_segment": "f.market_segment",
        "platform_family": "f.platform_family",
        "psc": "f.psc AS psc",
        "psc_code": "f.psc AS psc_code",
        "psc_description": "pm.psc_description",
        "naics_code": "f.naics_code",
        "naics_description": "COALESCE(f.naics_description, nm.naics_description) AS naics_description",
        "nsn": "f.nsn",
        "niin": "f.niin",
        "nsn_source_system": (
            "f.nsn_source_system" if "nsn_source_system" in actual_cols
            else "CAST(NULL AS VARCHAR) AS nsn_source_system"
        ),
        "nsn_derivation_method": (
            "f.nsn_derivation_method" if "nsn_derivation_method" in actual_cols
            else "CAST(NULL AS VARCHAR) AS nsn_derivation_method"
        ),
        "nsn_resolution_status": (
            "f.nsn_resolution_status" if "nsn_resolution_status" in actual_cols
            else "CAST(NULL AS VARCHAR) AS nsn_resolution_status"
        ),
        "location_quality": (
            "f.location_quality" if "location_quality" in actual_cols
            else "CAST(NULL AS VARCHAR) AS location_quality"
        ),
        "base_award_description": "f.base_award_description",
        "latest_action_description": "f.latest_action_description",
        "obligations_in_selected_period_usd": "f.obligations_in_selected_period_usd",
        "number_of_actions_in_selected_period": "f.number_of_actions_in_selected_period",
        "earliest_action_date_in_selected_period": "f.earliest_action_date_in_selected_period",
        "latest_action_date_in_selected_period": "f.latest_action_date_in_selected_period",
        "award_type_code": "CAST(NULL AS VARCHAR) AS award_type_code",
        "award_type_description": "CAST(NULL AS VARCHAR) AS award_type_description",
    }
    select_parts = []
    for field in selected_fields:
        expr = field_exprs.get(field)
        if not expr:
            continue
        select_parts.append(expr if " AS " in expr else f"{expr} AS {quote_ident(field)}")
    select_clause = ", ".join(select_parts) or "f.contract_id"

    sql = f"""
        WITH candidate_contracts AS (
            SELECT
                r.*,
                ({obligations_expr}) AS obligations_in_selected_period_usd,
                ({actions_expr}) AS number_of_actions_in_selected_period,
                {earliest_expr} AS earliest_action_date_in_selected_period,
                {latest_expr} AS latest_action_date_in_selected_period
            FROM v_contracts_rolled r
            WHERE {where_clause}
            ORDER BY obligations_in_selected_period_usd DESC NULLS LAST, r.contract_id
            LIMIT ? OFFSET ?
        ),
        psc_map AS (
            SELECT psc_code, MAX(psc_description) AS psc_description
            FROM v_summary
            WHERE psc_code IS NOT NULL
            GROUP BY psc_code
        ),
        naics_map AS (
            SELECT naics_code, MAX(naics_description) AS naics_description
            FROM v_summary
            WHERE naics_code IS NOT NULL
            GROUP BY naics_code
        )
        SELECT {select_clause}
        FROM candidate_contracts f
        LEFT JOIN psc_map pm
          ON TRIM(CAST(f.psc AS VARCHAR)) = TRIM(CAST(pm.psc_code AS VARCHAR))
        LEFT JOIN naics_map nm
          ON TRIM(CAST(f.naics_code AS VARCHAR)) = TRIM(CAST(nm.naics_code AS VARCHAR))
        ORDER BY f.obligations_in_selected_period_usd DESC NULLS LAST, f.contract_id
    """
    params.extend([row_limit, offset])
    return sql, params


def build_contract_awards_explorer_query(
    payload: ExplorerRequest,
    row_limit: int,
    offset: int = 0,
    count_only: bool = False,
):
    # Item/source filters live at action grain. They are typically narrow, so
    # retain the action-based path only when those relationships are requested.
    granular_filter_keys = {"nsn", "niin", "part_number", "source_of_supply"}
    active_granular_filter = any(
        key in granular_filter_keys and value not in (None, "", [])
        for key, value in (payload.filters or {}).items()
    )
    requests_source_field = "source_of_supply" in {
        str(col).strip().lower() for col in (payload.columns or [])
    }
    if active_granular_filter or requests_source_field:
        return build_contract_awards_explorer_query_from_actions(
            payload,
            row_limit=row_limit,
            offset=offset,
            count_only=count_only,
        )
    return build_contract_awards_explorer_query_from_rollup(
        payload,
        row_limit=row_limit,
        offset=offset,
        count_only=count_only,
    )


def build_explorer_query(
    payload: ExplorerRequest,
    row_limit: int,
    offset: int = 0,
    count_only: bool = False
):
    table = payload.table if payload.table in ALLOWED_EXPLORER_TABLES else "v_contracts_rolled"

    if table == CONTRACT_AWARD_EXPLORER_TABLE:
        return build_contract_awards_explorer_query(
            payload,
            row_limit=row_limit,
            offset=offset,
            count_only=count_only,
        )

    actual_cols = get_duck_table_columns(table)

    if not actual_cols:
        raise HTTPException(status_code=503, detail=f"{table} is not available yet. Reload may still be running.")

    requested_cols = payload.columns or EXPLORER_DEFAULT_COLUMNS.get(table, [])
    select_parts: List[str] = []

    for requested_col in requested_cols:
        requested_clean = str(requested_col).strip().lower()
        if requested_clean not in ALLOWED_EXPLORER_COLUMNS:
            continue

        if requested_clean == "is_multi_platform_component":
            flag_expr = multi_platform_component_expr(actual_cols)
            if flag_expr:
                select_parts.append(f'{flag_expr} AS "is_multi_platform_component"')
            continue

        actual_col = resolve_explorer_column(table, requested_clean, actual_cols)
        if not actual_col:
            continue

        if actual_col == requested_clean:
            select_parts.append(quote_ident(actual_col))
        else:
            select_parts.append(f"{quote_ident(actual_col)} AS {quote_ident(requested_clean)}")

    if not select_parts:
        fallback_cols = EXPLORER_DEFAULT_COLUMNS.get(table, [])
        for requested_col in fallback_cols:
            actual_col = resolve_explorer_column(table, requested_col, actual_cols)
            if actual_col:
                select_parts.append(f"{quote_ident(actual_col)} AS {quote_ident(requested_col)}")

    if not select_parts:
        first_col = sorted(actual_cols)[0]
        select_parts = [quote_ident(first_col)]

    where_parts = ["1=1"]
    params: List[Any] = []
    has_valid_filter = False

    filters = payload.filters or {}

    # Spend filters only apply where a spend column actually exists.
    spend_col = None
    if table == "v_transactions" and "spend_amount" in actual_cols:
        spend_col = "spend_amount"
    elif table == SUBCONTRACT_EXPLORER_TABLE and "subcontract_value_usd" in actual_cols:
        spend_col = "subcontract_value_usd"
    elif "total_spend" in actual_cols:
        spend_col = "total_spend"
    elif "spend_amount" in actual_cols:
        spend_col = "spend_amount"

    if spend_col and "min_spend" in filters:
        try:
            where_parts.append(f"{quote_ident(spend_col)} >= ?")
            params.append(float(filters["min_spend"]))
            has_valid_filter = True
        except (ValueError, TypeError):
            pass

    if spend_col and "max_spend" in filters:
        try:
            where_parts.append(f"{quote_ident(spend_col)} <= ?")
            params.append(float(filters["max_spend"]))
            has_valid_filter = True
        except (ValueError, TypeError):
            pass

    # Generic search for the reference universe.
    search_value = filters.get("q") or filters.get("search") or filters.get("query")
    if search_value and table == NSN_REF_TABLE:
        search_cols = []
        for candidate in [
            "nsn", "niin", "part_number", "description", "item_name", "nomenclature",
            "cage", "cage_code", "vendor_cage", "vendor_name"
        ]:
            actual_col = resolve_explorer_column(table, candidate, actual_cols)
            if actual_col and actual_col not in search_cols:
                search_cols.append(actual_col)

        if search_cols:
            search_terms = []
            p = _like_param_contains(str(search_value))
            for c in search_cols:
                search_terms.append(f"UPPER(CAST({quote_ident(c)} AS VARCHAR)) LIKE ? ESCAPE '\\'")
                params.append(p)
            where_parts.append("(" + " OR ".join(search_terms) + ")")
            has_valid_filter = True

    for col, val in filters.items():
        if col in ["min_spend", "max_spend", "q", "search", "query"]:
            continue

        requested_col = str(col).strip().lower()
        if requested_col not in ALLOWED_EXPLORER_COLUMNS:
            continue

        if requested_col == "is_multi_platform_component":
            flag_expr = multi_platform_component_expr(actual_cols)
            normalised_value = str(val).strip().lower()
            if flag_expr and normalised_value in {"true", "false"}:
                where_parts.append(f"{flag_expr} = ?")
                params.append(normalised_value == "true")
                has_valid_filter = True
            continue

        if (
            table == NSN_REF_TABLE
            and requested_col == "platform_family"
            and "platform_families" in actual_cols
        ):
            values = val if isinstance(val, list) else [val]
            clean_values = [str(v).strip().upper() for v in values if str(v).strip()]
            if clean_values:
                membership_checks = [
                    "LIST_CONTAINS(STR_SPLIT(UPPER(CAST(\"platform_families\" AS VARCHAR)), ' | '), ?)"
                    for _ in clean_values
                ]
                where_parts.append("(" + " OR ".join(membership_checks) + ")")
                params.extend(clean_values)
                has_valid_filter = True
            continue

        actual_col = resolve_explorer_column(table, requested_col, actual_cols)
        if not actual_col:
            continue

        if val is None or str(val).strip() == "":
            continue

        has_valid_filter = True

        # NSN / NIIN filters match the canonical 9-digit NIIN.
        if requested_col in {"nsn", "niin"}:
            safe_niin = get_niin(str(val))
            where_parts.append(f"{normalised_niin_filter_expr(actual_col)} = ?")
            params.append(safe_niin)
            continue

        # CAGE filters are exact.
        if requested_col in {"cage", "cage_code", "vendor_cage"}:
            where_parts.append(f"UPPER(TRIM(CAST({quote_ident(actual_col)} AS VARCHAR))) = ?")
            params.append(str(val).strip().upper())
            continue

        if isinstance(val, list) and len(val) > 0:
            placeholders = ",".join(["?"] * len(val))
            where_parts.append(f"UPPER(TRIM(CAST({quote_ident(actual_col)} AS VARCHAR))) IN ({placeholders})")
            params.extend([str(v).strip().upper() for v in val])

        elif not isinstance(val, list):
            where_parts.append(f"UPPER(TRIM(CAST({quote_ident(actual_col)} AS VARCHAR))) = ?")
            params.append(str(val).strip().upper())

    if not has_valid_filter and table in EXPLORER_FILTER_REQUIRED_TABLES:
        raise HTTPException(
            status_code=400,
            detail="At least one filter is required for granular/reference data. Use cage_code, nsn, niin, part_number, or search."
        )

    where_clause = " AND ".join(where_parts)

    if count_only:
        sql = f"SELECT COUNT(*) AS count FROM {table} WHERE {where_clause}"
        return sql, params

    row_limit = safe_int(row_limit, 50, 1, 500_000)
    offset = safe_int(offset, 0, 0, 10_000_000)

    select_clause = ", ".join(select_parts)
    sql = f"SELECT {select_clause} FROM {table} WHERE {where_clause} LIMIT ? OFFSET ?"
    params.extend([row_limit, offset])

    return sql, params


@app.post("/api/explorer/preview")
def explorer_preview(payload: ExplorerRequest):
    sql, params = build_explorer_query(payload, row_limit=50, offset=0)

    try:
        df = duck_fetch_df(sql, params)
        if df.empty:
            return []
        return df_sanitize_for_json(df).to_dict(orient="records")

    except HTTPException:
        raise

    except Exception as e:
        logger.error(f"Explorer Preview Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate data preview.")


@app.post("/api/explorer/page")
def explorer_page(payload: ExplorerRequest):
    """
    Paginated explorer endpoint.

    Use this for the NSN/CAGE reference toggle:
    - show first 50/100 rows in the UI
    - let export handle the larger batch download
    """
    page_limit = safe_int(payload.limit, 100, 1, 500)
    page_offset = safe_int(payload.offset, 0, 0, 10_000_000)

    sql, params = build_explorer_query(payload, row_limit=page_limit, offset=page_offset)

    try:
        df = duck_fetch_df(sql, params)
        if df.empty:
            return []
        return df_sanitize_for_json(df).to_dict(orient="records")

    except HTTPException:
        raise

    except Exception as e:
        logger.error(f"Explorer Page Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate data page.")


@app.post("/api/explorer/count")
def explorer_count(payload: ExplorerRequest):
    """
    Count endpoint for pagination.

    Important for v_nsn_cage_reference so the UI can show:
    'Showing 100 of 12,431 rows' without loading all rows.
    """
    sql, params = build_explorer_query(payload, row_limit=0, count_only=True)

    try:
        df = duck_fetch_df(sql, params)
        if df.empty or "count" not in df.columns:
            return {"count": 0}

        val = df["count"].iloc[0]
        return {"count": 0 if pd.isna(val) else int(val)}

    except HTTPException:
        raise

    except Exception as e:
        logger.error(f"Explorer Count Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to count explorer rows.")


@app.post("/api/explorer/subcontract-descriptions")
def explorer_subcontract_descriptions(payload: SubcontractDescriptionsRequest):
    """Return non-additive source history for one retained subcontract row."""
    source_report_id = (payload.source_report_id or "").strip()
    source_dedup_key = (payload.source_dedup_key or "").strip()
    primary_description = (payload.primary_description or "").strip()
    lookup_key = source_report_id or source_dedup_key
    methodology = {
        "measure": "Net reported subcontract value for subawards dated within the selected fiscal-year period.",
        "source": "USAspending.gov first-tier subaward reports",
        "version": "Reported subcontract value v3",
        "additivity_note": "Prime obligations and subcontract values describe different procurement layers and should be analysed separately.",
    }
    fallback = {
        "reported_description_count": 1 if primary_description else 0,
        "descriptions": [primary_description] if primary_description else [],
        "audit": None,
        "methodology": methodology,
    }
    if not lookup_key:
        return fallback

    try:
        df = duck_fetch_df(
            """
            SELECT
                reported_description_count,
                equal_value_report_count,
                source_record_count,
                superseded_source_version_count,
                earliest_reported_action_date,
                latest_reported_action_date,
                report_id,
                report_dedup_key,
                report_last_modified_date,
                report_action_date,
                report_amount,
                report_description,
                is_current_source_version,
                CASE
                    WHEN NULLIF(TRIM(source_report_id), '') IS NOT NULL
                     AND report_id = source_report_id THEN TRUE
                    WHEN NULLIF(TRIM(source_dedup_key), '') IS NOT NULL
                     AND report_dedup_key = source_dedup_key THEN TRUE
                    ELSE FALSE
                END AS is_selected_source_report
            FROM v_subcontract_descriptions
            WHERE description_lookup_key = ?
            ORDER BY
                is_selected_source_report DESC,
                is_current_source_version DESC NULLS LAST,
                TRY_CAST(SUBSTR(report_last_modified_date, 1, 10) AS DATE) DESC NULLS LAST,
                TRY_CAST(SUBSTR(report_action_date, 1, 10) AS DATE) DESC NULLS LAST,
                report_id DESC NULLS LAST
            LIMIT 101
            """,
            [lookup_key],
        )
        if df.empty:
            return fallback

        descriptions = []
        for value in df["report_description"].tolist():
            if value is None or pd.isna(value):
                continue
            description = str(value).strip()
            if description and description not in descriptions:
                descriptions.append(description)
            if len(descriptions) >= 100:
                break
        if primary_description and primary_description not in descriptions:
            descriptions.insert(0, primary_description)

        def optional_int(column_name: str) -> Optional[int]:
            value = df[column_name].iloc[0]
            return None if pd.isna(value) else int(value)

        def optional_text(column_name: str) -> Optional[str]:
            value = df[column_name].iloc[0]
            return None if pd.isna(value) else str(value)

        source_record_count = optional_int("source_record_count") or len(df)
        report_rows = df.head(100)
        reports = []
        for _, row in report_rows.iterrows():
            reported_amount = row.get("report_amount")
            reports.append({
                "report_id": None if pd.isna(row.get("report_id")) else str(row.get("report_id")),
                "source_record_key": None if pd.isna(row.get("report_dedup_key")) else str(row.get("report_dedup_key")),
                "last_modified_date": None if pd.isna(row.get("report_last_modified_date")) else str(row.get("report_last_modified_date")),
                "action_date": None if pd.isna(row.get("report_action_date")) else str(row.get("report_action_date")),
                "reported_amount": None if pd.isna(reported_amount) else float(reported_amount),
                "description": None if pd.isna(row.get("report_description")) else str(row.get("report_description")),
                "is_current_source_version": bool(row.get("is_current_source_version")) if not pd.isna(row.get("is_current_source_version")) else False,
                "is_selected_source_report": bool(row.get("is_selected_source_report")) if not pd.isna(row.get("is_selected_source_report")) else False,
            })

        return {
            "reported_description_count": len(descriptions),
            "descriptions": descriptions,
            "audit": {
                "equal_value_report_count": optional_int("equal_value_report_count"),
                "source_record_count": source_record_count,
                "superseded_source_version_count": optional_int("superseded_source_version_count"),
                "earliest_reported_action_date": optional_text("earliest_reported_action_date"),
                "latest_reported_action_date": optional_text("latest_reported_action_date"),
                "reports_returned": len(reports),
                "reports_truncated": source_record_count > len(reports),
                "reports": reports,
            },
            "methodology": methodology,
        }
    except Exception as e:
        logger.error(f"Subcontract Description Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to load subcontract source details.")


def cleanup_temp_file(filepath: str):
    try:
        if os.path.exists(filepath):
            os.remove(filepath)
            logger.info(f"Cleaned up temp export file: {filepath}")
    except Exception as e:
        logger.error(f"Failed to delete temp file {filepath}: {e}")


@app.get("/api/explorer/vendors/search")
def explorer_vendor_search(q: str = Query(..., min_length=2)):
    """Powers the ultra-fast React Typeahead for Vendor selection"""
    sql = """
        SELECT MAX(vendor_name) as vendor_name, cage_code, MAX(city) as city, MAX(state) as state 
        FROM v_summary 
        WHERE UPPER(vendor_name) LIKE ? OR UPPER(cage_code) LIKE ? 
        GROUP BY cage_code
        LIMIT 10
    """
    params = [f"%{q.upper()}%", f"%{q.upper()}%"]

    try:
        df = duck_fetch_df(sql, params)
        if df.empty:
            return []
        return df_sanitize_for_json(df).to_dict(orient="records")

    except Exception as e:
        logger.error(f"Vendor Autocomplete Error: {e}")
        raise HTTPException(status_code=500, detail="Vendor search failed")


@app.get("/api/explorer/taxonomy/search")
def explorer_taxonomy_search(type: str = Query(...), q: str = Query(..., min_length=2)):
    """Powers the Autocomplete Dropdowns for NAICS and PSC codes"""
    col_code = "naics_code" if type == "naics" else "psc_code"
    col_desc = "naics_description" if type == "naics" else "psc_description"
    
    sql = f"""
        SELECT {col_code} as code, MAX({col_desc}) as description
        FROM v_summary 
        WHERE {col_code} IS NOT NULL 
          AND (UPPER(CAST({col_code} AS VARCHAR)) LIKE ? OR UPPER({col_desc}) LIKE ?)
        GROUP BY {col_code}
        LIMIT 15
    """
    params = [f"%{q.upper()}%", f"%{q.upper()}%"]

    try:
        df = duck_fetch_df(sql, params)
        if df.empty:
            return []
        return df_sanitize_for_json(df).to_dict(orient="records")

    except Exception as e:
        logger.error(f"Taxonomy Autocomplete Error: {e}")
        raise HTTPException(status_code=500, detail="Taxonomy search failed")


@app.post("/api/explorer/export")
def explorer_export(payload: ExplorerRequest, background_tasks: BackgroundTasks, request: Request):
    expected_key = os.getenv("MIMIR_EXPORT_PROXY_SECRET", "").strip()
    supplied_key = request.headers.get("X-Mimir-Export-Key", "").strip()
    require_proxy = os.getenv("REQUIRE_EXPORT_PROXY", "0").strip().lower() in {"1", "true", "yes"}
    proxy_verified = bool(expected_key and hmac.compare_digest(supplied_key, expected_key))

    if require_proxy and not expected_key:
        logger.error("REQUIRE_EXPORT_PROXY is enabled without MIMIR_EXPORT_PROXY_SECRET")
        raise HTTPException(status_code=503, detail="Export authorization is not configured.")
    if require_proxy and not proxy_verified:
        raise HTTPException(status_code=403, detail="Export requires an authorized application session.")

    if proxy_verified:
        plan_tier = request.headers.get("X-Mimir-Plan-Tier", "free").strip().lower()
        if plan_tier == "enterprise":
            export_limit = safe_int(
                os.getenv("EXPLORER_EXPORT_LIMIT_ENTERPRISE", 100000),
                100000,
                5000,
                100000,
            )
        elif plan_tier == "professional":
            export_limit = 5000
        else:
            raise HTTPException(status_code=403, detail="This plan does not include CSV exports.")
    else:
        # Transitional behavior only. Enable REQUIRE_EXPORT_PROXY after the UI proxy is deployed.
        export_limit = 5000 if payload.subscription_status == "active" else 1000

    if payload.table == SUBCONTRACT_EXPLORER_TABLE:
        requested_columns = payload.columns or EXPLORER_DEFAULT_COLUMNS[SUBCONTRACT_EXPLORER_TABLE]
        payload.columns = [
            column for column in requested_columns
            if str(column).strip().lower() not in SUBCONTRACT_INTERNAL_EXPORT_COLUMNS
        ]
        payload.filters = {
            key: value for key, value in (payload.filters or {}).items()
            if str(key).strip().lower() not in SUBCONTRACT_INTERNAL_EXPORT_COLUMNS
        }

    sql, params = build_explorer_query(payload, row_limit=export_limit, offset=0)
    
    filename = f"mimir_export_{uuid.uuid4().hex[:8]}.csv"
    filepath = str((LOCAL_CACHE_DIR / filename).resolve())
    
    try:
        if payload.table == NSN_REF_TABLE:
            df = duck_fetch_df(sql, params)
            customer_headers = {
                "nsn": "National Stock Number (NSN)",
                "niin": "National Item Identification Number (NIIN)",
                "cage_code": "CAGE Code",
                "vendor_name": "Organization Name",
                "part_number": "Part Number",
                "description": "Item Description",
                "fsc_code": "Federal Supply Class (FSC)",
                "platform_family": "Mapped Platform",
                "market_segment": "Market Domain",
                "psc": "Product and Service Code (PSC)",
                "supplier_status": "Relationship Status",
                "supplier_status_detail": "Relationship Status Detail",
                "demil_code": "Controlled Item Code",
                "shelf_life_code": "Shelf-Life Code",
                "mgmt_control_code": "Management Control Code",
                "unit_of_issue": "Unit of Issue",
                "source_of_supply": "Managing Supply Activity",
                "govt_estimated_price": "Government Estimated Unit Price (USD)",
                "acquisition_advice_code": "Acquisition Advice Code (AAC)",
                "rnsc_codes": "Reference Status Code (RNSC)",
                "rncc_codes": "Reference Category Code (RNCC)",
                "rnvc_codes": "Reference Variation Code (RNVC)",
                "cage_status_codes": "CAGE Status Code",
                "is_procurement_authorized": "Procurement Authorized",
                "is_active_authorized_source": "Active Authorized Source",
            }
            df.rename(columns=customer_headers).to_csv(filepath, index=False)
        else:
            with DUCK_LOCK:
                conn = ensure_duck_conn()
                copy_sql = f"COPY ({sql}) TO '{filepath}' (HEADER, DELIMITER ',');"
                conn.execute(copy_sql, params)

            if payload.table == SUBCONTRACT_EXPLORER_TABLE:
                customer_headers = {
                    "prime_name": "Prime Contractor",
                    "prime_cage": "Prime CAGE",
                    "prime_award_id": "Prime Award ID",
                    "subcontractor_name": "Subcontractor",
                    "subcontractor_cage": "Subcontractor CAGE",
                    "subcontract_id": "Subcontract ID",
                    "subcontract_description": "Subcontract Description",
                    "subcontract_action_date": "Subcontract Action Date",
                    "year": "Fiscal Year",
                    "subcontract_value_usd": "Mimir Modelled Subcontract Value (USD)",
                    "subcontract_value_raw_usd": "Reported Raw Value (USD)",
                    "prime_award_control_value": "Prime Award Value (USD)",
                    "subcontractor_city": "Subcontract Place of Performance City",
                    "subcontractor_state": "Subcontract Place of Performance State",
                    "platform_family": "Platform",
                    "market_segment": "Market Domain",
                    "psc_code": "PSC Code",
                    "psc_description": "PSC Description",
                    "naics_code": "NAICS Code",
                    "naics_description": "NAICS Description",
                }
                renamed_filepath = f"{filepath}.headers"
                with open(filepath, "r", encoding="utf-8", newline="") as source_file, open(
                    renamed_filepath, "w", encoding="utf-8", newline=""
                ) as target_file:
                    reader = csv.reader(source_file)
                    writer = csv.writer(target_file)
                    header = next(reader, [])
                    writer.writerow([customer_headers.get(column, column) for column in header])
                    writer.writerows(reader)
                os.replace(renamed_filepath, filepath)
        
        background_tasks.add_task(cleanup_temp_file, filepath)
        
        with open(filepath, "r", encoding="utf-8", newline="") as export_file:
            row_count = max(0, sum(1 for _ in csv.reader(export_file)) - 1)

        return FileResponse(
            path=filepath, 
            media_type='text/csv', 
            filename="mimir_data_export.csv",
            headers={"X-Export-Row-Count": str(row_count)},
        )

    except HTTPException:
        raise

    except Exception as e:
        logger.error(f"Explorer Export Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate export file.")

# ==========================================
#        MARKET DASHBOARD ENDPOINTS
# ==========================================

@app.get("/api/dashboard/status")
def get_status():
    s = get_readiness_state()
    return {
        "ready": bool(s["ready"]),
        "is_loading": bool(s["is_loading"]),
        "last_loaded": s["last_loaded"],
        "duck_ok": bool(s["duck_ok"]),
        "geo_ok": bool(s["geo_ok"]),
        "profiles_ok": bool(s["profiles_ok"]),
    }




@app.post("/api/dashboard/reload")
async def trigger_reload():
    # Truthful guard: lock + is_loading
    if RELOAD_LOCK.locked() or GLOBAL_CACHE.get("is_loading"):
        return {"message": "Reload already running"}

    # ✅ Offload reload from request thread
    asyncio.create_task(asyncio.to_thread(reload_all_data))
    return {"message": "Reloading..."}



# [FIND THIS FUNCTION]
@app.get("/api/dashboard/filter-options")
def get_filter_options(
    years: Optional[List[int]] = Query(None),
    vendor: Optional[str] = None,
    parent: Optional[str] = None,
    cage: Optional[str] = None,
    domain: Optional[str] = None,
    agency: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None
):
    filters = {
        "vendor": vendor,
        "parent": parent,
        "cage": cage,
        "domain": domain,
        "agency": agency,
        "platform": platform,
        "psc": psc
    }

    # If no filters: return cached options + top parents (DuckDB)
    if not any(filters.values()):
        opts = (GLOBAL_CACHE.get("options", {}) or {}).copy()

        where_sql, params = build_summary_where(years, {})
        top_df = query_summary_df(
            where_sql=where_sql,
            params=params,
            select_sql="clean_parent as label, sum(total_spend) as spend",
            # ✅ FIX: Add this line below
            group_by_sql="clean_parent", 
            order_by_sql="spend DESC",
            limit=50
        )
        opts["top_parents"] = top_df["label"].dropna().astype(str).tolist() if not top_df.empty else []
        return opts


    # With filters: compute option lists from filtered universe (DuckDB DISTINCTs)
    where_sql, params = build_summary_where(years, filters)

    # Run all 4 aggregations concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        f_agencies = executor.submit(query_summary_df, where_sql, params, "DISTINCT sub_agency", order_by_sql="sub_agency ASC", limit=5000)
        f_domains  = executor.submit(query_summary_df, where_sql, params, "DISTINCT market_segment", order_by_sql="market_segment ASC", limit=5000)
        f_plats    = executor.submit(query_summary_df, where_sql, params, "DISTINCT platform_family", order_by_sql="platform_family ASC", limit=5000)
        f_psc      = executor.submit(query_summary_df, where_sql, params, "DISTINCT psc_code, psc_description", order_by_sql="psc_code ASC, psc_description ASC", limit=5000)

        agencies_df = f_agencies.result()
        domains_df  = f_domains.result()
        plats_df    = f_plats.result()
        psc_df      = f_psc.result()

    return {
        "years": (GLOBAL_CACHE.get("options", {}) or {}).get("years", []),
        "agencies": agencies_df["sub_agency"].dropna().astype(str).tolist() if "sub_agency" in agencies_df.columns else [],
        "domains": domains_df["market_segment"].dropna().astype(str).tolist() if "market_segment" in domains_df.columns else [],
        "platforms": plats_df["platform_family"].dropna().astype(str).tolist() if "platform_family" in plats_df.columns else [],
        "psc_pairs": (
            psc_df.dropna().drop_duplicates()[["psc_code", "psc_description"]].to_dict(orient="records")
            if ("psc_code" in psc_df.columns and "psc_description" in psc_df.columns)
            else []
        ),
    }




# --- UPDATE IN API.PY ---

def get_recompete_kpi(filters):
    where_sql, params = build_summary_where(None, filters)

    try:
        risk_schema = duck_fetch_df("DESCRIBE v_risk")
        risk_columns = {
            str(value).strip().lower()
            for value in risk_schema.get("column_name", pd.Series(dtype=str)).dropna().tolist()
        }
    except Exception:
        risk_columns = set()
    risk_identity = "award_key" if "award_key" in risk_columns else "contract_id"
    
    query = f"""
        SELECT 
            SUM(spend_amount) as total_value,
            COUNT(DISTINCT {risk_identity}) as count
        FROM v_risk
        WHERE {where_sql}
          AND try_cast(completion_date as date) >= current_date()
          AND try_cast(completion_date as date) <= current_date() + INTERVAL 90 DAY
    """
    try:
        df = duck_fetch_df(query, params)
        if df.empty or pd.isna(df['total_value'].iloc[0]):
            return {"label": "Obligations on Awards Ending (90d)", "value": "N/A", "sub_label": "No Data"}
            
        total_value = float(df['total_value'].iloc[0] or 0)
        count = int(df['count'].iloc[0] or 0)
        
        return {
            "label": "Obligations on Awards Ending (90d)",
            "value": f"${total_value/1e9:.2f}B",
            "sub_label": f"{count} awards ending",
            "status": "warning"
        }
    except Exception as e:
        logger.error(f"Risk KPI Error: {e}")
        return {"label": "Obligations on Awards Ending (90d)", "value": "N/A", "sub_label": "No Data"}
    

@app.get("/api/dashboard/kpis")
def get_market_kpis(
    years: Optional[List[int]] = Query(None),
    vendor: Optional[str] = None,
    parent: Optional[str] = None,
    cage: Optional[str] = None,
    domain: Optional[str] = None,
    agency: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None
):
    filters = {
        "vendor": vendor,
        "parent": parent,
        "cage": cage,
        "domain": domain,
        "agency": agency,
        "platform": platform,
        "psc": psc
    }

    where_sql, params = build_summary_where(years, filters)

    kpi_df = query_summary_df(
        where_sql=where_sql,
        params=params,
        select_sql="sum(total_spend) as total_spend, sum(contract_count) as total_contracts",
        limit=1
    )

    # Safely extract and check for NaN before casting
    ts_val = kpi_df["total_spend"].iloc[0] if (not kpi_df.empty and "total_spend" in kpi_df.columns) else 0.0
    total_spend = 0.0 if pd.isna(ts_val) else float(ts_val)

    tc_val = kpi_df["total_contracts"].iloc[0] if (not kpi_df.empty and "total_contracts" in kpi_df.columns) else 0
    total_contracts = 0 if pd.isna(tc_val) else int(tc_val)

    recompete_data = get_recompete_kpi(filters)

    return {
        "total_spend_b": total_spend / 1_000_000_000.0,
        "total_contracts": total_contracts,
        "recompete_risk": recompete_data
    }


# --- ADD TO API.PY ---



# Usage in your main endpoint:
# kpis["recompete_risk"] = get_recompete_kpi(filtered_df)

# --- REPLACE THIS FUNCTION IN API.PY ---

# [Find this function in api.py]
@app.get("/api/dashboard/trend")
def get_spend_trend(
    years: Optional[List[int]] = Query(None),
    vendor: Optional[str] = None,
    parent: Optional[str]=None,
    cage: Optional[str] = None,
    domain: Optional[str] = None,
    agency: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None,
    mode: str = "yearly",
):
    filters = {
        "vendor": vendor, "parent": parent, "cage": cage,
        "domain": domain, "agency": agency, "platform": platform, "psc": psc,
    }

    # ✅ Let DuckDB handle the year filtering instantly
    where_sql, params = build_summary_where(years, filters)

    if mode == "yearly":
        df = query_summary_df(
            where_sql=where_sql,
            params=params,
            select_sql="CAST(year AS INTEGER) AS fy, SUM(total_spend) AS spend",
            group_by_sql="CAST(year AS INTEGER)",
            order_by_sql="fy ASC",
            limit=0
        )

        if df.empty:
            return []

        min_year = int(df["fy"].min())
        max_year = int(df["fy"].max())
        data_map = {int(r["fy"]): float(r["spend"]) for _, r in df.iterrows()}

        final_data = []
        for y in range(min_year, max_year + 1):
            final_data.append({"label": str(y), "spend": float(data_map.get(y, 0.0))})
        return final_data

    if mode == "monthly":
        # ✅ Let DuckDB group the months
        df = query_summary_df(
            where_sql=where_sql,
            params=params,
            select_sql="CAST(COALESCE(month, 1) AS INTEGER) AS month_num, SUM(total_spend) AS spend",
            group_by_sql="CAST(COALESCE(month, 1) AS INTEGER)",
            limit=0
        )

        if df.empty:
            return []

        df.columns = ["label", "spend"]

        # ✅ Retained your exact fiscal sorting logic
        def get_fiscal_sort(m):
            try:
                m = int(m)
                return m - 9 if m >= 10 else m + 3
            except:
                return 0

        df["sort_index"] = df["label"].apply(get_fiscal_sort)
        df = df.sort_values("sort_index", ascending=True)
        df["spend"] = df["spend"].astype(float)

        return df[["label", "spend"]].to_dict(orient="records")

    return []


# ✅ NEW: Drill-down endpoint
from typing import Optional, List
from fastapi import Query

@app.get("/api/dashboard/subsidiaries")
def get_dashboard_subsidiaries(
    parent: str,
    years: Optional[List[int]] = Query(None),
    agency: Optional[str] = None,
    domain: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None
):
    if not parent:
        return []

    clean_parent = sanitize(parent)
    loc_map = GLOBAL_CACHE.get("location_map", {}) or {}

    # 1. Base Filter
    where_parts = ["upper(clean_parent) = ?"]
    params = [clean_parent]

    # 2. Append Global Filters
    if agency:
        where_parts.append("upper(sub_agency) = ?")
        params.append(sanitize(agency))
    if domain:
        where_parts.append("upper(market_segment) = ?")
        params.append(sanitize(domain))
    if platform:
        where_parts.append("upper(platform_family) = ?")
        params.append(sanitize(platform))
    if psc:
        where_parts.append("upper(psc_code) = ?")
        params.append(sanitize(psc))
    if years and len(years) > 0:
        placeholders = ",".join(["?"] for _ in years)
        where_parts.append(f"year IN ({placeholders})")
        params.extend([int(y) for y in years])

    where_clause = " AND ".join(where_parts)

    # Filter by clean_parent and aggregate by cage + vendor_name
    df = query_summary_df(
        where_sql=where_clause,
        params=params,
        select_sql="""
            cage_code,
            vendor_name,
            SUM(total_spend) AS total_spend,
            SUM(contract_count) AS contract_count
        """,
        group_by_sql="cage_code, vendor_name",
        order_by_sql="total_spend DESC",
        limit=200
    )

    if df.empty:
        return []

    # Fast vectorized cleanup and mapping
    df["cage"] = df["cage_code"].astype(str).str.strip().str.upper()
    df["name"] = df["vendor_name"].astype(str)
    df["total_obligations"] = df["total_spend"].fillna(0).astype(float)
    df["contract_count"] = df["contract_count"].fillna(0).astype(int)
    
    # Map location data vector-style
    df["city"] = df["cage"].apply(lambda c: str(loc_map.get(c, {}).get("city", "") or "N/A"))
    df["state"] = df["cage"].apply(lambda c: str(loc_map.get(c, {}).get("state", "") or "N/A"))

    return df[["cage", "name", "total_obligations", "contract_count", "city", "state"]].to_dict(orient="records")



@app.get("/api/dashboard/top-vendors")
def get_top_vendors(
    years: Optional[List[int]] = Query(None),
    vendor: Optional[str] = None,
    parent: Optional[str] = None,
    cage: Optional[str] = None,
    domain: Optional[str] = None,
    agency: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None
):
    filters = {
        "vendor": vendor,
        "parent": parent,
        "cage": cage,
        "domain": domain,
        "agency": agency,
        "platform": platform,
        "psc": psc
    }

    where_sql, params = build_summary_where(years, filters)

    df = query_summary_df(
        where_sql=where_sql,
        params=params,
        select_sql="clean_parent as vendor, sum(total_spend) as total_spend, sum(contract_count) as contract_count",
        group_by_sql="clean_parent",  # ✅ ADDED
        order_by_sql="total_spend DESC",
        limit=50
    )

    if df.empty:
        return []

    # Vectorized casting and math (C-speed)
    df["vendor"] = df["vendor"].astype(str)
    df["spend_m"] = (df["total_spend"].fillna(0).astype(float) / 1_000_000.0)
    df["contracts"] = df["contract_count"].fillna(0).astype(int)

    # Export directly to list of dictionaries
    return df[["vendor", "spend_m", "contracts"]].to_dict(orient="records")


# --- REPLACE THIS FUNCTION IN API.PY ---

# --- REPLACE THIS FUNCTION IN API.PY ---

@app.get("/api/dashboard/distributions")
def get_market_distributions(
    years: Optional[List[int]] = Query(None),
    vendor: Optional[str] = None,
    parent: Optional[str] = None,
    cage: Optional[str] = None,
    domain: Optional[str] = None,
    agency: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None,
    threshold_m: Optional[str] = None, # ✅ ADDED
    mode: Optional[str] = None
):
    filters = {
        "vendor": vendor,
        "parent": parent,
        "cage": cage,
        "domain": domain,
        "agency": agency,
        "platform": platform,
        "psc": psc,
        "threshold_m": threshold_m
    }

    where_sql, params = build_summary_where(years, filters)

    total_df = query_summary_df(where_sql, params, "sum(total_spend) as total_spend", limit=1)
    # Safely extract and check for NaN before casting
    ts_val = total_df["total_spend"].iloc[0] if (not total_df.empty and "total_spend" in total_df.columns) else 0.0
    total = 0.0 if pd.isna(ts_val) else float(ts_val)
    if total <= 0:
        return {"platform_dist": [], "domain_dist": []}

    def dist_for(col: str) -> List[Dict[str, Any]]:
        d = query_summary_df(
            where_sql, params,
            select_sql=f"{col} as label, sum(total_spend) as spend",
            group_by_sql="1",  # ✅ ADDED (Groups by 1st column, which is 'label')
            order_by_sql="spend DESC",
            limit=10
        )
        if d.empty or "label" not in d.columns:
            return []
        d = d.dropna(subset=["label"])
        if d.empty:
            return []

        top = d.head(4).copy() # Use .copy() to avoid SettingWithCopyWarning
        top_sum = float(top["spend"].sum()) if "spend" in top.columns else 0.0
        other_val = max(0.0, total - top_sum)

        # Vectorized percentage calculation
        top["label"] = top["label"].astype(str)
        top["value"] = ((top["spend"].fillna(0).astype(float) / total) * 100.0).round(1)

        out = top[["label", "value"]].to_dict(orient="records")
        
        if other_val > 0:
            out.append({"label": "Other", "value": round((other_val / total) * 100.0, 1)})
        return out

    return {
        "platform_dist": dist_for("platform_family"),
        "domain_dist": dist_for("psc_description"),
    }


@app.get("/api/dashboard/map")
def get_map_data(
    years: Optional[List[int]] = Query(None),
    vendor: Optional[str]=None,
    parent: Optional[str]=None,
    cage: Optional[str]=None,
    domain: Optional[str]=None,
    agency: Optional[str]=None,
    platform: Optional[str]=None,
    psc: Optional[str]=None
):
    filters = {
        "vendor": vendor, "parent": parent, "cage": cage,
        "domain": domain, "agency": agency, "platform": platform, "psc": psc
    }

    where_sql, params = build_summary_where(years, filters)

    # ✅ DuckDB CTE perfectly mimics the Pandas 150k intermediate frame + string cleanup + final 50k merge
    query = f"""
        WITH active_vendors AS (
            SELECT 
                UPPER(TRIM(CAST(cage_code AS VARCHAR))) AS join_key,
                MAX(vendor_name) AS vendor_name,
                SUM(total_spend) AS total_spend
            FROM v_summary
            WHERE {where_sql}
            GROUP BY UPPER(TRIM(CAST(cage_code AS VARCHAR)))
            ORDER BY total_spend DESC
            LIMIT 150000
        )
        SELECT 
            a.join_key AS id,
            a.vendor_name AS vendor,
            a.join_key AS cage,
            CAST(g.latitude AS DOUBLE) AS lat,
            CAST(g.longitude AS DOUBLE) AS lon,
            CAST(a.total_spend AS DOUBLE) AS spend
        FROM active_vendors a
        INNER JOIN v_geo g 
            ON a.join_key = UPPER(TRIM(CAST(g.cage_code AS VARCHAR)))
        WHERE g.latitude IS NOT NULL 
          AND g.longitude IS NOT NULL
        ORDER BY a.total_spend DESC
        LIMIT 50000
    """
    
    try:
        df = duck_fetch_df(query, params)
        if df.empty and cage:
            clean_cage = sanitize(cage).upper().strip()
            try:
                df = duck_fetch_df(
                    """
                    SELECT
                        UPPER(TRIM(CAST(cage_code AS VARCHAR))) AS id,
                        COALESCE(vendor_name, 'CAGE ' || UPPER(TRIM(CAST(cage_code AS VARCHAR)))) AS vendor,
                        UPPER(TRIM(CAST(cage_code AS VARCHAR))) AS cage,
                        CAST(latitude AS DOUBLE) AS lat,
                        CAST(longitude AS DOUBLE) AS lon,
                        CAST(0 AS DOUBLE) AS spend
                    FROM v_cage_locations
                    WHERE UPPER(TRIM(CAST(cage_code AS VARCHAR))) = ?
                      AND latitude IS NOT NULL
                      AND longitude IS NOT NULL
                    LIMIT 1
                    """,
                    [clean_cage],
                )
            except Exception:
                df = pd.DataFrame()

        if df.empty: return []
        
        # ✅ FIX 4: Sanitize NaNs
        df = df_sanitize_for_json(df)
        
        # Cast columns vector-style to ensure JSON serialization succeeds
        df["id"] = df["id"].astype(str)
        df["vendor"] = df["vendor"].astype(str)
        df["cage"] = df["cage"].astype(str)
        df["lat"] = df["lat"].astype(float)
        df["lon"] = df["lon"].astype(float)
        df["spend"] = df["spend"].astype(float)

        # Export directly to a list of dictionaries at C-speed
        return df[["id", "vendor", "cage", "lat", "lon", "spend"]].to_dict(orient="records")
    except Exception as e:
        logger.error(f"Map Query Error: {e}")
        return []


# --- RESTORED: MARKET OPPORTUNITIES ---
# --- UPDATE IN API.PY ---

@app.get("/api/dashboard/opportunities")
def get_market_opportunities(
    domain: Optional[str] = None,
    agency: Optional[str] = None,
    platform: Optional[str] = None,
    vendor: Optional[str] = None,
):
    # ✅ OPTIMIZED: Query the local DuckDB view instead of AWS Athena!
    query_base = """
    SELECT 
        id as noticeid, sol_num, title, agency, deadline, 
        naics as naicscode, set_aside_type as setaside, 
        poc_email as primarycontactemail, COUNT(*) OVER() as total_matches
    FROM v_opportunities
    WHERE try_cast(deadline as date) >= current_date()
    """

    conditions = []
    params = []

    if agency:
        conditions.append("upper(agency) LIKE ? ESCAPE '#'")
        params.append(f"%{agency.upper().replace('%', '#%').replace('_', '#_')}%")

    # Helper for searching title and description
    def add_search(val: str):
        conditions.append("(upper(title) LIKE ? ESCAPE '#' OR upper(description) LIKE ? ESCAPE '#')")
        clean_val = f"%{val.upper().replace('%', '#%').replace('_', '#_')}%"
        params.extend([clean_val, clean_val])

    if domain: add_search(domain)
    if platform: add_search(platform)
    if vendor: add_search(vendor)

    if conditions:
        query_base += " AND " + " AND ".join(conditions)

    query_base += " ORDER BY try_cast(deadline as date) ASC NULLS LAST LIMIT 50"

    # Use the blazing fast DuckDB connection pool
    df = duck_fetch_df(query_base, params)
    if df.empty:
        return []
        
    return df.to_dict(orient="records")

# ==========================================
#        GLOBAL SEARCH (NEW)
# ==========================================


@app.get("/api/search/global")
def search_global(q: str):
    if not q or len(q) < 2: return []
    clean_q = sanitize(q)
    
    try:
        search_val_text = f"%{clean_q}%"
        search_val_cage = f"{clean_q}%" 
        
        # 1. FAST NATIVE SEARCH: Pull a larger buffer of raw fragments
        # We use LIMIT 100 so we have enough fragments to sum together accurately, 
        # but small enough that DuckDB doesn't break a sweat.
        query = """
            SELECT type, label, value, score, cage, city, state
            FROM search_index
            WHERE label ILIKE ? OR cage ILIKE ? OR value ILIKE ?
            ORDER BY score DESC
            LIMIT 100
        """
        
        df_search = duck_fetch_df(query, [search_val_text, search_val_cage, search_val_text])

        # 2. IN-MEMORY AGGREGATION: Group and sum the fragments safely in Python
        aggregated_results = {}
        
        for r in df_search.itertuples(index=False):
            item_type = getattr(r, "type", "")
            item_label = getattr(r, "label", "")
            item_value = getattr(r, "value", "")
            item_score = float(getattr(r, "score", 0) or 0)
            item_cage = getattr(r, "cage", "")
            item_city = getattr(r, "city", "")
            item_state = getattr(r, "state", "")

            type_upper = item_type.upper()

            # Create the unique deduplication key
            if type_upper in ["CHILD", "VENDOR"] and item_cage and item_cage != "AGGREGATE":
                dedupe_key = f"site-{item_cage.strip().upper()}"
            elif type_upper == "PARENT":
                dedupe_key = f"parent-{item_label.strip().upper()}"
            else:
                dedupe_key = f"{type_upper}-{str(item_value or item_label).strip().upper()}"

            # If it's a new entity, initialize it. If it exists, add the score to the total.
            if dedupe_key not in aggregated_results:
                aggregated_results[dedupe_key] = {
                    "type": item_type,
                    "label": item_label,
                    "value": item_value,
                    "cage": item_cage,
                    "city": item_city,
                    "state": item_state,
                    "total_score": 0.0
                }
            
            aggregated_results[dedupe_key]["total_score"] += item_score

        # 3. SORT & SLICE: Rank by the newly combined totals and grab the top 8
        sorted_entities = sorted(aggregated_results.values(), key=lambda x: x["total_score"], reverse=True)[:8]

        results = []
        for item in sorted_entities:
            item_type = item["type"]
            item_cage = item["cage"]
            item_city = item["city"]
            item_state = item["state"]
            total_val = item["total_score"]

            # --- SMART FORMATTING FOR DOLLAR AMOUNTS ---
            spend_str = ""
            if total_val > 0:
                if total_val >= 1_000_000_000:
                    spend_str = f"${total_val / 1_000_000_000:,.1f}B"
                elif total_val >= 1_000_000:
                    spend_str = f"${total_val / 1_000_000:,.1f}M"
                else:
                    spend_str = f"${total_val / 1_000:,.0f}K"

            # --- SMART LABELING ---
            if item_type == 'OPPORTUNITY':
                sub_label = f"Bid • {item_city}" 

            elif item_type == 'CHILD' and item_cage and item_cage != 'AGGREGATE':
                loc_str = f"{item_city}, {item_state}" if item_city and item_state else item_city
                base_label = f"{loc_str} • CAGE: {item_cage}" if loc_str else f"CAGE: {item_cage}"
                sub_label = f"{base_label} • {spend_str}" if spend_str else base_label

            elif item_type == 'PARENT':
                sub_label = f"Corporate Rollup • {spend_str}" if spend_str else "Corporate Rollup"
            
            else:
                sub_label = spend_str if spend_str else ""

            results.append({
                "type": item_type,
                "label": item["label"],
                "value": item["value"],
                "sub_label": sub_label,
                "cage": item_cage
            })
            
    except Exception as e:
        logger.error(f"Native search query failed: {e}")
        results = []

    # 4. NSN / NIIN Detection (Always injects at the top if it matches)
    nsn_pattern = r'^\d{4}-?\d{2}-?\d{3}-?\d{4}$|^\d{9}$|^\d{13}$'
    if re.match(nsn_pattern, q.strip()):
        results.insert(0, {
            "type": "NSN",
            "label": f"Lookup Part: {q.strip()}",
            "value": q.strip(),
            "sub_label": "Supply Chain Search"
        })

    return results

# ==========================================
#        PLATFORM INTELLIGENCE
# ==========================================

@app.get("/api/platform/top")
def get_top_platforms(
    limit: int = 12,
    years: Optional[List[int]] = Query(None),   # FY years
    vendor: Optional[str] = None,
    parent: Optional[str] = None,
    cage: Optional[str] = None,
    agency: Optional[str] = None,
    domain: Optional[str] = None,
    platform: Optional[str] = None,            # still supported; narrows to one platform if provided
    psc: Optional[str] = None
):
    limit_i = safe_int(limit, 12, 1, 100)

    filters = {
        "vendor": vendor,
        "parent": parent,
        "cage": cage,
        "agency": agency,
        "domain": domain,
        "platform": platform,
        "psc": psc,
    }

    where_sql, params = build_summary_where(years, filters, use_fy_logic=True)

    # Low-RAM: aggregate inside DuckDB over summary_clean parquet
    df = query_summary_df(
        where_sql=where_sql,
        params=params,
        select_sql="""
            platform_family,
            SUM(total_spend) AS total_spend,
            SUM(contract_count) AS contract_count
        """,
        group_by_sql="platform_family",
        order_by_sql="total_spend DESC",
        limit=limit_i
    )

    if df.empty or "platform_family" not in df.columns:
        return []

    out = []
    for r in df.itertuples(index=False):
        name = getattr(r, "platform_family", None)
        if not name or str(name).strip() == "":
            continue
        out.append({
            "name": str(name),
            "spend": float(getattr(r, "total_spend", 0) or 0),
            "contracts": int(getattr(r, "contract_count", 0) or 0),
        })
    return out


@app.get("/api/platform/profile")
def get_platform_profile(
    name: str,
    years: Optional[List[int]] = Query(None),
    agency: Optional[str] = None,
    domain: Optional[str] = None
):
    if not name:
        return {"found": False}

    search_upper = name.strip().upper()

    filters = {
        "vendor": None,
        "parent": None,
        "cage": None,
        "domain": domain,
        "agency": agency,
        "platform": search_upper,
        "psc": None
    }

    # ✅ 1. Let DuckDB handle the exact years filter instantly
    where_sql, params = build_summary_where(years, filters)

    # ✅ 2. Single optimized DuckDB query handles everything
    df = query_summary_df(
        where_sql=where_sql,
        params=params,
        select_sql="""
            platform_family,
            vendor_name,
            cage_code,
            sub_agency,
            SUM(total_spend) AS total_spend,
            SUM(contract_count) AS contract_count
        """,
        group_by_sql="platform_family, vendor_name, cage_code, sub_agency",
        order_by_sql="total_spend DESC",
        limit=0
    )

    if df.empty:
        return {
            "found": True, "name": name, "total_obligations": 0.0,
            "contractor_count": 0, "contract_count": 0, "top_vendors": [], "top_agencies": []
        }

    # ✅ 3. Native Pandas aggregations remain identical
    official_name = name
    try:
        official_name = df["platform_family"].mode()[0]
    except Exception:
        official_name = name

    total_obligations = float(pd.to_numeric(df["total_spend"], errors="coerce").fillna(0).sum())
    contract_count = int(pd.to_numeric(df["contract_count"], errors="coerce").fillna(0).sum())
    contractor_count = int(df["vendor_name"].nunique())

    top_vendors_df = (
        df.groupby(["cage_code", "vendor_name"], dropna=False)["total_spend"]
        .sum().reset_index().sort_values("total_spend", ascending=False).head(10)
    )
    top_vendors = [
        {"name": r["vendor_name"], "cage": r["cage_code"], "total": float(r["total_spend"])}
        for _, r in top_vendors_df.iterrows()
    ]

    top_agencies = (
        df.groupby("sub_agency", dropna=False)["total_spend"]
        .sum().sort_values(ascending=False).head(5).index.astype(str).tolist()
    )

    return {
        "found": True,
        "name": official_name,
        "total_obligations": total_obligations,
        "contractor_count": contractor_count,
        "contract_count": contract_count,
        "top_vendors": top_vendors,
        "top_agencies": top_agencies,
    }


@app.get("/api/platform/shared-exposure")
def get_platform_shared_exposure(
    name: str,
    years: Optional[List[int]] = Query(None),
):
    """Return non-additive DLA value for shared-use NIINs associated with a platform."""
    if not name:
        return {"platform": name, "shared_use_exposure": 0.0, "niin_count": 0}

    safe_plat = sanitize(name).upper()
    ys = safe_years(years, min_year=2019, max_year=2200, max_len=50)
    year_clause = ""
    params: List[Any] = [safe_plat]
    if ys:
        placeholders = ",".join(["?"] * len(ys))
        year_clause = f"AND TRY_CAST(s.year AS INTEGER) IN ({placeholders})"
        params.extend(ys)

    query = f"""
        WITH platform_niins AS (
            SELECT DISTINCT LPAD(TRIM(CAST(niin AS VARCHAR)), 9, '0') AS niin
            FROM v_platform_bom
            WHERE UPPER(TRIM(CAST(platform_family AS VARCHAR))) = ?
        )
        SELECT
            COALESCE(SUM(TRY_CAST(s.spend_amount AS DOUBLE)), 0) AS shared_use_exposure,
            COUNT(DISTINCT LPAD(TRIM(CAST(s.niin AS VARCHAR)), 9, '0')) AS niin_count
        FROM v_nsn_summary s
        INNER JOIN platform_niins p
            ON LPAD(TRIM(CAST(s.niin AS VARCHAR)), 9, '0') = p.niin
        WHERE TRY_CAST(s.platform_count AS INTEGER) > 1
          {year_clause}
    """

    try:
        df = duck_fetch_df(query, params)
    except Exception as exc:
        logger.error("Failed to fetch shared-use platform exposure: %s", exc)
        return {"platform": name, "shared_use_exposure": 0.0, "niin_count": 0}

    if df.empty:
        return {"platform": name, "shared_use_exposure": 0.0, "niin_count": 0}

    row = df.iloc[0]
    return {
        "platform": name,
        "shared_use_exposure": float(row.get("shared_use_exposure", 0) or 0),
        "niin_count": int(row.get("niin_count", 0) or 0),
        "additive": False,
        "basis": "SHARED_USE_NIIN_ASSOCIATION",
    }


@app.get("/api/platform/contractors")
def get_platform_contractors(
    name: str,
    limit: int = 100,
    offset: int = 0,
    years: Optional[List[int]] = Query(None),
    agency: Optional[str] = None
):
    if not name:
        return []

    limit = max(1, min(int(limit), 500))
    offset = max(0, int(offset))

    safe_plat = sanitize(name)

    # Same strict platform filter, strict sub_agency filter
    filters = {
        "vendor": None,
        "parent": None,
        "cage": None,
        "domain": None,
        "agency": agency,
        "platform": safe_plat,
        "psc": None
    }

    where_sql, params = build_summary_where(years, filters)

    df = query_summary_df(
        where_sql=where_sql,
        params=params,
        select_sql="""
            cage_code,
            vendor_name,
            SUM(total_spend) AS total_spend,
            SUM(contract_count) AS contract_count
        """,
        group_by_sql="cage_code, vendor_name",  # ✅ ADDED
        order_by_sql="total_spend DESC",
        limit=0
    )

    if df.empty:
        return []

    grouped = df.sort_values("total_spend", ascending=False, kind="mergesort")
    page = grouped.iloc[offset: offset + limit]

    return [
        {
            "name": r["vendor_name"],
            "cage": r["cage_code"],
            "total": float(r["total_spend"]),
            "contracts": int(r["contract_count"]),
            "role": "PRIME"
        }
        for _, r in page.iterrows()
    ]



@app.get("/api/platform/parts")
def get_platform_parts(
    name: str,
    include_zero: bool = True,
    limit: int = 100,
    offset: int = 0,
    min_spend: float = 0,
    years: Optional[List[int]] = Query(None),
    agency: Optional[str] = None
):
    safe_plat = sanitize(name).upper()

    ys = safe_years(years, min_year=2019, max_year=2200, max_len=50)
    year_clause = ""
    params: List[Any] = [safe_plat]
    if ys:
        placeholders = ",".join(["?"] * len(ys))
        year_clause = f"AND s.year IN ({placeholders})"
        params.extend(ys)

    # Financials stay at NIIN/CAGE/fiscal-year grain. Platform membership is
    # joined separately, so part-reference multiplicity cannot multiply value.
    query = f"""
        WITH bom_niins AS (
            SELECT DISTINCT LPAD(TRIM(CAST(niin AS VARCHAR)), 9, '0') AS niin
            FROM v_platform_bom
            WHERE UPPER(TRIM(CAST(platform_family AS VARCHAR))) = ?
        ),
        scoped AS (
            SELECT
                LPAD(TRIM(CAST(s.niin AS VARCHAR)), 9, '0') AS niin,
                TRY_CAST(s.year AS INTEGER) AS fiscal_year,
                UPPER(TRIM(CAST(s.cage AS VARCHAR))) AS cage,
                NULLIF(TRIM(CAST(s.vendor AS VARCHAR)), '') AS vendor,
                TRY_CAST(s.total_revenue AS DOUBLE) AS line_value,
                TRY_CAST(s.total_units_sold AS DOUBLE) AS line_units,
                TRY_CAST(s.last_sold AS DATE) AS last_sold,
                TRY_CAST(s.platform_count AS INTEGER) AS platform_count,
                CAST(s.platform_attribution_status AS VARCHAR) AS platform_attribution_status
            FROM v_nsn_supplier_lookup s
            INNER JOIN bom_niins b
                ON LPAD(TRIM(CAST(s.niin AS VARCHAR)), 9, '0') = b.niin
            WHERE 1=1 {year_clause}
        ),
        year_totals AS (
            SELECT
                niin,
                fiscal_year,
                SUM(COALESCE(line_value, 0)) AS year_value,
                SUM(COALESCE(line_units, 0)) AS year_units,
                MAX(last_sold) AS year_last_sold,
                MAX(platform_count) AS platform_count,
                MAX(platform_attribution_status) AS platform_attribution_status
            FROM scoped
            GROUP BY 1, 2
        ),
        niin_totals AS (
            SELECT
                niin,
                SUM(year_value) AS observed_value,
                SUM(year_units) AS observed_units,
                MAX(year_last_sold) AS last_sold_date,
                STRING_AGG(
                    CAST(fiscal_year AS VARCHAR) || ':' || CAST(year_value AS VARCHAR),
                    '|' ORDER BY fiscal_year
                ) AS annual_revenue_trend,
                MAX(platform_count) AS platform_count,
                MAX(platform_attribution_status) AS platform_attribution_status
            FROM year_totals
            GROUP BY 1
        ),
        vendor_totals AS (
            SELECT
                niin,
                cage,
                MAX(vendor) AS vendor,
                SUM(COALESCE(line_value, 0)) AS vendor_value
            FROM scoped
            GROUP BY 1, 2
        ),
        top_vendors AS (
            SELECT
                niin,
                MAX_BY(cage, vendor_value) AS top_vendor,
                MAX_BY(vendor, vendor_value) AS top_vendor_name
            FROM vendor_totals
            GROUP BY 1
        )
        SELECT
            b.niin,
            COALESCE(NULLIF(TRIM(CAST(p.nsn AS VARCHAR)), ''), b.niin) AS nsn,
            NULLIF(TRIM(CAST(p.item_name AS VARCHAR)), '') AS description,
            NULLIF(TRIM(CAST(p.fsc_code AS VARCHAR)), '') AS fsc_code,
            COALESCE(t.observed_value, 0) AS amount,
            COALESCE(t.observed_units, 0) AS total_units_sold,
            COALESCE(t.annual_revenue_trend, '') AS annual_revenue_trend,
            t.last_sold_date,
            v.top_vendor,
            v.top_vendor_name,
            COALESCE(t.platform_count, 0) AS platform_count,
            COALESCE(t.platform_attribution_status, 'UNMAPPED') AS platform_attribution_status
        FROM bom_niins b
        LEFT JOIN niin_totals t ON b.niin = t.niin
        LEFT JOIN top_vendors v ON b.niin = v.niin
        LEFT JOIN v_nsn_profile_lookup p
            ON b.niin = LPAD(TRIM(CAST(p.niin AS VARCHAR)), 9, '0')
        WHERE (? OR COALESCE(t.observed_value, 0) > 0)
          AND COALESCE(t.observed_value, 0) >= ?
        ORDER BY amount DESC, b.niin
        LIMIT ? OFFSET ?
    """
    params.extend([
        bool(include_zero),
        float(min_spend),
        max(1, min(int(limit), 5000)),
        max(0, int(offset)),
    ])

    try:
        df = duck_fetch_df(query, params)
    except Exception as e:
        logger.error(f"Failed to fetch platform parts from NIIN/CAGE fiscal sidecar: {e}")
        return []

    results = []
    for row in df.itertuples(index=False):
        platform_count = int(getattr(row, "platform_count", 0) or 0)
        results.append({
            "item_id": str(getattr(row, "nsn", "") or getattr(row, "niin", "")),
            "nsn": str(getattr(row, "nsn", "") or getattr(row, "niin", "")),
            "niin": str(getattr(row, "niin", "") or "").zfill(9),
            "description": getattr(row, "description", "") or "",
            "fsc_code": getattr(row, "fsc_code", "") or "",
            "total_units_sold": int(getattr(row, "total_units_sold", 0) or 0),
            "amount": float(getattr(row, "amount", 0) or 0),
            "annual_revenue_trend": getattr(row, "annual_revenue_trend", "") or "",
            "top_vendor": getattr(row, "top_vendor", "") or "",
            "top_vendor_name": getattr(row, "top_vendor_name", "") or "",
            "last_sold": getattr(row, "last_sold_date", None),
            "platform_count": platform_count,
            "platform_attribution_status": getattr(row, "platform_attribution_status", "UNMAPPED"),
            "shared_platform_exposure": platform_count > 1,
            "financial_grain": "NIIN_CAGE_FISCAL_YEAR",
        })

    return results


@app.get("/api/platform/parts/count")
def get_platform_parts_count(
    name: str,
    years: Optional[List[int]] = Query(None), # Added to prevent FastAPI 422 crash from UI filters
    agency: Optional[str] = None,
    domain: Optional[str] = None,
    platform: Optional[str] = None
):
    safe_plat = sanitize(name).upper()
    
    query = """
        SELECT COUNT(DISTINCT niin) as total
        FROM v_platform_bom
        WHERE platform_family = ?
    """
    
    try:
        df = duck_fetch_df(query, [safe_plat])
        if not df.empty:
            count_val = int(df.iloc[0]['total'])
            return {"count": count_val}
    except Exception as e:
        logger.error(f"Parts count query failed: {e}")
        
    return {"count": 0}

# ==========================================
#   ✅ PASTE THIS INTO api.py
# ==========================================

# ==========================================
#   ✅ REPLACE get_platform_awards IN api.py
# ==========================================

@app.get("/api/platform/awards")
def get_platform_awards(
    name: str,
    limit: int = 50,
    offset: int = 0,
    years: Optional[List[int]] = Query(None),
    agency: Optional[str] = None,
    threshold: Optional[float] = 0.5,
):
    safe_plat = sanitize(name)
    limit_i = max(1, min(int(limit), 500))
    offset_i = max(0, int(offset))

    where_parts = ["platform_family = ?"]
    params = [safe_plat]

    if agency:
        where_parts.append("(sub_agency = ? OR parent_agency = ?)")
        safe_ag = sanitize(agency)
        params.extend([safe_ag, safe_ag])

    if years:
        placeholders = ",".join(["?"] * len(years))
        where_parts.append(f"year IN ({placeholders})")
        params.extend(years)

    if threshold and threshold > 0:
        where_parts.append("spend_amount >= ?")
        params.append(threshold * 1_000_000)

    where_clause = " AND ".join(where_parts)

    query = f"""
        SELECT 
            contract_id,
            action_date,
            vendor_name,
            vendor_cage,
            COALESCE(sub_agency, parent_agency) AS agency,
            description,
            spend_amount AS spend,
            naics_code AS naics,
            platform_family
        FROM v_transactions
        WHERE {where_clause}
        ORDER BY action_date DESC
        LIMIT ? OFFSET ?
    """
    params.extend([limit_i, offset_i])

    try:
        df = duck_fetch_df(query, params)
        if df.empty: return []
        
        # Replace NaN with safe JSON nulls/empty strings
        df = df.fillna("")
        return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Awards query failed: {e}")
        return []


@app.get("/api/platform/subcontracts")
def get_platform_subcontracts(
    name: str,
    psc: Optional[str] = None,
    prime_vendor: Optional[str] = None,
    sub_vendor: Optional[str] = None,
    mode: str = "rollup", # "rollup" or "lines"
    limit: int = 50,
    offset: int = 0,
):
    safe_plat = sanitize(name)
    limit_i = max(1, min(int(limit), 500))
    offset_i = max(0, int(offset))

    path = LOCAL_CACHE_DIR / "network.parquet"
    if not path.exists():
        return []

    # Build WHERE clause dynamically
    where_parts = ["platform_family = ?"]
    params = [safe_plat]

    if psc:
        # ✅ FIX: The frontend sends "1620 - AIRCRAFT COMPONENTS", we extract just the 4-digit code to filter
        raw_psc = str(psc).split(" - ")[0].strip()
        where_parts.append("psc = ?")
        params.append(sanitize(raw_psc))
    if prime_vendor:
        where_parts.append("prime_name = ?")
        params.append(sanitize(prime_vendor))
    if sub_vendor:
        # ✅ FIX: Match against the exact site name
        where_parts.append("sub_name = ?")
        params.append(sanitize(sub_vendor))

    where_clause = " AND ".join(where_parts)

    global DUCK_CONN
    try:
        with DUCK_LOCK:
            ensure_duck_conn()
            
            if mode == "rollup":
                sql = f"""
                    WITH psc_mapping AS (
                        SELECT psc_code, MAX(psc_description) as psc_desc 
                        FROM v_summary 
                        WHERE psc_code IS NOT NULL 
                        GROUP BY psc_code
                    ),
                    grouped_network AS (
                        SELECT 
                            sub_name as sub_vendor,
                            sub_cage as sub_cage,
                            prime_name as prime_vendor,
                            prime_cage as prime_cage,
                            MAX(psc) as raw_psc,
                            array_to_string((array_agg(DISTINCT description))[1:3], ' | ') as description,
                            COUNT(*) as contract_count,
                            SUM(subaward_value) as subaward_value,
                            MAX(action_date) as action_date
                        FROM v_network
                        WHERE {where_clause}
                        GROUP BY sub_name, sub_cage, prime_name, prime_cage
                    )
                    SELECT 
                        g.sub_vendor, g.sub_cage, g.prime_vendor, g.prime_cage,
                        g.description, g.contract_count, g.subaward_value, g.action_date,
                        COALESCE(g.raw_psc, 'UNKNOWN') || ' - ' || COALESCE(m.psc_desc, 'Unclassified Component') as psc
                    FROM grouped_network g
                    LEFT JOIN psc_mapping m ON g.raw_psc = m.psc_code
                    ORDER BY g.subaward_value DESC
                    LIMIT ? OFFSET ?
                """
            else:
                sql = f"""
                    SELECT * FROM v_network 
                    WHERE {where_clause} 
                    ORDER BY subaward_value DESC 
                    LIMIT ? OFFSET ?
                """
                
            # Removed the str(path) from parameters since we aren't using read_parquet anymore
            all_params = tuple(params) + (limit_i, offset_i)
            df = DUCK_CONN.execute(sql, all_params).fetchdf()
            df = df_sanitize_for_json(df)
            
            return df.to_dict(orient="records")
            
    except Exception as e:
        logger.exception(f"DuckDB Subcontracts Query Failed")
        return []



from typing import Optional, List
from fastapi import Query

from typing import Optional, List
from fastapi import Query

@app.get("/api/company/opportunities")
def get_company_opportunities(
    cage: Optional[str] = None, 
    name: Optional[str] = None,
    # ✅ Global Filter Parameters
    agency: Optional[str] = None,
    domain: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None,
    years: Optional[List[int]] = Query(None) # Added to prevent FastAPI 422 crash
):
    target_naics = set()
    profiles_df = GLOBAL_CACHE.get("profiles_df", pd.DataFrame())
    
    if not profiles_df.empty:
        if cage and cage != "AGGREGATE":
            matches = profiles_df[profiles_df['cage_code'] == cage]
            for _, row in matches.iterrows():
                if row.get('top_naics_codes'):
                    codes = str(row['top_naics_codes']).split(',')
                    target_naics.update([c.strip() for c in codes])

        if name:
            clean_name = sanitize(name)
            matches = profiles_df[profiles_df['vendor_name'].str.contains(clean_name, case=False, na=False)]
            for _, row in matches.iterrows():
                if row.get('top_naics_codes'):
                    codes = str(row['top_naics_codes']).split(',')
                    target_naics.update([c.strip() for c in codes])

    clean_naics_list = []
    for n in target_naics:
        s = str(n).split('.')[0].split(' - ')[0].strip()
        if len(s) >= 3 and s.isdigit(): 
            clean_naics_list.append(s)

    if not clean_naics_list:
        return []

    # 3. DuckDB SQL Query construction
    naics_conditions = []
    params = []
    for n in set(clean_naics_list):
        naics_conditions.append("CAST(naics AS VARCHAR) LIKE ?")
        params.append(f"{n}%")
        
    where_parts = [f"({' OR '.join(naics_conditions)})"]
    
    # ✅ Apply Agency & PSC natively
    if agency:
        where_parts.append("(upper(agency) LIKE ? OR upper(sub_agency) LIKE ?)")
        safe_ag = f"%{sanitize(agency).upper()}%"
        params.extend([safe_ag, safe_ag])
        
    if psc:
        where_parts.append("upper(psc) = ?")
        params.append(sanitize(psc).upper())

    # ✅ THE TRICK: Apply Platform & Domain heuristically via text search
    if platform:
        where_parts.append("(upper(title) LIKE ? OR upper(description) LIKE ?)")
        safe_plat = f"%{sanitize(platform).upper()}%"
        params.extend([safe_plat, safe_plat])
        
    if domain:
        safe_domain = sanitize(domain).upper()
        # If domain looks like a code (e.g. "334" or "R425"), search naics/psc
        if len(safe_domain) > 0 and (safe_domain[0].isdigit() or (len(safe_domain) == 4 and safe_domain[0].isalpha())):
            where_parts.append("(psc LIKE ? OR naics LIKE ?)")
            params.extend([f"{safe_domain}%", f"{safe_domain}%"])
        else:
            # Otherwise, text search the title and description
            where_parts.append("(upper(title) LIKE ? OR upper(description) LIKE ?)")
            safe_domain_like = f"%{safe_domain}%"
            params.extend([safe_domain_like, safe_domain_like])

    where_clause = " AND ".join(where_parts)
    
    query = f"""
        SELECT id, title, sol_num, agency, deadline, set_aside_type, poc_email
        FROM v_opportunities
        WHERE {where_clause}
          AND try_cast(deadline as date) >= current_date()
        ORDER BY try_cast(deadline as date) ASC
        LIMIT 50
    """
    
    try:
        df = duck_fetch_df(query, params)
        if df.empty: return []
        
        # Sanitize NaNs
        df = df_sanitize_for_json(df)
        
        results = []
        for row in df.itertuples(index=False):
            sol_val = getattr(row, 'sol_num', '')
            if pd.isna(sol_val) or not str(sol_val).strip():
                 sol_val = getattr(row, 'id', '')
                 
            results.append({
                "noticeid": getattr(row, 'id', ''),
                "title": getattr(row, 'title', ''),
                "sol_num": sol_val, 
                "sol#": sol_val,    
                "department_indagency": getattr(row, 'agency', ''),
                "responsedeadline": getattr(row, 'deadline', ''),
                "setaside": getattr(row, 'set_aside_type', ''),
                "primarycontactemail": getattr(row, 'poc_email', '')
            })
        return results
    except Exception as e:
        logger.error(f"Opportunities DuckDB Error: {e}")
        return []


from pydantic import BaseModel
import os
from openai import AsyncOpenAI

# Initialize OpenAI client
aclient = AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

class UnlockRequest(BaseModel):
    name: str
    cage: str
    prime_exposure: float
    sub_exposure: float
    top_platforms: list
    top_capabilities: list
    top_nsns: list

import pandas as pd

@app.post("/api/public/unlock-brief")
async def generate_unlocked_brief(request: Request):
    try:
        data = await request.json()
        safe_name = data.get("name")
        safe_cage = data.get("cage")
        is_parent = data.get("is_parent", False)
        
        filters = {"cage": safe_cage} if not is_parent else {"parent": safe_name}
        where_sql, params = build_summary_where(years=None, filters=filters)

        # 1. Top Platforms
        plats_df = query_summary_df(
            where_sql, params, select_sql="platform_family, sum(total_spend) as spend",
            group_by_sql="platform_family", order_by_sql="spend DESC", limit=10
        )
        deep_platforms = plats_df.dropna(subset=['platform_family']).to_dict(orient="records") if not plats_df.empty else []

        # 2. Top Funding Agencies
        agency_df = query_summary_df(
            where_sql, params, select_sql="sub_agency, sum(total_spend) as spend",
            group_by_sql="sub_agency", order_by_sql="spend DESC", limit=5
        )
        deep_agencies = []
        if not agency_df.empty:
            for _, row in agency_df.dropna(subset=['sub_agency']).iterrows():
                deep_agencies.append({"name": str(row['sub_agency']).title(), "spend": float(row['spend'])})

        # 3. Top Capabilities (NAICS by Revenue)
        cap_df = query_summary_df(
            where_sql, params, select_sql="naics_description, sum(total_spend) as spend",
            group_by_sql="naics_description", order_by_sql="spend DESC", limit=3
        )
        deep_capabilities = []
        if not cap_df.empty:
            for _, row in cap_df.dropna(subset=['naics_description']).iterrows():
                deep_capabilities.append({"name": str(row['naics_description']).title(), "spend": float(row['spend'])})

        # 4. Top NIINs. Financial values and shares use the same completed
        # five-fiscal-year supplier-sidecar window as the company dashboard.
        deep_nsns = []
        latest_completed_fy = datetime.utcnow().year - 1
        nsn_years = list(range(latest_completed_fy - 4, latest_completed_fy + 1))
        nsn_period_label = f"FY{nsn_years[0]}–FY{nsn_years[-1]}"

        try:
            nsn_rows = [] if is_parent or not safe_cage else get_company_parts(
                cage=safe_cage,
                limit=10,
                offset=0,
                years=nsn_years,
                rollup="nsn",
                domain=None,
                agency=None,
                platform=None,
                psc=None,
            )

            for row in nsn_rows:
                    raw_desc = str(row.get("description") or "Unknown Part").strip()
                    if "!" in raw_desc:
                        raw_desc = raw_desc.split("!", 1)[-1]
                    deep_nsns.append({
                        "nsn": str(row.get("niin") or row.get("nsn") or ""),
                        "desc": raw_desc.title().strip(), 
                        "spend": float(row.get("amount") or 0),
                        "unit_price": float(row.get("avg_unit_price") or 0),
                        "platform": str(row.get("platform_family") or "Unspecified"),
                        "market_share": round(float(row.get("direct_sales_market_share_pct") or 0), 1) or None,
                        "period_label": nsn_period_label,
                    })
        except Exception as e:
            print(f"Company NIIN sidecar fetch error: {e}")

        # 5. Top Contracts (Filtered >= $250k)
        txn_where = "(vendor_cage = ?) AND spend_amount >= 250000" if not is_parent else "(upper(vendor_name) LIKE ?) AND spend_amount >= 250000"
        txn_params = [safe_cage] if not is_parent else [f"%{safe_name.upper()}%"]
        
        contracts_df = get_subset_from_disk(
            "transactions.parquet",
            where_clause=txn_where, params=tuple(txn_params),
            columns_sql="action_date, sub_agency, description, spend_amount", 
            order_by_sql="spend_amount DESC, action_date DESC", limit=10
        )
        deep_contracts = []
        if not contracts_df.empty:
            for _, row in contracts_df.iterrows():
                raw_desc = str(row['description']).strip()
                if '!' in raw_desc: raw_desc = raw_desc.split('!', 1)[-1]
                deep_contracts.append({
                    "date": str(row['action_date']).split(' ')[0],
                    "agency": str(row.get('sub_agency', 'DoD')).title(),
                    "desc": raw_desc.strip(),
                    "spend": float(row['spend_amount'])
                })

        # 6. Network (Primes vs Subs)
        net_data = get_company_network(name=safe_name, cage=safe_cage if not is_parent else None, years=None, limit=10)
        primes_list = net_data.get("primes", []) if isinstance(net_data, dict) else []
        subs_list = net_data.get("subs", []) if isinstance(net_data, dict) else []
        
        prime_spend = float(data.get("prime_exposure", 0))
        sub_spend = sum(float(p.get("total", 0) or 0) for p in primes_list)
        total_mapped = prime_spend + sub_spend
        is_primarily_prime = prime_spend >= sub_spend
        
        deep_network = subs_list if is_primarily_prime and subs_list else primes_list
        network_title = "Top Subcontractors" if is_primarily_prime and subs_list else "Top Prime Customers"

        # 7. FORMAT NUMBERS & SHARES FOR LLM
        def fmt_m(val): return f"${val/1_000_000:.1f}M"
        def fmt_share(spend): 
            pct = (spend / total_mapped * 100) if total_mapped > 0 else 0
            return f" ({min(pct, 100):.0f}%)" # Caps at 100% to prevent multi-platform overlap bugs

        prime_pct = (prime_spend / total_mapped * 100) if total_mapped > 0 else 0
        sub_pct = (sub_spend / total_mapped * 100) if total_mapped > 0 else 0

        formatted_agencies = [f"{a['name']}: {fmt_m(a['spend'])}{fmt_share(a['spend'])}" for a in deep_agencies]
        formatted_platforms = [f"{p['platform_family']}: {fmt_m(p['spend'])}{fmt_share(p['spend'])}" for p in deep_platforms[:5]]
        formatted_caps = [f"{c['name']}: {fmt_m(c['spend'])}{fmt_share(c['spend'])}" for c in deep_capabilities]

        # 8. GENERATE HEADLINE METRIC (FIXED: Smarter DLA takeaway)
        headline_metric = "Diversified defense footprint across multiple agencies and programs."
        if total_mapped > 0 and deep_agencies:
            top_agency = deep_agencies[0]
            share_pct = (top_agency['spend'] / total_mapped) * 100
            
            if share_pct >= 50:
                if "Logistics Agency" in top_agency['name']:
                    headline_metric = f"Sustainment Focus: Defense Logistics Agency accounts for {share_pct:.0f}% of mapped exposure, indicating a strong aftermarket moat."
                elif "Nav" in top_agency['name'] or "Air Force" in top_agency['name'] or "Army" in top_agency['name']:
                    headline_metric = f"OEM / Program Focus: {top_agency['name']} drives {share_pct:.0f}% of federal exposure."
                else:
                    headline_metric = f"Highly Concentrated: {top_agency['name']} accounts for {share_pct:.0f}% of federal exposure."
            elif len(deep_agencies) >= 2:
                top_2_spend = deep_agencies[0]['spend'] + deep_agencies[1]['spend']
                headline_metric = f"Top 2 customers account for {(top_2_spend/total_mapped)*100:.0f}% of observed federal exposure."

        # 9. STRUCTURED A&D ANALYST PROMPT
        system_prompt = """
        You are an elite Aerospace & Defense (A&D) investment banking analyst writing a concise, hard-hitting intelligence brief on a defense contractor.
        
        Summarize ONLY what is supported by the mapped federal financial data provided. Do not speculate on their commercial business or total global revenue.
        
        MANDATORY FORMAT & RULES:
        You MUST structure your response exactly with these three bolded headings. No intro/outro text. Write 1-3 highly analytical, professional sentences per section.
        
        **Position:** - Open EXACTLY with: "Between FY18 and Present, [Company] generated [Total] in identified DoD and federal contract revenue, split [Prime%] prime and [Sub%] subcontract."
        - State if their federal footprint operates primarily as a Prime, Tier 1, or lower-tier supplier based on the prime/sub mix.
        - DO NOT use words like "trust", "robust", "vulnerable", or "primary supplier". Use standard A&D terminology: "embedded", "Tier 1/2", "prime-oriented".
        
        **Dependency:** - Detail what drives their federal revenue using the provided Agencies, Platforms, and Capabilities (NAICS/PSCs).
        - NEVER say "a significant portion of their business" (we do not know their commercial revenue). Use "a significant portion of their federal profile".
        - If Defense Logistics Agency (DLA) is dominant, explicitly state they are embedded in the sustainment, aftermarket spares, and readiness lifecycle of legacy platforms. 
        
        **Implication:** - Provide the strategic A&D takeaway. 
        - If they are heavy in DLA/Sustainment on specific platforms, state that this creates high switching costs, recurring revenue streams, and an embedded sole-source positioning on mature/legacy fleets. 
        - DO NOT call reliance on DLA or DoD a "vulnerability" or suggest they need to "diversify". In defense, single-platform or single-agency embedding constitutes a protective moat and high barriers to entry, not a weakness.
        """

        user_message = f"""
        Entity: {safe_name} (CAGE: {safe_cage})
        
        DATABASE SIGNALS (FY18 - Present):
        - Total Mapped Revenue: {fmt_m(total_mapped)} ({prime_pct:.0f}% Prime / {sub_pct:.0f}% Sub)
        - Agency Mix: {', '.join(formatted_agencies) if formatted_agencies else 'None mapped'}
        - Top Platforms: {', '.join(formatted_platforms) if formatted_platforms else 'None mapped'}
        - Core Federal Capabilities: {', '.join(formatted_caps) if formatted_caps else 'None mapped'}
        """

        completion = await aclient.chat.completions.create(
            model="gpt-4o", 
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_message}],
            max_tokens=400, 
            temperature=0.2, # Keeps it highly grounded while allowing for the specific A&D terminology
        )

        return {
            "success": True, 
            "ai_brief": completion.choices[0].message.content,
            "headline_metric": headline_metric, 
            "deep_data": {
                "platforms": deep_platforms,
                "agencies": deep_agencies,
                "nsns": deep_nsns,
                "nsn_period_label": nsn_period_label,
                "contracts": deep_contracts,
                "network": deep_network,
                "network_title": network_title
            }
        }

    except Exception as e:
        return {"success": False, "error": str(e)}


# ==========================================
#        PUBLIC TEASER & RATE LIMITING
# ==========================================

from fastapi import Request, HTTPException, Depends, Response
import time

RATE_LIMIT_CACHE = {}
NSN_RATE_LIMIT_CACHE = {}
MAX_REQUESTS_PER_IP = 3
RATE_LIMIT_WINDOW_SECONDS = 86400 # 24 hours

def check_rate_limit(request: Request):
    # ✅ VIP BYPASS: If the frontend sends an auth token, skip the rate limit entirely!
    if "Authorization" in request.headers:
        return None 

    client_ip = request.client.host
    now = time.time()
    
    # Cleanup expired IPs
    for ip in list(RATE_LIMIT_CACHE.keys()):
        if now - RATE_LIMIT_CACHE[ip]['timestamp'] > RATE_LIMIT_WINDOW_SECONDS:
            del RATE_LIMIT_CACHE[ip]
            
    # Check limit for unauthenticated users
    if client_ip in RATE_LIMIT_CACHE:
        if RATE_LIMIT_CACHE[client_ip]['count'] >= MAX_REQUESTS_PER_IP:
             # ✅ FIXED: Updated to the premium up-sell message
             raise HTTPException(
                 status_code=429, 
                 detail="Daily free search limit reached. Upgrade your Mimir account for unlimited deep-dive company intelligence."
             )
        RATE_LIMIT_CACHE[client_ip]['count'] += 1
    else:
        RATE_LIMIT_CACHE[client_ip] = {'count': 1, 'timestamp': now}
        
    # Return the remaining count so the frontend can display it
    remaining = MAX_REQUESTS_PER_IP - RATE_LIMIT_CACHE[client_ip]['count']
    return max(0, remaining)


def check_nsn_rate_limit(request: Request):
    """Keep NSN lookups separate from the company lookup allowance."""
    client_ip = request.client.host if request.client else "unknown"
    now = time.time()

    for ip in list(NSN_RATE_LIMIT_CACHE.keys()):
        if now - NSN_RATE_LIMIT_CACHE[ip]["timestamp"] > RATE_LIMIT_WINDOW_SECONDS:
            del NSN_RATE_LIMIT_CACHE[ip]

    if client_ip in NSN_RATE_LIMIT_CACHE:
        if NSN_RATE_LIMIT_CACHE[client_ip]["count"] >= MAX_REQUESTS_PER_IP:
            raise HTTPException(
                status_code=429,
                detail="Daily free NSN lookup limit reached. Sign in to continue in the full Mimir platform.",
            )
        NSN_RATE_LIMIT_CACHE[client_ip]["count"] += 1
    else:
        NSN_RATE_LIMIT_CACHE[client_ip] = {"count": 1, "timestamp": now}

    return max(0, MAX_REQUESTS_PER_IP - NSN_RATE_LIMIT_CACHE[client_ip]["count"])

@app.get("/api/public/company/teaser")
def get_public_company_teaser(
    request: Request,
    response: Response, # Added Response to inject headers
    cage: Optional[str] = None,
    name: Optional[str] = None,
    remaining_lookups: int = Depends(check_rate_limit) # Capture the remaining count
):
    """
    Public-facing endpoint for high-level company metrics.
    Rate-limited by IP address. Bypasses granular filters.
    """
    # Expose the remaining lookups to the frontend via a custom header
    response.headers["X-RateLimit-Remaining"] = str(remaining_lookups)
    
    full_profile = get_company_profile(cage=cage, name=name, years=None)
    if not full_profile or not full_profile.get("found"):
        return {"found": False, "message": "Entity not found in the defense intelligence database."}

    safe_name = full_profile.get("name")
    safe_cage = full_profile.get("cage")
    is_parent = safe_cage == "AGGREGATE"

    # Prime Spend
    prime_spend = float(full_profile.get("total_obligations", 0))

    # Sub Spend & Dynamic Network
    net_data = get_company_network(name=safe_name, cage=safe_cage if not is_parent else None, years=None, limit=100)
    primes_list = net_data.get("primes", []) if isinstance(net_data, dict) else []
    subs_list = net_data.get("subs", []) if isinstance(net_data, dict) else []
    
    sub_spend = sum(float(p.get("total", 0) or 0) for p in primes_list)
    
    # Smart Toggle for Prime vs Sub
    is_primarily_prime = prime_spend >= sub_spend
    
    if is_primarily_prime and len(subs_list) > 0:
        network_type = "Key Supply Chain Partners"
        target_network = subs_list
    elif len(primes_list) > 0:
        network_type = "Key Customers"
        target_network = primes_list
    elif len(subs_list) > 0:
        network_type = "Key Supply Chain Partners"
        target_network = subs_list
    else:
        network_type = "Network Partners"
        target_network = []

    target_total = sum(float(x.get("total", 0) or 0) for x in target_network)
    network_partners = []
    top_customer_dependency = 0
    
    if target_total > 0:
        for i, p in enumerate(target_network[:3]):
            pct = int(round((float(p.get("total", 0) or 0) / target_total) * 100))
            if pct > 0:
                network_partners.append({"name": p.get("name"), "share": pct})
                if i == 0: top_customer_dependency = pct
                
    network_hidden = max(0, len(target_network) - 3)

    # Platform, Capabilities, and NSNs via DuckDB
    filters = {"cage": safe_cage} if not is_parent else {"parent": safe_name}
    where_sql, params = build_summary_where(years=None, filters=filters)
    
    top_platforms = []
    plats_hidden = 0
    top_plat_name = None
    top_plat_dependency = 0
    top_capabilities = []
    caps_hidden = 0
    top_nsns = []
    nsns_hidden = 0

    try:
        # A. Fetch Platforms
        plats_df = query_summary_df(
            where_sql, params, select_sql="platform_family, sum(total_spend) as spend",
            group_by_sql="platform_family", order_by_sql="spend DESC", limit=50
        )
        if not plats_df.empty and "platform_family" in plats_df.columns:
            valid_plats = plats_df.dropna(subset=['platform_family'])
            valid_plats = valid_plats[valid_plats['platform_family'].astype(str).str.strip() != ""]
            plat_total = valid_plats['spend'].sum()
            
            if plat_total > 0:
                top_plat_name = valid_plats.iloc[0]['platform_family']
                for i, row in valid_plats.head(3).iterrows():
                    pct = int(round((row['spend'] / plat_total) * 100))
                    if pct > 0: 
                        top_platforms.append({"name": row['platform_family'], "share": pct})
                        if i == 0: top_plat_dependency = pct
            plats_hidden = max(0, len(valid_plats) - 3)

        # B. Fetch Top Capabilities (By PSC)
        caps_df = query_summary_df(
            where_sql, params, select_sql="psc_description, sum(total_spend) as spend",
            group_by_sql="psc_description", order_by_sql="spend DESC", limit=15
        )
        if not caps_df.empty and "psc_description" in caps_df.columns:
            valid_caps = caps_df.dropna(subset=['psc_description'])
            valid_caps = valid_caps[valid_caps['psc_description'].astype(str).str.strip() != ""]
            for _, row in valid_caps.head(3).iterrows():
                top_capabilities.append(str(row['psc_description']).title())
            caps_hidden = max(0, len(valid_caps) - 3)
            
        # C. Fetch Top NSNs (Components)
        txn_nsn_where = "vendor_cage = ? AND nsn IS NOT NULL AND nsn != ''" if not is_parent else "upper(vendor_name) LIKE ? AND nsn IS NOT NULL AND nsn != ''"
        txn_nsn_params = [safe_cage] if not is_parent else [f"%{safe_name.upper()}%"]
        nsn_df = duck_fetch_df(f"""
            SELECT nsn, ANY_VALUE(description) as description, sum(spend_amount) as spend
            FROM v_transactions
            WHERE {txn_nsn_where}
            GROUP BY nsn
            ORDER BY spend DESC
            LIMIT 15
        """, params=txn_nsn_params)
        
        if not nsn_df.empty:
            for _, row in nsn_df.head(3).iterrows():
                raw_desc = str(row['description']).strip()
                if '!' in raw_desc:
                    raw_desc = raw_desc.split('!', 1)[-1]
                
                desc = raw_desc.title().strip()
                if desc.lower() in ["nan", "none", ""]: desc = "Unspecified Component"
                top_nsns.append({"nsn": str(row['nsn']).strip(), "desc": desc})
            nsns_hidden = max(0, len(nsn_df) - 3)

    except Exception as e:
        logger.error(f"Teaser Aggregate Query Error: {e}")

    # Fetch a Single Top Contract for the "Aha" moment
    top_contract_desc = None
    try:
        txn_where, txn_params = ("vendor_cage = ?", [safe_cage]) if not is_parent else ("upper(vendor_name) LIKE ?", [f"%{safe_name.upper()}%"])
        txn_df = get_subset_from_disk(
            "transactions.parquet",
            where_clause=txn_where, params=tuple(txn_params),
            columns_sql="description, spend_amount", order_by_sql="spend_amount DESC", limit=1
        )
        if not txn_df.empty:
            raw_desc = str(txn_df.iloc[0]['description']).strip()
            if '!' in raw_desc:
                raw_desc = raw_desc.split('!', 1)[-1]
                
            desc = raw_desc.strip()
            if desc and len(desc) > 3 and desc.upper() != "NAN":
                top_contract_desc = desc
    except Exception:
        pass

    # ==========================================
    # AGGRESSIVE STRATEGIC SIGNAL GENERATION
    # ==========================================
    total_exposure = prime_spend + sub_spend
    
    # Calculate ratios to avoid highlighting 60% of a 1% bucket
    sub_ratio = (sub_spend / total_exposure) if total_exposure > 0 else 0
    prime_ratio = (prime_spend / total_exposure) if total_exposure > 0 else 0

    insight_parts = []
    
    # 1. Macro Positioning
    if prime_ratio >= 0.90:
        insight_parts.append("Functions almost exclusively as a direct-to-DoD prime contractor.")
    elif sub_ratio >= 0.90:
        insight_parts.append("Functions almost exclusively as a sub-tier supplier within the industrial base.")
    else:
        role_str = "prime contractor" if is_primarily_prime else "sub-tier supplier"
        insight_parts.append(f"Operates as a hybrid {role_str} with balanced direct and indirect exposure.")

    # 2. Platform Dependency
    if top_platforms and top_plat_dependency >= 50:
        insight_parts.append(f"High structural dependency on the {top_plat_name} program (~{top_plat_dependency}% of mapped platform revenue).")
    elif top_platforms:
        insight_parts.append(f"Portfolio is diversified across multiple defense programs, led by {top_plat_name}.")

    # 3. Network Dependency (Only flag if the network bucket is a meaningful part of their overall business)
    if network_partners and top_customer_dependency >= 40:
        if not is_primarily_prime:
            # They are a Sub. Prime_list is their upstream.
            insight_parts.append(f"Upstream revenue is heavily concentrated, with ~{top_customer_dependency}% tied directly to {network_partners[0]['name'].title()}.")
        elif is_primarily_prime and sub_ratio > 0.15:
            # They are a Prime, AND sub-contracting is a material part of their business (>15%)
            insight_parts.append(f"Downstream execution relies heavily on {network_partners[0]['name'].title()} (~{top_customer_dependency}% of tracked subcontract flow).")

    if not insight_parts:
        insight_parts.append("Balanced strategic positioning across multiple platforms and prime integrators.")

    insight = " ".join(insight_parts)

    return {
        "found": True,
        "name": safe_name,
        "cage": safe_cage,
        "is_parent": is_parent,
        "location": f"{full_profile.get('city', '')}, {full_profile.get('state', '')}".strip(', '),
        "time_period": "FY18–Present",
        "prime_exposure": prime_spend,
        "sub_exposure": sub_spend,
        "total_exposure": total_exposure,
        "top_capabilities": top_capabilities,
        "capabilities_hidden": caps_hidden,
        "top_platforms": top_platforms,
        "platforms_hidden": plats_hidden,
        "network_type": network_type,
        "network_partners": network_partners,
        "network_hidden": network_hidden,
        "top_nsns": top_nsns,
        "nsns_hidden": nsns_hidden,
        "insight": insight,
        "top_contract_desc": top_contract_desc,
        "remaining_lookups": remaining_lookups 
    }
# ==========================================
#        COMPANY INTELLIGENCE
# ==========================================

# [Find and replace get_company_profile in api.py]

@app.get("/api/company/profile")
def get_company_profile(
    cage: Optional[str] = None,
    name: Optional[str] = None,
    years: Optional[List[int]] = Query(None)  # ✅ ADD THIS
):
    profiles_df = GLOBAL_CACHE.get("profiles_df", pd.DataFrame())
    loc_map = GLOBAL_CACHE.get("location_map", {}) or {}
    profiles_ready = (
        not profiles_df.empty
        and {"cage_code", "vendor_name"}.issubset(set(profiles_df.columns))
    )
    
    # 1. SPECIFIC CAGE (Drill-down) -> CHILD LOGIC
    if cage:
        clean_cage = cage.strip().upper()
        match = (
            profiles_df[profiles_df['cage_code'] == clean_cage]
            if profiles_ready
            else pd.DataFrame()
        )

        if not match.empty:
            row = match.iloc[0]
            loc = loc_map.get(clean_cage, {}) or {}

            k = _calc_child_kpis_from_kpis_disk(clean_cage, years)
            over = {}
            if k.get("has_kpis"):
                over = {
                    "total_obligations": k.get("total_obligations", 0.0),
                    "total_contracts": k.get("total_contracts", 0),
                    "last_active": k.get("last_active", 0),
                }

            return format_profile_response_with_loc(
                row,
                loc.get('city'),
                loc.get('state'),
                type="CHILD",
                overrides=over
            )

        # Exact-CAGE reference fallback. This supplies identity and location
        # without implying observed prime or subcontract financial activity.
        try:
            reference_match = duck_fetch_df(
                """
                SELECT
                    cage_code,
                    vendor_name,
                    city,
                    state,
                    location_quality,
                    entity_source
                FROM v_cage_locations
                WHERE UPPER(TRIM(CAST(cage_code AS VARCHAR))) = ?
                LIMIT 1
                """,
                [clean_cage],
            )
        except Exception:
            reference_match = pd.DataFrame()

        if not reference_match.empty:
            reference_row = reference_match.iloc[0]
            return {
                "found": True,
                "type": "CHILD",
                "profile_source": "CAGE_REFERENCE_ONLY",
                "name": reference_row.get("vendor_name") or f"CAGE {clean_cage}",
                "cage": clean_cage,
                "total_obligations": 0.0,
                "total_contracts": 0,
                "last_active": 0,
                "top_naics": [],
                "top_platforms": [],
                "network_flow_total": 0.0,
                "network_contract_count": 0,
                "network_last_active_year": 0,
                "city": reference_row.get("city") or "",
                "state": reference_row.get("state") or "",
                "location_quality": reference_row.get("location_quality"),
                "entity_source": reference_row.get("entity_source"),
            }

    # 2. NAME MATCH
    if name:
        clean_name = name.strip().upper().replace("'", "")

        # A. PARENT LOGIC (Aggregate)
        stats = get_parent_aggregate_stats(name, years)
        if stats:
            return {
                "found": True,
                "type": "PARENT", 
                "profile_source": "AWARD_BACKED",
                "name": name.upper(),
                "cage": "AGGREGATE",
                "total_obligations": stats["total_obligations"],
                "total_contracts": stats["total_contracts"],
                "last_active": stats["last_active"],
                "top_naics": stats["top_naics"],
                "top_platforms": stats["top_platforms"],
                "network_flow_total": 0.0,
                "network_contract_count": 0,
                "network_last_active_year": 0,
                "city": "", 
                "state": ""
            }

        # B. CHILD LOGIC (Specific Entity found by Name)
        child_match = (
            profiles_df[profiles_df['vendor_name'] == clean_name]
            if profiles_ready
            else pd.DataFrame()
        )
        if not child_match.empty:
            row = child_match.iloc[0]
            c_code = str(row.get('cage_code') or "").strip().upper()
            loc = loc_map.get(c_code, {}) or {}

            k = _calc_child_kpis_from_kpis_disk(c_code, years)
            over = {}
            if k.get("has_kpis"):
                over = {
                    "total_obligations": k.get("total_obligations", 0.0),
                    "total_contracts": k.get("total_contracts", 0),
                    "last_active": k.get("last_active", 0),
                }

            return format_profile_response_with_loc(
                row,
                loc.get('city'),
                loc.get('state'),
                type="CHILD",
                overrides=over
            )

    return {"found": False}

# ✅ Helper function (Updated to use Master NAICS List)
def format_profile_response_with_loc(row, city, state, type="CHILD", overrides: Optional[Dict[str, Any]] = None):
    raw_codes = row.get('top_naics_codes', '').split(',') if row.get('top_naics_codes') else []
    hydrated_naics = []
    naics_map = GLOBAL_CACHE.get("naics_map", {})

    for code in raw_codes:
        clean_c = code.lower().replace('unknown', '').strip()
        match = re.match(r'^(\d+)', clean_c)
        if not match:
            continue
        c = match.group(1)

        desc = naics_map.get(c)
        if not desc and len(c) > 2:
            desc = naics_map.get(c[:5])
        if not desc and len(c) > 2:
            desc = naics_map.get(c[:4])

        hydrated_naics.append(f"{c} - {desc}" if desc else c)

    overrides = overrides or {}

    return {
        "found": True,
        "type": type,
        "name": row.get('vendor_name'),
        "cage": row.get('cage_code'),

        # ✅ NEW: prefer overrides (disk KPIs) but fallback to profiles.parquet columns
        "total_obligations": float(overrides.get("total_obligations", row.get("total_lifetime_spend", 0) or 0)),
        "total_contracts": int(overrides.get("total_contracts", row.get("total_contracts", 0) or 0)),
        "last_active": int(overrides.get("last_active", row.get("last_active_year", 0) or 0)),

        "top_naics": hydrated_naics,
        "top_platforms": row.get('top_platforms', '').split(',') if row.get('top_platforms') else [],
        "profile_source": row.get("profile_source", "AWARD_BACKED"),
        "network_flow_total": float(row.get("network_flow_total", 0) or 0),
        "network_included_report_count": int(row.get("network_included_report_count", 0) or 0),
        "network_excluded_report_count": int(row.get("network_excluded_report_count", 0) or 0),
        "network_source_report_count": int(row.get("network_source_report_count", 0) or 0),
        "network_contract_count": int(row.get("network_contract_count", 0) or 0),
        "network_last_active_year": int(row.get("network_last_active_year", 0) or 0),
        "city": str(city) if city else "",
        "state": str(state) if state else ""
    }


# --- REPLACE IN api.py ---

# --- REPLACE get_company_network IN api.py ---

from typing import Optional, List
from fastapi import Query

@app.get("/api/company/network")
def get_company_network(
    name: str,
    cage: Optional[str] = None,
    years: Optional[List[int]] = Query(default=None),
    limit: int = 50
):
    """
    Returns top upstream (primes) and downstream (subs) network relationships for a company.

    Supports optional FY filtering via `years` (e.g. years=2018&years=2019...).
    Requires `network.parquet` to include a `year` INT column (best added in Athena and materialized in ETL).
    """

    safe_name = sanitize(name).replace("'", "").upper().strip()
    is_drill_down = (cage and len(cage) > 2 and cage != "AGGREGATE")

    def run_duck_query(sql: str, params: tuple):
        try:
            with DUCK_LOCK:
                conn = ensure_duck_conn()

                net_path = (LOCAL_CACHE_DIR / "network.parquet").resolve()
                if not net_path.exists():
                    return []

                final_sql = sql.replace(
                    "FROM network_source",
                    f"FROM read_parquet('{str(net_path)}')"
                )
                return conn.execute(final_sql, params).fetchdf().to_dict(orient="records")
        except Exception as e:
            logger.error(f"Network query failed: {e}")
            return []

    # --- Year filter builder (FY) ---
# --- Year filter builder (FY) ---
    year_filter_sql = ""
    year_params: List[int] = []

    # Normalize incoming years once
    yrs: List[int] = [int(y) for y in (years or []) if y is not None]

    # Detect which FY column exists in network.parquet: prefer fiscal_year, fallback to year
    year_col: Optional[str] = None
    network_columns_lower: set[str] = set()
    try:
        with DUCK_LOCK:
            conn = ensure_duck_conn()
            net_path = (LOCAL_CACHE_DIR / "network.parquet").resolve()
            if net_path.exists():
                cols = conn.execute(
                    f"SELECT * FROM read_parquet('{str(net_path)}') LIMIT 0"
                ).df().columns
                network_columns_lower = {str(column).strip().lower() for column in cols}
                if "fiscal_year" in cols:
                    year_col = "fiscal_year"
                elif "year" in cols:
                    year_col = "year"
    except Exception:
        year_col = None

    if yrs and year_col:
        placeholders = ",".join(["?"] * len(yrs))
        year_filter_sql = f" AND {year_col} IN ({placeholders})"
        year_params = yrs
    elif yrs and not year_col:
        logger.warning("network.parquet missing year/fiscal_year; skipping year filter for network.")
        year_filter_sql = ""
        year_params = []

    value_treatments_expr = (
        "array_to_string(array_agg(DISTINCT internal_value_treatment), ', ')"
        if "internal_value_treatment" in network_columns_lower
        else "'Legacy subcontract model'"
    )

    # --- SQL Template (Parameterized) ---
    sql_template = """
        SELECT
            {group_col} as name,
            {cage_col} as cage,
            arbitrary(platform_family) as platform, 
            sum(subaward_value) as total,
            sum(subaward_value_raw) as total_raw,
            count(subaward_value) as included_reports,
            count(*) as source_reports,
            {value_treatments_expr} as value_treatments,
            count(contract_id) as transactions
        FROM network_source
        WHERE {where_clause}{year_filter}
        GROUP BY {group_col}, {cage_col}
        ORDER BY total DESC
        LIMIT ?
    """

    if is_drill_down:
        safe_cage = sanitize(cage)

        # Downstream: this facility is the prime; show top subs
        subs = run_duck_query(
            sql_template.format(
                group_col="sub_name",
                cage_col="sub_cage",
                where_clause="prime_cage = ?",
                year_filter=year_filter_sql,
                value_treatments_expr=value_treatments_expr
            ),
            tuple([safe_cage, *year_params, int(limit)])
        )

        # Upstream: this facility is the sub; show top primes
        primes = run_duck_query(
            sql_template.format(
                group_col="prime_name",
                cage_col="prime_cage",
                where_clause="sub_cage = ?",
                year_filter=year_filter_sql,
                value_treatments_expr=value_treatments_expr
            ),
            tuple([safe_cage, *year_params, int(limit)])
        )
    else:
        # Parent Mode (aggregate by gold parent)
        subs = run_duck_query(
            sql_template.format(
                group_col="sub_name",
                cage_col="sub_cage",
                where_clause="upper(prime_gold_parent) = ?",
                year_filter=year_filter_sql,
                value_treatments_expr=value_treatments_expr
            ),
            tuple([safe_name, *year_params, int(limit)])
        )

        primes = run_duck_query(
            sql_template.format(
                group_col="prime_name",
                cage_col="prime_cage",
                where_clause="upper(sub_gold_parent) = ?",
                year_filter=year_filter_sql,
                value_treatments_expr=value_treatments_expr
            ),
            tuple([safe_name, *year_params, int(limit)])
        )

    return {"subs": subs, "primes": primes}


# --- HELPER FUNCTIONS FOR TREND LOGIC (Ensure these are defined in main.py) ---
def calculate_trend_sum(trend_str: str, years: List[int]) -> float:
    if not trend_str or not years:
        return 0.0
    total = 0.0
    target_years = set(years)
    try:
        segments = str(trend_str).split("|")
        for seg in segments:
            if ":" in seg:
                y_str, amount_str = seg.split(":")
                if int(y_str) in target_years:
                    total += float(amount_str)
    except:
        pass
    return total

def _parse_trend_to_dict(trend_str: str) -> Dict[int, float]:
    out: Dict[int, float] = {}
    if not trend_str:
        return out
    for seg in str(trend_str).split("|") :
        if ":" not in seg:
            continue
        try:
            y, v = seg.split(":", 1)
            out[int(y)] = float(v)
        except:
            pass
    return out

def _sum_trend_dicts(dicts: List[Dict[int, float]]) -> str:
    total: Dict[int, float] = {}
    for d in dicts:
        for y, v in d.items():
            total[y] = total.get(y, 0.0) + float(v or 0.0)
    return "|".join([f"{y}:{total[y]}" for y in sorted(total.keys())])

def _clean_optional_value(value, default=None):
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
    except Exception:
        pass
    text = str(value).strip()
    if text.upper() in ("", "NAN", "NONE", "NULL", "NAT"):
        return default
    return value

def nsn_profile_fast_lookup(safe_niin: str, years: Optional[List[int]] = None) -> Optional[Dict[str, Any]]:
    """
    Small pre-aggregated lookup built by ETL. This keeps NSN clicks fast and leaves
    the older products.parquet scan as a fallback if the sidecar is absent.
    """
    try:
        fast_df = duck_fetch_df(
            """
            SELECT *
            FROM v_nsn_profile_lookup
            WHERE niin = ?
            LIMIT 1
            """,
            [safe_niin],
        )
    except Exception:
        return None

    if fast_df.empty:
        return None

    row = fast_df.iloc[0].to_dict()

    def g(key, default=None):
        return _clean_optional_value(row.get(key), default)

    trend_raw = g("annual_revenue_trend", "") or ""
    ys = safe_years(years, min_year=1900, max_year=2200, max_len=50)
    if ys:
        total_rev = float(calculate_trend_sum(str(trend_raw), ys))
    else:
        total_rev = float(g("total_revenue", 0) or 0)

    fsc_code = str(g("fsc_code", "") or "")
    nsn_value = str(g("nsn", "") or "")
    if not nsn_value:
        nsn_value = f"{fsc_code}-{safe_niin}" if fsc_code else safe_niin

    return {
        "found": True,
        "nsn": nsn_value,
        "niin": safe_niin,
        "item_name": g("item_name", None) or g("description", None) or "Unknown",
        "fsc_code": fsc_code,

        "total_revenue": total_rev,
        "total_units_sold": int(float(g("total_units_sold", 0) or 0)),
        "market_price": float(g("market_price", 0) or 0),
        "last_sold_date": g("last_sold_date", None),
        "annual_revenue_trend": trend_raw,

        "demil_code": g("demil_code", None),
        "shelf_life_code": g("shelf_life_code", None),
        "mgmt_control_code": g("mgmt_control_code", None),
        "unit_of_issue": g("unit_of_issue", None),
        "source_of_supply": g("source_of_supply", None),
        "govt_estimated_price": float(g("govt_estimated_price", 0) or 0),
        "acquisition_advice_code": g("acquisition_advice_code", None),

        "has_sales_history": bool(total_rev > 0),
        "has_reference_history": False,
        "reference_supplier_count": 0,
        "reference_part_count": 0,
        "reference_rows": 0,
        "reference_sources": None,
    }

# --- REPLACEMENT ENDPOINT ---
# --- REPLACEMENT ENDPOINT ---
@app.get("/api/company/parts")
def get_company_parts(
    cage: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    years: Optional[List[int]] = Query(default=None),
    rollup: Optional[str] = None,
    domain: Optional[str] = None,
    agency: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None
):
    if not cage: return []
    safe_cage = sanitize(cage)

    try:
        filtered = get_subset_from_disk(
            "products.parquet",
            where_clause="cage = ?",
            params=(safe_cage,)
        )
    except Exception as e:
        print(f"DuckDB Error in get_company_parts: {e}")
        filtered = pd.DataFrame()

    # Product rows provide reference context only. Financial values are sourced
    # from the NIIN/CAGE supplier sidecar in rollup mode below.
    if platform and not filtered.empty:
        safe_plat = sanitize(platform)
        filtered = filtered[filtered['platform_family'].fillna('').astype(str).str.upper() == safe_plat]
    if psc and not filtered.empty:
        safe_psc = sanitize(psc)
        filtered = filtered[filtered['fsc_code'].fillna('').astype(str).str.upper() == safe_psc]

    # =========================
    # ROLLUP MODE: NSN / NIIN
    # =========================
    if rollup == "nsn":
        def collect_part_numbers(series: pd.Series) -> List[str]:
            if series is None: return []
            vals = series.fillna("").astype(str).str.strip()
            vals = vals[vals != ""]
            seen = set()
            out_list: List[str] = []
            for v in vals.tolist():
                if v not in seen:
                    seen.add(v)
                    out_list.append(v)
            return out_list

        reference_agg = pd.DataFrame()
        if not filtered.empty:
            base = filtered["niin"] if "niin" in filtered.columns else filtered["nsn"]
            filtered["niin_key"] = (
                base.astype(str)
                .str.replace(r"[^0-9]", "", regex=True)
                .str.strip()
                .str.zfill(9)
            )
            reference_agg = (
                filtered.groupby("niin_key", observed=True, dropna=False)
                .agg(
                    reference_description=("description", "first"),
                    reference_platform=("platform_family", "first"),
                    part_numbers=("part_number", collect_part_numbers),
                )
                .reset_index()
            )

        ys = safe_years(years, min_year=2019, max_year=2200, max_len=50)
        scoped_where = ["UPPER(TRIM(CAST(cage AS VARCHAR))) = ?"]
        scoped_params: List[Any] = [safe_cage.upper()]

        if ys:
            placeholders = ",".join(["?"] * len(ys))
            scoped_where.append(f"year IN ({placeholders})")
            scoped_params.extend(ys)
        if platform:
            scoped_where.append("UPPER(TRIM(COALESCE(platform_family, ''))) = ?")
            scoped_params.append(sanitize(platform).upper())
        if domain:
            scoped_where.append("UPPER(TRIM(COALESCE(market_segment, ''))) = ?")
            scoped_params.append(sanitize(domain).upper())
        if agency:
            agency_value = f"%{sanitize(agency).upper()}%"
            scoped_where.append(
                "(UPPER(COALESCE(parent_agency, '')) LIKE ? OR UPPER(COALESCE(sub_agency, '')) LIKE ?)"
            )
            scoped_params.extend([agency_value, agency_value])

        try:
            supplier_schema = duck_fetch_df("DESCRIBE v_nsn_supplier_lookup")
            supplier_columns = {
                str(value).strip().lower()
                for value in supplier_schema.get("column_name", pd.Series(dtype=str)).dropna().tolist()
            }
        except Exception:
            supplier_columns = set()

        units_expr = (
            "TRY_CAST(total_units_sold AS DOUBLE)"
            if "total_units_sold" in supplier_columns
            else "CAST(NULL AS DOUBLE)"
        )

        market_where = ["1=1"]
        market_params: List[Any] = []
        if ys:
            placeholders = ",".join(["?"] * len(ys))
            market_where.append(f"s.year IN ({placeholders})")
            market_params.extend(ys)

        financial_query = f"""
            WITH scoped AS (
                SELECT
                    LPAD(TRIM(CAST(niin AS VARCHAR)), 9, '0') AS niin,
                    year,
                    TRY_CAST(total_revenue AS DOUBLE) AS line_value,
                    {units_expr} AS line_units,
                    CAST(last_sold AS VARCHAR) AS last_sold,
                    NULLIF(TRIM(CAST(platform_family AS VARCHAR)), '') AS platform_family
                FROM v_nsn_supplier_lookup
                WHERE {' AND '.join(scoped_where)}
            ),
            company_year AS (
                SELECT
                    niin,
                    year,
                    SUM(COALESCE(line_value, 0)) AS year_value,
                    SUM(line_units) AS year_units,
                    MAX(last_sold) AS year_last_sold,
                    MAX(platform_family) AS platform_family
                FROM scoped
                GROUP BY 1, 2
            ),
            company AS (
                SELECT
                    niin,
                    SUM(year_value) AS observed_value,
                    SUM(year_units) AS observed_units,
                    MAX(year_last_sold) AS last_sold_date,
                    MIN(year) AS period_start_fy,
                    MAX(year) AS period_end_fy,
                    MAX(platform_family) AS platform_family,
                    ARRAY_TO_STRING(
                        ARRAY_AGG(
                            CAST(year AS VARCHAR) || ':' || CAST(year_value AS VARCHAR)
                            ORDER BY year
                        ),
                        '|'
                    ) AS annual_revenue_trend
                FROM company_year
                GROUP BY 1
            ),
            market AS (
                SELECT
                    LPAD(TRIM(CAST(s.niin AS VARCHAR)), 9, '0') AS niin,
                    SUM(TRY_CAST(s.total_revenue AS DOUBLE)) AS market_value
                FROM v_nsn_supplier_lookup s
                INNER JOIN company c
                    ON LPAD(TRIM(CAST(s.niin AS VARCHAR)), 9, '0') = c.niin
                WHERE {' AND '.join(market_where)}
                GROUP BY 1
            )
            SELECT
                c.niin AS niin_key,
                COALESCE(NULLIF(TRIM(CAST(p.nsn AS VARCHAR)), ''), c.niin) AS nsn,
                NULLIF(TRIM(CAST(p.item_name AS VARCHAR)), '') AS description,
                NULLIF(TRIM(CAST(p.fsc_code AS VARCHAR)), '') AS fsc_code,
                c.platform_family,
                c.observed_value,
                c.observed_units,
                c.last_sold_date,
                c.period_start_fy,
                c.period_end_fy,
                c.annual_revenue_trend,
                CASE
                    WHEN COALESCE(m.market_value, 0) > 0
                    THEN 100.0 * c.observed_value / m.market_value
                    ELSE NULL
                END AS observed_dla_share_pct
            FROM company c
            LEFT JOIN market m ON c.niin = m.niin
            LEFT JOIN v_nsn_profile_lookup p ON c.niin = p.niin
        """

        try:
            agg = duck_fetch_df(financial_query, scoped_params + market_params)
        except Exception as e:
            logger.error(f"Company NIIN financial rollup failed: {e}")
            return []

        if agg.empty:
            return []

        if psc:
            if reference_agg.empty:
                return []
            agg = agg[agg["niin_key"].isin(set(reference_agg["niin_key"].tolist()))]
            if agg.empty:
                return []

        if not reference_agg.empty:
            agg = agg.merge(reference_agg, on="niin_key", how="left")
            agg["description"] = agg["description"].fillna(agg["reference_description"])
            agg["platform_family"] = agg["platform_family"].fillna(agg["reference_platform"])
        else:
            agg["part_numbers"] = [[] for _ in range(len(agg))]

        agg["part_numbers"] = agg["part_numbers"].apply(
            lambda value: value if isinstance(value, list) else []
        )
        agg["part_numbers_count"] = agg["part_numbers"].apply(len)
        agg = agg.sort_values(["observed_value", "niin_key"], ascending=[False, True], kind="mergesort")

        start = int(offset)
        end = start + int(limit)
        page = agg.iloc[start:end]

        results: List[Dict] = []
        for row in page.itertuples(index=False):
            clean_niin = str(row.niin_key)
            observed_value = float(getattr(row, "observed_value", 0) or 0)
            raw_units = getattr(row, "observed_units", None)
            observed_units = None if raw_units is None or pd.isna(raw_units) else float(raw_units)
            avg_unit_price = (
                observed_value / observed_units
                if observed_units is not None and observed_units > 0
                else None
            )

            results.append({
                "niin": clean_niin,
                "nsn": getattr(row, "nsn", None) or clean_niin,
                "description": getattr(row, "description", None),
                "part_number": "",
                "part_numbers": getattr(row, "part_numbers", []) or [],
                "part_numbers_count": int(getattr(row, "part_numbers_count", 0) or 0),
                "platform_family": getattr(row, "platform_family", None),
                "fsc_code": getattr(row, "fsc_code", None),
                "total_units_sold": int(observed_units) if observed_units is not None else None,
                "units": int(observed_units) if observed_units is not None else None,
                "total_revenue": observed_value,
                "amount": observed_value,
                "last_sold": getattr(row, "last_sold_date", None),
                "last_sold_date": getattr(row, "last_sold_date", None),
                "avg_unit_price": avg_unit_price,
                "max_unit_price": avg_unit_price,
                "annual_revenue_trend": getattr(row, "annual_revenue_trend", "") or "",
                "market_share_pct": 0.0,
                "direct_sales_market_share_pct": max(
                    0.0,
                    min(100.0, float(getattr(row, "observed_dla_share_pct", 0) or 0)),
                ),
                "observed_period_start_fy": int(getattr(row, "period_start_fy", 0) or 0),
                "observed_period_end_fy": int(getattr(row, "period_end_fy", 0) or 0),
            })

        return results

    if filtered.empty:
        return []

    if years:
        filtered["amount"] = filtered["annual_revenue_trend"].apply(
            lambda s: calculate_trend_sum(s or "", years)
        )
    else:
        filtered["amount"] = pd.to_numeric(filtered.get("total_revenue", 0), errors="coerce").fillna(0)

    # =========================
    # NON-ROLLUP MODE
    # =========================
    tie_col = "niin" if "niin" in filtered.columns else "nsn"
    filtered = filtered.sort_values(["amount", tie_col], ascending=[False, True], kind="mergesort")

    start = int(offset)
    end = start + int(limit)
    page = filtered.iloc[start:end]

    results: List[Dict] = []
    for row in page.itertuples():
        raw_niin = str(getattr(row, "niin", "")).strip()
        clean_niin = raw_niin.zfill(9)

        raw_nsn = str(getattr(row, "nsn", "")).strip()
        final_nsn = clean_niin if len(raw_nsn) < 9 else raw_nsn

        results.append({
            "niin": clean_niin,
            "nsn": final_nsn,
            "description": getattr(row, "description", None),
            "part_number": getattr(row, "part_number", None),
            "platform_family": getattr(row, "platform_family", None),
            "total_units_sold": int(getattr(row, "total_units_sold", 0) or 0),
            "units": int(getattr(row, "total_units_sold", 0) or 0),
            "total_revenue": float(getattr(row, "total_revenue", 0) or 0),
            "amount": float(getattr(row, "amount", 0) or 0),
            "last_sold": getattr(row, "last_sold_date", None),
            "last_sold_date": getattr(row, "last_sold_date", None),
            "avg_unit_price": float(getattr(row, "avg_unit_price", 0) or 0),
            "max_unit_price": float(getattr(row, "avg_unit_price", 0) or 0),
            "annual_revenue_trend": getattr(row, "annual_revenue_trend", "") or "",
            "market_share_pct": float(getattr(row, "market_share_pct", 0) or 0),
            "direct_sales_market_share_pct": float(getattr(row, "direct_sales_market_share_pct", 0) or 0),
        })

    return results




@app.get("/api/company/parts/count")
def get_company_parts_count(
    cage: str,
    rollup: Optional[str] = None,
    years: Optional[List[int]] = Query(default=None),
    domain: Optional[str] = None,
    agency: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None,
):
    if not cage:
        return {"count": 0}

    safe_cage = sanitize(cage)

    try:
        if rollup == "nsn":
            where_parts = ["UPPER(TRIM(CAST(cage AS VARCHAR))) = ?"]
            params: List[Any] = [safe_cage.upper()]
            ys = safe_years(years, min_year=2019, max_year=2200, max_len=50)
            if ys:
                placeholders = ",".join(["?"] * len(ys))
                where_parts.append(f"year IN ({placeholders})")
                params.extend(ys)
            if platform:
                where_parts.append("UPPER(TRIM(COALESCE(platform_family, ''))) = ?")
                params.append(sanitize(platform).upper())
            if domain:
                where_parts.append("UPPER(TRIM(COALESCE(market_segment, ''))) = ?")
                params.append(sanitize(domain).upper())
            if agency:
                agency_value = f"%{sanitize(agency).upper()}%"
                where_parts.append(
                    "(UPPER(COALESCE(parent_agency, '')) LIKE ? OR UPPER(COALESCE(sub_agency, '')) LIKE ?)"
                )
                params.extend([agency_value, agency_value])

            eligible_join = ""
            query_params: List[Any] = list(params)
            if psc:
                path = LOCAL_CACHE_DIR / "products.parquet"
                if not path.exists():
                    return {"count": 0}
                eligible_join = """
                    INNER JOIN (
                        SELECT DISTINCT
                            LPAD(
                                regexp_replace(
                                    COALESCE(CAST(niin AS VARCHAR), CAST(nsn AS VARCHAR), ''),
                                    '[^0-9]', ''
                                ),
                                9,
                                '0'
                            ) AS niin
                        FROM read_parquet(?)
                        WHERE UPPER(TRIM(cage)) = ?
                          AND UPPER(TRIM(CAST(fsc_code AS VARCHAR))) = ?
                    ) eligible
                        ON LPAD(TRIM(CAST(s.niin AS VARCHAR)), 9, '0') = eligible.niin
                """
                query_params = [str(path), safe_cage, sanitize(psc).upper(), *params]

            count_df = duck_fetch_df(
                f"""
                    SELECT COUNT(DISTINCT LPAD(TRIM(CAST(s.niin AS VARCHAR)), 9, '0')) AS n
                    FROM v_nsn_supplier_lookup s
                    {eligible_join}
                    WHERE {' AND '.join(where_parts)}
                """,
                query_params,
            )
            n = int(count_df["n"].iloc[0] or 0) if not count_df.empty else 0
        else:
            path = LOCAL_CACHE_DIR / "products.parquet"
            if not path.exists():
                return {"count": 0}
            with DUCK_LOCK:
                global DUCK_CONN
                ensure_duck_conn()
                sql = """
                    SELECT COUNT(*) AS n
                    FROM read_parquet(?)
                    WHERE UPPER(TRIM(cage)) = ?
                """
                n = DUCK_CONN.execute(sql, (str(path), safe_cage)).fetchone()[0]

        return {"count": int(n or 0)}
    except Exception:
        logger.exception("company parts count error")
        return {"count": 0}



@app.get("/api/company/awards")
def get_company_awards(
    cage: Optional[str] = None, 
    name: Optional[str] = None,
    limit: int = 50, 
    offset: int = 0,
    years: Optional[List[int]] = Query(None),
    agency: Optional[str] = None,
    threshold_m: Optional[float] = 0,
    domain: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None
):
    where_parts = []
    params = []

    # 1. Base Filters
    if cage and cage != "AGGREGATE":
        where_parts.append("(vendor_cage = ?)")
        safe_cage = sanitize(cage)
        params.append(safe_cage)
    elif name:
        where_parts.append("upper(vendor_name) LIKE ?")
        safe_name = f"%{sanitize(name)}%"
        params.append(safe_name)
    else:
        return []

    # 2. Global Filters
    if agency:
        where_parts.append("(upper(sub_agency) = ? OR upper(parent_agency) = ?)")
        safe_ag = sanitize(agency)
        params.extend([safe_ag, safe_ag])

    if domain:
        where_parts.append("upper(market_segment) = ?")
        params.append(sanitize(domain))

    if platform:
        where_parts.append("upper(platform_family) = ?")
        params.append(sanitize(platform))

    if psc:
        where_parts.append("upper(psc) = ?")
        params.append(sanitize(psc))

    if years and len(years) > 0:
        placeholders = ",".join(["?" for _ in years])
        where_parts.append(f"year IN ({placeholders})")
        params.extend(years)

    if threshold_m and threshold_m > 0:
        where_parts.append("spend_amount >= ?")
        params.append(float(threshold_m) * 1_000_000)

    # 3. Execute via DuckDB
    where_clause = " AND ".join(where_parts)

    try:
        path = LOCAL_CACHE_DIR / "transactions.parquet"
        if not path.exists():
             return []

        df = get_subset_from_disk(
            "transactions.parquet",
            where_clause=where_clause,
            params=tuple(params),
            columns_sql="contract_id, action_date, sub_agency, parent_agency, description, spend_amount, naics_code, psc, platform_family",
            order_by_sql="spend_amount DESC", # ✅ Permanently sorted by highest value
            limit=limit,
            offset=offset
        )
        
        return [
            {
                "contract_id": r.contract_id,
                "action_date": str(r.action_date),
                "agency": r.sub_agency if hasattr(r, 'sub_agency') and pd.notna(r.sub_agency) else getattr(r, 'parent_agency', ''),
                "description": r.description,
                "spend_amount": float(r.spend_amount or 0),
                "naics_code": r.naics_code,
                "psc": r.psc
            }
            for r in df.itertuples()
        ]
    except Exception as e:
        logger.error(f"Awards Query Error: {e}")
        return []
    

@app.get("/api/company/opportunities/recommended")
def get_company_opportunities_recommended(
    cage: Optional[str] = None, 
    name: Optional[str] = None,
    agency: Optional[str] = None,
    domain: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None
):
    # 1. Reuse existing profile logic to find capabilities
    profile = get_company_profile(cage, name)

    if not profile.get('found') or not profile.get('top_naics'):
        return []

    # 2. Extract and Clean NAICS codes
    clean_naics_list = []
    for n in profile['top_naics']:
        code_part = str(n).split(' - ')[0].strip().split('.')[0]
        if len(code_part) >= 3:
            clean_naics_list.append(code_part)

    if not clean_naics_list:
        return []

    # 3. DuckDB SQL Query construction
    naics_conditions = []
    params = []
    for n in set(clean_naics_list):
        naics_conditions.append("CAST(naics AS VARCHAR) LIKE ?")
        params.append(f"{n}%")
        
    where_clause = f"({' OR '.join(naics_conditions)})"

    # ✅ ADD GLOBAL FILTERS
    if agency:
        # Match your opportunities parquet schema
        where_clause += " AND (upper(agency) = ? OR upper(sub_agency) = ?)"
        safe_ag = sanitize(agency)
        params.extend([safe_ag, safe_ag])
    if psc:
        where_clause += " AND upper(psc) = ?"
        params.append(sanitize(psc))

    query = f"""
        SELECT id, title, sol_num, agency, deadline, set_aside_type, poc_email
        FROM v_opportunities
        WHERE {where_clause}
          AND try_cast(deadline as date) >= current_date()
        ORDER BY try_cast(deadline as date) ASC
        LIMIT 50
    """
    
    try:
        df = duck_fetch_df(query, params)
        if df.empty: return []
        
        # ✅ FIX 3: Sanitize NaNs
        df = df_sanitize_for_json(df)
        
        results = []
        for row in df.itertuples(index=False):
            sol_val = getattr(row, 'sol_num', '')
            if pd.isna(sol_val) or not str(sol_val).strip():
                 sol_val = getattr(row, 'id', '')
                 
            results.append({
                "noticeid": getattr(row, 'id', ''),
                "title": getattr(row, 'title', ''),
                "sol_num": sol_val, 
                "sol#": sol_val,    
                "department_indagency": getattr(row, 'agency', ''),
                "responsedeadline": getattr(row, 'deadline', ''),
                "setaside": getattr(row, 'set_aside_type', ''),
                "primarycontactemail": getattr(row, 'poc_email', '')
            })
        return results
    except Exception as e:
        logger.error(f"Recommended Opps DuckDB Error: {e}")
        return []


# ==========================================
#        NEWS INTELLIGENCE
# ==========================================

# [Find and replace get_company_news in api.py]

# [Find and replace get_company_news in api.py]

@app.get("/api/company/news")
def get_company_news(name: str, city: Optional[str] = None, state: Optional[str] = None):
    logger.info("NEWS LOOKUP name=%s city=%s state=%s", name, city, state)

    if not name:
        return []

    # Cache key includes local context
    key_name = (name or "").strip().upper()
    key_city = (city or "").strip().upper()
    key_state = (state or "").strip().upper()
    cache_key = f"{key_name}|{key_city}|{key_state}"

    cached = NEWS_CACHE.get(cache_key)
    if cached is not None:
        return cached

    # 1. Clean Company Name
    clean_name = name.upper()

    suffixes = [" CORPORATION", " COMPANY", " INC.", " INC", " LLC", " CORP.", " CORP", " LTD.", " LTD", ","]
    for suffix in suffixes:
        if clean_name.endswith(suffix):
            clean_name = clean_name[:-len(suffix)]
    clean_name = clean_name.strip()
    
    # 2. Build Query (STRICT MODE)
    queries = []
    is_local_search = False

    if city and len(city) > 2:
        is_local_search = True
        # STRICT: Search for "Name" AND "City". No fallback to just "Name".
        queries.append(urllib.parse.quote(f'"{clean_name}" "{city}" defense'))
    else:
        # PARENT: Only run generic search if NO city is provided
        queries.append(urllib.parse.quote(f'"{clean_name}" defense'))

    # 3. Fetch Raw Items
    raw_items = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

    for q in queries:
        if len(raw_items) >= 20: break 
        
        url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=3) as response:
                root = ET.fromstring(response.read())
                
                for item in root.findall('.//item'):
                    title = item.find('title').text or ""
                    link = item.find('link').text or ""
                    pub_date = item.find('pubDate').text or ""
                    description = item.find('description').text or ""
                    
                    # Clean Source
                    source = "Google News"
                    if " - " in title:
                        parts = title.rsplit(" - ", 1)
                        title = parts[0]
                        source = parts[1]

                    # ✅ ZERO-TOLERANCE VALIDATOR
                    # If we asked for "East Hartford", the story MUST say "East Hartford".
                    # If it doesn't, we assume Google gave us a bad fuzzy match (e.g. Andover) and we kill it.
                    if is_local_search:
                        # Normalize everything to lowercase for checking
                        blob = (title + " " + description).lower()
                        check_city = city.lower()
                        
                        if check_city not in blob:
                            # 🚫 REJECT: The city is not in the text.
                            continue

                    raw_items.append({
                        "title": unescape(title),
                        "link": link,
                        "date": pub_date[:16],
                        "source": source
                    })
        except Exception as e:
            print(f"News Error: {e}")
            continue

    # 4. DEDUPLICATION (Retained for clean results)
    final_items = []
    
    GENERIC_ENTITIES = {
        'US', 'USA', 'U.S.', 'UK', 'U.K.', 'PENTAGON', 'DOD', 'DEFENSE', 'MILITARY', 
        'ARMY', 'NAVY', 'AIR', 'FORCE', 'GOVERNMENT', 'CONGRESS', 'HOUSE', 'SENATE',
        'LOCKHEED', 'MARTIN', 'BOEING', 'NORTHROP', 'GRUMMAN', 'RTX', 'RAYTHEON',
        'COMPANY', 'CORP', 'INC', 'CEO', 'CHIEF', 'REPORT', 'NEWS', 'UPDATE', 'TODAY',
        'DEAL', 'CONTRACT', 'AWARD', 'WINS', 'AGREEMENT', 'SALES', 'QUARTER', 'PROFIT',
        'SAYS', 'NEW', 'BIG', 'VISITS', 'AT', 'FOR', 'TO', 'OF', 'IN', 'ON', 'WITH'
    }

    seen_topics = set()

    def get_topics(text):
        words = re.findall(r'\b[A-Z][a-z]+\b|\b[A-Z]+\b', text)
        valid_topics = set()
        for w in words:
            w_upper = w.upper()
            if len(w_upper) > 2 and w_upper not in GENERIC_ENTITIES:
                valid_topics.add(w_upper)
        return valid_topics

    for item in raw_items:
        if len(final_items) >= 6: break
        
        current_topics = get_topics(item['title'])
        is_duplicate = False
        
        if current_topics:
            overlap = current_topics.intersection(seen_topics)
            if len(overlap) > 0:
                is_duplicate = True
        
        if not is_duplicate:
            final_items.append(item)
            seen_topics.update(current_topics)

    # ✅ FINAL CHANGE: NO FALLBACK FOR LOCAL
    if is_local_search:
        NEWS_CACHE.set(cache_key, final_items)
        return final_items

    # Only use fallback for Corporate View (if everything got filtered by mistake)
    if not final_items and raw_items:
        out = raw_items[:5]
        NEWS_CACHE.set(cache_key, out)
        return out

    NEWS_CACHE.set(cache_key, final_items)
    return final_items


# ==========================================
#        NSN / PART INTELLIGENCE
# ==========================================

# ==========================================
#        NSN / PART INTELLIGENCE
# ==========================================

def get_niin(input_str: str) -> str:
    # Remove all non-digits
    clean = ''.join(filter(str.isdigit, str(input_str)))
    
    # ✅ FIX: If it's short (e.g. 14851472), pad it (014851472). 
    # If it's long (e.g. 5945014851472), take the last 9.
    if len(clean) < 9:
        return clean.zfill(9)
    return clean[-9:]

from fastapi import Query
from typing import Optional, List

from fastapi import Query
from typing import Optional, List
import pandas as pd


# ==========================================
# NSN/CAGE REFERENCE HELPERS
# ==========================================

def nsn_ref_col(cols: set, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c.lower() in cols:
            return c.lower()
    return None


def nsn_ref_niin_where(cols: set, safe_niin: str) -> Tuple[str, List[Any]]:
    niin_col = nsn_ref_col(cols, ["niin"])
    nsn_col = nsn_ref_col(cols, ["nsn"])

    # The ETL writes NIIN as a normalized nine-character string. Keeping this
    # predicate direct allows DuckDB to prune the NIIN-sorted Parquet row groups.
    if niin_col:
        return f"{quote_ident(niin_col)} = ?", [safe_niin]

    if nsn_col:
        return f"{normalised_niin_filter_expr(nsn_col)} = ?", [safe_niin]

    return "1=0", []


def nsn_ref_profile_lookup(safe_niin: str) -> Dict[str, Any]:
    cols = get_duck_table_columns(NSN_REF_TABLE)
    if not cols:
        return {}

    where_sql, params = nsn_ref_niin_where(cols, safe_niin)
    if where_sql == "1=0":
        return {}

    nsn_col = nsn_ref_col(cols, ["nsn"])
    desc_col = nsn_ref_col(cols, ["description", "item_name", "nomenclature", "product_description", "item_description"])
    fsc_col = nsn_ref_col(cols, ["fsc_code", "fsc"])
    cage_col = nsn_ref_col(cols, ["cage_code", "cage", "vendor_cage"])
    part_col = nsn_ref_col(cols, ["part_number", "part_no", "pn"])
    source_col = nsn_ref_col(cols, ["reference_source", "source", "source_layer", "data_source"])

    select_parts = [
        f"'{safe_niin}' AS niin",
        "COUNT(*) AS reference_rows",
    ]

    if nsn_col:
        select_parts.append(f"MAX(NULLIF(TRIM(CAST({quote_ident(nsn_col)} AS VARCHAR)), '')) AS nsn")
    else:
        select_parts.append("CAST(NULL AS VARCHAR) AS nsn")

    if desc_col:
        select_parts.append(f"MAX(NULLIF(TRIM(CAST({quote_ident(desc_col)} AS VARCHAR)), '')) AS item_name")
    else:
        select_parts.append("CAST(NULL AS VARCHAR) AS item_name")

    if fsc_col:
        select_parts.append(f"MAX(NULLIF(TRIM(CAST({quote_ident(fsc_col)} AS VARCHAR)), '')) AS fsc_code")
    else:
        select_parts.append("CAST(NULL AS VARCHAR) AS fsc_code")

    if cage_col:
        select_parts.append(f"COUNT(DISTINCT UPPER(TRIM(CAST({quote_ident(cage_col)} AS VARCHAR)))) AS reference_supplier_count")
    else:
        select_parts.append("CAST(0 AS BIGINT) AS reference_supplier_count")

    if part_col:
        select_parts.append(f"COUNT(DISTINCT NULLIF(TRIM(CAST({quote_ident(part_col)} AS VARCHAR)), '')) AS reference_part_count")
    else:
        select_parts.append("CAST(0 AS BIGINT) AS reference_part_count")

    if source_col:
        select_parts.append(f"string_agg(DISTINCT NULLIF(TRIM(CAST({quote_ident(source_col)} AS VARCHAR)), ''), ', ') AS reference_sources")
    else:
        select_parts.append("CAST(NULL AS VARCHAR) AS reference_sources")

    sql = f"""
        SELECT
            {", ".join(select_parts)}
        FROM {NSN_REF_TABLE}
        WHERE {where_sql}
    """

    try:
        df = duck_fetch_df(sql, params)
        if df.empty:
            return {}

        row = df.iloc[0].to_dict()

        if not row.get("reference_rows"):
            return {}

        return row

    except Exception:
        logger.exception("NSN reference profile lookup failed for NIIN=%s", safe_niin)
        return {}


def nsn_ref_supplier_lookup(safe_niin: str) -> Dict[str, Dict[str, Any]]:
    cols = get_duck_table_columns(NSN_REF_TABLE)
    if not cols:
        return {}

    where_sql, params = nsn_ref_niin_where(cols, safe_niin)
    if where_sql == "1=0":
        return {}

    cage_col = nsn_ref_col(cols, ["cage_code", "cage", "vendor_cage"])
    if not cage_col:
        return {}

    vendor_col = nsn_ref_col(cols, ["vendor_name", "company_name", "manufacturer_name", "entity_name"])
    part_col = nsn_ref_col(cols, ["part_number", "part_no", "pn"])
    source_col = nsn_ref_col(cols, ["reference_source", "source", "source_layer", "data_source"])
    rncc_col = nsn_ref_col(cols, ["rncc_codes", "rncc"])
    rnvc_col = nsn_ref_col(cols, ["rnvc_codes", "rnvc"])
    rnsc_col = nsn_ref_col(cols, ["rnsc_codes", "rnsc"])
    cage_status_col = nsn_ref_col(cols, ["cage_status_codes", "cage_status"])
    supplier_status_col = nsn_ref_col(cols, ["supplier_status"])
    supplier_status_detail_col = nsn_ref_col(cols, ["supplier_status_detail"])
    procurement_authorized_col = nsn_ref_col(cols, ["is_procurement_authorized"])
    active_authorized_col = nsn_ref_col(cols, ["is_active_authorized_source"])

    select_parts = [
        f"UPPER(TRIM(CAST({quote_ident(cage_col)} AS VARCHAR))) AS cage"
    ]

    if vendor_col:
        select_parts.append(f"MAX(NULLIF(TRIM(CAST({quote_ident(vendor_col)} AS VARCHAR)), '')) AS vendor")
    else:
        select_parts.append("CAST(NULL AS VARCHAR) AS vendor")

    if part_col:
        select_parts.append(f"string_agg(DISTINCT NULLIF(TRIM(CAST({quote_ident(part_col)} AS VARCHAR)), ''), ', ') AS part_numbers")
    else:
        select_parts.append("CAST(NULL AS VARCHAR) AS part_numbers")

    if source_col:
        select_parts.append(f"string_agg(DISTINCT NULLIF(TRIM(CAST({quote_ident(source_col)} AS VARCHAR)), ''), ', ') AS source")
    else:
        select_parts.append("CAST(NULL AS VARCHAR) AS source")

    for source_column, alias in (
        (rncc_col, "rncc_codes"),
        (rnvc_col, "rnvc_codes"),
        (rnsc_col, "rnsc_codes"),
        (cage_status_col, "cage_status_codes"),
        (supplier_status_col, "supplier_status"),
        (supplier_status_detail_col, "supplier_status_detail"),
    ):
        if source_column:
            select_parts.append(
                f"string_agg(DISTINCT NULLIF(TRIM(CAST({quote_ident(source_column)} AS VARCHAR)), ''), ',') AS {alias}"
            )
        else:
            select_parts.append(f"CAST(NULL AS VARCHAR) AS {alias}")

    if procurement_authorized_col:
        select_parts.append(
            f"BOOL_OR(COALESCE(CAST({quote_ident(procurement_authorized_col)} AS BOOLEAN), FALSE)) AS is_procurement_authorized"
        )
    else:
        select_parts.append("FALSE AS is_procurement_authorized")

    if active_authorized_col:
        select_parts.append(
            f"BOOL_OR(COALESCE(CAST({quote_ident(active_authorized_col)} AS BOOLEAN), FALSE)) AS is_active_authorized_source"
        )
    else:
        select_parts.append("FALSE AS is_active_authorized_source")

    sql = f"""
        SELECT
            {", ".join(select_parts)}
        FROM {NSN_REF_TABLE}
        WHERE {where_sql}
          AND TRIM(CAST({quote_ident(cage_col)} AS VARCHAR)) <> ''
        GROUP BY 1
    """

    try:
        df = duck_fetch_df(sql, params)
        if df.empty:
            return {}

        out = {}

        for r in df.to_dict(orient="records"):
            cage = str(r.get("cage") or "").strip().upper()
            if not cage or cage in {"NAN", "NONE", "NULL"}:
                continue

            out[cage] = {
                "vendor": r.get("vendor"),
                "part_numbers": r.get("part_numbers") or "—",
                "source": r.get("source"),
                "rncc_codes": r.get("rncc_codes"),
                "rnvc_codes": r.get("rnvc_codes"),
                "rnsc_codes": r.get("rnsc_codes"),
                "cage_status_codes": r.get("cage_status_codes"),
                "supplier_status": r.get("supplier_status"),
                "supplier_status_detail": r.get("supplier_status_detail"),
                "is_procurement_authorized": bool(r.get("is_procurement_authorized") or False),
                "is_active_authorized_source": bool(r.get("is_active_authorized_source") or False),
            }

        return out

    except Exception:
        logger.exception("NSN reference supplier lookup failed for NIIN=%s", safe_niin)
        return {}


@app.get("/api/nsn/profile")
def get_nsn_profile(
    nsn: str,
    # Keep parameters to prevent 422 errors.
    # Revenue metrics remain revenue-backed; reference metadata comes from v_nsn_cage_reference.
    years: Optional[List[int]] = Query(None),
    agency: Optional[str] = None,
    domain: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None
):
    # 1) Clean Input
    clean = ''.join(filter(str.isdigit, str(nsn)))
    safe_niin = clean.zfill(9) if len(clean) < 9 else clean[-9:]

    # 2) Fast revenue-backed profile lookup. Falls back to the older products.parquet
    # scan if the new ETL sidecar has not been published yet.
    fast_profile = nsn_profile_fast_lookup(safe_niin, years)
    if fast_profile:
        return fast_profile

    # 3) Revenue-backed product lookup remains unchanged.
    df = get_subset_from_disk(
        "products.parquet",
        where_clause="niin = ?",
        params=(safe_niin,),
        limit=50000
    )

    # 3) Fast path: avoid scanning the huge NSN/CAGE reference file for revenue-backed NSNs.
    ref_profile = {}

    # 4) If there is no revenue-backed product row, then use the full reference universe.
    if df.empty:
        ref_profile = nsn_ref_profile_lookup(safe_niin)

        if not ref_profile:
            return {"found": False}

        fsc_code = ref_profile.get("fsc_code") or ""
        item_name = ref_profile.get("item_name") or "Unknown"

        return {
            "found": True,
            "nsn": f"{fsc_code}-{safe_niin}" if fsc_code else safe_niin,
            "niin": safe_niin,
            "item_name": item_name,
            "fsc_code": fsc_code,

            "total_revenue": 0.0,
            "total_units_sold": 0,
            "market_price": 0.0,
            "last_sold_date": None,
            "annual_revenue_trend": {},

            "demil_code": None,
            "shelf_life_code": None,
            "mgmt_control_code": None,
            "unit_of_issue": None,
            "source_of_supply": None,
            "govt_estimated_price": 0.0,
            "acquisition_advice_code": None,

            "has_sales_history": False,
            "has_reference_history": True,
            "reference_supplier_count": int(ref_profile.get("reference_supplier_count") or 0),
            "reference_part_count": int(ref_profile.get("reference_part_count") or 0),
            "reference_rows": int(ref_profile.get("reference_rows") or 0),
            "reference_sources": ref_profile.get("reference_sources"),
        }

    match = df.copy()

    match["total_revenue"] = pd.to_numeric(match.get("total_revenue", 0), errors="coerce").fillna(0)
    match["total_units_sold"] = pd.to_numeric(match.get("total_units_sold", 0), errors="coerce").fillna(0)

    # 5) Calculate Dynamic Revenue based on Years.
    if years:
        match["dynamic_amount"] = match["annual_revenue_trend"].apply(
            lambda s: calculate_trend_sum(s or "", years)
        )
        total_rev = float(match["dynamic_amount"].sum())
    else:
        total_rev = float(match["total_revenue"].sum())
        
    total_units = int(match["total_units_sold"].sum())

    # 6) Best Row Logic.
    best_row = match.iloc[0]
    if "description" in match.columns:
        for r in match.itertuples(index=False):
            desc = getattr(r, "description", None)
            if desc and str(desc).strip() and str(desc).strip().upper() != "UNKNOWN":
                best_row = r
                break
    else:
        best_row = match.iloc[0]

    def g(obj, key, default=None):
        try:
            return getattr(obj, key)
        except Exception:
            try:
                return obj.get(key, default)
            except Exception:
                return default

    fsc_code = (
        g(best_row, "fsc_code", "")
        or g(best_row, "fsc", "")
        or ref_profile.get("fsc_code", "")
    )

    desc = (
        g(best_row, "description", None)
        or g(best_row, "item_name", None)
        or ref_profile.get("item_name")
        or "Unknown"
    )

    # 7) Weighted average price.
    match["avg_unit_price"] = pd.to_numeric(match.get("avg_unit_price", 0), errors="coerce").fillna(0)
    if total_units > 0:
        avg_unit_price = float((match["avg_unit_price"] * match["total_units_sold"]).sum() / total_units)
    else:
        avg_unit_price = float(match["avg_unit_price"].mean() if not match.empty else 0.0)

    last_sold_date = str(match["last_sold_date"].max()) if "last_sold_date" in match.columns else None
    
    # 8) Trend.
    trend_dicts = match["annual_revenue_trend"].apply(_parse_trend_to_dict).tolist()
    trend = _sum_trend_dicts(trend_dicts)

    return {
        "found": True,
        "nsn": f"{fsc_code}-{safe_niin}" if fsc_code else safe_niin,
        "niin": safe_niin,
        "item_name": desc,
        "fsc_code": fsc_code,

        "total_revenue": float(total_rev),
        "total_units_sold": int(total_units),

        "market_price": float(avg_unit_price),
        "last_sold_date": last_sold_date,
        "annual_revenue_trend": trend,

        "demil_code": g(best_row, "demil_code", None),
        "shelf_life_code": g(best_row, "shelf_life_code", None),
        "mgmt_control_code": g(best_row, "mgmt_control_code", None),
        "unit_of_issue": g(best_row, "unit_of_issue", None),
        "source_of_supply": g(best_row, "source_of_supply", None),
        "govt_estimated_price": float(g(best_row, "govt_estimated_price", 0) or 0),
        "acquisition_advice_code": g(best_row, "acquisition_advice_code", None),

        "has_sales_history": bool(total_rev > 0),
        "has_reference_history": bool(ref_profile),
        "reference_supplier_count": int(ref_profile.get("reference_supplier_count") or 0) if ref_profile else 0,
        "reference_part_count": int(ref_profile.get("reference_part_count") or 0) if ref_profile else 0,
        "reference_rows": int(ref_profile.get("reference_rows") or 0) if ref_profile else 0,
        "reference_sources": ref_profile.get("reference_sources") if ref_profile else None,
    }



@app.get("/api/nsn/suppliers")
def get_nsn_suppliers(
    nsn: str,
    years: Optional[List[int]] = Query(None),
    agency: Optional[str] = None,
    domain: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None
):
    safe_niin = get_niin(nsn).zfill(9)

    # 1. DUCKDB: Instant math for revenue and contract counts.
    # This stays revenue-backed.
    where_parts = ["niin = ?"]
    params = [safe_niin]

    ys = safe_years(years, min_year=1900, max_year=2200, max_len=50)
    if ys:
        years_csv = ",".join([str(y) for y in ys])
        where_parts.append(f"year IN ({years_csv})")
    if agency:
        where_parts.append("(upper(coalesce(parent_agency,'')) LIKE ? OR upper(coalesce(sub_agency,'')) LIKE ?)")
        safe_ag = f"%{sanitize(agency).upper()}%"
        params.extend([safe_ag, safe_ag])
    if domain:
        where_parts.append("upper(coalesce(market_segment,'')) LIKE ?")
        params.append(f"%{sanitize(domain).upper()}%")
    if platform:
        where_parts.append("upper(coalesce(platform_family,'')) LIKE ?")
        params.append(f"%{sanitize(platform).upper()}%")
    if psc:
        where_parts.append("trim(upper(coalesce(psc,''))) = ?")
        params.append(sanitize(psc).upper())

    where_sql = " AND ".join(where_parts)

    try:
        fast_supplier_query = f"""
            SELECT
                TRIM(UPPER(cage)) as cage,
                MAX(vendor) as vendor,
                COUNT(DISTINCT contract_id) as contracts,
                MAX(last_sold) as last_sold,
                SUM(total_revenue) as total_revenue
            FROM v_nsn_supplier_lookup
            WHERE {where_sql}
            GROUP BY 1
        """

        sales_df = duck_fetch_df(fast_supplier_query, params)
        sales_records = sales_df.to_dict('records') if not sales_df.empty else []
    except Exception:
        duck_query = f"""
            SELECT 
                TRIM(UPPER(vendor_cage)) as cage,
                MAX(vendor_name) as vendor,
                COUNT(DISTINCT contract_id) as contracts,
                    MAX(action_date) as last_sold,
                SUM(spend_amount) as total_revenue
            FROM v_transactions
            WHERE {where_sql}
            GROUP BY 1
        """

        try:
            sales_df = duck_fetch_df(duck_query, params)
            sales_records = sales_df.to_dict('records') if not sales_df.empty else []
        except Exception as e:
            logger.error(f"NSN Suppliers DuckDB Error: {e}")
            sales_records = []

    # 2. LOCAL DUCKDB: full NSN/CAGE reference lookup. The reference parquet is
    # sorted by NIIN, so this direct lookup remains row-group prunable.
    approved_map = {}

    try:
        approved_map = nsn_ref_supplier_lookup(safe_niin)
    except Exception:
        logger.exception("NSN reference supplier lookup failed")
        approved_map = {}

    # ✅ RESTORED BUSINESS LOGIC: Government & Standards Dictionary
    gov_dict = {
        '19207': 'US ARMY TACOM - DESIGN ACTIVITY',
        '81349': 'MILITARY STANDARDS / PROMULGATING ACTIVITY',
        '80205': 'NATIONAL AEROSPACE STANDARDS (NAS)',
        '96906': 'MILITARY STANDARDS (MS)',
        '57685': 'NAVAL AIR SYSTEMS COMMAND',
        '81348': 'FEDERAL SPECIFICATIONS',
        '88044': 'AERONAUTICAL STANDARDS GROUP',
        '9009H': 'WSK PZL-KALISZ S.A. (POLAND)',
        '100CB': 'ARITEX CADING S.A. (SPAIN)',
        'A486G': 'PATRIA AVIATION OY (FINLAND)',
        'K1037': 'HANWHA AEROSPACE (SOUTH KOREA)',
        'D0019': 'RHEINMETALL LANDSYSTEME (GERMANY)'
    }

    def resolve_vendor_name(cage_code, tx_name):
        # 1. Prefer the transaction/reference name if valid.
        if tx_name and str(tx_name).strip() and str(tx_name).upper() not in ["NAN", "NONE"]:
            return tx_name

        # 2. Fallback to Military/Gov Dictionary.
        if cage_code in gov_dict:
            return gov_dict[cage_code]

        # 3. Fallback to Global Cache.
        cached_name = GLOBAL_CACHE.get("cage_name_map", {}).get(cage_code)
        if cached_name:
            return cached_name

        # 4. Final fallback.
        return f"Unknown Manufacturer (CAGE: {cage_code})"

    # 3. MERGE Results.
    out = []
    sales_cages = set()
    
    for r in sales_records:
        c = str(r.get('cage', '')).strip().upper()
        if not c or c == 'NAN':
            continue
        
        sales_cages.add(c)

        ref_row = approved_map.get(c, {})
        ref_vendor = ref_row.get("vendor") if isinstance(ref_row, dict) else None

        r['vendor'] = resolve_vendor_name(c, r.get('vendor') or ref_vendor)
        r['is_approved_source'] = bool(ref_row.get("is_active_authorized_source", False)) if isinstance(ref_row, dict) else False
        r['is_procurement_authorized'] = bool(ref_row.get("is_procurement_authorized", False)) if isinstance(ref_row, dict) else False
        r['is_active_authorized_source'] = r['is_approved_source']
        r['part_numbers'] = ref_row.get("part_numbers", "—") if isinstance(ref_row, dict) else "—"
        r['reference_source'] = ref_row.get("source") if isinstance(ref_row, dict) else None
        r['rncc_codes'] = ref_row.get("rncc_codes") if isinstance(ref_row, dict) else None
        r['rnvc_codes'] = ref_row.get("rnvc_codes") if isinstance(ref_row, dict) else None
        r['rnsc_codes'] = ref_row.get("rnsc_codes") if isinstance(ref_row, dict) else None
        r['cage_status_codes'] = ref_row.get("cage_status_codes") if isinstance(ref_row, dict) else None
        r['supplier_status'] = ref_row.get("supplier_status") if isinstance(ref_row, dict) else None
        r['supplier_status_detail'] = ref_row.get("supplier_status_detail") if isinstance(ref_row, dict) else None
        r['total_revenue'] = float(r.get('total_revenue') or 0.0)
        r['contracts'] = int(r.get('contracts') or 0)

        out.append(r)

    # Append reference sources that have 0 sales in the filtered time window.
    for c, ref_row in approved_map.items():
        if c not in sales_cages and bool(ref_row.get("is_active_authorized_source", False)):
            ref_vendor = ref_row.get("vendor") if isinstance(ref_row, dict) else None

            out.append({
                "cage": c,
                "vendor": resolve_vendor_name(c, ref_vendor),
                "contracts": 0,
                "last_sold": None,
                "total_revenue": 0.0,
                "is_approved_source": True,
                "is_procurement_authorized": True,
                "is_active_authorized_source": True,
                "part_numbers": ref_row.get("part_numbers", "—") if isinstance(ref_row, dict) else "—",
                "reference_source": ref_row.get("source") if isinstance(ref_row, dict) else None,
                "rncc_codes": ref_row.get("rncc_codes") if isinstance(ref_row, dict) else None,
                "rnvc_codes": ref_row.get("rnvc_codes") if isinstance(ref_row, dict) else None,
                "rnsc_codes": ref_row.get("rnsc_codes") if isinstance(ref_row, dict) else None,
                "cage_status_codes": ref_row.get("cage_status_codes") if isinstance(ref_row, dict) else None,
                "supplier_status": ref_row.get("supplier_status") if isinstance(ref_row, dict) else None,
                "supplier_status_detail": ref_row.get("supplier_status_detail") if isinstance(ref_row, dict) else None,
            })

    # Sort by Revenue, then Authorization/reference status.
    out.sort(key=lambda x: (x['total_revenue'], x['is_approved_source']), reverse=True)
    return out

@app.get("/api/nsn/platforms")
def get_nsn_platforms(
    nsn: str,
    years: Optional[List[int]] = Query(None),
    agency: Optional[str] = None,
    domain: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None
):
    safe_niin = get_niin(nsn).zfill(9)
    where_parts = ["LPAD(TRIM(CAST(niin AS VARCHAR)), 9, '0') = ?"]
    params: List[Any] = [safe_niin]

    ys = safe_years(years, min_year=2019, max_year=2200, max_len=50)
    if ys:
        placeholders = ",".join(["?"] * len(ys))
        where_parts.append(f"year IN ({placeholders})")
        params.extend(ys)
    if agency:
        where_parts.append("(UPPER(sub_agency) = ? OR UPPER(parent_agency) = ?)")
        clean_agency = sanitize(agency).upper()
        params.extend([clean_agency, clean_agency])
    if domain:
        where_parts.append("UPPER(COALESCE(market_segment, '')) = ?")
        params.append(sanitize(domain).upper())
    if psc:
        where_parts.append("UPPER(COALESCE(psc, '')) = ?")
        params.append(sanitize(psc).upper())

    query = f"""
        SELECT
            MAX(NULLIF(TRIM(CAST(platform_families AS VARCHAR)), '')) AS platform_families,
            MAX(TRY_CAST(platform_count AS INTEGER)) AS platform_count,
            MAX(NULLIF(TRIM(CAST(platform_attribution_status AS VARCHAR)), '')) AS platform_attribution_status,
            SUM(TRY_CAST(spend_amount AS DOUBLE)) AS observed_value,
            SUM(TRY_CAST(contracts AS BIGINT)) AS contracts
        FROM v_nsn_summary
        WHERE {' AND '.join(where_parts)}
    """

    try:
        df = duck_fetch_df(query, params)
        if df.empty:
            return []

        row = df.iloc[0]
        raw_platforms = str(row.get("platform_families") or "").strip()
        platforms = [value.strip() for value in raw_platforms.split("|") if value.strip()]
        if platform:
            selected_platform = sanitize(platform).upper()
            platforms = [value for value in platforms if value.upper() == selected_platform]

        platform_count = int(row.get("platform_count") or len(platforms) or 0)
        is_shared = platform_count > 1
        observed_value = float(row.get("observed_value") or 0.0)
        contracts = int(row.get("contracts") or 0)

        return [
            {
                "platform": value,
                "spend": None if is_shared else observed_value,
                "associated_observed_value": observed_value,
                "contracts": contracts,
                "platform_count": platform_count,
                "platform_attribution_status": row.get("platform_attribution_status") or "UNMAPPED",
                "shared_platform_exposure": is_shared,
            }
            for value in platforms[:20]
        ]
    except Exception as e:
        logger.error(f"Error in NSN platform associations: {e}")
        return []


@app.get("/api/nsn/history")
def get_nsn_history(
    nsn: str,
    years: Optional[List[int]] = Query(None),
    agency: Optional[str] = None,
    domain: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None
):
    safe_niin = get_niin(nsn)

    # ✅ Hit the fast DuckDB summary view!
    where_parts = [f"niin = {sql_literal(safe_niin)}", "year IS NOT NULL"]

    ys = safe_years(years, min_year=1900, max_year=2200, max_len=50)
    if ys:
        years_csv = ",".join([str(y) for y in ys])
        where_parts.append(f"year IN ({years_csv})")
        
    if agency:
        where_parts.append(f"(upper(sub_agency) = {sql_literal(sanitize(agency).upper())} OR upper(parent_agency) = {sql_literal(sanitize(agency).upper())})")
    if domain:
        where_parts.append(f"upper(market_segment) = {sql_literal(sanitize(domain).upper())}")
    if platform:
        where_parts.append(f"upper(platform_family) = {sql_literal(sanitize(platform).upper())}")
    if psc:
        where_parts.append(f"upper(psc) = {sql_literal(sanitize(psc).upper())}")

    where_clause = " AND ".join(where_parts)

    query = f"""
        SELECT 
            cast(year as varchar) as label,
            SUM(spend_amount) as spend
        FROM v_nsn_summary
        WHERE {where_clause}
        GROUP BY 1
        ORDER BY label ASC
    """
    
    try:
        df = duck_fetch_df(query)
        if df.empty:
            return []

        results = df.to_dict(orient="records")
        
        # Chart formatting: gap-fill missing fiscal years
        fy_map = {int(r["label"]): float(r["spend"]) for r in results}
        min_fy = min(fy_map.keys())
        max_fy = max(fy_map.keys())

        out = []
        for fy in range(min_fy, max_fy + 1):
            out.append({
                "label": f"{fy}",
                "spend": float(fy_map.get(fy, 0.0))
            })

        return out

    except Exception as e:
        logger.error(f"Error in NSN history DuckDB: {e}")
        return []


@app.get("/api/nsn/contracts")
def get_nsn_contracts(
    nsn: str,
    limit: int = 50,
    offset: int = 0,
    years: Optional[List[int]] = Query(None),
    vendor: Optional[str] = None,
    parent: Optional[str] = None,
    cage: Optional[str] = None,
    domain: Optional[str] = None,
    agency: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None,
):
    safe_niin = get_niin(nsn)
    limit_i = safe_int(limit, 50, 1, 200)
    offset_i = safe_int(offset, 0, 0, 2_000_000)

    # ✅ ROUTED TO DUCKDB
    where_parts = ["niin = ?"]
    params = [safe_niin]

    ys = safe_years(years, min_year=1900, max_year=2200, max_len=50)
    if ys:
        years_csv = ",".join([str(y) for y in ys])
        where_parts.append(f"year IN ({years_csv})")

    if vendor:
        where_parts.append("upper(coalesce(vendor_name,'')) LIKE ?")
        params.append(f"%{sanitize(vendor).upper()}%")
    if cage:
        where_parts.append("trim(upper(coalesce(vendor_cage,''))) = ?")
        params.append(sanitize(cage).upper())
    if domain:
        where_parts.append("upper(coalesce(market_segment,'')) LIKE ?")
        params.append(f"%{sanitize(domain).upper()}%")
    if platform:
        where_parts.append("upper(coalesce(platform_family,'')) LIKE ?")
        params.append(f"%{sanitize(platform).upper()}%")
    if psc:
        where_parts.append("trim(upper(coalesce(psc,''))) = ?")
        params.append(sanitize(psc).upper())
    if agency:
        where_parts.append("(upper(coalesce(parent_agency,'')) LIKE ? OR upper(coalesce(sub_agency,'')) LIKE ?)")
        safe_ag = f"%{sanitize(agency).upper()}%"
        params.extend([safe_ag, safe_ag])

    where_sql = " AND ".join(where_parts)

    query = f"""
    WITH supplier_rows AS (
        SELECT
            niin,
            year,
            contract_id,
            CAST(last_sold AS VARCHAR) AS action_date,
            parent_agency,
            sub_agency,
            vendor AS vendor_name,
            cage AS vendor_cage,
            market_segment,
            platform_family,
            psc,
            CAST(total_revenue AS DOUBLE) AS spend_amount
        FROM v_nsn_supplier_lookup
    ),
    rolled AS (
        SELECT
            contract_id,
            MAX(action_date) AS action_date,
            MAX(COALESCE(sub_agency, parent_agency)) AS agency,
            MAX(vendor_name) AS vendor_name,
            MAX(vendor_cage) AS vendor_cage,
            MAX(NULLIF(platform_family, '')) AS platform_family,
            MAX(NULLIF(psc, '')) AS psc,
            SUM(spend_amount) AS spend_amount
        FROM supplier_rows
        WHERE {where_sql}
          AND spend_amount IS NOT NULL
        GROUP BY contract_id
    ),
    metadata AS (
        SELECT
            contract_id,
            MAX_BY(
                CASE WHEN CAST(description AS VARCHAR) IN ('NAN', 'NONE', '') THEN NULL ELSE description END,
                action_date
            ) AS description,
            MAX_BY(
                CASE WHEN CAST(naics_code AS VARCHAR) IN ('NAN', 'NONE', '') THEN NULL ELSE CAST(naics_code AS VARCHAR) END,
                action_date
            ) AS naics_code,
            MAX_BY(NULLIF(psc, ''), action_date) AS psc
        FROM v_transactions
        WHERE niin = ?
        GROUP BY contract_id
    )
    SELECT
        r.contract_id,
        r.action_date,
        r.agency,
        r.vendor_name,
        r.vendor_cage,
        r.platform_family,
        COALESCE(r.psc, m.psc) AS psc,
        m.naics_code,
        r.spend_amount,
        m.description
    FROM rolled r
    LEFT JOIN metadata m ON r.contract_id = m.contract_id
    ORDER BY r.action_date DESC
    OFFSET {offset_i}
    LIMIT {limit_i}
    """
    
    try:
        df = duck_fetch_df(query, params + [safe_niin])
        df = df_sanitize_for_json(df)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"NSN Contracts DuckDB Error: {e}")
        return []


@app.get("/api/public/nsn/teaser")
def get_public_nsn_teaser(
    request: Request,
    response: Response,
    nsn: str,
):
    """A deliberately limited public preview of the full NSN workspace."""
    clean = re.sub(r"[^0-9]", "", str(nsn or ""))
    if len(clean) not in {8, 9, 13}:
        raise HTTPException(status_code=400, detail="Enter a valid 13-digit NSN or 9-digit NIIN.")

    remaining_lookups = check_nsn_rate_limit(request)
    safe_niin = clean.zfill(9) if len(clean) < 9 else clean[-9:]
    input_fsc = clean[:4] if len(clean) == 13 else ""
    if remaining_lookups is not None:
        response.headers["X-RateLimit-Remaining"] = str(remaining_lookups)

    profile = get_nsn_profile(
        nsn=clean,
        years=None,
        agency=None,
        domain=None,
        platform=None,
        psc=None,
    )
    if not profile or not profile.get("found"):
        return {
            "found": False,
            "message": "NSN or NIIN not found in the Mimir reference and procurement data.",
            "remaining_lookups": remaining_lookups,
        }

    ref_profile = nsn_ref_profile_lookup(safe_niin)
    supplier_map = nsn_ref_supplier_lookup(safe_niin)

    fsc_code = str(profile.get("fsc_code") or ref_profile.get("fsc_code") or input_fsc or "").strip()
    if input_fsc and re.fullmatch(r"[0-9]{4}", fsc_code) and input_fsc != fsc_code:
        return {
            "found": False,
            "message": "NSN not found in the Mimir reference and procurement data.",
            "remaining_lookups": remaining_lookups,
        }

    has_observed_activity = int(profile.get("total_contracts") or 0) > 0
    full_nsn_digits = ""
    if len(clean) == 13:
        full_nsn_digits = clean
    elif has_observed_activity and re.fullmatch(r"[0-9]{4}", fsc_code):
        full_nsn_digits = f"{fsc_code}{safe_niin}"
    formatted_nsn = (
        f"{full_nsn_digits[:4]}-{full_nsn_digits[4:6]}-{full_nsn_digits[6:9]}-{full_nsn_digits[9:]}"
        if len(full_nsn_digits) == 13
        else None
    )

    supplier_count = max(
        int(ref_profile.get("reference_supplier_count") or 0),
        len(supplier_map),
    )
    part_number_count = int(ref_profile.get("reference_part_count") or 0)

    approved_candidates = []
    for cage, supplier in supplier_map.items():
        if not supplier.get("is_active_authorized_source"):
            continue
        vendor = str(supplier.get("vendor") or "").strip()
        approved_candidates.append(
            {
                "cage": cage,
                "vendor": vendor if vendor and vendor.upper() not in {"NAN", "NONE", "NULL"} else f"CAGE {cage}",
                "part_number": next(
                    (
                        value.strip()
                        for value in str(supplier.get("part_numbers") or "").split(",")
                        if value.strip() and value.strip() != "—"
                    ),
                    None,
                ),
            }
        )

    approved_candidates.sort(key=lambda item: (item["vendor"].startswith("CAGE "), item["vendor"]))
    approved_source = approved_candidates[0] if approved_candidates else None

    platform_rows = get_nsn_platforms(
        nsn=clean,
        years=None,
        agency=None,
        domain=None,
        platform=None,
        psc=None,
    )
    platform_names = []
    for row in platform_rows:
        platform_name = str(row.get("platform") or "").strip()
        if platform_name and platform_name not in platform_names:
            platform_names.append(platform_name)

    platform_count = max(
        [int(row.get("platform_count") or 0) for row in platform_rows] + [len(platform_names)]
    )

    recent_contracts = []
    observed_contract_count = 0
    try:
        activity_df = duck_fetch_df(
            """
            WITH rolled AS (
                SELECT
                    contract_id,
                    MAX(CAST(last_sold AS VARCHAR)) AS action_date,
                    MAX(COALESCE(sub_agency, parent_agency)) AS agency,
                    MAX(vendor) AS vendor_name,
                    MAX(cage) AS vendor_cage,
                    SUM(TRY_CAST(total_revenue AS DOUBLE)) AS observed_value
                FROM v_nsn_supplier_lookup
                WHERE niin = ?
                  AND contract_id IS NOT NULL
                  AND TRIM(CAST(contract_id AS VARCHAR)) <> ''
                GROUP BY contract_id
            )
            SELECT
                contract_id,
                action_date,
                agency,
                vendor_name,
                vendor_cage,
                observed_value,
                COUNT(*) OVER () AS observed_contract_count
            FROM rolled
            ORDER BY action_date DESC NULLS LAST, contract_id
            LIMIT 3
            """,
            [safe_niin],
        )

        if not activity_df.empty:
            observed_contract_count = int(activity_df.iloc[0].get("observed_contract_count") or 0)
            for row in activity_df.to_dict(orient="records"):
                recent_contracts.append(
                    {
                        "contract_id": _clean_optional_value(row.get("contract_id")),
                        "action_date": _clean_optional_value(row.get("action_date")),
                        "agency": _clean_optional_value(row.get("agency")),
                        "vendor_name": _clean_optional_value(row.get("vendor_name")),
                        "vendor_cage": _clean_optional_value(row.get("vendor_cage")),
                        "observed_value": float(row.get("observed_value") or 0.0),
                    }
                )
    except Exception:
        logger.exception("Public NSN activity lookup failed for NIIN=%s", safe_niin)

    return {
        "found": True,
        "item_name": profile.get("item_name") or ref_profile.get("item_name") or "Unknown item",
        "nsn": formatted_nsn,
        "niin": safe_niin,
        "fsc_code": fsc_code or None,
        "associated_part_number_count": part_number_count,
        "associated_supplier_site_count": supplier_count,
        "approved_source": approved_source,
        "approved_sources_hidden": max(0, len(approved_candidates) - (1 if approved_source else 0)),
        "platforms": platform_names[:2],
        "platforms_hidden": max(0, platform_count - min(2, len(platform_names))),
        "is_multi_platform": platform_count > 1,
        "recent_contracts": recent_contracts,
        "observed_contract_count": observed_contract_count,
        "contracts_hidden": max(0, observed_contract_count - len(recent_contracts)),
        "remaining_lookups": remaining_lookups,
    }

DEFAULT_TOP_NSN_CACHE = None  # Put this at the top of your file
# ✅ NEW: Cache to store the default dashboard state in memory
DEFAULT_TOP_NSN_CACHE = None

# ✅ NEW: Top NSNs by Spend (respects global filters)
# NOTE: This is *not* "related parts for this NSN". It's a global leaderboard of NSNs under the current filter context.
@app.get("/api/nsn/top")
def get_top_nsns(
    years: Optional[List[int]] = Query(None),
    vendor: Optional[str] = None,
    parent: Optional[str] = None,
    cage: Optional[str] = None,
    domain: Optional[str] = None,
    agency: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None,
    limit: int = 12,
):
    global DEFAULT_TOP_NSN_CACHE
    limit_i = safe_int(limit, 12, 4, 50)
    
    # ✅ QUICK EXIT: Instant cache load for the default dashboard
    is_default_load = (
        not vendor and not parent and not cage and 
        not domain and not agency and not platform and not psc and
        (not years or len(years) >= 7) and 
        limit_i in (12, 16)
    )

    if is_default_load and DEFAULT_TOP_NSN_CACHE is not None:
        return DEFAULT_TOP_NSN_CACHE

    # Keep the landing grid local/parquet-backed. This endpoint is loaded every
    # time the NSN dashboard opens, so it must not depend on a live Athena scan.
    use_supplier_lookup = bool(vendor or cage or parent)
    source_table = "v_nsn_supplier_lookup" if use_supplier_lookup else "v_nsn_summary"
    spend_col = "total_revenue" if use_supplier_lookup else "spend_amount"
    contracts_expr = "COUNT(DISTINCT contract_id)" if use_supplier_lookup else "SUM(contracts)"

    cond: List[str] = ["1=1"]
    params: List[Any] = []

    def add_contains(col: str, value: Optional[str]):
        if value:
            cond.append(f"UPPER(CAST({col} AS VARCHAR)) LIKE ?")
            params.append(f"%{str(value).strip().upper()}%")

    ys = safe_years(years, min_year=1900, max_year=2200, max_len=50)
    if ys:
        cond.append(f"year IN ({','.join(['?'] * len(ys))})")
        params.extend(ys)

    if use_supplier_lookup:
        add_contains("vendor", vendor or parent)
        add_contains("cage", cage)
    if agency:
        cond.append("(UPPER(CAST(sub_agency AS VARCHAR)) LIKE ? OR UPPER(CAST(parent_agency AS VARCHAR)) LIKE ?)")
        params.extend([f"%{agency.strip().upper()}%", f"%{agency.strip().upper()}%"])
    add_contains("platform_family", platform)
    add_contains("psc", psc)
    if domain:
        cond.append("(UPPER(CAST(market_segment AS VARCHAR)) LIKE ? OR UPPER(CAST(psc AS VARCHAR)) LIKE ?)")
        params.extend([f"%{domain.strip().upper()}%", f"%{domain.strip().upper()}%"])

    where_clause = " AND ".join(cond)

    query = f"""
    WITH top_spending AS (
        SELECT
            LPAD(CAST(niin AS VARCHAR), 9, '0') AS join_niin,
            SUM(COALESCE({spend_col}, 0)) AS spend,
            {contracts_expr} AS contracts
        FROM {source_table}
        WHERE {where_clause}
          AND niin IS NOT NULL
          AND TRIM(CAST(niin AS VARCHAR)) <> ''
        GROUP BY 1
        ORDER BY spend DESC
        LIMIT {limit_i}
    ),
    profile_match AS (
        SELECT 
            LPAD(CAST(niin AS VARCHAR), 9, '0') AS niin,
            MAX(NULLIF(CAST(nsn AS VARCHAR), '')) AS nsn,
            MAX(NULLIF(CAST(item_name AS VARCHAR), '')) AS item_name,
            MAX(NULLIF(CAST(fsc_code AS VARCHAR), '')) AS fsc_code
        FROM v_nsn_profile_lookup
        WHERE LPAD(CAST(niin AS VARCHAR), 9, '0') IN (SELECT join_niin FROM top_spending)
        GROUP BY 1
    )
    SELECT
        CASE 
            WHEN p.nsn IS NOT NULL THEN p.nsn
            WHEN p.fsc_code IS NOT NULL THEN p.fsc_code || '-' || t.join_niin
            ELSE t.join_niin 
        END AS nsn,
        COALESCE(p.item_name, 'Unknown Item') AS description,
        t.spend,
        t.contracts
    FROM top_spending t
    LEFT JOIN profile_match p ON t.join_niin = p.niin
    ORDER BY t.spend DESC
    """

    try:
        df = duck_fetch_df(query, params)
        result = df_sanitize_for_json(df).to_dict(orient="records") if not df.empty else []
    except Exception:
        logger.exception("NSN top lookup failed via DuckDB")
        result = []
    
    # ✅ SAVE TO CACHE IF DEFAULT LOAD
    if is_default_load and result:
        DEFAULT_TOP_NSN_CACHE = result
        
    return result



# ==========================================
#        AWARD / CONTRACT DASHBOARD
# ==========================================



# --- UPDATE THIS FUNCTION ---
@app.get("/api/award/profile")
def get_award_profile(id: str):
    if not id:
        return None

    safe_id = sanitize(id)

    try:
        rolled_cols = get_duck_table_columns("v_contracts_rolled")
        description_expr = (
            "COALESCE(NULLIF(TRIM(base_award_description), ''), description)"
            if "base_award_description" in rolled_cols
            else "description"
        )

        # ✅ FIX: Instantly grab the complete 7-year profile from the rolled view
        df = duck_fetch_df(f"""
            SELECT
                contract_id,
                vendor_name,
                vendor_cage,
                sub_agency,
                parent_agency,
                {description_expr} AS description,
                platform_family,
                city,
                state,
                country,
                naics_code,
                psc,
                pricing_type,
                competition_type,
                offers_count,
                set_aside_type,
                solicitation_id,
                total_spend,
                start_date,
                last_action_date
            FROM v_contracts_rolled
            WHERE contract_id = ?
            LIMIT 1
        """, [safe_id])
        if df.empty:
            return None
        
        # Convert safely, handling potential NaNs
        df = df_sanitize_for_json(df)
        r = df.to_dict(orient="records")[0]

        sub_ag = r.get("sub_agency")
        parent_ag = r.get("parent_agency")
        agency = sub_ag if pd.notna(sub_ag) and str(sub_ag).strip() else parent_ag

        # Exact match to your original business logic return
        return {
            "contract_id": r.get("contract_id"),
            "vendor_name": r.get("vendor_name"),
            "vendor_cage": r.get("vendor_cage"),
            "agency": agency,
            "sub_agency": sub_ag,
            "description": r.get("description"),

            "platform_family": r.get("platform_family"),
            "city": r.get("city"),
            "state": r.get("state"),
            "country": r.get("country"),

            "naics_code": r.get("naics_code"),
            "psc": r.get("psc"),

            "pricing_type": r.get("pricing_type"),
            "competition_type": r.get("competition_type"),
            "offers_count": r.get("offers_count"),
            "set_aside_type": r.get("set_aside_type"),
            "solicitation_id": r.get("solicitation_id"),

            "total_spend": float(r.get("total_spend") or 0),
            "start_date": str(r.get("start_date")) if pd.notna(r.get("start_date")) else None,
            "last_action_date": str(r.get("last_action_date")) if pd.notna(r.get("last_action_date")) else None
        }

    except Exception as e:
        logger.error(f"Profile API Error: {e}")
        return None


# --- ADD THIS NEW FUNCTION ---
@app.get("/api/award/solicitation")
def get_solicitation_lookup(sol_num: str):
    if not sol_num:
        return None

    raw = str(sol_num).strip()
    safe_a = safe_ident(raw)  # strict identifier chars
    safe_b = safe_ident(raw.replace("-", "")) if raw.replace("-", "") else safe_a

    query = f"""
    SELECT 
        sol_num,
        title,
        url,
        deadline,
        agency
    FROM "market_intel_gold"."view_unified_opportunities_dod"
    WHERE upper(sol_num) = {sql_literal(safe_a)}
       OR upper(sol_num) = {sql_literal(safe_b)}
    LIMIT 1
    """
    results = run_athena_query(query)
    return results[0] if results else None

@app.get("/api/award/history")
def get_award_history(id: str):
    if not id:
        return []

    safe_id = sanitize(id)

    df = duck_fetch_df("""
        SELECT
            action_date,
            spend_amount,
            COALESCE(NULLIF(TRIM(action_description), ''), description) AS description
        FROM v_transactions
        WHERE contract_id = ?
        ORDER BY action_date ASC
        LIMIT 200
    """, [safe_id])

    if df.empty:
        return []

    if "spend_amount" in df.columns:
        df["spend_amount"] = pd.to_numeric(df["spend_amount"], errors="coerce").fillna(0)
    else:
        df["spend_amount"] = 0

    out = []
    for r in df.itertuples(index=False):
        # robust field access
        action_date = getattr(r, "action_date", None)
        spend_amount = getattr(r, "spend_amount", 0)
        description = getattr(r, "description", None)

        out.append({
            "action_date": str(action_date) if action_date is not None else None,
            "spend_amount": float(spend_amount or 0),
            "description": description
        })

    return out


# Place this right above your endpoint
DEFAULT_AWARD_PAGE_CACHE = None

@app.get("/api/award/search")
def search_awards(
    q: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
    years: Optional[List[int]] = Query(None),
    vendor: Optional[str] = None,
    cage: Optional[str] = None,  
    agency: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None,
    domain: Optional[str] = None,
    min_spend: Optional[float] = None, 
    # ✅ NEW: Accept Keyset Pagination Params
    after_spend: Optional[float] = None,
    after_date: Optional[str] = None,
    after_id: Optional[str] = None
):
    global DEFAULT_AWARD_PAGE_CACHE
    
    limit_i = int(limit or 20)
    offset_i = int(offset or 0)

    # ✅ QUICK EXIT: Is this the default dashboard load?
    # Ensure after_spend is None so we don't accidentally cache a "Load More" page
    is_default_load = (
        not q and not vendor and not cage and not agency and
        not platform and not psc and not domain and
        min_spend is None and
        (not years or len(years) >= 7) and   # <--- THIS IS THE FIX
        offset_i == 0 and limit_i == 20 and
        after_spend is None 
    )

    if is_default_load and DEFAULT_AWARD_PAGE_CACHE is not None:
        return DEFAULT_AWARD_PAGE_CACHE

    cond: List[str] = ["1=1"]

    # --- 1. APPLY SEARCH & FILTERS ---
    if q and str(q).strip():
        v = sanitize(q)
        like_v = sql_like_contains(v)
        cond.append(
            "("
            f" upper(coalesce(contract_id,'')) LIKE {like_v} ESCAPE '#' OR"
            f" upper(coalesce(vendor_name,'')) LIKE {like_v} ESCAPE '#' OR"
            f" upper(coalesce(description,'')) LIKE {like_v} ESCAPE '#' OR"
            f" upper(coalesce(vendor_cage,'')) LIKE {like_v} ESCAPE '#'"
            ")"
        )

    if vendor and str(vendor).strip():
        cond.append(f"upper(vendor_name) LIKE {sql_like_contains(sanitize(vendor))} ESCAPE '#'")

    # ✅ NEW — CAGE FILTER (PUT IT HERE)
    if cage and str(cage).strip():
        c = sanitize(cage)
        cond.append(f"upper(coalesce(vendor_cage,'')) = {sql_literal(c)}")

    # ✅ NEW — MIN SPEND FILTER (PUT IT HERE)
    if min_spend is not None:
        try:
            ms = float(min_spend)
            if ms >= 0:
                cond.append(f"total_spend >= {ms}")
        except Exception:
            pass

    if platform and str(platform).strip():
        cond.append(f"upper(platform_family) = {sql_literal(sanitize(platform))}")

    if agency and str(agency).strip():
        a = sanitize(agency)
        cond.append(f"(upper(sub_agency) = {sql_literal(a)} OR upper(parent_agency) = {sql_literal(a)})")

    if psc and str(psc).strip():
        cond.append(f"upper(psc) LIKE {sql_like_contains(sanitize(psc))} ESCAPE '#'")

    if domain and str(domain).strip():
        d = sanitize(domain)
        like_d = sql_like_contains(d)
        cond.append(
            "("
            f" upper(coalesce(market_segment,'')) LIKE {like_d} ESCAPE '#' OR"
            f" upper(coalesce(tech_type,'')) LIKE {like_d} ESCAPE '#' OR"
            f" upper(coalesce(capability_name,'')) LIKE {like_d} ESCAPE '#'"
            ")"
        )

    ys = safe_years(years, min_year=1900, max_year=2200, max_len=50)
    if ys:
        years_csv = ",".join([str(y) for y in ys])
        cond.append(f"year IN ({years_csv})")

    # --- 2. APPLY KEYSET PAGINATION ---
    # ✅ This replaces OFFSET for subsequent pages, saving massive RAM and CPU
    if after_spend is not None and after_date and after_id:
        s_val = float(after_spend)
        d_val = sql_literal(after_date)
        i_val = sql_literal(after_id)
        
        cond.append(f"""
            (
                total_spend < {s_val}
                OR (total_spend = {s_val} AND last_action_date < {d_val})
                OR (total_spend = {s_val} AND last_action_date = {d_val} AND contract_id < {i_val})
            )
        """)

    # --- 3. APPLY DEFAULT HOMEPAGE RULES ---
    is_specific_search = bool(
        (q and str(q).strip()) or
        (vendor and str(vendor).strip()) or
        (cage and str(cage).strip()) or
        (agency and str(agency).strip()) or
        (platform and str(platform).strip()) or
        (psc and str(psc).strip()) or
        (domain and str(domain).strip()) or
        (min_spend is not None)
    )

    if not is_specific_search:
        cond.append("total_spend >= 1000000")
        if not ys:
            cond.append("try_cast(last_action_date as date) >= current_date() - INTERVAL 730 DAY")

    where_clause = " AND ".join(cond)

    # If we have keyset parameters, we explicitly omit the OFFSET clause in SQL
    offset_sql = f"OFFSET {offset_i}" if after_spend is None else ""

    # ✅ UPDATED: Added contract_id DESC to guarantee deterministic sorting
    data_query = f"""
        SELECT
            contract_id,
            last_action_date,
            total_spend,
            vendor_name,
            vendor_cage,
            sub_agency,
            parent_agency,
            description
        FROM v_contracts_rolled
        WHERE {where_clause}
        ORDER BY total_spend DESC, last_action_date DESC, contract_id DESC
        {offset_sql}
        LIMIT {limit_i + 1}
    """

    try:
        df = duck_fetch_df(data_query)
        df = df_sanitize_for_json(df)
        rows = df.to_dict(orient="records")

        has_more = len(rows) > limit_i
        rows = rows[:limit_i]

        data = []
        for r in rows:
            final_ag = r.get("sub_agency") or r.get("parent_agency")
            data.append({
                "contract_id": r.get("contract_id"),
                "vendor_name": r.get("vendor_name"),
                "vendor_cage": r.get("vendor_cage"),
                "last_action_date": str(r.get("last_action_date")) if r.get("last_action_date") else None,
                "description": r.get("description"),
                "total_spend": float(r.get("total_spend") or 0),
                "agency": final_ag,
                "sub_agency": r.get("sub_agency"),
                "parent_agency": r.get("parent_agency"),
            })

        final_response = {"data": data, "total": None, "offset": offset_i, "limit": limit_i, "has_more": has_more}

        # Save to cache if this is the initial 18-second dashboard load
        if is_default_load:
            DEFAULT_AWARD_PAGE_CACHE = final_response

        return final_response

    except Exception as e:
        logger.error(f"Award Search DuckDB Error: {e}")
        return {"data": [], "total": 0, "offset": offset_i, "limit": limit_i}


@app.get("/api/award/stats")
def get_database_stats():
    """
    Memory-safe row count using Parquet metadata.
    Avoids DuckDB COUNT(*) over many files.
    """
    try:
        import glob
        import pyarrow.parquet as pq

        single_path = (LOCAL_CACHE_DIR / "contracts_rolled.parquet").resolve()
        folder_glob = (LOCAL_CACHE_DIR / "contracts_rolled" / "*.parquet").resolve()

        total_rows = 0

        # Case A: single consolidated file
        if os.path.exists(single_path):
            pf = pq.ParquetFile(str(single_path))
            total_rows = int(pf.metadata.num_rows)

        # Case B: multi-part dataset (your current setup)
        else:
            for p in glob.glob(str(folder_glob)):
                try:
                    pf = pq.ParquetFile(p)
                    total_rows += int(pf.metadata.num_rows)
                except Exception:
                    pass

        return [{"total": int(total_rows)}]

    except Exception:
        logger.exception("Stats Parquet Metadata Error")
        return [{"total": 0}]


# ==========================================
#        PIPELINE / OPPORTUNITIES
# ==========================================

@app.get("/api/pipeline/live")
def get_pipeline_live(
    naics: Optional[str] = None, 
    set_aside: Optional[str] = None, 
    state: Optional[str] = None,
    source: Optional[str] = None,
    platform: Optional[str] = None,
    agency: Optional[str] = None,
    domain: Optional[str] = None,
    keyword: Optional[str] = None,
    opp_type: Optional[str] = None, # ✅ NEW: Route DIBBS vs SAM
    years: Optional[List[int]] = Query(None), 
    psc: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    conds = ["try_cast(deadline as date) >= current_date()"]
    params = []
    
    # Matches original data quality filter: Accept rows with EITHER a Sol Num OR a Notice ID
    conds.append("(NULLIF(TRIM(sol_num), '') IS NOT NULL OR NULLIF(TRIM(id), '') IS NOT NULL)")
    
    # ✅ Route DIBBS vs SAM
    if opp_type == "DIBBS":
        conds.append("(upper(COALESCE(sol_num, id)) LIKE 'SPE%' OR upper(COALESCE(sol_num, id)) LIKE 'SPM%' OR upper(COALESCE(sol_num, id)) LIKE 'SPR%')")
    elif opp_type == "SAM":
        conds.append("(upper(COALESCE(sol_num, id)) NOT LIKE 'SPE%' AND upper(COALESCE(sol_num, id)) NOT LIKE 'SPM%' AND upper(COALESCE(sol_num, id)) NOT LIKE 'SPR%')")
        
    if naics:
        conds.append("naics LIKE ?")
        params.append(f"%{sanitize(naics)}%")
    if set_aside:
        conds.append("upper(set_aside_type) LIKE ?")
        params.append(f"%{sanitize(set_aside).upper()}%")
    if state:
        conds.append("state = ?")
        params.append(sanitize(state))
    if source:
        conds.append("source_system = ?")
        params.append(sanitize(source))
    
    # Matches original keyword logic across search_text, title, and description
    if keyword:
        safe_k = f"%{sanitize(keyword).upper()}%"
        conds.append("(upper(search_text) LIKE ? OR upper(title) LIKE ? OR upper(description) LIKE ?)")
        params.extend([safe_k, safe_k, safe_k])
        
    # Matches original DLA/LOGISTICS specific fallback logic
    if agency:
        safe_ag = f"%{sanitize(agency).upper()}%"
        if "DLA" in safe_ag or "LOGISTICS" in safe_ag:
            conds.append("(upper(agency) LIKE '%LOGISTICS%' OR upper(sub_agency) LIKE '%LOGISTICS%' OR source_system = 'DLA')")
        else:
            conds.append("(upper(agency) LIKE ? OR upper(sub_agency) LIKE ?)")
            params.extend([safe_ag, safe_ag])
            
    if platform:
        safe_plat = f"%{sanitize(platform).upper()}%"
        conds.append("(upper(title) LIKE ? OR upper(description) LIKE ?)")
        params.extend([safe_plat, safe_plat])
        
    if domain:
        safe_domain = sanitize(domain).upper()
        if len(safe_domain) > 0 and (safe_domain[0].isdigit() or (len(safe_domain) == 4 and safe_domain[0].isalpha())):
            conds.append("(psc LIKE ? OR naics LIKE ?)")
            params.extend([f"{safe_domain}%", f"{safe_domain}%"])
        else:
            safe_domain_like = f"%{safe_domain}%"
            conds.append("(upper(title) LIKE ? OR upper(description) LIKE ?)")
            params.extend([safe_domain_like, safe_domain_like])
            
    where_sql = " AND ".join(conds)
    
    count_query = f"SELECT count(*) as c FROM v_opportunities WHERE {where_sql}"
    total_matches = 0
    try:
        total_matches = duck_fetch_df(count_query, params)['c'].iloc[0]
    except:
        pass
        
    query = f"""
        SELECT * FROM v_opportunities 
        WHERE {where_sql}
        ORDER BY try_cast(deadline as date) ASC
        LIMIT {int(limit)} OFFSET {int(offset)}
    """
    
    df = duck_fetch_df(query, params)
    if df.empty: return []
    
    # ✅ FIX 1: Clean DuckDB NaNs into JSON-safe Nones
    df = df_sanitize_for_json(df)
    
    results = []
    today = date.today()
    for row in df.itertuples():
        
        # Matches original Date Logic and race condition safety precisely
        days_left = 0
        try:
            dt_str = str(row.deadline)[:10]
            dt_obj = datetime.strptime(dt_str, "%Y-%m-%d").date()
            days_left = (dt_obj - today).days
        except:
            pass
            
        if days_left < 0: continue
        
        # Matches original Smart Identifier logic precisely
        display_sol = getattr(row, 'sol_num', '')
        if not display_sol or str(display_sol) == 'nan':
            display_sol = getattr(row, 'id', '')
            
        results.append({
            "id": getattr(row, 'id', ''),
            "title": getattr(row, 'title', ''),
            "agency": getattr(row, 'agency', ''),
            "sub_agency": getattr(row, 'sub_agency', ''),
            "sol_num": display_sol,
            "due_date": getattr(row, 'deadline', ''),
            "deadline": getattr(row, 'deadline', ''),
            "set_aside": getattr(row, 'set_aside_type', ''),
            "set_aside_type": getattr(row, 'set_aside_type', ''),
            "naics": getattr(row, 'naics', ''),
            "psc": getattr(row, 'psc', ''),
            "description": str(getattr(row, 'description', '') or '')[:2000],
            "primarycontactemail": getattr(row, 'poc_email', ''),
            "source_system": getattr(row, 'source_system', ''),
            "url": getattr(row, 'url', ''), # ✅ Add this line
            "days_left": int(days_left),
            "total_matches": int(total_matches)
        })
    return results

@app.get("/api/pipeline/recent-wins")
def get_recent_wins(
    # ✅ Add global parameters to prevent FastAPI 422 Validation Errors
    agency: Optional[str] = None,
    domain: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None,
    years: Optional[List[int]] = Query(None) # Accepted to prevent crash, but safely ignored below
):
    conds = [
        "try_cast(action_date as date) >= current_date() - INTERVAL 180 DAY",
        "spend_amount > 1000000"
    ]
    params = []

    # ✅ Apply real filters to transactions
    if agency:
        conds.append("(upper(parent_agency) = ? OR upper(sub_agency) = ?)")
        safe_ag = sanitize(agency).upper()
        params.extend([safe_ag, safe_ag])
    if platform:
        conds.append("upper(platform_family) = ?")
        params.append(sanitize(platform).upper())
    if domain:
        conds.append("upper(market_segment) = ?")
        params.append(sanitize(domain).upper())
    if psc:
        conds.append("upper(psc) = ?")
        params.append(sanitize(psc).upper())

    where_sql = " AND ".join(conds)

    query = f"""
    SELECT 
        vendor_name,
        vendor_cage,
        parent_agency as agency,
        contract_id,
        spend_amount as amount,
        action_date as signed_date,
        description
    FROM v_transactions
    WHERE {where_sql}
    ORDER BY action_date DESC
    LIMIT 50
    """
    try:
        df = duck_fetch_df(query, params)
        df = df_sanitize_for_json(df)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Recent Wins DuckDB Error: {e}")
        return []

# --- ADD THIS TO api.py ---

@app.get("/api/pipeline/details")
def get_solicitation_details(id: str):
    if not id:
        return {"found": False}

    safe_id = sanitize(id).upper()

    query = f"""
    SELECT *
    FROM v_opportunities
    WHERE upper(sol_num) = ? OR upper(id) = ?
    LIMIT 1
    """
    try:
        df = duck_fetch_df(query, [safe_id, safe_id])
        if df.empty:
            return {"found": False}

        df = df_sanitize_for_json(df)
        row = df.iloc[0]

        return {
            "id": str(row.get("id", "")),
            "title": str(row.get("title", "")),
            "agency": str(row.get("agency", "")),
            "sub_agency": str(row.get("sub_agency", "")),
            "sol_num": str(row.get("sol_num", "")),
            "noticeid": str(row.get("sol_num", "")), 
            "deadline": str(row.get("deadline", "")),
            "set_aside": str(row.get("set_aside_type", "")),
            "poc_email": str(row.get("poc_email", "")),
            "description": str(row.get("description", "")),
            "state": str(row.get("state", "")),
            "naics": str(row.get("naics", "")),
            "psc": str(row.get("psc", "")),
            "url": str(row.get("url", "")) # ✅ Add this line
        }
    except Exception as e:
        logger.error(f"Details DuckDB Error: {e}")
        return {"found": False}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)
