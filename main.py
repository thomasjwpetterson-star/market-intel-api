from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
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

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("mimir-api")

# ✅ GLOBAL MEMORY STORE (Initialized with empty defaults)
# We will swap this entire dictionary reference atomically.
GLOBAL_CACHE = {
    "df": pd.DataFrame(),
    "geo_df": pd.DataFrame(),
    "profiles_df": pd.DataFrame(),
    "risk_df": pd.DataFrame(),
    "df_opportunities": pd.DataFrame(),
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
# Pool size 1 is safest/lowest RAM; 2 is usually fine.
DUCK_POOL_SIZE = int(os.getenv("DUCK_POOL_SIZE", "1"))
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
    conn.execute(f"PRAGMA memory_limit='{os.getenv('DUCKDB_MEM', '900MB')}';")
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
            return DUCK_CONN.execute(sql, params).fetchdf()

    # Read mode: use pool
    c = None
    try:
        c = _DUCK_POOL.get(timeout=float(os.getenv("DUCK_POOL_TIMEOUT", "5")))
        # Use a cursor for isolation of statement state
        return c.cursor().execute(sql, params).fetchdf()
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
        limiter.total_tokens = int(os.getenv("ANYIO_THREADS", "40"))
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
        if "nan" in str(e).lower():
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

    geo_ok = not cache.get("geo_df", pd.DataFrame()).empty
    profiles_ok = not cache.get("profiles_df", pd.DataFrame()).empty

    duck_ok = DUCK_CONN is not None

    # ✅ "Real readiness" means: data loaded and not currently loading
    ready = bool(duck_ok and geo_ok and profiles_ok and (not is_loading) and (last_loaded and last_loaded > 0))

    return {
        "ready": ready,
        "duck_ok": bool(duck_ok),
        "geo_ok": bool(geo_ok),
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
    "action_date", "year", "total_spend", "spend_amount", "total_revenue"
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
            return df.fillna(0).astype(object).where(pd.notnull(df), None)

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
    filters: Dict[str, Optional[str]], 
    use_fy_logic: bool = False  # ✅ NEW: Toggle for FY calculation
) -> Tuple[str, List[Any]]:
    """
    Returns (where_sql, params) for summary.parquet queries.
    All comparisons are done in UPPER space.
    
    If use_fy_logic is True:
      Calculates FY dynamically: year + (month >= 10)
    Else:
      Uses the 'year' column directly.
    """
    where_parts: List[str] = ["1=1"]
    params: List[Any] = []

    # --- YEAR / FY FILTERING ---
    if years and len(years) > 0:
        ys = safe_years(years, min_year=1900, max_year=2200, max_len=50)
        if ys:
            placeholders = ','.join(['?' for _ in ys])
            
            if use_fy_logic:
                # Complex FY Calculation
                fy_expr = "CAST(year AS INTEGER) + CASE WHEN CAST(COALESCE(month, 1) AS INTEGER) >= 10 THEN 1 ELSE 0 END"
                where_parts.append(f"{fy_expr} IN ({placeholders})")
            else:
                # Standard Column Match
                where_parts.append(f"year IN ({placeholders})")
                
            params.extend(ys)

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

    sql = f"SELECT {select_sql} FROM read_parquet(?) WHERE {where_sql}"
    
    if group_by_sql:
        sql += f" GROUP BY {group_by_sql}"
        
    if order_by_sql:
        sql += f" ORDER BY {order_by_sql}"
        
    local_params = [str(path)] + list(params)

    if limit > 0:
        sql += " LIMIT ?"
        local_params.append(limit)
    if offset > 0:
        sql += " OFFSET ?"
        local_params.append(offset)

    with DUCK_LOCK:
        ensure_duck_conn() 
        df = DUCK_CONN.execute(sql, local_params).fetchdf()
        return df.fillna(0).astype(object).where(pd.notnull(df), None)



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

def get_parent_aggregate_stats(parent_name: str):
    if not parent_name:
        return None

    clean = parent_name.strip().upper().replace("'", "")
    where_sql = "upper(clean_parent) = ?"
    params = [clean]

    # totals
    totals = query_summary_df(
        where_sql, params,
        select_sql="sum(total_spend) as total_spend, sum(contract_count) as contract_count, max(year) as last_active",
        limit=1
    )
    if totals.empty:
        return None

    total_spend = float(totals["total_spend"].iloc[0]) if "total_spend" in totals.columns else 0.0
    total_contracts = int(totals["contract_count"].iloc[0]) if "contract_count" in totals.columns else 0
    last_active = int(totals["last_active"].iloc[0]) if "last_active" in totals.columns and pd.notna(totals["last_active"].iloc[0]) else 0

    # top NAICS (code + description if present)
    naics = query_summary_df(
        where_sql, params,
        select_sql="naics_code, naics_description, count(*) as n",
        group_by_sql="naics_code, naics_description",  # ✅ ADDED
        order_by_sql="n DESC",
        limit=5
    )
    top_naics: List[str] = []
    if not naics.empty and "naics_code" in naics.columns:
        for r in naics.itertuples(index=False):
            code = str(getattr(r, "naics_code", "")).strip()
            desc = str(getattr(r, "naics_description", "")).strip() if "naics_description" in naics.columns else ""
            if code and desc and desc.lower() != "nan":
                top_naics.append(f"{code} - {desc}")
            elif code:
                top_naics.append(code)

    plats = query_summary_df(
        where_sql, params,
        select_sql="platform_family, sum(total_spend) as spend",
        group_by_sql="platform_family",  # ✅ ADDED
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
            "df_opportunities": pd.DataFrame()
        }

        # 2. DOWNLOAD FILES
        LOCAL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        files = [
            "products.parquet", "summary.parquet", "geo.parquet", 
            "profiles.parquet", "risk.parquet", "network.parquet", 
            "transactions.parquet", "opportunities.parquet"
        ]

        def fetch_file(filename: str) -> str:
            final_path = (LOCAL_CACHE_DIR / filename).resolve()
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

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
             list(executor.map(fetch_file, files))
        
        # 3. REFRESH DUCKDB VIEWS (Using writer connection)
        # 3. REFRESH DUCKDB VIEWS (Using writer connection)
        with DUCK_INIT_LOCK:
            conn = ensure_duck_conn()
    
            prod_path = str((LOCAL_CACHE_DIR / "products.parquet").resolve())
            trans_path = str((LOCAL_CACHE_DIR / "transactions.parquet").resolve())
            net_path = str((LOCAL_CACHE_DIR / "network.parquet").resolve())

            # ✅ FIX: Use f-strings instead of parameters for CREATE VIEW
            conn.execute(f"CREATE OR REPLACE VIEW v_products AS SELECT * FROM read_parquet('{prod_path}');")
            conn.execute(f"CREATE OR REPLACE VIEW v_transactions AS SELECT * FROM read_parquet('{trans_path}');")
            conn.execute(f"CREATE OR REPLACE VIEW v_network AS SELECT * FROM read_parquet('{net_path}');")

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
        ram_files = ["geo.parquet", "profiles.parquet", "risk.parquet", "opportunities.parquet"]
        
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

        # 6. LOAD SUMMARY (Atomic Swap Logic + Clean NaN)
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            summary_in_path = (LOCAL_CACHE_DIR / "summary.parquet").resolve()
            summary_final_path = (LOCAL_CACHE_DIR / SUMMARY_PARQUET_CLEAN).resolve()
            summary_temp_path = (LOCAL_CACHE_DIR / f"{SUMMARY_PARQUET_CLEAN}.tmp").resolve()

            if summary_temp_path.exists():
                try:
                    summary_temp_path.unlink()
                except Exception:
                    pass

            parquet_file = pq.ParquetFile(str(summary_in_path))
            name_corrections = {
                "THE BOEING": "THE BOEING COMPANY",
                "BOEING": "THE BOEING COMPANY",
                "BOEING CO": "THE BOEING COMPANY",
            }

            writer = None

            for i in range(parquet_file.num_row_groups):
                chunk = parquet_file.read_row_group(i).to_pandas()
                
                # Cleanup Strings
                string_cols = [
                    "vendor_name", "clean_parent", "ultimate_parent_name", "cage_code",
                    "platform_family", "sub_agency", "market_segment", "psc_description"
                ]
                for col in string_cols:
                    if col in chunk.columns:
                        if isinstance(chunk[col].dtype, pd.ArrowDtype):
                            chunk[col] = chunk[col].astype(str)
                        chunk[col] = chunk[col].astype(str).str.upper().str.strip()

                # Apply Parent Mapping
                cage_col = "cage_code" if "cage_code" in chunk.columns else "vendor_cage"
                if cage_col in chunk.columns and cage_map:
                    chunk["clean_parent"] = chunk[cage_col].map(cage_map)
                else:
                    chunk["clean_parent"] = None

                if "ultimate_parent_name" in chunk.columns:
                    chunk["clean_parent"] = chunk["clean_parent"].fillna(chunk["ultimate_parent_name"])
                if "vendor_name" in chunk.columns:
                    chunk["clean_parent"] = chunk["clean_parent"].fillna(chunk["vendor_name"])

                chunk["clean_parent"] = chunk["clean_parent"].astype(str).str.upper().str.strip()
                chunk["clean_parent"] = chunk["clean_parent"].replace(name_corrections)
                if "vendor_name" in chunk.columns:
                    chunk["vendor_name"] = chunk["vendor_name"].replace(name_corrections)

                # ✅ YOUR CRITICAL LOGIC: Clean NANs
                for col in ["platform_family", "market_segment", "sub_agency", "psc_description"]:
                    if col in chunk.columns:
                        m = chunk[col].astype(str).str.upper().isin(["NAN", "NAN.0", "NONE", "", "UNKNOWN"])
                        chunk.loc[m, col] = None

                if "cage_code" in chunk.columns:
                    chunk["cage_code"] = chunk["cage_code"].astype(str).str.upper().str.strip()
                    bad = chunk["cage_code"].isin(["NAN", "NONE", "NULL", ""])
                    chunk.loc[bad, "cage_code"] = None
                
                table = pa.Table.from_pandas(chunk, preserve_index=False)

                if writer is None:
                    writer = pq.ParquetWriter(str(summary_temp_path), table.schema, compression="zstd")
                
                writer.write_table(table)
                del chunk
                del table
                if i % 5 == 0:
                    gc.collect()

            if writer:
                writer.close()

            # Atomic Swap
            with DUCK_INIT_LOCK:
                conn = ensure_duck_conn()
                
                if summary_temp_path.exists():
                    summary_temp_path.replace(summary_final_path) 
                
                conn.execute(
                    f"CREATE OR REPLACE VIEW v_summary AS SELECT * FROM read_parquet('{str(summary_final_path)}');"
                )
            
            logger.info("Summary updated safely via atomic swap.")
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
        
        if not new_global_cache["geo_df"].empty:
             g_df = new_global_cache["geo_df"]
             if "cage_code" in g_df.columns:
                new_global_cache["location_map"] = g_df.set_index("cage_code")[["city", "state"]].to_dict(orient="index")

        # 8. BUILD SEARCH INDEX
        search_list: List[Dict[str, Any]] = []
        try:
            loc_map = new_global_cache.get("location_map", {}) or {}

            # Use writer connection for these heavy queries during reload
            parent_df = duck_fetch_df("""
                SELECT
                    clean_parent AS label,
                    SUM(total_spend) AS total_spend,
                    COUNT(DISTINCT cage_code) AS cage_count
                FROM v_summary
                WHERE clean_parent IS NOT NULL AND TRIM(CAST(clean_parent AS VARCHAR)) <> ''
                GROUP BY 1
                ORDER BY total_spend DESC
                LIMIT 5000;
            """, use_writer=True)

            if not parent_df.empty:
                for r in parent_df.itertuples(index=False):
                    val = getattr(r, "label", None)
                    spend = float(getattr(r, "total_spend", 0) or 0)
                    cages = int(getattr(r, "cage_count", 0) or 0)
                    if val and spend > 0 and (cages > 1 or spend > 1e9):
                        search_list.append({
                            "label": str(val),
                            "value": str(val),
                            "type": "PARENT",
                            "score": float(spend),
                            "cage": "AGGREGATE"
                        })

            child_df = duck_fetch_df("""
                SELECT
                    vendor_name,
                    cage_code,
                    SUM(total_spend) AS total_spend
                FROM v_summary
                WHERE vendor_name IS NOT NULL AND TRIM(CAST(vendor_name AS VARCHAR)) <> ''
                  AND cage_code IS NOT NULL AND TRIM(CAST(cage_code AS VARCHAR)) <> ''
                GROUP BY 1,2
                ORDER BY total_spend DESC
                LIMIT 20000;
            """, use_writer=True)

            if not child_df.empty:
                for r in child_df.itertuples(index=False):
                    if float(getattr(r, "total_spend", 0) or 0) <= 0:
                        continue
                    raw_cage = str(getattr(r, "cage_code", "")).strip().upper()
                    if raw_cage in ["NAN", "NONE", "NULL", ""]:
                        continue
                    loc = loc_map.get(raw_cage, {}) or {}
                    search_list.append({
                        "label": str(getattr(r, "vendor_name", "")),
                        "value": str(getattr(r, "vendor_name", "")),
                        "type": "CHILD",
                        "score": float(getattr(r, "total_spend", 0) or 0),
                        "cage": raw_cage,
                        "city": loc.get("city", ""),
                        "state": loc.get("state", "")
                    })

            plat_df = duck_fetch_df("""
                SELECT platform_family AS label, SUM(total_spend) AS total_spend
                FROM v_summary
                WHERE platform_family IS NOT NULL AND TRIM(CAST(platform_family AS VARCHAR)) <> ''
                GROUP BY 1
                ORDER BY total_spend DESC
                LIMIT 2000;
            """, use_writer=True)
            if not plat_df.empty:
                for r in plat_df.itertuples(index=False):
                    if getattr(r, "label", None):
                        search_list.append({
                            "label": str(getattr(r, "label")),
                            "value": str(getattr(r, "label")),
                            "type": "PLATFORM",
                            "score": float(getattr(r, "total_spend", 0) or 0)
                        })

            ag_df = duck_fetch_df("""
                SELECT sub_agency AS label, SUM(total_spend) AS total_spend
                FROM v_summary
                WHERE sub_agency IS NOT NULL AND TRIM(CAST(sub_agency AS VARCHAR)) <> ''
                GROUP BY 1
                ORDER BY total_spend DESC
                LIMIT 2000;
            """, use_writer=True)
            if not ag_df.empty:
                for r in ag_df.itertuples(index=False):
                    if getattr(r, "label", None):
                        search_list.append({
                            "label": str(getattr(r, "label")),
                            "value": str(getattr(r, "label")),
                            "type": "AGENCY",
                            "score": float(getattr(r, "total_spend", 0) or 0)
                        })

            search_list.sort(key=lambda x: x.get("score", 0), reverse=True)
            new_global_cache["search_index"] = search_list

        except Exception:
            logger.exception("Search index build failed")
            new_global_cache["search_index"] = []

        gc.collect()

        # 9. ATOMIC POINTER SWAP
        new_global_cache["is_loading"] = False
        new_global_cache["last_loaded"] = time.time()

        # ✅ SURGICAL FIX 2: Do NOT declare global here again. Just assign.
        GLOBAL_CACHE = new_global_cache

        logger.info("RELOAD COMPLETE (Atomic Swap). search_index=%d", len(search_list))
        
        new_global_cache = None
        gc.collect()

    except Exception:
        logger.exception("Reload crash")
        # Restore safe state on crash so API doesn't hang
        GLOBAL_CACHE = {**GLOBAL_CACHE, "is_loading": False}
    finally:
        RELOAD_LOCK.release()



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

    agencies_df = query_summary_df(where_sql, params, "DISTINCT sub_agency", order_by_sql="sub_agency ASC", limit=5000)
    domains_df  = query_summary_df(where_sql, params, "DISTINCT market_segment", order_by_sql="market_segment ASC", limit=5000)
    plats_df    = query_summary_df(where_sql, params, "DISTINCT platform_family", order_by_sql="platform_family ASC", limit=5000)

    psc_df = query_summary_df(
        where_sql, params,
        "DISTINCT psc_code, psc_description",
        order_by_sql="psc_code ASC, psc_description ASC",
        limit=5000
    )

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
    """
    Calculates Risk using the specialized 'risk_df' sidecar.
    """
    # ✅ FIX: Use GLOBAL_CACHE["risk_df"]
    df = GLOBAL_CACHE.get("risk_df", pd.DataFrame())
    
    if df.empty: 
        return {"label": "Expiring Value (90d)", "value": "N/A", "sub_label": "No Data"}

    # 2. Apply Filters (Vendor, Agency, etc.) to the Sidecar
    filtered_risk = FilterEngine.apply_pandas(df, None, filters)

    # 3. Filter for the 90-Day Window
    today = pd.Timestamp.now()
    next_90 = today + pd.Timedelta(days=90)
    
    # Ensure date column is datetime objects
    dates = pd.to_datetime(filtered_risk['completion_date'], errors='coerce')
    mask = (dates >= today) & (dates <= next_90)
    
    final_slice = filtered_risk[mask]

    # 4. Calculate Totals
    total_value = final_slice['spend_amount'].sum()
    count = final_slice['contract_id'].nunique()

    return {
        "label": "Expiring Value (90d)",
        "value": f"${total_value/1e9:.2f}B",
        "sub_label": f"{count} Contracts Ending",
        "status": "warning"
    }

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

    total_spend = float(kpi_df["total_spend"].iloc[0]) if (not kpi_df.empty and "total_spend" in kpi_df.columns) else 0.0
    total_contracts = int(kpi_df["total_contracts"].iloc[0]) if (not kpi_df.empty and "total_contracts" in kpi_df.columns) else 0

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

    where_sql, params = build_summary_where(None, filters)

    # We reproduce your FY logic:
    # FY = year + (month>=10)
    # month_num = month (or 1 default)
    base_df = query_summary_df(
        where_sql=where_sql,
        params=params,
        select_sql="""
            CAST(year AS INTEGER) AS year_num,
            CAST(COALESCE(month, 1) AS INTEGER) AS month_num,
            SUM(total_spend) AS spend
        """,
        group_by_sql="CAST(year AS INTEGER), CAST(COALESCE(month, 1) AS INTEGER)",  # ✅ ADDED
        order_by_sql="year_num ASC, month_num ASC",
        limit=0
    )

    if base_df.empty:
        return []

    base_df["year_num"] = pd.to_numeric(base_df["year_num"], errors="coerce").fillna(0).astype(int)
    base_df["month_num"] = pd.to_numeric(base_df["month_num"], errors="coerce").fillna(1).astype(int)
    base_df["spend"] = pd.to_numeric(base_df["spend"], errors="coerce").fillna(0.0).astype(float)

    base_df["fy"] = base_df["year_num"] + (base_df["month_num"] >= 10).astype(int)

    # Apply FY filter (your step 4)
    if years and len(years) > 0:
        ys = safe_years(years, min_year=1900, max_year=2200, max_len=50)
        if ys:
            base_df = base_df[base_df["fy"].isin(ys)]
            if base_df.empty:
                return []

    if mode == "yearly":
        grouped = base_df.groupby("fy", dropna=False)["spend"].sum().reset_index()

        active = grouped[grouped["spend"] > 0]
        if active.empty:
            return []

        min_year = int(active["fy"].min())
        max_year = int(active["fy"].max())

        data_map = {int(r["fy"]): float(r["spend"]) for _, r in grouped.iterrows()}

        final_data = []
        for y in range(min_year, max_year + 1):
            final_data.append({"label": str(y), "spend": float(data_map.get(y, 0.0))})
        return final_data

    if mode == "monthly":
        grouped = base_df.groupby("month_num", dropna=False)["spend"].sum().reset_index()
        grouped.columns = ["label", "spend"]

        def get_fiscal_sort(m):
            try:
                m = int(m)
                return m - 9 if m >= 10 else m + 3
            except:
                return 0

        grouped["sort_index"] = grouped["label"].apply(get_fiscal_sort)
        grouped = grouped.sort_values("sort_index", ascending=True)
        grouped["spend"] = grouped["spend"].astype(float)

        return grouped[["label", "spend"]].to_dict(orient="records")

    return []


# ✅ NEW: Drill-down endpoint
@app.get("/api/dashboard/subsidiaries")
def get_dashboard_subsidiaries(parent: str):
    if not parent:
        return []

    clean_parent = sanitize(parent)
    loc_map = GLOBAL_CACHE.get("location_map", {}) or {}

    # Filter by clean_parent and aggregate by cage + vendor_name
    df = query_summary_df(
        where_sql="upper(clean_parent) = ?",
        params=[clean_parent],
        select_sql="""
            cage_code,
            vendor_name,
            SUM(total_spend) AS total_spend,
            SUM(contract_count) AS contract_count
        """,
        group_by_sql="cage_code, vendor_name",  # ✅ ADDED
        order_by_sql="total_spend DESC",
        limit=200
    )

    if df.empty:
        return []

    out = []
    for r in df.itertuples(index=False):
        cage_val = str(getattr(r, "cage_code", "")).strip().upper()
        loc = loc_map.get(cage_val, {}) or {}
        out.append({
            "cage": cage_val,
            "name": str(getattr(r, "vendor_name", "")),
            "total_obligations": float(getattr(r, "total_spend", 0) or 0),
            "contract_count": int(getattr(r, "contract_count", 0) or 0),
            "city": str(loc.get("city", "")) if loc.get("city") else "N/A",
            "state": str(loc.get("state", "")) if loc.get("state") else "N/A",
        })

    return out



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

    out = []
    for r in df.itertuples(index=False):
        out.append({
            "vendor": str(getattr(r, "vendor", "")),
            "spend_m": float(getattr(r, "total_spend", 0.0) or 0.0) / 1_000_000.0,
            "contracts": int(getattr(r, "contract_count", 0) or 0),
        })
    return out


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

    total_df = query_summary_df(where_sql, params, "sum(total_spend) as total_spend", limit=1)
    total = float(total_df["total_spend"].iloc[0]) if (not total_df.empty and "total_spend" in total_df.columns) else 0.0
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

        top = d.head(4)
        top_sum = float(top["spend"].sum()) if "spend" in top.columns else 0.0
        other_val = max(0.0, total - top_sum)

        out = []
        for r in top.itertuples(index=False):
            out.append({
                "label": str(getattr(r, "label", "")),
                "value": round((float(getattr(r, "spend", 0.0) or 0.0) / total) * 100.0, 1),
            })
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
    # 1. Load Geo Data
    geo_df = GLOBAL_CACHE.get("geo_df", pd.DataFrame())
    if geo_df.empty:
        return []

    # Normalize Key
    if "join_key" not in geo_df.columns:
        if "cage_code" in geo_df.columns:
             geo_df["join_key"] = geo_df["cage_code"].astype(str).str.strip().str.upper()
        else:
             return []

    # 2. Get Spend Data
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

    # ✅ FIX: Request 150,000 rows and IGNORE the global 2,000 row limit
    active_vendors = query_summary_df(
        where_sql=where_sql,
        params=params,
        select_sql="cage_code, vendor_name, SUM(total_spend) AS total_spend",
        group_by_sql="cage_code, vendor_name",
        order_by_sql="total_spend DESC",
        limit=150000,
        ignore_cap=True 
    )

    if active_vendors.empty:
        return []

    # 3. Normalize & Merge
    active_vendors["join_key"] = active_vendors["cage_code"].astype(str).str.strip().str.upper()

    mapped_vendors = pd.merge(
        geo_df,
        active_vendors,
        on="join_key",
        how="inner",
        suffixes=("_geo", "_act")
    )

    if mapped_vendors.empty:
        return []

    # 4. Final Limit (Visual limit)
    mapped_vendors = mapped_vendors.sort_values("total_spend", ascending=False).head(50000)

    vendor_col = "vendor_name_act" if "vendor_name_act" in mapped_vendors.columns else "vendor_name"
    cage_col = "cage_code_act" if "cage_code_act" in mapped_vendors.columns else "cage_code"

    out = []
    for r in mapped_vendors.itertuples(index=False):
        if pd.isna(r.latitude) or pd.isna(r.longitude):
            continue

        out.append({
            "id": getattr(r, cage_col, ""),
            "vendor": getattr(r, vendor_col, ""),
            "cage": str(getattr(r, cage_col, "")).strip().upper(),
            "lat": float(r.latitude),
            "lon": float(r.longitude),
            "spend": float(getattr(r, "total_spend", 0)),
        })
    return out



# --- RESTORED: MARKET OPPORTUNITIES ---
# --- UPDATE IN API.PY ---

@app.get("/api/dashboard/opportunities")
def get_market_opportunities(
    domain: Optional[str] = None,
    agency: Optional[str] = None,
    platform: Optional[str] = None,
    vendor: Optional[str] = None,
):
    query_base = """
    SELECT 
        id as noticeid,
        sol_num,
        title,
        agency,
        deadline,
        naics as naicscode,
        set_aside_type as setaside,
        poc_email as primarycontactemail,
        COUNT(*) OVER() as total_matches
    FROM "market_intel_gold"."view_unified_opportunities_dod"
    WHERE 1=1
    """

    conditions: List[str] = []

    if agency:
        conditions.append(safe_contains_upper("agency", agency))

    def title_or_desc_contains(val: str) -> str:
        return f"({safe_contains_upper('title', val)} OR {safe_contains_upper('description', val)})"

    if domain:
        conditions.append(title_or_desc_contains(domain))

    if platform:
        conditions.append(title_or_desc_contains(platform))

    if vendor:
        conditions.append(title_or_desc_contains(vendor))

    if conditions:
        query_base += " AND " + " AND ".join(conditions)

    query_base += " ORDER BY try(from_iso8601_timestamp(deadline)) ASC NULLS LAST LIMIT 50"

    return run_athena_query(query_base)

# ==========================================
#        GLOBAL SEARCH (NEW)
# ==========================================

@app.get("/api/search/global")
def search_global(q: str):
    """
    Unified search: Checks Name, CAGE, and now displays Location.
    """
    if not q or len(q) < 2: return []
    clean_q = sanitize(q)
    results = []

    # 1. FAST CACHE SEARCH
    index = GLOBAL_CACHE.get("search_index", [])
    
    count = 0
    for item in index:
        # A. Check Name
        match_name = clean_q in item['label'].upper()
        
        # B. Check CAGE
        match_cage = False
        item_cage = item.get('cage')
        if item_cage and item_cage != 'AGGREGATE':
            match_cage = clean_q in str(item_cage).upper()

        if match_name or match_cage:
            spend_str = f"${item['score']/1_000_000:,.1f}M" if item.get('score') else ""
            
            # --- SMART LABELING WITH LOCATION ---
            if item.get('type') == 'CHILD' and item_cage:
                # ✅ NEW: Check for location data
                city = item.get('city')
                state = item.get('state')
                
                if city and state:
                    loc_str = f" • {city}, {state}"
                elif city:
                    loc_str = f" • {city}"
                else:
                    loc_str = ""

                sub_label = f"{spend_str}{loc_str} • CAGE: {item_cage}"

            elif item.get('type') == 'PARENT':
                sub_label = f"{spend_str} • Corporate Group"
            else:
                sub_label = f"{spend_str} Obligations"

            results.append({
                "type": item['type'],
                "label": item['label'],
                "value": item['value'],
                "sub_label": sub_label,
                "cage": item.get('cage')
            })
            
            count += 1
            if count >= 8: break 

    # 2. NSN / NIIN Detection (Regex)
    # Matches 9-digit NIINs or 13-digit NSNs
    nsn_pattern = r'^\d{4}-?\d{2}-?\d{3}-?\d{4}$|^\d{9}$|^\d{13}$'
    if re.match(nsn_pattern, q.strip()):
        results.insert(0, {
            "type": "NSN",
            "label": f"Lookup Part: {q.strip()}",
            "value": q.strip(),
            "sub_label": "Supply Chain Search"
        })

    # 3. LIVE PIPELINE SEARCH (Athena)
    # Only run if the query looks specific enough (and isn't just a CAGE code)
    if len(clean_q) > 3 and not re.match(r"^\d{5}$", clean_q):
        try:
            qv = clean_q.upper()
            opp_query = f"""
            SELECT title, sol_num, agency
            FROM "market_intel_gold"."view_unified_opportunities_dod"
            WHERE {safe_contains_upper("title", qv)} OR {safe_contains_upper("sol_num", qv)}
            ORDER BY try(from_iso8601_timestamp(deadline)) ASC NULLS LAST
            LIMIT 3
            """
            opps = run_athena_query(opp_query)
            for o in opps:
                results.append({
                    "type": "OPPORTUNITY",
                    "label": o.get("title"),
                    "value": o.get("sol_num"),
                    "sub_label": f"Bid • {o.get('agency')}",
                })
        except Exception:
            logger.exception("global search opportunity lookup failed")

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

    # Build filters using the SAME semantics as your original:
    # platform is strict equality; agency/domain strict equality
    filters = {
        "vendor": None,
        "parent": None,
        "cage": None,
        "domain": domain,
        "agency": agency,
        "platform": search_upper,
        "psc": None
    }

    where_sql, params = build_summary_where(None, filters)

    # Pull minimal rows needed for stats
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
        group_by_sql="platform_family, vendor_name, cage_code, sub_agency",  # ✅ ADDED
        order_by_sql="total_spend DESC",
        limit=0
    )

    if df.empty:
        return {
            "found": True, "name": name, "total_obligations": 0.0,
            "contractor_count": 0, "contract_count": 0, "top_vendors": [], "top_agencies": []
        }

    # Fiscal year filtering (your exact logic)
    if years and len(years) > 0:
        ys = safe_years(years, min_year=1900, max_year=2200, max_len=50)
        if ys:
            # We need month/year for FY; easiest is to re-query with month/year and filter in python
            df2 = query_summary_df(
                where_sql=where_sql,
                params=params,
                select_sql="""
                    platform_family,
                    vendor_name,
                    cage_code,
                    sub_agency,
                    CAST(year AS INTEGER) AS year_num,
                    CAST(COALESCE(month, 1) AS INTEGER) AS month_num,
                    total_spend,
                    contract_count
                """,
                # No GROUP BY needed here since we aren't summing yet
                limit=0
            )
            if df2.empty:
                return {
                    "found": True, "name": name, "total_obligations": 0.0,
                    "contractor_count": 0, "contract_count": 0, "top_vendors": [], "top_agencies": []
                }

            df2["year_num"] = pd.to_numeric(df2["year_num"], errors="coerce").fillna(0).astype(int)
            df2["month_num"] = pd.to_numeric(df2["month_num"], errors="coerce").fillna(1).astype(int)
            df2["fy"] = df2["year_num"] + (df2["month_num"] >= 10).astype(int)

            df2 = df2[df2["fy"].isin(ys)]
            if df2.empty:
                return {
                    "found": True, "name": name, "total_obligations": 0.0,
                    "contractor_count": 0, "contract_count": 0, "top_vendors": [], "top_agencies": []
                }

            # Now re-aggregate back to your expected stats universe
            df = (
                df2.groupby(["platform_family", "vendor_name", "cage_code", "sub_agency"], dropna=False)
                .agg(total_spend=("total_spend", "sum"), contract_count=("contract_count", "sum"))
                .reset_index()
            )

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
    safe_plat = sanitize(name)
    
    # 1. Try Disk First (Fastest - Primary Platform Match)
    df = pd.DataFrame()
    try:
        df = get_subset_from_disk(
            "products.parquet",
            where_clause="upper(platform_family) = ?", 
            params=(safe_plat,),
            limit=limit + offset + 200
        )
    except Exception:
        df = pd.DataFrame()

    # 2. ✅ RICH FALLBACK: If Disk returned nothing/little, use Athena to find Shared Parts
    # This query JOINS the Platform Map -> NIINs -> Gold Data to get rich stats
    if df.empty:
        safe_val = sql_literal(safe_plat)
        
        query = f"""
        WITH platform_codes AS (
            -- 1. Get WSDC Codes for this Platform (e.g. F-35)
            SELECT DISTINCT TRIM(CAST(wsdc_code_ref AS VARCHAR)) AS wsdc_code_ref
            FROM "market_intel_silver"."ref_platform_map"
            WHERE upper(platform_family) = {safe_val}
        ),
        target_niins AS (
            -- 2. Expand WSDC to NIINs
            SELECT DISTINCT LPAD(CAST(niin AS VARCHAR), 9, '0') as join_niin
            FROM "market_intel_silver"."ref_wsdc"
            WHERE wsdc_code IN (SELECT wsdc_code_ref FROM platform_codes)
              AND niin IS NOT NULL
        )
        -- 3. JOIN with GOLD DATA to get the Revenue/Trend/Supplier info
        SELECT 
            p.nsn,
            p.niin,
            p.description,
            
            -- ✅ FILL MISSING DATA
            p.cage, 
            COALESCE(p.total_revenue, 0) as total_revenue,
            COALESCE(p.total_units_sold, 0) as total_units_sold,
            p.annual_revenue_trend,
            p.last_sold_date,
            
            -- Derive FSC
            SUBSTR(p.nsn, 1, 4) as fsc_code,
            
            -- Context
            '{safe_plat}' as platform_family
            
        FROM "market_intel_gold"."view_dashboard_products" p
        INNER JOIN target_niins t ON LPAD(CAST(p.niin AS VARCHAR), 9, '0') = t.join_niin
        
        ORDER BY p.total_revenue DESC
        LIMIT {limit}
        """
        
        raw_data = run_athena_query(query)
        if raw_data:
            df = pd.DataFrame(raw_data)
        else:
            return []

    # 3. Standard Logic (Filtering & Formatting)
    mask = pd.Series(True, index=df.index)
    
    if 'total_revenue' in df.columns:
        df['total_revenue'] = pd.to_numeric(df['total_revenue'], errors='coerce').fillna(0)
        if not include_zero:
            mask &= (df['total_revenue'] > 0)
        if min_spend > 0 and not years:
            mask &= (df['total_revenue'] >= min_spend)
    
    filtered = df[mask].copy()
    
    # Trend/Year Logic
    if years and 'annual_revenue_trend' in filtered.columns:
        filtered['amount'] = filtered['annual_revenue_trend'].apply(
            lambda s: calculate_trend_sum(s or "", years)
        )
        filtered = filtered.sort_values("amount", ascending=False)
    elif 'total_revenue' in filtered.columns:
        filtered['amount'] = filtered['total_revenue']
        filtered = filtered.sort_values("total_revenue", ascending=False)
    else:
        filtered['amount'] = 0

    # Paginate
    start = int(offset)
    end = start + int(limit)
    page = filtered.iloc[start:end]

    # Format
    results = []
    for row in page.itertuples():
        niin_val = getattr(row, "niin", "")
        nsn_val = getattr(row, "nsn", "")
        if not nsn_val and niin_val: nsn_val = niin_val
        
        results.append({
            "item_id": str(nsn_val),
            "nsn": str(nsn_val),
            "niin": str(niin_val).zfill(9),
            "description": getattr(row, "description", ""),
            "fsc_code": getattr(row, "fsc_code", ""),
            "total_units_sold": int(getattr(row, "total_units_sold", 0) or 0),
            "amount": float(getattr(row, "amount", 0) or 0),
            "annual_revenue_trend": getattr(row, "annual_revenue_trend", ""),
            # ✅ Map 'cage' to 'top_vendor' so the UI can display it
            "top_vendor": getattr(row, "cage", ""), 
            "last_sold": getattr(row, "last_sold_date", "")
        })
    return results

@app.get("/api/platform/parts/count")
def get_platform_parts_count(name: str):
    # ✅ FIX: Use safe SQL literal for exact match
    safe_val = sql_literal((name or "").strip())
    # ✅ FIX: Use safe LIKE for fuzzy match with new '#' escape
    like_val = sql_like_contains((name or "").strip().upper())

    # 1. Fast check
    map_check = run_athena_query(f"""
        SELECT COUNT(*) AS n
        FROM "market_intel_silver"."ref_platform_map"
        WHERE (platform_family = {safe_val}
               OR UPPER(platform_family) LIKE {like_val} ESCAPE '#') 
          AND wsdc_code_ref IS NOT NULL
          AND TRIM(CAST(wsdc_code_ref AS VARCHAR)) <> ''
    """)
    
    if not map_check or int(map_check[0].get("n") or 0) == 0:
        return {"count": 0}

    # 2. Count Universe
    query = f"""
    WITH platform_codes AS (
        SELECT DISTINCT TRIM(CAST(wsdc_code_ref AS VARCHAR)) AS wsdc_code_ref
        FROM "market_intel_silver"."ref_platform_map"
        WHERE wsdc_code_ref IS NOT NULL
          AND TRIM(CAST(wsdc_code_ref AS VARCHAR)) <> ''
          AND (
                platform_family = {safe_val}
             OR UPPER(platform_family) LIKE {like_val} ESCAPE '#'
          )
    )
    SELECT COUNT(DISTINCT w.niin) as total
    FROM "market_intel_silver"."ref_wsdc" w
    WHERE w.wsdc_code IN (SELECT wsdc_code_ref FROM platform_codes)
      AND w.niin IS NOT NULL AND w.niin <> ''
    """

    results = cached_athena_query(query)

    if results and len(results) > 0:
        val = results[0].get('total', 0)
        return {"count": int(val)}

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
    threshold: Optional[float] = 0
):
    safe_plat = sanitize(name)
    
    # ✅ FIX: Query DISK (transactions.parquet)
    # Fetch recent awards for this platform directly from disk
    limit_query = limit + offset + 100 # Fetch enough for pagination
    df = get_subset_from_disk(
        "transactions.parquet",
        where_clause="platform_family = ?",
        params=(safe_plat,),
        order_by_sql="action_date DESC",
        limit=limit_query
    )


    
    if df.empty: return []

    # Apply remaining filters in RAM (on the small subset)
    mask = pd.Series(True, index=df.index)
    if agency:
        safe_ag = sanitize(agency)
        if "sub_agency" in df.columns:
            mask &= (df["sub_agency"] == safe_ag)
    if years:
        mask &= df["year"].isin(years)
    if threshold and threshold > 0:
        mask &= (df["spend_amount"] >= (threshold * 1_000_000))

    filtered = df[mask]
    start = int(offset)
    end = start + int(limit)
    page = filtered.iloc[start:end]

    return [
        {
            "contract_id": r.contract_id,
            "action_date": str(r.action_date),
            "vendor_name": r.vendor_name,
            "vendor_cage": r.vendor_cage,
            "agency": r.sub_agency if hasattr(r, 'sub_agency') else r.parent_agency,
            "description": r.description,
            "spend": float(r.spend_amount),
            "naics": r.naics_code
        }
        for r in page.itertuples()
    ]




# ✅ RESTORED ENDPOINT: Fixes 404 "Not Found" error
# This aliases the old route to the new fast logic so the UI works without changes.
@app.get("/api/company/opportunities")
def get_company_opportunities(cage: Optional[str] = None, name: Optional[str] = None):
    # 1. Reuse existing profile logic to find capabilities
    # This works for both specific CAGEs (Drill-down) and Parents (Aggregate)
    target_naics = set()
    
    # Try InMemory Profiles First (Fastest/Safest)
    profiles_df = GLOBAL_CACHE.get("profiles_df", pd.DataFrame())
    
    if not profiles_df.empty:
        # Match by CAGE
        if cage and cage != "AGGREGATE":
            matches = profiles_df[profiles_df['cage_code'] == cage]
            for _, row in matches.iterrows():
                if row.get('top_naics_codes'):
                    codes = str(row['top_naics_codes']).split(',')
                    target_naics.update([c.strip() for c in codes])

        # Match by Name (Parent Logic)
        if name:
            clean_name = sanitize(name)
            # Find ALL children matching this parent name to build a composite NAICS list
            matches = profiles_df[profiles_df['vendor_name'].str.contains(clean_name, case=False, na=False)]
            for _, row in matches.iterrows():
                if row.get('top_naics_codes'):
                    codes = str(row['top_naics_codes']).split(',')
                    target_naics.update([c.strip() for c in codes])

    # 2. Clean & Format found codes
    clean_naics_list = []
    for n in target_naics:
        s = str(n).split('.')[0].split(' - ')[0].strip()
        # Capture standard 5-6 digit codes (e.g. 336411)
        if len(s) >= 3 and s.isdigit(): 
            clean_naics_list.append(s)

    if not clean_naics_list:
        return []

    # 3. Search the In-Memory Opportunities Cache (Fast & Safe)
    df_opps = GLOBAL_CACHE.get("df_opportunities", pd.DataFrame())
    if df_opps.empty:
        return []

    # Create regex pattern for fast filtering
    pattern = "|".join([re.escape(c) for c in sorted(set(clean_naics_list))])
    mask = df_opps['naics'].astype(str).str.contains(pattern, regex=True, na=False)

    # Sort by closest deadline first
    filtered = df_opps[mask].sort_values("deadline", ascending=True).head(50)

    # 4. Format for UI (Including the critical alias)
    results = []
    for row in filtered.itertuples():
        # Handle potential missing attributes safely
        sol_val = getattr(row, 'sol_num', '') or getattr(row, 'sol#', '')
        
        results.append({
            "noticeid": getattr(row, 'id', ''),
            "title": getattr(row, 'title', ''),
            
            # ✅ THE FIX: Send BOTH keys to ensure UI compatibility
            "sol_num": sol_val, 
            "sol#": sol_val,     
            
            "department_indagency": getattr(row, 'agency', ''),
            "responsedeadline": getattr(row, 'deadline', ''),
            "setaside": getattr(row, 'set_aside_type', ''),
            "primarycontactemail": getattr(row, 'poc_email', '')
        })
    return results


# ==========================================
#        COMPANY INTELLIGENCE
# ==========================================

# [Find and replace get_company_profile in api.py]

@app.get("/api/company/profile")
def get_company_profile(cage: Optional[str] = None, name: Optional[str] = None):
    profiles_df = GLOBAL_CACHE.get("profiles_df", pd.DataFrame())
    loc_map = GLOBAL_CACHE.get("location_map", {}) or {}
    
    # 1. SPECIFIC CAGE (Drill-down) -> CHILD LOGIC
    if cage:
        clean_cage = cage.strip().upper()
        match = profiles_df[profiles_df['cage_code'] == clean_cage]
        
        if not match.empty:
            row = match.iloc[0]
            # Lookup Location
            loc = loc_map.get(clean_cage, {})
            # Return with City/State (Enables Local News)
            return format_profile_response_with_loc(row, loc.get('city'), loc.get('state'), type="CHILD")

    # 2. NAME MATCH
    if name:
        clean_name = name.strip().upper().replace("'", "")

        # A. PARENT LOGIC (Aggregate)
        stats = get_parent_aggregate_stats(name)
        if stats:
            return {
                "found": True,
                "type": "PARENT", 
                "name": name.upper(),
                "cage": "AGGREGATE",
                "total_obligations": stats["total_obligations"],
                "total_contracts": stats["total_contracts"],
                "last_active": stats["last_active"],
                "top_naics": stats["top_naics"],
                "top_platforms": stats["top_platforms"],
                # ✅ CRITICAL: Force empty location for Parents.
                # This ensures the News API runs the "General Search" strategy.
                "city": "", 
                "state": ""
            }

        # B. CHILD LOGIC (Specific Entity found by Name)
        child_match = profiles_df[profiles_df['vendor_name'] == clean_name]
        if not child_match.empty:
            row = child_match.iloc[0]
            # Use CAGE to find location
            c_code = row.get('cage_code')
            loc = loc_map.get(c_code, {})
            # Return with City/State (Enables Local News)
            return format_profile_response_with_loc(row, loc.get('city'), loc.get('state'), type="CHILD")

    return {"found": False}

# ✅ Helper function (Updated to use Master NAICS List)
def format_profile_response_with_loc(row, city, state, type="CHILD"):
    raw_codes = row.get('top_naics_codes', '').split(',') if row.get('top_naics_codes') else []
    hydrated_naics = []
    naics_map = GLOBAL_CACHE.get("naics_map", {})
    
    for code in raw_codes:
        # --- FIX 1: Sanitize the Code ---
        # Remove "unknown" (case insensitive) and whitespace
        clean_c = code.lower().replace('unknown', '').strip()
        # Regex: Keep only the leading digits (removes any trailing junk)
        match = re.match(r'^(\d+)', clean_c)
        
        if not match: 
            continue # Skip if no digits found
            
        c = match.group(1) 
        
        # --- Lookup Logic ---
        desc = naics_map.get(c)
        
        # Fallback for old codes (e.g. 811219 -> 8112)
        if not desc and len(c) > 2:
            desc = naics_map.get(c[:5])
        if not desc and len(c) > 2:
            desc = naics_map.get(c[:4])
            
        if desc:
            # We send the FULL string here so the UI has it for the tooltip
            hydrated_naics.append(f"{c} - {desc}")
        else:
            hydrated_naics.append(c)

    return {
        "found": True,
        "type": type,
        "name": row.get('vendor_name'),
        "cage": row.get('cage_code'),
        "total_obligations": float(row.get('total_lifetime_spend', 0)),
        "total_contracts": int(row.get('total_contracts', 0)),
        "last_active": int(row.get('last_active_year', 0)),
        "top_naics": hydrated_naics, 
        "top_platforms": row.get('top_platforms', '').split(',') if row.get('top_platforms') else [],
        "city": str(city) if city else "",
        "state": str(state) if state else ""
    }


# --- REPLACE IN api.py ---

# --- REPLACE get_company_network IN api.py ---

@app.get("/api/company/network")
def get_company_network(name: str, cage: Optional[str] = None):
    # This endpoint now uses DuckDB to query 'network.parquet' from disk.
    
    safe_name = sanitize(name).replace("'", "")
    is_drill_down = (cage and len(cage) > 2 and cage != 'AGGREGATE')
    
    def run_duck_query(sql, params):
        try:
            with DUCK_LOCK:
                conn = ensure_duck_conn()

                net_path = (LOCAL_CACHE_DIR / "network.parquet").resolve()
                if not net_path.exists():
                    return []

                # Use read_parquet directly
                final_sql = sql.replace("FROM network_source", f"FROM read_parquet('{str(net_path)}')")
                return conn.execute(final_sql, params).fetchdf().to_dict(orient="records")
        except Exception as e:
            logger.error(f"Network query failed: {e}")
            return []


    # SQL Template for Aggregation
    sql_template = """
        SELECT 
            {group_col} as name, 
            {cage_col} as cage, 
            arbitrary(subaward_description) as platform, 
            sum(flow_amount_capped) as total, 
            sum(flow_amount_raw) as total_raw, 
            count(contract_id) as transactions
        FROM network_source
        WHERE {where_clause}
        GROUP BY {group_col}, {cage_col}
        ORDER BY total DESC
        LIMIT 50
    """

    if is_drill_down:
        safe_cage = sanitize(cage)
        # Downstream
        subs = run_duck_query(
            sql_template.format(group_col="sub_name", cage_col="sub_cage", where_clause="prime_cage = ?"), 
            (safe_cage,)
        )
        # Upstream
        primes = run_duck_query(
            sql_template.format(group_col="prime_name", cage_col="prime_cage", where_clause="sub_cage = ?"), 
            (safe_cage,)
        )
    else:
        # Parent Mode
        # Downstream
        subs = run_duck_query(
            sql_template.format(group_col="sub_name", cage_col="sub_cage", where_clause="upper(prime_gold_parent) = ?"), 
            (safe_name,)
        )
        # Upstream
        primes = run_duck_query(
            sql_template.format(group_col="prime_name", cage_col="prime_cage", where_clause="upper(sub_gold_parent) = ?"), 
            (safe_name,)
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

# --- REPLACEMENT ENDPOINT ---
# --- REPLACEMENT ENDPOINT ---
@app.get("/api/company/parts")
def get_company_parts(
    cage: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    years: Optional[List[int]] = Query(default=None),
    rollup: Optional[str] = None,
):
    if not cage: return []
    safe_cage = sanitize(cage)

    # ✅ HYBRID FIX: Fetch ONLY this company's parts from Disk (DuckDB)
    # This prevents loading the 300MB file, but gives us a DataFrame to run your exact logic on.
    try:
        # ✅ FIX: Use the shared global connection + Parameters
        # Pass "cage = ?" and the tuple (safe_cage,)
        filtered = get_subset_from_disk(
            "products.parquet",
            where_clause="cage = ?",
            params=(safe_cage,)
        )
    except Exception as e:
        print(f"DuckDB Error in get_company_parts: {e}")
        return []
    
    if filtered.empty: return []

    # --- LOGIC RESTORED: Amount Calculation ---
    if years:
        # Re-apply trend calc logic on the subset
        filtered["amount"] = filtered["annual_revenue_trend"].apply(
            lambda s: calculate_trend_sum(s or "", years)
        )
    else:
        # Fallback to total_revenue
        filtered["amount"] = pd.to_numeric(filtered.get("total_revenue", 0), errors="coerce").fillna(0)

    # =========================
    # ROLLUP MODE: NSN / NIIN
    # =========================
    if rollup == "nsn":
        base = filtered["niin"] if "niin" in filtered.columns else filtered["nsn"]
        
        # Logic: Strip non-digits, pad to 9 chars
        filtered["niin_key"] = (
            base.astype(str)
            .str.replace(r"[^0-9]", "", regex=True)
            .str.strip()
            .str.zfill(9)
        )

        # Parse trend to dict so we can sum per NIIN
        filtered["_trend_dict"] = filtered["annual_revenue_trend"].apply(_parse_trend_to_dict)

        # Logic: Weighted Average Price
        def weighted_avg_price(group: pd.DataFrame) -> float:
            units = pd.to_numeric(group.get("total_units_sold", 0), errors="coerce").fillna(0).astype(float)
            price = pd.to_numeric(group.get("avg_unit_price", 0), errors="coerce").fillna(0).astype(float)
            denom = float(units.sum())
            if denom <= 0:
                return float(price.mean() if len(price) else 0.0)
            return float((price * units).sum() / denom)

        # Logic: Collect Part Numbers
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

        # Perform Aggregation (Exactly as original)
        agg = (
            filtered.groupby("niin_key", observed=True, dropna=False)
            .agg(
                description=("description", "first"),
                platform_family=("platform_family", "first"),
                total_units_sold=("total_units_sold", "sum"),
                total_revenue=("total_revenue", "sum"),
                amount=("amount", "sum"),
                last_sold_date=("last_sold_date", "max"),
                direct_sales_market_share_pct=("direct_sales_market_share_pct", "max"),
                part_numbers=("part_number", collect_part_numbers),
            )
            .reset_index()
        )

        # Recalculate Weighted Avg Price
        avg_map = (
            filtered.groupby("niin_key", observed=True)
            .apply(weighted_avg_price)
            .to_dict()
        )
        agg["avg_unit_price"] = agg["niin_key"].map(avg_map).fillna(0)

        # Re-Sum Trend Strings
        trend_map = (
            filtered.groupby("niin_key", observed=True)["_trend_dict"]
            .apply(lambda s: _sum_trend_dicts(list(s)))
            .to_dict()
        )
        agg["annual_revenue_trend"] = agg["niin_key"].map(trend_map).fillna("")

        agg["part_numbers_count"] = agg["part_numbers"].apply(
            lambda x: len(x) if isinstance(x, list) else 0
        )

        # Stable Sort
        agg = agg.sort_values(["amount", "niin_key"], ascending=[False, True], kind="mergesort")

        # Paginate
        start = int(offset)
        end = start + int(limit)
        page = agg.iloc[start:end]

        results: List[Dict] = []
        for row in page.itertuples(index=False):
            clean_niin = str(row.niin_key)
            final_nsn = clean_niin  # keep UI consistent

            results.append({
                "niin": clean_niin,
                "nsn": final_nsn,
                "description": getattr(row, "description", None),
                "part_number": "",
                "part_numbers": getattr(row, "part_numbers", []) or [],
                "part_numbers_count": int(getattr(row, "part_numbers_count", 0) or 0),
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
                "market_share_pct": 0.0,
                "direct_sales_market_share_pct": float(getattr(row, "direct_sales_market_share_pct", 0) or 0),
            })

        return results

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
def get_company_parts_count(cage: str, rollup: Optional[str] = None):
    if not cage:
        return {"count": 0}

    safe_cage = sanitize(cage)

    path = LOCAL_CACHE_DIR / "products.parquet"
    if not path.exists():
        return {"count": 0}

    try:
        with DUCK_LOCK:
            global DUCK_CONN
            ensure_duck_conn()

            if rollup == "nsn":
                sql = """
                    SELECT COUNT(DISTINCT
                        LPAD(
                            regexp_replace(
                                COALESCE(CAST(niin AS VARCHAR), CAST(nsn AS VARCHAR), ''),
                                '[^0-9]', ''
                            ),
                        9, '0')
                    ) AS n
                    FROM read_parquet(?)
                    WHERE UPPER(TRIM(cage)) = ?
                """
                n = DUCK_CONN.execute(sql, (str(path), safe_cage)).fetchone()[0]
            else:
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
    threshold: Optional[float] = 0
):
    where_parts = []
    params = []

    # 1. Base Filters (Verified against ETL columns)
    if cage and cage != "AGGREGATE":
        # ✅ FIX: ETL confirms column is 'vendor_cage'
        where_parts.append("(vendor_cage = ?)")
        safe_cage = sanitize(cage)
        params.append(safe_cage)
    elif name:
        # ✅ FIX: ETL confirms 'vendor_name' exists. 
        # Removed 'ultimate_parent_name' as it is NOT in your transactions.parquet ETL list.
        where_parts.append("upper(vendor_name) LIKE ?")
        safe_name = f"%{sanitize(name)}%"
        params.append(safe_name)
    else:
        return []

    # 2. Global Filters
    if agency:
        # ✅ FIX: ETL confirms 'sub_agency' and 'parent_agency' exist
        where_parts.append("(upper(sub_agency) = ? OR upper(parent_agency) = ?)")
        safe_ag = sanitize(agency)
        params.extend([safe_ag, safe_ag])

    if years and len(years) > 0:
        placeholders = ",".join(["?" for _ in years])
        where_parts.append(f"year IN ({placeholders})")
        params.extend(years)

    if threshold and threshold > 0:
        where_parts.append("spend_amount >= ?")
        params.append(threshold * 1_000_000)

    # 3. Execute via DuckDB (Disk-based, Low RAM)
    where_clause = " AND ".join(where_parts)

    try:
        path = LOCAL_CACHE_DIR / "transactions.parquet"
        if not path.exists():
             return []

        df = get_subset_from_disk(
            "transactions.parquet",
            where_clause=where_clause,
            params=tuple(params),
            # ✅ FIX: Only select columns guaranteed by your ETL
            columns_sql="contract_id, action_date, sub_agency, parent_agency, description, spend_amount, naics_code, psc",
            order_by_sql="action_date DESC",
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
def get_company_opportunities_recommended(cage: Optional[str] = None, name: Optional[str] = None):
    # 1. Reuse existing profile logic to find capabilities
    profile = get_company_profile(cage, name)

    if not profile.get('found') or not profile.get('top_naics'):
        return []

    # 2. Extract and Clean NAICS codes
    raw_naics_list = profile['top_naics']
    clean_naics_list = []

    for n in raw_naics_list:
        code_part = str(n).split(' - ')[0].strip().split('.')[0]
        if len(code_part) >= 3:
            clean_naics_list.append(code_part)

    if not clean_naics_list:
        return []

    # 3. Search the In-Memory Opportunities Cache (Fast & Safe)
    df_opps = GLOBAL_CACHE.get("df_opportunities", pd.DataFrame())
    if df_opps.empty:
        return []

    # Create regex pattern for fast filtering
    pattern = "|".join([re.escape(c) for c in sorted(set(clean_naics_list))])
    mask = df_opps['naics'].astype(str).str.contains(pattern, regex=True, na=False)

    # Sort by deadline and take top 50
    filtered = df_opps[mask].sort_values("deadline", ascending=True).head(50)

    # 4. Format for UI (Including the critical alias)
    results = []
    for row in filtered.itertuples():
        # Handle potential missing attributes safely
        sol_val = getattr(row, 'sol_num', '') or getattr(row, 'sol#', '')
        
        results.append({
            "noticeid": getattr(row, 'id', ''),
            "title": getattr(row, 'title', ''),
            
            # ✅ THE FIX: Send BOTH keys to ensure UI compatibility
            "sol_num": sol_val, 
            "sol#": sol_val,     
            
            "department_indagency": getattr(row, 'agency', ''),
            "responsedeadline": getattr(row, 'deadline', ''),
            "setaside": getattr(row, 'set_aside_type', ''),
            "primarycontactemail": getattr(row, 'poc_email', '')
        })
    return results


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

@app.get("/api/nsn/profile")
def get_nsn_profile(nsn: str):
    # 1) Clean Input (Standard 9-digit NIIN logic)
    clean = ''.join(filter(str.isdigit, str(nsn)))
    safe_niin = clean.zfill(9) if len(clean) < 9 else clean[-9:]

    # 2) Query from Disk (Hybrid mode: products are NOT in RAM)
    df = get_subset_from_disk(
        "products.parquet",
        where_clause="niin = ?",
        params=(safe_niin,),
        limit=50000
    )


    if df.empty:
        return {"found": False}

    # 3) Treat the subset as the match
    match = df.copy()

    match["total_revenue"] = pd.to_numeric(match.get("total_revenue", 0), errors="coerce").fillna(0)
    match["total_units_sold"] = pd.to_numeric(match.get("total_units_sold", 0), errors="coerce").fillna(0)


    total_rev = float(match["total_revenue"].sum())
    total_units = int(match["total_units_sold"].sum())

    # 5) Best Row Logic (prefer a real description)
    best_row = match.iloc[0]
    if "description" in match.columns:
        for r in match.itertuples(index=False):
            desc = getattr(r, "description", None)
            if desc and str(desc).strip() and str(desc).strip().upper() != "UNKNOWN":
                best_row = r
                break
    else:
        best_row = match.iloc[0]

    # Helper getters (works whether best_row is Series or namedtuple)
    def g(obj, key, default=None):
        try:
            return getattr(obj, key)
        except:
            try:
                return obj.get(key, default)
            except:
                return default

    fsc_code = g(best_row, "fsc_code", "") or g(best_row, "fsc", "")
    desc = g(best_row, "description", None) or g(best_row, "item_name", None) or "Unknown"

    avg_unit_price = g(best_row, "avg_unit_price", 0) or 0
    last_sold_date = g(best_row, "last_sold_date", None)
    trend = g(best_row, "annual_revenue_trend", "") or ""

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

        "has_sales_history": bool(total_rev > 0)
    }



@app.get("/api/nsn/suppliers")
def get_nsn_suppliers(nsn: str):
    safe_niin = get_niin(nsn).zfill(9)

    query = f"""
    WITH 
    -- 1. Identify Sales (Sanitized)
    item_sales AS (
        SELECT 
            TRIM(UPPER(vendor_cage)) as vendor_cage,
            MAX(vendor_name) as vendor_name,
            COUNT(DISTINCT contract_id) as contracts,
            MAX(action_date) as last_sold,
            SUM(spend_amount) as total_revenue
        FROM "market_intel_gold"."dashboard_master_view"
        WHERE substr(regexp_replace(nsn, '[^0-9]', ''), 5, 9) = {sql_literal(safe_niin)}
        GROUP BY 1
    ),
    
    -- 2. Identify Approved Sources & AGGREGATE Part Numbers [Image of SQL Array Aggregation]
    approved_list AS (
        SELECT 
            TRIM(UPPER(cage_code)) as cage_code,
            -- ✅ Collect all part numbers for this CAGE into a single comma-separated string
            array_join(array_agg(DISTINCT part_number), ', ') as part_numbers
        FROM "market_intel_silver"."ref_approved_sources"
        WHERE LPAD(CAST(niin AS VARCHAR), 9, '0') = {sql_literal(safe_niin)}
        GROUP BY 1
    ),

    -- 3. Master List of CAGEs
    all_cages AS (
        SELECT vendor_cage as cage FROM item_sales WHERE vendor_cage IS NOT NULL
        UNION
        SELECT cage_code as cage FROM approved_list WHERE cage_code IS NOT NULL
    ),

    -- 4. SAM Registry
    sam_lookup AS (
        SELECT 
            TRIM(UPPER(cage_code)) as cage_code, 
            MAX(legal_business_name) as legal_name
        FROM "market_intel_silver"."ref_sam_entities"
        WHERE TRIM(UPPER(cage_code)) IN (SELECT cage FROM all_cages)
        GROUP BY 1
    ),

    -- 5. Global Gold History
    gold_global_lookup AS (
        SELECT 
            TRIM(UPPER(vendor_cage)) as vendor_cage, 
            MAX(vendor_name) as history_name
        FROM "market_intel_gold"."dashboard_master_view"
        WHERE TRIM(UPPER(vendor_cage)) IN (SELECT cage FROM all_cages)
        GROUP BY 1
    ),
    
    -- 6. Gov Dictionary
    gov_dictionary AS (
        SELECT * FROM (VALUES
            ('19207', 'US ARMY TACOM - DESIGN ACTIVITY'),
            ('81349', 'MILITARY STANDARDS / PROMULGATING ACTIVITY'),
            ('80205', 'NATIONAL AEROSPACE STANDARDS (NAS)'),
            ('96906', 'MILITARY STANDARDS (MS)'),
            ('57685', 'NAVAL AIR SYSTEMS COMMAND'),
            ('81348', 'FEDERAL SPECIFICATIONS'),
            ('88044', 'AERONAUTICAL STANDARDS GROUP'),
            ('9009H', 'WSK PZL-KALISZ S.A. (POLAND)'),
            ('100CB', 'ARITEX CADING S.A. (SPAIN)'),
            ('A486G', 'PATRIA AVIATION OY (FINLAND)'),
            ('K1037', 'HANWHA AEROSPACE (SOUTH KOREA)'),
            ('D0019', 'RHEINMETALL LANDSYSTEME (GERMANY)')
        ) AS t(cage, name)
    )

    SELECT
        COALESCE(
            i.vendor_name, s.legal_name, g.history_name, gov.name,
            'Unknown Manufacturer (CAGE: ' || c.cage || ')'
        ) as vendor,
        
        c.cage,
        
        -- ✅ Return the Part Numbers
        COALESCE(a.part_numbers, '—') as part_numbers,
        
        COALESCE(i.contracts, 0) as contracts,
        i.last_sold,
        COALESCE(i.total_revenue, 0) as total_revenue,
        
        CASE WHEN a.cage_code IS NOT NULL THEN true ELSE false END as is_approved_source

    FROM all_cages c
    LEFT JOIN item_sales i ON c.cage = i.vendor_cage
    LEFT JOIN approved_list a ON c.cage = a.cage_code
    LEFT JOIN sam_lookup s ON c.cage = s.cage_code
    LEFT JOIN gold_global_lookup g ON c.cage = g.vendor_cage
    LEFT JOIN gov_dictionary gov ON c.cage = gov.cage
    
    ORDER BY total_revenue DESC NULLS LAST, is_approved_source DESC
    """
    return run_athena_query(query)

@app.get("/api/nsn/platforms")
def get_nsn_platforms(nsn: str):
    safe_niin = get_niin(nsn)

    df = get_subset_from_disk(
        "products.parquet",
        where_clause="niin = ?",
        params=(safe_niin,),
        limit=50000
    )
    if df.empty:
        return []

    if "platform_family" not in df.columns:
        return []

    df["total_revenue"] = pd.to_numeric(df.get("total_revenue", 0), errors="coerce").fillna(0)

    grouped = (
        df.groupby("platform_family", observed=True)
        .agg(total_revenue=("total_revenue", "sum"), contracts=("cage", "count"))
        .reset_index()
        .sort_values("total_revenue", ascending=False)
        .head(10)
    )

    out = []
    for r in grouped.itertuples(index=False):
        plat = getattr(r, "platform_family", None)
        if not plat:
            continue
        if str(plat).strip().upper() == "UNKNOWN":
            continue
        out.append({
            "platform": str(plat),
            "spend": float(getattr(r, "total_revenue", 0) or 0),
            "contracts": int(getattr(r, "contracts", 0) or 0)
        })
    return out


@app.get("/api/nsn/history")
def get_nsn_history(nsn: str):
    # 1) Clean Input (always 9-digit NIIN)
    safe_niin = get_niin(nsn)

    # 2) Query from Disk (only this NIIN)
    df = get_subset_from_disk(
        "products.parquet",
        where_clause="niin = ?",
        params=(safe_niin,),
        limit=50000
    )

    if df.empty:
        return []

    if "annual_revenue_trend" not in df.columns:
        return []

    # 3) Aggregate all vendor rows into a single FY->spend map
    fy_map: Dict[int, float] = {}

    for trend_str in df["annual_revenue_trend"]:
        if not trend_str or pd.isna(trend_str):
            continue

        for seg in str(trend_str).split("|"):
            if ":" not in seg:
                continue
            try:
                y_str, amt_str = seg.split(":", 1)
                fy = int(str(y_str).strip())          # treat trend year as fiscal year
                amt = float(str(amt_str).strip())
                fy_map[fy] = fy_map.get(fy, 0.0) + amt
            except:
                continue

    if not fy_map:
        return []

    # 4) Chart formatting: gap-fill missing fiscal years so the chart is continuous
    min_fy = int(min(fy_map.keys()))
    max_fy = int(max(fy_map.keys()))

    results = []
    for fy in range(min_fy, max_fy + 1):
        results.append({
            "label": f"FY{fy}",
            "spend": float(fy_map.get(fy, 0.0))
        })

    return results




@app.get("/api/nsn/contracts")
def get_nsn_contracts(nsn: str, limit: int = 50, offset: int = 0):
    niin = get_niin(nsn) # Helper ensures this is digits only, but let's be safe

    # Guardrails
    limit_i = safe_int(limit, 50, 1, 200)
    offset_i = safe_int(offset, 0, 0, 2_000_000)

    # ✅ FIX: Use safe LIKE logic with '#' escape
    # Even though NIIN is usually digits, this protects against weird input
    safe_niin_pattern = sql_like_contains(niin)

    query = f"""
    SELECT
        contract_id,
        action_date,
        COALESCE(sub_agency, parent_agency) AS agency,
        vendor_name,
        vendor_cage,
        platform_family,
        psc,
        naics_code,
        spend_amount,
        description
    FROM "market_intel_gold"."dashboard_master_view"
    WHERE upper(coalesce(nsn,'')) LIKE {safe_niin_pattern} ESCAPE '#'
      AND spend_amount IS NOT NULL
    ORDER BY action_date DESC
    OFFSET {offset_i}
    LIMIT {limit_i}
    """
    return cached_athena_query(query)


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
    limit_i = safe_int(limit, 12, 4, 50)
    cond: List[str] = ["1=1"]

    ys = safe_years(years, min_year=1900, max_year=2200, max_len=50)
    if ys:
        years_csv = ",".join([str(y) for y in ys])
        cond.append(
            "("
            " (year(try_cast(substr(action_date, 1, 10) as date))"
            "  + if(month(try_cast(substr(action_date, 1, 10) as date)) >= 10, 1, 0))"
            f" IN ({years_csv})"
            ")"
        )

    if vendor:
        cond.append(safe_contains_upper("vendor_name", vendor))
    if parent:
        cond.append(safe_contains_upper("ultimate_parent_name", parent))
    if cage:
        cond.append(safe_contains_upper("vendor_cage", cage))
    if agency:
        a = agency
        cond.append(f"({safe_contains_upper('sub_agency', a)} OR {safe_contains_upper('parent_agency', a)})")
    if platform:
        cond.append(safe_contains_upper("platform_family", platform))
    if psc:
        cond.append(safe_contains_upper("psc", psc))
    if domain:
        d = domain
        cond.append(f"({safe_contains_upper('psc', d)} OR {safe_contains_upper('naics_code', d)})")

    where_clause = " AND ".join(cond)

    niin_expr = "substr(regexp_replace(nsn, '[^0-9]', ''), -9)"

    query = f"""
    WITH top_spending AS (
        SELECT
            LPAD({niin_expr}, 9, '0') AS join_niin,
            MAX(description) as raw_desc,
            SUM(spend_amount) AS spend,
            COUNT(DISTINCT contract_id) AS contracts
        FROM "market_intel_gold"."dashboard_master_view"
        WHERE {where_clause}
          AND nsn IS NOT NULL
          AND length(regexp_replace(nsn, '[^0-9]', '')) >= 8
        GROUP BY 1
        ORDER BY spend DESC
        LIMIT {limit_i}
    ),
    catalog_match AS (
        SELECT 
            LPAD(CAST(niin AS VARCHAR), 9, '0') as niin,
            item_name,
            fsc
        FROM "market_intel_silver"."ref_flis_nsn"
        WHERE LPAD(CAST(niin AS VARCHAR), 9, '0') IN (SELECT join_niin FROM top_spending)
    )
    SELECT
        CASE 
            WHEN c.fsc IS NOT NULL THEN CAST(c.fsc AS VARCHAR) || '-' || t.join_niin 
            ELSE t.join_niin 
        END AS nsn,
        COALESCE(
            c.item_name, 
            CASE 
                WHEN strpos(t.raw_desc, '!') > 0 THEN split_part(t.raw_desc, '!', 2)
                ELSE t.raw_desc 
            END,
            'Unknown Item'
        ) AS description,
        t.spend,
        t.contracts
    FROM top_spending t
    LEFT JOIN catalog_match c ON t.join_niin = c.niin
    ORDER BY t.spend DESC
    """
    return cached_athena_query(query)



# ==========================================
#        AWARD / CONTRACT DASHBOARD
# ==========================================



# --- UPDATE THIS FUNCTION ---
@app.get("/api/award/profile")
def get_award_profile(id: str):
    if not id:
        return None

    safe_id = sanitize(id)

    # Guard: file must exist
    path = str(LOCAL_CACHE_DIR / "transactions.parquet")
    if not os.path.exists(path):
        return None

    # Pull a bounded set for this contract_id (contracts can have multiple actions)
    df = get_subset_from_disk(
        "transactions.parquet",
        where_clause="contract_id = ?",
        params=(safe_id,),
        order_by_sql="action_date DESC",
        limit=5000
    )

    if df.empty:
        return None

    # Ensure types
    if "spend_amount" in df.columns:
        df["spend_amount"] = pd.to_numeric(df["spend_amount"], errors="coerce").fillna(0)
    else:
        df["spend_amount"] = 0

    # Latest/earliest
    df_sorted = df.sort_values("action_date", ascending=False, kind="mergesort")
    latest = df_sorted.iloc[0]
    earliest = df_sorted.iloc[-1]
    total_spend = float(df_sorted["spend_amount"].sum())

    # Agency fallback
    sub_ag = latest.get("sub_agency") if "sub_agency" in df_sorted.columns else None
    parent_ag = latest.get("parent_agency") if "parent_agency" in df_sorted.columns else None
    agency = sub_ag if pd.notna(sub_ag) and str(sub_ag).strip() else parent_ag

    return {
        "contract_id": latest.get("contract_id"),
        "vendor_name": latest.get("vendor_name"),
        "vendor_cage": latest.get("vendor_cage"),
        "agency": agency,
        "sub_agency": sub_ag,
        "description": latest.get("description"),

        "platform_family": latest.get("platform_family"),
        "city": latest.get("city") if "city" in df_sorted.columns else None,
        "state": latest.get("state") if "state" in df_sorted.columns else None,
        "country": latest.get("country") if "country" in df_sorted.columns else None,

        "naics_code": latest.get("naics_code"),
        "psc": latest.get("psc"),

        "pricing_type": latest.get("pricing_type") if "pricing_type" in df_sorted.columns else None,
        "competition_type": latest.get("competition_type") if "competition_type" in df_sorted.columns else None,
        "offers_count": latest.get("offers_count") if "offers_count" in df_sorted.columns else None,
        "set_aside_type": latest.get("set_aside_type") if "set_aside_type" in df_sorted.columns else None,
        "solicitation_id": latest.get("solicitation_identifier") if "solicitation_identifier" in df_sorted.columns else None,

        "total_spend": total_spend,
        "start_date": earliest.get("action_date"),
        "last_action_date": latest.get("action_date")
    }


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

    path = os.path.join(LOCAL_CACHE_DIR, "transactions.parquet")
    if not os.path.exists(path):
        return []

    df = get_subset_from_disk(
        "transactions.parquet",
        where_clause="contract_id = ?",
        params=(safe_id,),
        order_by_sql="action_date ASC",
        limit=200
    )

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


@app.get("/api/award/search")
def search_awards(
    q: Optional[str] = None, 
    limit: int = 20, 
    offset: int = 0,
    years: Optional[List[int]] = Query(None),
    vendor: Optional[str] = None,
    agency: Optional[str] = None,
    platform: Optional[str] = None,
    psc: Optional[str] = None,
    domain: Optional[str] = None
):
    where_parts = ["1=1"]
    params = []

    # 2. Text Search (q) - Checks Contract ID, Vendor, Description, AND Cage
    if q:
        safe_q = f"%{sanitize(q)}%" # Add wildcards here
        # Note: We append the SAME parameter 4 times because we have 4 ? placeholders
        where_parts.append("""(
            upper(coalesce(contract_id, '')) LIKE ? OR 
            upper(coalesce(vendor_name, '')) LIKE ? OR 
            upper(coalesce(description, '')) LIKE ? OR
            upper(coalesce(vendor_cage, '')) LIKE ?
        )""")
        params.extend([safe_q, safe_q, safe_q, safe_q])

    # 3. Exact/Category Filters
    if vendor:
        where_parts.append("upper(vendor_name) LIKE ?")
        params.append(f"%{sanitize(vendor)}%")
    
    if platform:
        where_parts.append("upper(platform_family) = ?")
        params.append(sanitize(platform))

    if agency:
        where_parts.append("(upper(sub_agency) = ? OR upper(parent_agency) = ?)")
        a = sanitize(agency)
        params.extend([a, a])

    # 4. Complex Filters (PSC & Domain)
    if psc:
        where_parts.append("upper(psc) LIKE ?")
        params.append(f"%{sanitize(psc)}%")

    if domain:
        where_parts.append("(upper(naics_code) LIKE ? OR upper(psc) LIKE ?)")
        d = f"%{sanitize(domain)}%"
        params.extend([d, d])

    if years and len(years) > 0:
        placeholders = ",".join(["?" for _ in years])
        where_parts.append(f"year IN ({placeholders})")
        params.extend(years)

    # 5. Optimization: High Value Threshold if NO filters are active
    is_filtering = q or vendor or agency or platform or psc or domain or (years and len(years) > 0)
    if not is_filtering:
        where_parts.append("spend_amount > 100000")

    # 6. Run Query on Disk
    where_sql = " AND ".join(where_parts)
    
    try:
        # Check if file exists to prevent crash
        path = LOCAL_CACHE_DIR / "transactions.parquet"
        if not path.exists():
             return {"data": [], "total": 0, "offset": offset, "limit": limit}

        # A. Get Total Count (Fast)
        # We need to manually construct the count query with the same params
        with DUCK_LOCK:
            conn = ensure_duck_conn()

            count_sql = f"SELECT COUNT(*) FROM read_parquet(?) WHERE {where_sql}"
            total_count = conn.execute(count_sql, (str(path),) + tuple(params)).fetchone()[0]


        # B. Get Page Data
        df = get_subset_from_disk(
            "transactions.parquet",
            where_clause=where_sql,
            params=tuple(params),
            columns_sql="contract_id, action_date, vendor_name, vendor_cage, sub_agency, parent_agency, description, spend_amount",
            order_by_sql="action_date DESC",
            limit=limit,
            offset=offset
        )
        
        # 7. Format Output (Exact Keys for Frontend)
        results = []
        for row in df.itertuples():
            # Logic: Fallback to parent_agency if sub_agency is missing
            final_agency = row.sub_agency if pd.notna(row.sub_agency) else row.parent_agency
            
            results.append({
                "contract_id": row.contract_id,
                "vendor_name": row.vendor_name,
                "vendor_cage": row.vendor_cage,
                "last_action_date": str(row.action_date), # ✅ Mapped for UI
                "description": row.description,
                "total_spend": float(row.spend_amount or 0), # ✅ Mapped for UI
                "agency": final_agency,
                "sub_agency": row.sub_agency
            })

        return {
            "data": results,
            "total": total_count,
            "offset": offset,
            "limit": limit
        }

    except Exception as e:
        print(f"Search Query Error: {e}")
        return {"data": [], "total": 0, "error": str(e)}

@app.get("/api/award/stats")
def get_database_stats():
    """
    Returns the total row count of the database for the 'Searching X Records' badge.
    """
    query = 'SELECT count(*) as total FROM "market_intel_gold"."dashboard_master_view"'
    # This queries metadata and is usually very fast
    return cached_athena_query(query)


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
    limit: int = 50,
    offset: int = 0
):
    # Use Global Cache (RAM)
    df = GLOBAL_CACHE.get("df_opportunities", pd.DataFrame())
    if df.empty: return []

    mask = pd.Series(True, index=df.index)
    today = date.today()
    today_str = today.isoformat() 

    # --- 1. FILTER: DEADLINE (Keep this, it's correct) ---
    if 'deadline' in df.columns:
        # Only show future/active opportunities
        mask &= (df['deadline'] >= today_str)

    # --- 2. FILTER: DATA QUALITY (RELAXED) ---
    # ✅ FIX: Accept rows with EITHER a Sol Num OR a Notice ID
    # This restores the millions of records that are valid but lack a formal sol_num
    if 'sol_num' in df.columns and 'id' in df.columns:
        has_sol = (df['sol_num'].notna()) & (df['sol_num'] != '')
        has_id = (df['id'].notna()) & (df['id'] != '')
        mask &= (has_sol | has_id)
    elif 'sol_num' in df.columns:
        # Fallback if ID column missing (unlikely)
        mask &= (df['sol_num'].notna()) & (df['sol_num'] != '')

    # --- 3. STANDARD FILTERS ---
    if naics:
        safe_naics = sanitize(naics)
        mask &= df['naics'].astype(str).str.contains(safe_naics, regex=False, na=False)
        
    if set_aside:
        safe_set = sanitize(set_aside)
        mask &= df['set_aside_type'].astype(str).str.upper().str.contains(safe_set, regex=False, na=False)
        
    if state:
        safe_state = sanitize(state)
        mask &= (df['state'] == safe_state)
        
    if source:
        safe_source = sanitize(source)
        mask &= (df['source_system'] == safe_source)

    # --- 4. GLOBAL BRIDGE FILTERS ---
    if keyword:
        safe_k = sanitize(keyword)
        if 'search_text' in df.columns:
            mask &= df['search_text'].str.contains(safe_k, regex=False, na=False)
        else:
            mask &= (df['title'].astype(str).str.upper().str.contains(safe_k, regex=False, na=False)) | \
                    (df['description'].astype(str).str.upper().str.contains(safe_k, regex=False, na=False))

    if agency:
        safe_ag = sanitize(agency)
        if "DLA" in safe_ag or "LOGISTICS" in safe_ag:
             ag_mask = (df['agency'].astype(str).str.upper().str.contains("LOGISTICS", regex=False, na=False)) | \
                       (df['sub_agency'].astype(str).str.upper().str.contains("LOGISTICS", regex=False, na=False)) | \
                       (df['source_system'] == 'DLA')
             mask &= ag_mask
        else:
             ag_mask = (df['agency'].astype(str).str.upper().str.contains(safe_ag, regex=False, na=False)) | \
                       (df['sub_agency'].astype(str).str.upper().str.contains(safe_ag, regex=False, na=False))
             mask &= ag_mask

    if platform:
        safe_plat = sanitize(platform)
        p_mask = (df['title'].astype(str).str.upper().str.contains(safe_plat, regex=False, na=False)) | \
                 (df['description'].astype(str).str.upper().str.contains(safe_plat, regex=False, na=False))
        mask &= p_mask

    if domain:
        safe_domain = sanitize(domain)
        if len(safe_domain) > 0 and (safe_domain[0].isdigit() or (len(safe_domain) == 4 and safe_domain[0].isalpha())):
            d_mask = (df['psc'].astype(str).str.startswith(safe_domain, na=False)) | \
                     (df['naics'].astype(str).str.startswith(safe_domain, na=False))
            mask &= d_mask
        else:
            d_mask = (df['title'].astype(str).str.upper().str.contains(safe_domain, regex=False, na=False)) | \
                     (df['description'].astype(str).str.upper().str.contains(safe_domain, regex=False, na=False))
            mask &= d_mask

    filtered = df[mask]
    
    if "deadline" in filtered.columns:
        filtered = filtered.sort_values("deadline", ascending=True)

    start = int(offset)
    end = start + int(limit)
    page = filtered.iloc[start:end]

    results = []
    
    for row in page.itertuples():
        # Date Logic
        days_left = 0
        try:
            dt_str = str(row.deadline)[:10]
            dt_obj = datetime.strptime(dt_str, "%Y-%m-%d").date()
            delta = dt_obj - today
            days_left = delta.days
        except:
            days_left = 0

        # Race condition safety: Skip if it expired in the last few seconds
        if days_left < 0: 
            continue

        # ✅ FIX: Smart Identifier
        # If sol_num is missing, fallback to Notice ID.
        # This guarantees the UI has something to display and link.
        display_sol = getattr(row, 'sol_num', '')
        if not display_sol or str(display_sol) == 'nan':
            display_sol = getattr(row, 'id', '') # Fallback to Notice ID

        results.append({
            "id": getattr(row, 'id', ''),
            "title": getattr(row, 'title', ''),
            "agency": getattr(row, 'agency', ''),
            "sub_agency": getattr(row, 'sub_agency', ''),
            
            # ✅ Return the computed valid ID here
            "sol_num": display_sol,
            
            "due_date": getattr(row, 'deadline', ''),
            "deadline": getattr(row, 'deadline', ''),
            "set_aside": getattr(row, 'set_aside_type', ''),
            "set_aside_type": getattr(row, 'set_aside_type', ''),
            "naics": getattr(row, 'naics', ''),
            "psc": getattr(row, 'psc', ''),
            "description": str(getattr(row, 'description', ''))[:2000],
            "primarycontactemail": getattr(row, 'poc_email', ''),
            "source_system": getattr(row, 'source_system', ''),
            "days_left": int(days_left),
            "total_matches": len(filtered)
        })

    return results

@app.get("/api/pipeline/recent-wins")
def get_recent_wins():
    query = """
    SELECT 
        vendor_name,
        vendor_cage,
        parent_agency as agency,
        contract_id,
        spend_amount as amount,
        action_date as signed_date,
        description
    FROM "market_intel_gold"."dashboard_master_view"
    WHERE action_date >= cast(current_date - interval '180' day as varchar) -- ✅ Changed to 6 Months
      AND spend_amount > 1000000 
    ORDER BY action_date DESC
    LIMIT 50
    """
    return cached_athena_query(query)

# --- ADD THIS TO api.py ---

@app.get("/api/pipeline/details")
def get_solicitation_details(id: str):
    if not id:
        return {"found": False}

    raw = str(id).strip()
    # allow reasonably safe identifier characters
    safe_id = safe_ident(raw)

    query = f"""
    SELECT 
        id,
        title,
        agency,
        sol_num,
        sol_num as noticeid,
        deadline,
        set_aside_type as set_aside,
        office,
        poc_email,
        description,
        url,
        state,
        naics,
        psc,
        sub_agency
    FROM "market_intel_gold"."view_unified_opportunities_dod"
    WHERE upper(sol_num) = {sql_literal(safe_id)} OR upper(id) = {sql_literal(safe_id)}
    LIMIT 1
    """
    results = run_athena_query(query)
    if not results:
        return {"found": False}
    return results[0]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)