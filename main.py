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
import re
import concurrent.futures
import time
import gc 
import duckdb # ✅ NEW: Disk-based query engine
import asyncio  # <--- Add this
from pathlib import Path
import logging
import random

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("mimir-api")

# ✅ GLOBAL MEMORY STORE 
global_data: Dict[str, pd.DataFrame] = {}

# ✅ FIX: SINGLE SOURCE OF TRUTH STARTUP
# ✅ FIX: SINGLE SOURCE OF TRUTH STARTUP
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("API starting up (Hybrid V5 Mode)...")
    
    # 1. Initialize DuckDB (IN-MEMORY MODE)
    # This fixes the "Conflicting lock" error by giving every worker its own RAM database.
    global DUCK_CONN
    with DUCK_LOCK:
        try:
            # CHANGE: connect to ":memory:" instead of a file path
            DUCK_CONN = duckdb.connect(":memory:", read_only=False)
            
            # Install extensions
            DUCK_CONN.execute("INSTALL parquet; LOAD parquet;")
            DUCK_CONN.execute("PRAGMA threads=4;") 
            DUCK_CONN.execute("PRAGMA memory_limit='2GB';")
            
            logger.info("DuckDB (In-Memory) connected successfully")
        except Exception:
            logger.exception("DuckDB init failed")

    # 2. Fire-and-forget Background Data Load
    loop = asyncio.get_running_loop()
    loop.run_in_executor(None, reload_all_data)
    
    yield
    
    # 3. Shutdown
    logger.info("API shutting down...")
    with CACHE_LOCK:
        global_data.clear()
        GLOBAL_CACHE.clear()
        gc.collect()
    
    with DUCK_LOCK:
        if DUCK_CONN:
            try:
                DUCK_CONN.close()
            except Exception:
                pass

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
    # For LIKE '%...%': escape %, _ and \ so user input cannot widen matches unexpectedly.
    if s is None:
        raw = ""
    else:
        raw = str(s)
    raw = raw.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return "'%" + raw.replace("'", "''") + "%'"

def safe_ident(s: Any) -> str:
    t = ("" if s is None else str(s)).strip().upper()
    if not t:
        return ""
    if not SAFE_IDENT_RE.match(t):
        raise HTTPException(status_code=400, detail="Invalid identifier")
    return t

def safe_contains_upper(field_sql: str, user_value: Any) -> str:
    # Builds: upper(field) LIKE '%VALUE%' ESCAPE '\'
    v = "" if user_value is None else str(user_value)
    return f"upper({field_sql}) LIKE {sql_like_contains(v.upper())} ESCAPE '\\\\'"

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


# ✅ Pass lifespan to FastAPI
app = FastAPI(
    title="Mimir Hybrid Intelligence API - Instant Mode V5",
    default_response_class=JSONResponse,
    lifespan=lifespan 
)

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

@app.get("/ready")
def ready_check():
    with CACHE_LOCK:
        df = GLOBAL_CACHE.get("df", pd.DataFrame())
        is_loading = bool(GLOBAL_CACHE.get("is_loading", False))
        last_loaded = GLOBAL_CACHE.get("last_loaded", 0)
    with DUCK_LOCK:
        duck_ok = DUCK_CONN is not None
    ready = (not df.empty) and duck_ok
    return {
        "status": "ok" if ready else "starting",
        "ready": bool(ready),
        "is_loading": bool(is_loading),
        "last_loaded": last_loaded,
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
LOCAL_CACHE_DIR = Path(os.getenv("LOCAL_CACHE_DIR", "./local_data"))
LOCAL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
DATABASE = 'market_intel_gold'

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

# --- GLOBAL STATE ---
GLOBAL_CACHE = {
    "df": pd.DataFrame(),           
    "geo_df": pd.DataFrame(),       
    "profiles_df": pd.DataFrame(),  
    "options": {},
    "search_index": [],
    "is_loading": False,
    "last_loaded": 0,

    "cage_name_map": {},   # cage_code -> vendor_name
    "location_map": {}, 
}

# ✅ DuckDB: single shared connection + lock (safe + fast)
DUCKDB_PATH = LOCAL_CACHE_DIR / "mimir.duckdb"
DUCK_CONN = None
DUCK_LOCK = threading.RLock()
RELOAD_LOCK = threading.Lock()

CACHE_LOCK = threading.RLock()

def get_cache_snapshot() -> Dict[str, Any]:
    with CACHE_LOCK:
        # return references; handlers should not mutate
        return {
            "GLOBAL_CACHE": GLOBAL_CACHE,
            "global_data": global_data,
            "DUCK_CONN": DUCK_CONN,
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

def run_athena_query(query: str):
    if not query or not str(query).strip():
        return []

    start_ts = time.time()
    qid = None

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

        if final_state != "SUCCEEDED":
            reason = ""
            try:
                reason = (status["QueryExecution"]["Status"] or {}).get("StateChangeReason", "") if status else ""
            except Exception:
                reason = ""
            logger.error("Athena query failed state=%s qid=%s reason=%s", final_state, qid, reason)
            return []

        outloc = status["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
        key = outloc.replace(f"s3://{BUCKET_NAME}/", "")
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        df = pd.read_csv(BytesIO(obj["Body"].read()))
        out = df.where(pd.notnull(df), None).to_dict(orient="records")

        logger.info("Athena query ok qid=%s rows=%s dur_ms=%.1f",
                    qid, len(out), (time.time() - start_ts) * 1000.0)
        return out

    except Exception:
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
    # We DO NOT allow semicolons or comments even if parameterized
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
        if limit > 2000: 
            limit = 2000 

        # Validate ORDER BY
        order_clause = ""
        if order_by_sql:
            parts = order_by_sql.strip().split()
            col = parts[0]
            # Must be in allowed list
            if col not in ALLOWED_ORDER_BY:
                 # Fallback to safe default rather than crashing
                 col = "action_date"
            
            direction = parts[1].upper() if len(parts) > 1 else "DESC"
            if direction not in {"ASC", "DESC"}: 
                direction = "DESC"
            
            order_clause = f" ORDER BY {col} {direction}"

        # 3. Construct Query
        # We parameterize the filename logic to avoid path traversal
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
            if DUCK_CONN is None:
                # Emergency Re-connect
                DUCK_CONN = duckdb.connect(":memory:", read_only=False)
                DUCK_CONN.execute("INSTALL parquet; LOAD parquet;")
            
            all_params = (str(path),) + tuple(local_params)
            return DUCK_CONN.execute(sql, all_params).fetchdf()

    except Exception:
        logger.exception(f"DuckDB Query Failed: {filename}")
        return pd.DataFrame()


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
    """
    Aggregates data from the global cache for a specific parent entity.
    Used to generate the "Corporate Group" view.
    """
    df = GLOBAL_CACHE["df"]
    if df.empty or not parent_name: return None

    clean_name = parent_name.strip().upper().replace("'", "")
    
    # Identify the column to group by
    col_to_check = 'clean_parent' if 'clean_parent' in df.columns else 'ultimate_parent_name'
    if col_to_check not in df.columns: return None

    # Filter for the specific parent family
    mask = df[col_to_check].astype(str).str.upper().eq(clean_name)
    
    if not mask.any(): return None

    slice_df = df[mask]
    
    # --- SAFE METADATA EXTRACTION ---
    
    # 1. Top Capabilities (NAICS)
    top_naics = []
    
    # Check if we have both columns available
    if 'naics_code' in slice_df.columns and 'naics_description' in slice_df.columns:
        # ✅ FIX: Convert to string first to avoid Categorical errors
        code_series = slice_df['naics_code'].astype(str)
        # Convert Categorical to String before filling NA
        desc_series = slice_df['naics_description'].astype(object).fillna("Unknown").astype(str)
        
        combined_naics = code_series + " - " + desc_series
        
        # Count the top 5 most common COMBINED strings
        top_naics = combined_naics.value_counts().head(5).index.tolist()
        
    elif 'naics_code' in slice_df.columns:
        top_naics = slice_df['naics_code'].dropna().value_counts().head(5).index.tolist()
            
    # 2. Top Platforms
    top_platforms = []
    if 'platform_family' in slice_df.columns:
        top_platforms = slice_df['platform_family'].value_counts().head(5).index.tolist()
    
    return {
        "total_obligations": float(slice_df['total_spend'].sum()),
        "total_contracts": int(slice_df['contract_count'].sum()),
        "last_active": int(slice_df['year'].max()),
        "top_naics": top_naics, 
        "top_platforms": top_platforms
    }

def reload_all_data():
    if not RELOAD_LOCK.acquire(blocking=False):
        logger.info("Reload already in progress, skipping.")
        return

    try:
        logger.info("STARTING DATA LOAD (Logic Restored + RAM Optimized)...")
        
        # 1. PREPARE TEMPORARY STATE
        # NOTE: network_df is REMOVED from cache to save RAM
        new_global_cache = {
            "is_loading": False,
            "last_loaded": time.time(),
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
        
        new_global_data = {
            "summary": pd.DataFrame(),
            "geo": pd.DataFrame(),
            "profiles": pd.DataFrame(),
            "risk": pd.DataFrame(),
            "opportunities": pd.DataFrame(),
            "transactions": pd.DataFrame(),
            "products": pd.DataFrame(),
            # "network": pd.DataFrame() <--- Removed from RAM
        }

        # 2. DOWNLOAD FILES
        LOCAL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        files = [
            "products.parquet", "summary.parquet", "geo.parquet", 
            "profiles.parquet", "risk.parquet", "network.parquet", 
            "transactions.parquet", "opportunities.parquet"
        ]

        def fetch_file(filename: str) -> str:
            local_path = str(LOCAL_CACHE_DIR / filename)
            if not os.path.exists(local_path):
                logger.info("Downloading %s...", filename)
                s3_local = boto3.client("s3", region_name=AWS_REGION, config=BOTO_CFG)
                s3_local.download_file(BUCKET_NAME, f"{CACHE_PREFIX}{filename}", local_path)
            return filename

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
             list(executor.map(fetch_file, files))
        
        # 3. REFRESH DUCKDB VIEWS
        global DUCK_CONN
        with DUCK_LOCK:
            if DUCK_CONN is None:
                # CHANGE: Ensure we reconnect to memory if connection was lost
                DUCK_CONN = duckdb.connect(":memory:", read_only=False)
                DUCK_CONN.execute("INSTALL parquet; LOAD parquet;")
                DUCK_CONN.execute("PRAGMA threads=4;")
            
            # Re-create views (Since it's in-memory, we must recreate them every reload)
            # Use absolute string paths to be safe
            prod_path = str(LOCAL_CACHE_DIR / 'products.parquet')
            trans_path = str(LOCAL_CACHE_DIR / 'transactions.parquet')
            net_path = str(LOCAL_CACHE_DIR / 'network.parquet')

            DUCK_CONN.execute(f"CREATE OR REPLACE VIEW v_products AS SELECT * FROM read_parquet('{prod_path}');")
            DUCK_CONN.execute(f"CREATE OR REPLACE VIEW v_transactions AS SELECT * FROM read_parquet('{trans_path}');")
            DUCK_CONN.execute(f"CREATE OR REPLACE VIEW v_network AS SELECT * FROM read_parquet('{net_path}');")

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

        # 5. LOAD RAM FILES (Network REMOVED)
        # We load only small files here
        ram_files = ["geo.parquet", "profiles.parquet", "risk.parquet", "opportunities.parquet"]
        
        for file in ram_files:
            local_path = str(LOCAL_CACHE_DIR / file)
            try:
                df = pd.read_parquet(local_path, engine="pyarrow", dtype_backend="pyarrow")
                
                # Quick string cleanup
                for col in ["vendor_name", "cage_code"]:
                    if col in df.columns:
                        df[col] = df[col].astype(str).str.upper().str.strip()

                if file == "profiles.parquet" and "top_platforms" in df.columns:
                    df["top_platforms"] = (
                        df["top_platforms"].astype(str)
                        .str.replace(r"\bNAN\b,?", "", regex=True)
                        .str.replace(r",+$", "", regex=True)
                        .str.replace(r"^,+", "", regex=True)
                    )

                key_map = {
                    "geo.parquet": "geo",
                    "profiles.parquet": "profiles",
                    "risk.parquet": "risk",
                    "opportunities.parquet": "opportunities"
                }
                
                k = key_map.get(file)
                if k: new_global_data[k] = df
                
                # Cache refs
                if file == "geo.parquet": new_global_cache["geo_df"] = df
                if file == "profiles.parquet": new_global_cache["profiles_df"] = df
                if file == "risk.parquet": new_global_cache["risk_df"] = df
                if file == "opportunities.parquet": new_global_cache["df_opportunities"] = df

            except Exception:
                logger.exception(f"Failed to load {file}")

        # 6. LOAD SUMMARY (CHUNKED & RAM OPTIMIZED)
        try:
            summary_path = str(LOCAL_CACHE_DIR / "summary.parquet")
            
            # Define columns to treat as categories immediately to save RAM
            cat_cols = ["platform_family", "sub_agency", "market_segment", 
                        "vendor_name", "clean_parent", "ultimate_parent_name", "psc_description"]
            
            # Helper to process a single chunk
            def process_chunk(chunk):
                # 1. Cleanup Strings
                string_cols = ["vendor_name", "clean_parent", "ultimate_parent_name", "cage_code"]
                for col in string_cols:
                    if col in chunk.columns:
                        if isinstance(chunk[col].dtype, pd.ArrowDtype):
                             chunk[col] = chunk[col].astype(str)
                        chunk[col] = chunk[col].astype(str).str.upper().str.strip()

                # 2. Immediate Compression
                for col in cat_cols:
                    if col in chunk.columns:
                        chunk[col] = chunk[col].astype("category")
                return chunk

            # Read via PyArrow Tables (Zero-Copy) then convert to Pandas in chunks
            import pyarrow.parquet as pq
            parquet_file = pq.ParquetFile(summary_path)
            
            chunks = []
            # Read 1 row group at a time (much smaller memory footprint)
            for i in range(parquet_file.num_row_groups):
                table = parquet_file.read_row_group(i)
                chunk_df = table.to_pandas(self_destruct=True) # Free arrow memory immediately
                chunks.append(process_chunk(chunk_df))
            
            # Combine all small chunks into one big dataframe
            df = pd.concat(chunks, ignore_index=True)
            
            # Clear chunks from memory
            chunks = None
            gc.collect()

            # --- RETAIN YOUR EXISTING LOGIC BELOW ---
            
            # Parent Mapping
            cage_col = "cage_code" if "cage_code" in df.columns else "vendor_cage"
            if cage_col in df.columns and cage_map:
                # We temporarily convert to object to map, then re-categorize
                # This is unavoidable but safer now that we have memory headroom
                df["clean_parent"] = df[cage_col].map(cage_map)
                df["clean_parent"] = df["clean_parent"].fillna(df["ultimate_parent_name"])
                df["clean_parent"] = df["clean_parent"].fillna(df["vendor_name"]).astype(str).str.upper().str.strip()
            
            # Boeing Fix
            name_corrections = {
                "THE BOEING": "THE BOEING COMPANY",
                "BOEING": "THE BOEING COMPANY",
                "BOEING CO": "THE BOEING COMPANY",
            }
            # Note: Operating on strings here
            df["clean_parent"] = df["clean_parent"].replace(name_corrections)
            if "vendor_name" in df.columns:
                # We need to cast back to string to replace, then re-categorize
                df["vendor_name"] = df["vendor_name"].astype(str).replace(name_corrections).astype("category")

            # Clean NANs
            for col in ["platform_family", "market_segment", "sub_agency", "psc_description"]:
                if col in df.columns:
                    # Ensure they are valid categories
                    pass 

            # ✅ FINAL RE-COMPRESSION
            df["clean_parent"] = df["clean_parent"].astype("category")

            new_global_cache["df"] = df
            new_global_data["summary"] = df
            
            # (Options logic remains the same)
            new_global_cache["options"] = {
                "years": sorted(df["year"].unique().tolist()) if "year" in df.columns else [],
                "agencies": sorted(df["sub_agency"].dropna().unique().tolist()) if "sub_agency" in df.columns else [],
                "domains": sorted(df["market_segment"].dropna().unique().tolist()) if "market_segment" in df.columns else [],
                "platforms": sorted(df["platform_family"].dropna().unique().tolist()) if "platform_family" in df.columns else [],
            }

        except Exception:
            logger.exception("Summary load failed")

            new_global_cache["df"] = df
            new_global_data["summary"] = df
            
            new_global_cache["options"] = {
                "years": sorted(df["year"].unique().tolist()) if "year" in df.columns else [],
                "agencies": sorted(df["sub_agency"].dropna().unique().tolist()) if "sub_agency" in df.columns else [],
                "domains": sorted(df["market_segment"].dropna().unique().tolist()) if "market_segment" in df.columns else [],
                "platforms": sorted(df["platform_family"].dropna().unique().tolist()) if "platform_family" in df.columns else [],
            }

        except Exception:
            logger.exception("Summary load failed")

        # 7. BUILD MAPS & SEARCH INDEX (Standard Logic)
        if not new_global_cache["profiles_df"].empty:
             p_df = new_global_cache["profiles_df"]
             if {"cage_code", "vendor_name"}.issubset(p_df.columns):
                new_global_cache["cage_name_map"] = p_df.set_index("cage_code")["vendor_name"].to_dict()
        
        if not new_global_cache["geo_df"].empty:
             g_df = new_global_cache["geo_df"]
             if "cage_code" in g_df.columns:
                new_global_cache["location_map"] = g_df.set_index("cage_code")[["city", "state"]].to_dict(orient="index")

        search_list = []
        if not new_global_cache["df"].empty:
            summary_df = new_global_cache["df"]
            col = "clean_parent" if "clean_parent" in summary_df.columns else "vendor_name"
            
            # Using observed=True handles the categorical data correctly and quickly
            p_stats = summary_df.groupby(col, observed=True).agg({"total_spend": "sum", "cage_code": "nunique"}).reset_index()
            for r in p_stats.itertuples():
                val = getattr(r, col)
                if r.total_spend > 0 and (r.cage_code > 1 or r.total_spend > 1e9):
                    search_list.append({
                        "label": str(val), "value": str(val),
                        "type": "PARENT", "score": float(r.total_spend), "cage": "AGGREGATE"
                    })
            
            c_stats = summary_df.groupby(["vendor_name", "cage_code"], observed=True)["total_spend"].sum().reset_index()
            loc_map = new_global_cache.get("location_map", {})
            for r in c_stats.itertuples():
                if r.total_spend > 0:
                    raw_cage = str(r.cage_code).strip().upper()
                    if raw_cage in ["NAN", "NONE"]: continue
                    loc = loc_map.get(raw_cage, {})
                    search_list.append({
                        "label": str(r.vendor_name), "value": str(r.vendor_name), "type": "CHILD",
                        "score": float(r.total_spend), "cage": raw_cage,
                        "city": loc.get("city", ""), "state": loc.get("state", "")
                    })

            if "platform_family" in summary_df.columns:
                for r in summary_df.groupby("platform_family", observed=True)["total_spend"].sum().reset_index().itertuples():
                    if r.platform_family:
                        search_list.append({"label": str(r.platform_family), "value": str(r.platform_family), "type": "PLATFORM", "score": float(r.total_spend)})
            
            if "sub_agency" in summary_df.columns:
                for r in summary_df.groupby("sub_agency", observed=True)["total_spend"].sum().reset_index().itertuples():
                    if r.sub_agency:
                        search_list.append({"label": str(r.sub_agency), "value": str(r.sub_agency), "type": "AGENCY", "score": float(r.total_spend)})

            search_list.sort(key=lambda x: x.get("score", 0), reverse=True)
            new_global_cache["search_index"] = search_list

        gc.collect()

        # 8. ATOMIC SWAP
        with CACHE_LOCK:
            GLOBAL_CACHE.clear()
            GLOBAL_CACHE.update(new_global_cache)
            global_data.clear()
            global_data.update(new_global_data)
            GLOBAL_CACHE["is_loading"] = False
            
        logger.info("RELOAD COMPLETE (Optimized). search_index=%d", len(search_list))
        new_global_cache = None
        new_global_data = None
        gc.collect()

    except Exception:
        logger.exception("Reload crash")
        with CACHE_LOCK:
            GLOBAL_CACHE["is_loading"] = False
    finally:
        RELOAD_LOCK.release()



# ==========================================
#        MARKET DASHBOARD ENDPOINTS
# ==========================================

@app.get("/api/dashboard/status")
def get_status():
    return {"ready": not GLOBAL_CACHE["df"].empty, "count": len(GLOBAL_CACHE["df"])}



@app.post("/api/dashboard/reload")
def trigger_reload(background_tasks: BackgroundTasks):
    if GLOBAL_CACHE.get("is_loading"):
        return {"message": "Reload already running"}
    background_tasks.add_task(reload_all_data)
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
    with CACHE_LOCK:
        df = global_data.get("summary", pd.DataFrame())
    if df.empty:
        return []

    filters = {
        "vendor": vendor,
        "parent": parent,
        "cage": cage,
        "domain": domain,
        "agency": agency,
        "platform": platform,
        "psc": psc
    }

    # If NO filters, return the cached dropdown options + top parents (respecting year selection)
    if not any(filters.values()):
        opts = GLOBAL_CACHE.get("options", {}).copy()

        # Handle None years as "all years"
        if years:
            v_mask = df["year"].isin(years)
        else:
            v_mask = pd.Series(True, index=df.index)

        col = "clean_parent" if "clean_parent" in df.columns else "vendor_name"
        opts["top_parents"] = (
            df[v_mask]
            .groupby(col, observed=True)["total_spend"]
            .sum()
            .nlargest(50)
            .index
            .tolist()
        )
        return opts

    # Apply filters using your existing engine (keeps existing logic)
    filtered = FilterEngine.apply_pandas(df, years, filters)

    return {
        "years": sorted(df["year"].unique().tolist()) if "year" in df.columns else [],
        "agencies": sorted(filtered["sub_agency"].dropna().unique().tolist()) if "sub_agency" in filtered.columns else [],
        "domains": sorted(filtered["market_segment"].dropna().unique().tolist()) if "market_segment" in filtered.columns else [],
        "platforms": sorted(filtered["platform_family"].dropna().unique().tolist()) if "platform_family" in filtered.columns else [],
        "psc_pairs": (
            filtered[["psc_code", "psc_description"]]
            .dropna()
            .drop_duplicates()
            .sort_values(["psc_code", "psc_description"])
            .to_dict(orient="records")
        ) if ("psc_code" in filtered.columns and "psc_description" in filtered.columns) else [],
    }



# --- UPDATE IN API.PY ---

def get_recompete_kpi(filters):
    """
    Calculates Risk using the specialized 'risk_df' sidecar.
    """
    # 1. Access the Sidecar from Cache
    # We use .get() to prevent crashing if the file hasn't loaded yet
    df = global_data.get("risk", pd.DataFrame())
    
    if df.empty: 
        return {"label": "Expiring Value (90d)", "value": "N/A", "sub_label": "No Data"}

    # 2. Apply Filters (Vendor, Agency, etc.) to the Sidecar
    # We pass 'None' for years because risk data is always future-looking
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
    vendor: Optional[str]=None,
    parent: Optional[str]=None, 
    cage: Optional[str]=None, 
    domain: Optional[str]=None, 
    agency: Optional[str]=None, 
    platform: Optional[str]=None, 
    psc: Optional[str]=None
):
    df = global_data.get("summary", pd.DataFrame())
    if df.empty: return {"total_spend_b": 0, "total_contracts": 0}
    
    # 1. Define Filters
    filters = {
        "vendor": vendor,
        "parent": parent,
        "cage": cage,
        "domain": domain,
        "agency": agency,
        "platform": platform,
        "psc": psc
    }
    
    # 2. Apply Filters to MAIN Data (For Total Spend/Contracts)
    filtered = FilterEngine.apply_pandas(df, years, filters)
    
    # 3. Apply Filters to RISK Data
    recompete_data = get_recompete_kpi(filters)

    # 4. Return Result
    # ✅ FIX: Explicit cast to python int/float to avoid numpy serialization errors
    if 'contract_count' in filtered.columns:
        total_contracts = int(filtered['contract_count'].sum())
    else:
        total_contracts = len(filtered)
    
    # Ensure spend is a native float
    total_spend = float(filtered['total_spend'].sum())

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
    # 1. Use the Summary Data (Fastest)
    with CACHE_LOCK:
        df = global_data.get("summary", pd.DataFrame())
    if df.empty:
        return []

    # 2. SETUP FILTERS
    filters = {
        "vendor": vendor, "parent": parent, "cage": cage,
        "domain": domain, "agency": agency, "platform": platform, "psc": psc,
    }

    filtered = FilterEngine.apply_pandas(df, None, filters) # Pass None for years initially
    if filtered.empty: return []

    # 🛑 WORK ON COPY (Prevents cache corruption)
    working_df = filtered.copy()

    # 3. CALCULATE FISCAL YEAR (Smart Logic)
    # Scenario A: We have 'action_date' (Transactions)
    if "action_date" in working_df.columns:
        working_df["dt"] = pd.to_datetime(working_df["action_date"], errors="coerce")
        working_df["month_num"] = working_df["dt"].dt.month
        # FY Logic: If Month >= 10, FY = Year + 1
        base_year = working_df["year"] if "year" in working_df.columns else working_df["dt"].dt.year
        working_df["fy"] = base_year + (working_df["month_num"] >= 10).astype(int).fillna(0)

    # Scenario B: We have explicit 'month' and 'year' columns (Your Summary Data)
    elif "month" in working_df.columns and "year" in working_df.columns:
        # Ensure numeric types
        working_df["month_num"] = pd.to_numeric(working_df["month"], errors='coerce').fillna(1).astype(int)
        working_df["year_num"] = pd.to_numeric(working_df["year"], errors='coerce').fillna(0).astype(int)
        
        # FY Logic: If Month >= 10, FY = Year + 1
        working_df["fy"] = working_df["year_num"] + (working_df["month_num"] >= 10).astype(int)
        
    else:
        # Fallback: Just use year, default month to 1
        if "year" in working_df.columns:
            working_df["fy"] = working_df["year"]
            working_df["month_num"] = 1 
        else:
            return []

    # 4. APPLY YEAR FILTER (Using the new 'fy')
    if years and len(years) > 0:
        working_df = working_df[working_df["fy"].isin(years)]
        if working_df.empty: return []

    # 5. YEARLY MODE (Standard Logic)
    if mode == "yearly":
        grouped = working_df.groupby("fy", observed=True)["total_spend"].sum().reset_index()
        
        # Active Range Logic (Trim empty leading/trailing years)
        active_years = grouped[grouped['total_spend'] > 0]
        if not active_years.empty:
            min_year = int(active_years["fy"].min())
            max_year = int(active_years["fy"].max())
        else:
            return []

        # Gap Filling (Ensure 0s for missing years)
        all_years = range(min_year, max_year + 1)
        data_map = {row["fy"]: row['total_spend'] for _, row in grouped.iterrows()}
        
        final_data = []
        for y in all_years:
            val = data_map.get(y, 0.0)
            final_data.append({
                "label": str(y),
                "spend": float(val) 
            })
        return final_data

    # 6. MONTHLY MODE (With Fiscal Sorting)
    elif mode == "monthly":
        grouped = working_df.groupby("month_num", observed=True)["total_spend"].sum().reset_index()
        grouped.columns = ["label", "spend"] # label is 1..12

        # Fiscal Sorting: Oct(10) is first, Sep(9) is last
        def get_fiscal_sort(m):
            try:
                m = int(m)
                return m - 9 if m >= 10 else m + 3
            except:
                return 0

        grouped["sort_index"] = grouped["label"].apply(get_fiscal_sort)
        grouped = grouped.sort_values("sort_index", ascending=True)
        
        # Explicit Float Cast
        grouped["spend"] = grouped["spend"].astype(float)

        return grouped[["label", "spend"]].to_dict(orient="records")

    return []

# ✅ NEW: Drill-down endpoint
@app.get("/api/dashboard/subsidiaries")
def get_dashboard_subsidiaries(parent: str):
    df = global_data.get("summary", pd.DataFrame())
    if df.empty: return []

    clean_parent = sanitize(parent)
    
    # 1. Filter by the clean parent column
    mask = pd.Series(False, index=df.index)
    if "clean_parent" in df.columns:
        mask |= df["clean_parent"].eq(clean_parent)
    elif "ultimate_parent_name" in df.columns:
        mask |= df["ultimate_parent_name"].astype(str).str.upper().eq(clean_parent)
        
    filtered = df[mask]
    if filtered.empty: return []

    # 2. Group by CAGE AND Name (So we get the specific ID for drill-down)
    # We use the cage column we identified in load_data
    cage_col = 'cage_code' if 'cage_code' in df.columns else 'vendor_cage'
    
    grouped = filtered.groupby([cage_col, 'vendor_name'], observed=True).agg({
        'total_spend': 'sum', 
        'contract_count': 'sum',
        'city': 'first',   # <--- Grab location
        'state': 'first'
    }).reset_index()
    
    grouped = grouped.sort_values('total_spend', ascending=False).head(200)
    
    return [
        {
            "cage": r[cage_col],           # ✅ CRITICAL: Needed for drill-down
            "name": r['vendor_name'],      # Frontend expects 'name', not 'vendor_name'
            "total_obligations": r['total_spend'], # Frontend expects this key
            "contract_count": int(r['contract_count']),
            "city": str(r['city']) if pd.notna(r['city']) else "N/A",
            "state": str(r['state']) if pd.notna(r['state']) else "N/A"
        } 
        for _, r in grouped.iterrows()
    ]


@app.get("/api/dashboard/top-vendors")
def get_top_vendors(
    years: Optional[List[int]] = Query(None), 
    vendor: Optional[str]=None,
    parent: Optional[str]=None,
    cage: Optional[str]=None,
    domain: Optional[str]=None, 
    agency: Optional[str]=None, 
    platform: Optional[str]=None, 
    psc: Optional[str]=None
):
    with CACHE_LOCK:
        df = global_data.get("summary", pd.DataFrame())
    if df.empty:
        return []
    
    filters = {
        "vendor": vendor,
        "parent": parent,
        "cage": cage,
        "domain": domain,
        "agency": agency,
        "platform": platform,
        "psc": psc
    }
    filtered = FilterEngine.apply_pandas(df, years, filters)
    
    group_col = 'clean_parent' if 'clean_parent' in filtered.columns else 'vendor_name'
    
    grouped = filtered.groupby(group_col, observed=True).agg({
        'total_spend': 'sum', 
        'contract_count': 'sum'
    })
    
    grouped = grouped.sort_values('total_spend', ascending=False).head(50).reset_index()
    
    return [
        {
            "vendor": r[group_col],  
            # ✅ FIX: Explicit float() and int() casts
            "spend_m": float(r['total_spend']) / 1_000_000.0, 
            "contracts": int(r['contract_count'])
        } 
        for _, r in grouped.iterrows()
    ]

# --- REPLACE THIS FUNCTION IN API.PY ---

# --- REPLACE THIS FUNCTION IN API.PY ---

@app.get("/api/dashboard/distributions")
def get_market_distributions(
    years: Optional[List[int]] = Query(None), 
    vendor: Optional[str]=None,
    parent: Optional[str]=None,
    cage: Optional[str]=None,
    domain: Optional[str]=None, 
    agency: Optional[str]=None, 
    platform: Optional[str]=None, 
    psc: Optional[str]=None
):
    df = global_data.get("summary", pd.DataFrame())
    if df.empty: return {"platform_dist": [], "domain_dist": []}
    
    filters = {
        "vendor": vendor,
        "parent": parent,
        "cage": cage,
        "domain": domain,
        "agency": agency,
        "platform": platform,
        "psc": psc
    }
    filtered = FilterEngine.apply_pandas(df, years, filters)
    
    def get_dist(group_col):
        if group_col not in filtered.columns: return []
        
        # 1. Calculate TRUE Total Spend (✅ FIX: Explicit Float Cast)
        total_market_spend = float(filtered['total_spend'].sum())
        if total_market_spend == 0: return []

        # 2. Filter for NAMED categories
        valid_rows = filtered[
            filtered[group_col].notna() & 
            (filtered[group_col].astype(str).str.lower() != 'nan')
        ]
        
        # 3. Group and Sort
        grouped = valid_rows.groupby(group_col, observed=True)['total_spend'].sum().reset_index()
        grouped = grouped.sort_values('total_spend', ascending=False)

        # 4. Take Top 4
        top_4 = grouped.head(4)
        
        # 5. Calculate "Other"
        top_4_sum = float(top_4['total_spend'].sum()) # ✅ FIX: Cast
        other_val = total_market_spend - top_4_sum

        # 6. Build Result List
        results = [
            {
                "label": str(r[group_col]), 
                # ✅ FIX: Cast r['total_spend'] to float
                "value": round((float(r['total_spend']) / total_market_spend) * 100, 1)
            } 
            for _, r in top_4.iterrows()
        ]

        # 7. Append "Other"
        if other_val > 0:
            results.append({
                "label": "Other",
                "value": round((other_val / total_market_spend) * 100, 1)
            })

        return results

    return {
        "platform_dist": get_dist('platform_family'), 
        "domain_dist": get_dist('psc_description') 
    }

@app.get("/api/dashboard/map")
def get_map_data(
    # CHANGE: Default to None
    years: Optional[List[int]] = Query(None), 
    vendor: Optional[str]=None,
    parent: Optional[str]=None,
    cage: Optional[str]=None,           # ✅ NEW
    domain: Optional[str]=None, 
    agency: Optional[str]=None, 
    platform: Optional[str]=None, 
    psc: Optional[str]=None
):
    df = global_data.get("summary", pd.DataFrame())
    geo_df = global_data.get("geo", pd.DataFrame())
    if df.empty or geo_df.empty: return []
    
    filters = {
    "vendor": vendor,
    "parent": parent,
    "cage": cage,                        # ✅ NEW
    "domain": domain,
    "agency": agency,
    "platform": platform,
    "psc": psc
}
    filtered_summary = FilterEngine.apply_pandas(df, years, filters)
    
    active_vendors = filtered_summary.groupby(['cage_code', 'vendor_name'], observed=True)['total_spend'].sum().reset_index()
    
    mapped_vendors = pd.merge(geo_df, active_vendors, on="cage_code", how="inner", suffixes=("_geo", "_act"))
    mapped_vendors = mapped_vendors.sort_values("total_spend", ascending=False).head(50000)

    vendor_col = "vendor_name_act" if "vendor_name_act" in mapped_vendors.columns else ("vendor_name" if "vendor_name" in mapped_vendors.columns else None)

    return [
        {
            "id": i,
            "vendor": (r[vendor_col] if vendor_col else ""),
            "cage": str(r["cage_code"]).strip().upper(),
            "lat": float(r["latitude"]),
            "lon": float(r["longitude"]),
            "spend": float(r["total_spend"]),
        }
        for i, r in mapped_vendors.iterrows()
]


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

@app.get("/api/platform/profile")
def get_platform_profile(
    name: str,
    years: Optional[List[int]] = Query(None),
    agency: Optional[str] = None,
    domain: Optional[str] = None
):
    df = global_data.get("summary", pd.DataFrame())
    if df.empty or not name:
        return {"found": False}

    # 1. Base Platform Filter (Strict + Strip)
    search_upper = name.strip().upper()
    
    # We work on a filtered copy to avoid settingwithcopy warnings on global cache
    # First, filter by platform to reduce size immediately
    mask = df["platform_family"].astype(str).str.upper().str.strip().eq(search_upper)
    
    # 2. Strict Agency Filter
    if agency:
        safe_ag = sanitize(agency)
        mask = mask & df["sub_agency"].astype(str).str.upper().str.strip().eq(safe_ag)

    if domain:
        mask = mask & df["market_segment"].astype(str).str.upper().str.strip().eq(sanitize(domain))

    filtered = df[mask].copy()

    # 3. APPLY FISCAL YEAR FILTER (The Fix)
    if years and len(years) > 0:
        # Check if we have month/year columns (Summary Data)
        if "month" in filtered.columns and "year" in filtered.columns:
            # Ensure numeric
            m = pd.to_numeric(filtered["month"], errors='coerce').fillna(1).astype(int)
            y = pd.to_numeric(filtered["year"], errors='coerce').fillna(0).astype(int)
            
            # FY Calculation: If Month >= 10, FY = Year + 1
            filtered["fy"] = y + (m >= 10).astype(int)
            
            # Filter by FY
            filtered = filtered[filtered["fy"].isin(years)]
            
        elif "year" in filtered.columns:
            # Fallback (Just use CY if month is missing, but Summary usually has it)
            filtered = filtered[filtered["year"].isin(years)]

    # Get official name safely
    official_name = name
    if not filtered.empty:
        official_name = filtered["platform_family"].mode()[0]
    elif not df[df["platform_family"].astype(str).str.upper().eq(search_upper)].empty:
        official_name = df[df["platform_family"].astype(str).str.upper().eq(search_upper)]["platform_family"].mode()[0]

    if filtered.empty:
        return {
            "found": True, "name": official_name, "total_obligations": 0.0,
            "contractor_count": 0, "contract_count": 0, "top_vendors": [], "top_agencies": []
        }

    # 4. Calculate Stats
    total_obligations = float(filtered["total_spend"].sum())
    contract_count = int(filtered["contract_count"].sum())
    contractor_count = int(filtered["vendor_name"].nunique())

    # 5. Top Vendors
    cage_col = "cage_code" if "cage_code" in filtered.columns else "vendor_cage"
    
    if cage_col in filtered.columns:
        top_vendors_df = (
            filtered.groupby([cage_col, "vendor_name"], observed=True)["total_spend"]
            .sum().reset_index().sort_values("total_spend", ascending=False).head(10)
        )
        top_vendors = [
            {"name": r["vendor_name"], "cage": r[cage_col], "total": float(r["total_spend"])}
            for _, r in top_vendors_df.iterrows()
        ]
    else:
        top_vendors_df = filtered.groupby("vendor_name", observed=True)["total_spend"].sum().nlargest(10)
        top_vendors = [{"name": n, "total": v} for n, v in top_vendors_df.items()]

    # 6. Top Agencies
    top_agencies = filtered.groupby("sub_agency", observed=True)["total_spend"].sum().nlargest(5).index.tolist()

    return {
        "found": True,
        "name": official_name,
        "total_obligations": total_obligations,
        "contractor_count": contractor_count,
        "contract_count": contract_count,
        "top_vendors": top_vendors,
        "top_agencies": top_agencies,
    }

@app.get("/api/platform/top")
def get_top_platforms(
    limit: int = 12,
    years: Optional[List[int]] = Query(None),
    vendor: Optional[str] = None,
    agency: Optional[str] = None,
    domain: Optional[str] = None,
    platform: Optional[str] = None  # ✅ 1. Add Parameter
):
    df = GLOBAL_CACHE["df"]
    if df.empty: return []

    mask = df['platform_family'].notna() & (df['platform_family'] != '')

    # Manual Strict Filtering
    if agency:
        safe_ag = sanitize(agency)
        mask = mask & df["sub_agency"].astype(str).str.upper().str.strip().eq(safe_ag)

    if vendor:
        safe_v = sanitize(vendor)
        mask = mask & df["vendor_name"].astype(str).str.upper().str.contains(safe_v, na=False, regex=False)

    if years and len(years) > 0:
        mask = mask & df["year"].isin(years)

    if domain:
        safe_d = sanitize(domain)
        mask = mask & df["market_segment"].astype(str).str.upper().str.strip().eq(safe_d)

    # ✅ 2. Add Platform Logic
    if platform:
        safe_p = sanitize(platform)
        # We use strict equality here to ensure exact matches, 
        # or you can use .contains() if you want fuzzy matching
        mask = mask & df["platform_family"].astype(str).str.upper().str.strip().eq(safe_p)

    filtered = df[mask]
    
    grouped = filtered.groupby('platform_family', observed=True).agg({
        'total_spend': 'sum',
        'contract_count': 'sum'
    }).reset_index()

    grouped = grouped.sort_values('total_spend', ascending=False).head(limit)

    return [
        {
            "name": r['platform_family'],
            "spend": float(r['total_spend']),
            "contracts": int(r['contract_count'])
        }
        for _, r in grouped.iterrows()
    ]

@app.get("/api/platform/contractors")
def get_platform_contractors(
    name: str,
    limit: int = 100,
    offset: int = 0,
    years: Optional[List[int]] = Query(None),
    agency: Optional[str] = None
):
    df = global_data.get("summary", pd.DataFrame())
    if df.empty or not name: return []

    limit = max(1, min(int(limit), 500))
    offset = max(0, int(offset))

    # Base Filter
    search_upper = name.strip().upper()
    mask = df["platform_family"].astype(str).str.upper().str.strip().eq(search_upper)

    # Global Filters
    if agency:
        safe_ag = sanitize(agency)
        # ✅ FIX: Check sub_agency ONLY
        mask = mask & df["sub_agency"].astype(str).str.upper().str.strip().eq(safe_ag)

    if years and len(years) > 0:
        mask = mask & df["year"].isin(years)

    filtered = df[mask]
    if filtered.empty: return []

    # Aggregation
    cage_col = "cage_code" if "cage_code" in filtered.columns else "vendor_cage"

    if cage_col:
        grouped = (
            filtered
            .groupby([cage_col, "vendor_name"], observed=True)
            .agg(total_spend=("total_spend", "sum"), contract_count=("contract_count", "sum"))
            .reset_index()
            .sort_values("total_spend", ascending=False)
        )
        
        page = grouped.iloc[offset: offset + limit]
        
        return [
            {
                "name": r["vendor_name"],
                "cage": r[cage_col],
                "total": float(r["total_spend"]),
                "contracts": int(r["contract_count"]),
                "role": "PRIME"
            }
            for _, r in page.iterrows()
        ]
    
    return []


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
    
    # ✅ FIX: Query DISK instead of RAM
    # We fetch only the rows for this platform
    df = get_subset_from_disk(
    "products.parquet",
    where_clause="platform_family = ?",
    params=(safe_plat,)
)

    
    if df.empty: return []

    # (Keep existing logic - it now runs on the small subset)
    # 3. Apply Zero/Min Spend Logic
    mask = pd.Series(True, index=df.index)
    if not include_zero:
        mask &= (df['total_revenue'] > 0)
    
    if min_spend > 0 and not years:
        mask &= (df['total_revenue'] >= min_spend)

    filtered = df[mask].copy()
    if filtered.empty: return []

    # 5. Dynamic Year Calculation
    if years:
        def apply_trend_calc(row):
            return calculate_trend_sum(row.get('annual_revenue_trend', ''), years)
        filtered['amount'] = filtered.apply(apply_trend_calc, axis=1)
        if min_spend > 0:
            filtered = filtered[filtered['amount'] >= min_spend]
        filtered = filtered.sort_values("amount", ascending=False)
    else:
        filtered['amount'] = filtered['total_revenue']
        filtered = filtered.sort_values("total_revenue", ascending=False)

    # 6. Paginate
    start = int(offset)
    end = start + int(limit)
    page = filtered.iloc[start:end]

    # 7. Format Output
    results = []
    for row in page.itertuples():
        clean_niin = str(row.niin).strip().zfill(9)
        raw_nsn = str(row.nsn).strip()
        final_nsn = clean_niin if len(raw_nsn) < 9 else raw_nsn

        results.append({
            "item_id": final_nsn,
            "nsn": final_nsn,
            "niin": clean_niin,
            "description": row.description,
            "fsc_code": row.fsc_code,
            "top_vendor": row.cage,
            "top_vendor_name": "See Details",
            "total_units_sold": int(row.total_units_sold),
            "amount": float(row.amount),
            "last_sold": row.last_sold_date,
            "annual_revenue_trend": row.annual_revenue_trend
        })
    return results

@app.get("/api/platform/parts/count")
def get_platform_parts_count(name: str):
    safe_name = sanitize(name)

    # 1. Fast check to avoid wasting Athena costs if platform doesn't exist
    map_check = run_athena_query(f"""
        SELECT COUNT(*) AS n
        FROM "market_intel_silver"."ref_platform_map"
        WHERE (platform_family = '{safe_name}'
               OR UPPER(platform_family) LIKE '%' || UPPER('{safe_name}') || '%')
          AND wsdc_code_ref IS NOT NULL
          AND TRIM(CAST(wsdc_code_ref AS VARCHAR)) <> ''
    """)
    
    if not map_check or int(map_check[0].get("n") or 0) == 0:
        return {"count": 0}

    # 2. Count the Universe of Parts
    # We mirror the logic of 'get_platform_parts' to ensure the numbers align
    query = f"""
    WITH platform_codes AS (
        SELECT DISTINCT TRIM(CAST(wsdc_code_ref AS VARCHAR)) AS wsdc_code_ref
        FROM "market_intel_silver"."ref_platform_map"
        WHERE wsdc_code_ref IS NOT NULL
          AND TRIM(CAST(wsdc_code_ref AS VARCHAR)) <> ''
          AND (
                platform_family = '{safe_name}'
             OR UPPER(platform_family) LIKE '%' || UPPER('{safe_name}') || '%'
          )
    )
    SELECT COUNT(DISTINCT w.niin) as total
    FROM "market_intel_silver"."ref_wsdc" w
    WHERE w.wsdc_code IN (SELECT wsdc_code_ref FROM platform_codes)
      AND w.niin IS NOT NULL AND w.niin <> ''
    """

    results = cached_athena_query(query)

    if results and len(results) > 0:
        # Athena returns numbers as strings sometimes depending on the driver, ensure int
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






@app.get("/api/company/opportunities/recommended")
def get_company_opportunities_recommended(cage: Optional[str] = None, name: Optional[str] = None):
    profile = get_company_profile(cage, name)

    if not profile.get('found') or not profile.get('top_naics'):
        return []

    raw_naics_list = profile['top_naics']
    clean_naics_list = []

    for n in raw_naics_list:
        code_part = str(n).split(' - ')[0].strip().split('.')[0]
        if len(code_part) >= 3:
            clean_naics_list.append(code_part)

    if not clean_naics_list:
        return []

    df_opps = global_data.get("opportunities", pd.DataFrame())
    if df_opps.empty:
        return []

    pattern = "|".join([re.escape(c) for c in sorted(set(clean_naics_list))])
    mask = df_opps['naics'].astype(str).str.contains(pattern, regex=True, na=False)

    filtered = df_opps[mask].sort_values("deadline", ascending=True).head(50)

    results = []
    for row in filtered.itertuples():
        results.append({
            "noticeid": row.id,
            "title": row.title,
            "sol_num": row.sol_num,
            "department_indagency": row.agency,
            "responsedeadline": row.deadline,
            "setaside": row.set_aside_type,
            "primarycontactemail": row.poc_email
        })
    return results


# ==========================================
#        COMPANY INTELLIGENCE
# ==========================================

# [Find and replace get_company_profile in api.py]

@app.get("/api/company/profile")
def get_company_profile(cage: Optional[str] = None, name: Optional[str] = None):
    profiles_df = global_data.get("profiles", pd.DataFrame())
    loc_map = GLOBAL_CACHE["location_map"] # ✅ Use the new fast map
    
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
        global DUCK_CONN
        try:
            with DUCK_LOCK:
                if DUCK_CONN is None:
                    DUCK_CONN = duckdb.connect(":memory:", read_only=False)
                    DUCK_CONN.execute("INSTALL parquet; LOAD parquet;")
                
                net_path = LOCAL_CACHE_DIR / "network.parquet"
                if not net_path.exists(): return []
                
                # Use read_parquet directly
                final_sql = sql.replace("FROM network_source", f"FROM read_parquet('{str(net_path)}')")
                return DUCK_CONN.execute(final_sql, params).fetchdf().to_dict(orient="records")
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
            if DUCK_CONN is None:
                DUCK_CONN = duckdb.connect(":memory:", read_only=False)
                DUCK_CONN.execute("INSTALL parquet; LOAD parquet;")
                DUCK_CONN.execute("PRAGMA threads=4;")
                DUCK_CONN.execute("PRAGMA enable_object_cache=true;")

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

    # 1. Base Filters (Parameterized)
    if cage and cage != "AGGREGATE":
        # Check both columns using parameters
        where_parts.append("(vendor_cage = ? OR cage_code = ?)")
        safe_cage = sanitize(cage)
        params.extend([safe_cage, safe_cage])
    elif name:
        # Fuzzy match using parameters
        where_parts.append("upper(vendor_name) LIKE ?")
        safe_name = f"%{sanitize(name)}%"
        params.append(safe_name)
    else:
        return []

    # 2. Global Filters (Agency)
    if agency:
        where_parts.append("(upper(sub_agency) = ? OR upper(parent_agency) = ?)")
        safe_ag = sanitize(agency)
        params.extend([safe_ag, safe_ag])

    # 3. Years
    if years and len(years) > 0:
        # DuckDB supports list parameters for IN clauses, but simpler to loop for robustness
        placeholders = ",".join(["?" for _ in years])
        where_parts.append(f"year IN ({placeholders})")
        params.extend(years)

    # 4. Spend Threshold
    if threshold and threshold > 0:
        where_parts.append("spend_amount >= ?")
        val = threshold * 1_000_000
        params.append(val)

    # 5. Execution
    where_clause = " AND ".join(where_parts)

    try:
        df = get_subset_from_disk(
            "transactions.parquet",
            where_clause=where_clause,
            params=tuple(params), # Pass the accumulated parameters
            columns_sql="contract_id, action_date, sub_agency, parent_agency, description, spend_amount, naics_code, psc",
            order_by_sql="action_date DESC",
            limit=limit,
            offset=offset
        )
        
        # 6. Format Output
        return [
            {
                "contract_id": r.contract_id,
                "action_date": str(r.action_date),
                "agency": r.sub_agency if pd.notna(r.sub_agency) else r.parent_agency,
                "description": r.description,
                "spend_amount": float(r.spend_amount or 0),
                "naics_code": r.naics_code,
                "psc": r.psc
            }
            for r in df.itertuples()
        ]
    except Exception as e:
        print(f"Awards Query Error: {e}")
        return []
    

@app.get("/api/company/opportunities")
def get_company_opportunities(cage: Optional[str] = None, name: Optional[str] = None):
    # 1. Access Main Data Cache
    df = global_data.get("summary", pd.DataFrame())
    if df.empty: return []

    target_naics = set()
    
    # 2. MATCHING LOGIC (The Fix)
    # We use 'str.contains' so "Lockheed" successfully matches "Lockheed Martin Corp"
    # This ensures we actually find the NAICS codes attached to the profile.
    if name:
        clean_name = sanitize(name)
        if 'clean_parent' in df.columns:
            mask = df['clean_parent'].astype(str).str.contains(clean_name, case=False, regex=False)
            found_naics = df.loc[mask, 'naics_code'].dropna().unique().tolist()
            target_naics.update(found_naics)

    if cage and cage != "AGGREGATE":
        clean_cage = sanitize(cage)
        cage_col = 'cage_code' if 'cage_code' in df.columns else 'vendor_cage'
        if cage_col in df.columns:
            mask = df[cage_col] == clean_cage
            found_naics = df.loc[mask, 'naics_code'].dropna().unique().tolist()
            target_naics.update(found_naics)

    # 3. CLEAN UP (Strip descriptions)
    clean_naics_list = []
    for n in target_naics:
        s = str(n).split('.')[0].split(' - ')[0].strip()
        if len(s) >= 5: clean_naics_list.append(s)
            
    # DEBUG: You should see ~5-10 codes here now (336411, 336413, 541715, etc.)
    print(f"🔍 OPPS DEBUG: Name='{name}' | Found {len(clean_naics_list)} codes", flush=True)

    if not clean_naics_list:
        return []

    # 4. QUERY (Matches the Athena Simulation exactly)
    # Use LIKE for partial matches ("336411" matches "336411 - Aircraft")
        # NAICS values are digits; enforce digits-only and build safe LIKEs
    safe_codes: List[str] = []
    for code in clean_naics_list:
        c = "".join(ch for ch in str(code) if ch.isdigit())
        if len(c) >= 3:
            safe_codes.append(c)

    if not safe_codes:
        return []

    naics_conditions = [f"naics LIKE {sql_like_contains(c)} ESCAPE '\\\\'" for c in safe_codes]
    where_clause = " OR ".join(naics_conditions)

    query = f"""
    SELECT 
        id as noticeid, 
        title, 
        sol_num, 
        agency as department_indagency, 
        deadline as responsedeadline, 
        set_aside_type as setaside, 
        poc_email as primarycontactemail
    FROM "market_intel_gold"."view_unified_opportunities_dod"
    WHERE ({where_clause})
      AND try(from_iso8601_timestamp(deadline)) > current_timestamp - INTERVAL '30' DAY
    ORDER BY try(from_iso8601_timestamp(deadline)) DESC
    LIMIT 50
    """
    return run_athena_query(query)


# ==========================================
#        NEWS INTELLIGENCE
# ==========================================

# [Find and replace get_company_news in api.py]

# [Find and replace get_company_news in api.py]

@app.get("/api/company/news")
def get_company_news(name: str, city: Optional[str] = None, state: Optional[str] = None):
    # DEBUG: Check your terminal logs to confirm 'city' is actually being passed!
    # If this prints 'City: None' or 'City: ', then your View/ETL update hasn't worked yet.
    logger.info("NEWS LOOKUP name=%s city=%s state=%s", name, city, state)

    if not name: return []
    
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
    # If is_local_search is True, we return whatever strict matches we found (even if empty).
    # We DO NOT fallback to raw_items or generic news.
    if is_local_search:
        return final_items
        
    # Only use fallback for Corporate View (if everything got filtered by mistake)
    if not final_items and raw_items:
        return raw_items[:5]

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
        WHERE substr(regexp_replace(nsn, '[^0-9]', ''), 5, 9) = '{safe_niin}'
        GROUP BY 1
    ),
    
    -- 2. Identify Approved Sources & AGGREGATE Part Numbers [Image of SQL Array Aggregation]
    approved_list AS (
        SELECT 
            TRIM(UPPER(cage_code)) as cage_code,
            -- ✅ Collect all part numbers for this CAGE into a single comma-separated string
            array_join(array_agg(DISTINCT part_number), ', ') as part_numbers
        FROM "market_intel_silver"."ref_approved_sources"
        WHERE LPAD(CAST(niin AS VARCHAR), 9, '0') = '{safe_niin}'
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
    niin = get_niin(nsn)

    limit_i = safe_int(limit, 50, 1, 200)
    offset_i = safe_int(offset, 0, 0, 2_000_000)

    # only digits, safe for LIKE-contains with escaping
    like_val = niin

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
        CASE 
            WHEN strpos(description, '!') > 0 THEN split_part(description, '!', 2)
            ELSE description 
        END AS description
    FROM "market_intel_gold"."dashboard_master_view"
    WHERE upper(coalesce(nsn,'')) LIKE {sql_like_contains(like_val)} ESCAPE '\\\\'
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
        # Note: We use DUCK_CONN directly here to keep it efficient
        with DUCK_LOCK:
            if DUCK_CONN is None:
                DUCK_CONN = duckdb.connect(str(DUCKDB_PATH), read_only=False)
            
            count_sql = f"SELECT COUNT(*) FROM read_parquet('{str(path)}') WHERE {where_sql}"
            total_count = DUCK_CONN.execute(count_sql, tuple(params)).fetchone()[0]

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
    # 1. Access RAM Cache
    df = global_data.get("opportunities", pd.DataFrame())
    if df.empty: return []

    mask = pd.Series(True, index=df.index)

    # --- Standard Filters ---
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

    # --- Global Bridge Filters ---

    # 1. KEYWORD (Uses Pre-Computed 'search_text' for speed)
    if keyword:
        safe_k = sanitize(keyword)
        mask &= df['search_text'].str.contains(safe_k, regex=False, na=False)

    # 2. AGENCY (Robust DLA Logic restored)
    if agency:
        safe_ag = sanitize(agency)
        if "DLA" in safe_ag or "LOGISTICS" in safe_ag:
             # Match DLA in agency OR sub_agency OR source_system
             ag_mask = (df['agency'].astype(str).str.upper().str.contains("LOGISTICS", regex=False, na=False)) | \
                       (df['sub_agency'].astype(str).str.upper().str.contains("LOGISTICS", regex=False, na=False)) | \
                       (df['source_system'] == 'DLA')
             mask &= ag_mask
        else:
             # Match Agency OR Sub-Agency
             ag_mask = (df['agency'].astype(str).str.upper().str.contains(safe_ag, regex=False, na=False)) | \
                       (df['sub_agency'].astype(str).str.upper().str.contains(safe_ag, regex=False, na=False))
             mask &= ag_mask

    # 3. PLATFORM
    if platform:
        safe_plat = sanitize(platform)
        # Search Title OR Description
        p_mask = (df['title'].astype(str).str.upper().str.contains(safe_plat, regex=False, na=False)) | \
                 (df['description'].astype(str).str.upper().str.contains(safe_plat, regex=False, na=False))
        mask &= p_mask

    # 4. DOMAIN (Heuristic Logic Restored)
    if domain:
        safe_domain = sanitize(domain)
        # Heuristic: If digit start, search codes. Else search text.
        if len(safe_domain) > 0 and (safe_domain[0].isdigit() or (len(safe_domain) == 4 and safe_domain[0].isalpha())):
            d_mask = (df['psc'].astype(str).str.startswith(safe_domain, na=False)) | \
                     (df['naics'].astype(str).str.startswith(safe_domain, na=False))
            mask &= d_mask
        else:
            d_mask = (df['title'].astype(str).str.upper().str.contains(safe_domain, regex=False, na=False)) | \
                     (df['description'].astype(str).str.upper().str.contains(safe_domain, regex=False, na=False))
            mask &= d_mask

    # 5. Apply & Sort
    filtered = df[mask]
    
    # Sort by Deadline (Nearest First)
    filtered = filtered.sort_values("deadline", ascending=True)

    # 6. Paginate
    start = int(offset)
    end = start + int(limit)
    page = filtered.iloc[start:end]

    # 7. Format Output (Restoring 'days_left' calc)
    results = []
    today = date.today()
    
    for row in page.itertuples():
        # Calculate days_left in Python
        days_left = 0
        try:
            # Assumes deadline is ISO format YYYY-MM-DD...
            dt_str = str(row.deadline)[:10]
            dt_obj = datetime.strptime(dt_str, "%Y-%m-%d").date()
            delta = dt_obj - today
            days_left = delta.days
        except:
            days_left = 0

        results.append({
            "id": row.id,
            "title": row.title,
            "agency": row.agency,
            "sub_agency": row.sub_agency,
            "sol_num": row.sol_num,
            "due_date": row.deadline, # Frontend alias
            "deadline": row.deadline,
            "set_aside": row.set_aside_type, # Frontend alias
            "set_aside_type": row.set_aside_type,
            "naics": row.naics,
            "psc": row.psc,
            "description": str(row.description)[:2000] if row.description else "", # Limit desc length
            "primarycontactemail": row.poc_email,
            "source_system": row.source_system,
            "days_left": int(days_left),
            "total_matches": len(filtered) # Replaces SQL Window Function
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