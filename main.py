from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse 
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, date
import boto3
import os
import pandas as pd
from io import BytesIO
from typing import Optional, List, Dict
import threading
import time
from functools import lru_cache 
import re
import urllib.request
import xml.etree.ElementTree as ET
from html import unescape
from difflib import SequenceMatcher
import re
import concurrent.futures
import time

# --- CONFIGURATION ---
BUCKET_NAME = "a-and-d-intel-lake-newaccount" # Ensure this matches your real bucket
CACHE_PREFIX = "app_cache/"

# ✅ GLOBAL MEMORY STORE (This makes it "Instant")
# We will load DataFrames here so they stay in RAM.
global_data: Dict[str, pd.DataFrame] = {}

# ✅ FIX: Startup Logic using Lifespan (Prevents timeouts & handles errors better)
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- PHASE 1: DOWNLOAD FROM S3 ---
    print("🚀 API Starting up...")
    
    files = [
        "products.parquet", "summary.parquet", "geo.parquet", 
        "profiles.parquet", "risk.parquet", "network.parquet", 
        "transactions.parquet", "opportunities.parquet"
    ]

    try:
        s3 = boto3.client('s3', region_name='us-east-1') # Explicit region is safer
        
        if not os.path.exists("local_data"):
            os.makedirs("local_data")

        print("⬇️ Checking S3 Cache...")
        for file in files:
            local_path = f"local_data/{file}"
            # Check if file exists locally to save bandwidth/time on restarts
            if not os.path.exists(local_path): 
                print(f"   📥 Downloading {file}...")
                s3.download_file(BUCKET_NAME, f"{CACHE_PREFIX}{file}", local_path)
            else:
                print(f"   ✅ {file} exists locally.")

        # --- PHASE 2: LOAD INTO RAM (The "Instant Speed" Step) ---
        print("🧠 Loading data into memory...")
        
        for file in files:
            key_name = file.replace(".parquet", "") # e.g. "products"
            local_path = f"local_data/{file}"
            
            # Read parquet into the global dictionary
            # 'pyarrow' engine is usually fastest for parquet
            global_data[key_name] = pd.read_parquet(local_path, engine='auto')
            print(f"   ⚡ Loaded {key_name} ({len(global_data[key_name]):,} rows)")

        print("🎉 SYSTEM READY. All data is in memory.")
                
    except Exception as e:
        print(f"❌ CRITICAL STARTUP ERROR: {str(e)}")
        # We raise the error to crash the deployment if data fails. 
        # Better to crash than to serve an empty API.
        raise e  
    
    yield  # 2. APP RUNS HERE (Traffic is accepted now)
    
    # 3. SHUTDOWN: Cleanup (Optional)
    print("🛑 API Shutting down...")
    global_data.clear() # Free up RAM on shutdown

# ✅ Pass lifespan to FastAPI
app = FastAPI(
    title="Mimir Hybrid Intelligence API - Instant Mode V5",
    default_response_class=ORJSONResponse,
    lifespan=lifespan 
)

# --- CONFIGURATION ---
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CONFIGURATION ---
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Bucket & Athena Config
raw_bucket = os.getenv('ATHENA_OUTPUT_BUCKET', 'a-and-d-intel-lake-newaccount')
BUCKET_NAME = raw_bucket.replace('s3://', '').split('/')[0]
CACHE_PREFIX = "app_cache/"
LOCAL_CACHE_DIR = "./local_data"
DATABASE = 'market_intel_gold'

# Detect Environment
IS_PRODUCTION = os.getenv('RENDER') or os.getenv('IS_PROD')

# AWS Clients
session = boto3.Session(region_name='us-east-1')
s3 = session.client('s3')
athena = session.client('athena')

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

# --- HELPER: Sanitize Inputs ---
def sanitize(input_str: Optional[str]) -> str:
    if not input_str: return ""
    return input_str.replace("'", "").replace(";", "").replace("--", "").strip().upper()

# --- HELPER: Run Athena Query ---
@lru_cache(maxsize=128)
def cached_athena_query(query: str):
    return run_athena_query(query)

# --- REPLACE IN api.py ---

def run_athena_query(query: str):
    try:
        resp = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': DATABASE},
            ResultConfiguration={'OutputLocation': f's3://{BUCKET_NAME}/temp_api_queries/'}
        )
        qid = resp['QueryExecutionId']
        
        # FIX: INCREASE TIMEOUT
        # Old: ~16 seconds
        # New: ~60 seconds (120 iterations * 0.5s)
        final_state = 'UNKNOWN'
        for i in range(120): 
            status = athena.get_query_execution(QueryExecutionId=qid)
            final_state = status['QueryExecution']['Status']['State']
            
            if final_state in ['SUCCEEDED', 'FAILED', 'CANCELLED']: 
                break
            
            # Wait 0.5s between checks
            time.sleep(0.5)
            
        if final_state != 'SUCCEEDED':
            # Better Error Logging
            reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Timeout or Unknown Error')
            print(f"❌ Athena Error [{final_state}]: {reason}")
            return []

        path = status['QueryExecution']['ResultConfiguration']['OutputLocation']
        key = path.replace(f's3://{BUCKET_NAME}/', '')
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        
        df = pd.read_csv(BytesIO(obj['Body'].read()))
        return df.where(pd.notnull(df), None).to_dict(orient='records')
        
    except Exception as e:
        print(f"❌ Query Exception: {e}")
        return []

# --- DATA LOADER ---
# --- DATA LOADER ---
def load_data():
    global GLOBAL_CACHE
    if GLOBAL_CACHE["is_loading"]: return
    GLOBAL_CACHE["is_loading"] = True
    
    if not IS_PRODUCTION and not os.path.exists(LOCAL_CACHE_DIR):
        os.makedirs(LOCAL_CACHE_DIR)

    try:
        # --- 1. Load Data Files (PARALLELIZED) ---
        # ✅ NEW: Added transactions.parquet to this list
        files = ["summary.parquet", "geo.parquet", "profiles.parquet", "risk.parquet", "network.parquet", "transactions.parquet", "products.parquet", "opportunities.parquet"]
        data_store = {}

        print(f"☁️ DOWNLOAD START: Fetching {len(files)} files in parallel...")
        
        def fetch_and_prep(filename):
            """Download file and optimize strings immediately."""
            t_start = time.time()
            s3_key = f"{CACHE_PREFIX}{filename}"
            local_path = f"{LOCAL_CACHE_DIR}/{filename}"
            
            try:
                if not IS_PRODUCTION and os.path.exists(local_path):
                    df = pd.read_parquet(local_path)
                    origin = "LOCAL"
                else:
                    obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
                    content = obj['Body'].read()
                    df = pd.read_parquet(BytesIO(content))
                    if not IS_PRODUCTION:
                        with open(local_path, "wb") as f: f.write(content)
                    origin = "S3"

                # ✅ OPTIMIZATION: Convert strings to Upper/Category ONCE at startup
                # This removes the need for .str.upper() during live queries (Huge Speedup)
                string_cols = ['vendor_name', 'clean_parent', 'ultimate_parent_name', 
                               'cage_code', 'platform_family', 'sub_agency', 'market_segment']
                
                for col in string_cols:
                    if col in df.columns:
                        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
                            df[col] = df[col].astype(str).str.upper().str.strip()
                            # Convert low cardinality columns to Category for memory/speed
                            if col in ['platform_family', 'sub_agency', 'market_segment']:
                                df[col] = df[col].astype('category')

                print(f"   ↳ Loaded {filename} from {origin} in {time.time() - t_start:.2f}s")
                return filename, df
            except Exception as e:
                print(f"   ⚠️ Failed to load {filename}: {e}")
                return filename, pd.DataFrame()

        # Execute parallel downloads
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_file = {executor.submit(fetch_and_prep, f): f for f in files}
            for future in concurrent.futures.as_completed(future_to_file):
                fname, df_result = future.result()
                data_store[fname] = df_result

        # --- 2. Hierarchy Mapping ---
        print("🔗 MAPPING: Fetching Parent-Child Hierarchy...")
        parent_query = "SELECT child_cage, parent_name FROM ref_parent_child"
        parents_list = run_athena_query(parent_query) 
        
        cage_map = {}
        if parents_list:
            parents_df = pd.DataFrame(parents_list)
            cage_map = pd.Series(
                parents_df.parent_name.values, 
                index=parents_df.child_cage.values
            ).to_dict()

        # ✅ NEW: Load Authoritative NAICS Reference from Silver
        print("📚 MAPPING: Fetching Master NAICS List...")
        # Use 'title' as the short description (e.g., "Engineering Services")
        naics_query = 'SELECT code, title FROM "market_intel_silver"."ref_naics"'
        naics_list = run_athena_query(naics_query)
        
        GLOBAL_CACHE["naics_map"] = {}
        if naics_list:
            # Build a dictionary: "541715" -> "Research and Development..."
            for item in naics_list:
                code = str(item.get('code', '')).strip()
                title = str(item.get('title', '')).strip()
                if code and title:
                    GLOBAL_CACHE["naics_map"][code] = title

        # --- 3. Apply Map & Clean Summary Data ---
        df = global_data.get("summary", pd.DataFrame())

        if not df.empty:
            cage_col = 'cage_code' if 'cage_code' in df.columns else 'vendor_cage'
            
            if cage_col in df.columns and cage_map:
                df['clean_parent'] = df[cage_col].map(cage_map)
            else:
                df['clean_parent'] = None 
            
            if 'ultimate_parent_name' in df.columns:
                df['clean_parent'] = df['clean_parent'].fillna(df['ultimate_parent_name'])
            
            df['clean_parent'] = df['clean_parent'].fillna(df['vendor_name'])
            
            # 1. First standardize to UPPER and STRIP whitespace
            df['clean_parent'] = df['clean_parent'].astype(str).str.upper().str.strip()

            # 2. THEN apply the name fix
            name_corrections = {
                "THE BOEING": "THE BOEING COMPANY",
                "BOEING": "THE BOEING COMPANY",
                "BOEING CO": "THE BOEING COMPANY"
            }
            
            # Fix Parent Column
            df['clean_parent'] = df['clean_parent'].replace(name_corrections)

            # Fix Vendor Name Column (if it exists)
            if 'vendor_name' in df.columns:
                df['vendor_name'] = df['vendor_name'].replace(name_corrections)

            # ✅ FIX 1: GLOBAL NAN CLEANUP
            # Converting "NAN" to None ensures they fall into the "Other" bucket in charts
            # and are excluded from Dropdown lists.
            print("🧹 CLEANING: Removing 'NAN' artifacts...")
            cols_to_clean = ['platform_family', 'market_segment', 'sub_agency', 'psc_description']
            
            for col in cols_to_clean:
                if col in df.columns:
                    mask = df[col].astype(str).str.upper().isin(['NAN', 'NAN.0', 'NONE', '', 'UNKNOWN'])
                    if pd.api.types.is_categorical_dtype(df[col]):
                        df[col] = df[col].astype(object)
                    df.loc[mask, col] = None

        # ✅ FIX 2: PROFILE LIST CLEANUP
        # Removes "NAN" from the comma-separated strings in the sidebar
        profiles_df = data_store.get("profiles.parquet", pd.DataFrame())
        if not profiles_df.empty and 'top_platforms' in profiles_df.columns:
            profiles_df['top_platforms'] = (
                profiles_df['top_platforms']
                .astype(str)
                .str.replace(r'\bNAN\b,?', '', regex=True)
                .str.replace(r',+$', '', regex=True)
                .str.replace(r'^,+', '', regex=True)
            )

                # --- 4. Save to Cache ---
        GLOBAL_CACHE["df"] = df
        GLOBAL_CACHE["geo_df"] = data_store.get("geo.parquet", pd.DataFrame())

        print(
            "🧪 GEO CACHE CHECK:",
            len(GLOBAL_CACHE["geo_df"][GLOBAL_CACHE["geo_df"]["cage_code"] == "24022"]),
            "rows for CAGE 24022",
            flush=True
        )

        GLOBAL_CACHE["profiles_df"] = profiles_df

        # ✅ Build fast cage -> vendor_name map (for clickable supplier labels)
        try:
            if (
                not profiles_df.empty
                and "cage_code" in profiles_df.columns
                and "vendor_name" in profiles_df.columns
            ):
                GLOBAL_CACHE["cage_name_map"] = (
                    profiles_df[["cage_code", "vendor_name"]]
                    .dropna()
                    .drop_duplicates()
                    .assign(
                        cage_code=lambda d: d["cage_code"]
                        .astype(str)
                        .str.upper()
                        .str.strip()
                    )
                    .set_index("cage_code")["vendor_name"]
                    .to_dict()
                )
            else:
                GLOBAL_CACHE["cage_name_map"] = {}
        except Exception as e:
            print(f"⚠️ cage_name_map build failed: {e}")
            GLOBAL_CACHE["cage_name_map"] = {}

        GLOBAL_CACHE["risk_df"] = data_store.get("risk.parquet", pd.DataFrame())

        # UPDATE: Add Network DF to Cache
        print("🕸️ CACHING: Loading Network Graph into RAM...")
        GLOBAL_CACHE["network_df"] = data_store.get("network.parquet", pd.DataFrame())

        # ✅ CRITICAL FIX: Assign the transactions file to the global cache
        # Without this, the API endpoints read an empty dataframe!
        print("💰 CACHING: Loading Transactions into RAM...")
        GLOBAL_CACHE["df_transactions"] = data_store.get("transactions.parquet", pd.DataFrame())

        # ✅ NEW: Load Products and Opportunities
        print("📦 CACHING: Loading Products & Opportunities...")
        GLOBAL_CACHE["df_products"] = data_store.get("products.parquet", pd.DataFrame())
        GLOBAL_CACHE["df_opportunities"] = data_store.get("opportunities.parquet", pd.DataFrame())

        GLOBAL_CACHE["last_loaded"] = time.time()

        
        # --- 5. Build Options ---
        if not df.empty:
            GLOBAL_CACHE["options"] = {
                "years": sorted(df['year'].unique().tolist()) if 'year' in df.columns else [],
                "agencies": sorted(df['sub_agency'].dropna().unique().tolist()) if 'sub_agency' in df.columns else [],
                "domains": sorted(df['market_segment'].dropna().unique().tolist()) if 'market_segment' in df.columns else [],
                "platforms": sorted(df['platform_family'].dropna().unique().tolist()) if 'platform_family' in df.columns else [],
            }

        # --- 6. Location Map ---
        print("🗺️ MAPPING: Indexing Locations from Geo Data...")
        geo_df = GLOBAL_CACHE["geo_df"]
        if not geo_df.empty and 'city' in geo_df.columns:
            GLOBAL_CACHE["location_map"] = geo_df.set_index('cage_code')[['city', 'state']].to_dict(orient='index')
        else:
            GLOBAL_CACHE["location_map"] = {}

        # --- 7. Search Index (Smart Filtering) ---
        print("🔍 INDEXING: Building Search Index...")
        search_list = []
        
        # 1. PARENTS (Aggregate Groups)
        # ✅ LOGIC CHANGE: Only treat as "Parent" if it groups multiple CAGEs
        col = 'clean_parent' if 'clean_parent' in df.columns else 'vendor_name'
        
        # Group by Parent Name -> Count Unique CAGEs
        parent_stats = df.groupby(col).agg({
            'total_spend': 'sum',
            'cage_code': 'nunique'  # Count how many subs this parent has
        }).reset_index()

        for _, r in parent_stats.iterrows():
            # FILTER: Only show "Corporate Group" if it actually has children (plural)
            # OR if it's a massive entity (> $1B) that might be single-cage (rare)
            is_real_group = r['cage_code'] > 1 or r['total_spend'] > 1_000_000_000

            if r['total_spend'] > 0 and is_real_group:
                search_list.append({
                    "label": str(r[col]),
                    "value": str(r[col]),
                    "type": "PARENT",
                    "score": float(r['total_spend']),
                    "cage": "AGGREGATE" 
                })

        # -------------------------------------------------------
        # LAYER 2: CHILDREN (Specific Entities)
        # -------------------------------------------------------
        child_cols = ['vendor_name', 'cage_code']
        if 'clean_parent' in df.columns:
            child_cols.append('clean_parent')

        child_stats = df.groupby(child_cols)['total_spend'].sum().reset_index()
        
        # Grab map reference locally for speed
        loc_map = GLOBAL_CACHE.get("location_map", {})

        for _, r in child_stats.iterrows():
            v_name = str(r['vendor_name'])
            raw_cage = str(r['cage_code']).strip().upper()
            
            # Skip garbage
            if raw_cage in ['NAN', 'NONE', '', 'NULL', 'NAT']:
                continue

            if r['total_spend'] > 0:
                # ✅ LOOKUP LOCATION
                loc_data = loc_map.get(raw_cage, {})
                city = loc_data.get('city', '')
                state = loc_data.get('state', '')

                search_list.append({
                    "label": v_name,
                    "value": v_name,            
                    "type": "CHILD",
                    "score": float(r['total_spend']),
                    "cage": raw_cage,
                    "city": city,   # <--- Store City
                    "state": state  # <--- Store State
                })

        # 3. PLATFORMS
        if 'platform_family' in df.columns:
            p_stats = df.groupby('platform_family')['total_spend'].sum().reset_index()
            for _, r in p_stats.iterrows():
                if r['platform_family']: 
                    search_list.append({
                        "label": str(r['platform_family']),
                        "value": str(r['platform_family']),
                        "type": "PLATFORM",
                        "score": float(r['total_spend'])
                    })

        # 4. AGENCIES
        if 'sub_agency' in df.columns:
            a_stats = df.groupby('sub_agency')['total_spend'].sum().reset_index()
            for _, r in a_stats.iterrows():
                if r['sub_agency']: 
                    search_list.append({
                        "label": str(r['sub_agency']),
                        "value": str(r['sub_agency']),
                        "type": "AGENCY",
                        "score": float(r['total_spend'])
                    })

        search_list.sort(key=lambda x: x.get('score', 0), reverse=True)
        GLOBAL_CACHE["search_index"] = search_list
        print(f"✅ READY: Loaded {len(df)} records. Search Index: {len(search_list)} items.")

    except Exception as e:
        print(f"❌ LOAD ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        GLOBAL_CACHE["is_loading"] = False

threading.Thread(target=load_data).start()

# --- FILTER ENGINE (Centralized Logic) ---
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

# ==========================================
#        MARKET DASHBOARD ENDPOINTS
# ==========================================

@app.get("/api/dashboard/status")
def get_status():
    return {"ready": not GLOBAL_CACHE["df"].empty, "count": len(GLOBAL_CACHE["df"])}

@app.post("/api/dashboard/reload")
def trigger_reload(background_tasks: BackgroundTasks):
    background_tasks.add_task(load_data)
    return {"message": "Reloading..."}

# [FIND THIS FUNCTION]
@app.get("/api/dashboard/filter-options")
def get_filter_options(
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
    if df.empty: return {}
    
    filters = {
    "vendor": vendor,
    "parent": parent,
    "cage": cage,                        # ✅ NEW
    "domain": domain,
    "agency": agency,
    "platform": platform,
    "psc": psc
}
    
    if not any(filters.values()):
        opts = GLOBAL_CACHE["options"].copy()
        
        # LOGIC UPDATE: Handle None for years (All Data)
        if years:
            v_mask = df['year'].isin(years)
        else:
            v_mask = pd.Series(True, index=df.index)
            
        # Use clean_parent for the top list
        col = 'clean_parent' if 'clean_parent' in df.columns else 'vendor_name'
        opts["top_parents"] = df[v_mask].groupby(col, observed=True)['total_spend'].sum().nlargest(50).index.tolist()
        return opts
        
    filtered = FilterEngine.apply_pandas(df, years, filters)
    
    return {
    "years": sorted(df["year"].unique().tolist()),
    "agencies": sorted(filtered["sub_agency"].dropna().unique().tolist()),
    "domains": sorted(filtered["market_segment"].dropna().unique().tolist()),
    "platforms": sorted(filtered["platform_family"].dropna().unique().tolist()),

    # ✅ NEW: PSC code + description pairs for dropdown/autocomplete
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
    df = global_data.get("summary", pd.DataFrame())
    if df.empty: return []

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
    df = global_data.get("summary", pd.DataFrame())
    if df.empty: return []
    
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
    
    mapped_vendors = pd.merge(geo_df, active_vendors, on='cage_code', how='inner')
    mapped_vendors = mapped_vendors.sort_values('total_spend', ascending=False).head(50000)
    
    return [
    {
        "id": i,
        "vendor": r["vendor_name_y"],
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
    domain: Optional[str]=None,
    agency: Optional[str]=None,
    platform: Optional[str]=None,
    vendor: Optional[str]=None
):
    # 1. Query YOUR new view
    query_base = """
    SELECT 
        id as noticeid,     -- Frontend expects 'noticeid'
        sol_num,            -- Frontend needs this for Deep Dive
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
    
    conditions = []
    
    # 2. Filters (Agency is already pre-filtered to DoD, but user can narrow it further)
    if agency: 
        conditions.append(f"upper(agency) LIKE '%{sanitize(agency)}%'")
        
    # 3. Text Search Filters
    # We use OR logic inside each block so we search both Title and Description
    if domain:
        safe_q = sanitize(domain)
        conditions.append(f"(upper(title) LIKE '%{safe_q}%' OR upper(description) LIKE '%{safe_q}%')")
        
    if platform:
        safe_q = sanitize(platform)
        conditions.append(f"(upper(title) LIKE '%{safe_q}%' OR upper(description) LIKE '%{safe_q}%')")
        
    if vendor:
        safe_q = sanitize(vendor)
        conditions.append(f"(upper(title) LIKE '%{safe_q}%' OR upper(description) LIKE '%{safe_q}%')")

    # 4. Assemble
    if conditions:
        query_base += " AND " + " AND ".join(conditions)
        
    query_base += " ORDER BY deadline ASC LIMIT 50"
    
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
    if len(clean_q) > 3 and not re.match(r'^\d{5}$', clean_q):
        try:
            opp_query = f"""
            SELECT title, sol_num, agency 
            FROM "market_intel_gold"."view_unified_opportunities_dod"
            WHERE upper(title) LIKE '%{clean_q}%' OR upper(sol_num) LIKE '%{clean_q}%'
            ORDER BY deadline ASC LIMIT 3
            """
            opps = run_athena_query(opp_query)
            for o in opps:
                results.append({
                    "type": "OPPORTUNITY",
                    "label": o['title'],
                    "value": o['sol_num'],
                    "sub_label": f"Bid • {o['agency']}"
                })
        except:
            pass 

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
    # 1. Access RAM Cache
    df = global_data.get("products", pd.DataFrame())
    if df.empty or not name: return []

    safe_plat = sanitize(name)
    
    # 2. Filter by Platform Family (Exact Match is faster and safer than SQL LIKE)
    mask = (df['platform_family'] == safe_plat)
    
    # 3. Apply Zero/Min Spend Logic
    if not include_zero:
        mask &= (df['total_revenue'] > 0)
    
    # Note: We apply min_spend later if 'years' is present (to filter by *calculated* amount)
    # otherwise we apply it now for speed.
    if min_spend > 0 and not years:
        mask &= (df['total_revenue'] >= min_spend)

    # 4. Create Filtered View
    filtered = df[mask].copy()
    if filtered.empty: return []

    # 5. RESTORED LOGIC: Dynamic Year Calculation
    # If years are selected, we must recalculate 'amount' from the trend string
    if years:
        def apply_trend_calc(row):
            return calculate_trend_sum(row.get('annual_revenue_trend', ''), years)
        
        filtered['amount'] = filtered.apply(apply_trend_calc, axis=1)
        
        # Apply min_spend on the RECALCULATED amount (Logic parity with SQL HAVING)
        if min_spend > 0:
            filtered = filtered[filtered['amount'] >= min_spend]
            
        filtered = filtered.sort_values("amount", ascending=False)
    else:
        # Default: Use pre-computed total
        filtered['amount'] = filtered['total_revenue']
        filtered = filtered.sort_values("total_revenue", ascending=False)

    # 6. Paginate
    start = int(offset)
    end = start + int(limit)
    page = filtered.iloc[start:end]

    # 7. Format Output
    # We map the columns exactly as the frontend expects them from the original SQL
    results = []
    for row in page.itertuples():
        # NIIN/NSN Padding Logic
        clean_niin = str(row.niin).strip().zfill(9)
        raw_nsn = str(row.nsn).strip()
        # Prefer the full NSN if valid, else fallback to padded NIIN
        final_nsn = clean_niin if len(raw_nsn) < 9 else raw_nsn

        results.append({
            "item_id": final_nsn,       # UI Key
            "nsn": final_nsn,           # UI Key
            "niin": clean_niin,
            "description": row.description,
            "fsc_code": row.fsc_code,
            
            # Logic parity: Original SQL chose "MAX_BY(cage, revenue)". 
            # Our view already did that aggregation.
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
    # 1. Load from RAM
    df = global_data.get("transactions", pd.DataFrame())
    if df.empty or not name: return []

    # 2. Build Filter Mask
    mask = pd.Series(True, index=df.index)
    
    # Platform Filter (Exact Match preferred for speed, data is pre-cleaned in ETL)
    # We strip and upper() just in case the input varies
    clean_platform = sanitize(name)
    if "platform_family" in df.columns:
        mask &= (df["platform_family"] == clean_platform)

    if agency:
        safe_ag = sanitize(agency)
        if "sub_agency" in df.columns:
            mask &= (df["sub_agency"] == safe_ag)

    if years and len(years) > 0:
        mask &= df["year"].isin(years)

    if threshold and threshold > 0:
        mask &= (df["spend_amount"] >= (threshold * 1_000_000))

    # 3. Apply, Sort, Paginate
    filtered = df[mask]
    
    if "action_date" in filtered.columns:
        filtered = filtered.sort_values("action_date", ascending=False)
    
    start = int(offset)
    end = start + int(limit)
    page = filtered.iloc[start:end]

    # 4. Format Output
    return [
        {
            "contract_id": r.get("contract_id"),
            "action_date": str(r.get("action_date")),
            "vendor_name": r.get("vendor_name"),
            "vendor_cage": r.get("vendor_cage"),
            "agency": r.get("sub_agency") or r.get("parent_agency"),
            "description": r.get("description"),
            "spend": float(r.get("spend_amount", 0)),
            "naics": r.get("naics_code")
        }
        for _, r in page.iterrows()
    ]






@app.get("/api/company/opportunities")
def get_company_opportunities(cage: Optional[str] = None, name: Optional[str] = None):
    # 1. Get Profile (Reuse existing logic)
    profile = get_company_profile(cage, name)
    
    # Fast Fail
    if not profile.get('found') or not profile.get('top_naics'):
        return []

    # 2. Extract Clean NAICS
    raw_naics_list = profile['top_naics']
    clean_naics_list = []
    for n in raw_naics_list:
        # Split "336411 - Aircraft" -> "336411"
        code_part = str(n).split(' - ')[0].strip().split('.')[0]
        if len(code_part) >= 3: 
            clean_naics_list.append(code_part)
            
    if not clean_naics_list: return []

    # 3. Access RAM Cache
    df_opps = GLOBAL_CACHE.get("df_opportunities", pd.DataFrame())
    if df_opps.empty: return []

    # 4. Filter In-Memory (Regex Match)
    # We match if the Opportunity NAICS contains any of the company's NAICS codes
    pattern = "|".join([re.escape(c) for c in clean_naics_list])
    mask = df_opps['naics'].astype(str).str.contains(pattern, regex=True, na=False)
    
    # 5. Sort & Limit
    filtered = df_opps[mask].sort_values("deadline", ascending=True).head(50)

    # 6. Format
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
    # 1. LOAD DATA FROM RAM
    df = global_data.get("network", pd.DataFrame())
    if df.empty: return {"subs": [], "primes": []}

    # 2. CLEAN INPUTS
    safe_name = sanitize(name).replace("'", "")
    
    # 3. DETERMINE MODE
    is_drill_down = (cage and len(cage) > 2 and cage != 'AGGREGATE')

    # --- HELPER: Aggregation Logic ---
    def aggregate_network(subset, group_col, cage_col):
        if subset.empty: return []
        
        # Group by Name (and CAGE if available)
        grouped = subset.groupby([group_col, cage_col], observed=True).agg({
            'flow_amount_capped': 'sum',
            'flow_amount_raw': 'sum',
            'subaward_description': 'first', # Grab a sample platform/description
            'contract_id': 'count' # Count transactions
        }).reset_index()
        
        # Rename for Frontend
        grouped = grouped.rename(columns={
            group_col: 'name',
            cage_col: 'cage',
            'subaward_description': 'platform',
            'flow_amount_capped': 'total',
            'flow_amount_raw': 'total_raw',
            'contract_id': 'transactions'
        })
        
        # Sort and Limit
        return grouped.sort_values('total', ascending=False).head(50).to_dict(orient='records')

    # --- MODE A: SITE LEVEL (Drill-Down) ---
    if is_drill_down:
        safe_cage = sanitize(cage)
        
        # Downstream: Who does this CAGE hire?
        down_subset = df[df['prime_cage'] == safe_cage]
        subs = aggregate_network(down_subset, 'sub_name', 'sub_cage')
        
        # Upstream: Who hires this CAGE?
        up_subset = df[df['sub_cage'] == safe_cage]
        primes = aggregate_network(up_subset, 'prime_name', 'prime_cage')

    # --- MODE B: PARENT AGGREGATE (Corporate Level) ---
    else:
        # Downstream: Who does Lockheed (Corp) hire?
        # We match on the GOLD PARENT column we created in ETL
        down_subset = df[df['prime_gold_parent'] == safe_name]
        subs = aggregate_network(down_subset, 'sub_name', 'sub_cage')
        
        # Upstream: Who hires Lockheed (Corp)?
        up_subset = df[df['sub_gold_parent'] == safe_name]
        primes = aggregate_network(up_subset, 'prime_name', 'prime_cage')
    
    return {
        "subs": subs, 
        "primes": primes
    }


def calculate_trend_sum(trend_str: str, years: List[int]) -> float:
    """Sums the pipe-delimited trend string for specific years."""
    if not trend_str or not years:
        return 0.0
    total = 0.0
    target_years = set(years)
    try:
        segments = trend_str.split("|")
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
    for seg in str(trend_str).split("|"):
        if ":" not in seg:
            continue
        y, v = seg.split(":", 1)
        try:
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

@app.get("/api/company/parts")
def get_company_parts(
    cage: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    years: Optional[List[int]] = Query(default=None),
    rollup: Optional[str] = None,
):
    df = global_data.get("products", pd.DataFrame())

    if df.empty or not cage:
        return []

    safe_cage = sanitize(cage)
    filtered = df[df["cage"] == safe_cage].copy()
    if filtered.empty:
        return []

    # Amount = selected-year window, else all-time
    if years:
        filtered["amount"] = filtered["annual_revenue_trend"].apply(
            lambda s: calculate_trend_sum(s or "", years)
        )
    else:
        filtered["amount"] = pd.to_numeric(filtered.get("total_revenue", 0), errors="coerce").fillna(0)

    # =========================
    # ROLLUP MODE: NSN / NIIN
    # =========================
    if rollup == "nsn":
        # Canonical rollup key: digits-only, 9-char NIIN
        base = filtered["niin"] if "niin" in filtered.columns else filtered["nsn"]

        filtered["niin_key"] = (
            base.astype(str)
            .str.replace(r"[^0-9]", "", regex=True)
            .str.strip()
            .str.zfill(9)
        )

        # Parse trend to dict so we can sum per NIIN
        filtered["_trend_dict"] = filtered["annual_revenue_trend"].apply(_parse_trend_to_dict)

        def weighted_avg_price(group: pd.DataFrame) -> float:
            units = pd.to_numeric(group.get("total_units_sold", 0), errors="coerce").fillna(0).astype(float)
            price = pd.to_numeric(group.get("avg_unit_price", 0), errors="coerce").fillna(0).astype(float)
            denom = float(units.sum())
            if denom <= 0:
                return float(price.mean() if len(price) else 0.0)
            return float((price * units).sum() / denom)

        def collect_part_numbers(series: pd.Series) -> List[str]:
            if series is None:
                return []
            vals = series.fillna("").astype(str).str.strip()
            vals = vals[vals != ""]
            seen = set()
            out_list: List[str] = []
            for v in vals.tolist():
                if v not in seen:
                    seen.add(v)
                    out_list.append(v)
            return out_list

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

        # Weighted avg unit price per NIIN
        avg_map = (
            filtered.groupby("niin_key", observed=True)
            .apply(weighted_avg_price)
            .to_dict()
        )
        agg["avg_unit_price"] = agg["niin_key"].map(avg_map).fillna(0)

        # Sum annual trend strings per NIIN
        trend_map = (
            filtered.groupby("niin_key", observed=True)["_trend_dict"]
            .apply(lambda s: _sum_trend_dicts(list(s)))
            .to_dict()
        )
        agg["annual_revenue_trend"] = agg["niin_key"].map(trend_map).fillna("")

        agg["part_numbers_count"] = agg["part_numbers"].apply(
            lambda x: len(x) if isinstance(x, list) else 0
        )

        # Stable sort for pagination
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
    df = GLOBAL_CACHE.get("df_products", pd.DataFrame())
    if df.empty:
        return {"count": 0}

    safe_cage = sanitize(cage)
    filtered = df[df["cage"] == safe_cage]
    if filtered.empty:
        return {"count": 0}

    if rollup == "nsn":
        base = filtered["niin"] if "niin" in filtered.columns else filtered["nsn"]
        niin_key = (
            base.astype(str)
            .str.replace(r"[^0-9]", "", regex=True)
            .str.strip()
            .str.zfill(9)
        )
        return {"count": int(niin_key.nunique(dropna=True))}

    return {"count": int(len(filtered))}








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
    # 1. Load Data from RAM (Instant)
    # Note: Ensure you add 'transactions.parquet' to your load_data function first!
    df = global_data.get("transactions", pd.DataFrame())
    
    # If not loaded yet, try main df as fallback (slower but safe)
    if df.empty: 
        df = GLOBAL_CACHE.get("df", pd.DataFrame())

    if df.empty: return []

    # 2. Apply Filters (In-Memory)
    mask = pd.Series(True, index=df.index)

    # Identity Filter
    if cage and cage != "AGGREGATE":
        safe_cage = sanitize(cage)
        if "vendor_cage" in df.columns:
            mask &= (df["vendor_cage"] == safe_cage)
        elif "cage_code" in df.columns:
            mask &= (df["cage_code"] == safe_cage)
    elif name:
        safe_name = sanitize(name)
        mask &= df["vendor_name"].astype(str).str.contains(safe_name, regex=False)

    # Global Filters
    if agency:
        safe_ag = sanitize(agency)
        # Check both columns if available
        ag_mask = pd.Series(False, index=df.index)
        if "sub_agency" in df.columns:
            ag_mask |= (df["sub_agency"] == safe_ag)
        if "parent_agency" in df.columns:
            ag_mask |= (df["parent_agency"] == safe_ag)
        mask &= ag_mask

    if years and len(years) > 0:
        mask &= df["year"].isin(years)

    if threshold and threshold > 0:
        mask &= (df["spend_amount"] >= (threshold * 1_000_000))

    # 3. Sort and Paginate
    filtered = df[mask].sort_values("action_date", ascending=False)
    
    # Slice
    start = int(offset)
    end = start + int(limit)
    page = filtered.iloc[start:end]

    # 4. Format Output
    return [
        {
            "contract_id": r.get("contract_id"),
            "action_date": str(r.get("action_date")),
            "agency": r.get("sub_agency") or r.get("parent_agency"),
            "description": r.get("description"),
            "spend_amount": float(r.get("spend_amount", 0)),
            "naics_code": r.get("naics_code"),
            "psc": r.get("psc")
        }
        for _, r in page.iterrows()
    ]

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
    naics_conditions = [f"naics LIKE '%{code}%'" for code in clean_naics_list]
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
      -- The TRY() wrapper saves us from the "Alan D Rose" crash
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
    print(f"📰 NEWS LOOKUP -> Name: {name} | City: {city} | State: {state}")

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
    # 1. Access RAM Cache
    df = global_data.get("products", pd.DataFrame())
    if df.empty: return {"found": False}

    # 2. Clean Input (Standard 9-digit NIIN logic)
    clean = ''.join(filter(str.isdigit, str(nsn)))
    if len(clean) < 9:
        safe_niin = clean.zfill(9)
    else:
        safe_niin = clean[-9:]

    # 3. Lookup
    match = df[df['niin'] == safe_niin]
    
    if match.empty:
        return {"found": False}
        
    # 4. Aggregate
    total_rev = match['total_revenue'].sum()
    
    # Best Row Logic: Pick the one with a valid description if possible
    best_row = match.iloc[0]
    for r in match.itertuples():
        if r.description and r.description != "Unknown":
            best_row = r
            break
            
    # 5. Format Output
    return {
        "found": True,
        "nsn": f"{best_row.fsc_code}-{safe_niin}" if getattr(best_row, 'fsc_code', None) else safe_niin,
        "niin": safe_niin,
        "item_name": best_row.description,
        "fsc_code": getattr(best_row, 'fsc_code', ''),
        
        "total_revenue": float(total_rev),
        "total_units_sold": int(match['total_units_sold'].sum()),
        
        "market_price": float(best_row.avg_unit_price),
        "last_sold_date": best_row.last_sold_date,
        "annual_revenue_trend": best_row.annual_revenue_trend,
        
        "demil_code": getattr(best_row, 'demil_code', None),
        "shelf_life_code": getattr(best_row, 'shelf_life_code', None),
        "mgmt_control_code": getattr(best_row, 'mgmt_control_code', None),
        "unit_of_issue": getattr(best_row, 'unit_of_issue', None),
        "source_of_supply": getattr(best_row, 'source_of_supply', None),
        "govt_estimated_price": float(getattr(best_row, 'govt_estimated_price', 0)),
        "acquisition_advice_code": getattr(best_row, 'acquisition_advice_code', None),
        
        # ✅ FIX: Explicitly cast numpy bool to python bool
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
    # 1. Access RAM Cache
    df = global_data.get("products", pd.DataFrame())
    if df.empty: return []

    # 2. Clean Input
    clean = ''.join(filter(str.isdigit, str(nsn)))
    safe_niin = clean.zfill(9) if len(clean) < 9 else clean[-9:]

    # 3. Lookup Matches
    matches = df[df['niin'] == safe_niin]
    if matches.empty: return []

    # 4. Group by Platform Family
    # Matches will contain one row per CAGE per NIIN, with the platform_family populated
    grouped = matches.groupby('platform_family', observed=True).agg({
        'total_revenue': 'sum',
        'cage': 'count' # Using count of cages as a proxy for 'contracts' or breadth
    }).reset_index()

    # 5. Format & Sort
    grouped = grouped.sort_values('total_revenue', ascending=False).head(10)
    
    return [
        {
            "platform": r.platform_family,
            "spend": float(r.total_revenue),
            "contracts": int(r.cage) # Labeling count as contracts for compatibility
        }
        for r in grouped.itertuples()
        if r.platform_family and r.platform_family != "UNKNOWN"
    ]

@app.get("/api/nsn/history")
def get_nsn_history(nsn: str):
    # 1. Access RAM Cache
    df = global_data.get("products", pd.DataFrame())
    if df.empty: return []

    # 2. Clean Input
    clean = ''.join(filter(str.isdigit, str(nsn)))
    safe_niin = clean.zfill(9) if len(clean) < 9 else clean[-9:]

    # 3. Lookup Matches
    matches = df[df['niin'] == safe_niin]
    if matches.empty: return []

    # 4. Aggregate Histories (Summing up multiple vendors for the same NIIN)
    # The trend string format is "Year:Amount|Year:Amount"
    year_map = {}
    
    for trend_str in matches['annual_revenue_trend']:
        if not trend_str or pd.isna(trend_str): continue
        segments = str(trend_str).split('|')
        for seg in segments:
            if ':' in seg:
                try:
                    y, amt = seg.split(':')
                    y_int = int(y)
                    amt_float = float(amt)
                    year_map[y_int] = year_map.get(y_int, 0.0) + amt_float
                except:
                    continue

    # 5. Format for Chart
    results = []
    for year in sorted(year_map.keys()):
        results.append({
            "label": str(year),
            "spend": year_map[year]
        })
        
    return results



@app.get("/api/nsn/contracts")
def get_nsn_contracts(
    nsn: str,
    limit: int = 50,
    offset: int = 0
):
    # 1. Clean Input
    niin = get_niin(nsn)

    # 2. Guardrails
    limit = max(1, min(int(limit), 200))
    offset = max(0, int(offset))

    # 3. Query Athena Directly
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
        
        -- ✅ FIXED: Clean Description (Remove text before '!')
        CASE 
            WHEN strpos(description, '!') > 0 THEN split_part(description, '!', 2)
            ELSE description 
        END AS description
        
    FROM "market_intel_gold"."dashboard_master_view"
    WHERE nsn LIKE '%{niin}%'
      AND spend_amount IS NOT NULL
    ORDER BY action_date DESC
    OFFSET {offset}
    LIMIT {limit}
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
    limit: int = 12
):
    limit = max(4, min(int(limit), 50))
    cond = ["1=1"]

    if years and len(years) > 0:
        years_csv = ",".join([str(int(y)) for y in years if str(y).isdigit()])
        if years_csv:
            cond.append(
                f"(year(try_cast(substr(action_date, 1, 10) as date))"
                f" + if(month(try_cast(substr(action_date, 1, 10) as date)) >= 10, 1, 0)) IN ({years_csv})"
            )


    if vendor:
        v = sanitize(vendor)
        cond.append(f"upper(vendor_name) LIKE '%{v}%'")
    if parent:
        p = sanitize(parent)
        cond.append(f"upper(ultimate_parent_name) LIKE '%{p}%'")
    if cage:
        c = sanitize(cage)
        cond.append(f"upper(vendor_cage) LIKE '%{c}%'")
    if agency:
        a = sanitize(agency)
        cond.append(f"(upper(sub_agency) LIKE '%{a}%' OR upper(parent_agency) LIKE '%{a}%')")
    if platform:
        pl = sanitize(platform)
        cond.append(f"upper(platform_family) LIKE '%{pl}%'")
    if psc:
        psc_q = sanitize(psc)
        cond.append(f"upper(psc) LIKE '%{psc_q}%'")
    if domain:
        d = sanitize(domain)
        cond.append(f"(upper(psc) LIKE '%{d}%' OR upper(naics_code) LIKE '%{d}%')")

    where_clause = " AND ".join(cond)

    # 1. Calculate the Aggregates on the Transaction Table
    niin_expr = "substr(regexp_replace(nsn, '[^0-9]', ''), -9)"

    query = f"""
    WITH top_spending AS (
        SELECT
            LPAD({niin_expr}, 9, '0') AS join_niin,
            
            -- Keep the "Dirty" description as a backup
            MAX(description) as raw_desc,
            
            SUM(spend_amount) AS spend,
            COUNT(DISTINCT contract_id) AS contracts
        FROM "market_intel_gold"."dashboard_master_view"
        WHERE {where_clause}
          AND nsn IS NOT NULL
          AND length(regexp_replace(nsn, '[^0-9]', '')) >= 8
        GROUP BY 1
        ORDER BY spend DESC
        LIMIT {limit}
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
        -- ✅ FIXED: Explicitly CAST the FSC integer to VARCHAR before adding the dash
        CASE 
            WHEN c.fsc IS NOT NULL THEN CAST(c.fsc AS VARCHAR) || '-' || t.join_niin 
            ELSE t.join_niin 
        END AS nsn,
        
        -- Description Logic (Same as before)
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
    # 1. Access RAM Cache
    df = global_data.get("transactions", pd.DataFrame())
    if df.empty or not id: return None

    safe_id = sanitize(id)

    # 2. Filter for Contract ID
    # Using exact match
    matches = df[df["contract_id"] == safe_id]
    if matches.empty: return None

    # 3. Aggregate Stats in Memory
    # We take the most recent row for metadata, sum for spend
    matches = matches.sort_values("action_date", ascending=False)
    latest = matches.iloc[0]
    earliest = matches.iloc[-1]

    total_spend = matches["spend_amount"].sum()

    return {
        "contract_id": latest.contract_id,
        "vendor_name": latest.vendor_name,
        "vendor_cage": latest.vendor_cage,
        "agency": latest.sub_agency if hasattr(latest, 'sub_agency') else latest.parent_agency,
        "sub_agency": latest.sub_agency,
        "description": latest.description,
        
        "platform_family": latest.platform_family,
        # Safe getters for columns that might not exist in summary parquet but exist in master
        "city": getattr(latest, "city", None),
        "state": getattr(latest, "state", None),
        "country": getattr(latest, "country", None),
        
        "naics_code": latest.naics_code,
        "psc": latest.psc,
        
        # Acquisition fields (using getattr to be safe if columns missing in parquet)
        "pricing_type": getattr(latest, "pricing_type", None),
        "competition_type": getattr(latest, "competition_type", None),
        "offers_count": getattr(latest, "offers_count", None),
        "set_aside_type": getattr(latest, "set_aside_type", None),
        "solicitation_id": getattr(latest, "solicitation_identifier", None),

        "total_spend": float(total_spend),
        "start_date": earliest.action_date,
        "last_action_date": latest.action_date
    }

# --- ADD THIS NEW FUNCTION ---
@app.get("/api/award/solicitation")
def get_solicitation_lookup(sol_num: str):
    """Fetches details for a specific solicitation number."""
    if not sol_num: return None
    safe_num = sanitize(sol_num)
    
    query = f"""
    SELECT 
        sol_num,
        title,
        url,
        deadline,
        agency
    FROM "market_intel_gold"."view_unified_opportunities_dod"
    WHERE sol_num = '{safe_num}' 
       OR sol_num = '{safe_num.replace("-", "")}'
    LIMIT 1
    """
    results = run_athena_query(query)
    return results[0] if results else None

@app.get("/api/award/history")
def get_award_history(id: str):
    # 1. Access RAM Cache
    df = global_data.get("transactions", pd.DataFrame())
    if df.empty or not id: return []

    safe_id = sanitize(id)
    
    # 2. Filter
    mask = df["contract_id"] == safe_id
    filtered = df[mask].sort_values("action_date", ascending=True).head(200)

    # 3. Format
    results = []
    for row in filtered.itertuples():
        results.append({
            "action_date": row.action_date,
            "spend_amount": float(row.spend_amount),
            "description": row.description
        })
    return results

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
    # 1. Access RAM Cache (Transactions)
    df = global_data.get("transactions", pd.DataFrame())
    
    # Fallback if cache is empty (e.g. startup)
    if df.empty: return {"data": [], "total": 0, "error": "Data loading"}

    # 2. Build Mask
    mask = pd.Series(True, index=df.index)

    if years and len(years) > 0:
        mask &= df["year"].isin(years)

    if agency:
        safe_a = sanitize(agency)
        # Check both agency columns if available
        ag_mask = (df["sub_agency"] == safe_a)
        if "parent_agency" in df.columns:
            ag_mask |= (df["parent_agency"] == safe_a)
        mask &= ag_mask

    if vendor:
        safe_v = sanitize(vendor)
        mask &= df["vendor_name"].str.contains(safe_v, regex=False, na=False)

    if platform:
        safe_p = sanitize(platform)
        mask &= (df["platform_family"] == safe_p)

    if psc:
        safe_psc = sanitize(psc)
        mask &= df["psc"].str.contains(safe_psc, regex=False, na=False)
    
    if domain:
        # Domain usually maps to NAICS or PSC high level
        safe_d = sanitize(domain)
        mask &= (df["naics_code"].str.contains(safe_d, regex=False, na=False) | 
                 df["psc"].str.contains(safe_d, regex=False, na=False))

    # 3. Text Search (q)
    if q:
        safe_q = sanitize(q)
        # Search relevant text columns
        search_mask = df["contract_id"].str.contains(safe_q, regex=False, na=False) | \
                      df["vendor_name"].str.contains(safe_q, regex=False, na=False) | \
                      df["description"].str.contains(safe_q, regex=False, na=False)
        if "vendor_cage" in df.columns:
            search_mask |= df["vendor_cage"].str.contains(safe_q, regex=False, na=False)
            
        mask &= search_mask
    elif not q and not (vendor or agency or platform or psc or domain):
        # Optimization: If no filters at all, only show high value awards to keep list interesting
        mask &= (df["spend_amount"] > 100000)

    # 4. Apply Filter
    filtered = df[mask]
    total_count = len(filtered)

    # 5. Sort & Paginate
    # Sort by Action Date (Recent first)
    filtered = filtered.sort_values("action_date", ascending=False)
    
    start = int(offset)
    end = start + int(limit)
    page = filtered.iloc[start:end]

    # 6. Format Output (Matches your original API response structure)
    results = []
    for row in page.itertuples():
        results.append({
            "contract_id": row.contract_id,
            "vendor_name": row.vendor_name,
            "vendor_cage": row.vendor_cage,
            "last_action_date": row.action_date, # Mapping action_date to last_action_date
            "description": row.description,
            "total_spend": float(row.spend_amount), # Mapping spend_amount to total_spend
            "agency": row.sub_agency if hasattr(row, 'sub_agency') else row.parent_agency,
            "sub_agency": row.sub_agency
        })

    return {
        "data": results,
        "total": total_count,
        "offset": offset,
        "limit": limit
    }

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
    if not id: return {"found": False}
    safe_id = sanitize(id) 
    
    # Simple, fast lookup. No extra joins or parsing.
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
        -- ✅ Explicitly selecting these for the UI
        naics,
        psc,
        sub_agency
    FROM "market_intel_gold"."view_unified_opportunities_dod"
    WHERE upper(sol_num) = '{safe_id}' OR upper(id) = '{safe_id}'
    LIMIT 1
    """
    
    results = run_athena_query(query)
    
    if not results:
        return {"found": False}
    
    return results[0]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)

# --- HEALTH CHECK (Keeps Render Happy) ---
@app.get("/")
def health_check():
    return {"status": "ok", "message": "Mimir V5 is Live"}

@app.head("/")
def health_check_head():
    return {"status": "ok"}