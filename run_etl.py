import boto3
import pandas as pd
import os
from io import BytesIO
import time
import shutil
import uuid
import duckdb
from botocore.config import Config
import warnings
from datetime import datetime, timezone

# Suppress pandas warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- CONFIGURATION ---
raw_bucket_input = os.getenv('ATHENA_OUTPUT_BUCKET', 'a-and-d-intel-lake-newaccount')
BUCKET_NAME = raw_bucket_input.replace('s3://', '').split('/')[0]
CACHE_PREFIX = "app_cache/"
DATABASE = 'market_intel_gold'

# ✅ FORCE REBUILD SWITCH (temporary override)
FORCE_REBUILD = os.getenv("FORCE_REBUILD", "0").strip().lower() in ("1", "true", "yes")

# Temp locations
TEMP_DIR = "./temp_etl_downloads"
if not os.path.exists(TEMP_DIR):
    os.makedirs(TEMP_DIR)

ATHENA_OUTPUT_PREFIX = "temp_etl/"         # where Athena puts normal query CSV outputs
UNLOAD_OUTPUT_PREFIX = "temp_etl_unload/"  # where Athena UNLOAD writes parquet parts

# AWS Clients
session = boto3.Session(region_name='us-east-1')
athena = session.client('athena')

# Robust Retry Policy for Network Stability
s3_config = Config(
    read_timeout=900,
    connect_timeout=300,
    retries={'max_attempts': 10, 'mode': 'adaptive'}
)
s3 = session.client('s3', config=s3_config)

# -------------------------
# Checkpointing helpers
# -------------------------

# -------------------------
# Checkpointing helpers
# -------------------------
def is_cache_fresh(cache_name: str, max_age_hours: float = 12.0) -> bool:
    """
    Checks if a file exists in the S3 cache AND was modified within the last `max_age_hours`.
    """

    # 🧨 Global override — forces rebuild regardless of age
    if FORCE_REBUILD:
        print(f"🧨 FORCE_REBUILD=1 -> treating {cache_name} as stale")
        return False

    keys_to_check = [f"{CACHE_PREFIX}{cache_name}", f"{CACHE_PREFIX}{cache_name}.DONE"]
    
    for key in keys_to_check:
        try:
            meta = s3.head_object(Bucket=BUCKET_NAME, Key=key)
            # ✅ FIX: Use raw Unix timestamps to avoid any Python timezone math crashes
            last_mod_ts = meta['LastModified'].timestamp()
            now_ts = datetime.now(timezone.utc).timestamp()
            
            age_hours = (now_ts - last_mod_ts) / 3600.0
            
            if age_hours <= max_age_hours:
                return True
            else:
                print(f"   ⏱️ {cache_name} is {age_hours:.1f} hours old (Expired). Rebuilding...")
                
        except s3.exceptions.ClientError as e:
            # 404 just means the file isn't there yet, which is normal on the first run.
            if e.response['Error']['Code'] != '404':
                print(f"   ⚠️ S3 Access Error checking {cache_name}: {e}")
        except Exception as e:
            print(f"   ⚠️ Unexpected error checking {cache_name}: {e}")
            
    return False

# -------------------------
# Athena UNLOAD helpers
# -------------------------
def start_query_raw(query: str) -> str:
    resp = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': f's3://{BUCKET_NAME}/{ATHENA_OUTPUT_PREFIX}'}
    )
    return resp['QueryExecutionId']

def wait_for_query(qid: str):
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)

    if state != 'SUCCEEDED':
        reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown Error')
        print(f"❌ ATHENA ERROR: {reason}")
        raise Exception(f"Query Failed: {state} - {reason}")

def unload_to_s3(select_sql: str, unload_prefix: str) -> str:
    """
    Runs Athena UNLOAD to Parquet -> s3://BUCKET/<unload_prefix>/
    Returns unload_prefix (normalized with trailing slash).
    """
    if not unload_prefix.endswith("/"):
        unload_prefix += "/"

    full_dest = f"s3://{BUCKET_NAME}/{unload_prefix}"

    unload_query = f"""
    UNLOAD (
        {select_sql.strip().rstrip(';')}
    )
    TO '{full_dest}'
    WITH (
        format = 'PARQUET',
        compression = 'SNAPPY'
    )
    """

    qid = start_query_raw(unload_query)
    wait_for_query(qid)
    return unload_prefix

def list_s3_keys(prefix: str):
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        for obj in page.get('Contents', []):
            yield obj['Key']

def upload_unload_parts_to_cache(unload_prefix: str, cache_name: str):
    """
    Upload UNLOAD parquet parts as a dataset folder:
      app_cache/<cache_name without .parquet>/part-....parquet
    """
    cache_folder = f"{CACHE_PREFIX}{cache_name.replace('.parquet','')}/"
    print(f"💾 Uploading UNLOAD parts to s3://{BUCKET_NAME}/{cache_folder}")

    # ✅ FIX: Do not enforce .parquet extension. Athena often names compressed files 
    # with just `.snappy` or a raw UUID depending on the engine version.
    all_keys = list(list_s3_keys(unload_prefix))
    
    # Filter out S3 folder markers (keys ending in '/')
    part_keys = [k for k in all_keys if not k.endswith("/")]

    if not part_keys:
        print(f"🔍 DEBUG S3: Searched prefix: {unload_prefix}")
        print(f"🔍 DEBUG S3: Found keys: {all_keys}")
        raise Exception(f"No data files found under s3://{BUCKET_NAME}/{unload_prefix}")

    for k in part_keys:
        local_part = os.path.join(TEMP_DIR, os.path.basename(k))
        s3.download_file(BUCKET_NAME, k, local_part)
        
        # ✅ FIX: Force the .parquet extension locally so DuckDB can read it flawlessly
        dest_filename = os.path.basename(k)
        if not dest_filename.endswith(".parquet"):
            dest_filename += ".parquet"
            
        s3.upload_file(local_part, BUCKET_NAME, cache_folder + dest_filename)
        os.remove(local_part)

    # Optional marker
    s3.put_object(Bucket=BUCKET_NAME, Key=f"{CACHE_PREFIX}{cache_name}.DONE", Body=b"ok")

    print(f"   ✅ Uploaded {len(part_keys)} parquet parts to {cache_folder}")

# ✅ NEW HELPER ADDED HERE:
def merge_unload_parts_with_duckdb(unload_prefix: str, output_filename: str):
    """
    Downloads Athena's UNLOAD parts, uses DuckDB to safely deduplicate 
    and merge them into ONE file, and uploads it to S3.
    """
    print(f"🦆 Merging {unload_prefix} into ONE file using DuckDB...")
    
    parts_dir = os.path.join(TEMP_DIR, "duckdb_parts_" + uuid.uuid4().hex)
    os.makedirs(parts_dir, exist_ok=True)

    all_keys = list(list_s3_keys(unload_prefix))
    part_keys = [k for k in all_keys if not k.endswith("/")]

    print(f"   ⬇️ Downloading {len(part_keys)} parts locally...")
    for k in part_keys:
        dest_filename = os.path.basename(k)
        if not dest_filename.endswith(".parquet"):
            dest_filename += ".parquet"
        local_part = os.path.join(parts_dir, dest_filename)
        s3.download_file(BUCKET_NAME, k, local_part)

    print("   🔨 DuckDB is combining and deduplicating into a single Parquet file...")
    local_output = os.path.join(TEMP_DIR, output_filename)
    
    con = duckdb.connect('etl_temp.db')
    con.execute("PRAGMA temp_directory='./ducktmp';")
    con.execute("PRAGMA memory_limit='6GB';")
    con.execute("PRAGMA preserve_insertion_order=false;") 
    
    con.execute(f"""
        COPY (
            SELECT
                contract_id,
                MAX(last_action_date) AS last_action_date,
                MIN(start_date) AS start_date,
                SUM(total_spend) AS total_spend,
                ANY_VALUE(vendor_name) AS vendor_name,
                ANY_VALUE(vendor_cage) AS vendor_cage,
                ANY_VALUE(sub_agency) AS sub_agency,
                ANY_VALUE(parent_agency) AS parent_agency,
                ANY_VALUE(description) AS description,
                ANY_VALUE(platform_family) AS platform_family,
                ANY_VALUE(market_segment) AS market_segment,
                ANY_VALUE(tech_type) AS tech_type,
                ANY_VALUE(capability_name) AS capability_name,
                ANY_VALUE(naics_code) AS naics_code,
                ANY_VALUE(psc) AS psc,
                ANY_VALUE(city) AS city,
                ANY_VALUE(state) AS state,
                ANY_VALUE(country) AS country,
                ANY_VALUE(pricing_type) AS pricing_type,
                ANY_VALUE(competition_type) AS competition_type,
                ANY_VALUE(offers_count) AS offers_count,
                ANY_VALUE(set_aside_type) AS set_aside_type,
                ANY_VALUE(solicitation_id) AS solicitation_id,
                MAX(year) AS year
            FROM read_parquet('{parts_dir}/*.parquet')
            GROUP BY contract_id
        ) TO '{local_output}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    con.close()

    print(f"   ⬆️ Uploading consolidated {output_filename} to S3...")
    s3.upload_file(local_output, BUCKET_NAME, f"{CACHE_PREFIX}{output_filename}")

    os.remove(local_output)
    shutil.rmtree(parts_dir)
    print(f"   ✅ Successfully published consolidated {output_filename}!")

# AWS Clients
session = boto3.Session(region_name='us-east-1')
athena = session.client('athena')

# ✅ FIX 1: Robust Retry Policy for Network Stability
s3_config = Config(
    read_timeout=900, 
    connect_timeout=300, 
    retries={'max_attempts': 10, 'mode': 'adaptive'} # Adaptive mode handles throttling better
)
s3 = session.client('s3', config=s3_config)

def run_query(query):
    print(f"⏳ Executing: {query[:60]}...")
    resp = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': f's3://{BUCKET_NAME}/{ATHENA_OUTPUT_PREFIX}'}
    )
    qid = resp['QueryExecutionId']
    
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']: 
            break
        time.sleep(1)
        
    if state != 'SUCCEEDED':
        reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown Error')
        print(f"❌ ATHENA ERROR: {reason}")
        raise Exception(f"Query Failed: {state} - {reason}")
    
    if query.strip().upper().startswith("DROP") or query.strip().upper().startswith("CREATE"):
        return pd.DataFrame() 

    path = status['QueryExecution']['ResultConfiguration']['OutputLocation']
    key = path.replace(f's3://{BUCKET_NAME}/', '')
    
    # ✅ THE FIX: Download to temp file first (Stable & Low RAM)
    local_filename = f"{TEMP_DIR}/{qid}.csv"
    try:
        s3.download_file(BUCKET_NAME, key, local_filename)
        return pd.read_csv(local_filename, low_memory=False)
    except Exception as e:
        print(f"❌ Download Error: {e}")
        raise e
    finally:
        if os.path.exists(local_filename):
            os.remove(local_filename)

def optimize_and_upload():
    print("🚀 STARTING ETL PROCESS...")


    # --- 1. Load Raw Data ---
    print("📥 Fetching Summary Data...")
    if is_cache_fresh("summary.parquet"):
        print("   ↩️ Skipping summary.parquet (Fresh file already in S3)")
        df_sum = pd.DataFrame()
    else:
        df_sum = run_query("""
            SELECT 
                vendor_name, cage_code, sub_agency, market_segment, platform_family,
                psc_code, psc_description, CAST(naics_code AS VARCHAR) as naics_code,
                naics_description, city, state, month, year, total_spend, contract_count
            FROM dashboard_summary_v2
        """)

    print("📥 Fetching KPI by CAGE-Year...")
    if is_cache_fresh("kpis.parquet"):
        print("   ↩️ Skipping kpis.parquet")
        df_kpis = pd.DataFrame()
    else:
        df_kpis = run_query("""
            SELECT cage_code, year, SUM(total_spend) AS total_spend, SUM(contract_count) AS contract_count
            FROM dashboard_summary_v2 GROUP BY cage_code, year
        """)
    
    print("📥 Fetching Geo Data...")
    if is_cache_fresh("geo.parquet"):
        print("   ↩️ Skipping geo.parquet")
        df_geo = pd.DataFrame()
    else:
        df_geo = run_query("""
            SELECT cage_code, vendor_name, latitude, longitude, city, state
            FROM view_vendor_sites_hybrid
        """)
    
    print("📥 Generating Full Profile Universe...")
    if is_cache_fresh("profiles.parquet"):
        print("   ↩️ Skipping profiles.parquet")
        df_profiles = pd.DataFrame()
    else:
        df_profiles = run_query("""
            WITH base AS (
            SELECT
                cage_code,
                vendor_name,
                total_spend,
                contract_count,
                year,
                naics_code,
                naics_description,
                platform_family
            FROM dashboard_summary_v2
            WHERE cage_code IS NOT NULL
            ),
            agg AS (
            SELECT
                cage_code,
                SUM(total_spend) AS total_lifetime_spend,
                SUM(contract_count) AS total_contracts,
                MAX(year) AS last_active_year,
                array_join(
                slice(
                    array_agg(DISTINCT CAST(naics_code AS VARCHAR) || ' - ' || COALESCE(naics_description, 'Unknown')),
                    1, 5
                ),
                ','
                ) AS top_naics_codes,
                array_join(
                slice(array_agg(DISTINCT platform_family), 1, 5),
                ','
                ) AS top_platforms
            FROM base
            GROUP BY cage_code
            ),
            pick_name AS (
            SELECT cage_code, vendor_name
            FROM (
                SELECT
                cage_code,
                -- ✅ safety: strip trailing "(0975)"-style site codes if they exist
                regexp_replace(vendor_name, '\\s*\\(\\d{4}\\)\\s*$', '') AS vendor_name,
                year,
                total_spend,
                ROW_NUMBER() OVER (
                    PARTITION BY cage_code
                    ORDER BY year DESC, total_spend DESC, vendor_name DESC
                ) AS rn
                FROM base
                WHERE vendor_name IS NOT NULL AND trim(vendor_name) <> ''
            )
            WHERE rn = 1
            )
            SELECT
            a.cage_code,
            p.vendor_name,
            a.total_lifetime_spend,
            a.total_contracts,
            a.last_active_year,
            a.top_naics_codes,
            a.top_platforms
            FROM agg a
            LEFT JOIN pick_name p
            ON a.cage_code = p.cage_code
        """)

    print("📥 Fetching Risk Sidecar...")
    if is_cache_fresh("risk.parquet"):
        print("   ↩️ Skipping risk.parquet")
        df_risk = pd.DataFrame()
    else:
        df_risk = run_query('SELECT * FROM view_dashboard_risk_sidecar')

    # ---------------------------------------------------------
    # ### [UPDATED] FETCH NETWORK GRAPH DIRECTLY ###
    print("📥 Fetching Network Graph...")
    if is_cache_fresh("network.parquet"):
        print("   ↩️ Skipping network.parquet")
        df_network = pd.DataFrame()
    else:
        # ✅ Query the view directly, bypassing AWS Glue catalog race conditions
        df_network = run_query('SELECT * FROM ref_company_network')
        print(f"   ✅ Network Graph Loaded: {len(df_network):,} edges")
    # ---------------------------------------------------------
    # ---------------------------------------------------------

    # --- 2. OPTIMIZE & NORMALIZE ---
    print("⚡ Optimizing Data Types & Keys...")

    # Normalize columns just in case
    if 'vendor_cage' in df_sum.columns: df_sum = df_sum.rename(columns={'vendor_cage': 'cage_code'})
    if 'vendor_cage' in df_profiles.columns: df_profiles = df_profiles.rename(columns={'vendor_cage': 'cage_code'})

    # Helper: Strict String Cleaner for NAICS (Removes .0)
    def clean_naics(val):
        s = str(val).strip()
        if s.endswith('.0'): 
            s = s[:-2] 
        if s == 'nan' or s == 'None': 
            return ""
        return s

    if 'naics_code' in df_sum.columns:
        df_sum['naics_code'] = df_sum['naics_code'].apply(clean_naics)

    def clean_cage(val):
        s = str(val).upper().strip()
        if s.isdigit() and len(s) < 5:
            return s.zfill(5)
        return s

    if 'cage_code' in df_sum.columns: df_sum['cage_code'] = df_sum['cage_code'].apply(clean_cage)
    if 'cage_code' in df_geo.columns: df_geo['cage_code'] = df_geo['cage_code'].apply(clean_cage)
    if 'cage_code' in df_profiles.columns: df_profiles['cage_code'] = df_profiles['cage_code'].apply(clean_cage)

    # ✅ Clean KPI frame keys + downcast types (small + fast)
    if 'cage_code' in df_kpis.columns:
        df_kpis['cage_code'] = df_kpis['cage_code'].apply(clean_cage)

    if 'year' in df_kpis.columns:
        df_kpis['year'] = pd.to_numeric(df_kpis['year'], errors='coerce')
        df_kpis = df_kpis.dropna(subset=['year'])
        df_kpis['year'] = df_kpis['year'].astype('int16')

    if 'total_spend' in df_kpis.columns:
        df_kpis['total_spend'] = pd.to_numeric(df_kpis['total_spend'], errors='coerce').fillna(0).astype('float32')

    if 'contract_count' in df_kpis.columns:
        df_kpis['contract_count'] = pd.to_numeric(df_kpis['contract_count'], errors='coerce').fillna(0).astype('int32')

    # ---------------------------------------------------------
    # ### [NEW 2/3] CLEAN NETWORK DATA ###
    # We apply the same cleaning standards to the new dataframe
    if not df_network.empty:
        # Downcast floats
        df_network['flow_amount_raw'] = pd.to_numeric(df_network['flow_amount_raw'], errors='coerce').fillna(0).astype('float32')
        if 'flow_amount_capped' in df_network.columns:
            df_network['flow_amount_capped'] = pd.to_numeric(df_network['flow_amount_capped'], errors='coerce').fillna(0).astype('float32')
        
        # Ensure join keys are uppercase strings
        df_network['prime_gold_parent'] = df_network['prime_gold_parent'].fillna('UNKNOWN').astype(str).str.upper()
        df_network['sub_gold_parent'] = df_network['sub_gold_parent'].fillna('UNKNOWN').astype(str).str.upper()

        # Clean CAGEs using your helper function
        if 'prime_cage' in df_network.columns: df_network['prime_cage'] = df_network['prime_cage'].apply(clean_cage)
        if 'sub_cage' in df_network.columns: df_network['sub_cage'] = df_network['sub_cage'].apply(clean_cage)
    # ---------------------------------------------------------

    print("⚡ Pre-computing Search Indices for Dashboard...")
    
    if not df_sum.empty:
        # 1. Force critical columns to be clean uppercase strings (Not categories yet)
        text_cols = ['vendor_name', 'platform_family', 'sub_agency', 'market_segment', 'psc_description']
        for col in text_cols:
            if col in df_sum.columns:
                df_sum[col] = df_sum[col].astype(str).str.upper().str.strip().replace('NAN', '')

        # 2. Create a SINGLE "Fast Filter" column for global text search
        df_sum['fast_search'] = (
            df_sum['vendor_name'] + " " + 
            df_sum['platform_family'] + " " + 
            df_sum['cage_code'].fillna('')
        ).astype(str)

        # 3. NOW convert to categories to save RAM
        cat_cols = ['sub_agency', 'market_segment', 'platform_family', 'psc_code', 'psc_description', 'month', 'naics_code', 'city', 'state']
        for col in df_sum.columns:
            if col in cat_cols:
                df_sum[col] = df_sum[col].astype('category')

        df_sum['total_spend'] = pd.to_numeric(df_sum['total_spend'], errors='coerce').fillna(0).astype('float32')
        df_sum['year'] = pd.to_numeric(df_sum['year'], errors='coerce').fillna(0).astype('int16')

    if not df_geo.empty:
        df_geo['latitude'] = pd.to_numeric(df_geo['latitude'], errors='coerce')
        df_geo['longitude'] = pd.to_numeric(df_geo['longitude'], errors='coerce')
        for col in ['city', 'state']:
            if col in df_geo.columns:
                df_geo[col] = df_geo[col].fillna("").astype(str).str.upper().str.strip()
        df_geo = df_geo.dropna(subset=['latitude', 'longitude'])

    if not df_risk.empty:
        risk_text_cols = ['vendor_name', 'sub_agency', 'platform_family', 'market_segment']
        for col in risk_text_cols:
            if col in df_risk.columns:
                df_risk[col] = df_risk[col].fillna("").astype(str).str.upper().str.strip()
        
        if 'spend_amount' in df_risk.columns:
            df_risk['spend_amount'] = pd.to_numeric(df_risk['spend_amount'], errors='coerce').fillna(0)

    # --- 3. Upload Parquet Files ---
    print("💾 Uploading Optimized Parquet Files to S3...")
    
    def upload_df(df, filename):
        if df.empty: return

    # ✅ THE FIX: Write to disk buffer first
        local_path = f"{TEMP_DIR}/{filename}"
        try:
            df.to_parquet(local_path, compression='snappy')
            s3.upload_file(local_path, BUCKET_NAME, f"{CACHE_PREFIX}{filename}")
            print(f"   ✅ Uploaded {filename} ({len(df):,} rows)")
        except Exception as e:
            print(f"   ❌ FAILED to upload {filename}: {e}")
            raise e
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)

    upload_df(df_sum, "summary.parquet")
    upload_df(df_geo, "geo.parquet")
    upload_df(df_profiles, "profiles.parquet")
    upload_df(df_risk, "risk.parquet")
    upload_df(df_kpis, "kpis.parquet")
    
    # ---------------------------------------------------------
    # ### [NEW 3/3] UPLOAD NETWORK ###
    upload_df(df_network, "network.parquet")
    # ---------------------------------------------------------

    # ---------------------------------------------------------
    # ### [NEW] FETCH & UPLOAD TRANSACTIONS (Last 7 Years) ###
    # This powers the instant "Awards" tab without hitting Athena
    # ---------------------------------------------------------
    # ### [UPDATED] FETCH & UPLOAD TRANSACTIONS (5 Years) ###
    # ---------------------------------------------------------
    # ### [UPDATED] FETCH & UPLOAD TRANSACTIONS (5 Years) ###
    print("📥 Fetching Transaction History (Last 5 Years)...")
    if is_cache_fresh("transactions.parquet", max_age_hours=12):
        print("   ↩️ Skipping transactions.parquet (Fresh file already in S3)")
        df_transactions = pd.DataFrame()
    else:
        df_transactions = run_query("""
            SELECT 
                contract_id, action_date, vendor_name, vendor_cage, 
                sub_agency, parent_agency, description, spend_amount, 
                naics_code, psc, platform_family, market_segment, year,
                SUBSTR(REGEXP_REPLACE(nsn, '[^0-9]', ''), -9) AS niin
            FROM dashboard_master_view
            WHERE year >= 2021
        """)
    
    if not df_transactions.empty:
        df_transactions['spend_amount'] = pd.to_numeric(df_transactions['spend_amount'], errors='coerce').fillna(0).astype('float32')
        df_transactions['year'] = pd.to_numeric(df_transactions['year'], errors='coerce').fillna(0).astype('int16')
        for col in ['vendor_name', 'vendor_cage', 'sub_agency', 'parent_agency', 'platform_family']:
            if col in df_transactions.columns:
                df_transactions[col] = df_transactions[col].astype(str).str.upper().str.strip()

    upload_df(df_transactions, "transactions.parquet")

    # ---------------------------------------------------------
    # ### [UPDATED] FETCH ROLLED-UP CONTRACTS (Preserves ALL Business Logic) ###
    # ---------------------------------------------------------
    # ---------------------------------------------------------
    # ### [UPDATED] FETCH ROLLED-UP CONTRACTS (Preserves ALL Business Logic) ###
    # ---------------------------------------------------------
    print("📥 Fetching Rolled-up Contracts (Full 7-Year Intelligence)...")

    if is_cache_fresh("contracts_rolled.parquet", max_age_hours=12):
        print("   ↩️ Skipping contracts_rolled.parquet (Fresh file already in S3)")
    else:
        print("📦 Athena UNLOAD -> Parquet (avoids local RAM blowup)...")

        # ✅ FIX: Added MIN(action_date) and all missing metadata columns
        select_sql = """
            SELECT 
                contract_id,
                MAX(action_date) AS last_action_date,
                MIN(action_date) AS start_date,
                SUM(COALESCE(spend_amount, 0)) AS total_spend,
                MAX_BY(vendor_name, action_date) AS vendor_name,
                MAX_BY(vendor_cage, action_date) AS vendor_cage,
                MAX_BY(sub_agency, action_date) AS sub_agency,
                MAX_BY(parent_agency, action_date) AS parent_agency,
                MAX_BY(description, action_date) AS description,
                MAX_BY(platform_family, action_date) AS platform_family,
                MAX_BY(market_segment, action_date) AS market_segment,
                MAX_BY(tech_type, action_date) AS tech_type,
                MAX_BY(capability_name, action_date) AS capability_name,
                MAX_BY(naics_code, action_date) AS naics_code,
                MAX_BY(psc, action_date) AS psc,
                MAX_BY(city, action_date) AS city,
                MAX_BY(state, action_date) AS state,
                MAX_BY(country, action_date) AS country,
                MAX_BY(pricing_type, action_date) AS pricing_type,
                MAX_BY(competition_type, action_date) AS competition_type,
                MAX_BY(CAST(offers_count AS VARCHAR), action_date) AS offers_count,
                MAX_BY(set_aside_type, action_date) AS set_aside_type,
                MAX_BY(solicitation_identifier, action_date) AS solicitation_id,
                CAST(MAX(year) AS INTEGER) AS year
            FROM dashboard_master_view
            WHERE year >= 2018
            GROUP BY contract_id
        """

        unload_prefix = f"{UNLOAD_OUTPUT_PREFIX}contracts_rolled/{uuid.uuid4().hex}/"
        out_prefix = unload_to_s3(select_sql, unload_prefix)

        merge_unload_parts_with_duckdb(out_prefix, "contracts_rolled.parquet")

    # ---------------------------------------------------------
    # ### [NEW] FETCH PRODUCTS (With Logistics Data) ###
    # ---------------------------------------------------------
    # ---------------------------------------------------------
    # ### [NEW] FETCH PRODUCTS (Powers Vendor/Platform Details instantly) ###
    # ---------------------------------------------------------
    # ---------------------------------------------------------
    # ### [NEW] FETCH PRODUCTS (With Logistics Data) ###
    # ---------------------------------------------------------
    # ---------------------------------------------------------
    # ### [NEW] FETCH NSN DIMENSIONAL SUMMARY (OOM-SAFE UNLOAD) ###
    # ---------------------------------------------------------
    print("📥 Fetching NSN Filter Summary (OOM Safe)...")
    if is_cache_fresh("nsn_summary.parquet", max_age_hours=12):
        print("   ↩️ Skipping nsn_summary.parquet")
    else:
        print("📦 Athena UNLOAD -> Parquet (avoids local RAM blowup)...")
        
        nsn_summary_sql = """
            SELECT 
                SUBSTR(REGEXP_REPLACE(nsn, '[^0-9]', ''), -9) as niin,
                CAST(year AS INTEGER) as year,
                UPPER(TRIM(platform_family)) as platform_family,
                UPPER(TRIM(market_segment)) as market_segment,
                UPPER(TRIM(sub_agency)) as sub_agency,
                UPPER(TRIM(parent_agency)) as parent_agency,
                UPPER(TRIM(psc)) as psc,
                CAST(SUM(spend_amount) AS REAL) as spend_amount,
                CAST(COUNT(DISTINCT contract_id) AS INTEGER) as contracts
            FROM "market_intel_gold"."dashboard_master_view"
            WHERE nsn IS NOT NULL AND spend_amount IS NOT NULL
            GROUP BY 1, 2, 3, 4, 5, 6, 7
        """

        nsn_unload_prefix = f"{UNLOAD_OUTPUT_PREFIX}nsn_summary/{uuid.uuid4().hex}/"
        nsn_out_prefix = unload_to_s3(nsn_summary_sql, nsn_unload_prefix)

        # --- DuckDB Stitching (Zero Pandas = Zero OOM) ---
        print("   🦆 Merging NSN Summary using DuckDB...")
        nsn_parts_dir = os.path.join(TEMP_DIR, "duckdb_nsn_parts_" + uuid.uuid4().hex)
        os.makedirs(nsn_parts_dir, exist_ok=True)

        all_keys = list(list_s3_keys(nsn_out_prefix))
        part_keys = [k for k in all_keys if not k.endswith("/")]

        print(f"   ⬇️ Downloading {len(part_keys)} parts locally...")
        for k in part_keys:
            dest_filename = os.path.basename(k)
            if not dest_filename.endswith(".parquet"):
                dest_filename += ".parquet"
            s3.download_file(BUCKET_NAME, k, os.path.join(nsn_parts_dir, dest_filename))

        nsn_local_output = os.path.join(TEMP_DIR, "nsn_summary.parquet")
        
        con = duckdb.connect('etl_temp.db')
        con.execute("PRAGMA temp_directory='./ducktmp';")
        con.execute("PRAGMA memory_limit='6GB';")
        
        # We clean the null strings inside DuckDB during the COPY to replicate Pandas behavior safely
        con.execute(f"""
            COPY (
                SELECT 
                    niin,
                    year,
                    CASE WHEN platform_family IN ('NAN', 'NONE', 'UNKNOWN', '') THEN NULL ELSE platform_family END as platform_family,
                    CASE WHEN market_segment IN ('NAN', 'NONE', 'UNKNOWN', '') THEN NULL ELSE market_segment END as market_segment,
                    CASE WHEN sub_agency IN ('NAN', 'NONE', 'UNKNOWN', '') THEN NULL ELSE sub_agency END as sub_agency,
                    CASE WHEN parent_agency IN ('NAN', 'NONE', 'UNKNOWN', '') THEN NULL ELSE parent_agency END as parent_agency,
                    CASE WHEN psc IN ('NAN', 'NONE', 'UNKNOWN', '') THEN NULL ELSE psc END as psc,
                    spend_amount,
                    contracts
                FROM read_parquet('{nsn_parts_dir}/*.parquet')
            ) TO '{nsn_local_output}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)
        con.close()

        print(f"   ⬆️ Uploading nsn_summary.parquet to S3...")
        s3.upload_file(nsn_local_output, BUCKET_NAME, f"{CACHE_PREFIX}nsn_summary.parquet")

        os.remove(nsn_local_output)
        shutil.rmtree(nsn_parts_dir)
        print("   ✅ Successfully published nsn_summary.parquet!")



    # -------------------------------------------------------
    # 8. Product Catalog (Aggregated)
    # -------------------------------------------------------
    # -------------------------------------------------------
    # 8. Product Catalog (Aggregated)
    # -------------------------------------------------------
    print("📥 Fetching Product Catalog (Aggregated with Platform Context)...")
    
    if is_cache_fresh("products.parquet", max_age_hours=12):
        print("   ↩️ Skipping products.parquet (Fresh file already in S3)")
        df_products = pd.DataFrame()
    else:
        # LOGIC EXPLAINED:
        # 1. 'part_platforms' (CTE): Scans the Master View to find which Platform buys this Part the most.
        # 2. 'view_dashboard_products': Provides the clean pre-calculated trends and revenue.
        # 3. 'ref_flis_mgmt': Provides the Logistics/Demil codes (which are not in the Master View).
        
        df_products = run_query("""
            WITH part_platforms AS (
            SELECT 
                substr(regexp_replace(nsn, '[^0-9]', ''), -9) as join_niin,
                MAX_BY(platform_family, spend_amount) as derived_platform
            FROM "market_intel_gold"."dashboard_master_view"
            WHERE platform_family IS NOT NULL 
              AND nsn IS NOT NULL
            GROUP BY 1
        )
        SELECT 
            -- Identifiers
            LPAD(CAST(p.niin AS VARCHAR), 9, '0') as niin,
            p.nsn,
            p.cage,
            
            -- Metadata
            p.description,
            p.part_number,
            
            -- ✅ FIX 1: Derive FSC Code from NSN (First 4 digits)
            -- This works because FSC is literally the prefix of the NSN.
            SUBSTR(REGEXP_REPLACE(p.nsn, '[^0-9]', ''), 1, 4) AS fsc_code,
            
            -- Metrics
            p.total_revenue,
            p.total_units_sold,  -- Correct column name from your schema
            p.avg_unit_price,
            p.last_sold_date,
            p.annual_revenue_trend,
            p.market_share_pct,
            p.direct_sales_market_share_pct,
                            
            
            -- ✅ FIX 2: Restore Platform Family (Joined from the Master View CTE above)
            COALESCE(pp.derived_platform, 'UNKNOWN') as platform_family,
            
            -- Logistics Columns (Joined from Silver)
            m.ciic as demil_code,
            m.slc as shelf_life_code,
            m.mgmt_ctl as mgmt_control_code,
            m.ui as unit_of_issue,
            COALESCE(m.sos, m.moe) as source_of_supply,
            m.unit_price as govt_estimated_price,
            m.aac as acquisition_advice_code

        FROM "market_intel_gold"."view_dashboard_products" p
        
        -- Join Logistics (Silver)
        LEFT JOIN "market_intel_silver"."ref_flis_mgmt" m 
            ON LPAD(CAST(p.niin AS VARCHAR), 9, '0') = LPAD(CAST(m.niin AS VARCHAR), 9, '0')
            
        -- Join Platforms (Master View Calculation)
        LEFT JOIN part_platforms pp
            ON LPAD(CAST(p.niin AS VARCHAR), 9, '0') = pp.join_niin
            
        WHERE p.total_revenue > 0 
    """)
    
    if not df_products.empty:
        # Optimization: Downcast numbers to save RAM
        df_products['total_revenue'] = pd.to_numeric(df_products['total_revenue'], errors='coerce').fillna(0).astype('float32')
        df_products['govt_estimated_price'] = pd.to_numeric(df_products['govt_estimated_price'], errors='coerce').fillna(0).astype('float32')
        df_products['market_share_pct'] = pd.to_numeric(df_products['market_share_pct'], errors='coerce').fillna(0).astype('float32')
        df_products['direct_sales_market_share_pct'] = pd.to_numeric(df_products['direct_sales_market_share_pct'], errors='coerce').fillna(0).astype('float32')
        
        # Strings: Cleanup
        for col in ['cage', 'platform_family', 'niin', 'demil_code', 'shelf_life_code']:
            if col in df_products.columns:
                df_products[col] = df_products[col].astype(str).str.upper().str.strip().replace('NAN', '')

    upload_df(df_products, "products.parquet")

    # ---------------------------------------------------------
    # ### [NEW] FETCH OPPORTUNITIES (Powers Pipeline Instantly) ###
    # ---------------------------------------------------------
    print("📥 Fetching Active Opportunities...")
    if is_cache_fresh("opportunities.parquet"):
        print("   ↩️ Skipping opportunities.parquet")
        df_opportunities = pd.DataFrame()
    else:
        df_opportunities = run_query("""
            SELECT 
                id, sol_num, title, agency, sub_agency, 
                deadline, set_aside_type, naics, psc, 
                description, poc_email, source_system, state
            FROM "market_intel_gold"."view_unified_opportunities_dod"
            WHERE try(from_iso8601_timestamp(deadline)) >= current_date
        """)
        
    if not df_opportunities.empty:
        # Create a "Search Text" column for super-fast text filtering
        df_opportunities['search_text'] = (
            df_opportunities['title'].fillna('') + " " + 
            df_opportunities['description'].fillna('') + " " + 
            df_opportunities['sol_num'].fillna('')
        ).str.upper()

    upload_df(df_opportunities, "opportunities.parquet")

    # --- 5. Cleanup ---
    # ---------------------------------------------------------

    # --- 4. Clear Local Cache ---
    if os.path.exists("./local_data"):
        try:
            shutil.rmtree("./local_data")
            print("🧹 Cleared stale local_data cache.")
        except Exception as e:
            print(f"⚠️ Could not clear local cache: {e}")
    
    print("🎉 ETL COMPLETE. Please restart your API now.")

if __name__ == "__main__":
    optimize_and_upload()