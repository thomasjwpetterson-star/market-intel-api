import boto3
import pandas as pd
import os
from io import BytesIO
import time
import shutil
from botocore.config import Config
import warnings

# Suppress pandas warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- CONFIGURATION ---
raw_bucket_input = os.getenv('ATHENA_OUTPUT_BUCKET', 'a-and-d-intel-lake-newaccount')
BUCKET_NAME = raw_bucket_input.replace('s3://', '').split('/')[0]
CACHE_PREFIX = "app_cache/" 
DATABASE = 'market_intel_gold'

# ✅ ADD THIS:
TEMP_DIR = "./temp_etl_downloads" 
if not os.path.exists(TEMP_DIR):
    os.makedirs(TEMP_DIR)

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
        ResultConfiguration={'OutputLocation': f's3://{BUCKET_NAME}/temp_etl/'}
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

    # =========================================================
    # [NEW] STEP 0: PRE-CALCULATE THE VIEW (MATERIALIZATION)
    # Fix: Removed "market_intel_gold". prefix to prevent Athena Parser Error
    # =========================================================
    print("🏗️ PRE-CALCULATION: Materializing Network Graph...")
    
    # FIX 1: Drop table using simple name (Context handles the DB)
    run_query('DROP TABLE IF EXISTS cache_network_materialized')

    # FIX 2: Manually clean S3 to prevent HIVE_PATH_ALREADY_EXISTS error
    s3_folder = "market_intel_gold/cache_network_materialized/"
    try:
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=s3_folder)
        for page in pages:
            if 'Contents' in page:
                delete_keys = [{'Key': obj['Key']} for obj in page['Contents']]
                s3.delete_objects(Bucket=BUCKET_NAME, Delete={'Objects': delete_keys})
    except Exception:
        pass # Ignore if folder is already empty
    
    # FIX 3: Create table using simple name
    run_query(f"""
        CREATE TABLE cache_network_materialized
        WITH (
            format = 'PARQUET',
            parquet_compression = 'SNAPPY',
            external_location = 's3://{BUCKET_NAME}/market_intel_gold/cache_network_materialized/'
        ) AS
        SELECT * FROM ref_company_network
    """)
    print("   ✅ Network Graph Materialized successfully.")
    # =========================================================

    # --- 1. Load Raw Data ---
    print("📥 Fetching Summary Data...")
    
    df_sum = run_query("""
        SELECT 
            vendor_name,
            cage_code, 
            sub_agency,
            market_segment,
            platform_family,
            psc_code,
            psc_description,
            CAST(naics_code AS VARCHAR) as naics_code,
            naics_description,
            city,
            state,
            month,
            year,
            total_spend,
            contract_count
        FROM dashboard_summary_v2
    """)
    
    print("📥 Fetching Geo Data...")
    df_geo = run_query("""
        SELECT 
            cage_code, 
            vendor_name, 
            latitude, 
            longitude,
            city,
            state
        FROM view_vendor_sites_hybrid
    """)
    
    print("📥 Generating Full Profile Universe (Dynamic)...")
    
    df_profiles = run_query("""
        SELECT 
            cage_code,
            MAX(vendor_name) as vendor_name,
            SUM(total_spend) as total_lifetime_spend,
            SUM(contract_count) as total_contracts,
            MAX(year) as last_active_year,
            
            array_join(
                slice(
                    array_agg(distinct 
                        CAST(naics_code AS VARCHAR) || ' - ' || COALESCE(naics_description, 'Unknown')
                    ), 1, 5
                ), 
            ',') as top_naics_codes,
            
            array_join(slice(array_agg(distinct platform_family), 1, 5), ',') as top_platforms
            
        FROM dashboard_summary_v2
        GROUP BY cage_code
    """)

    print("📥 Fetching Risk Sidecar...")
    df_risk = run_query('SELECT * FROM view_dashboard_risk_sidecar')

    # ---------------------------------------------------------
    # ### [UPDATED] FETCH NETWORK GRAPH FROM CACHE ###
    print("📥 Fetching Network Graph (Fast Cache)...")
    # UPDATED: Pointing to the table we created in Step 0
    # FIX 3: Removed database prefix here too for consistency
    df_network = run_query('SELECT * FROM cache_network_materialized')
    print(f"   ✅ Network Graph Loaded: {len(df_network):,} edges")
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
    
    # 1. Force critical columns to be clean uppercase strings (Not categories yet)
    # This prevents the API from having to run .astype(str).str.upper() on 4M rows
    text_cols = ['vendor_name', 'platform_family', 'sub_agency', 'market_segment', 'psc_description']
    for col in text_cols:
        if col in df_sum.columns:
            df_sum[col] = df_sum[col].astype(str).str.upper().str.strip().replace('NAN', '')

    # 2. Create a SINGLE "Fast Filter" column for global text search
    # Instead of searching 5 columns separately, the API will search just this one.
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

    df_geo['latitude'] = pd.to_numeric(df_geo['latitude'], errors='coerce')
    df_geo['longitude'] = pd.to_numeric(df_geo['longitude'], errors='coerce')
    for col in ['city', 'state']:
        if col in df_geo.columns:
            df_geo[col] = df_geo[col].fillna("").astype(str).str.upper().str.strip()
    df_geo = df_geo.dropna(subset=['latitude', 'longitude'])

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
    
    # ---------------------------------------------------------
    # ### [NEW 3/3] UPLOAD NETWORK ###
    upload_df(df_network, "network.parquet")
    # ---------------------------------------------------------

    # ---------------------------------------------------------
    # ### [NEW] FETCH & UPLOAD TRANSACTIONS (Last 3 Years) ###
    # This powers the instant "Awards" tab without hitting Athena
    print("📥 Fetching Transaction History (Last 3 Years)...")
    df_transactions = run_query("""
        SELECT 
            contract_id, action_date, vendor_name, vendor_cage, 
            sub_agency, parent_agency, description, spend_amount, 
            naics_code, psc, platform_family, year
        FROM dashboard_master_view
        WHERE year >= 2022
    """)
    
    # Basic optimization before upload
    if not df_transactions.empty:
        df_transactions['spend_amount'] = pd.to_numeric(df_transactions['spend_amount'], errors='coerce').fillna(0).astype('float32')
        df_transactions['year'] = pd.to_numeric(df_transactions['year'], errors='coerce').fillna(0).astype('int16')
        # Uppercase strings to match your new API logic
        for col in ['vendor_name', 'vendor_cage', 'sub_agency', 'parent_agency', 'platform_family']:
            if col in df_transactions.columns:
                df_transactions[col] = df_transactions[col].astype(str).str.upper().str.strip()

    upload_df(df_transactions, "transactions.parquet")

    # ---------------------------------------------------------
    # ### [NEW] FETCH PRODUCTS (Powers Vendor/Platform Details instantly) ###
    # ---------------------------------------------------------
    # ---------------------------------------------------------
    # ### [NEW] FETCH PRODUCTS (With Logistics Data) ###
    # ---------------------------------------------------------
    # -------------------------------------------------------
    # 8. Product Catalog (Aggregated)
    # -------------------------------------------------------
    print("📥 Fetching Product Catalog (Aggregated with Platform Context)...")
    
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