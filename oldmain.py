from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import boto3
import os
import pandas as pd
from io import BytesIO
from typing import Optional, List
import threading
import time

app = FastAPI(title="Mimir Hybrid Intelligence API - Turbo Mode")

# --- CORS ---
origins = ["http://localhost:3000", "http://127.0.0.1:3000", "http://localhost:10000", "http://127.0.0.1:10000"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- AWS Config ---
athena = boto3.client(
    'athena',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name='us-east-1'
)

DATABASE = 'market_intel_gold'
SUMMARY_TABLE = "dashboard_summary_v2"
MASTER_TABLE = "dashboard_master_view"
# UPDATED: Pointing to the new SQL-based robust view
GEO_VIEW = "ref_vendor_sites_sql" 

OUTPUT_BUCKET = os.getenv('ATHENA_OUTPUT_BUCKET', 's3://a-and-d-intel-lake-newaccount/athena_temp_results/')
if not OUTPUT_BUCKET.endswith('/'): OUTPUT_BUCKET += '/'

# --- GLOBAL DATA CACHE ---
GLOBAL_CACHE = {
    "df": pd.DataFrame(),      # Summary Data (Spend, Counts)
    "geo_df": pd.DataFrame(),  # Geo Data (Vendor Lat/Lon)
    "is_loading": False
}

def run_athena_and_download(query):
    """Helper to run Athena query and download CSV to Pandas"""
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': OUTPUT_BUCKET}
    )
    query_id = response['QueryExecutionId']
    
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']: break
        time.sleep(0.5)
        
    if state != 'SUCCEEDED': raise Exception(f"Athena Query Failed: {state}")

    s3 = boto3.client('s3', aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'), aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'), region_name='us-east-1')
    path = status['QueryExecution']['ResultConfiguration']['OutputLocation']
    bucket = path.split('/')[2]
    key = '/'.join(path.split('/')[3:])
    
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(BytesIO(obj['Body'].read()), thousands=',')

def load_data_into_memory():
    """Downloads summary AND geo data into Pandas for instant filtering"""
    global GLOBAL_CACHE
    if GLOBAL_CACHE["is_loading"]: return
    GLOBAL_CACHE["is_loading"] = True
    
    print("🚀 TURBO MODE: Warming up... (Downloading data from S3)")
    
    try:
        # --- 1. LOAD SUMMARY DATA ---
        query_summary = f"SELECT * FROM {SUMMARY_TABLE}"
        df_summary = run_athena_and_download(query_summary)

        # --- NEW: Ensure CAGE is a string ---
        if 'vendor_cage' in df_summary.columns:
            # Clean and Upper Case for matching
            df_summary['vendor_cage'] = df_summary['vendor_cage'].astype(str).replace('nan', '').str.strip().str.upper()
        # ------------------------------------
        
        # Optimize Types
        df_summary['total_spend'] = pd.to_numeric(df_summary['total_spend'], errors='coerce').fillna(0)
        df_summary['contract_count'] = pd.to_numeric(df_summary['contract_count'], errors='coerce').fillna(0)
        df_summary['year'] = pd.to_numeric(df_summary['year'], errors='coerce').fillna(0).astype(int)
        
        # Normalize Strings & Vendor Name
        str_cols = ['vendor_name', 'sub_agency', 'market_segment', 'platform_family', 'psc_code', 'psc_description', 'month']
        for col in str_cols:
            if col in df_summary.columns:
                df_summary[col] = df_summary[col].astype(str).replace('nan', '').str.strip()
        
        # KEY FIX: Upper case vendor name for better matching with Geo data
        df_summary['vendor_name'] = df_summary['vendor_name'].str.upper()
        
        GLOBAL_CACHE["df"] = df_summary
        print(f"✅ SUMMARY DATA: {len(df_summary)} rows loaded.")

        # --- 2. LOAD GEO DATA (New SQL View) ---
        print("🌍 LOADING MAP DATA: Fetching distinct vendor locations...")
        
        # UPDATED QUERY: ADDED 'cage_code' HERE
        query_geo = f'SELECT cage_code, vendor_name, latitude, longitude FROM "market_intel_gold"."{GEO_VIEW}"'
        
        df_geo = run_athena_and_download(query_geo)
        
        # Optimize Geo Data
        df_geo['latitude'] = pd.to_numeric(df_geo['latitude'], errors='coerce')
        df_geo['longitude'] = pd.to_numeric(df_geo['longitude'], errors='coerce')
        
        # KEY FIX: Normalize Geo CAGE to match Summary CAGE
        if 'cage_code' in df_geo.columns:
            df_geo['cage_code'] = df_geo['cage_code'].astype(str).replace('nan', '').str.strip().str.upper()
        
        # Normalize Vendor Name
        df_geo['vendor_name'] = df_geo['vendor_name'].astype(str).replace('nan', '').str.strip().str.upper()
        
        df_geo = df_geo.dropna(subset=['latitude', 'longitude'])
        
        GLOBAL_CACHE["geo_df"] = df_geo
        print(f"✅ MAP DATA: {len(df_geo)} locations loaded instantly.")

    except Exception as e:
        print(f"❌ LOAD ERROR: {e}")
    finally:
        GLOBAL_CACHE["is_loading"] = False

# Start loading immediately
threading.Thread(target=load_data_into_memory).start()

# --- FILTER ENGINE ---
def filter_dataframe(df, years, vendor, domain, agency, platform, psc):
    if df.empty: return df
    
    # 1. Base Filter (Year)
    mask = df['year'].isin(years)
    
    # 2. Logic Filters
    if vendor: mask &= (df['vendor_name'] == vendor.upper()) # Ensure case match
    if domain: mask &= (df['market_segment'] == domain)
    if agency: mask &= (df['sub_agency'] == agency)
    if platform: mask &= (df['platform_family'] == platform)
    
    # 3. PSC Filter
    if psc: mask &= ( (df['psc_code'] == psc) | (df['psc_description'] == psc) )
        
    return df[mask].copy()

# --- ENDPOINTS ---

@app.get("/api/dashboard/status")
def get_status():
    return {
        "ready": not GLOBAL_CACHE["df"].empty and not GLOBAL_CACHE["geo_df"].empty,
        "loading": GLOBAL_CACHE["is_loading"]
    }

@app.get("/api/dashboard/filter-options")
def get_filter_options(
    years: List[int] = Query([2024]), 
    domain: Optional[str] = None, 
    agency: Optional[str] = None, 
    platform: Optional[str] = None,
    psc: Optional[str] = None,
    vendor: Optional[str] = None
):
    df = GLOBAL_CACHE["df"]
    if df.empty: return {"years": [], "agencies": [], "domains": [], "platforms": [], "top_parents": [], "other_parents": []}

    # Reactive Filtering
    filtered = filter_dataframe(df, years, None, domain, agency, platform, psc)
    
    vendor_groups = filtered.groupby('vendor_name')['total_spend'].sum().sort_values(ascending=False)
    sorted_v = vendor_groups.index.tolist()

    # Cascading Logic
    valid_options = filter_dataframe(df, years, vendor, None, None, None, None)

    return {
        "years": sorted(df['year'].unique().tolist()),
        "agencies": sorted(valid_options['sub_agency'].unique().tolist()),
        "domains": sorted(valid_options['market_segment'].unique().tolist()),
        "platforms": sorted(valid_options['platform_family'].unique().tolist()),
        "top_parents": sorted_v[:50],
        "other_parents": sorted(sorted_v[50:])
    }

@app.get("/api/dashboard/kpis")
def get_market_kpis(years: List[int] = Query([2024]), vendor: Optional[str]=None, domain: Optional[str]=None, agency: Optional[str]=None, platform: Optional[str]=None, psc: Optional[str]=None):
    df = GLOBAL_CACHE["df"]
    if df.empty: return {"total_spend_b": 0, "total_contracts": 0}
    filtered = filter_dataframe(df, years, vendor, domain, agency, platform, psc)
    return {"total_spend_b": filtered['total_spend'].sum()/1_000_000_000.0, "total_contracts": int(filtered['contract_count'].sum())}

@app.get("/api/dashboard/trend")
def get_spend_trend(years: List[int] = Query([2024]), vendor: Optional[str]=None, domain: Optional[str]=None, agency: Optional[str]=None, platform: Optional[str]=None, psc: Optional[str]=None, mode: str='yearly'):
    df = GLOBAL_CACHE["df"]
    if df.empty: return []
    filtered = filter_dataframe(df, years, vendor, domain, agency, platform, psc)
    
    if mode == 'yearly':
        grouped = filtered.groupby('year')['total_spend'].sum().reset_index()
        grouped.columns = ['label', 'spend']
        grouped['spend'] = grouped['spend'] / 1_000_000_000.0
        return grouped.to_dict(orient='records')
    else:
        if 'month' not in filtered.columns: return []
        grouped = filtered.groupby('month')['total_spend'].sum().reset_index().sort_values('month')
        grouped.columns = ['label', 'spend']
        grouped['spend'] = grouped['spend'] / 1_000_000_000.0
        return grouped.to_dict(orient='records')

@app.get("/api/dashboard/top-vendors")
def get_top_vendors(years: List[int] = Query([2024]), threshold_m: float=1.0, domain: Optional[str]=None, agency: Optional[str]=None, platform: Optional[str]=None, psc: Optional[str]=None, vendor: Optional[str]=None):
    df = GLOBAL_CACHE["df"]
    if df.empty: return []
    filtered = filter_dataframe(df, years, vendor, domain, agency, platform, psc)
    grouped = filtered.groupby('vendor_name').agg({'total_spend': 'sum', 'contract_count': 'sum'}).reset_index().sort_values('total_spend', ascending=False).head(50)
    return [{"vendor": r['vendor_name'], "spend_m": r['total_spend']/1_000_000.0, "contracts": int(r['contract_count'])} for _, r in grouped.iterrows()]

@app.get("/api/dashboard/distributions")
def get_market_distributions(years: List[int] = Query([2024]), domain: Optional[str]=None, agency: Optional[str]=None, platform: Optional[str]=None, psc: Optional[str]=None, vendor: Optional[str]=None):
    df = GLOBAL_CACHE["df"]
    if df.empty: return {"platform_dist": [], "domain_dist": []}
    
    filtered = filter_dataframe(df, years, vendor, domain, agency, platform, psc)
    
    def get_dist(group_col):
        if group_col not in filtered.columns: return []
        valid = filtered[(filtered[group_col] != '') & (filtered[group_col] != '0') & (filtered[group_col].notna())]
        
        grouped = valid.groupby(group_col)['total_spend'].sum().reset_index()
        grouped.columns = ['label', 'value']
        grouped = grouped[grouped['value'] > 0].sort_values('value', ascending=False)
        
        total = grouped['value'].sum()
        if total == 0: return []
        
        top_5 = grouped.head(5)
        other_val = grouped.iloc[5:]['value'].sum()
        
        result = [{"label": r['label'], "value": round((r['value']/total)*100, 1), "raw_value": r['value']} for _, r in top_5.iterrows()]
        if other_val > 0: result.append({"label": "Other", "value": round((other_val/total)*100, 1), "raw_value": other_val})
        return result

    return {"platform_dist": get_dist('platform_family'), "domain_dist": get_dist('psc_description')}


# --- MAP ENDPOINT (HIGH PERFORMANCE MODE) ---
@app.get("/api/dashboard/map")
def get_map_data(years: List[int] = Query([2024]), vendor: Optional[str]=None, domain: Optional[str]=None, agency: Optional[str]=None, platform: Optional[str]=None, psc: Optional[str]=None):
    df = GLOBAL_CACHE["df"]
    geo_df = GLOBAL_CACHE["geo_df"]
    
    if df.empty or geo_df.empty: return []
    
    # 1. Filter
    filtered_summary = filter_dataframe(df, years, vendor, domain, agency, platform, psc)
    
    # 2. Aggregate
    active_vendors = filtered_summary.groupby(['vendor_cage', 'vendor_name'])['total_spend'].sum().reset_index()
    active_vendors = active_vendors[active_vendors['total_spend'] > 0]
    
    # 3. Join
    mapped_vendors = pd.merge(geo_df, active_vendors, left_on='cage_code', right_on='vendor_cage', how='inner')

    # 4. FILTER: Keep > $1M (Remove noise)
    mapped_vendors = mapped_vendors[mapped_vendors['total_spend'] > 1_000_000]
    
    # 5. LIMIT: Increased to 50,000 for WebGL rendering
    mapped_vendors = mapped_vendors.sort_values('total_spend', ascending=False).head(50000)
    
    print(f"📍 Map Update: Sending {len(mapped_vendors)} points to WebGL frontend.")
    
    return [
        {
            "id": i, 
            "vendor": r['vendor_name_y'], 
            "lat": r['latitude'], 
            "lon": r['longitude'], 
            "spend": r['total_spend']
        }
        for i, r in mapped_vendors.iterrows()
    ]

# --- DEEP DIVE (Athena) ---
@app.get("/api/dashboard/details")
def get_detailed_records(vendor: str, month: Optional[str] = None):
    m_filter = ""
    if month:
        m_map = {"Jan":"01","Feb":"02","Mar":"03","Apr":"04","May":"05","Jun":"06","Jul":"07","Aug":"08","Sep":"09","Oct":"10","Nov":"11","Dec":"12"}
        m_filter = f"AND action_date LIKE '2024-{m_map.get(month)}-%'"
    
    safe_v = vendor.replace("'", "''")
    query = f"SELECT * FROM {MASTER_TABLE} WHERE vendor_name = '{safe_v}' {m_filter} LIMIT 100"
    
    try:
        df = run_athena_and_download(query)
        return df.to_dict(orient='records')
    except: return []

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)