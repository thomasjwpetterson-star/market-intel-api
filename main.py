from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import boto3
import time
import os
from typing import Optional
from functools import lru_cache

app = FastAPI(title="Mimir Hybrid Intelligence API")

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://market-intel-ui.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

athena = boto3.client('athena', region_name='us-east-1')
DATABASE = 'market_intel_gold'

# Table Routing
# MASTER_TABLE: Full 25+ columns for drill-through
# SUMMARY_TABLE: Pre-aggregated columns for high-speed dashboarding
MASTER_TABLE = "dashboard_master_view" 
SUMMARY_TABLE = "dashboard_summary"      

OUTPUT_BUCKET = os.getenv('ATHENA_OUTPUT_BUCKET', 's3://a-and-d-intel-lake-newaccount/athena_temp_results/')

# --- CORE ENGINE ---

def run_athena_query(query_string: str):
    """Universal query runner with result reuse and optimized polling."""
    try:
        response = athena.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={'Database': DATABASE},
            ResultConfiguration={'OutputLocation': OUTPUT_BUCKET},
            ResultReuseConfiguration={'ResultReuseByAgeConfiguration': {'Enabled': True, 'MaxAgeInMinutes': 60}}
        )
        query_id = response['QueryExecutionId']
        while True:
            status = athena.get_query_execution(QueryExecutionId=query_id)
            state = status['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']: break
            time.sleep(0.15) 
        if state != 'SUCCEEDED':
            raise Exception(f"Athena Query Failed: {status['QueryExecution']['Status'].get('StateChangeReason')}")
        return athena.get_query_results(QueryExecutionId=query_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@lru_cache(maxsize=128)
def get_cached_results(query_string: str):
    """In-memory cache to make common dashboard interactions instantaneous."""
    return run_athena_query(query_string)

def parse_athena_results(results):
    rows = results['ResultSet']['Rows']
    if len(rows) < 1: return []
    headers = [col['VarCharValue'] for col in rows[0]['Data']]
    
    formatted_data = []
    for r in rows[1:]:
        row_dict = {}
        for i, col in enumerate(r['Data']):
            val = col.get('VarCharValue', '0')
            
            # ATTEMPT CONVERSION: Try to turn strings into numbers
            # This prevents the .toFixed() error in the frontend
            try:
                # If it's a digit or decimal, convert to float/int
                if "." in val:
                    row_dict[headers[i]] = float(val)
                else:
                    row_dict[headers[i]] = int(val)
            except ValueError:
                # If it's actual text (like a name), leave it as a string
                row_dict[headers[i]] = val
                
        formatted_data.append(row_dict)
    return formatted_data

# --- FAST SUMMARY ENDPOINTS (Dashboard Core) ---

@app.get("/api/dashboard/kpis")
def get_market_kpis(year: int = 2024, domain: Optional[str] = None):
    """Hits SUMMARY table. Future-proofed with domain filter support."""
    d_filter = f"AND market_segment = '{domain}'" if domain else ""
    sql = f"""
    SELECT 
        SUM(total_spend) / 1000000000 as total_spend_b,
        SUM(contract_count) as total_contracts,
        MAX_BY(platform_family, total_spend) as top_program,
        MAX_BY(market_segment, total_spend) as top_domain,
        (MAX(total_spend) / SUM(total_spend)) * 100 as concentration_pct
    FROM {DATABASE}.{SUMMARY_TABLE}
    WHERE year = {year} {d_filter}
    """
    data = parse_athena_results(get_cached_results(sql))
    return data[0] if data else {}

@app.get("/api/dashboard/top-vendors")
def get_top_vendors(threshold_m: float = 1.0, domain: Optional[str] = None):
    """Hits SUMMARY table. Pre-filters noise for snappy list rendering."""
    d_filter = f"AND market_segment = '{domain}'" if domain else ""
    sql = f"""
    SELECT 
        vendor_name as vendor,
        SUM(total_spend) / 1000000 as spend_m,
        SUM(contract_count) as contracts
    FROM {DATABASE}.{SUMMARY_TABLE}
    WHERE 1=1 {d_filter}
    GROUP BY vendor_name
    HAVING SUM(total_spend) >= {threshold_m * 1000000}
    ORDER BY spend_m DESC LIMIT 20
    """
    return parse_athena_results(get_cached_results(sql))

@app.get("/api/dashboard/trend")
def get_spend_trend(vendor: Optional[str] = None, domain: Optional[str] = None):
    """Hits SUMMARY table. Logic optimized for chronological line charting."""
    v_filter = f"AND vendor_name = '{vendor}'" if vendor else ""
    d_filter = f"AND market_segment = '{domain}'" if domain else ""
    sql = f"""
    SELECT month, SUM(total_spend) / 1000000000 as spend, MIN(action_date) as sort_date
    FROM {DATABASE}.{SUMMARY_TABLE}
    WHERE year = 2024 {v_filter} {d_filter}
    GROUP BY month ORDER BY sort_date ASC
    """
    return parse_athena_results(get_cached_results(sql))

# --- DEEP MASTER ENDPOINT (Drill-Through) ---

@app.get("/api/dashboard/details")
def get_detailed_records(vendor: str, month: Optional[str] = None):
    """
    DRILL-THROUGH: Hits MASTER view.
    Allows for deep-diving into the raw 25+ columns for specific selections.
    """
    month_filter = ""
    if month:
        # Mapping month names to standard ISO format for action_date filtering
        month_map = {"Jan":"01","Feb":"02","Mar":"03","Apr":"04","May":"05","Jun":"06",
                     "Jul":"07","Aug":"08","Sep":"09","Oct":"10","Nov":"11","Dec":"12"}
        m_code = month_map.get(month)
        month_filter = f"AND action_date LIKE '2024-{m_code}-%'"

    sql = f"""
    SELECT * FROM {DATABASE}.{MASTER_TABLE} 
    WHERE vendor_name = '{vendor}'
    {month_filter}
    ORDER BY action_date DESC
    LIMIT 100
    """
    return parse_athena_results(get_cached_results(sql))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))