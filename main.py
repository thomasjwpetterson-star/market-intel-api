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
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

athena = boto3.client('athena', region_name='us-east-1')
DATABASE = 'market_intel_gold'

# Table Routing
MASTER_TABLE = "dashboard_master_view" 
SUMMARY_TABLE = "dashboard_summary"      

OUTPUT_BUCKET = os.getenv('ATHENA_OUTPUT_BUCKET', 's3://a-and-d-intel-lake-newaccount/athena_temp_results/')

# --- CORE ENGINE ---

def run_athena_query(query_string: str):
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
            try:
                if "." in val: row_dict[headers[i]] = float(val)
                else: row_dict[headers[i]] = int(val)
            except ValueError:
                row_dict[headers[i]] = val
        formatted_data.append(row_dict)
    return formatted_data

# Helper for Filter Pane Logic
def build_where_clause(vendor, domain, agency, platform):
    filters = []
    if vendor: filters.append(f"vendor_name = '{vendor}'")
    if domain: filters.append(f"market_segment = '{domain}'")
    if agency: filters.append(f"funding_agency = '{agency}'")
    if platform: filters.append(f"platform_family = '{platform}'")
    return " AND ".join(filters) if filters else "1=1"

# --- ENDPOINTS ---

@app.get("/api/dashboard/kpis")
def get_market_kpis(year: int = 2024, vendor: Optional[str] = None, domain: Optional[str] = None, agency: Optional[str] = None, platform: Optional[str] = None):
    where = build_where_clause(vendor, domain, agency, platform)
    sql = f"""
    SELECT 
        SUM(total_spend) / 1000000000 as total_spend_b,
        SUM(contract_count) as total_contracts,
        MAX_BY(platform_family, total_spend) as top_program,
        MAX_BY(market_segment, total_spend) as top_domain,
        (MAX(total_spend) / NULLIF(SUM(total_spend), 0)) * 100 as concentration_pct
    FROM {DATABASE}.{SUMMARY_TABLE}
    WHERE year = {year} AND {where}
    """
    data = parse_athena_results(get_cached_results(sql))
    return data[0] if data else {}

@app.get("/api/dashboard/top-vendors")
def get_top_vendors(threshold_m: float = 1.0, domain: Optional[str] = None, agency: Optional[str] = None):
    where = build_where_clause(None, domain, agency, None)
    sql = f"""
    SELECT 
        vendor_name as vendor,
        SUM(total_spend) / 1000000 as spend_m,
        SUM(contract_count) as contracts
    FROM {DATABASE}.{SUMMARY_TABLE}
    WHERE {where}
    GROUP BY vendor_name
    HAVING SUM(total_spend) >= {threshold_m * 1000000}
    ORDER BY spend_m DESC LIMIT 20
    """
    return parse_athena_results(get_cached_results(sql))

@app.get("/api/dashboard/trend")
def get_spend_trend(vendor: Optional[str] = None, domain: Optional[str] = None, agency: Optional[str] = None, platform: Optional[str] = None):
    where = build_where_clause(vendor, domain, agency, platform)
    sql = f"""
    SELECT month, SUM(total_spend) / 1000000000 as spend, MIN(action_date) as sort_date
    FROM {DATABASE}.{SUMMARY_TABLE}
    WHERE year = 2024 AND {where}
    GROUP BY month ORDER BY sort_date ASC
    """
    return parse_athena_results(get_cached_results(sql))

@app.get("/api/dashboard/details")
def get_detailed_records(vendor: str, month: Optional[str] = None):
    month_filter = ""
    if month:
        month_map = {"Jan":"01","Feb":"02","Mar":"03","Apr":"04","May":"05","Jun":"06","Jul":"07","Aug":"08","Sep":"09","Oct":"10","Nov":"11","Dec":"12"}
        month_filter = f"AND action_date LIKE '2024-{month_map.get(month)}-%'"
    sql = f"SELECT * FROM {DATABASE}.{MASTER_TABLE} WHERE recipient_parent_name = '{vendor}' {month_filter} ORDER BY action_date DESC LIMIT 100"
    return parse_athena_results(get_cached_results(sql))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))