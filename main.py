from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import boto3
import time
import os

app = FastAPI(title="Mimir Advisors API")

# 1. Allow your React Dashboard to talk to this Server
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. AWS Configuration
athena = boto3.client('athena', region_name='us-east-1')
DATABASE = 'market_intel_gold'
# ⚠️ MAKE SURE THIS BUCKET NAME IS CORRECT
OUTPUT_BUCKET = 's3://a-and-d-intel-lake-newaccount/athena_temp_results/' 

def run_athena_query(query_string):
    """Helper to run SQL and wait for results."""
    try:
        # Start Query
        response = athena.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={'Database': DATABASE},
            ResultConfiguration={'OutputLocation': OUTPUT_BUCKET}
        )
        query_id = response['QueryExecutionId']

        # Wait for Completion
        while True:
            status = athena.get_query_execution(QueryExecutionId=query_id)
            state = status['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(0.5)

        if state != 'SUCCEEDED':
            raise Exception(f"Query Failed: {status['QueryExecution']['Status'].get('StateChangeReason')}")

        # Get Results
        results = athena.get_query_results(QueryExecutionId=query_id)
        return results

    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def parse_athena_results(results):
    """Converts complex Athena JSON into a clean list of dictionaries."""
    rows = results['ResultSet']['Rows']
    # If empty or just header
    if len(rows) < 2:
        return []
        
    headers = [col['VarCharValue'] for col in rows[0]['Data']]
    clean_data = []
    
    for row in rows[1:]:
        values = [col.get('VarCharValue', None) for col in row['Data']]
        clean_data.append(dict(zip(headers, values)))
        
    return clean_data

@app.get("/")
def health_check():
    return {"status": "Mimir Intelligence Online", "version": "1.0"}

@app.get("/api/dashboard/trend")
def get_spend_trend():
    """
    Returns monthly spend for the main bar chart.
    Uses 'spend_amount' and parses 'action_date' safely.
    """
    sql = """
    SELECT 
        DATE_FORMAT(DATE_PARSE(action_date, '%Y-%m-%d'), '%b') as month,
        SUM(spend_amount) / 1000000000 as spend
    FROM market_intel_gold.dashboard_master_view
    WHERE DATE_PARSE(action_date, '%Y-%m-%d') >= DATE('2024-01-01')
    GROUP BY 1, MONTH(DATE_PARSE(action_date, '%Y-%m-%d'))
    ORDER BY MONTH(DATE_PARSE(action_date, '%Y-%m-%d')) ASC
    """
    
    # Run and Parse
    raw = run_athena_query(sql)
    data = parse_athena_results(raw)
    
    # Convert 'spend' from string to float so the chart can read it
    for row in data:
        if row.get('spend'):
            row['spend'] = float(row['spend'])
        else:
            row['spend'] = 0.0
        
    return data