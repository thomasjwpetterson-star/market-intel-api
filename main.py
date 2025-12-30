from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import boto3
import time
import os

app = FastAPI(title="Mimir Advisors API")

# 1. This is the part that is currently blocking your dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://market-intel-ui.vercel.app"  # Ensure this matches your Vercel domain exactly
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

athena = boto3.client('athena', region_name='us-east-1')
DATABASE = 'market_intel_gold'
OUTPUT_BUCKET = os.getenv('ATHENA_OUTPUT_BUCKET', 's3://a-and-d-intel-lake-newaccount/athena_temp_results/')

def run_athena_query(query_string):
    try:
        # ATHENA CACHING: ResultReuseByAgeConfiguration
        # This saves costs by reusing identical query results for 60 minutes.
        response = athena.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={'Database': DATABASE},
            ResultConfiguration={'OutputLocation': OUTPUT_BUCKET},
            ResultReuseConfiguration={
                'ResultReuseByAgeConfiguration': {
                    'Enabled': True,
                    'MaxAgeInMinutes': 60 
                }
            }
        )
        query_id = response['QueryExecutionId']

        while True:
            status = athena.get_query_execution(QueryExecutionId=query_id)
            state = status['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(0.5)

        if state != 'SUCCEEDED':
            reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
            raise Exception(f"Athena Query Failed: {reason}")

        return athena.get_query_results(QueryExecutionId=query_id)

    except Exception as e:
        print(f"API Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def parse_athena_results(results):
    rows = results['ResultSet']['Rows']
    if len(rows) < 2: return []
    headers = [col['VarCharValue'] for col in rows[0]['Data']]
    return [dict(zip(headers, [c.get('VarCharValue', '0') for c in r['Data']])) for r in rows[1:]]

@app.get("/api/dashboard/trend")
def get_spend_trend():
    sql = """
    SELECT 
        DATE_FORMAT(DATE_PARSE(action_date, '%Y-%m-%d'), '%b') as month,
        SUM(spend_amount) / 1000000000 as spend
    FROM market_intel_gold.dashboard_master_view
    WHERE DATE_PARSE(action_date, '%Y-%m-%d') >= DATE('2024-01-01')
    GROUP BY 1, MONTH(DATE_PARSE(action_date, '%Y-%m-%d'))
    ORDER BY MONTH(DATE_PARSE(action_date, '%Y-%m-%d')) ASC
    """
    data = parse_athena_results(run_athena_query(sql))
    for row in data:
        row['spend'] = float(row['spend']) if row.get('spend') else 0.0
    return data

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))