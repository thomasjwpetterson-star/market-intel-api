import boto3
import os
import time
import pandas as pd
from io import BytesIO

# --- CONFIGURATION ---
DATABASE = 'market_intel_gold'
VIEW_NAME = 'ref_vendor_sites'  # The view we just created
# Ensure this bucket matches your environment variable or hardcode it if needed
OUTPUT_BUCKET = os.getenv('ATHENA_OUTPUT_BUCKET', 's3://a-and-d-intel-lake-newaccount/athena_temp_results/')

if not OUTPUT_BUCKET.endswith('/'):
    OUTPUT_BUCKET += '/'

def test_athena_connection():
    print(f"🔍 Testing Athena connection to view: {VIEW_NAME}...")
    
    # 1. Initialize Clients
    try:
        athena = boto3.client(
            'athena',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name='us-east-1'
        )
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name='us-east-1'
        )
    except Exception as e:
        print(f"❌ Failed to initialize AWS clients. Check your .env keys: {e}")
        return

    # 2. Run Query
    query = f'SELECT * FROM "{DATABASE}"."{VIEW_NAME}" LIMIT 10'
    print(f"   Executing: {query}")

    try:
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': DATABASE},
            ResultConfiguration={'OutputLocation': OUTPUT_BUCKET}
        )
        query_id = response['QueryExecutionId']
        
        # 3. Wait for Results
        while True:
            status = athena.get_query_execution(QueryExecutionId=query_id)
            state = status['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(0.5)
            
        if state != 'SUCCEEDED':
            reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown Error')
            print(f"❌ Query Failed: {reason}")
            return

        # 4. Fetch & Display Results
        path = status['QueryExecution']['ResultConfiguration']['OutputLocation']
        print(f"✅ Query Succeeded! Fetching results from S3...")
        
        bucket = path.split('/')[2]
        key = '/'.join(path.split('/')[3:])
        
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(BytesIO(obj['Body'].read()))
        
        print("\n📊 DATA PREVIEW:")
        if df.empty:
            print("⚠️  Result DataFrame is EMPTY. The view exists, but your SQL logic filtered out all rows.")
            print("   (Check if your INNER JOIN conditions are too strict or if 'ref_sam_entities' is empty)")
        else:
            print(df.to_string())
            print(f"\n✅ Columns found: {df.columns.tolist()}")
            print(f"✅ Row count (Limit 10): {len(df)}")

    except Exception as e:
        print(f"❌ Execution Error: {str(e)}")

if __name__ == "__main__":
    test_athena_connection()