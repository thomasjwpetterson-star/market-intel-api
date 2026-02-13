import boto3
import os

# --- CONFIGURATION ---
# 1. Path to your existing CSV file
LOCAL_FILE_PATH = "/Users/tompetterson/Documents/my-saas-projects/market-intel-api/gold/ref_exact_locations.csv"

# 2. Get Bucket Name (Same logic as your ETL)
raw_bucket = os.getenv('ATHENA_OUTPUT_BUCKET', 'a-and-d-intel-lake-newaccount')
BUCKET_NAME = raw_bucket.replace('s3://', '').split('/')[0]

# 3. Target S3 Key (Where Athena will look for it)
# We rename it to 'data.csv' to keep the folder structure clean for the Athena Table
S3_KEY = "gold/ref_vendor_sites_exact/data.csv"

def upload_gold_file():
    # Check if file exists locally
    if not os.path.exists(LOCAL_FILE_PATH):
        print(f"❌ Error: File not found at {LOCAL_FILE_PATH}")
        return

    print(f"🚀 Starting Upload...")
    print(f"   Source: {LOCAL_FILE_PATH}")
    print(f"   Target: s3://{BUCKET_NAME}/{S3_KEY}")

    # Initialize S3 Client
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name='us-east-1'
    )

    try:
        # Upload
        s3.upload_file(LOCAL_FILE_PATH, BUCKET_NAME, S3_KEY)
        print("✅ Upload Complete! The data is now live in S3.")
        print("👉 You can now proceed to run the Athena SQL setup (Step 2).")
    except Exception as e:
        print(f"❌ Upload Failed: {e}")

if __name__ == "__main__":
    upload_gold_file()