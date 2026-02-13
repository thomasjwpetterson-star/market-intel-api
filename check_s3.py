import boto3
import os

# 1. Config (Matches your API)
raw_bucket_input = os.getenv('ATHENA_OUTPUT_BUCKET', 'a-and-d-intel-lake-newaccount')
BUCKET_NAME = raw_bucket_input.replace('s3://', '').split('/')[0]
CACHE_PREFIX = "app_cache/"

print(f"🕵️  DIAGNOSTIC: Checking Bucket -> '{BUCKET_NAME}'")
print(f"    Prefix -> '{CACHE_PREFIX}'")

s3 = boto3.client('s3', region_name='us-east-1')

try:
    # 2. List Objects
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=CACHE_PREFIX)
    
    if 'Contents' not in response:
        print("❌ ERROR: Bucket connected, but NO files found at this prefix.")
        print("   -> Did the ETL script actually upload to 'app_cache/'?")
    else:
        print("✅ SUCCESS: Found the following files:")
        found_files = []
        for obj in response['Contents']:
            key = obj['Key']
            size_mb = obj['Size'] / (1024 * 1024)
            found_files.append(key.replace(CACHE_PREFIX, ""))
            print(f"   - {key} ({size_mb:.2f} MB)")
        
        # 3. Verify Profiles specifically
        if "profiles.parquet" in found_files:
            print("\n🎉 'profiles.parquet' EXISTS. Your data is ready.")
        else:
            print("\n⚠️  WARNING: 'profiles.parquet' is MISSING.")
            print("   -> The ETL script ran, but it didn't upload the profile file.")
            print("   -> Are you sure you ran the UPDATED ETL code?")

except Exception as e:
    print(f"\n❌ CRITICAL ACCESS ERROR: {e}")
    print("   -> Your API keys might be wrong, or you don't have list permissions.")