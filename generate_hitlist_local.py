import pandas as pd
import os

# INPUTS
ADDRESS_FILE = "/Users/tompetterson/Downloads/2c6b8fa3-9254-4122-a4bb-b12824a1e30f.csv"         # The file you just downloaded
SUMMARY_FILE = "./local_data/summary.parquet" # Your local spend data

# OUTPUT
TARGET_FILE = "targets_full.csv"

def run():
    print("🔄 MERGING LOCAL SPEND WITH RAW ADDRESSES...")
    
    # 1. Load Addresses (from Athena CSV)
    if not os.path.exists(ADDRESS_FILE):
        print(f"❌ Missing '{ADDRESS_FILE}'. Please run the Athena query and download the CSV.")
        return
    
    print("   Loading addresses...")
    df_addr = pd.read_csv(ADDRESS_FILE)
    # Ensure keys are standard
    df_addr['cage_code'] = df_addr['cage_code'].astype(str).str.strip().str.upper()

    # 2. Load Spend (from Local Parquet)
    if not os.path.exists(SUMMARY_FILE):
        print(f"❌ Missing '{SUMMARY_FILE}'. Run your ETL first.")
        return

    print("   Loading spend data...")
    df_sum = pd.read_parquet(SUMMARY_FILE)
    df_sum['vendor_cage'] = df_sum['vendor_cage'].astype(str).str.strip().str.upper()

    # 3. Aggregate Spend (if summary has multiple rows per vendor)
    # We want total lifetime spend to prioritize the whales
    if 'total_spend' in df_sum.columns:
        df_spend = df_sum.groupby('vendor_cage')['total_spend'].sum().reset_index()
    else:
        print("⚠️ 'total_spend' column missing. Defaulting to 0.")
        df_spend = df_sum[['vendor_cage']].drop_duplicates()
        df_spend['total_spend'] = 0

    # 4. Join 'Em Up
    print("   Joining datasets...")
    merged = df_addr.merge(df_spend, left_on='cage_code', right_on='vendor_cage', how='left')
    
    # Fill NaN spend with 0 (for the long tail)
    merged['total_spend'] = merged['total_spend'].fillna(0)

    # 5. Prioritize
    # Sort by Spend (Highest first) -> Then by Name
    merged = merged.sort_values(by=['total_spend', 'vendor_name'], ascending=[False, True])
    
    # 6. Save
    # We only keep the columns the geocoder needs
    final_df = merged[['cage_code', 'vendor_name', 'total_spend', 'full_address']]
    
    final_df.to_csv(TARGET_FILE, index=False)
    print(f"✅ SUCCESS: Generated '{TARGET_FILE}' with {len(final_df):,} vendors.")
    print("   -> Top of the list:")
    print(final_df.head(3))
    print("\n🚀 Now run 'python run_quota_maxer.py'")

if __name__ == "__main__":
    run()