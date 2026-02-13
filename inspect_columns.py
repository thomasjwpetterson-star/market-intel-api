import pandas as pd

try:
    print("📂 READING GEO.PARQUET...")
    df_geo = pd.read_parquet('./local_data/geo.parquet')
    print(f"✅ Columns found: {list(df_geo.columns)}")
    
    print("\n📂 READING SUMMARY.PARQUET...")
    df_sum = pd.read_parquet('./local_data/summary.parquet')
    print(f"✅ Columns found: {list(df_sum.columns)}")

except Exception as e:
    print(e)