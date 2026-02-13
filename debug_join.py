import pandas as pd

# 1. Load your local files
print("📂 Loading local cache...")
try:
    s = pd.read_parquet('./local_data/summary.parquet')
    g = pd.read_parquet('./local_data/geo.parquet')
except Exception as e:
    print(f"❌ Error loading files: {e}")
    exit()

# 2. Find Chemring in Summary (The Money Side)
print("\n--- SUMMARY DATA CHECK ---")
chem_sum = s[s['vendor_name'].str.contains('CHEMRING AUSTRALIA', na=False, case=False)]

if chem_sum.empty:
    print("❌ Chemring NOT FOUND in Summary data. (Did you filter it out in ETL?)")
else:
    # Grab the first matching CAGE code
    cage_sum = chem_sum.iloc[0]['vendor_cage']
    print(f"✅ Found in Summary.")
    print(f"   Raw CAGE: {repr(cage_sum)}") # repr() shows hidden characters like 'Z3487 '
    print(f"   Spend: ${chem_sum.iloc[0].get('total_spend', 0):,.2f}")

# 3. Find Chemring in Geo (The Map Side)
print("\n--- GEO DATA CHECK ---")
chem_geo = g[g['cage_code'] == 'Z3487']

if chem_geo.empty:
    print("❌ Chemring NOT FOUND in Geo data.")
else:
    cage_geo = chem_geo.iloc[0]['cage_code']
    print(f"✅ Found in Geo.")
    print(f"   Raw CAGE: {repr(cage_geo)}")

# 4. The Moment of Truth: Attempt the Join
print("\n--- JOIN TEST ---")
# Simulate the exact API logic
merged = pd.merge(g, chem_sum, left_on='cage_code', right_on='vendor_cage', how='inner')

if merged.empty:
    print("❌ JOIN FAILED.")
    print("   The API is dropping this vendor because the CAGE codes do not match exactly.")
    if 'cage_sum' in locals() and 'cage_geo' in locals():
        print(f"   Compare: {repr(cage_sum)} (Summary) vs {repr(cage_geo)} (Geo)")
else:
    print(f"✅ JOIN SUCCESS! Resulting rows: {len(merged)}")
    print("   The API *should* be displaying it. The issue is likely the 'total_spend > 0' filter or the Frontend map.")