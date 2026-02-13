import pandas as pd
import numpy as np

# FILE PATHS
SUMMARY_FILE = "./local_data/summary.parquet"
GEO_FILE = "./local_data/geo.parquet"

def run():
    print("📊 ANALYZING SPEND DISTRIBUTION...")
    
    # 1. Load Data
    # We need Summary for Spend, and Geo to know which ones are International
    try:
        df_sum = pd.read_parquet(SUMMARY_FILE)
        df_geo = pd.read_parquet(GEO_FILE)
    except Exception as e:
        print(f"❌ Error loading files: {e}")
        return

    # 2. Merge Spend + Country Info
    # (We only care about fixing International sites, remember?)
    # Ensure keys match format
    df_sum['vendor_cage'] = df_sum['vendor_cage'].astype(str).str.strip().str.upper()
    df_geo['cage_code'] = df_geo['cage_code'].astype(str).str.strip().str.upper()

    merged = df_geo.merge(df_sum, left_on='cage_code', right_on='vendor_cage', how='inner')
    
    # 3. Filter for International Only (The ones that cost money to fix)
    # (US sites are free via Zip code, so we ignore them for this budget check)
    intl_sites = merged[~merged['country_code'].isin(['USA', 'US', 'United States'])].copy()
    
    print(f"\n🌍 Total International Sites: {len(intl_sites):,}")

    # 4. Bucketing Logic
    bins = [-1, 0, 100000, 1000000, 1000000000000]
    labels = ['Zero Spend', '< $100k', '$100k - $1M', '> $1M (Whales)']
    
    intl_sites['tier'] = pd.cut(intl_sites['total_spend'].fillna(0), bins=bins, labels=labels)
    
    # 5. The Report
    stats = intl_sites.groupby('tier', observed=False).size().reset_index(name='count')
    stats['percent'] = (stats['count'] / len(intl_sites) * 100).round(1)
    
    # Calculate Total Value captured by each tier
    value_stats = intl_sites.groupby('tier', observed=False)['total_spend'].sum().reset_index(name='total_value')
    total_market_value = intl_sites['total_spend'].sum()
    value_stats['value_percent'] = (value_stats['total_value'] / total_market_value * 100).round(1)

    print("\n💰 INTERNATIONAL SPEND BREAKDOWN:")
    print(f"{'TIER':<20} | {'SITES (Cost)':<12} | {'% Volume':<10} | {'% Market Value':<15}")
    print("-" * 65)
    
    for i in range(len(stats)):
        row = stats.iloc[i]
        val_row = value_stats.iloc[i]
        print(f"{row['tier']:<20} | {row['count']:<12,} | {row['percent']:<10}% | {val_row['value_percent']}%")

    # 6. Recommendation
    whales = stats[stats['tier'] == '> $1M (Whales)']['count'].values[0]
    mid = stats[stats['tier'] == '$100k - $1M']['count'].values[0]
    
    print("-" * 65)
    print(f"\n💡 STRATEGY RECOMMENDATION:")
    if whales < 50000:
        print(f"✅ GREEN LIGHT: You have only {whales:,} high-value sites.")
        print(f"   You can geocode ALL of them + the {mid:,} mid-tier sites immediately.")
        print("   This fits easily within the 100k free tier.")
    else:
        print(f"⚠️ CAUTION: You have {whales:,} high-value sites.")
        print("   Stick to ONLY the >$1M tier for this month.")

if __name__ == "__main__":
    run()