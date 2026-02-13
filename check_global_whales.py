import pandas as pd

def run():
    print("📊 COUNTING GLOBAL WHALES...")
    
    # 1. Load Data
    try:
        df_sum = pd.read_parquet("./local_data/summary.parquet")
    except:
        print("❌ Could not load summary.parquet")
        return

    # 2. Clean Spend Column
    # Ensure it's numeric
    df_sum['total_spend'] = pd.to_numeric(df_sum['total_spend'], errors='coerce').fillna(0)

    # 3. Count Whales (> $1M)
    whales = df_sum[df_sum['total_spend'] >= 1000000]
    mega_whales = df_sum[df_sum['total_spend'] >= 10000000] # $10M+
    
    count = len(whales['vendor_cage'].unique())
    mega_count = len(mega_whales['vendor_cage'].unique())
    
    print(f"\n💰 GLOBAL SPEND ANALYSIS (US + INT'L)")
    print("-" * 40)
    print(f"🐳 $1M+ Vendors:   {count:,} sites")
    print(f"🦖 $10M+ Vendors:  {mega_count:,} sites")
    print("-" * 40)
    
    if count < 80000:
        print(f"✅ GO! You can geocode ALL {count:,} whales this month.")
        print("   This fixes US stacks AND International precision in one run.")
    else:
        print(f"⚠️ WAIT. {count:,} is too many for one batch.")
        print(f"   Start with the {mega_count:,} Mega-Whales ($10M+) first.")

if __name__ == "__main__":
    run()