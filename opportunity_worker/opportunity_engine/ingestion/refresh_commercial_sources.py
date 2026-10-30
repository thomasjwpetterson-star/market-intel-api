import os
"""Connect held inventory and governed PDF price evidence without changing them."""
import argparse,concurrent.futures,datetime,json,uuid
from pathlib import Path
from opportunity_engine.aws import AWS
ap=argparse.ArgumentParser();ap.add_argument('--resume');args=ap.parse_args()
R=Path(os.environ.get('MIMIR_OPPORTUNITY_WORKDIR',str(Path(__file__).resolve().parent)));run=args.resume or ('commercial_'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+uuid.uuid4().hex[:6]);root=R/'inputs'/run;root.mkdir(parents=True,exist_ok=True);aws=AWS()
queries={
'governed_price_lines':'SELECT * FROM market_intel_silver.fact_sam_governed_unit_pricing',
'dla_forecasts':'SELECT * FROM market_intel_silver.fact_dla_forecast_demand_20260916_candidate',
'dla_inventory':'SELECT * FROM market_intel_silver.fact_dla_inventory_snapshot_20260916_candidate'}
cols=aws.glue.get_table(DatabaseName='market_intel_silver',Name='fact_sam_governed_unit_pricing')['Table']['StorageDescriptor']['Columns']
def portable(col):
 name='"'+col['Name']+'"';typ=col['Type']
 expr='json_format(cast('+name+' AS JSON))' if typ.startswith(('array','struct','map')) else 'cast('+name+' AS varchar)' if ('timestamp' in typ or typ=='date') else name
 return expr+' AS '+name
queries['governed_price_lines']='SELECT '+','.join(portable(c) for c in cols if not c['Type'].startswith(('array','struct','map')))+' FROM market_intel_silver.fact_sam_governed_unit_pricing'
def one(pair):
 n,q=pair;print('Connecting',n,flush=True);m=aws.extract(n,q,root,run);m['as_of']='native_retrieval_date_per_row';m['excluded_nested_columns']=[c['Name'] for c in cols if c['Type'].startswith(('array','struct','map'))] if n=='governed_price_lines' else []; m['interpretation']='Held native evidence. Inventory and incoming supply must be considered; forecast is not a purchase order. Price units, scope and dates must match.';print('Connected',n,m['scanned_bytes'],flush=True);return n,m
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:sources=dict(pool.map(one,queries.items()))
m=dict(run_id=run,sources=sources);p=root/'manifest.json';p.write_text(json.dumps(m,indent=2)+'\n');(R/'inputs/commercial_latest.json').write_text(json.dumps(dict(path=str(p.resolve())),indent=2)+'\n');print('Commercial connections complete',flush=True)
