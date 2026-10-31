"""Stock-supported item demand beyond published forecasts and direct sellers.

The value is a conditional annual-usage scenario, never a net purchase quantity.
No source forecasts (including zero/negative rows) are overridden by this model.
"""
import hashlib
import json
from pathlib import Path

import duckdb

POLICY='reported-annual-demand-stock-outlook-v1'


def prepare(sources, folder, as_of):
    required=('dla_inventory','dla_forecasts','item_price_references','item_reference_facts','demand_candidates')
    if not all(n in sources for n in required):return {}
    folder=Path(folder);folder.mkdir(parents=True,exist_ok=True)
    c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='3GB';SET preserve_insertion_order=false")
    c.execute("SET temp_directory='"+str(folder/'tmp').replace("'","''")+"'")
    for name in required:
        c.read_parquet([f['path'] for f in sources[name]['files']]).create_view(name)
    c.execute("CREATE TEMP TABLE forecasted AS SELECT DISTINCT niin FROM dla_forecasts WHERE substr(cast(forecast_month AS VARCHAR),1,10)>='"+as_of+"'")
    c.execute("""CREATE TEMP TABLE latest AS SELECT niin,max(substr(snapshot_date,1,10)) snapshot_date
    FROM dla_inventory WHERE public_release_eligible AND try_cast(substr(snapshot_date,1,10) AS DATE) BETWEEN cast(? AS DATE)-INTERVAL 90 DAY AND cast(? AS DATE)
    GROUP BY niin""",[as_of,as_of])
    c.execute("""CREATE TEMP TABLE inventory AS SELECT i.niin,l.snapshot_date inventory_date,
    CASE WHEN count(DISTINCT try_cast(i.annual_demand_quantity AS DOUBLE))=1 THEN min(try_cast(i.annual_demand_quantity AS DOUBLE)) END annual_demand_quantity,
    CASE WHEN count(DISTINCT try_cast(i.total_stock AS DOUBLE))=1 THEN min(try_cast(i.total_stock AS DOUBLE)) END stock_on_hand,
    CASE WHEN count(DISTINCT try_cast(i.backorder_qty AS DOUBLE))=1 THEN min(try_cast(i.backorder_qty AS DOUBLE)) END backorder_quantity,
    to_json(list(DISTINCT struct_pack(nsn:=i.nsn,annual_demand:=i.annual_demand_quantity,stock:=i.total_stock,backorders:=i.backorder_qty,date:=i.snapshot_date,condition:=i.condition_code,source_release:=i.source_release,source_file:=i.source_file,public_release_eligible:=i.public_release_eligible))) inventory_evidence_json
    FROM dla_inventory i JOIN latest l ON i.niin=l.niin AND substr(i.snapshot_date,1,10)=l.snapshot_date
    WHERE i.public_release_eligible GROUP BY i.niin,l.snapshot_date""")
    c.execute("""CREATE TEMP TABLE names AS SELECT niin,min(description) item_name,min(nsn) nsn
    FROM item_reference_facts SEMI JOIN inventory USING(niin) WHERE rnvc_codes='2' GROUP BY niin""")
    c.execute("""CREATE TEMP TABLE context AS SELECT i.*,n.item_name,n.nsn,p.catalogue_unit,
    p.government_reference_unit_price,p.market_price_median,p.market_price_low,p.market_price_high,p.market_observations,p.market_sellers,p.first_price_date,p.latest_price_date,p.price_preview_json,p.sample_reference_evidence,p.source_snapshot_sha256,
    round(i.annual_demand_quantity*p.government_reference_unit_price,2) government_reference_lot_scenario,
    round(i.annual_demand_quantity*p.market_price_median,2) market_price_lot_scenario,
    CASE WHEN i.backorder_quantity>0 THEN 'reported_backorders' ELSE 'stock_below_reported_annual_demand' END inventory_signal
    FROM inventory i JOIN item_price_references p ON p.niin=i.niin AND p.unit='EA' AND p.catalogue_unit='EA'
    JOIN names n ON n.niin=i.niin ANTI JOIN forecasted f ON f.niin=i.niin
    WHERE i.annual_demand_quantity>0 AND (i.backorder_quantity>0 OR (i.stock_on_hand>=0 AND i.stock_on_hand<i.annual_demand_quantity))
    AND coalesce(p.market_price_median,p.government_reference_unit_price)>0""")
    def emit(name,sql):
        path=folder/(name+'.parquet');c.execute('CREATE OR REPLACE TEMP VIEW result AS '+sql)
        c.execute('COPY result TO ? (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 2048)',[str(path)])
        sha=hashlib.sha256(path.read_bytes()).hexdigest();rows=c.execute('SELECT count(*) FROM result').fetchone()[0]
        return dict(path=str(path),files=[dict(path=str(path),sha256=sha)],sha256=sha,rows=rows,as_of=as_of,kind=POLICY,status='SUCCEEDED')
    context_sql="""SELECT *,sha256('stock-outlook-v1|'||niin) episode_id,
    sha256(concat_ws('|',niin,inventory_evidence_json,cast(market_price_median AS VARCHAR),cast(government_reference_unit_price AS VARCHAR),price_preview_json,'ASOF')) record_hash,
    'ASOF' window_start,cast(cast(cast('ASOF' AS DATE)+INTERVAL 1 YEAR-INTERVAL 1 DAY AS DATE) AS VARCHAR) window_end,
    'reported_annual_usage_EA_assumed_not_source_forecast' valuation_status,
    'Annual-demand figure repeated for the next twelve months, using catalogue EA and a held same-item price. Gross usage scenario, not a source forecast, new procurement, net stock requirement or expected company revenue. Stock is not subtracted; incoming supply and existing contracts remain unknown.' interpretation
    FROM context""".replace('ASOF',as_of)
    out={'item_replenishment_context':emit('item_replenishment_context',context_sql)}
    c.read_parquet(out['item_replenishment_context']['path']).create_view('outlook')
    expressions={
        'episode_id':'episode_id','record_hash':'record_hash','lane':"'stock_supported_item_demand'",'title':"coalesce(nullif(item_name,''),'NSN '||nsn)||' — replenishment outlook'",'scope_text':"coalesce(item_name,'Item supply')||'. NSN '||nsn||'. The DLA inventory snapshot reports annual demand and backorder or stock-pressure context. If that annual usage repeats, this is a twelve-month gross usage scenario; no source forecast or purchase request is inferred.'",
        'customer_name':"'Defense Logistics Agency'",'psc':'substr(nsn,1,4)','window_start':'window_start','window_end':'window_end','date_basis':"'Conditional next-twelve-month usage scenario; no solicitation, award or response date is predicted'",
        'value_basis':"'reported_annual_usage_times_exact_item_reference_price'",'value_scope':"'Conditional gross annual usage scenario; not new procurement or company revenue'",
        'value_inputs_json':"to_json(struct_pack(niin:=niin,nsn:=nsn,scenario_quantity:=annual_demand_quantity,quantity_basis:='DLA-reported annual demand quantity',quantity_unit_assumption:='Consistent catalogue EA; inventory source does not state units',inventory_date:=inventory_date,inventory_signal:=inventory_signal,stock_on_hand:=stock_on_hand,backorder_quantity:=backorder_quantity,inventory_evidence:=json(inventory_evidence_json),price_evidence:=json(price_preview_json),source_price_median:=market_price_median,source_government_reference:=government_reference_unit_price,company_addressable_value:=NULL::DOUBLE,source_forecast_present:=false,method:='Reported annual demand repeated for twelve months; stock not subtracted; current incoming supply and existing orders unconfirmed'))",
        'source_name':"'dla_reported_annual_demand'",'source_evidence_id':"sha256(inventory_evidence_json)",'source_url':"'https://www.dla.mil/Information-Operations/Services/Applications/FOIA/'",'source_locator':"niin||'|inventory-date|'||inventory_date",'native_identifier':'nsn',
        'procurement_state':"'reported_annual_demand_with_stock_pressure'",'reconciled_state':"'future_buying_not_established'",'access_state':"'Exact item-identifying company reference required; qualification, configuration, supply route and data rights remain specific to the item and notice'",
        'rationale':"'A recent public DLA inventory record reports positive annual demand and backorders or stock below that annual quantity. No future DLA forecast row for this item is present. Existing company/item references provide the matching route, independent of who sold previous purchases.'",
        'limitations':"'Annual usage can change and may be covered by incoming stock or existing contracts. This is a replenishment outlook, not an announced opportunity. The source does not state the unit; only consistent catalogue EA is priced. An item reference does not establish technical qualification or manufacturing rights.'"}
    fields=c.execute('DESCRIBE demand_candidates').fetchall();projection=','.join('cast('+expressions.get(n,'NULL')+' AS '+typ+') "'+n+'"' for n,typ,*_ in fields)
    out['demand_candidates']=emit('demand_candidates',"SELECT * FROM demand_candidates WHERE lane<>'stock_supported_item_demand' UNION ALL SELECT "+projection+' FROM outlook')
    c.close();print('Stock-supported annual usage contexts',out['item_replenishment_context']['rows'],flush=True);return out


def augment(directory):
    """Add compatible value evidence; annual usage never becomes a forecast row."""
    r=Path(directory);m=json.loads((r/'manifest.json').read_text())
    if 'item_replenishment_context' not in m['tables']:return
    c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='3GB';SET preserve_insertion_order=false")
    c.execute("SET temp_directory='"+str(r/'replenishment_values_tmp').replace("'","''")+"'")
    for name in ('company_item_valuations','company_matches','item_replenishment_context'):
        c.read_parquet(m['tables'][name]['path']).create_view('old_'+name)
    mapping={
        'company_id':'m.company_id','episode_id':'x.episode_id','record_hash':'x.record_hash','niin':'x.niin','quantity':'x.annual_demand_quantity','catalogue_unit':'x.catalogue_unit','first_month':'x.window_start','last_month':'x.window_end','month_count':'1',
        'monthly_evidence_json':"to_json([struct_pack(period_start:=x.window_start,period_end:=x.window_end,quantity:=x.annual_demand_quantity,quantity_basis:='Conditional annual usage repeated; not a source monthly forecast',inventory_date:=x.inventory_date,inventory_evidence:=json(x.inventory_evidence_json))])",
        'catalogue_evidence_json':"to_json(struct_pack(sample_reference_evidence:=x.sample_reference_evidence,source_snapshot_sha256:=x.source_snapshot_sha256,price_preview:=json(x.price_preview_json)))"}
    context_columns={n for n,*_ in c.execute('DESCRIBE old_item_replenishment_context').fetchall()}
    for n in context_columns:mapping.setdefault(n,'x."'+n+'"')
    fields=c.execute('DESCRIBE old_company_item_valuations').fetchall();projection=','.join('cast('+mapping.get(n,'NULL')+' AS '+typ+') "'+n+'"' for n,typ,*_ in fields)
    sql='SELECT v.* FROM old_company_item_valuations v WHERE NOT EXISTS(SELECT 1 FROM old_item_replenishment_context x WHERE x.episode_id=v.episode_id) UNION ALL SELECT '+projection+' FROM old_item_replenishment_context x JOIN (SELECT DISTINCT company_id,episode_id,opportunity_hash FROM old_company_matches) m ON m.episode_id=x.episode_id AND m.opportunity_hash=x.record_hash'
    c.execute('CREATE TEMP VIEW result AS '+sql);path=r/'company_item_valuations.parquet';temp=r/'company_item_valuations.replenishment.parquet'
    c.execute('COPY result TO ? (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 2048)',[str(temp)]);rows=c.execute('SELECT count(*) FROM read_parquet(?)',[str(temp)]).fetchone()[0];temp.replace(path)
    columns=[dict(Name=n,Type={'VARCHAR':'string','DOUBLE':'double','BIGINT':'bigint','BOOLEAN':'boolean'}[t]) for n,t,*_ in c.execute('DESCRIBE result').fetchall()]
    m['tables']['company_item_valuations']=dict(path=str(path),rows=rows,sha256=hashlib.sha256(path.read_bytes()).hexdigest(),columns=columns);m['replenishment_policy']=POLICY;(r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close()
