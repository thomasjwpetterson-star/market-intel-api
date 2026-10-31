"""Product-market scale beyond direct awards, without attributing seller revenue.

The aligned market reference is an explicit comparison measure. It does not
establish which manufacturer's stock was delivered, margin, rights or workshare.
"""
import hashlib
import json
from pathlib import Path

import duckdb

POLICY='aligned-company-product-market-v1'
REFERENCE='https://www.dla.mil/Defense-Data-Standards/Resources/Data-Dictionary/udt_114435_param_orderby/IC/udt_114435_param_direction/ascending/udt_114435_param_detail/3013689/'


def build(directory):
    r=Path(directory);m=json.loads((r/'manifest.json').read_text())
    if 'item_reference_facts' not in m['sources'] or 'catalogue_sales_evidence' not in m['tables']:return
    c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='3GB';SET preserve_insertion_order=false")
    c.execute("SET temp_directory='"+str(r/'product_market_tmp').replace("'","''")+"'")
    c.read_parquet([f['path'] for f in m['sources']['item_reference_facts']['files']]).create_view('refs')
    c.read_parquet(m['tables']['catalogue_sales_evidence']['path']).create_view('sales')
    c.read_parquet(m['tables']['company_profiles']['path']).create_view('profiles')
    for name in ('company_activity_baselines','company_reporting_lag_baselines'):
        if name in m['tables']:c.read_parquet(m['tables'][name]['path']).create_view(name)
    if 'company_activity_baselines' not in m['tables']:
        c.execute('CREATE TEMP VIEW company_activity_baselines AS SELECT NULL::VARCHAR company_id,NULL::DOUBLE annual_net_obligations,NULL::VARCHAR period_start,NULL::VARCHAR period_end WHERE false')
    if 'company_reporting_lag_baselines' not in m['tables']:
        c.execute('CREATE TEMP VIEW company_reporting_lag_baselines AS SELECT NULL::VARCHAR company_id,NULL::DOUBLE lag_adjusted_annual_net_obligations,NULL::DOUBLE lag_adjusted_annual_dod_net_obligations,NULL::VARCHAR lag_adjusted_period_start,NULL::VARCHAR lag_adjusted_period_end WHERE false')
    c.execute("""CREATE TEMP TABLE periods AS SELECT p.company_id,
    CASE WHEN l.lag_adjusted_annual_dod_net_obligations>0 THEN l.lag_adjusted_annual_net_obligations ELSE a.annual_net_obligations END observed_direct_obligations,
    coalesce(CASE WHEN l.lag_adjusted_annual_dod_net_obligations>0 THEN l.lag_adjusted_period_start ELSE a.period_start END,cast(cast(? AS DATE)-INTERVAL 1 YEAR+INTERVAL 1 DAY AS VARCHAR)) market_period_start,
    coalesce(CASE WHEN l.lag_adjusted_annual_dod_net_obligations>0 THEN l.lag_adjusted_period_end ELSE a.period_end END,?) market_period_end
    FROM profiles p LEFT JOIN company_activity_baselines a USING(company_id) LEFT JOIN company_reporting_lag_baselines l USING(company_id)""",[m['as_of'],m['as_of']])
    # Codes are interpreted at each exact CAGE/part/NIIN record. A code on one
    # part is never combined with another part to invent procurement approval.
    c.execute("""CREATE TEMP TABLE part_roles AS SELECT cage_code,niin,upper(trim(part_number)) part_key,
    bool_or(trim(rnvc_codes)='2') item_identifying,
    bool_or(trim(rnvc_codes)='2' AND trim(rncc_codes)='3') design_control_reference,
    bool_or(trim(rnvc_codes)='2' AND trim(rnsc_codes)='A') procurement_authorized,
    bool_or(trim(rnvc_codes)='2' AND trim(rnsc_codes)='K') manufacturer_or_design_control,
    bool_or(trim(rnsc_codes)='L') distributor_reference,
    bool_or(trim(rnvc_codes)='9') inactive_or_obsolete_reference,
    bool_or(trim(rnsc_codes)='B') procurement_not_authorized
    FROM refs SEMI JOIN profiles p ON p.company_id='CAGE:'||refs.cage_code
    WHERE regexp_full_match(cage_code,'[A-Z0-9]{5}') AND regexp_full_match(niin,'[0-9]{9}')
    GROUP BY cage_code,niin,upper(trim(part_number))""")
    for partition in range(32):
        sql="""SELECT 'CAGE:'||cage_code company_id,niin,count(*)::BIGINT part_references,
        bool_or(item_identifying) item_identifying,bool_or(design_control_reference) design_control_reference,
        bool_or(procurement_authorized) procurement_authorized,bool_or(manufacturer_or_design_control) manufacturer_or_design_control,
        bool_or(distributor_reference) distributor_reference,bool_or(inactive_or_obsolete_reference) inactive_or_obsolete_reference,
        bool_or(procurement_not_authorized) procurement_not_authorized
        FROM part_roles WHERE hash(cage_code)%32=PART GROUP BY cage_code,niin""".replace('PART',str(partition))
        c.execute(('CREATE TEMP TABLE item_roles AS ' if partition==0 else 'INSERT INTO item_roles ')+sql)
    c.execute("""CREATE TEMP TABLE role_totals AS SELECT company_id,count(*)::BIGINT referenced_items,
    count(*) FILTER(WHERE item_identifying)::BIGINT item_identifying_items,
    count(*) FILTER(WHERE design_control_reference)::BIGINT design_control_items,
    count(*) FILTER(WHERE procurement_authorized)::BIGINT procurement_authorized_items,
    count(*) FILTER(WHERE manufacturer_or_design_control)::BIGINT manufacturer_or_design_control_items,
    count(*) FILTER(WHERE distributor_reference)::BIGINT distributor_reference_items,
    count(*) FILTER(WHERE NOT item_identifying AND inactive_or_obsolete_reference)::BIGINT only_nonidentifying_or_inactive_items
    FROM item_roles GROUP BY company_id""")
    # Retain the physical sale once per company; the exact part role join is
    # already unique. The source record remains inspectable in sales evidence.
    c.execute("""CREATE TEMP TABLE aligned_sales AS SELECT s.*,p.market_period_start,p.market_period_end,
    coalesce(r.item_identifying,false) current_item_identifying_part,
    coalesce(r.design_control_reference,false) current_design_control_part
    FROM sales s JOIN periods p USING(company_id)
    LEFT JOIN part_roles r ON r.cage_code=substr(s.company_id,6) AND r.niin=s.niin AND r.part_key=upper(trim(s.part_number))
    WHERE substr(s.transaction_date,1,10)>=substr(p.market_period_start,1,10) AND substr(s.transaction_date,1,10)<=substr(p.market_period_end,1,10)""")
    c.execute("""CREATE TEMP TABLE market AS SELECT company_id,count(*)::BIGINT market_physical_lines,
    count(DISTINCT niin)::BIGINT purchased_referenced_items,count(DISTINCT supplier_cage)::BIGINT market_sellers,
    count(DISTINCT substr(transaction_date,1,7))::BIGINT market_months_observed,
    sum(record_amount_usd) aligned_referenced_item_purchases,
    sum(CASE WHEN NOT direct_seller THEN record_amount_usd ELSE 0 END) aligned_other_seller_item_purchases,
    sum(CASE WHEN NOT direct_seller AND current_item_identifying_part THEN record_amount_usd ELSE 0 END) aligned_other_seller_current_part_purchases,
    sum(CASE WHEN NOT direct_seller AND current_design_control_part THEN record_amount_usd ELSE 0 END) aligned_other_seller_design_part_purchases,
    min(substr(transaction_date,1,10)) market_first_purchase_date,max(substr(transaction_date,1,10)) market_last_purchase_date
    FROM aligned_sales GROUP BY company_id""")
    sql="""SELECT p.company_id,p.market_period_start,p.market_period_end,p.observed_direct_obligations,
    t.* EXCLUDE(company_id),x.* EXCLUDE(company_id),
    CASE WHEN x.aligned_other_seller_current_part_purchases IS NOT NULL THEN greatest(coalesce(p.observed_direct_obligations,0),0)+x.aligned_other_seller_current_part_purchases END direct_and_linked_part_market_reference,
    CASE WHEN x.aligned_other_seller_current_part_purchases IS NOT NULL THEN 3*(greatest(coalesce(p.observed_direct_obligations,0),0)+x.aligned_other_seller_current_part_purchases) END linked_market_coverage_reference,
    'POLICY' product_market_policy,'REFERENCE' source_role_dictionary,
    'Observed direct obligations plus other-seller purchases carrying an exact item-identifying catalogue part, aligned to the same stated period. This is a gross linked-market coverage reference, not manufacturer revenue or expected company pipeline. Direct-seller item purchases are excluded from the addition. Purchases may include alternative sources, existing stock and other company sites. Missing months are not annualized. RNCC 3 is a design-control reference; RNSC K identifies manufacturing or design control, while L identifies a distributor. Neither alone proves all technical/data-rights or notice-specific qualification conditions.' product_market_interpretation
    FROM periods p LEFT JOIN role_totals t USING(company_id) LEFT JOIN market x USING(company_id)""".replace('POLICY',POLICY).replace('REFERENCE',REFERENCE)
    c.execute('CREATE TEMP VIEW result AS '+sql);path=r/'company_product_market_coverage.parquet'
    c.execute('COPY result TO ? (FORMAT PARQUET,COMPRESSION ZSTD)',[str(path)])
    columns=[dict(Name=n,Type={'VARCHAR':'string','BIGINT':'bigint','DOUBLE':'double'}[t]) for n,t,*_ in c.execute('DESCRIBE result').fetchall()]
    rows=c.execute('SELECT count(*) FROM result').fetchone()[0]
    m['tables']['company_product_market_coverage']=dict(path=str(path),rows=rows,columns=columns,sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    m['product_market_policy']=POLICY;(r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close();print('Product market coverage',rows,flush=True)
