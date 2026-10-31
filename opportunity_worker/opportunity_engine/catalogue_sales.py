"""Observed product-market sales, kept separate from producer revenue and pipeline."""
import hashlib,json
from pathlib import Path
import duckdb

def augment(directory):
 r=Path(directory);m=json.loads((r/'manifest.json').read_text())
 if 'item_market_price_evidence' not in m['tables'] or 'item_reference_facts' not in m['sources']:return
 c=duckdb.connect();c.execute("SET threads=2;SET memory_limit='3GB';SET preserve_insertion_order=false")
 c.execute("SET temp_directory='"+str(r/'catalogue_sales_tmp').replace("'","''")+"'")
 c.read_parquet(m['tables']['item_market_price_evidence']['path']).create_view('prices')
 c.read_parquet([f['path'] for f in m['sources']['item_reference_facts']['files']]).create_view('refs')
 c.execute("CREATE TEMP TABLE priced AS SELECT * FROM prices WHERE record_amount_usd>0 AND try_cast(substr(transaction_date,1,10) AS DATE)>DATE '"+m['as_of']+"'-INTERVAL 1 YEAR AND try_cast(substr(transaction_date,1,10) AS DATE)<=DATE '"+m['as_of']+"'")
 c.execute('CREATE TEMP TABLE priced_refs AS SELECT r.* FROM refs r SEMI JOIN (SELECT DISTINCT niin FROM priced) p USING(niin)')
 c.execute("""CREATE TEMP TABLE parts AS SELECT DISTINCT cage_code,niin,upper(trim(part_number)) part_number FROM priced_refs WHERE regexp_full_match(cage_code,'[A-Z0-9]{5}') AND nullif(trim(part_number),'') IS NOT NULL""")
 c.execute('CREATE TEMP TABLE part_counts AS SELECT niin,part_number,count(*) reference_cages FROM parts GROUP BY niin,part_number')
 c.execute('CREATE TEMP TABLE catalog AS SELECT cage_code,niin,count(*) reference_rows,min(evidence_id) sample_reference_evidence,min(source_snapshot_sha256) source_snapshot_sha256 FROM priced_refs WHERE regexp_full_match(cage_code,\'[A-Z0-9]{5}\') GROUP BY cage_code,niin')
 def emit(n,sql):
  c.execute('CREATE OR REPLACE TEMP VIEW result AS '+sql);p=r/(n+'.parquet');types={'VARCHAR':'string','DOUBLE':'double','BIGINT':'bigint','BOOLEAN':'boolean'};cols=[dict(Name=k,Type=types[t]) for k,t,*_ in c.execute('DESCRIBE result').fetchall()]
  c.execute("COPY result TO '"+str(p).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)");count=c.execute('SELECT count(*) FROM read_parquet(?)',[str(p)]).fetchone()[0]
  m['tables'][n]=dict(path=str(p),rows=count,sha256=hashlib.sha256(p.read_bytes()).hexdigest(),columns=cols);c.read_parquet(str(p)).create_view(n);print('Catalogue sales',n,count,flush=True)
 emit('catalogue_sales_evidence',"""SELECT 'CAGE:'||c.cage_code company_id,p.pricing_record_id,p.source_record_id,p.niin,p.nsn,p.part_number,p.item_name,p.supplier_cage,p.supplier_name,p.transaction_date,p.record_amount_usd,p.quantity,p.quantity_unit,p.unit_price_usd,
 c.cage_code=p.supplier_cage direct_seller,ep.cage_code IS NOT NULL exact_part_reference,pc.reference_cages exact_part_reference_cages,
 c.sample_reference_evidence,c.source_snapshot_sha256,p.source_url,p.source_s3_uri,p.source_row_locator,p.source_sha256
 FROM catalog c JOIN priced p USING(niin) LEFT JOIN parts ep ON ep.niin=p.niin AND ep.cage_code=c.cage_code AND ep.part_number=upper(trim(p.part_number)) LEFT JOIN part_counts pc ON pc.niin=p.niin AND pc.part_number=upper(trim(p.part_number))""")
 emit('company_catalogue_sales',"""SELECT company_id,count(*) physical_price_lines,count(DISTINCT niin) observed_catalogue_items,count(DISTINCT supplier_cage) observed_sellers,
 sum(record_amount_usd) observed_catalogue_item_sales,sum(CASE WHEN direct_seller THEN record_amount_usd ELSE 0 END) direct_seller_sales,
 sum(CASE WHEN NOT direct_seller THEN record_amount_usd ELSE 0 END) other_seller_item_sales,
 sum(CASE WHEN NOT direct_seller AND exact_part_reference THEN record_amount_usd ELSE 0 END) exact_part_other_seller_sales,
 sum(CASE WHEN NOT direct_seller AND exact_part_reference AND exact_part_reference_cages=1 THEN record_amount_usd ELSE 0 END) single_catalogue_part_other_seller_sales,
 min(substr(transaction_date,1,10)) first_sale_date,max(substr(transaction_date,1,10)) last_sale_date,
 'Trailing-year held dated procurement lines, deduplicated by physical source ID. Same NIIN is market context; exact part reference is stronger product context. Even a single catalogue CAGE does not prove the delivered manufacturer. Seller amounts are government purchases, not manufacturer revenue, and are not added to future opportunities or USAspending obligations.' interpretation
 FROM catalogue_sales_evidence GROUP BY company_id""")
 m['catalogue_sales_policy']='physical-source-niin-and-exact-part-v1';(r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close()
