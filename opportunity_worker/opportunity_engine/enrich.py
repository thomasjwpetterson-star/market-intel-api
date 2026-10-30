"""Append native commercial evidence and exact notice joins to a release."""
import hashlib,json
from pathlib import Path
import duckdb

def enrich(directory,commercial_manifest=None):
 r=Path(directory);m=json.loads((r/'manifest.json').read_text());c=duckdb.connect();c.execute("SET threads=1;SET memory_limit='1200MB'")
 def output(name,sql):
  p=r/(name+'.parquet');c.execute('CREATE OR REPLACE TABLE result AS '+sql)
  for col,typ,*_ in c.execute('DESCRIBE result').fetchall():
   if typ not in ('VARCHAR','BIGINT','INTEGER','DOUBLE','FLOAT','BOOLEAN'):c.execute('ALTER TABLE result ALTER COLUMN "'+col+'" TYPE VARCHAR')
  c.execute("COPY result TO '"+str(p).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)");cols=[dict(Name=n,Type='bigint' if t in ('BIGINT','INTEGER') else 'double' if t in ('DOUBLE','FLOAT') else 'boolean' if t=='BOOLEAN' else 'string') for n,t,*_ in c.execute('DESCRIBE result').fetchall()];m['tables'][name]=dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),rows=c.execute('select count(*) from result').fetchone()[0],columns=cols)
 if commercial_manifest:
  extra={n:s for n,s in json.loads(Path(commercial_manifest).read_text())['sources'].items() if n in ('governed_price_lines','dla_forecasts','dla_inventory')}
  for name,source in extra.items():
   for f in source['files']:
    if hashlib.sha256(Path(f['path']).read_bytes()).hexdigest()!=f['sha256']:raise ValueError('Source hash mismatch')
   paths=[f['path'] for f in source['files']];c.read_parquet(paths).create_view(name);output(name,'SELECT * FROM '+name);m['sources'][name]=source
  c.read_parquet(m['tables']['published_calls']['path']).create_view('published_calls')
  output('notice_price_links',"""SELECT p.episode_id,p.record_hash notice_record_hash,g.pricing_record_id,g.physical_source_line_key,g.notice_id,g.clin,g.priced_item_name,g.quantity,g.quantity_unit,g.unit_price_usd,g.price_basis,g.line_amount_usd,g.quality_status,g.include_in_mimir_range,g.promotion_eligible_without_review,g.attachment_s3_uri,g.attachment_sha256,g.attachment_source_locator,g.attachment_source_row_text,g.action_date,
 'Exact notice ID; historical line evidence, not a current requirement total. Verify units, configuration, quantity scope and observation date.' interpretation
 FROM published_calls p JOIN governed_price_lines g ON p.source_locator=g.notice_id""")
 # Avoid F-1/F-15 or C-1/C-130 prefix collisions in platform context.
 c.read_parquet(m['tables']['force_plans']['path']).create_view('force_plans');c.read_parquet(m['tables']['programme_funding']['path']).create_view('programme_funding')
 output('force_programme_links',"""WITH f AS(SELECT force_row_id,platform_source_label,unnest(regexp_extract_all(upper(platform_source_label),'(?:AH|UH|HH|CH|MH|SH|KC|RC|EC|MC|AC|WC|HC|C|F|B|T|P|E|V)[ -]?[0-9]{1,3}[A-Z]{0,2}')) variant FROM force_plans)
 SELECT DISTINCT f.force_row_id,p.funding_id,f.platform_source_label,f.variant,'Bounded variant-text association; not an external work package or funding allocation' relationship FROM f JOIN programme_funding p ON regexp_matches(upper(concat_ws(' ',p.budget_line_item_title,p.description_excerpt)),'(^|[^A-Z0-9])'||regexp_replace(f.variant,'[- ]','[ -]?','g')||'([^A-Z0-9]|$)')""")
 m['enrichment_logic_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest();(r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n');c.close();return m
