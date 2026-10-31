"""Exact demand-item prices are independent of a candidate company's eligibility."""
import hashlib,json
from pathlib import Path
import duckdb

VERSION='global-exact-notice-reference-v3'
INTERPRETATION='Exact notice NIIN across held catalogue references, independent of candidate company. Government reference dates are not retained. A single supplier historical average is shown only where one supplier has one unambiguous price. These are price references, not current quotes or evidence of candidate qualification; engineering costs and company workshare are excluded.'

def build(c,output,as_of,matches):
 from .notice_quantity import quantity_fact
 c.create_function('notice_quantity_json',lambda title,scope:json.dumps(quantity_fact(dict(source_name='sam_notices',title=title,scope_text=scope))),['VARCHAR']*2,'VARCHAR',null_handling='special')
 from .delivery_modes import required_mode
 c.create_function('notice_purchase_price_allowed',lambda title,body,psc: required_mode(title,body,psc,True)[0] not in ('maintenance_repair','onsite_services','equipment_installation','facility_construction','research_development'),['VARCHAR']*3,'BOOLEAN',null_handling='special')
 psc='o.psc' if any(x[0]=='psc' for x in c.execute('DESCRIBE base_opportunities').fetchall()) else 'NULL::VARCHAR'
 c.execute("""CREATE TEMP TABLE notice_item_keys AS SELECT DISTINCT o.episode_id,o.record_hash,n.nsn,right(n.nsn,9) niin,notice_quantity_json(o.title,o.scope_text) quantity_facts_json,notice_purchase_price_allowed(o.title,o.scope_text,PSC_EXPRESSION) purchase_price_allowed
 FROM base_opportunities o JOIN base_demand_decisions d USING(episode_id),unnest(scope_nsns(concat_ws(' ',o.title,o.scope_text))) n(nsn)
 WHERE o.lane='published_procurement' AND d.stage IN ('qualification_opening','published_requirement')""".replace('PSC_EXPRESSION',psc))
 c.execute("""CREATE TEMP TABLE notice_item_references AS SELECT DISTINCT k.episode_id,k.record_hash,k.nsn,i.*
 FROM notice_item_keys k JOIN item_reference_facts i ON i.niin=k.niin""")
 c.execute("""CREATE TEMP TABLE notice_item_prices AS SELECT episode_id,record_hash,nsn,
 CASE WHEN count(DISTINCT nullif(trim(unit_of_issue),''))=1 AND count_if((govt_estimated_price IS NOT NULL OR supplier_average_price IS NOT NULL) AND nullif(trim(unit_of_issue),'') IS NULL)=0 THEN min(nullif(trim(unit_of_issue),'')) END catalogue_unit,
 CASE WHEN count(DISTINCT nullif(trim(unit_of_issue),''))=1 AND count_if(govt_estimated_price IS NOT NULL AND nullif(trim(unit_of_issue),'') IS NULL)=0 AND count(DISTINCT govt_estimated_price)=1 THEN min(govt_estimated_price)::DOUBLE END government_reference_unit_price,
 CASE WHEN count(DISTINCT nullif(trim(unit_of_issue),''))=1 AND count_if(supplier_average_price IS NOT NULL AND nullif(trim(unit_of_issue),'') IS NULL)=0 AND count(DISTINCT CASE WHEN supplier_average_price IS NOT NULL THEN cage_code END)=1 AND count(DISTINCT supplier_average_price)=1 THEN min(supplier_average_price)::DOUBLE END supplier_average_unit_price,
 max(last_sold_date) FILTER(WHERE supplier_average_price IS NOT NULL) supplier_average_last_sale,count(*) reference_rows,
 to_json(list(DISTINCT struct_pack(evidence_id:=evidence_id,reference_cage:=cage_code,part_number:=part_number,description:=description,source_snapshot_sha256:=source_snapshot_sha256,source_row:=source_row,government_reference_price:=govt_estimated_price,supplier_average_price:=supplier_average_price,unit:=unit_of_issue,last_sale:=last_sold_date))) catalogue_evidence_json
 FROM notice_item_references GROUP BY episode_id,record_hash,nsn""")
 c.execute("""CREATE TEMP TABLE notice_item_lots AS SELECT p.*,k.quantity_facts_json,try_cast(json_extract_string(k.quantity_facts_json,'$.notice_aggregate_ceiling_usd') AS DOUBLE) source_aggregate_ceiling_usd,
 CASE WHEN k.purchase_price_allowed AND json_extract_string(k.quantity_facts_json,'$.notice_quantity_nsn')=p.nsn AND json_extract_string(k.quantity_facts_json,'$.notice_quantity_unit')=p.catalogue_unit THEN try_cast(json_extract_string(k.quantity_facts_json,'$.notice_quantity') AS BIGINT) END source_quantity,
 CASE WHEN k.purchase_price_allowed AND json_extract_string(k.quantity_facts_json,'$.notice_quantity_nsn')=p.nsn AND json_extract_string(k.quantity_facts_json,'$.notice_quantity_unit')=p.catalogue_unit THEN json_extract_string(k.quantity_facts_json,'$.notice_quantity_evidence') END source_quantity_quote
 FROM notice_item_prices p JOIN notice_item_keys k USING(episode_id,record_hash,nsn)""")
 output('company_notice_item_prices',"""SELECT cm.company_id,p.episode_id,p.record_hash,to_json(list(struct_pack(nsn:=p.nsn,price_scope:='global_exact_niin',catalogue_unit:=p.catalogue_unit,source_quantity:=p.source_quantity,source_quantity_quote:=p.source_quantity_quote,source_aggregate_ceiling_usd:=p.source_aggregate_ceiling_usd,government_reference_lot_scenario:=CASE WHEN coalesce(p.source_aggregate_ceiling_usd,0)<=0 OR p.source_quantity*p.government_reference_unit_price<=p.source_aggregate_ceiling_usd THEN round(p.source_quantity*p.government_reference_unit_price,2) END,supplier_average_lot_scenario:=CASE WHEN coalesce(p.source_aggregate_ceiling_usd,0)<=0 OR p.source_quantity*p.supplier_average_unit_price<=p.source_aggregate_ceiling_usd THEN round(p.source_quantity*p.supplier_average_unit_price,2) END,government_reference_unit_price:=p.government_reference_unit_price,supplier_average_unit_price:=p.supplier_average_unit_price,supplier_average_last_sale:=p.supplier_average_last_sale,reference_rows:=p.reference_rows,catalogue_evidence:=json(p.catalogue_evidence_json)) ORDER BY p.nsn)) items_json,
 '"""+INTERPRETATION.replace("'","''")+"""' interpretation
 FROM notice_item_lots p JOIN (SELECT DISTINCT company_id,episode_id,opportunity_hash FROM """+matches+""") cm ON cm.episode_id=p.episode_id AND cm.opportunity_hash=p.record_hash GROUP BY cm.company_id,p.episode_id,p.record_hash""")
 for n in ('notice_item_keys','notice_item_references','notice_item_prices','notice_item_lots'):c.execute('DROP TABLE '+n)

def refresh(directory):
 """Reprice the current release without redoing retrieval or changing any fit edge."""
 from .company_enrichment import nsns
 r=Path(directory);m=json.loads((r/'manifest.json').read_text());source=m['sources'].get('item_reference_facts')
 if not source:return
 c=duckdb.connect();c.execute("SET threads=4;SET memory_limit='6GB';SET preserve_insertion_order=false")
 for name in ('opportunities','demand_decisions','company_matches'):c.read_parquet(m['tables'][name]['path']).create_view('base_'+name)
 c.create_function('scope_nsns',nsns,['VARCHAR'],duckdb.list_type('VARCHAR'))
 c.read_parquet([f['path'] for f in source['files']]).create_view('raw_item_reference_facts')
 c.execute("CREATE TEMP VIEW item_reference_facts AS SELECT * REPLACE(CASE WHEN govt_estimated_price>0 THEN govt_estimated_price END AS govt_estimated_price,CASE WHEN supplier_average_price>0 AND (last_sold_date IS NULL OR substr(cast(last_sold_date AS VARCHAR),1,10)<='"+m['as_of']+"') THEN supplier_average_price END AS supplier_average_price) FROM raw_item_reference_facts")
 def output(name,sql):
  c.execute('CREATE OR REPLACE TEMP VIEW notice_price_result AS '+sql);p=r/(name+'.parquet');tmp=r/(name+'.repricing.parquet');c.execute("COPY notice_price_result TO '"+str(tmp).replace("'","''")+"' (FORMAT PARQUET,COMPRESSION ZSTD)");tmp.replace(p)
  columns=[dict(Name=n,Type='string') for n,*_ in c.execute('DESCRIBE notice_price_result').fetchall()]
  count=c.execute('SELECT count(*) FROM read_parquet(?)',[str(p)]).fetchone()[0]
  m['tables'][name]=dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest(),rows=count,columns=columns);print('Repriced exact notice items',count,flush=True)
 build(c,output,m['as_of'],'base_company_matches');c.close();m['notice_item_price_policy']=VERSION;(r/'manifest.json').write_text(json.dumps(m,indent=2)+'\n')
