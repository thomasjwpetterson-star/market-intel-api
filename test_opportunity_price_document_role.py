import ast,importlib.util,re,unittest
from pathlib import Path
try:import duckdb
except ImportError:duckdb=None
ROOT=Path(__file__).resolve().parent
P=ROOT/'opportunity_worker/opportunity_engine'
spec=importlib.util.spec_from_file_location('price_evidence',P/'price_evidence.py');policy=importlib.util.module_from_spec(spec);spec.loader.exec_module(policy)

@unittest.skipUnless(duckdb,'DuckDB required for real SQL predicate tests')
class OpportunityPriceTests(unittest.TestCase):
 def setUp(self):
  self.c=duckdb.connect();self.c.execute('CREATE MACRO regexp_like(s,p) AS regexp_matches(s,p)')
  self.c.execute('CREATE TABLE links(episode_id VARCHAR,pricing_record_id VARCHAR,include_in_mimir_range BOOLEAN,promotion_eligible_without_review BOOLEAN,quality_status VARCHAR,attachment_sha256 VARCHAR,attachment_s3_uri VARCHAR,attachment_source_row_text VARCHAR,unit_price_usd DOUBLE,quantity DOUBLE)')
  sha='84075394258b3f88d553c39b5950b58e045113c016aaaf4f270b5005f268a967'
  self.c.executemany('INSERT INTO links VALUES (?,?,?,?,?,?,?,?,?,?)',[
   ('a'*64,'a-bad',True,True,'AUTO',sha,'renamed.pdf','1 EA 48.00 48.00',48,1),
   ('a'*64,'b-good',True,True,'AUTO','good','Award.pdf','0001 VALVE',62345,2),
   ('a'*64,'c-held',False,False,'EXCLUDED_NON_PROCUREMENT_DOCUMENT','other','hold.pdf','0001',4,1),
   ('a'*64,'d-new-inventory',True,True,'AUTO','new','Government_Furnished_Property_List.pdf','1 EA 5 5',5,1)])
 def tearDown(self):self.c.close()
 def test_actual_repository_filters_before_limit(self):
  for path in [P/'api.py',ROOT/'opportunity_intelligence/api.py']:
   with self.subTest(path=path):
    tree=ast.parse(path.read_text());klass=next(n for n in tree.body if isinstance(n,ast.ClassDef)and n.name=='Repository');fn=next(n for n in klass.body if isinstance(n,ast.FunctionDef)and n.name=='price_lines')
    ns={'re':re,'procurement_price_sql':policy.procurement_price_sql};exec(compile(ast.Module(body=[fn],type_ignores=[]),'<reader>','exec'),ns)
    fake=type('Reader',(),{'price_lines':ns['price_lines'],'table':lambda self,name:'links','rows':lambda _,sql:self.c.execute(sql).fetchall()})()
    rows=fake.price_lines('a'*64);self.assertEqual([r[1]for r in rows],['b-good'])
 def test_future_join_filters_existing_flags_and_legacy_inventory(self):
  self.assertEqual(self.c.execute('SELECT count(*) FROM links g WHERE '+policy.procurement_price_sql('g',dialect='duckdb')).fetchone()[0],1)
  code=(P/'enrich.py').read_text();self.assertIn("procurement_price_sql('g',dialect='duckdb')",code);self.assertIn('price_document_role_logic_sha256',code)
 def test_policy_copies_cannot_drift(self):
  def node(path):return next(n for n in ast.parse(path.read_text()).body if isinstance(n,ast.FunctionDef)and n.name=='non_procurement_document_class')
  self.assertEqual(ast.dump(node(P/'price_evidence.py'),include_attributes=False),ast.dump(node(ROOT/'pricing_pipeline/glue_sam_governed_unit_pricing.py'),include_attributes=False))

 def test_actual_enricher_retypes_source_and_omits_inventory_notice_links(self):
  import tempfile,json,hashlib,sys
  sys.path.insert(0,str(ROOT/'opportunity_worker'))
  from opportunity_engine.enrich import enrich
  with tempfile.TemporaryDirectory() as folder:
   root=Path(folder)
   def write(name,sql):
    f=root/(name+'.parquet');self.c.execute("COPY ("+sql+") TO '"+str(f)+"' (FORMAT PARQUET)");return f
   gov=write('source_prices',"SELECT *,pricing_record_id physical_source_line_key,'notice' notice_id,'0001' clin,'VALVE' priced_item_name,'EA' quantity_unit,'SOURCE' price_basis,unit_price_usd*quantity line_amount_usd,'PROCUREMENT_EQUIPMENT_LINE' item_class,attachment_s3_uri attachment_name,'page=1' attachment_source_locator,NULL::VARCHAR action_date FROM links")
   calls=write('published_calls',"SELECT '"+'a'*64+"' episode_id,'record' record_hash,'notice' source_locator")
   force=write('force_plans',"SELECT ''::VARCHAR force_row_id,''::VARCHAR platform_source_label WHERE false")
   programme=write('programme_funding',"SELECT ''::VARCHAR funding_id,''::VARCHAR budget_line_item_title,''::VARCHAR description_excerpt WHERE false")
   (root/'manifest.json').write_text(json.dumps({'tables':{'published_calls':{'path':str(calls)},'force_plans':{'path':str(force)},'programme_funding':{'path':str(programme)}},'sources':{}}))
   (root/'sources.json').write_text(json.dumps({'sources':{'governed_price_lines':{'files':[{'path':str(gov),'sha256':hashlib.sha256(gov.read_bytes()).hexdigest()}]}}}))
   result=enrich(root,root/'sources.json')
   links=self.c.read_parquet(result['tables']['notice_price_links']['path']).fetchall()
   self.assertEqual(len(links),1)
   self.assertEqual(self.c.execute("SELECT count(*) FROM read_parquet('"+result['tables']['governed_price_lines']['path']+"') WHERE pricing_record_id IN ('a-bad','d-new-inventory') AND NOT include_in_mimir_range AND clin IS NULL").fetchone()[0],2)

 def test_alias_is_internal_validated_input(self):
  with self.assertRaises(ValueError):policy.procurement_price_sql('g; DROP TABLE links')

if __name__=='__main__':unittest.main()
