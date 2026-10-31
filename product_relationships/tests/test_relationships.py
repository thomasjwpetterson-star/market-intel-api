import json
import unittest
import duckdb
from fastapi import FastAPI,HTTPException
from fastapi.testclient import TestClient
from product_relationships import queries
from product_relationships.api import Repository,create_router

class Relationships(unittest.TestCase):
    def setUp(self):
        self.db=duckdb.connect()
        self.db.execute('CREATE MACRO regexp_like(s,p) AS regexp_matches(s,p); CREATE MACRO json_format(x) AS CAST(x AS VARCHAR); CREATE MACRO array_distinct(x) AS list_distinct(x)')
        self.db.execute('CREATE TABLE refs(niin VARCHAR,cage_code VARCHAR,part_number VARCHAR,normalized_part_number VARCHAR,rncc VARCHAR,rnvc VARCHAR,rnsc VARCHAR,cage_status VARCHAR,ingestion_date DATE,source_file VARCHAR)')
        self.db.execute('CREATE TABLE financial(niin VARCHAR,cage VARCHAR,nsn VARCHAR,item_name VARCHAR,contract_number VARCHAR,award_date DATE)')
        self.db.execute('CREATE TABLE parts(niin VARCHAR,cage VARCHAR,part_number VARCHAR,nsn VARCHAR,item_name VARCHAR,first_observed_date DATE,last_observed_date DATE)')
        self.db.execute('CREATE TABLE registry(cage_code VARCHAR,company_name VARCHAR,role_assessment VARCHAR)')
        # One seller carries selected-company, other-source, ambiguous, own-design, obsolete, and unmatchable lines.
        self.items=['00000000'+str(i) for i in range(1,8)]
        refs=[('000000001','OWN01','P-1','5','2','A'),('000000002','OTH01','P2','3','2','A'),('000000002','OWN01','OTHER-P2','5','2','A'),
              ('000000003','OWN01','P3','5','2','A'),('000000003','OTH01','P3','3','2','A'),
              ('000000004','SELL1','P4','3','2','A'),('000000005','OTH01','P5','3','9','A'),
              # RNCC3 and RNVC2 on separate rows must not create a design reference.
              ('000000006','OTH01','P6','3','1','A'),('000000006','OTH01','P6','5','2','A'),
              ('000000007','OWN01','P-7','5','2','A')]
        self.db.executemany("INSERT INTO refs VALUES (?,?,?,?,?,?,?,?,DATE '2026-08-17','V_FLIS_PART.CSV')",[(i,c,p,p,rc,vc,sc,'A') for i,c,p,rc,vc,sc in refs])
        for i in self.items:
            self.db.execute("INSERT INTO financial VALUES (?,'SELL1',?,'Item','ONE',DATE '2026-01-01')",[i,'2915'+i])
        # Duplicate financial/source rows must not inflate item totals.
        self.db.execute("INSERT INTO financial SELECT * FROM financial LIMIT 1")
        self.db.executemany("INSERT INTO parts VALUES (?,'SELL1',?,NULL,'Item',DATE '2020-01-01',DATE '2026-01-01')",[(self.items[0],'P-1'),(self.items[1],'P2'),(self.items[2],'P3'),(self.items[3],'P4'),(self.items[4],'P5'),(self.items[5],'P6'),(self.items[6],'P7')])
        self.db.execute("INSERT INTO registry VALUES ('SELL1','Generic seller','distribution_activity_indicated')")
    def rows(self,sql):
        for old,new in [(queries.REFERENCES,'refs'),(queries.PARTS,'parts'),('market_intel_platform_candidate.oi_fixture','registry')]:sql=sql.replace(old,new)
        sql=sql.replace('AS ROW(','AS STRUCT(')
        cursor=self.db.execute(sql)
        return [dict(zip([d[0] for d in cursor.description],row)) for row in cursor.fetchall()]
    def test_multibrand_seller_has_multiple_contexts_and_no_blanket_label(self):
        rows=self.rows(queries.portfolio_query('OWN01','SELL1'))
        roles={r['niin']:r['attribution'] for r in rows}
        self.assertEqual([roles[i] for i in self.items],['selected_catalog_match','other_source_reference','overlapping_catalog_sources','seller_design_reference','unattributed','unattributed','unattributed'])
        self.assertEqual(rows[0]['total_observed_items'],7)
    def test_counts_are_full_before_pagination_and_company_scope_is_separate(self):
        row=self.rows(queries.market_query('OWN01','market_intel_platform_candidate.oi_fixture',1))[0]
        self.assertEqual(row['observed_items'],4)
        self.assertEqual(row['total_selected_catalog_items'],1)
        self.assertEqual(row['total_other_source_items'],1)
        self.assertEqual(row['total_ambiguous_items'],1)
        portfolio=self.rows(queries.portfolio_query('OWN01','SELL1',1))[0]
        self.assertEqual(portfolio['total_observed_items'],7)
        self.assertEqual(portfolio['total_unattributed_items'],3)
    def test_shared_own_design_and_selected_reference_stay_ambiguous(self):
        self.db.execute("INSERT INTO refs VALUES ('000000004','OWN01','P4','P4','5','2','A','A',DATE '2026-08-17','V_FLIS_PART.CSV')")
        row=[r for r in self.rows(queries.portfolio_query('OWN01','SELL1')) if r['niin']=='000000004'][0]
        self.assertEqual(row['attribution'],'overlapping_catalog_sources')
    def test_large_company_scope_does_not_exceed_query_size_limit(self):
        sql=queries.market_query('OWN01','market_intel_platform_candidate.oi_fixture',limit=100)
        self.assertLess(len(sql.encode()),20000)
    def test_inactive_cage_is_not_an_active_source_match(self):
        self.db.execute("UPDATE refs SET cage_status='D' WHERE niin='000000001'")
        row=[r for r in self.rows(queries.portfolio_query('OWN01','SELL1')) if r['niin']=='000000001'][0]
        self.assertEqual(row['attribution'],'unattributed')
    def test_exact_rows_retain_conflicts_without_cartesian_codes(self):
        row=[r for r in self.rows(queries.portfolio_query('OWN01','SELL1')) if r['niin']=='000000006'][0]
        evidence=json.loads(row['reference_evidence_json'])
        self.assertEqual({(r['rncc'],r['rnvc']) for r in evidence},{('3','1'),('5','2')})
    def test_validation_and_auth_precede_any_source_read(self):
        for value in ("OWN01' OR 1=1",'lower','123456'):
            with self.assertRaises(ValueError): queries.cage(value)
        with self.assertRaises(ValueError):queries.market_query('OWN01','bad; DROP')
        class Never:
            def context(*args): raise AssertionError('Read before auth')
        def denied():raise HTTPException(401,'Sign in')
        app=FastAPI();app.include_router(create_router(Never(),denied))
        self.assertEqual(TestClient(app).get('/product-intelligence/companies/OWN01/market').status_code,401)
    def test_reader_handles_numbers_evidence_and_unavailable_separately(self):
        parent=self
        class Source:
            def table(self,name):return 'market_intel_platform_candidate.oi_fixture'
            def rows(self,sql):return parent.rows(sql)
        result=Repository(Source()).context('OWN01','SELL1',1,0)
        self.assertEqual(result['totals']['observed_items'],7)
        self.assertTrue(result['has_more']);self.assertEqual(len(result['records']),1)
        self.assertIn('source_references',result['records'][0])
        class Broken:
            def context(*args):raise RuntimeError('private-infrastructure-error')
        app=FastAPI();app.include_router(create_router(Broken(),lambda:{'id':'verified'}))
        response=TestClient(app).get('/product-intelligence/companies/OWN01/market')
        self.assertEqual(response.status_code,503)
        self.assertNotIn('private-infrastructure-error',response.text)
        self.assertEqual(response.headers['cache-control'],'private, no-store')

if __name__=='__main__':unittest.main()
