"""Source CSV and real serving SQL regressions for opportunity code identity."""
import ast
import io
import os
import tempfile
import unittest
from pathlib import Path
from typing import List, Optional
from datetime import date, datetime, timezone
from unittest.mock import Mock

import duckdb
import pandas as pd
from fastapi import Query
from opportunity_identifiers import normalize_opportunity_naics, opportunity_naics_sql, opportunity_view_sql, validate_opportunity_naics_codes
from solicitation_deadlines import active_solicitation_deadline_sql

ROOT=Path(__file__).resolve().parent


class OpportunityIdentifierTests(unittest.TestCase):
    def test_actual_csv_reader_preserves_nullable_codes_and_signed_values(self):
        node=next(n for n in ast.parse((ROOT/'run_etl.py').read_text()).body
                  if isinstance(n,ast.FunctionDef) and n.name=='run_query')
        csv='id,naics,amount\nnegative,541611,-12.34\nzero,,0\nshort,56121,7.50\nidentifier,011111,2.25\n'
        # Reproduce the original nullable numeric inference independently.
        old=pd.read_csv(io.StringIO(csv),low_memory=False)
        self.assertEqual(str(old['naics'].dtype),'float64')
        with tempfile.TemporaryDirectory() as folder:
            downloader=Mock()
            downloader.download_file.side_effect=lambda bucket,key,path:Path(path).write_text(csv)
            ns=dict(pd=pd,os=os,TEMP_DIR=folder,BUCKET_NAME='fixture',s3=downloader,
                    execute_athena_query=lambda query:('test-query',{'QueryExecution':{'ResultConfiguration':{'OutputLocation':'s3://fixture/query.csv'}}}))
            exec(compile(ast.Module(body=[node],type_ignores=[]),'run_etl.py','exec'),ns)
            actual=ns['run_query']('SELECT fixture')
        self.assertEqual(actual['naics'].tolist(),['541611',pd.NA,'56121','011111'])
        pd.testing.assert_series_equal(actual['amount'],old['amount'])
        pd.testing.assert_series_equal(actual['id'],old['id'])

    def test_only_storage_suffix_is_corrected_no_code_is_inferred(self):
        for before,expected in [('541611.0','541611'),(541611.0,'541611'),('56121.00','56121'),
                                ('011111','011111'),('31-33','31-33'),('541611.1','541611.1'),
                                ('1234567.0','1234567.0'),('', ''),(None,None),(float('nan'),None),(pd.NA,None)]:
            with self.subTest(value=before):self.assertEqual(normalize_opportunity_naics(before),expected)

    def test_producer_guard_holds_invalid_codes_without_inventing_replacements(self):
        validate_opportunity_naics_codes(['541611','56121','011111',None,pd.NA,''])
        for value in ['541611.1','5.41611e5','1234567','31-33',541611.0]:
            with self.subTest(value=value),self.assertRaises(ValueError):
                validate_opportunity_naics_codes([value])

    def test_actual_pipeline_filters_legacy_double_cache_after_adapter(self):
        c=duckdb.connect()
        self.addCleanup(c.close)
        with tempfile.TemporaryDirectory() as folder:
            path=str(Path(folder)/'legacy.parquet')
            c.execute("COPY (SELECT 'native' id,'BID' sol_num,'Fixture' title,'Army' agency,'Army office' sub_agency,'2099-01-01T00:00:00Z' deadline,NULL::VARCHAR set_aside_type,541611.0::DOUBLE naics,'R408' psc,'Description' description,NULL::VARCHAR poc_email,'SAM' source_system,'GA' state,'https://sam.gov/' url,'FIXTURE' search_text UNION ALL SELECT 'expired','OLD','Old','Army','Army office','2000-01-01T00:00:00Z',NULL,541611.0,'R408','Old',NULL,'SAM','GA','https://sam.gov/','OLD') TO '"+path+"' (FORMAT PARQUET)")
            c.execute("CREATE VIEW v_opportunities AS SELECT * FROM read_parquet('"+path+"')")
            with self.assertRaises(duckdb.BinderException):c.execute("SELECT id FROM v_opportunities WHERE naics LIKE '%541611%'")
            c.execute(opportunity_view_sql(path))
            node=next(n for n in ast.parse((ROOT/'main.py').read_text()).body
                      if isinstance(n,ast.FunctionDef) and n.name=='get_pipeline_live')
            node.decorator_list=[]
            ns=dict(Optional=Optional,List=List,Query=Query,date=date,datetime=datetime,
                    active_solicitation_deadline_sql=active_solicitation_deadline_sql,sanitize=lambda s:s,
                    duck_fetch_df=lambda sql,params:c.execute(sql,params).fetchdf(),df_sanitize_for_json=lambda df:df)
            exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),ns)
            actual=ns['get_pipeline_live'](naics='541611',limit=1)
            self.assertEqual([r['id'] for r in actual],['native'])
            self.assertEqual(actual[0]['naics'],'541611')
            self.assertEqual(actual[0]['total_matches'],1)

    def test_history_projection_keeps_every_other_field_and_native_row(self):
        c=duckdb.connect();self.addCleanup(c.close)
        c.execute("CREATE TABLE original AS SELECT 'historical' opportunity_id,'56121.0' naics,'2000-01-01T00:00:00Z' deadline,'untouched' description UNION ALL SELECT 'missing',NULL,'2099-01-01T00:00:00Z','second'")
        c.execute('CREATE TABLE public_solicitation_profile_next AS SELECT * FROM original')
        c.execute('UPDATE public_solicitation_profile_next SET naics = '+opportunity_naics_sql())
        self.assertEqual(c.execute('SELECT opportunity_id,naics FROM public_solicitation_profile_next ORDER BY 1').fetchall(),[('historical','56121'),('missing',None)])
        self.assertEqual(c.execute('SELECT * EXCLUDE(naics) FROM original EXCEPT ALL SELECT * EXCLUDE(naics) FROM public_solicitation_profile_next').fetchall(),[])

    def test_guard_is_wired_to_actual_ingest_binding_and_historical_reader(self):
        main=ast.parse((ROOT/'main.py').read_text())
        by_name={n.name:n for n in main.body if isinstance(n,ast.FunctionDef)}
        names=lambda n:{x.func.id for x in ast.walk(n) if isinstance(x,ast.Call) and isinstance(x.func,ast.Name)}
        self.assertIn('opportunity_view_sql',names(by_name['reload_all_data']))
        self.assertIn('opportunity_naics_sql',names(by_name['build_public_intelligence_release']))
        self.assertIn('normalize_opportunity_naics',names(by_name['get_public_solicitation_page_snapshot']))
        etl=ast.parse((ROOT/'run_etl.py').read_text())
        producer=next(n for n in etl.body if isinstance(n,ast.FunctionDef) and n.name=='optimize_and_upload')
        self.assertIn('validate_opportunity_naics_codes',names(producer))
        self.assertTrue(any(isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)
                            and n.func.attr=='map' and n.args and isinstance(n.args[0],ast.Name)
                            and n.args[0].id=='normalize_opportunity_naics' for n in ast.walk(producer)))

    def test_sql_names_are_internal_and_paths_are_escaped(self):
        with self.assertRaises(ValueError):opportunity_naics_sql('naics; DELETE FROM source')
        self.assertIn("source''s.parquet",opportunity_view_sql("source's.parquet"))


if __name__=='__main__':unittest.main()
