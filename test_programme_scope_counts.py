import ast
import re
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, List, Optional
from unittest.mock import Mock

import duckdb
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.testclient import TestClient


def namespace():
    wanted = {'programme_award_record_count', 'get_platform_profile', 'get_platform_parts',
              'build_contract_awards_explorer_query', 'build_contract_awards_explorer_query_from_rollup',
              '_rolled_contract_years', 'quote_ident', 'safe_int'}
    nodes = []
    for node in ast.parse(Path(__file__).with_name('main.py').read_text()).body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            node.decorator_list = []
            nodes.append(node)
    ns = dict(pd=pd, re=re, Any=Any, List=List, Optional=Optional, Query=Query,
              HTTPException=HTTPException, ExplorerRequest=lambda **kw: SimpleNamespace(columns=[], **kw),
              CONTRACT_AWARD_EXPLORER_TABLE='v_contract_awards_enriched',
              EXPLORER_DEFAULT_COLUMNS={'v_contract_awards_enriched':['contract_id']},
              CONTRACT_AWARD_SYNTHETIC_COLUMNS={'contract_id'})
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'main.py', 'exec'), ns)
    return ns


class ProgrammeScopeCounts(unittest.TestCase):
    def setUp(self):
        self.ns = namespace()
        self.db = duckdb.connect()
        self.addCleanup(self.db.close)
        self.db.execute('''CREATE TABLE v_contracts_rolled(
            contract_id VARCHAR, platform_family VARCHAR, sub_agency VARCHAR, market_segment VARCHAR,
            obligations_fy2024 DOUBLE, obligations_fy2025 DOUBLE,
            action_count_fy2024 INTEGER, action_count_fy2025 INTEGER)''')
        self.db.execute("""INSERT INTO v_contracts_rolled VALUES
            ('A','AMRAAM','NAVY','AIR',100,200,1,1),
            ('B','AMRAAM','AIR FORCE','AIR',0,0,0,1),
            ('C','F-35','NAVY','AIR',0,30,0,1),
            ('D','AMRAAM','NAVY','OTHER',0,50,0,1)""")
        self.ns['get_duck_table_columns'] = lambda table: {r[0] for r in self.db.execute('DESCRIBE '+table).fetchall()}
        self.ns['duck_fetch_df'] = lambda sql, params: self.db.execute(sql, params).fetchdf()

    def test_award_records_are_counted_once_across_years(self):
        self.assertEqual(self.ns['programme_award_record_count']('AMRAAM', [2024,2025]), 3)

    def test_buyer_year_and_domain_scope_are_preserved(self):
        count = self.ns['programme_award_record_count']
        self.assertEqual(count('AMRAAM', [2025], 'NAVY'), 2)
        self.assertEqual(count('AMRAAM', [2025], 'NAVY', 'AIR'), 1)
        self.assertEqual(count('AMRAAM', [2024]), 1)
        self.assertEqual(count('AMRAAM', [2027]), 0)

    def test_zero_obligation_action_still_counts_as_an_award(self):
        self.assertEqual(self.ns['programme_award_record_count']('AMRAAM', [2025], 'AIR FORCE'), 1)

    def test_missing_scope_column_fails_instead_of_ignoring_filter(self):
        self.ns['get_duck_table_columns'] = lambda table: {'platform_family'}
        with self.assertRaises(HTTPException) as error:
            self.ns['programme_award_record_count']('AMRAAM', agency='NAVY')
        self.assertEqual(error.exception.status_code, 503)

    def test_profile_separates_award_summary_and_supplier_grains(self):
        self.ns['build_summary_where'] = lambda years, filters: ('1=1', [])
        self.ns['query_summary_df'] = lambda **kw: pd.DataFrame([
            dict(platform_family='AMRAAM',vendor_name='SAME NAME',cage_code='11111',sub_agency='NAVY',total_spend=100,contract_count=7),
            dict(platform_family='AMRAAM',vendor_name='SAME NAME',cage_code='22222',sub_agency='NAVY',total_spend=200,contract_count=4)])
        value = self.ns['get_platform_profile']('AMRAAM', years=[2024,2025])
        self.assertEqual(value['contract_count'], 3)
        self.assertEqual(value['award_record_count'], 3)
        self.assertEqual(value['summary_group_contract_count'], 11)
        self.assertEqual(value['recorded_company_name_count'], 1)
        self.assertEqual(value['supplier_site_count'], 2)
        self.assertEqual(value['total_obligations'], 300)
        self.assertEqual(value['contract_count_basis'], 'enriched_award_records')

    def test_empty_funding_summary_does_not_erase_available_awards(self):
        self.ns['build_summary_where'] = lambda years, filters: ('1=1', [])
        self.ns['query_summary_df'] = lambda **kw: pd.DataFrame()
        value = self.ns['get_platform_profile']('AMRAAM', years=[2024,2025])
        self.assertEqual(value['award_record_count'], 3)
        self.assertEqual(value['summary_group_contract_count'], 0)

    def test_programme_parts_reject_buyer_filter_before_source_read(self):
        source = Mock(side_effect=AssertionError('Unsupported scope must not query data'))
        self.ns['duck_fetch_df'] = source
        app = FastAPI()
        app.get('/parts')(self.ns['get_platform_parts'])
        for buyer in ['NAVY', 'DEPARTMENT OF THE AIR FORCE']:
            response = TestClient(app).get('/parts', params={'name':'AMRAAM','agency':buyer})
            self.assertEqual(response.status_code, 422)
            self.assertIn('Buyer filtering is unavailable', response.json()['detail'])
        source.assert_not_called()


if __name__ == '__main__':
    unittest.main()
