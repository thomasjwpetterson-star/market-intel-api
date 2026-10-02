"""Exercise real public-route code without starting the full analytical runtime."""
import ast
import logging
import math
import re
import unittest
from pathlib import Path
from typing import Optional
from unittest.mock import Mock

import duckdb
import numpy as np
import pandas as pd
from fastapi import HTTPException, Response


def route_namespace(source=None):
    wanted = {'get_public_award_page_snapshot', 'df_sanitize_for_json', 'sanitize',
              '_safe_public_number', '_optional_public_number', '_clean_optional_value', '_clean_entity_name'}
    nodes = []
    for node in ast.parse(source or Path(__file__).with_name('main.py').read_text()).body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            node.decorator_list = []
            nodes.append(node)
    namespace = dict(pd=pd, np=np, math=math, re=re, Optional=Optional,
                     Response=Response, HTTPException=HTTPException,
                     logger=logging.getLogger('award-serving-test'),
                     require_public_snapshot_ready=Mock(), attach_publication_metadata=Mock(),
                     get_award_profile=Mock(side_effect=AssertionError('Legacy work must not run')))
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'main.py', 'exec'), namespace)
    return namespace


class PublicAwardServingTests(unittest.TestCase):
    def setUp(self):
        self.ns = route_namespace()
        self.db = duckdb.connect()
        self.addCleanup(self.db.close)
        self.db.execute('''CREATE TABLE public_award_profile AS SELECT
            'W9123620C2025' AS contract_id, 'EXAMPLE' AS vendor_name,
            12.0 AS total_spend, 2 AS action_count, NULL::DOUBLE AS latest_reported_total_obligated,
            5.0 AS obligations_fy2024, 7.0 AS obligations_fy2025,
            'Research and development' AS base_award_description''')
        self.db.execute('CREATE UNIQUE INDEX award_id ON public_award_profile(contract_id)')
        self.ns['duck_fetch_df'] = lambda sql, params: self.ns['df_sanitize_for_json'](self.db.execute(sql, params).fetchdf())

    def test_released_award_keeps_values_and_cache_contract(self):
        response = Response()
        result = self.ns['get_public_award_page_snapshot']('w9123620c2025', response)
        self.assertEqual(result['contract_id'], 'W9123620C2025')
        self.assertEqual(result['total_obligations'], 12)
        self.assertIsNone(result['latest_reported_total_obligated'])
        self.assertEqual(result['annual_obligations'], [{'year':2024,'value':5}, {'year':2025,'value':7}])
        self.ns['attach_publication_metadata'].assert_called_once_with(result, 'contract_award', 'W9123620C2025')
        self.assertIn('s-maxage=86400', response.headers['Cache-Control'])
        self.ns['get_award_profile'].assert_not_called()

    def test_missing_award_is_404_after_existing_profile_lookup(self):
        self.ns['get_award_profile'] = Mock(return_value=None)
        with self.assertRaises(HTTPException) as error:
            self.ns['get_public_award_page_snapshot']('MISSING', Response())
        self.assertEqual(error.exception.status_code, 404)
        self.ns['get_award_profile'].assert_called_once_with('MISSING')

    def test_existing_public_award_outside_indexable_cohort_remains_available(self):
        self.ns['get_award_profile'] = Mock(return_value={
            'contract_id': 'SPE4A623PH722', 'vendor_name': 'EXAMPLE',
            'total_spend': 25.0, 'description': 'Existing public profile',
            'agency': 'Department of Defense',
        })
        result = self.ns['get_public_award_page_snapshot']('SPE4A623PH722', Response())
        self.assertEqual(result['contract_id'], 'SPE4A623PH722')
        self.assertEqual(result['total_obligations'], 25.0)
        self.ns['get_award_profile'].assert_called_once_with('SPE4A623PH722')

    def test_projection_failure_is_retryable_not_a_false_not_found(self):
        self.ns['duck_fetch_df'] = Mock(side_effect=RuntimeError('Database temporarily unavailable'))
        with self.assertRaises(HTTPException) as error:
            self.ns['get_public_award_page_snapshot']('W9123620C2025', Response())
        self.assertEqual(error.exception.status_code, 503)
        self.ns['get_award_profile'].assert_not_called()
