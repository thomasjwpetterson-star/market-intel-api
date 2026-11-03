"""Focused SQL regression checks for the dashboard KPI handler.

The API's optional service dependencies are not needed here: execute only the
handler and its filter helpers against in-memory DuckDB views.
"""

import ast
from datetime import date, timedelta
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import unittest
from unittest.mock import patch

import duckdb
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.testclient import TestClient


class DashboardKpiPscTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source = ast.parse(Path(__file__).with_name("main.py").read_text())
        names = {"_like_param_contains", "build_summary_where", "get_recompete_kpi", "get_market_kpis"}
        functions = [node for node in source.body if isinstance(node, ast.FunctionDef) and node.name in names]
        cls.conn = duckdb.connect()
        app = FastAPI()
        cls.namespace = {
            "Any": Any, "Dict": Dict, "List": List, "Optional": Optional, "Tuple": Tuple,
            "pd": pd, "logger": logging.getLogger(__name__), "HTTPException": HTTPException,
            "app": app, "Query": Query,
            "safe_years": lambda years, **kwargs: years,
            "duck_fetch_df": lambda sql, params=None: cls.conn.execute(sql, params or []).fetchdf(),
        }
        exec(compile(ast.fix_missing_locations(ast.Module(body=functions, type_ignores=[])), "main.py", "exec"), cls.namespace)
        cls.namespace["query_summary_df"] = cls.query_summary_df
        cls.client = TestClient(app)

        cls.conn.execute("""CREATE TABLE v_summary (
            vendor_name VARCHAR, clean_parent VARCHAR, cage_code VARCHAR,
            market_segment VARCHAR, sub_agency VARCHAR, platform_family VARCHAR,
            psc_code VARCHAR, psc_description VARCHAR, year INTEGER,
            total_spend DOUBLE, contract_count INTEGER
        )""")
        cls.conn.executemany("INSERT INTO v_summary VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
            ("ACME", "PARENT A", "00001", "AIR", "NAVY", "JET", "1111", "AIRCRAFT", 2025, 2e9, 2),
            ("ACME", "PARENT A", "00001", "AIR", "NAVY", "JET", "2222", "SUPPORT OTHER", 2025, 3e9, 3),
            ("BRAVO", "PARENT B", "00002", "SEA", "ARMY", "SHIP", "3333", "OTHER SYSTEM", 2026, 4e9, 4),
            ("BRAVO", "PARENT B", "00002", "SEA", "ARMY", "SHIP", "4444", "MISSILES", 2026, 1e9, 1),
        ])
        cls.conn.execute("""CREATE TABLE v_risk (
            award_key VARCHAR, contract_id VARCHAR, spend_amount DOUBLE,
            completion_date DATE, vendor_name VARCHAR, clean_parent VARCHAR,
            cage_code VARCHAR, market_segment VARCHAR, sub_agency VARCHAR,
            platform_family VARCHAR, psc VARCHAR
        )""")
        soon = date.today() + timedelta(days=30)
        later = date.today() + timedelta(days=120)
        cls.conn.executemany("INSERT INTO v_risk VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
            ("A", "A", .5e9, soon, "ACME", "PARENT A", "00001", "AIR", "NAVY", "JET", "1111"),
            ("B", "B", 1e9, soon, "ACME", "PARENT A", "00001", "AIR", "NAVY", "JET", "2222"),
            ("C", "C", 2e9, soon, "BRAVO", "PARENT B", "00002", "SEA", "ARMY", "SHIP", "3333"),
            ("D", "D", 1e9, later, "BRAVO", "PARENT B", "00002", "SEA", "ARMY", "SHIP", "4444"),
        ])

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        cls.conn.close()

    @classmethod
    def query_summary_df(cls, where_sql, params, select_sql, **kwargs):
        return cls.conn.execute(f"SELECT {select_sql} FROM v_summary WHERE {where_sql}", params).fetchdf()

    def kpis(self, **kwargs):
        kwargs.setdefault("years", None)
        return self.namespace["get_market_kpis"](**kwargs)

    def test_unfiltered_empty_and_null_psc(self):
        expected = self.kpis()
        self.assertEqual(expected["total_spend_b"], 10)
        self.assertEqual(expected["total_contracts"], 10)
        self.assertEqual(expected["recompete_risk"]["value"], "$3.50B")
        self.assertEqual(expected["recompete_risk"]["sub_label"], "3 awards ending")
        self.assertEqual(self.kpis(psc=""), expected)
        self.assertEqual(self.kpis(psc=None), expected)

    def test_normal_psc_code_and_description(self):
        for psc in ("1111", "111", "AIRCRAFT", "aircraft"):
            with self.subTest(psc=psc):
                result = self.kpis(psc=psc)
                self.assertEqual(result["total_spend_b"], 2)
                self.assertEqual(result["total_contracts"], 2)
                self.assertEqual(result["recompete_risk"]["value"], "$0.50B")

    def test_other_matches_existing_summary_description_filter(self):
        result = self.kpis(psc="Other")
        self.assertEqual(result["total_spend_b"], 7)
        self.assertEqual(result["total_contracts"], 7)
        self.assertEqual(result["recompete_risk"]["value"], "$3.00B")
        self.assertEqual(result["recompete_risk"]["sub_label"], "2 awards ending")

    def test_combined_dashboard_filters(self):
        result = self.kpis(years=[2025], vendor="acm", parent="Parent A", cage="00001",
                           domain="AIR", agency="NAVY", platform="JET", psc="Other")
        self.assertEqual(result["total_spend_b"], 3)
        self.assertEqual(result["total_contracts"], 3)
        self.assertEqual(result["recompete_risk"]["value"], "$1.00B")

    def test_risk_query_failure_is_explicit(self):
        fetch = self.namespace["duck_fetch_df"]

        def fail_risk(sql, params=None):
            if "SUM(spend_amount)" in sql:
                raise duckdb.BinderException('Referenced column "psc_code" not found')
            return fetch(sql, params)

        self.namespace["duck_fetch_df"] = fail_risk
        try:
            with self.assertLogs(self.namespace["logger"], level="ERROR") as logs:
                with self.assertRaises(HTTPException) as error:
                    self.kpis(psc="Other")
            self.assertEqual(error.exception.status_code, 500)
            self.assertEqual(error.exception.detail, "Risk KPI calculation failed")
            self.assertIsNotNone(logs.records[0].exc_info)
        finally:
            self.namespace["duck_fetch_df"] = fetch

    def test_http_psc_filters_return_valid_risk(self):
        for psc, expected in ((None, "$3.50B"), ("", "$3.50B"),
                              ("1111", "$0.50B"), ("AIRCRAFT", "$0.50B"),
                              ("Other", "$3.00B")):
            with self.subTest(psc=psc):
                response = self.client.get("/api/dashboard/kpis", params={} if psc is None else {"psc": psc})
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["recompete_risk"]["value"], expected)

    def test_http_risk_failures_return_500_without_fallback_data(self):
        fetch = self.namespace["duck_fetch_df"]
        for failing_sql in ("DESCRIBE v_risk", "SUM(spend_amount)"):
            def fail_risk(sql, params=None):
                if failing_sql in sql:
                    raise duckdb.BinderException('Referenced column "psc_code" not found')
                return fetch(sql, params)

            with self.subTest(failing_sql=failing_sql), patch.dict(self.namespace, duck_fetch_df=fail_risk):
                with self.assertLogs(self.namespace["logger"], level="ERROR"):
                    response = self.client.get("/api/dashboard/kpis", params={"psc": "Other"})
                self.assertEqual(response.status_code, 500)
                self.assertEqual(response.json(), {"detail": "Risk KPI calculation failed"})

    def test_no_matching_risk_is_valid_no_data(self):
        for psc in ("MISSING", "MISSILES", "%", "_", "\\", "' OR 1=1 --"):
            with self.subTest(psc=psc):
                response = self.client.get("/api/dashboard/kpis", params={"psc": psc})
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["recompete_risk"]["value"], "N/A")
                self.assertEqual(response.json()["recompete_risk"]["sub_label"], "No Data")

    def test_legacy_contract_id_schema(self):
        self.conn.execute("ALTER TABLE v_risk RENAME COLUMN award_key TO legacy_award_key")
        try:
            self.assertEqual(self.kpis(psc="Other")["recompete_risk"]["sub_label"], "2 awards ending")
        finally:
            self.conn.execute("ALTER TABLE v_risk RENAME COLUMN legacy_award_key TO award_key")

    def test_completion_window_boundaries_and_distinct_awards(self):
        self.conn.execute("BEGIN")
        try:
            self.conn.execute("DELETE FROM v_risk")
            for award, offset, spend in (("A", 0, 1e9), ("A", 0, 1e9), ("B", 90, 2e9),
                                         ("C", -1, 4e9), ("D", 91, 8e9), ("E", None, 16e9)):
                completion = None if offset is None else date.today() + timedelta(days=offset)
                self.conn.execute("INSERT INTO v_risk VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                  [award, award, spend, completion, "ACME", "PARENT A", "00001",
                                   "AIR", "NAVY", "JET", "1111"])
            risk = self.kpis(psc="AIRCRAFT")["recompete_risk"]
            self.assertEqual(risk["value"], "$4.00B")
            self.assertEqual(risk["sub_label"], "2 awards ending")
        finally:
            self.conn.execute("ROLLBACK")


if __name__ == "__main__":
    unittest.main()
