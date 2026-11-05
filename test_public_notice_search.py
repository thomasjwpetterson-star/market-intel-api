"""Exercise the public search SQL against representative notice deadlines."""
import ast
import logging
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from unittest.mock import Mock

import duckdb
from fastapi import HTTPException, Response


class PublicNoticeSearchTests(unittest.TestCase):
    def setUp(self):
        self.db = duckdb.connect()
        self.addCleanup(self.db.close)
        self.db.execute("""CREATE TABLE public_intelligence_manifest (
            entity_type VARCHAR, entity_id VARCHAR, canonical_path VARCHAR, display_name VARCHAR
        )""")
        self.db.execute("""CREATE TABLE v_opportunities (
            id VARCHAR, sol_num VARCHAR, agency VARCHAR, sub_agency VARCHAR, source_system VARCHAR,
            psc VARCHAR, naics VARCHAR, set_aside_type VARCHAR, deadline VARCHAR, description VARCHAR
        )""")
        self.db.execute("""CREATE TABLE public_intelligence_search (
            entity_type VARCHAR, entity_id VARCHAR, canonical_path VARCHAR, display_name VARCHAR,
            richness_score INTEGER, search_text VARCHAR
        )""")
        functions = {"search_public_open_notices", "search_public_intelligence_manifest"}
        nodes = [node for node in ast.parse(Path(__file__).with_name("main.py").read_text()).body
                 if isinstance(node, ast.FunctionDef) and node.name in functions]
        for node in nodes:
            node.decorator_list = []
        self.ns = dict(Response=Response, HTTPException=HTTPException, Optional=Optional,
                       require_public_snapshot_ready=Mock(), df_sanitize_for_json=lambda df: df,
                       duck_fetch_df=lambda sql, params: self.db.execute(sql, params).fetchdf(),
                       logger=logging.getLogger(__name__))
        exec(compile(ast.Module(body=nodes, type_ignores=[]), "main.py", "exec"), self.ns)

    def add_notice(self, notice_id, deadline, description="Radar parts", published=True):
        self.db.execute("INSERT INTO v_opportunities VALUES (?, ?, 'Army', 'Army office', 'SAM', '5840', '334511', NULL, ?, ?)",
                        [notice_id, f"BID-{notice_id}", deadline, description])
        if published:
            self.db.execute("INSERT INTO public_intelligence_manifest VALUES ('solicitation', ?, ?, ?)",
                            [notice_id, f"/intelligence/solicitations/{notice_id}/notice", f"Notice {notice_id}"])

    def test_search_excludes_elapsed_deadlines_and_unpublished_rows_before_limit(self):
        now = datetime.now(timezone.utc)
        for index in range(25):
            self.add_notice(f"expired-{index}", (now - timedelta(seconds=5)).isoformat())
        future = (now + timedelta(days=1)).isoformat()
        self.add_notice("hidden", future, published=False)
        self.add_notice("date-only", (now + timedelta(days=1)).date().isoformat())
        self.add_notice("unrelated", future, description="Office chairs")
        self.add_notice("live", future)

        result = self.ns["search_public_open_notices"](Response(), "radar")
        self.assertEqual([entry["entity_id"] for entry in result["entries"]], ["live"])
        self.assertEqual(result["entries"][0]["buyer"], "Army office")
        self.assertTrue(result["entries"][0]["deadline"].endswith("Z"))
        self.assertFalse(result["has_more"])

    def test_exact_notice_number_and_browse_cap(self):
        now = datetime.now(timezone.utc)
        for index in range(14):
            self.add_notice(f"{index:02}", (now + timedelta(days=index + 1)).isoformat())
        search = self.ns["search_public_open_notices"]
        result = search(Response())
        self.assertEqual(len(result["entries"]), 12)
        self.assertEqual(result["entries"][0]["entity_id"], "00")
        self.assertTrue(result["has_more"])
        self.assertEqual([row["entity_id"] for row in search(Response(), "bid-13")["entries"]], ["13"])

    def test_part_number_scope_applies_before_result_limit(self):
        self.db.execute("INSERT INTO public_intelligence_search VALUES ('contract_award', 'PN-123', '/award', 'PN-123 award', 100, 'pn-123'), ('nsn', '6140015125788', '/part', 'Battery assembly', 1, 'pn-123')")
        search = self.ns["search_public_intelligence_manifest"]
        self.assertEqual(search(Response(), "PN-123", 1)["entries"][0]["entity_type"], "contract_award")
        self.assertEqual(search(Response(), "PN-123", 1, "nsn")["entries"][0]["entity_id"], "6140015125788")


if __name__ == "__main__":
    unittest.main()
