import ast
import logging
from pathlib import Path
from typing import Optional

import duckdb
import numpy as np
import pandas as pd
import pytest

from contract_lookup_layout import sort_lookup_file
from contract_lookup_serving import register_contract_lookup


def profile_namespace(db):
    names = {"get_award_profile", "sanitize", "df_sanitize_for_json"}
    nodes = []
    for node in ast.parse(Path(__file__).with_name("main.py").read_text()).body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            node.decorator_list = []
            nodes.append(node)
    ns = dict(pd=pd, np=np, Optional=Optional, GLOBAL_CACHE={},
              logger=logging.getLogger("lookup-test"),
              get_duck_table_columns=lambda view: {row[0] for row in db.execute(f"DESCRIBE {view}").fetchall()},
              duck_fetch_df=lambda sql, params: db.execute(sql, params).fetchdf())
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "main.py", "exec"), ns)
    return ns


def test_actual_profile_handler_parity_and_rollback(tmp_path):
    db = duckdb.connect(config={"threads": 1})
    try:
        source = tmp_path / "contracts_rolled.parquet"
        db.execute(f"""COPY (SELECT contract_id, vendor_name, 'CAGE' AS vendor_cage,
            NULL::VARCHAR AS sub_agency, 'DOD' AS parent_agency, 'old' AS description,
            ' base ' AS base_award_description, NULL::VARCHAR AS platform_family,
            NULL::VARCHAR AS city, 'VA' AS state, 'US' AS country, '123' AS naics_code,
            'R1' AS psc, 'FIXED' AS pricing_type, NULL::VARCHAR AS competition_type,
            NULL::INTEGER AS offers_count, '' AS set_aside_type, 'SOL' AS solicitation_id,
            12.25 AS total_spend, '2026-01-01'::DATE AS start_date,
            NULL::DATE AS last_action_date
            FROM (VALUES ('Z','first'),('A','a'),('Z','second')) t(contract_id,vendor_name))
            TO '{source}' (FORMAT PARQUET)""")
        db.read_parquet(str(source)).create_view("v_contracts_rolled")
        ns = profile_namespace(db)
        before = {key: ns["get_award_profile"](key) for key in ["Z", "A", "MISSING"]}
        sort_lookup_file(source, tmp_path / "contracts_lookup.parquet", memory_mb=128)
        ns["GLOBAL_CACHE"]["contract_lookup_ready"] = register_contract_lookup(db, tmp_path, enabled=True)
        assert before == {key: ns["get_award_profile"](key) for key in before}
        assert before["Z"]["vendor_name"] == "first"
        # Selecting an old release / disabling the feature drops its view and uses the old query.
        ns["GLOBAL_CACHE"]["contract_lookup_ready"] = register_contract_lookup(db, tmp_path, enabled=False)
        assert before == {key: ns["get_award_profile"](key) for key in before}
        assert not register_contract_lookup(db, tmp_path / "old-release", enabled=True)
    finally:
        db.close()


def test_schema_mismatch_fails_closed(tmp_path):
    db = duckdb.connect()
    try:
        db.execute("CREATE VIEW v_contracts_rolled AS SELECT 'A' AS contract_id")
        db.execute(f"COPY (SELECT 1 AS wrong) TO '{tmp_path / 'contracts_lookup.parquet'}' (FORMAT PARQUET)")
        with pytest.raises(ValueError, match="schema differs"):
            register_contract_lookup(db, tmp_path, enabled=True)
    finally:
        db.close()
