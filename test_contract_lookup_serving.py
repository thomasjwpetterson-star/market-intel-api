import ast
import logging
from pathlib import Path
import re
from typing import Optional

import duckdb
import numpy as np
import pandas as pd
import pytest

from contract_lookup_layout import sort_lookup_file
from contract_lookup_serving import register_contract_lookup


def profile_namespace(db):
    names = {
        "_award_lookup_identity", "get_award_profile", "get_award_history",
        "get_award_announcements", "sanitize", "df_sanitize_for_json",
    }
    nodes = []
    for node in ast.parse(Path(__file__).with_name("main.py").read_text()).body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            node.decorator_list = []
            nodes.append(node)
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "_NATIVE_AWARD_KEY" for target in node.targets
        ):
            nodes.append(node)
    ns = dict(pd=pd, np=np, re=re, Optional=Optional, GLOBAL_CACHE={},
              LOCAL_CACHE_DIR=Path("."), lookup_contract_announcements=lambda _, piid: [piid],
              logger=logging.getLogger("lookup-test"),
              get_duck_table_columns=lambda view: {row[0] for row in db.execute(f"DESCRIBE {view}").fetchall()},
              duck_fetch_df=lambda sql, params: db.execute(sql, params).fetchdf())
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "main.py", "exec"), ns)
    return ns


def test_actual_profile_handler_parity_and_rollback(tmp_path):
    db = duckdb.connect(config={"threads": 1})
    try:
        source = tmp_path / "contracts_rolled.parquet"
        first_key = "CONT_AWD_Z_9700_PARENT1_9700"
        second_key = "CONT_AWD_Z_9700_PARENT2_9700"
        unique_key = "CONT_IDV_A_9700"
        db.execute(f"""COPY (SELECT contract_id, award_key, vendor_name, 'CAGE' AS vendor_cage,
            NULL::VARCHAR AS sub_agency, 'DOD' AS parent_agency, 'old' AS description,
            ' base ' AS base_award_description, NULL::VARCHAR AS platform_family,
            NULL::VARCHAR AS city, 'VA' AS state, 'US' AS country, '123' AS naics_code,
            'R1' AS psc, 'FIXED' AS pricing_type, NULL::VARCHAR AS competition_type,
            NULL::INTEGER AS offers_count, '' AS set_aside_type, 'SOL' AS solicitation_id,
            12.25 AS total_spend, '2026-01-01'::DATE AS start_date,
            NULL::DATE AS last_action_date
            FROM (VALUES ('Z','{first_key}','first'),('A','{unique_key}','a'),
                  ('Z','{second_key}','second')) t(contract_id,award_key,vendor_name))
            TO '{source}' (FORMAT PARQUET)""")
        db.read_parquet(str(source)).create_view("v_contracts_rolled")
        ns = profile_namespace(db)
        before = {key: ns["get_award_profile"](key) for key in ["Z", "A", "MISSING"]}
        sort_lookup_file(source, tmp_path / "contracts_lookup.parquet", memory_mb=128)
        ns["GLOBAL_CACHE"]["contract_lookup_ready"] = register_contract_lookup(db, tmp_path, enabled=True)
        assert before == {key: ns["get_award_profile"](key) for key in before}
        assert before["Z"]["vendor_name"] == "first"
        assert ns["get_award_profile"](first_key)["vendor_name"] == "first"
        second = ns["get_award_profile"](second_key)
        assert second["vendor_name"] == "second" and second["award_key"] == second_key
        assert ns["get_award_profile"](second_key + "X") is None

        db.execute("""CREATE TABLE v_transactions (
            award_key VARCHAR, contract_id VARCHAR, action_date DATE, spend_amount DOUBLE,
            action_description VARCHAR, description VARCHAR
        )""")
        db.executemany("INSERT INTO v_transactions VALUES (?, 'Z', DATE '2026-01-01', ?, ?, NULL)", [
            (first_key, 100, "first action"), (second_key, 200, "second action"),
        ])
        assert [row["spend_amount"] for row in ns["get_award_history"](first_key)] == [100]
        assert [row["spend_amount"] for row in ns["get_award_history"](second_key)] == [200]
        assert sorted(row["spend_amount"] for row in ns["get_award_history"]("Z")) == [100, 200]
        assert ns["get_award_announcements"](first_key) == []
        assert ns["get_award_announcements"](unique_key) == ["A"]

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
