"""Schema reuse must refresh with releases and must not retain query failures."""
import ast
import functools
import logging
from pathlib import Path

import pandas as pd


def test_schema_metadata_reuses_only_successful_reads_in_same_release():
    names = {'_get_duck_table_columns_cached', 'get_duck_table_columns'}
    nodes = [n for n in ast.parse(Path('main.py').read_text()).body
             if isinstance(n, ast.FunctionDef) and n.name in names]
    calls = []
    fail = [False]

    def fetch(sql):
        calls.append(sql)
        if fail[0]:
            raise RuntimeError('transient reload')
        return pd.DataFrame({'column_name': ['contract_id', 'description']})

    ns = dict(lru_cache=functools.lru_cache, duck_fetch_df=fetch,
              GLOBAL_CACHE={'last_loaded': 1}, ALLOWED_EXPLORER_TABLES={'v_contracts_rolled'},
              INTERNAL_DUCK_RELATIONS=set(), CONTRACT_AWARD_EXPLORER_TABLE='synthetic',
              CONTRACT_AWARD_SYNTHETIC_COLUMNS=[], logger=logging.getLogger('schema-test'))
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'main.py', 'exec'), ns)
    get = ns['get_duck_table_columns']
    assert get('v_contracts_rolled') == {'contract_id', 'description'}
    get('v_contracts_rolled').clear()
    assert get('v_contracts_rolled') == {'contract_id', 'description'}
    assert len(calls) == 1
    assert get('unapproved_relation') == set() and len(calls) == 1
    ns['GLOBAL_CACHE']['last_loaded'] = 2
    fail[0] = True
    assert get('v_contracts_rolled') == set()
    fail[0] = False
    assert get('v_contracts_rolled') == {'contract_id', 'description'}
    assert len(calls) == 3
