import ast
from pathlib import Path
from types import SimpleNamespace
from typing import Any, List, Optional
import unittest
import duckdb
from fastapi import HTTPException


class DescriptionExplorer(unittest.TestCase):
    def setUp(self):
        self.db=duckdb.connect();self.addCleanup(self.db.close)
        self.db.execute('CREATE TABLE v_transactions(contract_id VARCHAR,year INTEGER,sub_agency VARCHAR,description VARCHAR,base_award_description VARCHAR,action_description VARCHAR,spend_amount DOUBLE)')
        self.db.execute("""INSERT INTO v_transactions VALUES
            ('A',2025,'NAVY','ADAPTER,TEST,AMRAAM',NULL,NULL,100),
            ('B',2025,'NAVY',NULL,NULL,'AIM-120 support',-10),
            ('C',2025,'AIR FORCE','AMRAAM',NULL,NULL,200),
            ('D',2024,'NAVY','AMRAAM',NULL,NULL,300),
            ('E',2025,'NAVY','UNRELATED',NULL,NULL,400)""")
        wanted={'build_explorer_query','resolve_explorer_column','quote_ident','safe_int'}
        nodes=[n for n in ast.parse(Path('main.py').read_text()).body if isinstance(n,ast.FunctionDef) and n.name in wanted]
        columns={r[0] for r in self.db.execute('DESCRIBE v_transactions').fetchall()}
        self.ns=dict(Any=Any,List=List,Optional=Optional,ExplorerRequest=SimpleNamespace,HTTPException=HTTPException,
            ALLOWED_EXPLORER_TABLES={'v_transactions'},ALLOWED_EXPLORER_COLUMNS=columns,
            EXPLORER_DEFAULT_COLUMNS={'v_transactions':['contract_id']},EXPLORER_FILTER_REQUIRED_TABLES={'v_transactions'},EXPLORER_COLUMN_ALIASES={},
            CONTRACT_AWARD_EXPLORER_TABLE='v_contract_awards_enriched',SUBCONTRACT_EXPLORER_TABLE='v_subcontracts',
            NSN_REF_TABLE='v_nsn_cage_reference',NSN_REF_EXPLORER_SOURCE='unused',NSN_SUPPLY_STATE_TABLE='unused1',NSN_PRICE_SUMMARY_TABLE='unused2',NSN_OPPORTUNITY_SUMMARY_TABLE='unused3',NSN_OPPORTUNITY_DETAIL_TABLE='unused4',
            get_duck_table_columns=lambda _:columns)
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'main.py','exec'),self.ns)

    def query(self,filters,count=False,offset=0):
        payload=SimpleNamespace(table='v_transactions',columns=['contract_id','description','action_description'],filters=filters)
        sql,params=self.ns['build_explorer_query'](payload,row_limit=1,offset=offset,count_only=count)
        return self.db.execute(sql,params).fetchall()

    def test_search_pages_and_count_share_exact_buyer_year_and_alias_filters(self):
        filters={'sub_agency':'NAVY','year':[2025],'description_terms':['AMRAAM','AIM-120']}
        self.assertEqual(self.query(filters,count=True),[(2,)])
        self.assertEqual(self.query(filters)[0][0],'A')
        self.assertEqual(self.query(filters,offset=1)[0][0],'B')

    def test_single_literal_search_excludes_unrelated_and_other_years(self):
        filters={'sub_agency':'NAVY','year':[2025],'search':'AMRAAM'}
        self.assertEqual(self.query(filters,count=True),[(1,)])
        self.assertEqual(self.query({**filters,'search':"AMRAAM' OR 1=1 --"},count=True),[(0,)])
        self.assertEqual(self.query({**filters,'search':'%_'},count=True),[(0,)])

    def test_invalid_description_terms_are_not_silently_ignored(self):
        for terms in [[],['A'],['AMRAAM']*9]:
            with self.assertRaises(HTTPException):self.query({'year':[2025],'description_terms':terms})


if __name__=='__main__':unittest.main()
