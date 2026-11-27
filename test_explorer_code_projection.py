import ast
import datetime
import itertools
from pathlib import Path
import pandas as pd
from explorer_page_read import read_snapshot_page
from types import SimpleNamespace
import duckdb
from dashboard_projection import build_code_projection, build_explorer_projection
from test_programme_scope_counts import namespace


def test_prepared_dictionaries_preserve_air_awards_descriptions_and_signed_totals_without_summary_scan():
    db=duckdb.connect()
    try:
        db.execute('''CREATE TABLE v_summary(psc_code VARCHAR,psc_description VARCHAR,naics_code VARCHAR,naics_description VARCHAR)''')
        db.execute("INSERT INTO v_summary VALUES ('1510','Aircraft','336411','Aircraft manufacturing'),('1510','Fixed wing aircraft','336411','Z aircraft manufacturing'),('9999',NULL,'999999',NULL)")
        db.execute('''CREATE TABLE v_contracts_rolled(award_key VARCHAR,contract_id VARCHAR,market_segment VARCHAR,psc VARCHAR,naics_code VARCHAR,naics_description VARCHAR,
            obligations_fy2025 VARCHAR,action_count_fy2025 VARCHAR,earliest_action_date_fy2025 VARCHAR,latest_action_date_fy2025 VARCHAR,unrelated_large_detail VARCHAR DEFAULT NULL)''')
        db.execute("INSERT INTO v_contracts_rolled(award_key,contract_id,market_segment,psc,naics_code,naics_description,obligations_fy2025,action_count_fy2025,earliest_action_date_fy2025,latest_action_date_fy2025) VALUES ('A','A','AIR','1510','336411',NULL,100,2,'2025-01-01','2025-02-01'),('B','B',' air ','1510','336411','Source description',-20,1,'2025-03-01','2025-03-01'),('C','C','AIR',NULL,'999999',NULL,'invalid',1,'2025-04-01','2025-04-01'),('D','D','NAVAL','1510','336411',NULL,400,1,'2025-05-01','2025-05-01'),('E','A','AIR','1510','336411',NULL,50,1,'2025-06-01','2025-06-01')")
        db.execute('ALTER TABLE v_contracts_rolled ADD COLUMN obligations_fy2024 VARCHAR')
        db.execute("UPDATE v_contracts_rolled SET obligations_fy2024=CASE award_key WHEN 'A' THEN '10' WHEN 'B' THEN '1' WHEN 'C' THEN '2' WHEN 'D' THEN '4' ELSE '5' END")
        db.execute("ALTER TABLE v_contracts_rolled ADD COLUMN action_count_fy2024 VARCHAR DEFAULT '1'")
        db.execute("ALTER TABLE v_contracts_rolled ADD COLUMN earliest_action_date_fy2024 VARCHAR DEFAULT '2024-01-01'")
        db.execute("ALTER TABLE v_contracts_rolled ADD COLUMN latest_action_date_fy2024 VARCHAR DEFAULT '2024-09-30'")
        ns=namespace()
        def columns(table):
            try:return {row[0] for row in db.execute('DESCRIBE '+table).fetchall()}
            except duckdb.CatalogException:return set()
        ns['get_duck_table_columns']=columns
        ns['CONTRACT_AWARD_SYNTHETIC_COLUMNS']={'contract_id','psc_description','naics_description','obligations_in_selected_period_usd','number_of_actions_in_selected_period','earliest_action_date_in_selected_period','latest_action_date_in_selected_period'}
        build=ns['build_contract_awards_explorer_query_from_rollup']
        cases=[]
        for years,domain,minimum in itertools.product([None,[2025],[2024],[2023],[2024,2025]],['AIR','NAVAL'],[None,-50,0]):
            filters={'market_segment':[domain]}
            if years is not None:filters['year']=years
            if minimum is not None:filters['min_spend']=minimum
            payload=SimpleNamespace(table='v_contract_awards_enriched',columns=['contract_id','psc_description','naics_description','obligations_in_selected_period_usd','number_of_actions_in_selected_period','earliest_action_date_in_selected_period','latest_action_date_in_selected_period'],filters=filters)
            for offset in [0,1,3]:
                sql,params=build(payload,2,offset)
                expected=db.execute(sql,params).fetchall()
                count_sql,count_params=build(payload,0,count_only=True)
                cases.append((payload,offset,expected,db.execute(count_sql,count_params).fetchall()))
        db.execute('BEGIN TRANSACTION')
        build_code_projection(db)
        build_explorer_projection(db)
        db.execute('COMMIT')
        assert db.execute('SELECT COUNT(*) FROM dashboard_explorer_awards').fetchone()[0] == 5
        assert 'unrelated_large_detail' not in columns('dashboard_explorer_awards')
        assert db.execute('SELECT COUNT(*) FROM dashboard_explorer_rank').fetchone()[0] == 5
        assert {r[0] for r in db.execute('SELECT _explorer_row_id FROM dashboard_explorer_rank').fetchall()} == set(range(1,6))
        assert not {'description','base_award_description','latest_action_description'} & columns('dashboard_explorer_rank')
        assert db.execute('''SELECT COUNT(*) FROM dashboard_explorer_rank r
            JOIN dashboard_explorer_awards a ON a._explorer_row_id=r._explorer_row_id
            WHERE r.contract_id=a.contract_id AND r._explorer_all_year_obligations=a._explorer_all_year_obligations''').fetchone()[0] == 5
        assert dict((r[0],r[1]) for r in db.execute('DESCRIBE dashboard_explorer_awards').fetchall())['obligations_fy2025']=='DOUBLE'
        db.execute('DROP TABLE v_contracts_rolled')
        db.execute('DROP TABLE v_summary')
        node = next(node for node in ast.parse(Path('main.py').read_text()).body
                    if isinstance(node,ast.FunctionDef) and node.name=='fetch_explorer_page')
        exec(compile(ast.Module(body=[node],type_ignores=[]),'main.py','exec'),ns)
        ns['build_explorer_query']=lambda payload,**kw: build(payload,**kw)
        def fetch(sql,params,followup=None):
            cursor=db.cursor()
            try:
                return read_snapshot_page(cursor,sql,params,followup) if followup else cursor.execute(sql,params).fetchdf()
            finally:
                cursor.close()
        ns['duck_fetch_df']=fetch
        for payload,offset,expected,expected_count in cases:
            sql,params=build(payload,2,offset)
            assert 'FROM dashboard_psc' in sql and 'FROM dashboard_naics' in sql
            assert 'FROM v_summary' not in sql
            assert 'FROM dashboard_explorer_awards r' in sql
            assert db.execute(sql,params).fetchall()==expected
            count_sql,count_params=build(payload,0,count_only=True)
            assert db.execute(count_sql,count_params).fetchall()==expected_count
            page=ns['fetch_explorer_page'](payload,2,offset)
            rows=[tuple(None if pd.isna(v) else v.date() if isinstance(v,datetime.datetime) else v for v in row)
                  for row in page.itertuples(index=False,name=None)]
            assert rows==expected
        # Rebuilding refreshes both dictionary lanes from the new release.
        db.execute("CREATE TABLE v_summary AS SELECT '1510' AS psc_code,'Revised aircraft' AS psc_description,'336411' AS naics_code,'Revised NAICS' AS naics_description")
        build_code_projection(db)
        assert db.execute('SELECT psc_desc FROM dashboard_psc').fetchone()[0]=='Revised aircraft'
        assert db.execute('SELECT naics_description FROM dashboard_naics').fetchone()[0]=='Revised NAICS'
    finally:db.close()


def test_explorer_projection_refresh_and_rollback_keep_native_grain():
    db = duckdb.connect()
    try:
        db.execute("CREATE TABLE v_contracts_rolled AS SELECT 'A' AS contract_id,'AIR' AS market_segment,-20.0 AS obligations_fy2025")
        build_explorer_projection(db)
        db.execute('UPDATE v_contracts_rolled SET obligations_fy2025=30')
        db.execute('BEGIN TRANSACTION')
        build_explorer_projection(db)
        assert db.execute('SELECT obligations_fy2025 FROM dashboard_explorer_awards').fetchone()[0] == 30
        db.execute('ROLLBACK')
        assert db.execute('SELECT obligations_fy2025 FROM dashboard_explorer_awards').fetchone()[0] == -20
        build_explorer_projection(db)
        assert db.execute('SELECT obligations_fy2025 FROM dashboard_explorer_awards').fetchone()[0] == 30
    finally:
        db.close()
