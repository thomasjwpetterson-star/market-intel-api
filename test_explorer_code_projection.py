import itertools
from types import SimpleNamespace
import duckdb
from dashboard_projection import build_code_projection
from test_programme_scope_counts import namespace


def test_prepared_dictionaries_preserve_air_awards_descriptions_and_signed_totals_without_summary_scan():
    db=duckdb.connect()
    try:
        db.execute('''CREATE TABLE v_summary(psc_code VARCHAR,psc_description VARCHAR,naics_code VARCHAR,naics_description VARCHAR)''')
        db.execute("INSERT INTO v_summary VALUES ('1510','Aircraft','336411','Aircraft manufacturing'),('1510','Fixed wing aircraft','336411','Z aircraft manufacturing'),('9999',NULL,'999999',NULL)")
        db.execute('''CREATE TABLE v_contracts_rolled(award_key VARCHAR,contract_id VARCHAR,market_segment VARCHAR,psc VARCHAR,naics_code VARCHAR,naics_description VARCHAR,
            obligations_fy2025 DOUBLE,action_count_fy2025 BIGINT,earliest_action_date_fy2025 DATE,latest_action_date_fy2025 DATE)''')
        db.execute("INSERT INTO v_contracts_rolled VALUES ('A','A','AIR','1510','336411',NULL,100,2,'2025-01-01','2025-02-01'),('B','B','AIR','1510','336411','Source description',-20,1,'2025-03-01','2025-03-01'),('C','C','AIR',NULL,'999999',NULL,0,1,'2025-04-01','2025-04-01'),('D','D','NAVAL','1510','336411',NULL,400,1,'2025-05-01','2025-05-01')")
        ns=namespace()
        def columns(table):
            try:return {row[0] for row in db.execute('DESCRIBE '+table).fetchall()}
            except duckdb.CatalogException:return set()
        ns['get_duck_table_columns']=columns
        build=ns['build_contract_awards_explorer_query_from_rollup']
        cases=[]
        for years,domain,minimum in itertools.product([None,[2025],[2024]],['AIR','NAVAL'],[None,-50,0]):
            filters={'market_segment':[domain]}
            if years is not None:filters['year']=years
            if minimum is not None:filters['min_spend']=minimum
            payload=SimpleNamespace(columns=['contract_id','psc_description','naics_description','obligations_in_selected_period_usd','number_of_actions_in_selected_period'],filters=filters)
            for offset in [0,1]:
                sql,params=build(payload,2,offset)
                expected=db.execute(sql,params).fetchall()
                cases.append((payload,offset,expected))
        build_code_projection(db)
        db.execute('DROP TABLE v_summary')
        for payload,offset,expected in cases:
            sql,params=build(payload,2,offset)
            assert 'FROM dashboard_psc' in sql and 'FROM dashboard_naics' in sql
            assert 'FROM v_summary' not in sql
            assert db.execute(sql,params).fetchall()==expected
        # Rebuilding refreshes both dictionary lanes from the new release.
        db.execute("CREATE TABLE v_summary AS SELECT '1510' AS psc_code,'Revised aircraft' AS psc_description,'336411' AS naics_code,'Revised NAICS' AS naics_description")
        build_code_projection(db)
        assert db.execute('SELECT psc_desc FROM dashboard_psc').fetchone()[0]=='Revised aircraft'
        assert db.execute('SELECT naics_description FROM dashboard_naics').fetchone()[0]=='Revised NAICS'
    finally:db.close()
