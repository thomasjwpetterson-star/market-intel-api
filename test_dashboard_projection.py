"""Prepared reads must equal native reads, including after the sources are gone."""
import copy
import itertools

import pytest
from dashboard_projection import build_dashboard_projection, build_parts_projection
from programme_parts import build_parts_query
from test_platform_dashboard_reliability import dashboard
import test_programme_parts


def read_parts(db, **kwargs):
    sql, params = build_parts_query('AMRAAM', **kwargs)
    cursor = db.execute(sql, params)
    return [dict(zip([c[0] for c in cursor.description], row)) for row in cursor.fetchall()]


def test_prepared_item_statistics_match_native_rows_counts_exposure_and_filters():
    fixture = test_programme_parts.BuyerItemFunding(); fixture.setUp()
    try:
        db = fixture.db
        for table in ('v_transactions', 'v_nsn_supplier_lookup'):
            db.execute('ALTER TABLE '+table+' ADD COLUMN psc DOUBLE')
            db.execute('UPDATE '+table+' SET psc=1510')
        build_parts_projection(db)
        cases = [dict(zip(('agency','years','domain','psc','include_zero'), values))
                 for values in itertools.product([None,'Navy','DoD','DLA','Air Force'],
                      [None,[2020],[2024,2025],[1900]], [None,'AIR','OTHER'], [None,'1510','9999'], [True,False])]
        for scope in cases:
            for mode in ({}, {'count_only':True}, {'shared_exposure':True}):
                native = read_parts(db, **scope, **mode)
                prepared = read_parts(db, prepared=True, **scope, **mode)
                assert prepared == native, (scope,mode,native,prepared)
        expected = read_parts(db, prepared=True, agency='Navy',years=[2025])
        for table in ('v_transactions','v_nsn_supplier_lookup','v_nsn_profile_lookup','v_platform_bom'):
            db.execute('DROP TABLE '+table)
        assert read_parts(db,prepared=True,agency='Navy',years=[2025]) == expected
    finally:
        fixture.doCleanups()


def complete_sources(db):
    # Complete production schema for the real-SQL route fixture.
    for name, kind in [('niin','VARCHAR'),('nsn','VARCHAR'),('platform_count','INTEGER')]:
        db.execute(f'ALTER TABLE v_transactions ADD COLUMN {name} {kind}')
    db.execute("UPDATE v_transactions SET niin='000000001',nsn='1234000000001',platform_count=2")
    db.execute("CREATE TABLE v_platform_bom AS SELECT '000000001' AS niin,'Virginia Class (SSN 774)' AS platform_family")
    db.execute("CREATE TABLE v_nsn_profile_lookup AS SELECT '000000001' AS niin,'1234000000001' AS nsn,'ITEM' AS item_name,'1234' AS fsc_code")
    db.execute('''CREATE TABLE v_nsn_supplier_lookup AS SELECT niin,year,vendor_cage AS cage,vendor_name AS vendor,
        sub_agency,parent_agency,market_segment,spend_amount AS total_revenue,0.0 AS total_units_sold,
        action_date AS last_sold,platform_count,psc FROM v_transactions WHERE FALSE''')


@pytest.mark.parametrize('initial_build',[False,True])
def test_cold_website_reads_keep_v7_overrides_and_need_no_native_scan(dashboard,monkeypatch,initial_build):
    from private_platform_reference import reference_for
    import dashboard_projection
    db, ns = dashboard
    complete_sources(db)
    scopes = [dict(name='VIRGINIA CLASS (SSN 774)', years=year, agency=buyer, domain=domain, psc=psc)
              for year,buyer,domain,psc in itertools.product([[2025],[2024],None],['Navy','DoD',None],['AIR',None],['1510',None])]
    expected = []
    for scope in scopes:
        expected.append({fn: ns[fn](**scope) for fn in ('get_platform_profile','get_platform_contractors','get_platform_awards','get_platform_subcontracts')})
    expected_lines=ns['get_platform_subcontracts']('VIRGINIA CLASS (SSN 774)',years=[2025],agency='Navy',mode='lines')
    db.execute('SET threads=1')
    original=dashboard_projection.build_explorer_action_projection
    def prepared_actions(conn, **kwargs):
        assert conn.execute("SELECT current_setting('threads')").fetchone()[0]==2
        return original(conn, **kwargs)
    monkeypatch.setattr(dashboard_projection,'build_explorer_action_projection',prepared_actions)
    metadata = build_dashboard_projection(db,reference_for(None),'release-A',preparation_threads=2,
                                          initial_build=initial_build)
    assert db.execute("SELECT current_setting('threads')").fetchone()[0]==1
    assert metadata['rows']['dashboard_actions']==6
    ns['GLOBAL_CACHE']['dashboard_projection_ready']=True
    for table in ('v_transactions','v_network','v_summary','v_platform_bom','v_nsn_supplier_lookup','v_nsn_profile_lookup'):
        db.execute('DROP TABLE '+table)
    assert ns['get_platform_subcontracts']('VIRGINIA CLASS (SSN 774)',years=[2025],agency='Navy',mode='lines')==expected_lines
    for scope, results in zip(scopes,expected):
        assert {fn:ns[fn](**scope) for fn in results}==results, scope


def test_failed_initial_preparation_keeps_readiness_and_release_marker_unpublished(dashboard,monkeypatch):
    from private_platform_reference import reference_for
    import dashboard_projection
    db,ns=dashboard;complete_sources(db)
    db.execute('SET threads=1')
    original=dashboard_projection.build_explorer_action_projection
    def fail_after_actions(conn, **kwargs):
        original(conn, **kwargs)
        raise RuntimeError('Incomplete initial action preparation')
    monkeypatch.setattr(dashboard_projection,'build_explorer_action_projection',fail_after_actions)
    with pytest.raises(RuntimeError,match='Incomplete initial action'):
        build_dashboard_projection(db,reference_for(None),'release-A',preparation_threads=2,initial_build=True)
    relations={r[0] for r in db.execute('SHOW TABLES').fetchall()}
    assert 'dashboard_actions' in relations # Prior stage was checkpointed.
    assert 'dashboard_explorer_actions' not in relations # Failed stage rolled back.
    assert 'dashboard_projection_release' not in relations
    assert not ns['GLOBAL_CACHE'].get('dashboard_projection_ready')
    assert db.execute("SELECT current_setting('threads')").fetchone()[0]==1
    monkeypatch.setattr(dashboard_projection,'build_explorer_action_projection',original)
    build_dashboard_projection(db,reference_for(None),'release-A',preparation_threads=2,initial_build=True)
    assert db.execute('SELECT source_release FROM dashboard_projection_release').fetchone()[0]=='release-A'


def test_projection_rebuild_is_atomic_and_release_bound(dashboard,monkeypatch):
    from private_platform_reference import reference_for
    import dashboard_projection
    db,ns=dashboard;complete_sources(db);ref=reference_for(None)
    db.execute('SET threads=1')
    observed=[]
    original_builder=dashboard_projection.build_action_projection
    def actions(conn,reference):
        observed.append(conn.execute("SELECT current_setting('threads')").fetchone()[0])
        return original_builder(conn,reference)
    monkeypatch.setattr(dashboard_projection,'build_action_projection',actions)
    build_dashboard_projection(db,ref,'release-A',preparation_threads=2)
    original=db.execute('SELECT SUM(spend_amount) FROM dashboard_actions').fetchone()[0]
    db.execute('UPDATE v_transactions SET spend_amount=0')
    db.execute('DROP TABLE v_nsn_profile_lookup')
    with pytest.raises(Exception):build_dashboard_projection(db,ref,'release-B',preparation_threads=2)
    assert db.execute("SELECT current_setting('threads')").fetchone()[0]==1
    assert db.execute('SELECT source_release FROM dashboard_projection_release').fetchone()[0]=='release-A'
    assert db.execute('SELECT SUM(spend_amount) FROM dashboard_actions').fetchone()[0]==original
    db.execute("CREATE TABLE v_nsn_profile_lookup AS SELECT '000000001' AS niin,'1234000000001' AS nsn,'ITEM' AS item_name,'1234' AS fsc_code")
    build_dashboard_projection(db,ref,'release-B')
    assert observed==[2,2,1]
    assert db.execute('SELECT source_release FROM dashboard_projection_release').fetchone()[0]=='release-B'
    assert db.execute('SELECT SUM(spend_amount) FROM dashboard_actions').fetchone()[0]==0


def test_reviewed_multi_programme_targets_do_not_duplicate_within_a_programme(dashboard):
    from private_platform_reference import reference_for
    db,ns=dashboard;complete_sources(db);ref=copy.deepcopy(reference_for(None))
    ref.registry.append({'canonical_entity_id':'OTHER','canonical_label':'Other Programme'})
    ref.rows[0]['target_entity_ids']='VIRGINIA|OTHER|VIRGINIA'
    build_dashboard_projection(db,ref,'multi')
    assert db.execute("SELECT COUNT(*) FROM dashboard_actions WHERE award_key='B'").fetchone()[0]==2
    assert db.execute("SELECT SUM(spend_amount) FROM dashboard_actions WHERE programme_key='OTHER PROGRAMME'").fetchone()[0]==50
    assert db.execute("SELECT COUNT(*) FROM dashboard_actions WHERE award_key='C'").fetchone()[0]==0


def test_committed_physical_identities_survive_checkpoint_reopen_and_atomic_refresh(dashboard,tmp_path,monkeypatch):
    import duckdb
    import dashboard_projection
    from private_platform_reference import reference_for
    seed,_=dashboard;complete_sources(seed)
    seed.execute('''CREATE TABLE v_contracts_rolled AS SELECT award_key,MAX(contract_id) AS contract_id,
        MAX(vendor_cage) AS vendor_cage,MAX(market_segment) AS market_segment,
        SUM(spend_amount) AS obligations_fy2025,COUNT(*) AS action_count_fy2025
        FROM v_transactions GROUP BY award_key''')
    path=str(tmp_path/'committed.duckdb')
    db=duckdb.connect(path)
    try:
        for (name,) in seed.execute('SHOW TABLES').fetchall():
            frame=seed.execute('SELECT * FROM '+name).fetchdf()
            db.register('native_seed',frame)
            db.execute('CREATE TABLE '+name+' AS SELECT * FROM native_seed')
            db.unregister('native_seed')
        db.execute('SET threads=1')
        build_dashboard_projection(db,reference_for(None),'release-A',preparation_threads=2,initial_build=True)
        expected={name:db.execute('SELECT * FROM '+name+' ORDER BY 1').fetchall()
                  for name in ('dashboard_explorer_actions','dashboard_explorer_rank')}
        db.execute('CHECKPOINT');db.close();db=duckdb.connect(path)
        for name,rows in expected.items():
            assert db.execute('SELECT * FROM '+name+' ORDER BY 1').fetchall()==rows
        for name,identity in [('actions','_explorer_action_row_id'),('awards','_explorer_row_id')]:
            assert db.execute('SELECT table_type FROM information_schema.tables WHERE table_name=?',['dashboard_explorer_'+name]).fetchone()[0]=='VIEW'
            assert db.execute(f'SELECT COUNT(*) FROM dashboard_explorer_{name} a JOIN dashboard_explorer_{name}_data d ON a.{identity}=d.rowid').fetchone()[0]==db.execute('SELECT COUNT(*) FROM dashboard_explorer_'+name).fetchone()[0]
        assert db.execute("SELECT COUNT(*) FROM duckdb_indexes() WHERE index_name='dashboard_explorer_rank_cage'").fetchone()[0]==1
        original=dashboard_projection.build_explorer_projection
        def fail(conn,checkpoint_stage=None):
            original(conn,checkpoint_stage)
            raise RuntimeError('incomplete refresh')
        monkeypatch.setattr(dashboard_projection,'build_explorer_projection',fail)
        with pytest.raises(RuntimeError,match='incomplete refresh'):
            build_dashboard_projection(db,reference_for(None),'release-B')
        assert db.execute('SELECT source_release FROM dashboard_projection_release').fetchone()[0]=='release-A'
        assert db.execute('SELECT * FROM dashboard_explorer_actions ORDER BY 1').fetchall()==expected['dashboard_explorer_actions']
        monkeypatch.setattr(dashboard_projection,'build_explorer_projection',original)
        build_dashboard_projection(db,reference_for(None),'release-B')
        assert db.execute('SELECT source_release FROM dashboard_projection_release').fetchone()[0]=='release-B'
        assert db.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ('dashboard_explorer_actions_data','dashboard_explorer_awards_data')").fetchone()[0]==0
    finally:
        db.close()
