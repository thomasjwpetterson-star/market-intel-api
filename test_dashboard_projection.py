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


def test_cold_website_reads_keep_v7_overrides_and_need_no_native_scan(dashboard,monkeypatch):
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
    def prepared_actions(conn):
        assert conn.execute("SELECT current_setting('threads')").fetchone()[0]==2
        return original(conn)
    monkeypatch.setattr(dashboard_projection,'build_explorer_action_projection',prepared_actions)
    metadata = build_dashboard_projection(db,reference_for(None),'release-A')
    assert db.execute("SELECT current_setting('threads')").fetchone()[0]==1
    assert metadata['rows']['dashboard_actions']==6
    ns['GLOBAL_CACHE']['dashboard_projection_ready']=True
    for table in ('v_transactions','v_network','v_summary','v_platform_bom','v_nsn_supplier_lookup','v_nsn_profile_lookup'):
        db.execute('DROP TABLE '+table)
    assert ns['get_platform_subcontracts']('VIRGINIA CLASS (SSN 774)',years=[2025],agency='Navy',mode='lines')==expected_lines
    for scope, results in zip(scopes,expected):
        assert {fn:ns[fn](**scope) for fn in results}==results, scope


def test_projection_rebuild_is_atomic_and_release_bound(dashboard):
    from private_platform_reference import reference_for
    db,ns=dashboard;complete_sources(db);ref=reference_for(None)
    db.execute('SET threads=1')
    build_dashboard_projection(db,ref,'release-A')
    original=db.execute('SELECT SUM(spend_amount) FROM dashboard_actions').fetchone()[0]
    db.execute('UPDATE v_transactions SET spend_amount=0')
    db.execute('DROP TABLE v_nsn_profile_lookup')
    with pytest.raises(Exception):build_dashboard_projection(db,ref,'release-B')
    assert db.execute("SELECT current_setting('threads')").fetchone()[0]==1
    assert db.execute('SELECT source_release FROM dashboard_projection_release').fetchone()[0]=='release-A'
    assert db.execute('SELECT SUM(spend_amount) FROM dashboard_actions').fetchone()[0]==original
    db.execute("CREATE TABLE v_nsn_profile_lookup AS SELECT '000000001' AS niin,'1234000000001' AS nsn,'ITEM' AS item_name,'1234' AS fsc_code")
    build_dashboard_projection(db,ref,'release-B')
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
