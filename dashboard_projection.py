"""Release-bound, disk-backed reads for the interactive programme dashboard.

Built once before readiness, using the existing writer and memory/spill limits.
Research keeps its native relations. Reviewed multi-programme associations are
expanded for browsing only; they are never an exclusive allocation of funding.
"""
import json
import logging
import re
import time


def build_action_projection(conn, reference):
    labels = {r['canonical_entity_id']: r['canonical_label'].upper() for r in reference.registry} if reference else {}
    decisions = []
    for row in reference.rows if reference else []:
        targets = sorted({labels[eid] for eid in reference.targets(row)})
        # A null target suppresses a reviewed clear/hold's old native label.
        decisions.extend({'award_key': row['award_key'], 'programme_key': target}
                         for target in targets or [None])
    conn.execute('''CREATE OR REPLACE TEMP TABLE dashboard_decisions AS
        SELECT json_extract_string(value,'$.award_key') AS award_key,
               json_extract_string(value,'$.programme_key') AS programme_key FROM json_each(?)''',
        [json.dumps(decisions)])
    conn.execute('''CREATE OR REPLACE TABLE dashboard_actions AS
        SELECT COALESCE(d.programme_key,UPPER(TRIM(t.platform_family))) AS programme_key,
               t.award_key,t.transaction_key,t.source_system,t.contract_id,t.year,
               t.vendor_name,t.vendor_cage,t.spend_amount,t.action_date,t.description,t.naics_code,
               t.sub_agency,t.parent_agency,t.market_segment,t.psc
        FROM v_transactions t LEFT JOIN dashboard_decisions d ON t.award_key=d.award_key
        WHERE (d.award_key IS NULL AND NULLIF(TRIM(t.platform_family),'') IS NOT NULL)
           OR d.programme_key IS NOT NULL
        ORDER BY programme_key,year,action_date''')
    conn.execute('DROP TABLE dashboard_decisions')
    # Clustered storage gives zone-map pruning even for large programmes; ART
    # handles the much more common small selective programme reads.
    conn.execute('CREATE INDEX dashboard_actions_programme ON dashboard_actions(programme_key)')


def build_subcontract_projection(conn):
    conn.execute('''CREATE OR REPLACE TABLE dashboard_network AS
        SELECT UPPER(TRIM(platform_family)) AS programme_key,n.* FROM v_network n
        WHERE NULLIF(TRIM(platform_family),'') IS NOT NULL ORDER BY programme_key,year''')
    conn.execute('CREATE INDEX dashboard_network_programme ON dashboard_network(programme_key)')
    conn.execute('''CREATE OR REPLACE TABLE dashboard_award_buyers AS
        SELECT DISTINCT t.award_key,UPPER(TRIM(t.sub_agency)) AS sub_agency,
               UPPER(TRIM(t.parent_agency)) AS parent_agency
        FROM v_transactions t SEMI JOIN dashboard_network n ON t.award_key=n.award_key
        ORDER BY t.award_key''')
    conn.execute('CREATE INDEX dashboard_award_buyers_key ON dashboard_award_buyers(award_key)')
    build_code_projection(conn)


def build_code_projection(conn):
    conn.execute('''CREATE OR REPLACE TABLE dashboard_psc AS
        SELECT CAST(psc_code AS VARCHAR) AS psc_code,MAX(psc_description) AS psc_desc
        FROM v_summary GROUP BY 1''')
    conn.execute('''CREATE OR REPLACE TABLE dashboard_naics AS
        SELECT CAST(naics_code AS VARCHAR) AS naics_code,MAX(naics_description) AS naics_description
        FROM v_summary GROUP BY 1''')

def build_explorer_projection(conn):
    # Prepare only Explorer's native fields, at the unchanged award grain.
    # Avoid sorting wide descriptions during startup. Cast annual measures once
    # using the exact same types/TRY_CAST semantics as the request builder.
    relations = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    if 'v_contracts_rolled' not in relations:
        return
    columns = [row[0] for row in conn.execute('DESCRIBE v_contracts_rolled').fetchall()]
    if 'market_segment' not in columns:
        return
    native_fields = {
        'award_key','contract_id','vendor_name','vendor_cage','parent_agency','sub_agency',
        'city','state','country','place_of_performance_city','place_of_performance_state',
        'place_of_performance_country','place_of_performance_zip','market_segment',
        'platform_family','psc','naics_code','naics_description','nsn','niin',
        'nsn_source_system','nsn_derivation_method','nsn_resolution_status','location_quality',
        'description','base_award_description','latest_action_description',
    }
    selected = []
    for column in columns:
        quoted = '"' + column.replace('"','""') + '"'
        annual = re.fullmatch(r'(obligations|action_count|earliest_action_date|latest_action_date)_fy[0-9]{4}',column)
        if annual:
            dtype = 'DOUBLE' if annual[1] == 'obligations' else 'BIGINT' if annual[1] == 'action_count' else 'DATE'
            selected.append(f'TRY_CAST({quoted} AS {dtype}) AS {quoted}')
        elif column in native_fields:
            selected.append(quoted)
    selected.append('UPPER(TRIM(CAST(market_segment AS VARCHAR))) AS _explorer_market_domain')
    started = time.monotonic()
    logger = logging.getLogger('mimir-api')
    logger.info('EXPLORER PROJECTION BUILD fields=%s',len(selected))
    conn.execute('CREATE OR REPLACE TABLE dashboard_explorer_awards AS SELECT '
                 + ','.join(selected) + ' FROM v_contracts_rolled')
    logger.info('EXPLORER PROJECTION READY rows=%s seconds=%.3f',
                conn.execute('SELECT COUNT(*) FROM dashboard_explorer_awards').fetchone()[0],
                time.monotonic()-started)


def financial_sql(floor_relation="dashboard_cache_floor", include_psc=True):
    """Native item evidence, with history only before the current action floor."""
    sql = '''SELECT LPAD(COALESCE(NULLIF(TRIM(CAST(t.niin AS VARCHAR)),''),
                   RIGHT(REGEXP_REPLACE(COALESCE(t.nsn,''),'[^0-9]','','g'),9)),9,'0') AS niin,
               TRY_CAST(t.year AS INTEGER) AS fiscal_year,
               UPPER(TRIM(t.vendor_cage)) AS cage,t.vendor_name AS vendor,
               UPPER(TRIM(t.sub_agency)) AS sub_agency,UPPER(TRIM(t.parent_agency)) AS parent_agency,
               t.market_segment,TRY_CAST(t.spend_amount AS DOUBLE) AS line_value,
               CAST(NULL AS DOUBLE) AS line_units,TRY_CAST(t.action_date AS DATE) AS last_sold,
               COALESCE(TRY_CAST(t.platform_count AS INTEGER),0) AS platform_count,
               CAST(t.psc AS VARCHAR) AS psc
        FROM v_transactions t
        WHERE NULLIF(TRIM(CAST(t.niin AS VARCHAR)),'') IS NOT NULL
           OR NULLIF(TRIM(CAST(t.nsn AS VARCHAR)),'') IS NOT NULL
        UNION ALL
        SELECT LPAD(TRIM(CAST(s.niin AS VARCHAR)),9,'0'),TRY_CAST(s.year AS INTEGER),
               UPPER(TRIM(s.cage)),s.vendor,UPPER(TRIM(s.sub_agency)),UPPER(TRIM(s.parent_agency)),
               s.market_segment,TRY_CAST(s.total_revenue AS DOUBLE),TRY_CAST(s.total_units_sold AS DOUBLE),
               TRY_CAST(s.last_sold AS DATE),COALESCE(TRY_CAST(s.platform_count AS INTEGER),0),CAST(s.psc AS VARCHAR)
        FROM v_nsn_supplier_lookup s CROSS JOIN dashboard_cache_floor c
        WHERE s.year < COALESCE(c.first_fy,2201)'''
    sql = sql.replace('dashboard_cache_floor', floor_relation)
    if not include_psc:
        sql = sql.replace(',\n               CAST(t.psc AS VARCHAR) AS psc', '')
        sql = sql.replace(',CAST(s.psc AS VARCHAR)', '')
    return sql


def build_parts_projection(conn):
    conn.execute('''CREATE OR REPLACE TABLE dashboard_bom AS
        SELECT DISTINCT UPPER(TRIM(platform_family)) AS programme_key,
               LPAD(TRIM(CAST(niin AS VARCHAR)),9,'0') AS niin FROM v_platform_bom
        ORDER BY programme_key,niin''')
    conn.execute('CREATE INDEX dashboard_bom_programme ON dashboard_bom(programme_key)')
    conn.execute('''CREATE OR REPLACE TABLE dashboard_cache_floor AS
        SELECT MIN(TRY_CAST(year AS INTEGER)) AS first_fy FROM v_transactions''')
    conn.execute('''CREATE OR REPLACE TABLE dashboard_item_financial AS
        WITH financial AS (''' + financial_sql() + ''')
        SELECT f.niin,fiscal_year,cage,vendor,sub_agency,parent_agency,market_segment,psc,
               SUM(COALESCE(line_value,0)) AS line_value,SUM(line_units) AS line_units,
               COUNT(*) AS financial_record_count,COUNT(line_units) AS unit_record_count,
               MAX(last_sold) AS last_sold,MAX(platform_count) AS platform_count
        FROM financial f SEMI JOIN dashboard_bom b ON f.niin=b.niin
        GROUP BY 1,2,3,4,5,6,7,8 ORDER BY f.niin,fiscal_year''')
    conn.execute('CREATE INDEX dashboard_item_financial_niin ON dashboard_item_financial(niin)')
    conn.execute('''CREATE OR REPLACE TABLE dashboard_item_profiles AS
        SELECT LPAD(TRIM(CAST(p.niin AS VARCHAR)),9,'0') AS niin,
               MAX(nsn) AS nsn,MAX(item_name) AS description,MAX(fsc_code) AS fsc_code
        FROM v_nsn_profile_lookup p
        SEMI JOIN dashboard_bom b ON LPAD(TRIM(CAST(p.niin AS VARCHAR)),9,'0')=b.niin
        GROUP BY 1 ORDER BY 1''')
    conn.execute('CREATE INDEX dashboard_item_profiles_niin ON dashboard_item_profiles(niin)')


def build_dashboard_projection(conn, reference, source_release):
    started = time.monotonic()
    conn.execute('BEGIN TRANSACTION')
    try:
        build_action_projection(conn, reference)
        build_subcontract_projection(conn)
        build_parts_projection(conn)
        build_explorer_projection(conn)
        conn.execute('''CREATE OR REPLACE TABLE dashboard_projection_release AS
            SELECT ? AS source_release,? AS mapping_release''',
            [source_release, getattr(reference, 'manifest', {}).get('release_id') if reference else None])
        counts = {table: conn.execute('SELECT COUNT(*) FROM '+table).fetchone()[0]
                  for table in ('dashboard_actions','dashboard_network','dashboard_bom','dashboard_item_financial')}
        conn.execute('COMMIT')
    except BaseException:
        conn.execute('ROLLBACK')
        raise
    return {'source_release': source_release, 'mapping_release': getattr(reference, 'manifest', {}).get('release_id') if reference else None,
            'rows': counts, 'build_seconds': round(time.monotonic()-started,3)}
