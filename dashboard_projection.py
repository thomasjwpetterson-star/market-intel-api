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

def build_item_filter_projection(conn):
    """Normalize distinct native item keys once per serving generation.

    Keep the raw value alongside its normalized suffix. Queries can then join
    native values without applying a regex to every action, while preserving
    formatted, shortened and legacy values exactly as the native fallback.
    """
    relations = {row[0] for row in conn.execute('SHOW TABLES').fetchall()}
    if 'v_transactions' not in relations:
        return
    columns = {row[0] for row in conn.execute('DESCRIBE v_transactions').fetchall()}
    for column in ('nsn','niin'):
        if column not in columns:
            continue
        started = time.monotonic()
        conn.execute(f'''CREATE OR REPLACE TABLE dashboard_explorer_{column}_filter AS
            WITH native_keys AS MATERIALIZED (
                SELECT DISTINCT CAST("{column}" AS VARCHAR) AS source_value
                FROM v_transactions WHERE "{column}" IS NOT NULL
            )
            SELECT source_value,
                LPAD(RIGHT(REGEXP_REPLACE(source_value,'[^0-9]','','g'),9),9,'0') AS niin
            FROM native_keys ORDER BY niin,source_value''')
        logging.getLogger('mimir-api').info('EXPLORER ITEM FILTER READY field=%s keys=%s seconds=%.3f',
            column,conn.execute(f'SELECT COUNT(*) FROM dashboard_explorer_{column}_filter').fetchone()[0],
            time.monotonic()-started)


def explorer_effective_award_key(columns):
    def value(column):
        return '"'+column+'"' if column in columns else 'CAST(NULL AS VARCHAR)'
    key = f'''COALESCE(NULLIF(TRIM(CAST({value('award_key')} AS VARCHAR)),''),
        CONCAT(COALESCE(NULLIF(TRIM(CAST({value('source_system')} AS VARCHAR)),''),'UNKNOWN'),
        '|PIID|',COALESCE(NULLIF(TRIM(CAST({value('contract_id')} AS VARCHAR)),''),'<NULL>'),
        '|CAGE|',COALESCE(NULLIF(TRIM(CAST({value('vendor_cage')} AS VARCHAR)),''),'<NULL>')))'''
    return key


def build_explorer_action_projection(conn):
    relations = {row[0] for row in conn.execute('SHOW TABLES').fetchall()}
    if 'v_transactions' not in relations:
        return
    columns = {row[0] for row in conn.execute('DESCRIBE v_transactions').fetchall()}
    if 'contract_id' not in columns or 'spend_amount' not in columns:
        return
    fields = {
        'award_key','source_system','contract_id','action_date','vendor_name','vendor_cage',
        'parent_agency','sub_agency','city','state','country','place_of_performance_city',
        'place_of_performance_state','place_of_performance_country','place_of_performance_zip',
        'market_segment','platform_family','psc','naics_code','description','base_award_description',
        'action_description','source_of_supply','nsn','niin','nsn_source_system',
        'nsn_derivation_method','nsn_resolution_status','location_quality','part_number','year',
        'spend_amount','award_type_code','award_type','award_type_description',
    }
    key = explorer_effective_award_key(columns)
    selected = []
    for column in sorted(columns & fields):
        fallback = (key if column == 'award_key' else
                    'UPPER(TRIM(CAST(vendor_cage AS VARCHAR)))' if column == 'vendor_cage' else '"description"')
        deduplicate = column in {'award_key','vendor_cage'} or (
            column in {'base_award_description','action_description'} and 'description' in columns)
        if deduplicate:
            # Keep a distinct value only when it differs. A separate null bit
            # distinguishes native NULL from a duplicate of the fallback text.
            # This is lossless for NULL, empty, whitespace and differing values.
            selected.append(f'CASE WHEN "{column}" IS NOT DISTINCT FROM {fallback} '
                            f'THEN NULL ELSE "{column}" END AS "{column}"')
            selected.append(f'"{column}" IS NULL AS "_explorer_{column}_is_null"')
        else:
            selected.append('"'+column+'"')
    selected.extend(['ROW_NUMBER() OVER () AS _explorer_action_row_id',key+' AS _explorer_award_key',
                     'CAST(hash('+key+') % 4294967296 AS UINTEGER) AS _explorer_award_hash'])
    if 'vendor_cage' in columns:
        selected.append('UPPER(TRIM(CAST(vendor_cage AS VARCHAR))) AS _explorer_vendor_cage')
    started = time.monotonic()
    logging.getLogger('mimir-api').info('EXPLORER ACTION PROJECTION BUILD fields=%s',len(selected))
    conn.execute('CREATE OR REPLACE TABLE dashboard_explorer_actions AS SELECT '
                 + ','.join(selected) + ' FROM v_transactions')
    logging.getLogger('mimir-api').info('EXPLORER ACTION PROJECTION READY rows=%s seconds=%.3f',
        conn.execute('SELECT COUNT(*) FROM dashboard_explorer_actions').fetchone()[0],time.monotonic()-started)


def build_explorer_projection(conn, checkpoint_stage=None):
    build_item_filter_projection(conn)
    build_explorer_action_projection(conn)
    if checkpoint_stage:
        checkpoint_stage('native_actions')
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
            if column == 'vendor_cage' or (column in {'base_award_description','latest_action_description'} and 'description' in columns):
                fallback = 'UPPER(TRIM(CAST(vendor_cage AS VARCHAR)))' if column == 'vendor_cage' else '"description"'
                selected.append(f'CASE WHEN {quoted} IS NOT DISTINCT FROM {fallback} '
                                f'THEN NULL ELSE {quoted} END AS {quoted}')
                selected.append(f'{quoted} IS NULL AS "_explorer_{column}_is_null"')
            else:
                selected.append(quoted)
    # Persist the identity as data. DuckDB's virtual rowid can change when the
    # surrounding readiness transaction commits newly created storage.
    selected.append('ROW_NUMBER() OVER () AS _explorer_row_id')
    if 'vendor_cage' in columns:
        selected.append('UPPER(TRIM(CAST(vendor_cage AS VARCHAR))) AS _explorer_vendor_cage')
    selected.append('UPPER(TRIM(CAST(market_segment AS VARCHAR))) AS _explorer_market_domain')
    totals = [f'COALESCE(TRY_CAST("{column}" AS DOUBLE),0.0)' for column in sorted(columns)
              if re.fullmatch(r'obligations_fy[0-9]{4}',column)]
    selected.append('(' + (' + '.join(totals) or '0.0') + ') AS _explorer_all_year_obligations')
    started = time.monotonic()
    logger = logging.getLogger('mimir-api')
    logger.info('EXPLORER PROJECTION BUILD fields=%s',len(selected))
    conn.execute('CREATE OR REPLACE TABLE dashboard_explorer_awards AS SELECT '
                 + ','.join(selected) + ' FROM v_contracts_rolled')
    logger.info('EXPLORER PROJECTION READY rows=%s seconds=%.3f',
                conn.execute('SELECT COUNT(*) FROM dashboard_explorer_awards').fetchone()[0],
                time.monotonic()-started)
    if checkpoint_stage:
        checkpoint_stage('native_awards')
    # Append one domain at a time. This clusters a compact ranking lane without
    # an external wide sort (and its unbounded temporary-storage requirement).
    rank_started = time.monotonic()
    rank_fields = {
        'contract_id','vendor_name','vendor_cage','parent_agency','sub_agency',
        'city','state','country','place_of_performance_city','place_of_performance_state',
        'place_of_performance_country','place_of_performance_zip','market_segment',
        'platform_family','psc','naics_code',
    }
    rank_columns = [column for column in columns if column in rank_fields or
        re.fullmatch(r'(obligations|action_count)_fy[0-9]{4}',column)]
    rank_columns += ['_explorer_market_domain','_explorer_all_year_obligations']
    if 'vendor_cage' in columns:
        rank_columns.append('_explorer_vendor_cage')
    rank_select = '_explorer_row_id,' + ','.join('"'+c.replace('"','""')+'"' for c in rank_columns)
    conn.execute('CREATE OR REPLACE TABLE dashboard_explorer_rank AS SELECT '
                 + rank_select + ' FROM dashboard_explorer_awards WHERE FALSE')
    domains = conn.execute('SELECT DISTINCT _explorer_market_domain FROM dashboard_explorer_awards').fetchall()
    for (domain,) in domains:
        conn.execute('INSERT INTO dashboard_explorer_rank SELECT ' + rank_select
                     + ' FROM dashboard_explorer_awards WHERE _explorer_market_domain IS NOT DISTINCT FROM ?', [domain])
    logger.info('EXPLORER RANK READY rows=%s seconds=%.3f',
                conn.execute('SELECT COUNT(*) FROM dashboard_explorer_rank').fetchone()[0],
                time.monotonic()-rank_started)


def verify_explorer_projection(conn):
    relations = {row[0] for row in conn.execute('SHOW TABLES').fetchall()}
    if 'dashboard_explorer_actions' in relations:
        native_actions = conn.execute('SELECT COUNT(*) FROM v_transactions').fetchone()[0]
        prepared_actions = conn.execute('SELECT COUNT(*) FROM dashboard_explorer_actions').fetchone()[0]
        if native_actions != prepared_actions:
            raise RuntimeError('Explorer action rows differ from the native serving records.')
        ids = [r[0] for r in conn.execute('SELECT _explorer_action_row_id FROM dashboard_explorer_actions LIMIT 10').fetchall()]
        if ids:
            matched = conn.execute('SELECT COUNT(*) FROM dashboard_explorer_actions WHERE _explorer_action_row_id IN ('
                + ','.join(['?']*len(ids))+')',ids).fetchone()[0]
            if matched != len(ids):
                raise RuntimeError('Explorer action identities differ from the committed serving records.')
        logging.getLogger('mimir-api').info('EXPLORER ACTION IDENTITY VERIFIED rows=%s sampled=%s',prepared_actions,len(ids))
    if 'dashboard_explorer_rank' not in relations:
        return
    native_count=conn.execute('SELECT COUNT(*) FROM dashboard_explorer_awards').fetchone()[0]
    rank_count=conn.execute('SELECT COUNT(*) FROM dashboard_explorer_rank').fetchone()[0]
    if native_count!=rank_count:
        raise RuntimeError('Explorer ranking row count differs from the serving records.')
    identities=[int(row[0]) for row in conn.execute('SELECT _explorer_row_id FROM dashboard_explorer_rank LIMIT 10').fetchall()]
    if identities:
        placeholders=','.join(['?']*len(identities))
        matched=conn.execute('SELECT COUNT(*) FROM dashboard_explorer_awards WHERE _explorer_row_id IN ('+placeholders+')',identities).fetchone()[0]
        if matched!=len(identities):
            raise RuntimeError('Explorer ranking identities differ from the committed serving records.')
    logging.getLogger('mimir-api').info('EXPLORER IDENTITY VERIFIED rows=%s sampled=%s',native_count,len(identities))


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


def build_dashboard_projection(conn, reference, source_release, preparation_threads=None,
                               initial_build=False):
    started = time.monotonic()
    runtime_threads = int(conn.execute("SELECT current_setting('threads')").fetchone()[0])
    # This replacement instance receives traffic only after readiness. Use the
    # existing two CPUs for preparation, then restore the serving query limit.
    build_threads = runtime_threads if preparation_threads is None else int(preparation_threads)
    conn.execute('SET threads='+str(build_threads))
    logging.getLogger('mimir-api').info('DASHBOARD PREPARATION threads=%s runtime_threads=%s',build_threads,runtime_threads)
    transaction_started = False
    def checkpoint_stage(stage):
        nonlocal transaction_started
        # Only a replacement instance that has never served may publish these
        # intermediate tables. Checkpoint compressed storage before preparing
        # the next large relation, rather than retaining every relation's
        # uncommitted spill files together until the final release marker.
        conn.execute('COMMIT')
        transaction_started = False
        conn.execute('CHECKPOINT')
        logging.getLogger('mimir-api').info('DASHBOARD PREPARATION CHECKPOINT stage=%s',stage)
        conn.execute('BEGIN TRANSACTION')
        transaction_started = True
    try:
        conn.execute('BEGIN TRANSACTION')
        transaction_started = True
        build_action_projection(conn, reference)
        build_subcontract_projection(conn)
        build_parts_projection(conn)
        if initial_build:
            checkpoint_stage('programme_dashboard')
        build_explorer_projection(conn,checkpoint_stage if initial_build else None)
        conn.execute('''CREATE OR REPLACE TABLE dashboard_projection_release AS
            SELECT ? AS source_release,? AS mapping_release''',
            [source_release, getattr(reference, 'manifest', {}).get('release_id') if reference else None])
        counts = {table: conn.execute('SELECT COUNT(*) FROM '+table).fetchone()[0]
                  for table in ('dashboard_actions','dashboard_network','dashboard_bom','dashboard_item_financial')}
        conn.execute('COMMIT')
    except BaseException:
        if transaction_started:
            conn.execute('ROLLBACK')
        raise
    finally:
        conn.execute('SET threads='+str(runtime_threads))
        logging.getLogger('mimir-api').info('DASHBOARD SERVING threads_restored=%s',runtime_threads)
    verify_explorer_projection(conn)
    return {'source_release': source_release, 'mapping_release': getattr(reference, 'manifest', {}).get('release_id') if reference else None,
            'rows': counts, 'build_seconds': round(time.monotonic()-started,3)}
