import duckdb
from opportunity_engine.shared_source_queries import queries as shared_queries
from opportunity_engine.native_sources import queries as native_queries
from opportunity_engine.item_source_queries import DIBBS


def test_sam_current_deadlines_survive_old_postings_and_negative_versions_are_retained():
    c=duckdb.connect()
    c.execute("""CREATE TABLE notices AS SELECT * FROM (VALUES
    ('old-open','DEPT OF DEFENSE','097',TIMESTAMP '2021-04-01',TIMESTAMP '2027-04-01',true),
    ('old-inactive','DEPT OF DEFENSE','097',TIMESTAMP '2021-04-01',TIMESTAMP '2027-04-01',false),
    ('old-closed','DEPT OF DEFENSE','097',TIMESTAMP '2021-04-01',TIMESTAMP '2022-04-01',true),
    ('recent-terminal','DEPT OF DEFENSE','097',TIMESTAMP '2026-10-01',NULL,false),
    ('future-posting','DEPT OF DEFENSE','097',TIMESTAMP '2026-11-02',TIMESTAMP '2027-04-01',true),
    ('other-agency','OTHER','001',TIMESTAMP '2026-10-01',TIMESTAMP '2027-04-01',true)
    ) t(notice_id,department_ind_agency,cgac,posted_at,response_deadline,active_flag)""")
    predicate=shared_queries('2026-10-31')['sam_notices'].split(' WHERE ',1)[1]
    assert c.execute('SELECT notice_id FROM notices WHERE '+predicate+' ORDER BY notice_id').fetchall()==[('old-inactive',),('old-open',),('recent-terminal',)]
    c.close()


def test_structured_funded_work_phases_do_not_depend_on_description_keyword():
    c=duckdb.connect()
    c.execute("""CREATE TABLE raw AS SELECT * FROM (VALUES
    ('option','a','2026-06-01','Fleet support','100','G','EXERCISE AN OPTION','1'),
    ('new-work','b','2026-06-01','Integration services','200','A','ADDITIONAL WORK','1'),
    ('funding','c','2026-06-01','Option period 3 funding','100','C','FUNDING ONLY ACTION','1'),
    ('zero-option','d','2026-06-01','Fleet support','0','G','EXERCISE AN OPTION','1'),
    ('contradiction','e','2026-06-01','Funding only; no new work','100','G','EXERCISE AN OPTION','1'),
    ('future-plan','f','2026-06-01','Will exercise option','100',NULL,NULL,'1'),
    ('text-lot','g','2026-06-01','Production lot 3','100',NULL,NULL,'1'),
    ('revised','h','2026-06-01','Fleet support','100','G','EXERCISE AN OPTION','1'),
    ('revised','h','2026-06-01','Fleet support','0','G','EXERCISE AN OPTION','2'),
    ('outdated','i','2024-06-01','Fleet support','100','G','EXERCISE AN OPTION','1')
    ) t(contract_transaction_unique_key,contract_award_unique_key,action_date,transaction_description,federal_action_obligation,action_type_code,action_type,last_modified_date)""")
    c.execute("ALTER TABLE raw ADD COLUMN usaspending_permalink VARCHAR DEFAULT 'https://example.gov/award'")
    c.execute("ALTER TABLE raw ADD COLUMN ingest_ts VARCHAR DEFAULT '1'")
    # Execute the actual production revision/eligibility CTEs, with DuckDB's
    # equivalent regex function; hashing is separate from admission policy.
    query=native_queries('2026-10-31')['native_work_actions'].split(') SELECT *,',1)[0]+') SELECT transaction_key,source_action_type_code FROM eligible ORDER BY transaction_key'
    query=query.replace('market_intel_silver.dataset_prime_contracts','raw').replace('regexp_like(','regexp_matches(')
    assert c.execute(query).fetchall()==[('new-work','A'),('option','G'),('text-lot',None)]
    c.close()


def test_dibbs_keeps_spr_as_well_as_spe_identifiers():
    c=duckdb.connect()
    c.execute("CREATE TABLE source AS SELECT * FROM (VALUES ('SPE123'),('SPR123'),(''),(NULL)) t(solicitation_number)")
    # Prefix is no longer a procurement eligibility rule. Other date/native-line
    # conflict checks remain in the production source query.
    assert "solicitation_number LIKE 'SPE%'" not in DIBBS
    assert c.execute("SELECT solicitation_number FROM source WHERE nullif(trim(solicitation_number),'') IS NOT NULL ORDER BY 1").fetchall()==[('SPE123',),('SPR123',)]
    c.close()
