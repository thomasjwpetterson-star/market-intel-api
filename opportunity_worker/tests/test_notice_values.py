"""Execute the production notice projection with a real comparator join."""
import ast,json
from pathlib import Path
import duckdb
import opportunity_engine.build as builder

def test_notice_comparable_values_are_historical_and_drillable():
    tree=ast.parse(Path(builder.__file__).read_text())
    call=next(n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='materialise' and n.args and isinstance(n.args[0],ast.Constant) and n.args[0].value=='published_calls')
    sql=ast.literal_eval(call.args[1]);c=duckdb.connect()
    fields=['notice_id','solicitation_id','aac_code','office','sub_tier','notice_hash','title','description','psc_code','naics_code','response_deadline','source_url','notice_type','posted_at','source_s3_uri','source_s3_version_id','source_sha256','selected_notice_version_id','active_flag','response_deadline_raw','response_deadline_precision']
    c.execute('CREATE TABLE native_notice_heads('+','.join('"'+n+'" VARCHAR' for n in fields)+')')
    row=dict.fromkeys(fields);row.update(notice_id='notice',solicitation_id='FA123',aac_code='OFFICE',office='Office',sub_tier='Air Force',notice_hash='native-hash',title='Repair fuel boost pump',description='Seeking capability information',psc_code='J016',response_deadline='2026-11-30',notice_type='Sources Sought',posted_at='2026-10-15',source_sha256='raw-source-hash')
    c.execute('INSERT INTO native_notice_heads VALUES('+','.join('?' for n in fields)+')',[row[n] for n in fields])
    comparators=json.dumps([{'award_key':'historical-award','observed_net_obligations':250000,'source_url':'https://example.gov/award'}])
    c.execute('CREATE TABLE notice_comparators(notice_id VARCHAR,observed_min_usd DOUBLE,observed_median_usd DOUBLE,observed_max_usd DOUBLE,comparators_json VARCHAR,cohort_id VARCHAR,awards BIGINT,evidence_digest VARCHAR)')
    c.execute('INSERT INTO notice_comparators VALUES (?,?,?,?,?,?,?,?)',['notice',250000,250000,250000,comparators,'cohort-id',1,'cohort-v1'])
    c.execute("CREATE MACRO nid(s) AS regexp_replace(upper(coalesce(s,'')),'[^A-Z0-9]','','g')");c.execute("CREATE MACRO dt(s) AS try_cast(substr(s,1,10) AS DATE)")
    q=c.execute(sql);out=dict(zip([x[0] for x in q.description],q.fetchone()))
    assert out['value_median']==250000
    assert 'Observed comparable' in out['value_scope']
    inputs=json.loads(out['value_inputs_json']);assert inputs['comparators'][0]['award_key']=='historical-award'
    assert inputs['cohort_id']=='cohort-id' and inputs['comparable_awards']==1
    assert inputs['future_demand_estimate'] is None and inputs['company_addressable_value'] is None
    assert out['record_hash']!='native-hash'
    previous_hash=out['record_hash'];c.execute("UPDATE notice_comparators SET evidence_digest='cohort-v2'");q=c.execute(sql);updated=dict(zip([x[0] for x in q.description],q.fetchone()));assert updated['record_hash']!=previous_hash
    c.execute('DELETE FROM notice_comparators');q=c.execute(sql);out=dict(zip([x[0] for x in q.description],q.fetchone()))
    assert out['value_median'] is None and out['value_basis']=='no_scoped_comparator_found'

    # The native UTC clock and original offset survive into every opportunity;
    # an inactive notice remains evidence without claiming it is cancelled.
    c.execute("UPDATE native_notice_heads SET response_deadline='2026-11-03 00:00:00',response_deadline_raw='2026-11-02T14:00:00-10:00',response_deadline_precision='timestamp_utc',active_flag='false'")
    q=c.execute(sql);out=dict(zip([x[0] for x in q.description],q.fetchone()));inputs=json.loads(out['value_inputs_json'])
    assert inputs['source_response_deadline_utc']=='2026-11-03T00:00:00Z'
    assert inputs['source_response_deadline_raw']=='2026-11-02T14:00:00-10:00'
    assert out['procurement_state']=='source_inactive'
    assert out['reconciled_state']=='source_not_currently_active'
    c.execute("UPDATE native_notice_heads SET response_deadline='2026-11-03',response_deadline_raw='2026-11-03',response_deadline_precision='date_only',active_flag='true'")
    q=c.execute(sql);out=dict(zip([x[0] for x in q.description],q.fetchone()));inputs=json.loads(out['value_inputs_json'])
    assert inputs['source_response_deadline_utc'] is None
    assert inputs['source_response_deadline_precision']=='date_only'
    assert out['procurement_state']=='procurement_published'
