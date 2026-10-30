"""Execute the production notice projection with a real comparator join."""
import ast,json
from pathlib import Path
import duckdb
import opportunity_engine.build as builder

def test_notice_comparable_values_are_historical_and_drillable():
    tree=ast.parse(Path(builder.__file__).read_text())
    call=next(n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='materialise' and n.args and isinstance(n.args[0],ast.Constant) and n.args[0].value=='published_calls')
    sql=ast.literal_eval(call.args[1]);c=duckdb.connect()
    fields=['notice_id','solicitation_id','aac_code','office','sub_tier','notice_hash','title','description','psc_code','naics_code','response_deadline','source_url','notice_type','posted_at','source_s3_uri','source_s3_version_id','source_sha256','selected_notice_version_id']
    c.execute('CREATE TABLE native_notice_heads('+','.join('"'+n+'" VARCHAR' for n in fields)+')')
    row=dict.fromkeys(fields);row.update(notice_id='notice',solicitation_id='FA123',aac_code='OFFICE',office='Office',sub_tier='Air Force',notice_hash='native-hash',title='Repair fuel boost pump',description='Seeking capability information',psc_code='J016',response_deadline='2026-11-30',notice_type='Sources Sought',posted_at='2026-10-15',source_sha256='raw-source-hash')
    c.execute('INSERT INTO native_notice_heads VALUES('+','.join('?' for n in fields)+')',[row[n] for n in fields])
    comparators=json.dumps([{'award_key':'historical-award','observed_net_obligations':250000,'source_url':'https://example.gov/award'}])
    c.execute('CREATE TABLE notice_comparators(notice_id VARCHAR,observed_min_usd DOUBLE,observed_median_usd DOUBLE,observed_max_usd DOUBLE,comparators_json VARCHAR)')
    c.execute('INSERT INTO notice_comparators VALUES (?,?,?,?,?)',['notice',250000,250000,250000,comparators])
    c.execute("CREATE MACRO nid(s) AS regexp_replace(upper(coalesce(s,'')),'[^A-Z0-9]','','g')");c.execute("CREATE MACRO dt(s) AS try_cast(substr(s,1,10) AS DATE)")
    q=c.execute(sql);out=dict(zip([x[0] for x in q.description],q.fetchone()))
    assert out['value_median']==250000
    assert 'Observed comparable' in out['value_scope']
    inputs=json.loads(out['value_inputs_json']);assert inputs['comparators'][0]['award_key']=='historical-award'
    assert inputs['future_demand_estimate'] is None and inputs['company_addressable_value'] is None
    assert out['record_hash']!='native-hash'
    c.execute('DELETE FROM notice_comparators');q=c.execute(sql);out=dict(zip([x[0] for x in q.description],q.fetchone()))
    assert out['value_median'] is None and out['value_basis']=='no_scoped_comparator_found'
