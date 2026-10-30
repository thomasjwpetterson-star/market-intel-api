"""Native temporal history fields remain routable in the published read model."""
import ast,hashlib
from pathlib import Path
import duckdb
import opportunity_engine.build as builder

def test_timestamp_history_is_published_as_an_evidenced_serving_copy(tmp_path):
    c=duckdb.connect();p=tmp_path/'source.parquet'
    c.execute("COPY (SELECT 'CAGE:45934' company_id,TIMESTAMP '2026-10-15 12:34:56' last_subcontract_report,12::BIGINT subcontract_reports) TO ? (FORMAT PARQUET)",[str(p)])
    c.read_parquet(str(p)).create_view('company_history_coverage')
    tables={};sources={'company_history_coverage':dict(rows=1,files=[dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest())])}
    tree=ast.parse(Path(builder.__file__).read_text())
    build=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='build')
    functions=[n for n in build.body if isinstance(n,ast.FunctionDef) and n.name in ('materialise','bind_source')]
    namespace=dict(c=c,out=tmp_path,tables=tables,sources=sources,quote=builder.quote,hashlib=hashlib)
    exec(compile(ast.fix_missing_locations(ast.Module(body=functions,type_ignores=[])),'binding_test','exec'),namespace)
    original=p.read_bytes();namespace['bind_source']('company_history_coverage')
    t=tables['company_history_coverage'];assert t['rows']==1 and not t.get('source_binding')
    assert next(x['Type'] for x in t['columns'] if x['Name']=='last_subcontract_report')=='string'
    assert c.execute('SELECT last_subcontract_report,subcontract_reports FROM read_parquet(?)',[t['path']]).fetchone()==('2026-10-15 12:34:56',12)
    assert p.read_bytes()==original
