"""Refresh the live authenticated opportunity service with atomic release gates."""
import argparse,datetime,json,sys,traceback
from opportunity_engine.aws import AWS,PREFIX
from opportunity_engine.__main__ import main
p=argparse.ArgumentParser(add_help=False);p.add_argument('--opportunity-refresh-mode',choices=['native','rebuild'],default='native');p.add_argument('--opportunity-as-of');p.add_argument('--opportunity-source-bindings-key');o,_=p.parse_known_args()
a=AWS();report=dict(started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),status='running',scope='live authenticated opportunity intelligence',refresh_mode=o.opportunity_refresh_mode);a.put_json(PREFIX+'/operations/last_run.json',report)
try:
 sys.argv=['opportunity-engine','--from-current','--workdir','/tmp/mimir-opportunity-engine','--publish']+(['--refresh'] if o.opportunity_refresh_mode=='native' else [])+(['--as-of',o.opportunity_as_of] if o.opportunity_as_of else [])+(['--source-bindings-key',o.opportunity_source_bindings_key] if o.opportunity_source_bindings_key else []);main();report['status']='succeeded'
except Exception as e:report.update(status='failed_preserved_previous_current_pointer',error_type=type(e).__name__,error=str(e));traceback.print_exc();raise
finally:report['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();a.put_json(PREFIX+'/operations/last_run.json',report)
