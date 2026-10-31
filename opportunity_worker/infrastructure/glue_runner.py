"""Refresh the live authenticated opportunity service with atomic release gates."""
import argparse,datetime,json,shutil,sys,traceback
from opportunity_engine.aws import AWS,PREFIX
from opportunity_engine.__main__ import main
p=argparse.ArgumentParser(add_help=False);p.add_argument('--opportunity-refresh-mode',choices=['native','rebuild','quality','publish'],default='native');p.add_argument('--opportunity-as-of');p.add_argument('--opportunity-source-bindings-key');p.add_argument('--opportunity-resume-derived-checkpoint-key');p.add_argument('--opportunity-retain-prior-unreferenced-innovation',choices=['true','false'],default='false');o,_=p.parse_known_args()
if o.opportunity_refresh_mode=='publish' and not o.opportunity_resume_derived_checkpoint_key:p.error('Validated publication requires a checkpoint key')
disk=shutil.disk_usage('/tmp');print(json.dumps(dict(runtime_python=sys.version.split()[0],temporary_disk_total_bytes=disk.total,temporary_disk_free_bytes=disk.free)),flush=True)
a=AWS();report=dict(started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),status='running',scope='live authenticated opportunity intelligence',refresh_mode=o.opportunity_refresh_mode);a.put_json(PREFIX+'/operations/last_run.json',report)
try:
 sys.argv=['opportunity-engine','--from-current','--workdir','/tmp/mimir-opportunity-engine','--publish']+(['--refresh'] if o.opportunity_refresh_mode=='native' else ['--review-current'] if o.opportunity_refresh_mode=='quality' else [])+(['--as-of',o.opportunity_as_of] if o.opportunity_as_of else [])+(['--source-bindings-key',o.opportunity_source_bindings_key] if o.opportunity_source_bindings_key else [])+([('--publish-validated-checkpoint-key' if o.opportunity_refresh_mode=='publish' else '--resume-derived-checkpoint-key'),o.opportunity_resume_derived_checkpoint_key] if o.opportunity_resume_derived_checkpoint_key else [])+(['--retain-prior-unreferenced-innovation'] if o.opportunity_retain_prior_unreferenced_innovation=='true' else []);main();report['status']='succeeded'
except Exception as e:report.update(status='failed_preserved_previous_current_pointer',error_type=type(e).__name__,error=str(e));traceback.print_exc();raise
finally:report['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();a.put_json(PREFIX+'/operations/last_run.json',report)
