"""Rebuild/publish from portable AWS source bindings, without prototype paths."""
import argparse,datetime,hashlib,json,uuid,os,subprocess,sys
from pathlib import Path
from .aws import AWS,BUCKET,PREFIX
from .build import build
from .validate import validate,validate_demand_integrity
from .publish import publish
from .enrich import enrich
from .registry import augment
from .company_enrichment import augment as augment_companies
from .company_history import rebuild as rebuild_history
from .events import track as track_events
from .checkpoint import archive_failed_attempt
from .demand_history import rebuild as rebuild_demand,refresh_forecast_episodes
from .quality import apply as apply_quality,review_current

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--from-current',action='store_true',required=True);ap.add_argument('--workdir',required=True);ap.add_argument('--as-of',default=datetime.datetime.now(datetime.timezone.utc).date().isoformat());ap.add_argument('--publish',action='store_true');ap.add_argument('--refresh',action='store_true');ap.add_argument('--review-current',action='store_true');ap.add_argument('--source-bindings-key');args=ap.parse_args();a=AWS();root=Path(args.workdir).resolve();root.mkdir(parents=True,exist_ok=True)
 pointer=json.loads(a.s3.get_object(Bucket=BUCKET,Key=PREFIX+'/current.json')['Body'].read());key=pointer['manifest_key']
 if not key.startswith(PREFIX+'/releases/'):raise ValueError('Invalid release pointer')
 m=json.loads(a.s3.get_object(Bucket=BUCKET,Key=key)['Body'].read())
 if args.review_current:
  if args.refresh or args.source_bindings_key:raise ValueError('Quality review reuses the published source snapshot')
  out=review_current(m,root,a)
  from .catalogue_sales import augment as augment_catalogue_sales
  augment_catalogue_sales(out)
  from .item_sources import augment as augment_item_sources
  augment_item_sources(out)
  from .company_activity import refresh as refresh_activity
  refresh_activity(out,a)
  from .pipeline_coverage import build as build_coverage
  build_coverage(out)
  from .logic_provenance import record as record_logic
  record_logic(out);track_events(out,m,a);v=validate(out)
  if args.publish:publish(out)
  print(json.dumps(dict(directory=str(out),validation_passed=v['passed'],published=args.publish)));return
 def download(uri,sha,suffix='.parquet'):
  if not uri.startswith('s3://'+BUCKET+'/'+PREFIX+'/'):raise ValueError('Unexpected source namespace')
  p=root/'sources'/(sha+suffix);p.parent.mkdir(exist_ok=True)
  if not p.exists() or hashlib.sha256(p.read_bytes()).hexdigest()!=sha:a.s3.download_file(BUCKET,uri.split('/',3)[3],str(p))
  if hashlib.sha256(p.read_bytes()).hexdigest()!=sha:raise ValueError('Source checksum mismatch')
  return str(p)
 sources=m['sources']
 if args.source_bindings_key:
  if not args.source_bindings_key.startswith(PREFIX+'/sources/') or not args.source_bindings_key.endswith('/manifest.json'):raise ValueError('Invalid source recovery binding')
  recovered=json.loads(a.s3.get_object(Bucket=BUCKET,Key=args.source_bindings_key)['Body'].read())
  if recovered['as_of']>args.as_of:raise ValueError('Source recovery binding is future dated')
  sources.update(recovered['sources'])
 for name,s in sources.items():
  if s.get('remote_only'):continue
  for f in s['files']:f['path']=download(f['s3_uri'],f['sha256'])
  s['path']=s['files'][0]['path']
 sbir=m['sbir_source'];sbir['path']=download(sbir['source_s3_uri'],sbir['source_sha256'],'.csv')
 if args.refresh:
  prior_bindings=root/'prior_bindings.json';prior_bindings.write_text(json.dumps(m))
  env=dict(os.environ,MIMIR_OPPORTUNITY_WORKDIR=str(root),MIMIR_OPPORTUNITY_PREVIOUS_MANIFEST=str(prior_bindings),PYTHONDONTWRITEBYTECODE='1',PYTHONPATH=os.pathsep.join(str(p) for p in sys.path if p))
  if args.source_bindings_key:env['MIMIR_OPPORTUNITY_REUSE_SOURCE_SNAPSHOT']='1'
  for module in ('refresh_sbir','refresh_topics','refresh_shared_sources','refresh_commercial_sources','refresh_company_registry','refresh_company_history','refresh_innovation'):
   command=[sys.executable,'-m','opportunity_engine.ingestion.'+module]
   if module in ('refresh_shared_sources','refresh_company_history'):command+=['--as-of',args.as_of]
   try:subprocess.run(command,env=env,check=True)
   except subprocess.CalledProcessError:
    # A partial public topic listing must not erase the last complete snapshot
    # or prevent unrelated DIBBS/pricing/company sources from updating.
    if module!='refresh_topics' or not sources.get('sbir_topics'):raise
    retained=dict(sources['sbir_topics'],status='retained_prior_snapshot_refresh_failed',refresh_attempt_as_of=args.as_of,refresh_failure='The current SBA listing failed completeness checks; prior complete source dates and contents are retained.')
    fallback=root/'inputs/retained_topics.json';fallback.parent.mkdir(exist_ok=True);fallback.write_text(json.dumps({'sources':{'sbir_topics':retained}}))
    (root/'inputs/topics_latest.json').write_text(json.dumps({'path':str(fallback)}))
    print('SBA topic refresh failed: preserving prior complete topic snapshot with original source dates; continuing independent data refreshes.',flush=True)
  sbir=json.loads((root/'inputs/sbir_source.json').read_text())
  for name in ('topics','shared_sources','commercial','company_registry','company_history','innovation'):
   pointer=json.loads((root/('inputs/'+name+'_latest.json')).read_text());sources.update(json.loads(Path(pointer['path']).read_text())['sources'])
 # Rebuild the actual company and derived-demand inputs on every native refresh.
 # Archived native projections also allow deterministic recovery/rebuild runs.
 if 'native_prime_awards' in sources and 'native_subcontract_reports' in sources:
  derived=root/'derived'/(args.as_of+'_'+uuid.uuid4().hex[:8]);sources.update(rebuild_history(sources,derived/'company',args.as_of));sources.update(rebuild_demand(sources,derived/'demand',args.as_of));sources.update(refresh_forecast_episodes(sources,derived/'forecasts',args.as_of))
 from .item_demand import prepare as prepare_item_demand
 sources.update(prepare_item_demand(sources,root/'derived'/('items_'+uuid.uuid4().hex[:8]),args.as_of))
 # All bindings are consumed together; source as-of dates are never advanced.
 seed=root/'seed.json';seed.write_text('{}');inputs=root/'inputs.json';inputs.write_text(json.dumps({'sources':sources}));sb=root/'sbir.json';sb.write_text(json.dumps(sbir));out=root/'runs'/(datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')+'_'+uuid.uuid4().hex[:6])
 if args.refresh:
  pre=root/'runs'/(out.name+'_prelinks');build(seed,sb,pre,args.as_of,inputs,skip_matching=True,research_only=True)
  (root/'latest_build.json').write_text(json.dumps(dict(directory=str(pre))))
  subprocess.run([sys.executable,'-m','opportunity_engine.ingestion.connect_cloud_evidence'],env=env,check=True)
  ptr=json.loads((root/'inputs/connections_latest.json').read_text());sources.update(json.loads(Path(ptr['path']).read_text())['sources']);inputs.write_text(json.dumps({'sources':sources}))
 build(seed,sb,out,args.as_of,inputs);validate_demand_integrity(out);enrich(out,inputs if 'governed_price_lines' in sources else None)
 if 'cage_registry' in sources:augment(out,[inputs])
 try:
  if 'company_work' in sources:augment_companies(out)
  from .item_demand import augment as augment_item_demand
  if all(n in sources for n in ('item_price_references','item_market_price_evidence')):augment_item_demand(out)
  if 'company_work' in sources:apply_quality(out)
  from .item_presence import hold as hold_item_presence
  if 'company_work' in sources:hold_item_presence(out)
  from .item_requests import link as link_item_requests
  if 'company_work' in sources:link_item_requests(out)
  from .item_sources import augment as augment_item_sources
  if 'company_work' in sources:augment_item_sources(out)
  from .catalogue_sales import augment as augment_catalogue_sales
  if 'company_work' in sources:augment_catalogue_sales(out)
  from .company_activity import refresh as refresh_activity
  if 'native_company_identity' in sources:refresh_activity(out,a)
  from .pipeline_coverage import build as build_coverage
  if 'company_work' in sources:build_coverage(out)
  from .logic_provenance import record as record_logic
  if 'company_work' in sources:record_logic(out)
  if all(n in m.get('tables',{}) for n in ('opportunities','demand_decisions','company_matches')):track_events(out,m,a)
  v=validate(out)
  if args.publish:publish(out)
 except Exception:
  try:archive_failed_attempt(out,a,m.get('run_id'))
  except Exception as checkpoint_error:print('Could not archive failed derivation:',str(checkpoint_error),flush=True)
  raise
 print(json.dumps(dict(directory=str(out),validation_passed=v['passed'],published=args.publish)))
if __name__=='__main__':main()
