"""Rebuild/publish from portable AWS source bindings, without prototype paths."""
import argparse,datetime,hashlib,json,uuid,os,subprocess,sys
from pathlib import Path
from .aws import AWS,BUCKET,PREFIX
from .build import build
from .validate import validate
from .publish import publish
from .enrich import enrich
from .registry import augment
from .company_enrichment import augment as augment_companies
from .company_history import rebuild as rebuild_history
from .events import track as track_events
from .demand_history import rebuild as rebuild_demand,refresh_forecast_episodes

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--from-current',action='store_true',required=True);ap.add_argument('--workdir',required=True);ap.add_argument('--as-of',default=datetime.datetime.now(datetime.timezone.utc).date().isoformat());ap.add_argument('--publish',action='store_true');ap.add_argument('--refresh',action='store_true');ap.add_argument('--source-bindings-key');args=ap.parse_args();a=AWS();root=Path(args.workdir).resolve();root.mkdir(parents=True,exist_ok=True)
 pointer=json.loads(a.s3.get_object(Bucket=BUCKET,Key=PREFIX+'/current.json')['Body'].read());key=pointer['manifest_key']
 if not key.startswith(PREFIX+'/releases/'):raise ValueError('Invalid release pointer')
 m=json.loads(a.s3.get_object(Bucket=BUCKET,Key=key)['Body'].read())
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
  env=dict(os.environ,MIMIR_OPPORTUNITY_WORKDIR=str(root),MIMIR_OPPORTUNITY_PREVIOUS_MANIFEST=str(prior_bindings),PYTHONDONTWRITEBYTECODE='1')
  for module in ('refresh_sbir','refresh_topics','refresh_shared_sources','refresh_commercial_sources','refresh_company_registry','refresh_company_history','refresh_innovation'):
   command=[sys.executable,'-m','opportunity_engine.ingestion.'+module]
   if module in ('refresh_shared_sources','refresh_company_history'):command+=['--as-of',args.as_of]
   subprocess.run(command,env=env,check=True)
  sbir=json.loads((root/'inputs/sbir_source.json').read_text())
  for name in ('topics','shared_sources','commercial','company_registry','company_history','innovation'):
   pointer=json.loads((root/('inputs/'+name+'_latest.json')).read_text());sources.update(json.loads(Path(pointer['path']).read_text())['sources'])
 # Rebuild the actual company and derived-demand inputs on every native refresh.
 # Archived native projections also allow deterministic recovery/rebuild runs.
 if 'native_prime_awards' in sources and 'native_subcontract_reports' in sources:
  derived=root/'derived'/(args.as_of+'_'+uuid.uuid4().hex[:8]);sources.update(rebuild_history(sources,derived/'company',args.as_of));sources.update(rebuild_demand(sources,derived/'demand',args.as_of));sources.update(refresh_forecast_episodes(sources,derived/'forecasts',args.as_of))
 # All bindings are consumed together; source as-of dates are never advanced.
 seed=root/'seed.json';seed.write_text('{}');inputs=root/'inputs.json';inputs.write_text(json.dumps({'sources':sources}));sb=root/'sbir.json';sb.write_text(json.dumps(sbir));out=root/'runs'/(datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')+'_'+uuid.uuid4().hex[:6])
 if args.refresh:
  pre=root/'runs'/(out.name+'_prelinks');build(seed,sb,pre,args.as_of,inputs,skip_matching=True)
  (root/'latest_build.json').write_text(json.dumps(dict(directory=str(pre))))
  subprocess.run([sys.executable,'-m','opportunity_engine.ingestion.connect_cloud_evidence'],env=env,check=True)
  ptr=json.loads((root/'inputs/connections_latest.json').read_text());sources.update(json.loads(Path(ptr['path']).read_text())['sources']);inputs.write_text(json.dumps({'sources':sources}))
 build(seed,sb,out,args.as_of,inputs);enrich(out,inputs if 'governed_price_lines' in sources else None)
 if 'cage_registry' in sources:augment(out,[inputs])
 if 'company_work' in sources:augment_companies(out)
 if all(n in m.get('tables',{}) for n in ('opportunities','demand_decisions','company_matches')):track_events(out,m,a)
 v=validate(out)
 if args.publish:publish(out)
 print(json.dumps(dict(directory=str(out),validation_passed=v['passed'],published=args.publish)))
if __name__=='__main__':main()
