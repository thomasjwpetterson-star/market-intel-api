"""Hydrate only distributed inputs required by local release gates."""
import hashlib
from pathlib import Path
from .aws import BUCKET,PREFIX

LOCAL_GATE_TABLES=('market_capability_customer_links','company_subcontract_facts','contract_value_cohorts')

def prepare(directory,manifest,aws=None):
    root=Path(directory)
    for name in LOCAL_GATE_TABLES:
        table=manifest['tables'].get(name,{})
        if not table.get('remote_binding'):continue
        source=manifest['sources'][table['source_binding']]
        if not source.get('files'):raise ValueError('Local release gate lacks immutable source files: '+name)
        for i,f in enumerate(source['files']):
            uri=f.get('s3_uri','');sha=f.get('sha256','')
            if not uri.startswith('s3://'+BUCKET+'/'+PREFIX+'/') or len(sha)!=64:raise ValueError('Invalid release-gate source binding: '+name)
            path=Path(f['path']) if f.get('path') and Path(f['path']).is_file() else root/('validation_'+name+'_'+str(i)+'.parquet')
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest()!=sha:
                if aws is None:
                    from .aws import AWS
                    aws=AWS()
                aws.s3.download_file(BUCKET,uri.split('/',3)[3],str(path))
            if hashlib.sha256(path.read_bytes()).hexdigest()!=sha:raise ValueError('Release-gate source checksum mismatch: '+name)
            f['path']=str(path)
    return manifest
