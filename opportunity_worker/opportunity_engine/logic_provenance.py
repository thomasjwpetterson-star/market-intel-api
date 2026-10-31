"""Bind every producer module to the published methodology version."""
import hashlib,json
from pathlib import Path

def record(directory):
 r=Path(directory);p=r/'manifest.json';m=json.loads(p.read_text());base=Path(__file__).parent
 files={str(f.relative_to(base)):hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(base.rglob('*')) if f.is_file() and f.suffix in ('.py','.json','.sql') and '__pycache__' not in f.parts}
 digest=hashlib.sha256(json.dumps(files,sort_keys=True).encode()).hexdigest()
 m['processing_logic_files']=files;m['processing_logic_sha256']=digest
 m['logic_sha256']=hashlib.sha256(json.dumps(dict(processing=digest,quality=m.get('source_quality_logic_sha256'),rules=m['rules'].get('logic_sha256') or m.get('rules',{}).get('version')),sort_keys=True).encode()).hexdigest()
 p.write_text(json.dumps(m,indent=2)+'\n')
