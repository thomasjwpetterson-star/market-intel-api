"""Bind raw evidence to verified immutable bytes, including duplicate snapshots."""
import hashlib
import re
from pathlib import Path
from .aws import BUCKET, PREFIX


def bind(aws, manifest, strict=True):
    verified = {}
    known = {raw['sha256']: raw['s3_uri'] for source in manifest['sources'].values() for raw in source.get('raw_archives', []) if raw.get('s3_uri')}
    gaps = []
    for source_name, source in manifest['sources'].items():
        for raw in source.get('raw_archives', []):
            digest = raw['sha256']
            try:
                if not re.fullmatch('[a-f0-9]{64}', digest):
                    raise ValueError('Invalid raw-source fingerprint')
                if digest in verified:
                    raw['s3_uri'] = verified[digest]
                    continue
                uri = raw.get('s3_uri') or known.get(digest)
                local = Path(raw['path']) if raw.get('path') else None
                key = PREFIX + '/sources/content/sha256=' + digest + '/source.archive'
                if not uri and local and local.is_file():
                    if hashlib.sha256(local.read_bytes()).hexdigest() != digest:
                        raise ValueError('Raw source content changed')
                    aws.upload(local, key)
                    uri = 's3://' + BUCKET + '/' + key
                uri = uri or 's3://' + BUCKET + '/' + key
                if not uri.startswith('s3://' + BUCKET + '/' + PREFIX + '/'):
                    raise ValueError('Raw archive is outside the owned evidence namespace')
                body = aws.s3.get_object(Bucket=BUCKET, Key=uri.split('/', 3)[3])['Body']
                hasher = hashlib.sha256()
                try:
                    while True:
                        chunk = body.read(1024 * 1024)
                        if not chunk:
                            break
                        hasher.update(chunk)
                finally:
                    body.close()
                if hasher.hexdigest() != digest:
                    raise ValueError('Immutable raw archive checksum differs')
                verified[digest] = uri
                raw['s3_uri'] = uri
            except Exception as error:
                if strict:
                    raise ValueError('Raw evidence archive unavailable: ' + source_name + '/' + digest) from error
                gaps.append(dict(source=source_name, sha256=digest, reason=type(error).__name__))
    manifest['raw_archive_recovery_gaps'] = gaps
    return verified
