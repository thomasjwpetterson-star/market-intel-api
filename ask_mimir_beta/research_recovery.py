"""Operator-only snapshots of MCP accounting, excluding Ask conversations.

Local files are private (0600). Optional upload requires a fully private existing
S3 bucket and server-side encryption. This is not a Supabase/Auth backup.
"""
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
from contextlib import closing
from .research_sessions import ResearchSessions

APPLICATION_ID=0x4D495231
TABLES={'research_session_windows':('id','owner','created','started','ends'),
        'research_session_calls':('request_key','owner','fingerprint','window_id','state','created','lease')}


def _digest(path):
    digest=hashlib.sha256()
    with open(path,'rb') as source:
        for chunk in iter(lambda:source.read(1024*1024),b''):digest.update(chunk)
    return digest.hexdigest()


def _read(path,snapshot_only=False):
    db=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)
    db.execute('BEGIN')
    try:
        if snapshot_only and (db.execute('PRAGMA application_id').fetchone()!=(APPLICATION_ID,) or {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}!=set(TABLES)):
            raise ValueError('Use a snapshot produced by the accounting recovery utility')
        if db.execute('PRAGMA integrity_check').fetchone()!=('ok',):raise ValueError('Accounting integrity check failed')
        if db.execute("SELECT 1 FROM research_session_calls c LEFT JOIN research_session_windows w ON w.id=c.window_id WHERE w.id IS NULL OR c.owner<>w.owner LIMIT 1").fetchone():raise ValueError('Accounting owner relationship check failed')
        return {name:db.execute('SELECT count(*) FROM '+name).fetchone()[0] for name in TABLES}
    finally:db.close()


def _copy(source,destination,now,exclusions=None):
    path=Path(destination)
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);os.close(fd)
    excluded_windows=0
    try:
        ResearchSessions(path)
        with closing(sqlite3.connect(path,uri=True)) as db,db:
            db.execute('PRAGMA application_id='+str(APPLICATION_ID));db.execute('PRAGMA foreign_keys=ON')
            db.execute('ATTACH DATABASE ? AS original',(Path(source).resolve().as_uri()+'?mode=ro',))
            # SQLite copies the fixed tables under one source read snapshot;
            # rows never accumulate in Python memory and Ask content is omitted.
            db.execute('INSERT INTO research_session_windows SELECT * FROM original.research_session_windows WHERE COALESCE(ends,created)>=?',(now-90*86400,))
            db.execute('INSERT INTO research_session_calls SELECT c.* FROM original.research_session_calls c JOIN main.research_session_windows w ON w.id=c.window_id')
            if exclusions is not None:
                db.executemany('DELETE FROM research_session_windows WHERE owner=?',((owner,) for owner in set(exclusions)))
                # Count excluded windows separately from cascaded call records.
                excluded_windows=sum(db.execute('SELECT count(*) FROM original.research_session_windows WHERE owner=? AND COALESCE(ends,created)>=?',(owner,now-90*86400)).fetchone()[0] for owner in set(exclusions))
                db.execute("UPDATE research_session_calls SET state='failed' WHERE state='pending'")
                db.execute('UPDATE research_session_windows SET ends=MIN(ends,?) WHERE ends IS NOT NULL',(now,))
            if db.execute('PRAGMA foreign_key_check').fetchall():raise ValueError('Accounting references are invalid')
        counts=_read(path,snapshot_only=True)
    except BaseException:
        path.unlink(missing_ok=True);raise
    return dict(windows=counts['research_session_windows'],calls=counts['research_session_calls'],sha256=_digest(path),**({'excluded_windows':excluded_windows} if exclusions is not None else {}))


def snapshot(source,destination,now=None):
    _read(source)
    return _copy(source,destination,time.time() if now is None else now)


def restore(source,destination,exclusions,now=None):
    """Create an isolated target; never overwrite a live file or replay jobs."""
    now=time.time() if now is None else now
    journal=json.loads(Path(exclusions).read_text())
    generated=dt.datetime.fromisoformat(journal['generated_at'].replace('Z','+00:00'))
    if generated.tzinfo is None or not -60<=now-generated.timestamp()<=86400:raise ValueError('A current deletion/revocation exclusion list is required')
    owners=journal.get('owners')
    if not isinstance(owners,list) or any(not isinstance(x,str) or not x for x in owners):raise ValueError('Invalid exclusion list')
    _read(source,snapshot_only=True)
    return _copy(source,destination,now,owners)


def upload(source,bucket,key,expected_owner,client=None):
    """Use an existing operator-approved bucket. Never grant public access."""
    if not key.startswith('customer-accounting-backups/') or '..' in key or not expected_owner.isdigit() or len(expected_owner)!=12:raise ValueError('Use the approved accounting prefix and bucket owner')
    _read(source,snapshot_only=True)
    if client is None:
        import boto3
        client=boto3.client('s3')
    block=client.get_public_access_block(Bucket=bucket,ExpectedBucketOwner=expected_owner)['PublicAccessBlockConfiguration']
    if not all(block.get(k) is True for k in ('BlockPublicAcls','IgnorePublicAcls','BlockPublicPolicy','RestrictPublicBuckets')):raise ValueError('Backup bucket must block all public access')
    digest=_digest(source)
    with open(source,'rb') as body:
        client.put_object(Bucket=bucket,Key=key,Body=body,ServerSideEncryption='AES256',ExpectedBucketOwner=expected_owner,ContentType='application/vnd.sqlite3',Metadata={'sha256':digest,'backup-schema':'mimir-research-accounting-v1'})
    saved=client.head_object(Bucket=bucket,Key=key,ExpectedBucketOwner=expected_owner)
    if saved.get('ServerSideEncryption')!='AES256' or saved.get('ContentLength')!=Path(source).stat().st_size or saved.get('Metadata',{}).get('sha256')!=digest:raise ValueError('Encrypted backup verification failed')
    return {'uploaded':True,'encryption':'SSE-S3','independent_disaster_recovery_certified':False}


def main():
    parser=argparse.ArgumentParser(description=__doc__);commands=parser.add_subparsers(dest='command',required=True)
    for name in ('snapshot','restore'):
        p=commands.add_parser(name);p.add_argument('--source',required=True);p.add_argument('--destination',required=True)
        if name=='restore':p.add_argument('--exclusions',required=True)
    p=commands.add_parser('upload');p.add_argument('--source',required=True);p.add_argument('--bucket',required=True);p.add_argument('--key',required=True);p.add_argument('--expected-owner',required=True)
    args=vars(parser.parse_args());command=args.pop('command')
    print(json.dumps(globals()[command](**args),sort_keys=True))


if __name__=='__main__':main()
