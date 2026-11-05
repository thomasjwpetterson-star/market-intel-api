import datetime as dt
import json
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
import hashlib
from unittest.mock import Mock
from ask_mimir_beta.research_sessions import ResearchSessions
from ask_mimir_beta.research_recovery import snapshot,restore,upload


class Recovery(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.now=time.time();self.db=self.root/'live.sqlite'
        ledger=ResearchSessions(self.db,clock=lambda:self.now)
        for owner in ['kept','deleted']:
            ledger.admit(owner,'free','request-'+owner,'a'*64);ledger.finish(owner,'free','request-'+owner,success=True)
        ledger.admit('pending','free','request-pending','b'*64)
        with closing(sqlite3.connect(self.db)) as db,db:db.execute("CREATE TABLE conversations(prompt TEXT)");db.execute("INSERT INTO conversations VALUES('PRIVATE-QUESTION-CANARY')")
        self.backup=self.root/'snapshot.sqlite';snapshot(self.db,self.backup,now=self.now)
        self.exclusions=self.root/'exclusions.json';self.exclusions.write_text(json.dumps({'generated_at':dt.datetime.fromtimestamp(self.now,dt.timezone.utc).isoformat(),'owners':['deleted']}))
    def test_snapshot_is_consistent_private_and_excludes_conversation_content(self):
        self.assertEqual(self.backup.stat().st_mode&0o777,0o600)
        self.assertNotIn(b'PRIVATE-QUESTION-CANARY',self.backup.read_bytes())
        with closing(sqlite3.connect(self.backup)) as db:self.assertEqual(db.execute('PRAGMA integrity_check').fetchone(),('ok',))
        with self.assertRaises(FileExistsError):snapshot(self.db,self.backup)
    def test_restore_reapplies_deletion_and_does_not_replay_pending_work(self):
        target=self.root/'restored.sqlite';out=restore(self.backup,target,self.exclusions,now=self.now)
        self.assertEqual(out['excluded_windows'],1)
        with closing(sqlite3.connect(target)) as db:
            self.assertEqual(db.execute("SELECT state FROM research_session_calls WHERE owner='pending'").fetchone(),('failed',))
            self.assertEqual(db.execute("SELECT count(*) FROM research_session_calls WHERE owner='deleted'").fetchone(),(0,))
        status=ResearchSessions(target,clock=lambda:self.now).status('kept','free')
        self.assertEqual(status['remaining_today'],1);self.assertFalse(status['can_continue'])
    def test_restore_rejects_stale_exclusions_and_never_overwrites_live_data(self):
        with self.assertRaises(FileExistsError):restore(self.backup,self.db,self.exclusions,now=self.now)
        with self.assertRaises(ValueError):restore(self.backup,self.root/'bad.sqlite',self.exclusions,now=self.now+86401)
        self.assertFalse((self.root/'bad.sqlite').exists())
    def test_cloud_copy_requires_blocked_public_access_and_encryption(self):
        client=Mock();client.get_public_access_block.return_value={'PublicAccessBlockConfiguration':{k:True for k in ['BlockPublicAcls','IgnorePublicAcls','BlockPublicPolicy','RestrictPublicBuckets']}}
        client.head_object.return_value={'ServerSideEncryption':'AES256','ContentLength':self.backup.stat().st_size,'Metadata':{'sha256':hashlib.sha256(self.backup.read_bytes()).hexdigest()}}
        out=upload(self.backup,'existing-bucket','customer-accounting-backups/test.sqlite','123456789012',client)
        self.assertTrue(out['uploaded']);self.assertEqual(client.put_object.call_args.kwargs['ServerSideEncryption'],'AES256')
        self.assertNotIn('ACL',client.put_object.call_args.kwargs)
        client.reset_mock();client.get_public_access_block.return_value={'PublicAccessBlockConfiguration':{}}
        with self.assertRaises(ValueError):upload(self.backup,'existing-bucket','customer-accounting-backups/test.sqlite','123456789012',client)
        client.put_object.assert_not_called()
        with self.assertRaises(ValueError):upload(self.db,'existing-bucket','customer-accounting-backups/wrong.sqlite','123456789012',client)
