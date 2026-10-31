import hashlib
import io
import pytest
from opportunity_engine.raw_archives import bind
from opportunity_engine.aws import BUCKET, PREFIX


class AWS:
    def __init__(self, objects=None):
        self.objects = objects or {}
        self.s3 = self

    def get_object(self, Bucket, Key):
        return {'Body': io.BytesIO(self.objects[Key])}

    def upload(self, path, key):
        self.objects[key] = path.read_bytes()


def test_duplicate_raw_reference_reuses_verified_original_bytes_without_temporary_path():
    content = b'original official source'
    digest = hashlib.sha256(content).hexdigest()
    key = PREFIX + '/sources/content/sha256=' + digest + '/source.archive'
    manifest = {'sources': {'feed': {'raw_archives': [{'sha256': digest, 'path': '/missing/source'}, {'sha256': digest, 's3_uri': 's3://' + BUCKET + '/' + key}]}}}
    bind(AWS({key: content}), manifest)
    assert manifest['sources']['feed']['raw_archives'][0]['s3_uri'].endswith(key)
    assert manifest['raw_archive_recovery_gaps'] == []


def test_current_raw_bytes_are_archived_before_worker_checkpoint_disappears(tmp_path):
    path = tmp_path / 'source'
    path.write_bytes(b'new original bytes')
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = {'sources': {'feed': {'raw_archives': [{'sha256': digest, 'path': str(path)}]}}}
    aws = AWS()
    bind(aws, manifest)
    assert len(aws.objects) == 1
    path.unlink()
    bind(aws, manifest)


def test_changed_or_missing_archives_cannot_be_published_as_original_bytes():
    digest = hashlib.sha256(b'original').hexdigest()
    key = PREFIX + '/sources/content/sha256=' + digest + '/source.archive'
    manifest = {'sources': {'feed': {'raw_archives': [{'sha256': digest, 'path': '/missing/source'}]}}}
    with pytest.raises(ValueError, match='Raw evidence archive unavailable'):
        bind(AWS({key: b'changed'}), manifest)
    bind(AWS(), manifest, strict=False)
    assert manifest['raw_archive_recovery_gaps'][0]['sha256'] == digest
    assert 's3_uri' not in manifest['sources']['feed']['raw_archives'][0]
