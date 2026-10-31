import io
import json
import pytest
from opportunity_engine import __main__ as entry
from opportunity_engine import catalogue_sales
from opportunity_engine.aws import PREFIX


def test_late_review_failure_preserves_completed_outputs_with_published_parent(tmp_path, monkeypatch):
    parent = {"run_id": "published_parent"}

    class S3:
        def get_object(self, Bucket, Key):
            value = {"manifest_key": PREFIX + "/releases/published_parent/manifest.json"} if Key.endswith("/current.json") else parent
            return {"Body": io.BytesIO(json.dumps(value).encode())}

    class AWS:
        s3 = S3()

    aws = AWS()
    output = tmp_path / "complete_review"
    output.mkdir()
    preserved = []
    monkeypatch.setattr(entry, "AWS", lambda: aws)
    monkeypatch.setattr(entry, "review_current", lambda *args: output)
    monkeypatch.setattr(entry, "validate_demand_integrity", lambda *args: None)
    monkeypatch.setattr(entry, "archive_failed_attempt", lambda *args: preserved.append(args))

    def fail(*args):
        raise RuntimeError("late derived evidence failure")

    monkeypatch.setattr(catalogue_sales, "augment", fail)
    monkeypatch.setattr("sys.argv", ["opportunity-engine", "--from-current", "--workdir", str(tmp_path), "--review-current"])
    with pytest.raises(RuntimeError, match="late derived evidence failure"):
        entry.main()
    assert preserved == [(output, aws, "published_parent")]
