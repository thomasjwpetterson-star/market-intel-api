"""Download and verify one immutable Ask Mimir runtime release."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict

import boto3


DEFAULT_BUCKET = "a-and-d-intel-lake-newaccount"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _download_verified(
    s3: Any,
    bucket: str,
    entry: Dict[str, Any],
    runtime_root: Path,
) -> Path:
    destination = (runtime_root / entry["local_path"]).resolve()
    if runtime_root.resolve() not in destination.parents:
        raise RuntimeError(f"Unsafe runtime path in manifest: {entry['local_path']}")
    destination.parent.mkdir(parents=True, exist_ok=True)

    expected_size = int(entry["size"])
    expected_hash = str(entry["sha256"])
    if (
        destination.exists()
        and destination.stat().st_size == expected_size
        and file_sha256(destination) == expected_hash
    ):
        return destination

    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.unlink(missing_ok=True)
    s3.download_file(bucket, entry["s3_key"], str(temporary))
    if temporary.stat().st_size != expected_size:
        temporary.unlink(missing_ok=True)
        raise RuntimeError(f"Size mismatch for {entry['s3_key']}")
    if file_sha256(temporary) != expected_hash:
        temporary.unlink(missing_ok=True)
        raise RuntimeError(f"SHA-256 mismatch for {entry['s3_key']}")
    temporary.replace(destination)
    return destination


def bootstrap() -> Dict[str, Any]:
    runtime_root = Path(
        os.getenv("ASK_MIMIR_RUNTIME_ROOT", str(Path(__file__).parent / ".runtime-data"))
    ).resolve()
    runtime_root.mkdir(parents=True, exist_ok=True)

    bucket = os.getenv("ASK_MIMIR_BUCKET", DEFAULT_BUCKET)
    manifest_key = os.getenv("ASK_MIMIR_MANIFEST_KEY", "").strip()
    if not manifest_key:
        raise RuntimeError("ASK_MIMIR_MANIFEST_KEY is required")

    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
    manifest_path = runtime_root / "runtime_manifest.json.tmp"
    s3.download_file(bucket, manifest_key, str(manifest_path))
    manifest = json.loads(manifest_path.read_text())
    if not manifest.get("release_id") or not manifest.get("files"):
        raise RuntimeError("Ask Mimir runtime manifest is incomplete")

    for entry in manifest["files"]:
        _download_verified(s3, bucket, entry, runtime_root)

    final_manifest = runtime_root / "runtime_manifest.json"
    manifest_path.replace(final_manifest)

    data_root = runtime_root / "data"
    artifact_root = runtime_root / "artifacts"
    os.environ["ASK_MIMIR_DATA_ROOT"] = str(data_root)
    os.environ["ASK_MIMIR_RELEASE_DIR"] = str(artifact_root / "metric-release")
    os.environ["ASK_MIMIR_TRANSACTIONS"] = str(data_root / "transactions.parquet")
    os.environ["ASK_MIMIR_COMPANY_CONTEXT_DIR"] = str(artifact_root / "company-context")
    os.environ["ASK_MIMIR_COMPANY_OPPORTUNITY_DIR"] = str(
        artifact_root / "company-opportunities"
    )
    os.environ["ASK_MIMIR_PLATFORM_SUPPLY_CHAIN_DIR"] = str(
        artifact_root / "platform-supply-chains"
    )
    os.environ["ASK_MIMIR_PROGRAM_MOMENTUM_PACK"] = str(
        artifact_root / "program-momentum" / "missile-program-momentum.json"
    )
    os.environ.setdefault("ASK_MIMIR_CACHE_DIR", str(runtime_root / "cache"))
    os.environ.setdefault("ASK_MIMIR_BETA_STATE", str(runtime_root / "beta-state.sqlite3"))
    os.environ.setdefault("ASK_MIMIR_AUDIT_LOG", str(runtime_root / "audit" / "answers.jsonl"))
    return manifest


if __name__ == "__main__":
    released = bootstrap()
    print(f"Ask Mimir runtime ready: {released['release_id']}")
