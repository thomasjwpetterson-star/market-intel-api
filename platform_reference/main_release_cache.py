"""Stage one version-pinned Main cache release, including Athena Parquet parts.

The live root pointer is read once at startup. A complete release directory is
verified before the API uses it; rollback selects the prior immutable manifest
on restart. Legacy schema-1 serving remains available until the v2 cutover.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any


REQUIRED_FILES = {
    "products.parquet", "summary.parquet", "geo.parquet",
    "cage_locations.parquet", "profiles.parquet", "risk.parquet",
    "network.parquet", "subcontract_descriptions.parquet",
    "transactions.parquet", "opportunities.parquet", "kpis.parquet",
    "nsn_summary.parquet", "nsn_profile_lookup.parquet",
    "nsn_supplier_lookup.parquet", "nsn_cage_reference.parquet",
    "dod_contract_announcements.parquet", "contract_award_metadata.parquet",
    "platform_bom.parquet", "contracts_rolled.parquet",
    "nsn_supply_state_lookup.parquet",
    "nsn_price_summary_lookup.parquet",
    "nsn_opportunity_summary_lookup.parquet",
    "nsn_opportunity_detail.parquet",
}
PLATFORM_FILES = {
    "products.parquet", "network.parquet", "transactions.parquet",
    "nsn_summary.parquet", "nsn_supplier_lookup.parquet",
    "nsn_cage_reference.parquet", "platform_bom.parquet",
    "contracts_rolled.parquet", "summary.parquet", "profiles.parquet",
    "risk.parquet",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_parquet(path: Path) -> None:
    if path.stat().st_size < 8:
        raise ValueError(f"Parquet object is too short: {path}")
    with path.open("rb") as stream:
        if stream.read(4) != b"PAR1":
            raise ValueError(f"Parquet header mismatch: {path}")
        stream.seek(-4, os.SEEK_END)
        if stream.read(4) != b"PAR1":
            raise ValueError(f"Parquet footer mismatch: {path}")


def validate_manifest(manifest: dict) -> dict[str, dict]:
    if manifest.get("schema_version") != 2 or not manifest.get("release_id"):
        raise ValueError("Main cache manifest must be a release-pinned schema-2 manifest")
    platform_release_id = manifest.get("platform_reference_release_id")
    if not platform_release_id:
        raise ValueError("Main cache manifest has no platform reference release")
    entries: dict[str, dict] = {}
    for row in manifest.get("files", []):
        filename = row.get("local_path")
        if filename in entries or not filename or filename != Path(filename).name:
            raise ValueError(f"duplicate or unsafe cache filename: {filename}")
        parts = row.get("parts")
        if parts:
            if not isinstance(parts, list) or not parts:
                raise ValueError(f"empty multipart cache: {filename}")
            for part in parts:
                if not all(part.get(key) for key in ("s3_key", "s3_version_id", "size", "etag")):
                    raise ValueError(f"unpinned multipart cache: {filename}")
            if filename not in PLATFORM_FILES:
                raise ValueError(f"unexpected multipart cache: {filename}")
        elif not all(row.get(key) for key in ("s3_key", "s3_version_id", "size", "sha256")):
            raise ValueError(f"unpinned cache: {filename}")
        if filename in PLATFORM_FILES and row.get("platform_reference_release_id") != platform_release_id:
            raise ValueError(f"platform cache uses the wrong release: {filename}")
        entries[filename] = row
    if set(entries) != REQUIRED_FILES:
        raise ValueError(f"Main cache set differs: missing={sorted(REQUIRED_FILES-set(entries))}, extra={sorted(set(entries)-REQUIRED_FILES)}")
    return entries


def get_main_manifest(s3: Any, bucket: str, pointer_key: str) -> dict:
    root = json.loads(s3.get_object(Bucket=bucket, Key=pointer_key)["Body"].read())
    if root.get("schema_version") != 2 or not root.get("promotion_ready"):
        raise ValueError("root pointer is not a promoted schema-2 release")
    main = root.get("components", {}).get("main", {})
    key = main.get("immutable_manifest_key")
    if not key or "/releases/" not in key or key.endswith("current_manifest.json"):
        raise ValueError("root has no immutable Main manifest")
    manifest = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    if manifest.get("release_id") != main.get("release_id"):
        raise ValueError("Main manifest release disagrees with root pointer")
    platform = root.get("components", {}).get("platform_reference", {})
    if manifest.get("platform_reference_release_id") != platform.get("release_id"):
        raise ValueError("Main and platform-reference releases disagree")
    validate_manifest(manifest)
    return manifest


def get_candidate_main_manifest(s3: Any, bucket: str, key: str) -> dict:
    """Load an immutable candidate directly for a nonlive dual-read rehearsal."""
    if "/releases/" not in key or key.endswith("current_manifest.json"):
        raise ValueError("candidate Main manifest key must be immutable")
    manifest = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    if manifest.get("release_status") != "CANDIDATE_ONLY" or manifest.get("customer_facing"):
        raise ValueError("dual-read manifest must be candidate-only")
    validate_manifest(manifest)
    return manifest


def _download_one(s3: Any, bucket: str, entry: dict, destination: Path) -> None:
    remote = s3.head_object(Bucket=bucket, Key=entry["s3_key"],
                            VersionId=entry["s3_version_id"])
    if int(remote["ContentLength"]) != int(entry["size"]):
        raise ValueError(f"version-pinned object size changed: {entry['s3_key']}")
    if entry.get("etag") and remote["ETag"].strip('"') != entry["etag"]:
        raise ValueError(f"version-pinned object ETag changed: {entry['s3_key']}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, entry["s3_key"], str(destination),
                     ExtraArgs={"VersionId": entry["s3_version_id"]})
    if destination.stat().st_size != int(entry["size"]):
        raise ValueError(f"download size mismatch: {entry['s3_key']}")
    if entry.get("sha256") and sha256(destination) != entry["sha256"]:
        raise ValueError(f"download SHA-256 mismatch: {entry['s3_key']}")
    verify_parquet(destination)


def stage_main_cache(s3: Any, bucket: str, manifest: dict, release_root: Path) -> Path:
    """Return a complete immutable local directory or raise without selecting it."""
    entries = validate_manifest(manifest)
    release_id = manifest["release_id"]
    if release_id != Path(release_id).name:
        raise ValueError("unsafe Main release ID")
    final = release_root / release_id
    marker = final / ".verified-manifest.json"
    fingerprint = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
    if marker.exists():
        try:
            if json.loads(marker.read_text()).get("fingerprint") == fingerprint:
                complete = True
                for filename, row in entries.items():
                    if row.get("parts"):
                        complete = complete and all(
                            (final / filename / f"part-{index:05d}.parquet").stat().st_size
                            == int(part["size"])
                            for index, part in enumerate(row["parts"])
                        )
                    else:
                        complete = complete and (final / filename).stat().st_size == int(row["size"])
                if complete:
                    return final
        except (OSError, json.JSONDecodeError):
            pass
    if final.exists():
        raise ValueError("existing unverified Main cache directory must be inspected")
    staging = release_root / (release_id + ".staging")
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    try:
        downloads = []
        for filename, row in sorted(entries.items()):
            if row.get("parts"):
                folder = staging / filename
                folder.mkdir()
                for index, part in enumerate(row["parts"]):
                    downloads.append((part, folder / f"part-{index:05d}.parquet"))
            else:
                downloads.append((row, staging / filename))
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(_download_one, s3, bucket, row, destination)
                       for row, destination in downloads]
            for future in as_completed(futures):
                future.result()
        (staging / ".verified-manifest.json").write_text(json.dumps({
            "release_id": release_id, "fingerprint": fingerprint,
            "platform_reference_release_id": manifest["platform_reference_release_id"],
        }, indent=2) + "\n")
        staging.rename(final)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return final
