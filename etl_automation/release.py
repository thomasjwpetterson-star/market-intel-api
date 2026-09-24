"""Validate Mimir cache objects and publish a candidate-only release manifest."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping, Sequence


MAIN_CANDIDATE_KEY = "mimir/runtime/candidate_manifest.json"
MAIN_CURRENT_KEY = "mimir/runtime/current_manifest.json"


@dataclass(frozen=True)
class Artifact:
    filename: str
    generated_by_refresh: bool = True
    allow_empty: bool = False

    def key(self, cache_prefix: str) -> str:
        prefix = cache_prefix.strip().strip("/") + "/"
        if not self.generated_by_refresh:
            prefix = "app_cache/"
        return f"{prefix}{self.filename}"


GENERATED_ARTIFACTS: tuple[Artifact, ...] = (
    Artifact("summary.parquet"),
    Artifact("kpis.parquet"),
    Artifact("geo.parquet"),
    Artifact("cage_locations.parquet"),
    Artifact("risk.parquet"),
    Artifact("network.parquet"),
    Artifact("subcontract_descriptions.parquet", allow_empty=True),
    Artifact("transactions.parquet"),
    Artifact("contracts_rolled.parquet"),
    Artifact("contract_award_metadata.parquet"),
    Artifact("nsn_summary.parquet"),
    Artifact("nsn_profile_lookup.parquet"),
    Artifact("nsn_supplier_lookup.parquet"),
    Artifact("products.parquet"),
    Artifact("nsn_cage_reference.parquet"),
    Artifact("nsn_supply_state_lookup.parquet"),
    Artifact("nsn_price_summary_lookup.parquet"),
    Artifact("nsn_opportunity_summary_lookup.parquet"),
    Artifact("nsn_opportunity_detail.parquet"),
    Artifact("platform_bom.parquet"),
    Artifact("opportunities.parquet", allow_empty=True),
    Artifact("profiles.parquet"),
)

MAIN_RELEASE_ARTIFACTS: tuple[Artifact, ...] = GENERATED_ARTIFACTS + (
    # Produced by its own daily Glue pipeline but pinned into the same release.
    Artifact("dod_contract_announcements.parquet", generated_by_refresh=False),
)

CORE_FILENAMES: tuple[str, ...] = tuple(
    artifact.filename
    for artifact in GENERATED_ARTIFACTS
    if artifact.filename not in {
        "profiles.parquet",
        "nsn_supply_state_lookup.parquet",
        "nsn_price_summary_lookup.parquet",
        "nsn_opportunity_summary_lookup.parquet",
        "nsn_opportunity_detail.parquet",
    }
)
DEPENDENT_FILENAMES: tuple[str, ...] = ("profiles.parquet",)
OPERATIONAL_SIDECAR_FILENAMES: tuple[str, ...] = (
    "nsn_supply_state_lookup.parquet",
    "nsn_price_summary_lookup.parquet",
    "nsn_opportunity_summary_lookup.parquet",
    "nsn_opportunity_detail.parquet",
)

DEPENDENCIES: Mapping[str, tuple[str, ...]] = {
    "profiles.parquet": ("summary.parquet", "network.parquet"),
}


class CandidateValidationError(RuntimeError):
    """Raised when a cache set is not safe to publish as a candidate."""


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _head_artifact(
    s3: Any,
    bucket: str,
    artifact: Artifact,
    cache_prefix: str,
) -> Dict[str, Any]:
    key = artifact.key(cache_prefix)
    try:
        remote = s3.head_object(
            Bucket=bucket,
            Key=key,
            ChecksumMode="ENABLED",
        )
    except Exception as exc:
        raise CandidateValidationError(
            f"Required cache object is unavailable: s3://{bucket}/{key}: {exc}"
        ) from exc

    version_id = str(remote.get("VersionId") or "").strip()
    if not version_id or version_id == "null":
        raise CandidateValidationError(
            f"S3 versioning is required for {key}"
        )
    size = int(remote.get("ContentLength") or 0)
    if size <= 0:
        raise CandidateValidationError(f"Cache object is empty: {key}")

    metadata = {
        str(key).lower(): str(value)
        for key, value in (remote.get("Metadata") or {}).items()
    }
    last_modified = remote.get("LastModified")
    if not isinstance(last_modified, datetime):
        raise CandidateValidationError(
            f"Cache object has no LastModified timestamp: {key}"
        )

    entry: Dict[str, Any] = {
        "local_path": artifact.filename,
        "s3_key": key,
        "s3_version_id": version_id,
        "size": size,
        "last_modified": _iso(last_modified),
        "generated_by_refresh": artifact.generated_by_refresh,
    }
    etag = str(remote.get("ETag") or "").strip('"')
    if etag:
        entry["s3_etag"] = etag
    for source, destination in (
        ("sha256", "sha256"),
        ("row-count", "row_count"),
        ("schema-sha256", "schema_sha256"),
        ("etl-run-id", "etl_run_id"),
        ("generated-at", "generated_at"),
    ):
        value = metadata.get(source, "").strip()
        if value:
            entry[destination] = int(value) if source == "row-count" else value
    return entry


def build_candidate_manifest(
    s3: Any,
    bucket: str,
    run_id: str,
    run_started_at: datetime,
    artifacts: Sequence[Artifact] = MAIN_RELEASE_ARTIFACTS,
    cache_prefix: str = "app_cache/",
) -> Dict[str, Any]:
    """Pin and validate one coherent cache generation without promoting it."""
    if not run_id.strip():
        raise ValueError("run_id is required")
    if run_started_at.tzinfo is None:
        raise ValueError("run_started_at must be timezone-aware")

    entries = {
        artifact.filename: _head_artifact(s3, bucket, artifact, cache_prefix)
        for artifact in artifacts
    }
    artifact_by_name = {artifact.filename: artifact for artifact in artifacts}
    freshness_floor = run_started_at.astimezone(timezone.utc) - timedelta(minutes=5)

    for filename, entry in entries.items():
        artifact = artifact_by_name[filename]
        if not artifact.generated_by_refresh:
            continue
        missing = [
            field
            for field in ("sha256", "row_count", "schema_sha256", "etl_run_id")
            if field not in entry
        ]
        if missing:
            raise CandidateValidationError(
                f"{filename} is missing ETL validation metadata: {', '.join(missing)}"
            )
        if entry["etl_run_id"] != run_id:
            raise CandidateValidationError(
                f"{filename} belongs to ETL run {entry['etl_run_id']}, expected {run_id}"
            )
        if entry["row_count"] < 0 or (
            entry["row_count"] == 0 and not artifact.allow_empty
        ):
            raise CandidateValidationError(
                f"{filename} has an invalid row count: {entry['row_count']}"
            )
        modified = datetime.fromisoformat(entry["last_modified"])
        if modified < freshness_floor:
            raise CandidateValidationError(
                f"{filename} predates this ETL run: {entry['last_modified']}"
            )

    dependency_checks = []
    for filename, dependencies in DEPENDENCIES.items():
        if filename not in entries:
            continue
        dependent_time = datetime.fromisoformat(entries[filename]["last_modified"])
        for dependency in dependencies:
            if dependency not in entries:
                raise CandidateValidationError(
                    f"{filename} depends on missing artifact {dependency}"
                )
            dependency_time = datetime.fromisoformat(
                entries[dependency]["last_modified"]
            )
            if dependent_time < dependency_time:
                raise CandidateValidationError(
                    f"{filename} was generated before dependency {dependency}"
                )
        dependency_checks.append(
            {"artifact": filename, "must_not_precede": list(dependencies)}
        )

    generated_at = datetime.now(timezone.utc)
    return {
        "schema_version": 1,
        "release_id": f"mimir-main-{run_id}",
        "status": "candidate",
        "generated_at": _iso(generated_at),
        "run_started_at": _iso(run_started_at),
        "etl_run_id": run_id,
        "cache_prefix": cache_prefix.strip().strip("/") + "/",
        "files": [entries[name] for name in sorted(entries)],
        "validation": {
            "all_required_objects_present": True,
            "all_generated_objects_from_same_run": True,
            "dependencies": dependency_checks,
        },
    }


def publish_candidate_manifest(
    s3: Any,
    bucket: str,
    manifest: Mapping[str, Any],
    candidate_key: str = MAIN_CANDIDATE_KEY,
) -> Dict[str, str]:
    """Write immutable and candidate manifests; never update the current pointer."""
    run_id = str(manifest["etl_run_id"])
    immutable_key = f"mimir/releases/{run_id}/manifest.json"
    body = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")
    for key in (immutable_key, candidate_key):
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
    return {
        "immutable_manifest_key": immutable_key,
        "candidate_manifest_key": candidate_key,
    }


def validate_and_publish_candidate(
    s3: Any,
    bucket: str,
    run_id: str,
    run_started_at: datetime,
    cache_prefix: str,
) -> Dict[str, Any]:
    manifest = build_candidate_manifest(
        s3=s3,
        bucket=bucket,
        run_id=run_id,
        run_started_at=run_started_at,
        cache_prefix=cache_prefix,
    )
    locations = publish_candidate_manifest(s3, bucket, manifest)
    return {**locations, "release_id": manifest["release_id"]}
