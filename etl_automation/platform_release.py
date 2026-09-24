"""One atomic release contract for main Mimir, public pages, and Ask Mimir."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Mapping


PLATFORM_CANDIDATE_KEY = "mimir/platform/candidate_manifest.json"
PLATFORM_CURRENT_KEY = "mimir/platform/current_manifest.json"
PLATFORM_COMPONENTS = ("main", "public", "ask_mimir")


class PlatformReleaseValidationError(RuntimeError):
    """Raised when child releases cannot form one coherent platform release."""


def _normalise_component(
    name: str,
    manifest: Mapping[str, Any],
    immutable_manifest_key: str,
    etl_run_id: str,
) -> dict[str, str]:
    release_id = str(manifest.get("release_id") or "").strip()
    child_run_id = str(manifest.get("etl_run_id") or "").strip()
    immutable_key = str(immutable_manifest_key or "").strip().strip("/")
    if not release_id:
        raise PlatformReleaseValidationError(f"{name} manifest has no release_id")
    if child_run_id != etl_run_id:
        raise PlatformReleaseValidationError(
            f"{name} belongs to ETL run {child_run_id or '<missing>'}, "
            f"expected {etl_run_id}"
        )
    if (
        not immutable_key
        or "/releases/" not in immutable_key
        or immutable_key.endswith("current_manifest.json")
        or immutable_key.endswith("candidate_manifest.json")
    ):
        raise PlatformReleaseValidationError(
            f"{name} must reference an immutable manifest key"
        )
    return {
        "release_id": release_id,
        "etl_run_id": child_run_id,
        "immutable_manifest_key": immutable_key,
    }


def build_platform_manifest(
    *,
    etl_run_id: str,
    main_manifest: Mapping[str, Any],
    main_manifest_key: str,
    public_manifest: Mapping[str, Any],
    public_manifest_key: str,
    ask_mimir_manifest: Mapping[str, Any],
    ask_mimir_manifest_key: str,
    previous_platform_release_id: str | None = None,
) -> dict[str, Any]:
    """Bind three immutable child manifests to one candidate release."""
    run_id = str(etl_run_id or "").strip()
    if not run_id:
        raise ValueError("etl_run_id is required")
    components = {
        "main": _normalise_component(
            "main", main_manifest, main_manifest_key, run_id
        ),
        "public": _normalise_component(
            "public", public_manifest, public_manifest_key, run_id
        ),
        "ask_mimir": _normalise_component(
            "ask_mimir", ask_mimir_manifest, ask_mimir_manifest_key, run_id
        ),
    }
    return {
        "schema_version": 1,
        "release_id": f"mimir-platform-{run_id}",
        "status": "candidate",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "etl_run_id": run_id,
        "previous_platform_release_id": previous_platform_release_id,
        "components": components,
        "validation": {
            "all_components_present": True,
            "all_components_from_same_run": True,
            "single_pointer_required": PLATFORM_CURRENT_KEY,
        },
    }


def validate_platform_manifest(manifest: Mapping[str, Any]) -> None:
    run_id = str(manifest.get("etl_run_id") or "").strip()
    release_id = str(manifest.get("release_id") or "").strip()
    components = manifest.get("components")
    if not run_id or not release_id or not isinstance(components, Mapping):
        raise PlatformReleaseValidationError("Platform manifest is incomplete")
    missing = set(PLATFORM_COMPONENTS) - set(components)
    if missing:
        raise PlatformReleaseValidationError(
            "Platform manifest is missing components: " + ", ".join(sorted(missing))
        )
    for name in PLATFORM_COMPONENTS:
        if not isinstance(components[name], Mapping):
            raise PlatformReleaseValidationError(
                f"Platform component {name} is not a manifest reference"
            )
        _normalise_component(
            name,
            components[name],
            str(components[name].get("immutable_manifest_key") or ""),
            run_id,
        )


def _manifest_body(manifest: Mapping[str, Any]) -> bytes:
    validate_platform_manifest(manifest)
    return (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")


def publish_platform_candidate(
    s3: Any,
    bucket: str,
    manifest: Mapping[str, Any],
    candidate_key: str = PLATFORM_CANDIDATE_KEY,
    immutable_prefix: str = "mimir/platform/releases",
) -> dict[str, str]:
    """Publish immutable and candidate roots without changing production."""
    release_id = str(manifest["release_id"])
    immutable_key = f"{immutable_prefix.strip().strip('/')}/{release_id}/manifest.json"
    body = _manifest_body(manifest)
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


def promote_platform_manifest(
    s3: Any,
    bucket: str,
    manifest: Mapping[str, Any],
    current_key: str = PLATFORM_CURRENT_KEY,
) -> dict[str, str]:
    """Atomically change production with one S3 object write."""
    body = _manifest_body(manifest)
    response = s3.put_object(
        Bucket=bucket,
        Key=current_key,
        Body=body,
        ContentType="application/json",
    )
    return {
        "release_id": str(manifest["release_id"]),
        "current_manifest_key": current_key,
        "s3_version_id": str(response.get("VersionId") or ""),
    }


def rollback_platform_manifest(
    s3: Any,
    bucket: str,
    immutable_manifest_key: str,
    current_key: str = PLATFORM_CURRENT_KEY,
) -> dict[str, str]:
    """Restore an exact prior immutable root using the same one-write cutover."""
    response = s3.get_object(Bucket=bucket, Key=immutable_manifest_key)
    raw_body = response["Body"].read()
    manifest = json.loads(raw_body)
    validate_platform_manifest(manifest)
    promoted = s3.put_object(
        Bucket=bucket,
        Key=current_key,
        Body=raw_body,
        ContentType="application/json",
    )
    return {
        "release_id": str(manifest["release_id"]),
        "source_manifest_key": immutable_manifest_key,
        "current_manifest_key": current_key,
        "s3_version_id": str(promoted.get("VersionId") or ""),
    }
