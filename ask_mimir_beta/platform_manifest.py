"""Resolve one atomic Mimir platform release from an S3 pointer.

The platform pointer is opt-in at the consumer.  Once selected, resolution is
fail-closed: a consumer never falls back to an independently moving legacy
pointer or mixes child releases from different ETL runs.
"""

from __future__ import annotations

import json
import hashlib
from typing import Any, Iterable


DEFAULT_PLATFORM_MANIFEST_KEY = "mimir/platform/current_manifest.json"
PLATFORM_COMPONENTS = ("main", "public", "ask_mimir")
PLATFORM_V2_COMPONENTS = (*PLATFORM_COMPONENTS, "platform_reference")


class PlatformManifestError(RuntimeError):
    """Raised when an atomic platform release is incomplete or inconsistent."""


def _object_json(
    s3: Any,
    bucket: str,
    key: str,
    version_id: str | None = None,
) -> tuple[dict[str, Any], dict[str, str]]:
    request = {"Bucket": bucket, "Key": key}
    if version_id:
        request["VersionId"] = version_id
    response = s3.get_object(**request)
    try:
        body = response["Body"].read()
        manifest = json.loads(body)
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise PlatformManifestError(f"Invalid JSON manifest at {key}") from error
    if not isinstance(manifest, dict):
        raise PlatformManifestError(f"Manifest at {key} is not a JSON object")
    return manifest, {
        "key": key,
        "version_id": str(response.get("VersionId") or version_id or ""),
        "etag": str(response.get("ETag") or "").strip('"'),
        "sha256": hashlib.sha256(body).hexdigest(),
    }


def _validate_component_reference(
    name: str,
    reference: Any,
    run_id: str,
    schema_version: int,
) -> dict[str, str]:
    if not isinstance(reference, dict):
        raise PlatformManifestError(f"Platform component {name} is not an object")
    release_id = str(reference.get("release_id") or "").strip()
    child_run_id = str(reference.get("etl_run_id") or "").strip()
    manifest_key = str(reference.get("immutable_manifest_key") or "").strip().strip("/")
    if not release_id:
        raise PlatformManifestError(f"Platform component {name} has no release_id")
    if child_run_id != run_id:
        raise PlatformManifestError(
            f"Platform component {name} belongs to {child_run_id or '<missing>'}, "
            f"expected {run_id}"
        )
    if (
        not manifest_key
        or "/releases/" not in manifest_key
        or manifest_key.endswith("current_manifest.json")
        or manifest_key.endswith("candidate_manifest.json")
    ):
        raise PlatformManifestError(
            f"Platform component {name} does not reference an immutable manifest"
        )
    version_id = str(reference.get("immutable_manifest_version_id") or "").strip()
    sha256 = str(reference.get("immutable_manifest_sha256") or "").strip().lower()
    if schema_version == 2 and (not version_id or len(sha256) != 64):
        raise PlatformManifestError(f"Platform component {name} lacks a manifest version or SHA-256")
    return {
        "release_id": release_id,
        "etl_run_id": child_run_id,
        "immutable_manifest_key": manifest_key,
        "immutable_manifest_version_id": version_id,
        "immutable_manifest_sha256": sha256,
    }


def resolve_platform_release(
    s3: Any,
    bucket: str,
    *,
    platform_key: str = DEFAULT_PLATFORM_MANIFEST_KEY,
    platform_version_id: str | None = None,
    components: Iterable[str] = PLATFORM_COMPONENTS,
    allow_candidate_preview: bool = False,
) -> dict[str, Any]:
    """Read one root pointer and validate its immutable child manifests."""
    requested = tuple(dict.fromkeys(str(name) for name in components))
    unknown = set(requested) - set(PLATFORM_V2_COMPONENTS)
    if unknown:
        raise PlatformManifestError(
            "Unknown platform components: " + ", ".join(sorted(unknown))
        )

    platform, platform_object = _object_json(
        s3,
        bucket,
        platform_key,
        platform_version_id,
    )
    run_id = str(platform.get("etl_run_id") or "").strip()
    release_id = str(platform.get("release_id") or "").strip()
    references = platform.get("components")
    schema_version = int(platform.get("schema_version") or 0)
    if schema_version not in (1, 2):
        raise PlatformManifestError("Unsupported platform manifest schema")
    if schema_version == 1 and "platform_reference" in requested:
        raise PlatformManifestError("Platform reference is unavailable in schema 1")
    if schema_version == 2 and not platform.get("promotion_ready") and not allow_candidate_preview:
        raise PlatformManifestError("Schema-2 platform release is not promotion-ready")
    if not run_id or not release_id or not isinstance(references, dict):
        raise PlatformManifestError("Platform manifest is incomplete")
    required_components = PLATFORM_V2_COMPONENTS if schema_version == 2 else PLATFORM_COMPONENTS
    missing_root_components = set(required_components) - set(references)
    if missing_root_components:
        raise PlatformManifestError(
            "Platform manifest is missing components: "
            + ", ".join(sorted(missing_root_components))
        )

    children: dict[str, dict[str, Any]] = {}
    child_objects: dict[str, dict[str, str]] = {}
    for name in requested:
        reference = _validate_component_reference(name, references[name], run_id, schema_version)
        child, child_object = _object_json(
            s3,
            bucket,
            reference["immutable_manifest_key"],
            reference["immutable_manifest_version_id"] or None,
        )
        if schema_version == 2 and child_object["sha256"] != reference["immutable_manifest_sha256"]:
            raise PlatformManifestError(f"Platform component {name} manifest SHA-256 differs from root")
        child_release_id = str(child.get("release_id") or "").strip()
        child_run_id = str(child.get("etl_run_id") or "").strip()
        if child_release_id != reference["release_id"]:
            raise PlatformManifestError(
                f"Platform component {name} resolved release {child_release_id or '<missing>'}, "
                f"expected {reference['release_id']}"
            )
        if child_run_id != run_id:
            raise PlatformManifestError(
                f"Platform component {name} manifest belongs to "
                f"{child_run_id or '<missing>'}, expected {run_id}"
            )
        children[name] = child
        child_objects[name] = child_object

    return {
        "platform": platform,
        "platform_object": platform_object,
        "components": children,
        "component_objects": child_objects,
    }
