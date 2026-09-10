"""Publish Ask Mimir artifacts and a last-written runtime manifest to S3."""

from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import json
import os
import shutil
import struct
import sys
import tempfile
import urllib.request
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

import boto3
import duckdb

from bootstrap_data import DEFAULT_BUCKET, file_sha256
from build_platform_source_depth import build_platform_source_depth
from capability_discovery import build_precomputed_capabilities
from company_context_store import build_precomputed_parent_contexts
from geographic_market import build_precomputed_state_markets
from market_segment import build_precomputed_market_segments
from platform_context import build_precomputed_platform_contexts
from product_intelligence import build_precomputed_product_families


ROOT = Path(__file__).resolve().parent
CURRENT_MANIFEST_KEY = "ask_mimir/runtime/current_manifest.json"
CANDIDATE_MANIFEST_KEY = "ask_mimir/runtime/candidate_manifest.json"
DATA_ROOT = Path(
    os.getenv("ASK_MIMIR_SERVING_DATA_ROOT", str(ROOT.parent / "local_data"))
).resolve()
DATA_FILES = (
    "transactions.parquet",
    "network.parquet",
    "nsn_supplier_lookup.parquet",
    "nsn_profile_lookup.parquet",
    "nsn_cage_reference.parquet",
    "platform_bom.parquet",
    "cage_locations.parquet",
    "geo.parquet",
    "opportunities.parquet",
    "contracts_rolled.parquet",
    "profiles.parquet",
)
PINNED_REFERENCE_FILES = (
    (
        "silver/dod_budget/ref_budget_facts/data/pb_fy2027/dod_budget_facts.parquet",
        "dod_budget_facts.parquet",
    ),
    (
        "silver/dod_budget/ref_fydp_budget_facts/releases/pb_fy2027_v1/data/dod_fydp_budget_facts.parquet",
        "dod_fydp_budget_facts.parquet",
    ),
)
CLASSIFICATION_REFERENCE = ROOT / "validation-output" / "classification_reference.parquet"
MARKET_SEGMENT_DIR = ROOT / "validation-output" / "market-segments"
CAPABILITY_DIR = ROOT / "validation-output" / "capability-markets"
PRODUCT_FAMILY_DIR = ROOT / "validation-output" / "product-families"
PRECOMPUTED_COMPANY_CONTEXT_DIR = (
    ROOT / "validation-output" / "precomputed-company-contexts"
)
PLATFORM_CONTEXT_DIR = ROOT / "validation-output" / "platform-contexts"
STATE_MARKET_DIR = ROOT / "validation-output" / "state-markets"
RUNTIME_ARTIFACT_ROOT = ROOT / ".runtime-data" / "artifacts"

HIGH_VALUE_PARENT_QUERIES = (
    "Lockheed Martin",
    "RTX",
    "Boeing",
    "Northrop Grumman",
    "General Dynamics",
    "BAE Systems",
    "L3Harris",
    "Honeywell",
    "Huntington Ingalls",
    "Textron",
    "Parker Hannifin",
    "TransDigm",
    "Curtiss-Wright",
)

KEY_PLATFORM_CONTEXTS = (
    "F-35",
    "F-16",
    "F-15",
    "F/A-18",
    "B-52",
    "CH-53K",
    "UH-60",
    "CH-47",
    "AH-64",
    "HIMARS",
    "M1 ABRAMS",
    "STRYKER",
    "TOMAHAWK",
    "JASSM",
    "AMRAAM",
    "SM-6",
    "PATRIOT AIR DEFENSE SYSTEM",
    "VIRGINIA CLASS (SSN 774)",
    "COLUMBIA CLASS SSBN",
)

BUILD_DOMAINS = (
    "serving-data",
    "references",
    "classification",
    "source-depth",
    "recent-awards",
    "metrics",
    "core-packs",
    "segments",
    "capabilities",
    "products",
    "companies",
    "platforms",
    "states",
)

DOMAIN_LOCAL_PATHS = {
    "serving-data": tuple(f"data/{name}" for name in DATA_FILES),
    "references": tuple(f"data/{name}" for _, name in PINNED_REFERENCE_FILES),
    "classification": ("data/classification_reference.parquet",),
    "source-depth": (
        "data/niin_source_depth.parquet",
        "data/platform_source_depth.parquet",
    ),
    "recent-awards": ("data/recent_awards_search.parquet",),
}

DOMAIN_ARTIFACT_DESTINATIONS = {
    "metrics": ("metric-release",),
    "core-packs": (
        "company-opportunities",
        "platform-supply-chains",
        "program-momentum",
    ),
    "segments": ("market-segments",),
    "capabilities": ("capability-markets",),
    "products": ("product-families",),
    "companies": ("company-context",),
    "platforms": ("platform-contexts",),
    "states": ("state-markets",),
}


def _artifact_source(name: str) -> Path:
    preferred = ROOT / "validation-output" / name
    if (preferred / "manifest.json").exists():
        return preferred
    fallback = RUNTIME_ARTIFACT_ROOT / name
    if (fallback / "manifest.json").exists():
        return fallback
    raise FileNotFoundError(
        f"Ask Mimir artifact pack is unavailable for {name}: "
        f"checked {preferred} and {fallback}"
    )


def artifact_directories() -> Tuple[Tuple[Path, str], ...]:
    validation_root = ROOT / "validation-output"
    metric_source = (
        validation_root
        if (validation_root / "manifest.json").exists()
        else _artifact_source("metric-release")
    )
    return (
        (metric_source, "metric-release"),
        (
            PRECOMPUTED_COMPANY_CONTEXT_DIR
            if (PRECOMPUTED_COMPANY_CONTEXT_DIR / "manifest.json").exists()
            else _artifact_source("company-context"),
            "company-context",
        ),
        (_artifact_source("company-opportunities"), "company-opportunities"),
        (_artifact_source("platform-supply-chains"), "platform-supply-chains"),
        (_artifact_source("program-momentum"), "program-momentum"),
        (MARKET_SEGMENT_DIR, "market-segments"),
        (CAPABILITY_DIR, "capability-markets"),
        (PRODUCT_FAMILY_DIR, "product-families"),
        (PLATFORM_CONTEXT_DIR, "platform-contexts"),
        (STATE_MARKET_DIR, "state-markets"),
    )


def artifact_source_for_destination(destination: str) -> Path:
    return next(
        source
        for source, artifact_destination in artifact_directories()
        if artifact_destination == destination
    )


def artifact_files(
    destinations: Iterable[str] | None = None,
) -> Iterable[Tuple[Path, str]]:
    selected = None if destinations is None else set(destinations)
    included = set()
    for source_dir, destination_dir in artifact_directories():
        if selected is not None and destination_dir not in selected:
            continue
        for path in sorted(source_dir.rglob("*")):
            if (
                not path.is_file()
                or "duckdb_tmp" in path.parts
                or ".dynamic-cache" in path.parts
            ):
                continue
            if destination_dir == "metric-release" and path.parent != source_dir:
                continue
            destination = f"artifacts/{destination_dir}/{path.relative_to(source_dir)}"
            if destination not in included:
                included.add(destination)
                yield path, destination


def manifest_entry(path: Path, local_path: str, s3_key: str) -> Dict[str, Any]:
    return {
        "local_path": local_path,
        "s3_key": s3_key,
        "size": path.stat().st_size,
        "sha256": file_sha256(path),
    }


def serving_manifest_entry(
    s3: Any,
    bucket: str,
    source_key: str,
    local_path: str,
    local_file: Path,
) -> Dict[str, Any]:
    remote = s3.head_object(Bucket=bucket, Key=source_key, ChecksumMode="ENABLED")
    remote_size = int(remote["ContentLength"])
    metadata_hash = str((remote.get("Metadata") or {}).get("sha256") or "").strip()
    if metadata_hash:
        digest = metadata_hash
    elif local_file.exists() and local_file.stat().st_size == remote_size:
        digest = file_sha256(local_file)
    else:
        with tempfile.NamedTemporaryFile() as temporary_file:
            s3.download_fileobj(bucket, source_key, temporary_file)
            temporary_file.flush()
            digest = file_sha256(Path(temporary_file.name))
    entry = {
        "local_path": local_path,
        "s3_key": source_key,
        "size": remote_size,
        "sha256": digest,
    }
    version_id = str(remote.get("VersionId") or "").strip()
    if not version_id or version_id == "null":
        raise RuntimeError(
            f"S3 versioning is required for atomic release input: {source_key}"
        )
    entry["s3_version_id"] = version_id
    etag = str(remote.get("ETag") or "").strip('"')
    if etag:
        entry["s3_etag"] = etag
    for field in (
        "ChecksumCRC32",
        "ChecksumCRC32C",
        "ChecksumCRC64NVME",
        "ChecksumSHA1",
        "ChecksumSHA256",
    ):
        value = str(remote.get(field) or "").strip()
        if value:
            entry[f"s3_{field.lower()}"] = value
    checksum_type = str(remote.get("ChecksumType") or "").strip()
    if checksum_type:
        entry["s3_checksum_type"] = checksum_type
    return entry


def remote_serving_manifest_entry(
    s3: Any,
    bucket: str,
    source_key: str,
    local_path: str,
) -> Dict[str, Any]:
    """Pin an app-cache object without transferring its contents locally."""
    remote = s3.head_object(Bucket=bucket, Key=source_key, ChecksumMode="ENABLED")
    version_id = str(remote.get("VersionId") or "").strip()
    if not version_id or version_id == "null":
        raise RuntimeError(
            f"S3 versioning is required for atomic release input: {source_key}"
        )
    entry = {
        "local_path": local_path,
        "s3_key": source_key,
        "size": int(remote["ContentLength"]),
        "s3_version_id": version_id,
    }
    metadata_hash = str((remote.get("Metadata") or {}).get("sha256") or "").strip()
    if metadata_hash:
        entry["sha256"] = metadata_hash
    etag = str(remote.get("ETag") or "").strip('"')
    if etag:
        entry["s3_etag"] = etag
    for field in (
        "ChecksumCRC32",
        "ChecksumCRC32C",
        "ChecksumCRC64NVME",
        "ChecksumSHA1",
        "ChecksumSHA256",
    ):
        value = str(remote.get(field) or "").strip()
        if value:
            entry[f"s3_{field.lower()}"] = value
    checksum_type = str(remote.get("ChecksumType") or "").strip()
    if checksum_type:
        entry["s3_checksum_type"] = checksum_type
    return entry


def verified_serving_manifest_entry(
    s3: Any,
    bucket: str,
    source_key: str,
    local_path: str,
    local_file: Path,
) -> Dict[str, Any]:
    """Pin and byte-verify one S3 serving object without downloading it again."""
    if not local_file.exists():
        raise FileNotFoundError(f"local serving input is missing: {local_file}")

    attributes = s3.get_object_attributes(
        Bucket=bucket,
        Key=source_key,
        ObjectAttributes=["Checksum", "ObjectParts", "ObjectSize"],
        MaxParts=1000,
    )
    version_id = str(attributes.get("VersionId") or "").strip()
    if not version_id or version_id == "null":
        raise RuntimeError(
            f"S3 versioning is required for atomic release input: {source_key}"
        )
    remote_size = int(attributes["ObjectSize"])
    if local_file.stat().st_size != remote_size:
        raise RuntimeError(
            f"local serving input is stale: {local_file.name} has "
            f"{local_file.stat().st_size} bytes; pinned S3 object has {remote_size}"
        )

    parts = list(attributes.get("ObjectParts", {}).get("Parts", []))
    part_marker = attributes.get("ObjectParts", {}).get("NextPartNumberMarker")
    while attributes.get("ObjectParts", {}).get("IsTruncated"):
        attributes = s3.get_object_attributes(
            Bucket=bucket,
            Key=source_key,
            VersionId=version_id,
            ObjectAttributes=["Checksum", "ObjectParts", "ObjectSize"],
            MaxParts=1000,
            PartNumberMarker=int(part_marker),
        )
        parts.extend(attributes.get("ObjectParts", {}).get("Parts", []))
        part_marker = attributes.get("ObjectParts", {}).get("NextPartNumberMarker")

    digest = hashlib.sha256()
    full_crc = 0
    bytes_read = 0
    with local_file.open("rb") as handle:
        if parts:
            for part in parts:
                chunk = handle.read(int(part["Size"]))
                if len(chunk) != int(part["Size"]):
                    raise RuntimeError(f"local input ended early: {local_file.name}")
                digest.update(chunk)
                bytes_read += len(chunk)
                expected_crc = str(part.get("ChecksumCRC32") or "")
                actual_crc = base64.b64encode(
                    struct.pack(">I", zlib.crc32(chunk) & 0xFFFFFFFF)
                ).decode()
                if not expected_crc or actual_crc != expected_crc:
                    raise RuntimeError(
                        f"local serving input does not match pinned S3 version: "
                        f"{local_file.name}, part {part['PartNumber']}"
                    )
        else:
            while chunk := handle.read(8 * 1024 * 1024):
                digest.update(chunk)
                full_crc = zlib.crc32(chunk, full_crc)
                bytes_read += len(chunk)
            expected_crc = str(
                attributes.get("Checksum", {}).get("ChecksumCRC32") or ""
            )
            actual_crc = base64.b64encode(
                struct.pack(">I", full_crc & 0xFFFFFFFF)
            ).decode()
            if not expected_crc or actual_crc != expected_crc:
                raise RuntimeError(
                    f"local serving input does not match pinned S3 version: "
                    f"{local_file.name}"
                )
        if handle.read(1):
            raise RuntimeError(f"local input has unexpected trailing bytes: {local_file.name}")

    if bytes_read != remote_size:
        raise RuntimeError(f"local input size changed while reading: {local_file.name}")
    return {
        "local_path": local_path,
        "s3_key": source_key,
        "size": remote_size,
        "sha256": digest.hexdigest(),
        "s3_version_id": version_id,
    }


def trigger_render_deploy(deploy_hook_url: str) -> int:
    request = urllib.request.Request(deploy_hook_url, method="POST")
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status not in {200, 202}:
            raise RuntimeError(f"Render deploy hook returned HTTP {response.status}")
        return response.status


def parse_domains(value: str | None) -> set[str] | None:
    if value is None:
        return None
    domains = {item.strip().lower() for item in value.split(",") if item.strip()}
    unknown = domains.difference(BUILD_DOMAINS)
    if unknown:
        raise ValueError(
            "Unknown release domain(s): "
            + ", ".join(sorted(unknown))
            + ". Valid domains: "
            + ", ".join(BUILD_DOMAINS)
        )
    if not domains:
        raise ValueError("--only requires at least one release domain")
    return domains


def load_manifest(s3: Any, bucket: str, key: str) -> Dict[str, Any]:
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read())


def manifest_file_map(manifest: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    entries: Dict[str, Dict[str, Any]] = {}
    for raw_entry in manifest.get("files", []):
        entry = dict(raw_entry)
        local_path = str(entry.get("local_path") or "").strip()
        if not local_path:
            raise RuntimeError("Runtime manifest contains a file without local_path")
        if local_path in entries:
            raise RuntimeError(f"Duplicate runtime path in manifest: {local_path}")
        entries[local_path] = entry
    return entries


def validate_release_manifest(
    s3: Any,
    bucket: str,
    manifest: Dict[str, Any],
) -> None:
    if not manifest.get("release_id"):
        raise RuntimeError("Runtime manifest has no release_id")
    entries = manifest_file_map(manifest)
    required = {f"data/{name}" for name in DATA_FILES}
    missing = sorted(required.difference(entries))
    if missing:
        raise RuntimeError(
            "Runtime manifest is missing required serving data: " + ", ".join(missing)
        )
    for local_path, entry in entries.items():
        request = {"Bucket": bucket, "Key": entry["s3_key"]}
        version_id = str(entry.get("s3_version_id") or "").strip()
        if version_id and version_id != "null":
            request["VersionId"] = version_id
        remote = s3.head_object(**request)
        if int(remote["ContentLength"]) != int(entry["size"]):
            raise RuntimeError(
                f"Published object size does not match manifest: {local_path}"
            )
        expected_etag = str(entry.get("s3_etag") or "").strip()
        actual_etag = str(remote.get("ETag") or "").strip('"')
        if expected_etag and actual_etag != expected_etag:
            raise RuntimeError(
                f"Published object ETag does not match manifest: {local_path}"
            )


def validate_local_inputs_against_base(
    base_manifest: Dict[str, Any],
    filenames: Iterable[str] = DATA_FILES,
) -> None:
    """Ensure incremental builders consume the data pinned by the base release."""
    entries = manifest_file_map(base_manifest)
    for filename in filenames:
        local_path = f"data/{filename}"
        entry = entries.get(local_path)
        path = DATA_ROOT / filename
        if entry is None:
            raise RuntimeError(f"Base manifest is missing {local_path}")
        if not path.exists():
            raise FileNotFoundError(f"Local release input is missing: {path}")
        if path.stat().st_size != int(entry["size"]):
            raise RuntimeError(
                f"Local input differs from base release: {filename} has a different size. "
                "Include serving-data in --only when publishing refreshed cache data."
            )
        if file_sha256(path) != entry["sha256"]:
            raise RuntimeError(
                f"Local input differs from base release: {filename} has a different hash. "
                "Include serving-data in --only when publishing refreshed cache data."
            )


def remove_domain_entries(
    entries: Dict[str, Dict[str, Any]],
    domain: str,
) -> None:
    for local_path in DOMAIN_LOCAL_PATHS.get(domain, ()):
        entries.pop(local_path, None)
    for destination in DOMAIN_ARTIFACT_DESTINATIONS.get(domain, ()):
        prefix = f"artifacts/{destination}/"
        for local_path in [path for path in entries if path.startswith(prefix)]:
            entries.pop(local_path, None)


def write_release_pointer(
    s3: Any,
    bucket: str,
    key: str,
    manifest_body: bytes,
) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=manifest_body,
        ContentType="application/json",
    )


def build_classification_reference() -> Path:
    summary = DATA_ROOT / "summary.parquet"
    if not summary.exists():
        raise FileNotFoundError(f"classification source was not found: {summary}")
    CLASSIFICATION_REFERENCE.parent.mkdir(parents=True, exist_ok=True)
    summary_sql = str(summary).replace("'", "''")
    output_sql = str(CLASSIFICATION_REFERENCE).replace("'", "''")
    with duckdb.connect() as connection:
        connection.execute(
            f"""
            COPY (
                SELECT
                    'PSC' AS classification_type,
                    TRIM(psc_code) AS code,
                    MODE(psc_description) AS description
                FROM read_parquet('{summary_sql}')
                WHERE COALESCE(TRIM(psc_code), '') <> ''
                  AND COALESCE(TRIM(psc_description), '') <> ''
                GROUP BY 2
                UNION ALL
                SELECT
                    'NAICS' AS classification_type,
                    TRIM(naics_code) AS code,
                    MODE(naics_description) AS description
                FROM read_parquet('{summary_sql}')
                WHERE COALESCE(TRIM(naics_code), '') <> ''
                  AND COALESCE(TRIM(naics_description), '') <> ''
                GROUP BY 2
            ) TO '{output_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    return CLASSIFICATION_REFERENCE


def build_recent_awards_search() -> Path:
    """Materialize the bounded recent-award corpus used by natural-language search."""
    source = DATA_ROOT / "contracts_rolled.parquet"
    output = DATA_ROOT / "recent_awards_search.parquet"
    if not source.exists():
        raise FileNotFoundError(f"recent-award search source was not found: {source}")
    source_sql = str(source).replace("'", "''")
    output_sql = str(output).replace("'", "''")
    with duckdb.connect() as connection:
        connection.execute("SET preserve_insertion_order=false")
        connection.execute("SET threads=4")
        connection.execute("SET memory_limit='1GB'")
        connection.execute(
            f"""
            COPY (
                SELECT
                    contract_id,
                    award_key,
                    vendor_name,
                    vendor_cage,
                    parent_agency,
                    sub_agency,
                    psc,
                    naics_code,
                    platform_family,
                    total_spend,
                    last_action_date,
                    base_award_description,
                    latest_action_description,
                    description,
                    UPPER(
                        COALESCE(base_award_description, '') || ' ' ||
                        COALESCE(latest_action_description, description, '')
                    ) AS search_text
                FROM read_parquet('{source_sql}')
                WHERE source_system = 'USA_SPENDING'
                  AND year BETWEEN 2025 AND 2026
            ) TO '{output_sql}' (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                ROW_GROUP_SIZE 100000
            )
            """
        )
    return output


def publish(
    bucket: str,
    profile: str | None,
    deploy_hook_url: str | None = None,
    domains: set[str] | None = None,
    base_manifest_key: str = CURRENT_MANIFEST_KEY,
    candidate_manifest_key: str = CANDIDATE_MANIFEST_KEY,
    promote: bool = False,
    verify_local_inputs: bool = True,
) -> Dict[str, Any]:
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client("s3", region_name="us-east-1")
    selected_domains = set(BUILD_DOMAINS if domains is None else domains)
    incremental = domains is not None
    base_manifest = load_manifest(s3, bucket, base_manifest_key) if incremental else None
    if incremental and "serving-data" not in selected_domains and verify_local_inputs:
        print("Verifying local inputs against the base release", file=sys.stderr)
        validate_local_inputs_against_base(base_manifest)

    generated_at = datetime.now(timezone.utc).isoformat()
    identity = hashlib.sha256(generated_at.encode()).hexdigest()[:12]
    release_id = f"ask-mimir-beta-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{identity}"
    prefix = f"ask_mimir/releases/{release_id}"
    entries_by_path = (
        manifest_file_map(base_manifest) if base_manifest is not None else {}
    )
    base_entries_by_path = copy.deepcopy(entries_by_path)
    for domain in selected_domains:
        remove_domain_entries(entries_by_path, domain)

    derived_release = copy.deepcopy(
        (base_manifest or {}).get("derived_release", {})
    )
    metric_release_id = (base_manifest or {}).get("metric_release_id")

    if "serving-data" in selected_domains:
        for filename in DATA_FILES:
            local_path = f"data/{filename}"
            source_key = f"app_cache/{filename}"
            if verify_local_inputs:
                print(f"Verifying serving data locally: {filename}", file=sys.stderr)
                entry = verified_serving_manifest_entry(
                    s3,
                    bucket,
                    source_key,
                    local_path,
                    DATA_ROOT / filename,
                )
            else:
                print(f"Pinning serving data remotely: {filename}", file=sys.stderr)
                remote_entry = remote_serving_manifest_entry(
                    s3,
                    bucket,
                    source_key,
                    local_path,
                )
                base_entry = base_entries_by_path.get(local_path)
                if (
                    base_entry
                    and base_entry.get("s3_key") == remote_entry.get("s3_key")
                    and base_entry.get("s3_version_id")
                    == remote_entry.get("s3_version_id")
                ):
                    entry = base_entry
                else:
                    entry = remote_entry
            entries_by_path[entry["local_path"]] = entry

    classification_path = None
    if "classification" in selected_domains:
        classification_path = build_classification_reference()
        serving_classification_path = DATA_ROOT / classification_path.name
        if (
            not serving_classification_path.exists()
            or file_sha256(serving_classification_path) != file_sha256(classification_path)
        ):
            shutil.copyfile(classification_path, serving_classification_path)

    if "source-depth" in selected_domains:
        derived_release["platform_source_depth"] = build_platform_source_depth(
            DATA_ROOT
        )

    if "companies" in selected_domains:
        shutil.rmtree(PRECOMPUTED_COMPANY_CONTEXT_DIR, ignore_errors=True)
    if "platforms" in selected_domains:
        shutil.rmtree(PLATFORM_CONTEXT_DIR, ignore_errors=True)
    if "states" in selected_domains:
        shutil.rmtree(STATE_MARKET_DIR, ignore_errors=True)

    if "segments" in selected_domains:
        derived_release["market_segments"] = build_precomputed_market_segments(
            DATA_ROOT,
            MARKET_SEGMENT_DIR,
        )
    if "capabilities" in selected_domains:
        derived_release["capability_markets"] = build_precomputed_capabilities(
            DATA_ROOT,
            CAPABILITY_DIR,
        )
    if "products" in selected_domains:
        derived_release["product_families"] = build_precomputed_product_families(
            DATA_ROOT,
            PRODUCT_FAMILY_DIR,
        )
    if "states" in selected_domains:
        derived_release["state_markets"] = build_precomputed_state_markets(
            DATA_ROOT,
            STATE_MARKET_DIR,
        )
    if "companies" in selected_domains:
        company_context_manifest = build_precomputed_parent_contexts(
            DATA_ROOT,
            _artifact_source("company-context"),
            PRECOMPUTED_COMPANY_CONTEXT_DIR,
            HIGH_VALUE_PARENT_QUERIES,
            release_id=release_id,
        )
        derived_release["precomputed_parent_context_count"] = len(
            company_context_manifest.get("precomputed_parent_queries", [])
        )
    if "platforms" in selected_domains:
        derived_release["precomputed_platform_contexts"] = (
            build_precomputed_platform_contexts(
                DATA_ROOT,
                PLATFORM_CONTEXT_DIR,
                KEY_PLATFORM_CONTEXTS,
                release_id=release_id,
            )
        )

    # This is deliberately last because it is the largest generated serving file.
    if "recent-awards" in selected_domains:
        build_recent_awards_search()

    artifact_destinations = {
        destination
        for domain in selected_domains
        for destination in DOMAIN_ARTIFACT_DESTINATIONS.get(domain, ())
    }
    for path, local_path in artifact_files(artifact_destinations):
        print(f"Publishing artifact: {local_path}", file=sys.stderr)
        key = f"{prefix}/{local_path}"
        s3.upload_file(str(path), bucket, key)
        entry = manifest_entry(path, local_path, key)
        entries_by_path[local_path] = entry

    if "metrics" in selected_domains:
        metric_release_id = json.loads(
            artifact_source_for_destination("metric-release")
            .joinpath("manifest.json")
            .read_text()
        )["release_id"]

    generated_by_domain = {
        "source-depth": (
            "niin_source_depth.parquet",
            "platform_source_depth.parquet",
        ),
        "recent-awards": ("recent_awards_search.parquet",),
    }
    for domain, filenames in generated_by_domain.items():
        if domain not in selected_domains:
            continue
        for filename in filenames:
            print(f"Publishing derived serving data: {filename}", file=sys.stderr)
            path = DATA_ROOT / filename
            key = f"{prefix}/data/{filename}"
            s3.upload_file(str(path), bucket, key)
            entry = manifest_entry(path, f"data/{filename}", key)
            entries_by_path[entry["local_path"]] = entry

    if "references" in selected_domains:
        for source_key, filename in PINNED_REFERENCE_FILES:
            print(f"Pinning reference data: {filename}", file=sys.stderr)
            entry = serving_manifest_entry(
                s3,
                bucket,
                source_key,
                f"data/{filename}",
                DATA_ROOT / filename,
            )
            entries_by_path[entry["local_path"]] = entry

    if classification_path is not None:
        classification_key = f"{prefix}/data/{classification_path.name}"
        s3.upload_file(str(classification_path), bucket, classification_key)
        entry = manifest_entry(
            classification_path,
            f"data/{classification_path.name}",
            classification_key,
        )
        entries_by_path[entry["local_path"]] = entry

    manifest = {
        "release_id": release_id,
        "generated_at": generated_at,
        "base_release_id": (base_manifest or {}).get("release_id"),
        "updated_domains": sorted(selected_domains),
        "metric_release_id": metric_release_id,
        "derived_release": derived_release,
        "files": [entries_by_path[path] for path in sorted(entries_by_path)],
    }
    manifest_key = f"{prefix}/runtime_manifest.json"
    manifest_body = json.dumps(manifest, indent=2).encode()
    write_release_pointer(s3, bucket, manifest_key, manifest_body)
    validate_release_manifest(s3, bucket, manifest)
    write_release_pointer(s3, bucket, candidate_manifest_key, manifest_body)

    current_manifest_key = None
    if promote:
        write_release_pointer(s3, bucket, CURRENT_MANIFEST_KEY, manifest_body)
        current_manifest_key = CURRENT_MANIFEST_KEY

    deploy_status = None
    if deploy_hook_url and promote:
        deploy_status = trigger_render_deploy(deploy_hook_url)
    return {
        "release_id": release_id,
        "base_release_id": (base_manifest or {}).get("release_id"),
        "updated_domains": sorted(selected_domains),
        "immutable_manifest_key": manifest_key,
        "candidate_manifest_key": candidate_manifest_key,
        "current_manifest_key": current_manifest_key,
        "promoted": promote,
        "render_deploy_status": deploy_status,
    }


def promote_candidate(
    bucket: str,
    profile: str | None,
    candidate_manifest_key: str = CANDIDATE_MANIFEST_KEY,
    deploy_hook_url: str | None = None,
) -> Dict[str, Any]:
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client("s3", region_name="us-east-1")
    manifest = load_manifest(s3, bucket, candidate_manifest_key)
    validate_release_manifest(s3, bucket, manifest)
    manifest_body = json.dumps(manifest, indent=2).encode()
    write_release_pointer(s3, bucket, CURRENT_MANIFEST_KEY, manifest_body)
    deploy_status = (
        trigger_render_deploy(deploy_hook_url) if deploy_hook_url else None
    )
    return {
        "release_id": manifest["release_id"],
        "candidate_manifest_key": candidate_manifest_key,
        "current_manifest_key": CURRENT_MANIFEST_KEY,
        "promoted": True,
        "render_deploy_status": deploy_status,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--profile")
    parser.add_argument(
        "--deploy-hook-url",
        default=os.getenv("ASK_MIMIR_RENDER_DEPLOY_HOOK_URL"),
    )
    parser.add_argument(
        "--only",
        help="Comma-separated release domains. Omit for a complete rebuild.",
    )
    parser.add_argument(
        "--base-manifest-key",
        default=CURRENT_MANIFEST_KEY,
        help="Manifest whose unchanged files are reused for an incremental release.",
    )
    parser.add_argument(
        "--candidate-manifest-key",
        default=CANDIDATE_MANIFEST_KEY,
    )
    parser.add_argument(
        "--promote",
        action="store_true",
        help="Promote the validated release to the production pointer immediately.",
    )
    parser.add_argument(
        "--promote-candidate",
        action="store_true",
        help="Validate and promote the existing candidate without rebuilding it.",
    )
    parser.add_argument(
        "--skip-local-input-verification",
        action="store_true",
        help=(
            "Reuse verified base entries and pin serving-data inputs directly from "
            "versioned S3 objects without downloading them locally."
        ),
    )
    arguments = parser.parse_args()
    if arguments.promote_candidate and (arguments.only or arguments.promote):
        parser.error("--promote-candidate cannot be combined with --only or --promote")
    if arguments.promote_candidate:
        result = promote_candidate(
            arguments.bucket,
            arguments.profile,
            arguments.candidate_manifest_key,
            arguments.deploy_hook_url,
        )
    else:
        result = publish(
            arguments.bucket,
            arguments.profile,
            arguments.deploy_hook_url,
            domains=parse_domains(arguments.only),
            base_manifest_key=arguments.base_manifest_key,
            candidate_manifest_key=arguments.candidate_manifest_key,
            promote=arguments.promote,
            verify_local_inputs=not arguments.skip_local_input_verification,
        )
    print(
        json.dumps(result, indent=2)
    )
