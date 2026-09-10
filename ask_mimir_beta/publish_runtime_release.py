"""Publish Ask Mimir artifacts and a last-written runtime manifest to S3."""

from __future__ import annotations

import argparse
import base64
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
from typing import Any, Dict, Iterable, List, Tuple

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
GENERATED_DATA_FILES = (
    "niin_source_depth.parquet",
    "platform_source_depth.parquet",
    "recent_awards_search.parquet",
)
PINNED_REFERENCE_FILES = (
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


def artifact_files() -> Iterable[Tuple[Path, str]]:
    included = set()
    for source_dir, destination_dir in artifact_directories():
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
    remote = s3.head_object(Bucket=bucket, Key=source_key)
    remote_size = int(remote["ContentLength"])
    if local_file.exists() and local_file.stat().st_size == remote_size:
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
) -> Dict[str, Any]:
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client("s3", region_name="us-east-1")
    generated_at = datetime.now(timezone.utc).isoformat()
    identity = hashlib.sha256(generated_at.encode()).hexdigest()[:12]
    release_id = f"ask-mimir-beta-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{identity}"
    prefix = f"ask_mimir/releases/{release_id}"
    entries: List[Dict[str, Any]] = []

    pinned_serving_entries = []
    for filename in DATA_FILES:
        print(f"Verifying serving data: {filename}", file=sys.stderr)
        pinned_serving_entries.append(
            verified_serving_manifest_entry(
                s3,
                bucket,
                f"app_cache/{filename}",
                f"data/{filename}",
                DATA_ROOT / filename,
            )
        )

    classification_path = build_classification_reference()
    serving_classification_path = DATA_ROOT / classification_path.name
    if (
        not serving_classification_path.exists()
        or file_sha256(serving_classification_path) != file_sha256(classification_path)
    ):
        shutil.copyfile(classification_path, serving_classification_path)
    source_depth_summary = build_platform_source_depth(DATA_ROOT)
    for generated_dir in (
        PRECOMPUTED_COMPANY_CONTEXT_DIR,
        PLATFORM_CONTEXT_DIR,
        STATE_MARKET_DIR,
    ):
        shutil.rmtree(generated_dir, ignore_errors=True)
    market_segment_manifest = build_precomputed_market_segments(
        DATA_ROOT,
        MARKET_SEGMENT_DIR,
    )
    capability_manifest = build_precomputed_capabilities(
        DATA_ROOT,
        CAPABILITY_DIR,
    )
    product_family_manifest = build_precomputed_product_families(
        DATA_ROOT,
        PRODUCT_FAMILY_DIR,
    )
    state_market_manifest = build_precomputed_state_markets(
        DATA_ROOT,
        STATE_MARKET_DIR,
    )
    company_context_manifest = build_precomputed_parent_contexts(
        DATA_ROOT,
        _artifact_source("company-context"),
        PRECOMPUTED_COMPANY_CONTEXT_DIR,
        HIGH_VALUE_PARENT_QUERIES,
        release_id=release_id,
    )
    platform_context_manifest = build_precomputed_platform_contexts(
        DATA_ROOT,
        PLATFORM_CONTEXT_DIR,
        KEY_PLATFORM_CONTEXTS,
        release_id=release_id,
    )
    # Build the largest new serving artifact after spill-heavy dossier generation.
    # This keeps release publication viable on constrained local disks.
    build_recent_awards_search()

    for path, local_path in artifact_files():
        print(f"Publishing artifact: {local_path}", file=sys.stderr)
        key = f"{prefix}/{local_path}"
        s3.upload_file(str(path), bucket, key)
        entries.append(manifest_entry(path, local_path, key))

    entries.extend(pinned_serving_entries)

    for filename in GENERATED_DATA_FILES:
        print(f"Publishing derived serving data: {filename}", file=sys.stderr)
        path = DATA_ROOT / filename
        key = f"{prefix}/data/{filename}"
        s3.upload_file(str(path), bucket, key)
        entries.append(manifest_entry(path, f"data/{filename}", key))

    for source_key, filename in PINNED_REFERENCE_FILES:
        print(f"Pinning reference data: {filename}", file=sys.stderr)
        entries.append(
            serving_manifest_entry(
                s3,
                bucket,
                source_key,
                f"data/{filename}",
                DATA_ROOT / filename,
            )
        )

    classification_key = f"{prefix}/data/{classification_path.name}"
    s3.upload_file(str(classification_path), bucket, classification_key)
    entries.append(
        manifest_entry(
            classification_path,
            f"data/{classification_path.name}",
            classification_key,
        )
    )

    manifest = {
        "release_id": release_id,
        "generated_at": generated_at,
        "metric_release_id": json.loads(
            artifact_source_for_destination("metric-release")
            .joinpath("manifest.json")
            .read_text()
        )["release_id"],
        "derived_release": {
            "platform_source_depth": source_depth_summary,
            "market_segments": market_segment_manifest,
            "capability_markets": capability_manifest,
            "product_families": product_family_manifest,
            "state_markets": state_market_manifest,
            "precomputed_parent_context_count": len(
                company_context_manifest.get("precomputed_parent_queries", [])
            ),
            "precomputed_platform_contexts": platform_context_manifest,
        },
        "files": entries,
    }
    manifest_key = f"{prefix}/runtime_manifest.json"
    manifest_body = json.dumps(manifest, indent=2).encode()
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=manifest_body,
        ContentType="application/json",
    )
    # This is the only mutable object in the release path and is written last.
    s3.put_object(
        Bucket=bucket,
        Key=CURRENT_MANIFEST_KEY,
        Body=manifest_body,
        ContentType="application/json",
    )

    deploy_status = None
    if deploy_hook_url:
        deploy_status = trigger_render_deploy(deploy_hook_url)
    return {
        "release_id": release_id,
        "immutable_manifest_key": manifest_key,
        "current_manifest_key": CURRENT_MANIFEST_KEY,
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
    arguments = parser.parse_args()
    print(
        json.dumps(
            publish(
                arguments.bucket,
                arguments.profile,
                arguments.deploy_hook_url,
            ),
            indent=2,
        )
    )
