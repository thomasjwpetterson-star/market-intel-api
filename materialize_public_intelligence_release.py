"""Build and atomically publish the public-intelligence serving release.

This job runs after the daily ETL has finished publishing its source Parquets.
It performs the expensive joins once with the batch job's memory allocation,
uploads immutable serving files, verifies them, and writes ``current.json``
last.  The web service therefore never has to construct the million-page
corpus during startup and can keep the previous release if this job fails.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import tempfile
import types
from typing import Any

import boto3
import duckdb
from botocore.exceptions import ClientError


DEFAULT_BUCKET = "a-and-d-intel-lake-newaccount"
DEFAULT_CACHE_PREFIX = "app_cache/"
DEFAULT_POINTER_KEY = "app_cache/public_intelligence/current.json"

RELEASE_FILENAMES = (
    "public_intelligence_manifest.parquet",
    "public_company_top_award.parquet",
    "public_company_top_nsn.parquet",
    "public_platform_award_scope.parquet",
    "public_award_profile.parquet",
    "public_solicitation_profile.parquet",
    "public_intelligence_search.parquet",
    "public_company_profile.parquet",
    "public_platform_profile.parquet",
    "public_nsn_profile.parquet",
)

SOURCE_FILES = {
    "v_summary": "summary.parquet",
    "v_profiles": "profiles.parquet",
    "v_geo": "geo.parquet",
    "v_cage_locations": "cage_locations.parquet",
    "v_transactions": "transactions.parquet",
    "v_network": "network.parquet",
    "v_opportunities": "opportunities.parquet",
    "v_nsn_profile_lookup": "nsn_profile_lookup.parquet",
    "v_nsn_summary": "nsn_summary.parquet",
    "v_nsn_supplier_lookup": "nsn_supplier_lookup.parquet",
    "v_nsn_cage_reference": "nsn_cage_reference.parquet",
    "v_nsn_supply_state": "nsn_supply_state_lookup.parquet",
    "v_nsn_price_summary": "nsn_price_summary_lookup.parquet",
    "v_nsn_opportunity_summary": "nsn_opportunity_summary_lookup.parquet",
    "v_nsn_opportunity_detail": "nsn_opportunity_detail.parquet",
    "v_contract_award_metadata": "contract_award_metadata.parquet",
    "v_platform_bom": "platform_bom.parquet",
}


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_file(s3: Any, bucket: str, key: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.unlink(missing_ok=True)
    s3.download_file(bucket, key, str(temporary))
    temporary.replace(destination)


def load_json_object(s3: Any, bucket: str, key: str) -> dict | None:
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except ClientError as exc:
        if str(exc.response.get("Error", {}).get("Code") or "") in {
            "404",
            "NoSuchKey",
            "NotFound",
        }:
            return None
        raise


def download_contracts_rolled(
    s3: Any,
    bucket: str,
    cache_prefix: str,
    source_dir: Path,
) -> str:
    single_key = f"{cache_prefix}contracts_rolled.parquet"
    single_path = source_dir / "contracts_rolled.parquet"
    try:
        s3.head_object(Bucket=bucket, Key=single_key)
        download_file(s3, bucket, single_key, single_path)
        return str(single_path)
    except ClientError as exc:
        if str(exc.response.get("Error", {}).get("Code") or "") not in {
            "404",
            "NoSuchKey",
            "NotFound",
        }:
            raise

    prefix = f"{cache_prefix}contracts_rolled/"
    destination_dir = source_dir / "contracts_rolled"
    paginator = s3.get_paginator("list_objects_v2")
    keys = [
        item["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for item in page.get("Contents", [])
        if str(item.get("Key") or "").endswith(".parquet")
    ]
    if not keys:
        raise RuntimeError("contracts_rolled serving data is missing")
    for index, key in enumerate(keys):
        download_file(s3, bucket, key, destination_dir / f"part-{index:05d}.parquet")
    return str(destination_dir / "*.parquet")


def restore_previous_manifest(
    connection: duckdb.DuckDBPyConnection,
    s3: Any,
    bucket: str,
    pointer_key: str,
    source_dir: Path,
) -> None:
    """Restore the prior manifest solely to preserve truthful lastmod dates."""

    prior = load_json_object(s3, bucket, pointer_key)
    if not prior:
        return
    manifest_entry = next(
        (
            entry
            for entry in prior.get("files") or []
            if entry.get("filename") == "public_intelligence_manifest.parquet"
        ),
        None,
    )
    if not manifest_entry or not manifest_entry.get("s3_key"):
        return
    prior_path = source_dir / "previous_public_intelligence_manifest.parquet"
    download_file(s3, bucket, str(manifest_entry["s3_key"]), prior_path)
    escaped = str(prior_path).replace("'", "''")
    connection.execute(
        f"CREATE TABLE public_intelligence_manifest AS SELECT * FROM read_parquet('{escaped}')"
    )


def derive_release_metadata(output_dir: Path) -> dict:
    """Validate an already-built release and recover its serving metadata."""

    missing = [name for name in RELEASE_FILENAMES if not (output_dir / name).exists()]
    if missing:
        raise RuntimeError("Existing release is incomplete: " + ", ".join(missing))

    connection = duckdb.connect()
    manifest = str(output_dir / "public_intelligence_manifest.parquet").replace("'", "''")
    summary = connection.execute(f"""
        SELECT
            MIN(release_id) AS release_id,
            MAX(release_id) AS max_release_id,
            MAX(schema_version) AS schema_version,
            MIN(quality_gate_version) AS quality_gate_version,
            MAX(quality_gate_version) AS max_quality_gate_version,
            COUNT(*) AS total_entries,
            COUNT(DISTINCT entity_type || ':' || entity_id) AS distinct_entities,
            COUNT(DISTINCT canonical_path) AS distinct_paths
        FROM read_parquet('{manifest}')
    """).fetchone()
    (
        release_id,
        max_release_id,
        schema_version,
        quality_gate_version,
        max_quality_gate_version,
        total_entries,
        distinct_entities,
        distinct_paths,
    ) = summary
    if not release_id or release_id != max_release_id:
        raise RuntimeError("Existing manifest contains mixed release ids")
    if quality_gate_version != max_quality_gate_version:
        raise RuntimeError("Existing manifest contains mixed quality-gate versions")
    if total_entries != distinct_entities or total_entries != distinct_paths:
        raise RuntimeError("Existing manifest contains duplicate entities or canonical paths")

    counts = {
        str(entity_type): int(count)
        for entity_type, count in connection.execute(f"""
            SELECT entity_type, COUNT(*)
            FROM read_parquet('{manifest}')
            GROUP BY entity_type
        """).fetchall()
    }
    expected_profile_counts = {
        "cage_company": "public_company_profile.parquet",
        "platform": "public_platform_profile.parquet",
        "nsn": "public_nsn_profile.parquet",
        "contract_award": "public_award_profile.parquet",
        "solicitation": "public_solicitation_profile.parquet",
    }
    projection_rows = {}
    for entity_type, filename in expected_profile_counts.items():
        source = str(output_dir / filename).replace("'", "''")
        row_count = int(
            connection.execute(f"SELECT COUNT(*) FROM read_parquet('{source}')").fetchone()[0]
        )
        if row_count != counts.get(entity_type, 0):
            raise RuntimeError(
                f"{filename} has {row_count} rows; manifest has {counts.get(entity_type, 0)} {entity_type} rows"
            )
        projection_rows[entity_type] = row_count

    try:
        generated = datetime.strptime(
            str(release_id),
            "public-intelligence-%Y%m%dT%H%M%SZ",
        ).replace(tzinfo=timezone.utc)
        generated_at = generated.isoformat().replace("+00:00", "Z")
    except ValueError:
        generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    sitemap_batch_size = 5_000
    storage_bytes = sum((output_dir / name).stat().st_size for name in RELEASE_FILENAMES)
    return {
        "release_id": str(release_id),
        "generated_at": generated_at,
        "schema_version": int(schema_version),
        "total_entries": int(total_entries),
        "counts": counts,
        "quality_gate_matches": counts,
        "cohort_caps": {
            "cage_company": 100_000,
            "platform": 1_000,
            "contract_award": 120_000,
            "solicitation": 4_000,
            "nsn": 800_000,
        },
        "requested_cohort_size": 1_100_000,
        "quality_gate_version": str(quality_gate_version),
        "sitemap_batch_size": sitemap_batch_size,
        "sitemap_batches": {
            entity_type: math.ceil(count / sitemap_batch_size)
            for entity_type, count in counts.items()
        },
        "platform_exclusions": [
            "COMMON MISSILE SYSTEMS",
            "HEALTHCARE",
            "OTHER ENVIRONMENTAL PROGRAMS",
            "STATUS OF FORCES AGREEMENT",
        ],
        "projection_stats": {
            "build_seconds": None,
            "company_profile_rows": projection_rows.get("cage_company", 0),
            "platform_profile_rows": projection_rows.get("platform", 0),
            "nsn_profile_rows": projection_rows.get("nsn", 0),
            "award_profile_rows": projection_rows.get("contract_award", 0),
            "solicitation_profile_rows": projection_rows.get("solicitation", 0),
            "storage_bytes": storage_bytes,
        },
    }


def publish_release_outputs(
    s3: Any,
    bucket: str,
    cache_prefix: str,
    pointer_key: str,
    output_dir: Path,
    release: dict,
) -> dict:
    """Upload immutable outputs and replace the mutable pointer last."""

    release_id = str(release["release_id"])
    immutable_prefix = f"{cache_prefix}public_intelligence/releases/{release_id}"
    entries = []
    for filename in RELEASE_FILENAMES:
        path = output_dir / filename
        if not path.exists() or path.stat().st_size <= 0:
            raise RuntimeError(f"Release output is missing: {filename}")
        digest = file_sha256(path)
        key = f"{immutable_prefix}/{filename}"
        print(f"Uploading {filename} ({path.stat().st_size:,} bytes)...")
        s3.upload_file(
            str(path),
            bucket,
            key,
            ExtraArgs={"Metadata": {"sha256": digest, "release-id": release_id}},
        )
        head = s3.head_object(Bucket=bucket, Key=key)
        if int(head.get("ContentLength") or 0) != path.stat().st_size:
            raise RuntimeError(f"Uploaded size mismatch for {key}")
        entries.append(
            {
                "filename": filename,
                "s3_key": key,
                "size": path.stat().st_size,
                "sha256": digest,
            }
        )

    pointer = {"release": release, "files": entries}
    pointer_body = json.dumps(pointer, sort_keys=True, indent=2).encode()
    immutable_manifest_key = f"{immutable_prefix}/release.json"
    s3.put_object(
        Bucket=bucket,
        Key=immutable_manifest_key,
        Body=pointer_body,
        ContentType="application/json",
    )
    s3.put_object(
        Bucket=bucket,
        Key=pointer_key,
        Body=pointer_body,
        ContentType="application/json",
    )
    print(
        f"Published {release_id}: {release['total_entries']:,} entities, "
        f"pointer=s3://{bucket}/{pointer_key}"
    )
    return pointer


def publish(
    bucket: str,
    cache_prefix: str,
    pointer_key: str,
    region: str,
) -> dict:
    s3 = boto3.client("s3", region_name=region)
    with tempfile.TemporaryDirectory(prefix="mimir-public-intelligence-") as temporary_root:
        root = Path(temporary_root)
        source_dir = root / "source"
        output_dir = root / "output"
        source_dir.mkdir(parents=True)
        output_dir.mkdir(parents=True)

        print("Downloading daily serving inputs...")
        def download_source(filename: str) -> None:
            download_file(
                s3,
                bucket,
                f"{cache_prefix}{filename}",
                source_dir / filename,
            )

        download_workers = max(
            1,
            min(int(os.getenv("PUBLIC_RELEASE_DOWNLOAD_WORKERS", "5")), len(SOURCE_FILES)),
        )
        with ThreadPoolExecutor(max_workers=download_workers) as executor:
            list(executor.map(download_source, SOURCE_FILES.values()))
        contracts_source = download_contracts_rolled(
            s3,
            bucket,
            cache_prefix,
            source_dir,
        )

        # These variables are read when main is imported.  The batch job gets a
        # larger memory allowance than the web service and explicitly enables
        # the full page projections.
        os.environ["LOCAL_CACHE_DIR"] = str(output_dir)
        os.environ["DUCKDB_MEM"] = os.getenv("PUBLIC_RELEASE_DUCKDB_MEM", "6GB")
        os.environ["DUCKDB_THREADS"] = os.getenv("PUBLIC_RELEASE_DUCKDB_THREADS", "4")
        os.environ["PUBLIC_INTELLIGENCE_BUILD_PAGE_PROFILES"] = "1"
        os.environ.setdefault("OPENAI_API_KEY", "public-release-build-not-used")

        # The legacy API module owns the release-manifest builder today, but
        # the batch path never loads .env files or invokes an OpenAI client.
        # Older ETL environments predate these two API-only dependencies; use
        # inert compatibility modules rather than making the data job depend on
        # unrelated runtime integrations. Normal API imports remain unchanged.
        try:
            import dotenv  # noqa: F401
        except ModuleNotFoundError:
            dotenv_module = types.ModuleType("dotenv")
            dotenv_module.load_dotenv = lambda *args, **kwargs: False
            sys.modules["dotenv"] = dotenv_module
        try:
            import openai  # noqa: F401
        except ModuleNotFoundError:
            openai_module = types.ModuleType("openai")
            openai_module.AsyncOpenAI = lambda *args, **kwargs: None
            sys.modules["openai"] = openai_module

        import main as api

        api.LOCAL_CACHE_DIR = output_dir
        connection = duckdb.connect(str(root / "public-release.duckdb"))
        api._apply_duck_pragmas(connection)
        for view_name, filename in SOURCE_FILES.items():
            source = str(source_dir / filename).replace("'", "''")
            connection.execute(
                f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{source}')"
            )

        contracts_glob = contracts_source.replace("'", "''")
        connection.execute(
            f"CREATE OR REPLACE VIEW v_contracts_rolled AS SELECT * FROM read_parquet('{contracts_glob}')"
        )
        connection.execute("""
            CREATE OR REPLACE VIEW v_subcontracts AS
            SELECT
                CAST(prime_cage AS VARCHAR) AS prime_cage,
                CAST(sub_cage AS VARCHAR) AS subcontractor_cage,
                CAST(contract_id AS VARCHAR) AS prime_award_id,
                CAST(action_date AS VARCHAR) AS subcontract_action_date,
                TRY_CAST(subaward_value AS DOUBLE) AS subcontract_value_usd
            FROM v_network
        """)
        restore_previous_manifest(
            connection,
            s3,
            bucket,
            pointer_key,
            source_dir,
        )

        print("Building quality-gated manifest and page projections...")
        result = api.build_public_intelligence_release(connection)
        release = result["release"]
        if tuple(api.PUBLIC_INTELLIGENCE_RELEASE_FILES) != RELEASE_FILENAMES:
            raise RuntimeError("Publisher and API release-file contracts differ")
        return publish_release_outputs(
            s3,
            bucket,
            cache_prefix,
            pointer_key,
            output_dir,
            release,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bucket",
        default=os.getenv("PUBLIC_RELEASE_BUCKET", os.getenv("ATHENA_OUTPUT_BUCKET", DEFAULT_BUCKET)).replace("s3://", "").split("/")[0],
    )
    parser.add_argument("--cache-prefix", default=os.getenv("PUBLIC_RELEASE_CACHE_PREFIX", DEFAULT_CACHE_PREFIX))
    parser.add_argument("--pointer-key", default=os.getenv("PUBLIC_INTELLIGENCE_POINTER_KEY", DEFAULT_POINTER_KEY))
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"))
    parser.add_argument(
        "--existing-output-dir",
        type=Path,
        help="Validate and publish an already-built release without downloading source data.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.existing_output_dir:
        existing_output_dir = args.existing_output_dir.resolve()
        existing_release = derive_release_metadata(existing_output_dir)
        existing_s3 = boto3.client("s3", region_name=args.region)
        publish_release_outputs(
            existing_s3,
            args.bucket,
            args.cache_prefix,
            args.pointer_key,
            existing_output_dir,
            existing_release,
        )
    else:
        publish(
            bucket=args.bucket,
            cache_prefix=args.cache_prefix,
            pointer_key=args.pointer_key,
            region=args.region,
        )
