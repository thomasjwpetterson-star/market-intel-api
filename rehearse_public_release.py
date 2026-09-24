"""Build a public release from version-pinned S3 inputs without touching live keys."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import time

import boto3
import duckdb


SOURCE_VIEWS = {
    "v_summary": "summary.parquet",
    "v_profiles": "profiles.parquet",
    "v_geo": "geo.parquet",
    "v_contracts_rolled": "contracts_rolled.parquet",
    "v_opportunities": "opportunities.parquet",
    "v_nsn_supplier_lookup": "nsn_supplier_lookup.parquet",
    "v_nsn_profile_lookup": "nsn_profile_lookup.parquet",
    "v_nsn_cage_reference": "nsn_cage_reference.parquet",
    "v_transactions": "transactions.parquet",
    "v_contract_award_metadata": "contract_award_metadata.parquet",
}

PUBLIC_ARTIFACT_TABLES = {
    "public_intelligence_manifest": ("entity_type", "entity_id"),
    "public_company_top_award": ("cage",),
    "public_company_top_nsn": ("cage", "nsn"),
    "public_platform_award_scope": ("slug", "vendor_cage", "contract_id", "scope_rank"),
    "public_award_profile": ("contract_id",),
    "public_solicitation_profile": ("opportunity_id",),
    "public_intelligence_search": ("entity_type", "entity_id"),
    "public_company_profile": ("cage",),
    "public_platform_profile": ("slug",),
    "public_nsn_profile": ("entity_id",),
}

RETAINED_PUBLIC_ARTIFACT_TABLES = {
    key: PUBLIC_ARTIFACT_TABLES[key]
    for key in (
        "public_intelligence_manifest",
        "public_company_top_award",
        "public_company_top_nsn",
        "public_platform_award_scope",
        "public_award_profile",
        "public_solicitation_profile",
        "public_intelligence_search",
    )
}


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--main-manifest", type=Path)
    parser.add_argument("--main-manifest-s3-key")
    parser.add_argument("--main-manifest-s3-version-id")
    parser.add_argument("--previous-public-dir", type=Path, required=True)
    parser.add_argument("--previous-public-s3-prefix")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--download-inputs", action="store_true")
    parser.add_argument("--publish-rehearsal-prefix")
    parser.add_argument(
        "--extension-directory",
        type=Path,
        default=Path("/private/tmp/mimir-duckdb-extensions-144"),
    )
    return parser.parse_args()


def download_versioned_inputs(s3, bucket: str, entries: dict, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    filenames = sorted(set(SOURCE_VIEWS.values()) | {"network.parquet"})

    def download(filename: str):
        entry = entries[filename]
        destination = output_dir / filename
        s3.download_file(
            bucket,
            entry["s3_key"],
            str(destination),
            ExtraArgs={"VersionId": entry["s3_version_id"]},
        )
        actual_hash = file_sha256(destination)
        expected_hash = str(entry.get("sha256") or "")
        if expected_hash and actual_hash != expected_hash:
            raise RuntimeError(
                f"Downloaded {filename} hash {actual_hash}, expected {expected_hash}"
            )
        return filename

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        list(executor.map(download, filenames))


def download_previous_public_artifacts(
    s3,
    bucket: str,
    prefix: str,
    output_dir: Path,
):
    output_dir.mkdir(parents=True, exist_ok=True)
    normalized = prefix.strip().strip("/")
    for table_name in RETAINED_PUBLIC_ARTIFACT_TABLES:
        filename = f"{table_name}.parquet"
        s3.download_file(bucket, f"{normalized}/{filename}", str(output_dir / filename))


def publish_rehearsal_artifacts(
    s3,
    bucket: str,
    prefix: str,
    release: dict,
    report: dict,
    output_dir: Path,
) -> dict:
    normalized = prefix.strip().strip("/")
    if not normalized.startswith("mimir/rehearsals/"):
        raise RuntimeError("Rehearsal writes must stay under mimir/rehearsals/")
    release_id = str(release["release_id"])
    artifact_entries = []
    for filename, artifact in sorted(report["artifacts"].items()):
        key = f"{normalized}/public/releases/{release_id}/{filename}"
        s3.upload_file(
            str(output_dir / filename),
            bucket,
            key,
            ExtraArgs={
                "Metadata": {
                    "sha256": artifact["sha256"],
                    "schema-sha256": hashlib.sha256(
                        json.dumps(
                            artifact["schema"], separators=(",", ":"), sort_keys=True
                        ).encode("utf-8")
                    ).hexdigest(),
                    "row-count": str(artifact["row_count"]),
                    "etl-run-id": str(release.get("etl_run_id") or ""),
                }
            },
        )
        head = s3.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
        artifact_entries.append(
            {
                **artifact,
                "filename": filename,
                "s3_key": key,
                "s3_version_id": str(head.get("VersionId") or ""),
            }
        )
    public_manifest = {
        "schema_version": 1,
        "release_id": release_id,
        "etl_run_id": release.get("etl_run_id"),
        "source_main_release_id": report["source_main_release_id"],
        "release": release,
        "artifacts": artifact_entries,
    }
    body = (json.dumps(public_manifest, indent=2, sort_keys=True) + "\n").encode()
    immutable_key = f"{normalized}/public/releases/{release_id}/manifest.json"
    candidate_key = f"{normalized}/public/candidate_manifest.json"
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
        "artifact_count": len(artifact_entries),
    }


def main():
    args = parse_args()
    s3 = boto3.client("s3", region_name="us-east-1")
    if args.main_manifest:
        manifest = json.loads(args.main_manifest.read_text())
    elif args.main_manifest_s3_key:
        request = {"Bucket": args.bucket, "Key": args.main_manifest_s3_key}
        if args.main_manifest_s3_version_id:
            request["VersionId"] = args.main_manifest_s3_version_id
        manifest = json.loads(s3.get_object(**request)["Body"].read())
    else:
        raise RuntimeError(
            "Provide --main-manifest or --main-manifest-s3-key for the rehearsal"
        )
    run_id = str(manifest["etl_run_id"])
    entries = {entry["local_path"]: entry for entry in manifest["files"]}
    missing = set(SOURCE_VIEWS.values()) - set(entries)
    if missing:
        raise RuntimeError("Main manifest is missing inputs: " + ", ".join(sorted(missing)))

    args.output_dir.mkdir(parents=True, exist_ok=True)
    os.environ["LOCAL_CACHE_DIR"] = str(args.output_dir.resolve())
    os.environ["ETL_AUTOMATION_RUN_ID"] = run_id
    os.environ["PUBLIC_INTELLIGENCE_BUILD_PAGE_PROFILES"] = "1"
    os.environ.setdefault("OPENAI_API_KEY", "release-rehearsal-not-used")

    import main as api

    api.LOCAL_CACHE_DIR = args.output_dir.resolve()
    if args.previous_public_s3_prefix:
        download_previous_public_artifacts(
            s3,
            args.bucket,
            args.previous_public_s3_prefix,
            args.previous_public_dir,
        )

    local_inputs = args.output_dir / "source-inputs"
    if args.download_inputs:
        download_versioned_inputs(s3, args.bucket, entries, local_inputs)
    urls = {}
    for filename in SOURCE_VIEWS.values():
        entry = entries[filename]
        if args.download_inputs:
            urls[filename] = str((local_inputs / filename).resolve())
        else:
            urls[filename] = s3.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": args.bucket,
                    "Key": entry["s3_key"],
                    "VersionId": entry["s3_version_id"],
                },
                ExpiresIn=43_200,
            )

    connection = duckdb.connect()
    if not args.download_inputs:
        escaped_extension_directory = str(args.extension_directory.resolve()).replace(
            "'", "''"
        )
        connection.execute(
            f"SET extension_directory = '{escaped_extension_directory}'"
        )
        connection.execute("INSTALL httpfs")
        connection.execute("LOAD httpfs")
    api._apply_duck_pragmas(connection)

    for table_name in RETAINED_PUBLIC_ARTIFACT_TABLES:
        path = args.previous_public_dir / f"{table_name}.parquet"
        if not path.exists():
            raise RuntimeError(f"Previous public artifact is missing: {path}")
        connection.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet(?)", [str(path)]
        )

    for view_name, filename in SOURCE_VIEWS.items():
        escaped_url = urls[filename].replace("'", "''")
        connection.execute(
            f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet('{escaped_url}')"
        )

    network_entry = entries["network.parquet"]
    if args.download_inputs:
        network_url = str((local_inputs / "network.parquet").resolve())
    else:
        network_url = s3.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": args.bucket,
                "Key": network_entry["s3_key"],
                "VersionId": network_entry["s3_version_id"],
            },
            ExpiresIn=43_200,
        )
    network_url = network_url.replace("'", "''")
    connection.execute(f"""
        CREATE VIEW v_subcontracts AS
        SELECT
            CAST(prime_cage AS VARCHAR) AS prime_cage,
            CAST(sub_cage AS VARCHAR) AS subcontractor_cage,
            CAST(contract_id AS VARCHAR) AS prime_award_id,
            CAST(action_date AS VARCHAR) AS subcontract_action_date,
            TRY_CAST(subaward_value AS DOUBLE) AS subcontract_value_usd
        FROM read_parquet('{network_url}')
    """)

    started_at = time.perf_counter()
    result = api.build_public_intelligence_release(connection)
    elapsed = time.perf_counter() - started_at
    artifacts = {}
    for table_name, key_columns in PUBLIC_ARTIFACT_TABLES.items():
        path = args.output_dir / f"{table_name}.parquet"
        schema = [
            {"name": row[0], "type": row[1], "nullable": row[2]}
            for row in connection.execute(f"DESCRIBE {table_name}").fetchall()
        ]
        row_count = int(
            connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        )
        duplicate_count = int(connection.execute(f"""
            SELECT COALESCE(SUM(row_count - 1), 0)
            FROM (
                SELECT COUNT(*) AS row_count
                FROM {table_name}
                GROUP BY {', '.join(key_columns)}
                HAVING COUNT(*) > 1
            ) duplicates
        """).fetchone()[0])
        artifacts[path.name] = {
            "table_name": table_name,
            "size": path.stat().st_size,
            "sha256": file_sha256(path),
            "schema": schema,
            "row_count": row_count,
            "key_columns": list(key_columns),
            "duplicate_key_rows": duplicate_count,
        }

    report = {
        "rehearsal": "version-pinned-public-cold-build",
        "etl_run_id": run_id,
        "source_main_release_id": manifest["release_id"],
        "elapsed_seconds": round(elapsed, 3),
        "release": result["release"],
        "artifacts": artifacts,
    }
    if args.publish_rehearsal_prefix:
        report["published"] = publish_rehearsal_artifacts(
            s3,
            args.bucket,
            args.publish_rehearsal_prefix,
            result["release"],
            report,
            args.output_dir,
        )
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
