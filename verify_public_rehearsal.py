"""Cold-load, compare, promote, and roll back an isolated public rehearsal."""

from __future__ import annotations

import argparse
import concurrent.futures
import copy
import hashlib
import io
import json
from pathlib import Path

import boto3
import duckdb

from etl_automation.platform_release import (
    build_platform_manifest,
    promote_platform_manifest,
    publish_platform_candidate,
    rollback_platform_manifest,
)


KEY_COLUMNS = {
    "public_intelligence_manifest": ("entity_type", "entity_id"),
    "public_company_top_award": ("cage",),
    "public_company_top_nsn": ("cage", "nsn"),
    "public_platform_award_scope": ("slug", "vendor_cage", "contract_id", "scope_rank"),
    "public_award_profile": ("contract_id",),
    "public_solicitation_profile": ("opportunity_id",),
    "public_intelligence_search": ("entity_type", "entity_id"),
}


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_s3_json(s3, bucket: str, key: str):
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


def schema_for(connection, path: Path):
    return [
        {"name": row[0], "type": row[1], "nullable": row[2]}
        for row in connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
        ).fetchall()
    ]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--rehearsal-prefix", required=True)
    parser.add_argument("--public-manifest-key", required=True)
    parser.add_argument("--baseline-public-prefix", required=True)
    parser.add_argument("--main-manifest-key", required=True)
    parser.add_argument("--ask-manifest-key", required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    prefix = args.rehearsal_prefix.strip().strip("/")
    if not prefix.startswith("mimir/rehearsals/"):
        raise RuntimeError("Verification writes must stay under mimir/rehearsals/")
    args.work_dir.mkdir(parents=True, exist_ok=True)
    candidate_dir = args.work_dir / "candidate"
    baseline_dir = args.work_dir / "baseline"
    candidate_dir.mkdir(exist_ok=True)
    baseline_dir.mkdir(exist_ok=True)

    s3 = boto3.client("s3", region_name="us-east-1")
    public_manifest = load_s3_json(s3, args.bucket, args.public_manifest_key)
    main_manifest = load_s3_json(s3, args.bucket, args.main_manifest_key)
    ask_manifest = load_s3_json(s3, args.bucket, args.ask_manifest_key)
    run_id = str(main_manifest["etl_run_id"])
    if public_manifest.get("etl_run_id") != run_id:
        raise RuntimeError("Public and main releases do not share an ETL run")
    if ask_manifest.get("etl_run_id") != run_id:
        raise RuntimeError("Ask Mimir and main releases do not share an ETL run")

    def download_candidate(entry):
        destination = candidate_dir / entry["filename"]
        s3.download_file(
            args.bucket,
            entry["s3_key"],
            str(destination),
            ExtraArgs={"VersionId": entry["s3_version_id"]},
        )
        return entry, destination

    def download_baseline(entry):
        destination = baseline_dir / entry["filename"]
        s3.download_file(
            args.bucket,
            f"{args.baseline_public_prefix.strip().strip('/')}/{entry['filename']}",
            str(destination),
        )
        return entry, destination

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        candidate_downloads = list(
            executor.map(download_candidate, public_manifest["artifacts"])
        )
        baseline_downloads = list(
            executor.map(download_baseline, public_manifest["artifacts"])
        )

    connection = duckdb.connect()
    comparisons = {}
    candidate_paths = dict((entry["table_name"], path) for entry, path in candidate_downloads)
    baseline_paths = dict((entry["table_name"], path) for entry, path in baseline_downloads)
    for entry, candidate_path in candidate_downloads:
        table_name = entry["table_name"]
        baseline_path = baseline_paths[table_name]
        candidate_hash = file_sha256(candidate_path)
        baseline_hash = file_sha256(baseline_path)
        candidate_schema = schema_for(connection, candidate_path)
        baseline_schema = schema_for(connection, baseline_path)
        if candidate_hash != entry["sha256"]:
            raise RuntimeError(f"Cold-loaded hash mismatch for {candidate_path.name}")
        if candidate_schema != baseline_schema:
            raise RuntimeError(f"Schema changed for {candidate_path.name}")
        candidate_rows = int(
            connection.execute(
                "SELECT COUNT(*) FROM read_parquet(?)", [str(candidate_path)]
            ).fetchone()[0]
        )
        baseline_rows = int(
            connection.execute(
                "SELECT COUNT(*) FROM read_parquet(?)", [str(baseline_path)]
            ).fetchone()[0]
        )
        key_columns = KEY_COLUMNS[table_name]
        duplicate_rows = int(connection.execute(f"""
            SELECT COALESCE(SUM(row_count - 1), 0)
            FROM (
                SELECT COUNT(*) AS row_count
                FROM read_parquet(?)
                GROUP BY {', '.join(key_columns)}
                HAVING COUNT(*) > 1
            ) duplicates
        """, [str(candidate_path)]).fetchone()[0])
        if duplicate_rows:
            raise RuntimeError(
                f"Cold-loaded {candidate_path.name} has {duplicate_rows} duplicate key rows"
            )
        candidate_only_rows = int(connection.execute("""
            SELECT COUNT(*) FROM (
                SELECT * FROM read_parquet(?)
                EXCEPT ALL
                SELECT * FROM read_parquet(?)
            ) differences
        """, [str(candidate_path), str(baseline_path)]).fetchone()[0])
        baseline_only_rows = int(connection.execute("""
            SELECT COUNT(*) FROM (
                SELECT * FROM read_parquet(?)
                EXCEPT ALL
                SELECT * FROM read_parquet(?)
            ) differences
        """, [str(baseline_path), str(candidate_path)]).fetchone()[0])
        comparisons[candidate_path.name] = {
            "schema_exact": True,
            "candidate_rows": candidate_rows,
            "baseline_rows": baseline_rows,
            "row_count_delta": candidate_rows - baseline_rows,
            "candidate_sha256": candidate_hash,
            "baseline_sha256": baseline_hash,
            "file_bytes_exact": candidate_hash == baseline_hash,
            "candidate_only_rows": candidate_only_rows,
            "baseline_only_rows": baseline_only_rows,
            "duplicate_key_rows": duplicate_rows,
        }

    candidate_manifest_path = candidate_paths["public_intelligence_manifest"]
    baseline_manifest_path = baseline_paths["public_intelligence_manifest"]
    manifest_semantics = connection.execute("""
        WITH candidate AS (
            SELECT * FROM read_parquet(?)
        ), baseline AS (
            SELECT * FROM read_parquet(?)
        )
        SELECT
            COUNT(*) FILTER (WHERE b.entity_id IS NULL) AS added,
            COUNT(*) FILTER (WHERE c.entity_id IS NULL) AS removed,
            COUNT(*) FILTER (
                WHERE c.entity_id IS NOT NULL AND b.entity_id IS NOT NULL
                  AND c.content_fingerprint = b.content_fingerprint
            ) AS unchanged_fingerprint,
            COUNT(*) FILTER (
                WHERE c.entity_id IS NOT NULL AND b.entity_id IS NOT NULL
                  AND c.content_fingerprint <> b.content_fingerprint
            ) AS updated_fingerprint
        FROM candidate c
        FULL OUTER JOIN baseline b
          ON c.entity_type = b.entity_type AND c.entity_id = b.entity_id
    """, [str(candidate_manifest_path), str(baseline_manifest_path)]).fetchone()
    manifest_comparison = dict(
        zip(
            ("added", "removed", "unchanged_fingerprint", "updated_fingerprint"),
            map(int, manifest_semantics),
        )
    )
    if manifest_comparison["removed"]:
        raise RuntimeError(
            f"Public candidate removed {manifest_comparison['removed']} published URLs"
        )

    # Prove the immutable Ask release uses the exact main candidate objects for
    # every shared serving file before stamping it into the platform root.
    main_files = {entry["local_path"]: entry for entry in main_manifest["files"]}
    shared_checks = []
    for entry in ask_manifest["files"]:
        local_path = str(entry.get("local_path") or "")
        if not local_path.startswith("data/"):
            continue
        filename = local_path.removeprefix("data/")
        if filename not in main_files:
            continue
        main_entry = main_files[filename]
        exact_key = entry.get("s3_key") == main_entry.get("s3_key")
        exact_version = entry.get("s3_version_id") == main_entry.get("s3_version_id")
        exact_hash = (
            entry.get("sha256") == main_entry.get("sha256")
            if entry.get("sha256") or main_entry.get("sha256")
            else exact_version
        )
        if not (exact_key and exact_version and exact_hash):
            raise RuntimeError(f"Ask Mimir does not exactly match main for {filename}")
        shared_checks.append(filename)
    if not shared_checks:
        raise RuntimeError("No shared Ask Mimir/main serving artifacts were verified")
    root = build_platform_manifest(
        etl_run_id=run_id,
        main_manifest=main_manifest,
        main_manifest_key=args.main_manifest_key,
        public_manifest=public_manifest,
        public_manifest_key=args.public_manifest_key,
        ask_mimir_manifest=ask_manifest,
        ask_mimir_manifest_key=args.ask_manifest_key,
    )
    platform_prefix = f"{prefix}/platform"
    platform_locations = publish_platform_candidate(
        s3,
        args.bucket,
        root,
        candidate_key=f"{platform_prefix}/candidate_manifest.json",
        immutable_prefix=f"{platform_prefix}/releases",
    )
    previous_root = copy.deepcopy(root)
    previous_root["release_id"] = root["release_id"] + "-rollback-baseline"
    previous_root["status"] = "active-rehearsal-baseline"
    previous_key = (
        f"{platform_prefix}/releases/{previous_root['release_id']}/manifest.json"
    )
    previous_body = (
        json.dumps(previous_root, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    s3.put_object(
        Bucket=args.bucket,
        Key=previous_key,
        Body=previous_body,
        ContentType="application/json",
    )
    current_key = f"{platform_prefix}/current_manifest.json"
    promote_platform_manifest(s3, args.bucket, previous_root, current_key=current_key)
    promoted = promote_platform_manifest(s3, args.bucket, root, current_key=current_key)
    rolled_back = rollback_platform_manifest(
        s3,
        args.bucket,
        previous_key,
        current_key=current_key,
    )
    current_body = s3.get_object(Bucket=args.bucket, Key=current_key)["Body"].read()
    rollback_exact = current_body == previous_body
    if not rollback_exact:
        raise RuntimeError("Rollback pointer does not exactly match the prior immutable root")

    report = {
        "rehearsal": "public-cold-start-exact-comparison-and-rollback",
        "etl_run_id": run_id,
        "public_release_id": public_manifest["release_id"],
        "artifact_comparisons": comparisons,
        "manifest_semantics": manifest_comparison,
        "ask_main_shared_artifacts": sorted(shared_checks),
        "ask_main_shared_artifact_count": len(shared_checks),
        "cold_start": {
            "version_pinned_downloads": len(candidate_downloads),
            "all_schemas_exact": all(
                item["schema_exact"] for item in comparisons.values()
            ),
            "all_keys_unique": all(
                item["duplicate_key_rows"] == 0 for item in comparisons.values()
            ),
        },
        "platform": {
            "candidate": platform_locations,
            "promoted_version_id": promoted["s3_version_id"],
            "rolled_back_version_id": rolled_back["s3_version_id"],
            "rollback_exact": rollback_exact,
            "test_current_key": current_key,
        },
    }
    report_body = (json.dumps(report, indent=2, sort_keys=True) + "\n").encode()
    report_key = f"{prefix}/reports/cold-start-rollback-report.json"
    s3.put_object(
        Bucket=args.bucket,
        Key=report_key,
        Body=report_body,
        ContentType="application/json",
    )
    print(json.dumps({**report, "report_key": report_key}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
