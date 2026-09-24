"""Cold-start all Mimir consumers from a rehearsal-only atomic root."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import boto3
import duckdb


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--platform-manifest-key", required=True)
    parser.add_argument("--platform-manifest-version-id")
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--report-key", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.platform_manifest_key.startswith("mimir/rehearsals/"):
        raise RuntimeError("Smoke test must use a rehearsal-only platform pointer")
    if not args.report_key.startswith("mimir/rehearsals/"):
        raise RuntimeError("Smoke report must stay under mimir/rehearsals/")
    args.work_dir.mkdir(parents=True, exist_ok=True)

    os.environ.update(
        {
            "MIMIR_USE_PLATFORM_MANIFEST": "1",
            "MIMIR_PLATFORM_MANIFEST_KEY": args.platform_manifest_key,
            "ATHENA_OUTPUT_BUCKET": args.bucket,
            "LOCAL_CACHE_DIR": str((args.work_dir / "main").resolve()),
            "ASK_MIMIR_RUNTIME_ROOT": str((args.work_dir / "ask").resolve()),
            "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY") or "smoke-test-not-used",
            "ASK_MIMIR_ALLOW_EXTERNAL_EVIDENCE": "0",
            "ASK_MIMIR_ALLOW_TEST_IDENTITIES": "0",
            "OFFLINE_SMOKE": "1",
        }
    )
    if args.platform_manifest_version_id:
        os.environ["MIMIR_PLATFORM_MANIFEST_VERSION_ID"] = (
            args.platform_manifest_version_id
        )

    from ask_mimir_beta.platform_manifest import resolve_platform_release

    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
    resolved = resolve_platform_release(
        s3,
        args.bucket,
        platform_key=args.platform_manifest_key,
        platform_version_id=args.platform_manifest_version_id,
        components=("main", "public", "ask_mimir"),
    )
    platform = resolved["platform"]

    import main as api

    api.reload_all_data()
    readiness = api.get_readiness_state()
    loaded_platform = api.GLOBAL_CACHE.get("platform_release") or {}
    loaded_public = api.GLOBAL_CACHE.get("public_intelligence_release") or {}
    if not readiness.get("ready"):
        raise RuntimeError(f"Main Mimir did not become ready: {readiness}")
    if loaded_platform.get("release_id") != platform["release_id"]:
        raise RuntimeError("Main Mimir did not bind to the selected platform root")
    public_manifest = resolved["components"]["public"]
    if loaded_public.get("release_id") != public_manifest["release_id"]:
        raise RuntimeError("Public routes did not bind to the selected public child")

    main_manifest = resolved["components"]["main"]
    row_checks = {}
    version_only_checks = []
    checker = duckdb.connect()
    for entry in main_manifest["files"]:
        local_path = (api.LOCAL_CACHE_DIR / entry["local_path"]).resolve()
        actual_rows = int(
            checker.execute(
                "SELECT COUNT(*) FROM read_parquet(?)", [str(local_path)]
            ).fetchone()[0]
        )
        if entry.get("row_count") is not None:
            expected_rows = int(entry["row_count"])
            if actual_rows != expected_rows:
                raise RuntimeError(
                    f"Main row count mismatch for {entry['local_path']}: "
                    f"{actual_rows} != {expected_rows}"
                )
        else:
            # Legacy reused sidecars can predate row-count metadata.  They are
            # still exact because the consumer requires an immutable S3
            # VersionId and validates the recorded byte size before opening.
            version_only_checks.append(entry["local_path"])
        row_checks[entry["local_path"]] = actual_rows
    checker.close()

    from fastapi import Response

    manifest_page = api.get_public_intelligence_manifest(
        Response(), page=1, page_size=5, order="entity_id"
    )
    if not manifest_page["entries"]:
        raise RuntimeError("Public manifest endpoint returned no entries")
    search_seed = str(manifest_page["entries"][0]["display_name"] or "")[:20]
    public_search = api.search_public_intelligence_manifest(
        Response(), q=search_seed, limit=5
    )

    sample_rows = api.duck_fetch_df(
        """
        SELECT entity_type, entity_id, canonical_path
        FROM public_intelligence_manifest
        QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_type ORDER BY richness_score DESC) = 1
        """
    ).to_dict(orient="records")
    samples = {row["entity_type"]: row for row in sample_rows}
    required_types = {"cage_company", "platform", "nsn", "contract_award", "solicitation"}
    if required_types - set(samples):
        raise RuntimeError("Public manifest is missing a smoke-test entity type")
    public_pages = {
        "cage_company": api.get_public_company_page_snapshot(
            samples["cage_company"]["entity_id"], Response()
        ),
        "platform": api.get_public_platform_page_snapshot(
            str(samples["platform"]["canonical_path"]).rstrip("/").split("/")[-1],
            Response(),
        ),
        "nsn": api.get_public_nsn_page_snapshot(
            samples["nsn"]["entity_id"], Response()
        ),
        "contract_award": api.get_public_award_page_snapshot(
            samples["contract_award"]["entity_id"], Response()
        ),
        "solicitation": api.get_public_solicitation_page_snapshot(
            samples["solicitation"]["entity_id"], Response()
        ),
    }
    for entity_type, payload in public_pages.items():
        if not payload.get("found") or not payload.get("seo_indexable"):
            raise RuntimeError(f"Public {entity_type} smoke page was not publishable")
        if payload.get("public_release_id") != public_manifest["release_id"]:
            raise RuntimeError(f"Public {entity_type} used a mixed release")

    from ask_mimir_beta import bootstrap_data

    ask_manifest = bootstrap_data.bootstrap()
    if ask_manifest.get("release_id") != resolved["components"]["ask_mimir"]["release_id"]:
        raise RuntimeError("Ask Mimir did not bind to the selected Ask child")
    if os.getenv("MIMIR_PLATFORM_RELEASE_ID") != platform["release_id"]:
        raise RuntimeError("Ask Mimir did not expose the selected platform release")

    # Import after bootstrap so all stores resolve the verified runtime paths.
    import sys

    ask_root = str((Path(__file__).parent / "ask_mimir_beta").resolve())
    if ask_root not in sys.path:
        sys.path.insert(0, ask_root)
    from ask_mimir_beta import lab_api

    ask_health = lab_api.health()
    if ask_health.get("status") != "ok":
        raise RuntimeError("Ask Mimir health check failed")
    if ask_health.get("runtime_release_id") != ask_manifest["release_id"]:
        raise RuntimeError("Ask Mimir health reports a mixed runtime release")

    report = {
        "smoke_test": "atomic-platform-cold-start",
        "platform_release_id": platform["release_id"],
        "etl_run_id": platform["etl_run_id"],
        "platform_object": resolved["platform_object"],
        "component_release_ids": {
            name: manifest["release_id"]
            for name, manifest in resolved["components"].items()
        },
        "main": {
            "ready": readiness,
            "version_pinned_files": len(row_checks),
            "row_counts_exact_where_declared": True,
            "version_and_size_only_files": sorted(version_only_checks),
        },
        "public": {
            "release_id": loaded_public["release_id"],
            "manifest_entries": loaded_public["total_entries"],
            "sample_page_types": sorted(public_pages),
            "sample_pages_publishable": True,
            "search_result_count": len(public_search["entries"]),
        },
        "ask_mimir": {
            "release_id": ask_manifest["release_id"],
            "verified_files": len(ask_manifest["files"]),
            "health_status": ask_health["status"],
            "release_binding_id": ask_health["release_binding_id"],
        },
        "live_serving_mutations": [],
    }
    body = (json.dumps(report, indent=2, sort_keys=True) + "\n").encode()
    response = s3.put_object(
        Bucket=args.bucket,
        Key=args.report_key,
        Body=body,
        ContentType="application/json",
    )
    print(
        json.dumps(
            {
                **report,
                "report_key": args.report_key,
                "report_version_id": str(response.get("VersionId") or ""),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
