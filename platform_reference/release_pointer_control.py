#!/usr/bin/env python3
"""Guarded S3 root-pointer promotion and rollback for a complete release.

This tool never promotes a candidate whose serving graph and dual-read gates
are incomplete. Each promotion saves the exact prior pointer body and version
before the conditional write; rollback creates a new version with that body.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import boto3


REQUIRED_COMPONENTS = {"main", "public", "ask_mimir", "platform_reference"}
REQUIRED_GATES = {"source_lineage", "complete_serving_graph", "main_dual_read",
                  "ask_dual_read", "public_dual_read", "rollback_rehearsal"}

from main_release_cache import validate_manifest as validate_main_manifest


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, indent=2) + "\n")
    tmp.replace(path)


def read_pointer(s3, bucket: str, key: str) -> dict:
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    return {"body": body, "sha256": digest(body),
            "etag": response["ETag"].strip('"'),
            "version_id": response.get("VersionId"),
            "manifest": json.loads(body)}


def validate_candidate(candidate: dict) -> None:
    if candidate.get("schema_version") != 2 or candidate.get("promotion_ready") is not True:
        raise ValueError("candidate is not a promotion-ready root manifest")
    run_id = str(candidate.get("etl_run_id") or "")
    if not run_id:
        raise ValueError("candidate root has no ETL run ID")
    if not REQUIRED_COMPONENTS.issubset(candidate.get("components", {})):
        raise ValueError("candidate is missing Main, Public, Ask or platform-reference component")
    gates = candidate.get("promotion_gates", {})
    if not REQUIRED_GATES.issubset(gates) or not all(gates[name] is True for name in REQUIRED_GATES):
        raise ValueError("candidate serving, dual-read or rollback gates are incomplete")
    for name in REQUIRED_COMPONENTS:
        component = candidate["components"][name]
        if not component.get("release_id") or not component.get("immutable_manifest_key"):
            raise ValueError(f"component {name} is not release-pinned")
        if component.get("etl_run_id") != run_id:
            raise ValueError(f"component {name} belongs to another ETL run")
        if not component.get("immutable_manifest_version_id") or len(
            str(component.get("immutable_manifest_sha256") or "")
        ) != 64:
            raise ValueError(f"component {name} lacks an exact manifest version and hash")


def validate_candidate_children(s3, bucket: str, candidate: dict) -> None:
    """Verify the exact S3 child bodies and the cross-component release graph."""
    children = {}
    run_id = candidate["etl_run_id"]
    for name in sorted(REQUIRED_COMPONENTS):
        reference = candidate["components"][name]
        response = s3.get_object(
            Bucket=bucket,
            Key=reference["immutable_manifest_key"],
            VersionId=reference["immutable_manifest_version_id"],
        )
        body = response["Body"].read()
        if digest(body) != reference["immutable_manifest_sha256"]:
            raise ValueError(f"component {name} manifest hash differs from root")
        child = json.loads(body)
        if child.get("release_id") != reference["release_id"] or child.get("etl_run_id") != run_id:
            raise ValueError(f"component {name} release or ETL run differs from root")
        children[name] = child

    main = children["main"]
    public = children["public"]
    ask = children["ask_mimir"]
    reference = children["platform_reference"]
    validate_main_manifest(main)
    if main["platform_reference_release_id"] != reference["release_id"]:
        raise ValueError("Main uses another platform reference")
    if public.get("source_main_release_id") != main["release_id"]:
        raise ValueError("Public was not built from this Main release")
    if reference.get("downstream_cache_rebuild_status") != "BUILT_FRIDAY":
        raise ValueError("platform reference has no completed Friday cache rebuild")
    if reference.get("rebuilt_main_release_id") != main["release_id"]:
        raise ValueError("platform reference wrapper names another Main release")
    if reference.get("financial_allocation_rows") != 0:
        raise ValueError("platform reference would allocate financial spend")
    if not ask.get("metric_release_id") or not ask.get("files"):
        raise ValueError("Ask runtime manifest is incomplete")
    if candidate.get("ask_metric_release_id") != ask["metric_release_id"]:
        raise ValueError("Ask metric release differs from root")


def snapshot(s3, bucket: str, key: str, journal_path: Path) -> dict:
    if journal_path.exists():
        raise FileExistsError("rollback journal already exists; choose a new path to preserve prior release history")
    current = read_pointer(s3, bucket, key)
    if not current["version_id"]:
        raise ValueError("live pointer bucket must retain object versions for rollback")
    journal = {"schema_version": 1, "state": "SNAPSHOT_ONLY",
               "captured_at_utc": utc_now(), "bucket": bucket, "pointer_key": key,
               "prior": {"version_id": current["version_id"], "etag": current["etag"],
                         "sha256": current["sha256"],
                         "body_base64": base64.b64encode(current["body"]).decode()},
               "prior_release_id": current["manifest"].get("release_id")}
    write_json(journal_path, journal)
    return {k: journal[k] for k in ("state", "prior_release_id", "pointer_key")}


def verify_snapshot(s3, journal_path: Path) -> dict:
    journal = json.loads(journal_path.read_text())
    current = read_pointer(s3, journal["bucket"], journal["pointer_key"])
    matches = (current["version_id"] == journal["prior"]["version_id"]
               and current["etag"] == journal["prior"]["etag"]
               and current["sha256"] == journal["prior"]["sha256"])
    return {"state": journal["state"], "pointer_unchanged_since_snapshot": matches,
            "current_version_id": current["version_id"],
            "snapshot_version_id": journal["prior"]["version_id"]}


def promote(s3, candidate_path: Path, journal_path: Path) -> dict:
    journal = json.loads(journal_path.read_text())
    if journal["state"] != "SNAPSHOT_ONLY":
        raise ValueError("promotion requires an unused pointer snapshot")
    body = candidate_path.read_bytes()
    candidate = json.loads(body)
    validate_candidate(candidate)
    validate_candidate_children(s3, journal["bucket"], candidate)
    current = read_pointer(s3, journal["bucket"], journal["pointer_key"])
    if current["version_id"] != journal["prior"]["version_id"] or current["sha256"] != journal["prior"]["sha256"]:
        raise RuntimeError("live pointer changed after snapshot; refresh the rollback journal")
    if current["etag"] != journal["prior"]["etag"]:
        raise RuntimeError("live pointer ETag changed after snapshot")
    journal.update(state="PROMOTION_PREPARED", candidate_release_id=candidate["release_id"],
                   candidate_sha256=digest(body), candidate_body_base64=base64.b64encode(body).decode(),
                   prepared_at_utc=utc_now())
    write_json(journal_path, journal)
    response = s3.put_object(Bucket=journal["bucket"], Key=journal["pointer_key"],
                             Body=body, ContentType="application/json",
                             IfMatch='"' + current["etag"] + '"',
                             Metadata={"release-id": candidate["release_id"],
                                       "sha256": digest(body)})
    observed = read_pointer(s3, journal["bucket"], journal["pointer_key"])
    if observed["sha256"] != digest(body) or observed["version_id"] != response.get("VersionId"):
        raise RuntimeError("pointer write could not be verified; use the journal to inspect or roll back")
    journal.update(state="PROMOTED", promoted_at_utc=utc_now(),
                   promoted_version_id=observed["version_id"], promoted_etag=observed["etag"])
    write_json(journal_path, journal)
    return {"state": journal["state"], "release_id": candidate["release_id"],
            "version_id": observed["version_id"], "rollback_journal": str(journal_path)}


def rollback(s3, journal_path: Path) -> dict:
    journal = json.loads(journal_path.read_text())
    if journal["state"] not in {"PROMOTION_PREPARED", "PROMOTED"}:
        raise ValueError("journal does not describe a promotion to roll back")
    current = read_pointer(s3, journal["bucket"], journal["pointer_key"])
    if current["sha256"] != journal["candidate_sha256"]:
        raise RuntimeError("pointer no longer contains this candidate; refusing to overwrite another release")
    if journal.get("promoted_version_id") and current["version_id"] != journal["promoted_version_id"]:
        raise RuntimeError("pointer version changed since promotion")
    prior_body = base64.b64decode(journal["prior"]["body_base64"])
    if digest(prior_body) != journal["prior"]["sha256"]:
        raise ValueError("rollback journal's prior pointer body is corrupt")
    response = s3.put_object(Bucket=journal["bucket"], Key=journal["pointer_key"],
                             Body=prior_body, ContentType="application/json",
                             IfMatch='"' + current["etag"] + '"',
                             Metadata={"release-id": journal["prior_release_id"],
                                       "rollback-of": journal["candidate_release_id"],
                                       "sha256": journal["prior"]["sha256"]})
    observed = read_pointer(s3, journal["bucket"], journal["pointer_key"])
    if observed["sha256"] != journal["prior"]["sha256"] or observed["version_id"] != response.get("VersionId"):
        raise RuntimeError("rollback pointer write could not be verified")
    journal.update(state="ROLLED_BACK", rolled_back_at_utc=utc_now(),
                   rollback_version_id=observed["version_id"])
    write_json(journal_path, journal)
    return {"state": journal["state"], "restored_release_id": journal["prior_release_id"],
            "version_id": observed["version_id"]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["snapshot", "verify", "promote", "rollback"])
    parser.add_argument("--bucket", default="a-and-d-intel-lake-newaccount")
    parser.add_argument("--pointer-key", default="mimir/platform/current_manifest.json")
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--candidate", type=Path)
    args = parser.parse_args()
    s3 = boto3.client("s3", region_name="us-east-1")
    if args.action == "snapshot":
        result = snapshot(s3, args.bucket, args.pointer_key, args.journal)
    elif args.action == "verify":
        result = verify_snapshot(s3, args.journal)
    elif args.action == "promote":
        if args.candidate is None:
            parser.error("--candidate is required for promote")
        result = promote(s3, args.candidate, args.journal)
    else:
        result = rollback(s3, args.journal)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
