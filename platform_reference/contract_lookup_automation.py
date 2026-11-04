"""Candidate-only, bounded lookup rebuild after the final v7 Main release.

Run in a separate ECS task, never in the serving process. Exact source versions,
all-field parity and offline handler/load checks gate the new candidate root.
The live pointer and original candidate are never written by this module.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid

import boto3
from botocore.exceptions import ClientError
import psutil

from .contract_lookup_release import with_contract_lookup
from .main_release_cache import asset_fingerprint, validate_manifest

GIB = 1024 ** 3
MIB = 1024 ** 2


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * MIB), b""):
            h.update(chunk)
    return h.hexdigest()


def prefix_for(run_id):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", run_id):
        raise ValueError("Unsafe ETL run ID")
    return f"mimir/rehearsals/weekly-{run_id}"


def read_json(s3, bucket, key, version=None, sha=None):
    r = s3.get_object(Bucket=bucket, Key=key, **({"VersionId": version} if version else {}))
    try:
        body = r["Body"].read(4 * MIB + 1)
    finally:
        r["Body"].close()
    if len(body) > 4 * MIB or not r.get("VersionId"):
        raise ValueError("Manifest must be small and versioned")
    if version and r["VersionId"] != version:
        raise ValueError("Manifest version differs")
    sha256 = hashlib.sha256(body).hexdigest()
    if sha and sha256 != sha:
        raise ValueError("Manifest hash differs")
    return json.loads(body), {"s3_key": key, "s3_version_id": r["VersionId"],
                              "sha256": sha256, "size": len(body)}


def put_file(s3, bucket, key, path):
    """Immutable retry: accept an existing object only with the same full hash."""
    sha = digest(path)
    try:
        with Path(path).open("rb") as stream:
            r = s3.put_object(Bucket=bucket, Key=key, Body=stream,
                ContentLength=Path(path).stat().st_size, IfNoneMatch="*", Metadata={"sha256": sha})
        version = r["VersionId"]
    except ClientError as error:
        if error.response["Error"]["Code"] not in {"PreconditionFailed", "412"}:
            raise
        r = s3.head_object(Bucket=bucket, Key=key)
        if r.get("Metadata", {}).get("sha256") != sha or r["ContentLength"] != Path(path).stat().st_size:
            raise ValueError("Immutable lookup output conflicts") from error
        version = r["VersionId"]
    h = s3.head_object(Bucket=bucket, Key=key, VersionId=version)
    if h["ContentLength"] != Path(path).stat().st_size or h.get("Metadata", {}).get("sha256") != sha:
        raise ValueError("Lookup upload was not verified")
    return {"s3_key": key, "s3_version_id": version, "sha256": sha,
            "size": h["ContentLength"], "etag": h["ETag"].strip('"')}


def put_json(s3, bucket, key, value, work):
    path = work / "upload.json"
    path.write_bytes(canonical(value))
    return put_file(s3, bucket, key, path)


def download(s3, bucket, ref, path):
    r = s3.get_object(Bucket=bucket, Key=ref["s3_key"], VersionId=ref["s3_version_id"])
    if (r.get("VersionId") != ref["s3_version_id"] or r["ContentLength"] != ref["size"] or
            (ref.get("etag") and r["ETag"].strip('"') != ref["etag"].strip('"'))):
        r["Body"].close()
        raise ValueError("Pinned source version/size/ETag differs")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("wb") as stream:
            for chunk in r["Body"].iter_chunks(4 * MIB):
                stream.write(chunk)
    finally:
        r["Body"].close()
    if path.stat().st_size != ref["size"] or (ref.get("sha256") and digest(path) != ref["sha256"]):
        raise ValueError("Pinned source hash/size differs")


def memory_events():
    path = Path("/sys/fs/cgroup/memory.events")
    if path.exists():
        return dict(line.split() for line in path.read_text().splitlines())
    path = Path("/sys/fs/cgroup/memory/memory.oom_control")
    return dict(line.split() for line in path.read_text().splitlines()) if path.exists() else {}


def active_container_memory():
    base = Path("/sys/fs/cgroup")
    if (base / "memory.current").exists():
        stat = dict(line.split() for line in (base / "memory.stat").read_text().splitlines())
        return int((base / "memory.current").read_text()) - int(stat.get("inactive_file", 0))
    base = base / "memory"
    if (base / "memory.usage_in_bytes").exists():
        stat = dict(line.split() for line in (base / "memory.stat").read_text().splitlines())
        return int((base / "memory.usage_in_bytes").read_text()) - int(stat.get("total_inactive_file", 0))
    return None


def guarded(command, work, log, *, rss_limit, seconds, deadline, disk_floor=4 * GIB):
    """Stop well before the task cap; capture guards even if a child exits early."""
    peak = 0
    began = time.monotonic()
    guard = None
    before = memory_events()
    with log.open("w") as output:
        child = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                                 start_new_session=True)
        process = psutil.Process(child.pid)
        try:
            while child.poll() is None:
                try:
                    rss = process.memory_info().rss + sum(p.memory_info().rss for p in process.children(recursive=True))
                except psutil.NoSuchProcess:
                    break
                peak = max(peak, rss)
                active = active_container_memory()
                if rss > rss_limit:
                    guard = "process_rss_limit"
                elif active is not None and active > 2 * GIB:
                    guard = "container_active_memory_above_2GiB"
                elif shutil.disk_usage(work).free < disk_floor:
                    guard = "scratch_headroom"
                elif time.monotonic() > deadline or time.monotonic() - began > seconds:
                    guard = "deadline"
                if guard:
                    break
                time.sleep(.1)
        finally:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
            code = child.wait()
    after = memory_events()
    if any(int(after.get(k, 0)) > int(before.get(k, 0)) for k in ("oom", "oom_kill")):
        guard = "container_oom_event"
    result = {"peak_process_rss_bytes": peak, "exit_code": code, "stop_guard": guard,
              "seconds": round(time.monotonic() - began, 3), "memory_events": after,
              "container_active_memory_bytes": active_container_memory()}
    print(json.dumps({"phase": "child_finished", **result}), flush=True)
    if code or guard:
        raise RuntimeError("Lookup stopped safely: " + json.dumps(result))
    return result


def rebuild(s3, bucket, run_id, work):
    prefix = prefix_for(run_id)
    output = prefix + "/contract-lookup"
    work.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + 4500
    complete, _ = read_json(s3, bucket, prefix + "/v7-auto/complete.json")
    expected_key = prefix + "/v7-auto/root/full_private_candidate.json"
    if complete.get("etl_run_id") != run_id or complete.get("root_key") != expected_key:
        raise ValueError("Final candidate belongs to another run or is incomplete")
    root, root_pin = read_json(s3, bucket, expected_key, complete["root_version_id"], complete["root_sha256"])
    if (root.get("etl_run_id") != run_id or root.get("customer_facing") or
            root.get("promotion_ready") is not False or root.get("release_status") != "CANDIDATE_ONLY"):
        raise ValueError("Only an unpromoted same-run candidate may be extended")
    main_ref = root["components"]["main"]
    main, main_pin = read_json(s3, bucket, main_ref["immutable_manifest_key"],
        main_ref["immutable_manifest_version_id"], main_ref["immutable_manifest_sha256"])
    entries = validate_manifest(main)
    if main.get("etl_run_id") != run_id or main["release_id"] != main_ref["release_id"]:
        raise ValueError("Final Main identity differs")
    source = entries["contracts_rolled.parquet"]
    fingerprint = asset_fingerprint(source)
    # A retry reuses only an entirely validated result from these exact pins.
    try:
        prior, _ = read_json(s3, bucket, output + "/complete.json")
    except ClientError as error:
        if error.response["Error"]["Code"] not in {"NoSuchKey", "404"}:
            raise
    else:
        if prior.get("source_root") != root_pin or prior.get("source_asset_sha256") != fingerprint:
            raise ValueError("Completed lookup belongs to different source pins")
        read_json(s3, bucket, prior["root"]["s3_key"], prior["root"]["s3_version_id"], prior["root"]["sha256"])
        return prior
    lease_key = output + "/lease.json"
    try:
        lease, lease_pin = read_json(s3, bucket, lease_key)
    except ClientError as error:
        if error.response["Error"]["Code"] not in {"NoSuchKey", "404"}:
            raise
        condition = {"IfNoneMatch": "*"}
    else:
        if lease.get("expires_at", 0) > time.time():
            raise RuntimeError("Another bounded lookup attempt holds the run lease")
        h = s3.head_object(Bucket=bucket, Key=lease_key, VersionId=lease_pin["s3_version_id"])
        condition = {"IfMatch": h["ETag"]}
    attempt = uuid.uuid4().hex
    s3.put_object(Bucket=bucket, Key=lease_key,
        Body=canonical({"attempt": attempt, "expires_at": time.time() + 7200}), **condition)
    attempt_prefix = f"mimir/releases/{main['release_id']}/lookups/{fingerprint}/{attempt}"
    parts = source.get("parts", [source])
    if not parts or len(parts) > 128:
        raise ValueError("Unexpected source partition count")
    # Reserve both complete sources, candidates and bounded 4GiB spill.
    if shutil.disk_usage(work).free < sum(p["size"] for p in parts) * 3 + 8 * GIB:
        raise ValueError("Insufficient scratch for full release and headroom")
    results = []
    for index, part in enumerate(parts):
        if time.monotonic() > deadline:
            raise TimeoutError("Lookup release deadline")
        path = work / "source" / f"part-{index:05d}.parquet"
        target = work / "contracts_lookup.parquet" / path.name
        target.parent.mkdir(exist_ok=True)
        download(s3, bucket, part, path)
        report = work / f"part-{index:05d}.json"
        log = work / f"part-{index:05d}.log"
        resource = guarded([sys.executable, "-u", "-m", "contract_lookup_layout", str(path),
            str(target), "--memory-mb", "512", "--temp-root", str(work / "spill"),
            "--report", str(report)], work, log, rss_limit=768 * MIB, seconds=900, deadline=deadline)
        proof = json.loads(report.read_text())
        proof.update(resource, index=index, source=part, source_sha256=digest(path),
            candidate=put_file(s3, bucket, f"{attempt_prefix}/parts/{path.name}", target))
        # Paths and elapsed time are diagnostic, not part of immutable proof identity.
        results.append(proof)
        print(json.dumps({"phase": "part_passed", "index": index, "rows": proof["rows_compared"]}), flush=True)
    proof = {"state": "ALL_PARTS_EXACT_PARITY_PASSED", "release_id": main["release_id"], "parts": results}
    candidate_main = with_contract_lookup(main, proof, main["release_id"])
    os.environ["LOOKUP_WORK_ROOT"] = str(work)
    timing_resource = guarded([sys.executable, "-u", "-m", "platform_reference.contract_lookup_benchmark"],
        work, work / "timing.log", rss_limit=1536 * MIB, seconds=1200, deadline=deadline)
    timing = json.loads((work / "query-benchmark.json").read_text())
    if timing.get("state") != "PASSED" or not timing.get("all_values_equal"):
        raise ValueError("Offline handler/load gate failed")
    candidate_main["contract_lookup_validation"].update(offline_handler_parity=True,
        mixed_load_passed=True, source_asset_sha256=fingerprint)
    main_key = attempt_prefix + "/manifest.json"
    annex = put_json(s3, bucket, main_key, candidate_main, work)
    updated = deepcopy(root)
    updated["release_id"] += "-contract-lookup"
    updated["components"]["main"].update(immutable_manifest_key=annex["s3_key"],
        immutable_manifest_version_id=annex["s3_version_id"], immutable_manifest_sha256=annex["sha256"])
    updated["promotion_gates"]["contract_lookup"] = True
    evidence = {"source_root": root_pin, "source_main": main_pin, "source_asset_sha256": fingerprint,
        "exact_parity": proof, "offline_queries": timing, "query_resources": timing_resource,
        "production_changed": False}
    evidence_pin = put_json(s3, bucket, attempt_prefix + "/validation.json", evidence, work)
    updated["contract_lookup_validation"] = evidence_pin
    new_root = put_json(s3, bucket, attempt_prefix + "/root.json", updated, work)
    result = {"etl_run_id": run_id, "state": "PASSED", "source_root": root_pin,
              "source_asset_sha256": fingerprint, "root": new_root, "validation": evidence_pin,
              "promotion_ready": False, "production_changed": False}
    put_json(s3, bucket, output + "/complete.json", result, work)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--work-dir", type=Path, default=Path("/tmp/contract-lookup"))
    args = parser.parse_args()
    print(json.dumps(rebuild(boto3.client("s3", region_name="us-east-1"),
        args.bucket, args.run_id, args.work_dir.resolve())), flush=True)


if __name__ == "__main__":
    main()
