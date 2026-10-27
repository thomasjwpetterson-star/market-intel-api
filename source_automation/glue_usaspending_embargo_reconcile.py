"""Repair material DoD action-month gaps left by modification-date ingestion.

Run after the daily silver job. One closed month is repaired per invocation;
the normal modification-date state is never changed. A failed validation leaves
only private staged objects, and the silver merge is never started.
"""

import csv
import hashlib
import json
import os
import tempfile
import time
import zipfile
from datetime import date, datetime, timedelta, timezone

import boto3
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BUCKET = "a-and-d-intel-lake-newaccount"
REGION = "us-east-1"
API = "https://api.usaspending.gov/api/v2"
SILVER_JOB = "usaspending_bronze_to_silver"
AWARD_TYPES = ["A", "B", "C", "D", "IDV_A", "IDV_B", "IDV_B_A",
               "IDV_B_B", "IDV_B_C", "IDV_D", "IDV_E", "IDV_C"]
SILVER_PREFIX = "silver/usaspending/dataset=prime_contracts/"
BRONZE_PREFIX = "bronze/usaspending/dataset=prime_contracts/"


def month_windows(today, count=6):
    first = date(today.year, today.month, 1)
    out = []
    for _ in range(count):
        end = first - timedelta(days=1)
        start = date(end.year, end.month, 1)
        out.append((start, end))
        first = start
    return list(reversed(out))


def weekly_windows(start, end):
    day = start
    while day <= end:
        stop = min(day + timedelta(days=6), end)
        yield day, stop
        day = stop + timedelta(days=1)


def api_post(path, payload):
    response = requests.post(API + path, json=payload, timeout=120, verify=False)
    response.raise_for_status()
    return response.json()


def official_count(start, end):
    body = api_post("/download/count/", {"filters": {
        "agencies": [{"type": "awarding", "tier": "toptier", "name": "Department of Defense"}],
        "award_type_codes": AWARD_TYPES,
        "time_period": [{"start_date": start.isoformat(), "end_date": end.isoformat()}],
    }})
    if body.get("transaction_rows_gt_limit") or body.get("calculated_transaction_count") is None:
        raise RuntimeError(f"Official count unavailable for {start} to {end}: {body}")
    return int(body["calculated_transaction_count"])


def athena_rows(athena, query):
    query_id = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": "market_intel_silver"},
        ResultConfiguration={"OutputLocation": f"s3://{BUCKET}/mimir/coverage-audit/athena/"},
    )["QueryExecutionId"]
    for _ in range(120):
        status = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]
        if status["State"] == "SUCCEEDED":
            return athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"][1:]
        if status["State"] in {"FAILED", "CANCELLED"}:
            raise RuntimeError(f"Athena {query_id}: {status.get('StateChangeReason')}")
        time.sleep(2)
    raise TimeoutError(f"Athena query did not finish: {query_id}")


def silver_counts(athena, windows):
    start, end = windows[0][0], windows[-1][1]
    query = ("SELECT substr(CAST(action_date AS VARCHAR),1,7), COUNT(*) "
             "FROM market_intel_silver.dataset_prime_contracts "
             f"WHERE TRY_CAST(action_date AS DATE) BETWEEN DATE '{start}' AND DATE '{end}' "
             "GROUP BY 1")
    return {r["Data"][0]["VarCharValue"]: int(r["Data"][1]["VarCharValue"])
            for r in athena_rows(athena, query)}


def select_gap(athena, today):
    windows = month_windows(today)
    observed = silver_counts(athena, windows)
    for start, end in windows:
        expected = official_count(start, end)
        actual = observed.get(start.strftime("%Y-%m"), 0)
        tolerance = max(100, int(expected * 0.005))
        if actual > expected + tolerance:
            raise RuntimeError(f"Silver exceeds published count for {start:%Y-%m}: {actual}>{expected}")
        if expected - actual > tolerance:
            return start, end, expected, actual
    return None


def download_week(start, end, workdir):
    payload = {"filters": {
        "agencies": [{"type": "awarding", "tier": "toptier", "name": "Department of Defense"}],
        "prime_award_types": AWARD_TYPES,
        "date_type": "action_date",
        "date_range": {"start_date": start.isoformat(), "end_date": end.isoformat()},
    }, "file_format": "csv", "columns": []}
    info = api_post("/bulk_download/awards/", payload)
    if not info.get("file_url") or not info.get("file_name"):
        raise RuntimeError(f"Bulk request did not return a file: {info}")
    status_url = info.get("status_url") or API + "/bulk_download/status/"
    for _ in range(240):
        response = requests.get(status_url,
                                params=None if info.get("status_url") else {"file_name": info["file_name"]},
                                timeout=60, verify=False)
        response.raise_for_status()
        status = response.json()
        if status.get("status") in {"finished", "ready"}:
            break
        if status.get("status") in {"failed", "error"}:
            raise RuntimeError(f"Bulk download failed: {status}")
        time.sleep(15)
    else:
        raise TimeoutError(f"Bulk download timed out for {start} to {end}")
    path = os.path.join(workdir, f"{start}-{end}.zip")
    for attempt in range(10):
        try:
            with requests.get(info["file_url"], stream=True,
                              timeout=(30, 600), verify=False) as response:
                if response.status_code < 400:
                    with open(path, "wb") as out:
                        for chunk in response.iter_content(1024 * 1024):
                            out.write(chunk)
                    break
                if response.status_code not in {403, 404, 429, 500, 502, 503, 504}:
                    response.raise_for_status()
        except requests.RequestException:
            if attempt == 9:
                raise
        if os.path.exists(path):
            os.remove(path)
        time.sleep(min(2 ** attempt, 30))
    else:
        raise RuntimeError(f"Generated ZIP unavailable for {start} to {end}")
    return path, status


def validate_and_stage_week(s3, start, end, zip_path, status, run_id, workdir):
    csv_path = os.path.join(workdir, f"{start}-{end}.csv")
    keys = set()
    rows = 0
    with zipfile.ZipFile(zip_path) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(members) != 1:
            raise RuntimeError(f"Expected one CSV for {start} to {end}: {members}")
        with archive.open(members[0]) as source, open(csv_path, "wb") as target:
            while chunk := source.read(1024 * 1024):
                target.write(chunk)
    with open(csv_path, newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        columns = reader.fieldnames
        if not columns or not {"contract_transaction_unique_key", "action_date"}.issubset(columns):
            raise RuntimeError("Bulk CSV is missing transaction key or action date")
        for row in reader:
            key = row["contract_transaction_unique_key"]
            action = date.fromisoformat(row["action_date"])
            if not key or key in keys or not start <= action <= end:
                raise RuntimeError(f"Duplicate/missing key or action date outside {start} to {end}")
            keys.add(key)
            rows += 1
    if rows != int(status.get("total_rows", -1)):
        raise RuntimeError(f"Download row count mismatch for {start} to {end}: {rows} vs {status}")
    prefix = f"mimir/rehearsals/embargo-reconcile-{run_id}/{start}-{end}/"
    source_key = prefix + "source.zip"
    digest = hashlib.sha256()
    with open(zip_path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    s3.upload_file(zip_path, BUCKET, source_key)
    source_head = s3.head_object(Bucket=BUCKET, Key=source_key)
    bronze_key = f"{BRONZE_PREFIX}run_id={run_id}/{start}-{end}.csv"
    s3.upload_file(csv_path, BUCKET, bronze_key)
    bronze_head = s3.head_object(Bucket=BUCKET, Key=bronze_key)
    return {"start": start.isoformat(), "end": end.isoformat(), "rows": rows,
            "columns": columns, "keys": keys, "source_key": source_key,
            "source_sha256": digest.hexdigest(),
            "source_version": source_head.get("VersionId"), "bronze_key": bronze_key,
            "bronze_version": bronze_head.get("VersionId")}


def active_silver_writer(glue):
    runs = glue.get_job_runs(JobName=SILVER_JOB, MaxResults=5)["JobRuns"]
    return any(r["JobRunState"] in {"STARTING", "RUNNING", "STOPPING", "WAITING"} for r in runs)


def snapshot_partition(s3, glue, fy, run_id):
    if active_silver_writer(glue):
        raise RuntimeError("Silver writer is active; retry reconciliation later")
    prefix = f"{SILVER_PREFIX}action_date_fiscal_year={fy}/"
    objects = [o for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix)
               for o in page.get("Contents", [])]
    if not objects:
        raise RuntimeError(f"Silver FY{fy} is empty; automatic merge is unsafe")
    copied = []
    for obj in objects:
        source = obj["Key"]
        head = s3.head_object(Bucket=BUCKET, Key=source)
        if head["ETag"] != obj["ETag"] or head["ContentLength"] != obj["Size"]:
            raise RuntimeError("Silver partition changed during snapshot")
        key = f"mimir/change-control/embargo-reconcile-{run_id}/silver-before/" + source[len(prefix):]
        source_pin = {"Bucket": BUCKET, "Key": source}
        if head.get("VersionId"):
            source_pin["VersionId"] = head["VersionId"]
        out = s3.copy_object(Bucket=BUCKET, Key=key, CopySource=source_pin,
                             CopySourceIfMatch=head["ETag"])
        copied.append({"source": source, "version": head.get("VersionId"),
                       "snapshot": key, "snapshot_version": out.get("VersionId"),
                       "bytes": obj["Size"]})
    if active_silver_writer(glue):
        raise RuntimeError("Silver writer started during snapshot")
    after = {o["Key"]: (o["ETag"], o["Size"])
             for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix)
             for o in page.get("Contents", [])}
    before = {o["Key"]: (o["ETag"], o["Size"]) for o in objects}
    if after != before:
        raise RuntimeError("Silver partition changed during snapshot")
    return copied


def main():
    s3 = boto3.client("s3", region_name=REGION)
    athena = boto3.client("athena", region_name=REGION)
    glue = boto3.client("glue", region_name=REGION)
    gap = select_gap(athena, date.today())
    if not gap:
        print("Recent closed action months match published DoD counts")
        return
    start, end, expected, before = gap
    run_id = "embargo-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(f"Repairing {start:%Y-%m}: official={expected}, silver={before}, run_id={run_id}")
    staged = []
    all_keys = set()
    header = None
    with tempfile.TemporaryDirectory() as workdir:
        for week_start, week_end in weekly_windows(start, end):
            zip_path, status = download_week(week_start, week_end, workdir)
            item = validate_and_stage_week(s3, week_start, week_end, zip_path, status, run_id, workdir)
            if header is not None and item["columns"] != header:
                raise RuntimeError("Bulk CSV schema differs across weeks")
            header = item["columns"]
            if all_keys.intersection(item["keys"]):
                raise RuntimeError("Bulk download contains cross-week duplicate transaction keys")
            all_keys.update(item.pop("keys"))
            staged.append(item)
            os.remove(zip_path)
            os.remove(os.path.join(workdir, f"{week_start}-{week_end}.csv"))
    if len(all_keys) != expected:
        raise RuntimeError(f"Published monthly count changed or download incomplete: {len(all_keys)} vs {expected}")
    fy = start.year + (1 if start.month >= 10 else 0)
    snapshot = snapshot_partition(s3, glue, fy, run_id)
    evidence = {"run_id": run_id, "month": start.strftime("%Y-%m"), "official_count": expected,
                "silver_before": before, "staged": staged, "snapshot": snapshot}
    key = f"mimir/change-control/embargo-reconcile-{run_id}/evidence.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(evidence, default=str).encode(),
                  ContentType="application/json")
    if active_silver_writer(glue):
        raise RuntimeError("Silver writer started before merge; retry later from staged artifacts")
    job_run = glue.start_job_run(JobName=SILVER_JOB, Arguments={
        "--RUN_ID": run_id,
        "--STATE_URI": f"s3://{BUCKET}/state/usaspending/prime_contracts_last_run.json",
    })["JobRunId"]
    print(f"Silver merge started: {job_run}")
    for _ in range(720):
        result = glue.get_job_run(JobName=SILVER_JOB, RunId=job_run)["JobRun"]
        if result["JobRunState"] == "SUCCEEDED":
            break
        if result["JobRunState"] in {"FAILED", "TIMEOUT", "STOPPED", "ERROR"}:
            raise RuntimeError(f"Silver merge {job_run} failed: {result.get('ErrorMessage')}; snapshot={key}")
        time.sleep(10)
    else:
        raise TimeoutError(f"Silver merge still running: {job_run}; snapshot={key}")
    after = silver_counts(athena, [(start, end)]).get(start.strftime("%Y-%m"), 0)
    tolerance = max(100, int(expected * 0.005))
    if abs(expected - after) > tolerance:
        raise RuntimeError(f"Post-merge count mismatch {start:%Y-%m}: {after} vs {expected}; snapshot={key}")
    print(json.dumps({"run_id": run_id, "month": start.strftime("%Y-%m"),
                      "silver_before": before, "silver_after": after,
                      "official_count": expected, "snapshot": key}))


if __name__ == "__main__":
    main()
