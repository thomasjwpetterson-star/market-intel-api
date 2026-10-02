import sys, json, time, datetime, zipfile, io
import boto3
import requests
import urllib3
from awsglue.utils import getResolvedOptions

# TEMP: because your Glue environment is failing cert validation (TLS interception)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "AWS_REGION", "LANDING_ROOT", "BRONZE_ROOT", "STATE_URI"])

AWS_REGION   = args.get("AWS_REGION", "us-east-1")
LANDING_ROOT = args["LANDING_ROOT"]
BRONZE_ROOT  = args["BRONZE_ROOT"]
STATE_URI    = args["STATE_URI"]

API_BASE = "https://api.usaspending.gov"
BULK_AWARDS_ENDPOINT = f"{API_BASE}/api/v2/bulk_download/awards/"
STATUS_ENDPOINT      = f"{API_BASE}/api/v2/bulk_download/status/"
DATE_TYPE = "last_modified_date"
OVERLAP_DAYS = 3

s3 = boto3.client("s3", region_name=AWS_REGION)

DOWNLOAD_RETRY_STATUS = {403, 404, 429, 500, 502, 503, 504}

def parse_s3_uri(uri: str):
    assert uri.startswith("s3://")
    rest = uri[5:]
    bucket, _, key = rest.partition("/")
    return bucket, key

def s3_put_json(uri: str, obj: dict):
    b, k = parse_s3_uri(uri)
    s3.put_object(Bucket=b, Key=k, Body=json.dumps(obj, indent=2).encode("utf-8"))

def s3_get_json(uri: str):
    b, k = parse_s3_uri(uri)
    try:
        resp = s3.get_object(Bucket=b, Key=k)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except Exception:
        return None

def iso_date(d: datetime.date) -> str:
    return d.strftime("%Y-%m-%d")

def download_generated_zip(file_url: str, attempts: int = 10) -> bytes:
    """Wait through the brief status-ready/file-host-not-ready publication race."""
    for attempt in range(attempts):
        try:
            response = requests.get(file_url, stream=True, timeout=300, verify=False)
            if response.status_code < 400:
                return response.content
            if response.status_code not in DOWNLOAD_RETRY_STATUS or attempt + 1 == attempts:
                print("DOWNLOAD HTTP", response.status_code, "body:", response.text[:2000])
                response.raise_for_status()
        except requests.RequestException:
            if attempt + 1 == attempts:
                raise
        delay_seconds = min(2 ** attempt, 30)
        print(
            f"Generated file is not readable yet; retrying in {delay_seconds}s "
            f"(attempt {attempt + 2}/{attempts})."
        )
        time.sleep(delay_seconds)
    raise RuntimeError("USAspending generated file was not readable after retries")

def main():
    today = datetime.date.today()
    # USAspending can first publish DoD transactions roughly 90 days after their
    # action date.  Querying by modification date lets a short rolling window
    # collect those late publications and later corrections regardless of the
    # original action date.
    default_start = today - datetime.timedelta(days=OVERLAP_DAYS)

    state = s3_get_json(STATE_URI) or {}
    last_end = state.get("end_date")  # YYYY-MM-DD

    if last_end:
        try:
            last_end_dt = datetime.datetime.strptime(last_end, "%Y-%m-%d").date()
            # Keep continuity after a missed run; on the normal daily cadence,
            # this resolves to the short rolling overlap above.
            start_dt = min(
                default_start,
                last_end_dt - datetime.timedelta(days=OVERLAP_DAYS),
            )
        except Exception:
            start_dt = default_start
    else:
        start_dt = default_start

    end_dt = today
    run_id = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    print(f"Using {DATE_TYPE} window start={start_dt} end={end_dt} run_id={run_id}")
    print(f"AWS_REGION={AWS_REGION}")
    print(f"LANDING_ROOT={LANDING_ROOT}")
    print(f"BRONZE_ROOT={BRONZE_ROOT}")
    print(f"STATE_URI={STATE_URI}")

    # IMPORTANT: filters must be top-level dict for this endpoint
    payload = {
    "filters": {
        # DoD-only (awarding agency)
        "agencies": [
            {"type": "awarding", "tier": "toptier", "name": "Department of Defense"}
        ],

        # Contracts + IDVs (prime contract universe)
        "prime_award_types": [
            "A","B","C","D",
            "IDV_A","IDV_B","IDV_B_A","IDV_B_B","IDV_B_C","IDV_C","IDV_D","IDV_E"
        ],

        # Date filter
        "date_type": DATE_TYPE,
        "date_range": {
            "start_date": iso_date(start_dt),
            "end_date": iso_date(end_dt)
        }
    },

    "file_format": "csv",
    "columns": []
}

    print("POST bulk download request...")
    r = requests.post(BULK_AWARDS_ENDPOINT, json=payload, timeout=120, verify=False)
    if r.status_code >= 400:
        print("POST payload:", payload)
        print("HTTP", r.status_code, "response body:", r.text[:8000])
    r.raise_for_status()
    info = r.json()

    file_name = info.get("file_name")
    file_url  = info.get("file_url")
    status_url = info.get("status_url") or STATUS_ENDPOINT
    if not file_name or not file_url:
        raise RuntimeError(f"Unexpected response (missing file_name/file_url): {info}")

    print(f"file_name={file_name}")
    print(f"file_url={file_url}")

    print("Polling status...")
    while True:
        status_params = None if info.get("status_url") else {"file_name": file_name}
        sr = requests.get(status_url, params=status_params, timeout=60, verify=False)
        if sr.status_code >= 400:
            print("STATUS HTTP", sr.status_code, "body:", sr.text[:2000])
        sr.raise_for_status()
        s = sr.json()
        status = s.get("status")
        print(f"status={status}")
        if status in ("finished", "ready"):
            break
        if status in ("failed", "error"):
            raise RuntimeError(f"Bulk download failed: {s}")
        time.sleep(15)

    print("Downloading ZIP...")
    zip_bytes = download_generated_zip(file_url)
    print(f"ZIP size bytes={len(zip_bytes)}")

    # Upload zip to landing
    landing_bucket, landing_prefix = parse_s3_uri(LANDING_ROOT)
    landing_key = f"{landing_prefix}{run_id}/{file_name}"
    print(f"Uploading ZIP to s3://{landing_bucket}/{landing_key}")
    s3.put_object(Bucket=landing_bucket, Key=landing_key, Body=zip_bytes)

    # Unzip and upload CSV(s) into bronze/run_id=.../
    bronze_bucket, bronze_prefix = parse_s3_uri(BRONZE_ROOT)
    bronze_run_prefix = f"{bronze_prefix}run_id={run_id}/"
    print(f"Unzipping and uploading CSV(s) to s3://{bronze_bucket}/{bronze_run_prefix}")

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".csv"):
                continue
            data = zf.read(name)
            base = name.split("/")[-1]
            out_key = bronze_run_prefix + base
            print(f" -> {out_key} ({len(data)} bytes)")
            s3.put_object(Bucket=bronze_bucket, Key=out_key, Body=data)

    new_state = {
        "run_id": run_id,
        "date_type": DATE_TYPE,
        "start_date": iso_date(start_dt),
        "end_date": iso_date(end_dt),
        "file_name": file_name,
        "file_url": file_url,
        "landing_zip": f"s3://{landing_bucket}/{landing_key}",
        "bronze_prefix": f"s3://{bronze_bucket}/{bronze_run_prefix}"
    }
    print(f"Writing state to {STATE_URI}")
    s3_put_json(STATE_URI, new_state)

    print("DONE")

if __name__ == "__main__":
    main()
