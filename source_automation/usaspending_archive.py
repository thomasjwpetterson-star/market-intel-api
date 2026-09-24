"""Acquire a full USAspending contract download into an immutable candidate.

This deliberately does not update the production Bronze/Silver paths.  A run
downloads the same full CSV product used for historical loads, identifies prime
and subaward members by their schemas, and writes a manifest that a separate
candidate Glue transform consumes.
"""

from __future__ import annotations

import csv
from datetime import date, datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import tempfile
import time
from typing import Any, Callable, Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import zipfile


BULK_AWARDS = "https://api.usaspending.gov/api/v2/bulk_download/awards/"
SOURCE_ID = "usaspending-contract-archive"
PRIME_REQUIRED = frozenset(
    {
        "contract_transaction_unique_key",
        "action_date_fiscal_year",
        "last_modified_date",
    }
)
SUBAWARD_REQUIRED = frozenset(
    {
        "subaward_sam_report_id",
        "subaward_action_date_fiscal_year",
        "subaward_sam_report_last_modified_date",
        "prime_award_unique_key",
    }
)


def current_fiscal_year(on_date: date) -> int:
    return on_date.year + (1 if on_date.month >= 10 else 0)


def reconciliation_fiscal_years(on_date: date) -> tuple[int, int]:
    current = current_fiscal_year(on_date)
    return current, current - 1


CONTRACT_TYPES = (
    "A",
    "B",
    "C",
    "D",
    "IDV_A",
    "IDV_B",
    "IDV_B_A",
    "IDV_B_B",
    "IDV_B_C",
    "IDV_C",
    "IDV_D",
    "IDV_E",
)


def fiscal_year_range(fiscal_year: int) -> tuple[str, str]:
    return f"{fiscal_year - 1}-10-01", f"{fiscal_year}-09-30"


def bulk_awards_request(fiscal_year: int) -> dict[str, object]:
    start_date, end_date = fiscal_year_range(fiscal_year)
    return {
        "filters": {
            # USAspending's Download Center accepts this sentinel and normalizes
            # it to an unfiltered agency list in the generated request.
            "agencies": [
                {"type": "awarding", "tier": "toptier", "name": "All"}
            ],
            "prime_award_types": list(CONTRACT_TYPES),
            "sub_award_types": ["procurement"],
            "date_type": "action_date",
            "date_range": {"start_date": start_date, "end_date": end_date},
        },
        "file_format": "csv",
        # An empty selection asks for the complete publisher-defined columns.
        "columns": [],
    }


def classify_columns(columns: Iterable[str]) -> str | None:
    names = frozenset(str(value).strip() for value in columns)
    if PRIME_REQUIRED <= names:
        return "prime_contracts"
    if SUBAWARD_REQUIRED <= names:
        return "sub_contracts"
    return None


def _json_request(
    url: str,
    payload: dict[str, object],
    *,
    attempts: int = 4,
    opener: Callable[..., Any] = urlopen,
) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    request = Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": "Mimir/1.0"},
        method="POST",
    )
    for attempt in range(attempts):
        try:
            with opener(request, timeout=120) as response:
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError) as error:
            if attempt + 1 == attempts:
                raise RuntimeError(f"USAspending file discovery failed: {error}") from error
            time.sleep(2**attempt)
    raise AssertionError("unreachable")


def _get_json(url: str, *, attempts: int = 4) -> dict[str, Any]:
    request = Request(url, headers={"User-Agent": "Mimir/1.0"})
    for attempt in range(attempts):
        try:
            with urlopen(request, timeout=120) as response:
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError) as error:
            if attempt + 1 == attempts:
                raise RuntimeError(f"USAspending status request failed: {error}") from error
            time.sleep(2**attempt)
    raise AssertionError("unreachable")


def start_full_contract_download(fiscal_year: int) -> dict[str, Any]:
    request_payload = bulk_awards_request(fiscal_year)
    response = _json_request(BULK_AWARDS, request_payload)
    file_name = str(response.get("file_name") or "").strip()
    file_url = str(response.get("file_url") or "").strip()
    status_url = str(response.get("status_url") or "").strip()
    if not file_name or not file_url or not status_url:
        raise RuntimeError(
            f"USAspending bulk response is missing file metadata for FY{fiscal_year}"
        )
    if not file_url.startswith("https://files.usaspending.gov/"):
        raise RuntimeError(f"Unexpected USAspending download host: {file_url}")
    if not status_url.startswith("https://api.usaspending.gov/"):
        raise RuntimeError(f"Unexpected USAspending status host: {status_url}")

    return {
        "fiscal_year": fiscal_year,
        "file_name": file_name,
        "file_url": file_url,
        "status_url": status_url,
        "request": request_payload,
        "download_request": response.get("download_request"),
    }


def wait_for_full_contract_download(
    source: dict[str, Any],
    *,
    poll_seconds: float = 15.0,
    timeout_seconds: float = 7200.0,
) -> dict[str, Any]:
    file_name = str(source["file_name"])
    status_url = str(source["status_url"])

    deadline = time.monotonic() + timeout_seconds
    while True:
        status_payload = _get_json(status_url)
        status = str(status_payload.get("status") or "").lower()
        if status in {"finished", "ready"}:
            return {**source, "status": status_payload}
        if status in {"failed", "error"}:
            raise RuntimeError(
                f"USAspending full prime/subaward download failed: {status_payload}"
            )
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"USAspending full prime/subaward download timed out: {file_name}"
            )
        time.sleep(poll_seconds)


def request_full_contract_download(
    fiscal_year: int,
    *,
    poll_seconds: float = 15.0,
    timeout_seconds: float = 7200.0,
) -> dict[str, Any]:
    return wait_for_full_contract_download(
        start_full_contract_download(fiscal_year),
        poll_seconds=poll_seconds,
        timeout_seconds=timeout_seconds,
    )


def request_full_contract_downloads(
    fiscal_years: Iterable[int],
    *,
    poll_seconds: float = 15.0,
    timeout_seconds: float = 7200.0,
) -> list[dict[str, Any]]:
    """Submit all FY exports first so USAspending generates them in parallel."""
    sources = [start_full_contract_download(int(year)) for year in fiscal_years]
    pending = {int(source["fiscal_year"]): source for source in sources}
    finished: dict[int, dict[str, Any]] = {}
    deadline = time.monotonic() + timeout_seconds

    while pending:
        for fiscal_year, source in list(pending.items()):
            status_payload = _get_json(str(source["status_url"]))
            status = str(status_payload.get("status") or "").lower()
            if status in {"finished", "ready"}:
                finished[fiscal_year] = {**source, "status": status_payload}
                del pending[fiscal_year]
            elif status in {"failed", "error"}:
                raise RuntimeError(
                    f"USAspending FY{fiscal_year} full download failed: {status_payload}"
                )
        if pending:
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "USAspending full prime/subaward downloads timed out for "
                    f"FYs {sorted(pending)}"
                )
            time.sleep(poll_seconds)
    return [finished[int(year)] for year in fiscal_years]


def _download(url: str, destination: Path) -> str:
    request = Request(url, headers={"User-Agent": "Mimir/1.0"})
    digest = hashlib.sha256()
    try:
        with urlopen(request, timeout=300) as response, destination.open("wb") as output:
            while True:
                chunk = response.read(8 * 1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
                output.write(chunk)
    except (HTTPError, URLError, TimeoutError) as error:
        raise RuntimeError(f"USAspending archive download failed: {error}") from error
    return digest.hexdigest()


def _inspect_csv(zf: zipfile.ZipFile, member: str) -> dict[str, object]:
    digest = hashlib.sha256()
    with zf.open(member) as raw:
        while True:
            chunk = raw.read(8 * 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)

    with zf.open(member) as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        reader = csv.reader(text)
        try:
            columns = next(reader)
        except StopIteration as error:
            raise RuntimeError(f"Empty CSV member in USAspending archive: {member}") from error
        row_count = sum(1 for _ in reader)

    dataset = classify_columns(columns)
    if dataset is None:
        return {}
    if row_count <= 0:
        raise RuntimeError(f"USAspending {dataset} member has no rows: {member}")
    normalized_header = "\x1f".join(value.strip() for value in columns)
    return {
        "dataset": dataset,
        "member": member,
        "row_count": row_count,
        "column_count": len(columns),
        "columns": columns,
        "schema_sha256": hashlib.sha256(normalized_header.encode("utf-8")).hexdigest(),
        "sha256": digest.hexdigest(),
        "uncompressed_bytes": zf.getinfo(member).file_size,
    }


def inspect_archive(path: Path) -> list[dict[str, object]]:
    if not zipfile.is_zipfile(path):
        raise RuntimeError(f"USAspending response is not a valid ZIP archive: {path}")
    results: list[dict[str, object]] = []
    with zipfile.ZipFile(path) as zf:
        for member in zf.namelist():
            if not member.lower().endswith(".csv"):
                continue
            inspected = _inspect_csv(zf, member)
            if inspected:
                results.append(inspected)
    datasets = {str(item["dataset"]) for item in results}
    required = {"prime_contracts", "sub_contracts"}
    if not required <= datasets:
        raise RuntimeError(
            "USAspending full archive did not contain both prime and subaward CSVs; "
            f"found {sorted(datasets)}"
        )
    return results


def candidate_prefix(run_id: str) -> str:
    if not run_id.strip() or "/" in run_id:
        raise ValueError("run_id must be a non-empty path-safe value")
    return f"mimir/raw-source-candidates/{SOURCE_ID}/{run_id}/"


def acquire_candidate(
    s3: Any,
    bucket: str,
    run_id: str,
    fiscal_years: Iterable[int],
) -> dict[str, object]:
    prefix = candidate_prefix(run_id)
    fiscal_years = tuple(int(value) for value in fiscal_years)
    if not fiscal_years:
        raise ValueError("at least one fiscal year is required")
    created_at = datetime.now(timezone.utc).isoformat()
    archives: list[dict[str, object]] = []
    artifacts: list[dict[str, object]] = []
    sources = request_full_contract_downloads(fiscal_years)

    with tempfile.TemporaryDirectory(prefix="mimir-usaspending-") as temp_dir:
        for source in sources:
            fiscal_year = int(source["fiscal_year"])
            local_zip = Path(temp_dir) / str(source["file_name"])
            archive_sha256 = _download(str(source["file_url"]), local_zip)
            inspected = inspect_archive(local_zip)
            landing_key = f"{prefix}landing/fy={fiscal_year}/{local_zip.name}"
            s3.upload_file(
                str(local_zip),
                bucket,
                landing_key,
                ExtraArgs={
                    "ServerSideEncryption": "AES256",
                    "Metadata": {
                        "sha256": archive_sha256,
                        "source-id": SOURCE_ID,
                        "source-fiscal-year": str(fiscal_year),
                    },
                },
            )
            archives.append(
                {
                    "fiscal_year": fiscal_year,
                    "source_url": source["file_url"],
                    "status_url": source["status_url"],
                    "file_name": source["file_name"],
                    "landing_key": landing_key,
                    "sha256": archive_sha256,
                    "compressed_bytes": local_zip.stat().st_size,
                    "request": source["request"],
                    "download_request": source.get("download_request"),
                    "source_status": source.get("status"),
                }
            )
            with zipfile.ZipFile(local_zip) as zf:
                for item in inspected:
                    member = str(item["member"])
                    name = Path(member).name
                    raw_key = (
                        f"{prefix}bronze/dataset={item['dataset']}/"
                        f"fy={fiscal_year}/{name}"
                    )
                    with zf.open(member) as source_file:
                        s3.upload_fileobj(
                            source_file,
                            bucket,
                            raw_key,
                            ExtraArgs={
                                "ServerSideEncryption": "AES256",
                                "Metadata": {
                                    "sha256": str(item["sha256"]),
                                    "row-count": str(item["row_count"]),
                                    "schema-sha256": str(item["schema_sha256"]),
                                    "source-id": SOURCE_ID,
                                },
                            },
                        )
                    artifacts.append(
                        {
                            **item,
                            "fiscal_year": fiscal_year,
                            "candidate_key": raw_key,
                        }
                    )
            # Keep peak Fargate scratch usage to one fiscal-year archive.
            local_zip.unlink()

    manifest: dict[str, object] = {
        "manifest_version": 1,
        "source_id": SOURCE_ID,
        "role": "canonical-reconciliation",
        "run_id": run_id,
        "created_at": created_at,
        "status": "validated-candidate",
        "scope": {
            "agency": "all",
            "fiscal_years": sorted(set(fiscal_years)),
            "archive_type": "prime-transactions-and-procurement-subawards",
            "columns": "all-source-columns",
        },
        "archives": archives,
        "artifacts": artifacts,
        "production_mutations": [],
        "promotion_required": True,
    }
    manifest_key = f"{prefix}manifest.json"
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=(json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )
    return {"manifest_key": manifest_key, "manifest": manifest}
