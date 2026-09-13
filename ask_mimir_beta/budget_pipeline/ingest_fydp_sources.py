"""Download, validate, hash and archive public FYDP source books in S3."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent
DEFAULT_MANIFEST = ROOT / "fydp_source_manifest.json"


def download_pdf(url: str, destination: Path) -> None:
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; MimirAdvisorsBudgetArchive/1.0)",
            "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
        },
    )
    with urlopen(request, timeout=180) as response, destination.open("wb") as output:
        shutil.copyfileobj(response, output)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_pdf(path: Path) -> None:
    if not path.exists() or path.stat().st_size < 10_000:
        raise ValueError(f"Downloaded file is missing or implausibly small: {path}")
    with path.open("rb") as handle:
        if handle.read(5) != b"%PDF-":
            raise ValueError(f"Source did not return a PDF: {path}")


def upload_file(session: Any, bucket: str, key: str, path: Path, metadata: Dict[str, str]) -> None:
    session.client("s3").upload_file(
        str(path),
        bucket,
        key,
        ExtraArgs={"ContentType": "application/pdf", "Metadata": metadata},
    )


def parse_s3_prefix(prefix: str) -> tuple[str, str]:
    if not prefix.startswith("s3://"):
        raise ValueError(f"Not an S3 prefix: {prefix}")
    bucket, _, key_prefix = prefix[5:].partition("/")
    return bucket, key_prefix.rstrip("/") + "/"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--download-dir", type=Path, required=True)
    parser.add_argument("--profile", default="new-account")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--no-upload", action="store_true")
    parser.add_argument("--reuse-downloads", action="store_true")
    args = parser.parse_args()

    manifest = json.loads(args.manifest.read_text())
    args.download_dir.mkdir(parents=True, exist_ok=True)
    bucket, key_prefix = parse_s3_prefix(manifest["s3_prefix"])

    session = None
    if not args.no_upload:
        import boto3

        session = boto3.Session(profile_name=args.profile, region_name=args.region)

    resolved = dict(manifest)
    resolved["archived_at"] = datetime.now(timezone.utc).isoformat()
    resolved_sources = []
    failed_sources = []

    for source in manifest["sources"]:
        destination = args.download_dir / source["local_filename"]
        try:
            if not (args.reuse_downloads and destination.exists()):
                print(f"Downloading {source['document_title']}...")
                download_errors = []
                for candidate_url in (
                    source["download_url"],
                    source.get("archive_download_url"),
                ):
                    if not candidate_url:
                        continue
                    try:
                        download_pdf(candidate_url, destination)
                        validate_pdf(destination)
                        break
                    except Exception as exc:
                        download_errors.append(f"{candidate_url}: {exc}")
                else:
                    raise ValueError("; ".join(download_errors))
            validate_pdf(destination)
            checksum = sha256_file(destination)
            archived = dict(source)
            archived["archive_status"] = "ARCHIVED" if session is not None else "VALIDATED"
            archived["sha256"] = checksum
            archived["bytes"] = destination.stat().st_size
            archived["retrieved_at"] = datetime.now(timezone.utc).isoformat()
            archived["s3_uri"] = f"s3://{bucket}/{key_prefix}{source['s3_key']}"
            resolved_sources.append(archived)

            if session is not None:
                print(f"Uploading {archived['s3_uri']}...")
                upload_file(
                    session,
                    bucket,
                    key_prefix + source["s3_key"],
                    destination,
                    {
                        "source-id": source["source_id"],
                        "sha256": checksum,
                        "submission-fy": str(manifest["submission_fiscal_year"]),
                    },
                )
        except Exception as exc:
            failed = dict(source)
            failed["archive_status"] = "DOWNLOAD_OR_VALIDATION_FAILED"
            failed["error"] = str(exc)
            failed_sources.append(failed)
            print(f"FAILED {source['source_id']}: {exc}")

    resolved["sources"] = resolved_sources
    resolved["failed_sources"] = failed_sources
    resolved_path = args.download_dir / "fydp_source_manifest.resolved.json"
    resolved_path.write_text(json.dumps(resolved, indent=2) + "\n")

    if session is not None:
        session.client("s3").upload_file(
            str(resolved_path),
            bucket,
            key_prefix + "fydp_source_manifest.resolved.json",
            ExtraArgs={"ContentType": "application/json"},
        )
    print(f"Resolved manifest: {resolved_path}")
    if failed_sources:
        print(
            "Some source sites blocked automated download. Download those PDFs from their "
            "landing pages into the named local files, then rerun with --reuse-downloads."
        )


if __name__ == "__main__":
    main()
