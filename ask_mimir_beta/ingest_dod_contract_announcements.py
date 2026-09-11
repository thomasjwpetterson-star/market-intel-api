"""Ingest daily DoD contract announcements with immutable source provenance."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import boto3
import pandas as pd


RSS_URL = (
    "https://www.defense.gov/DesktopModules/ArticleCS/RSS.ashx?"
    "ContentType=400&Site=945&max=100"
)
DIRECT_USER_AGENT = "MimirDataBot/1.0 (+https://www.mimiradvisors.org)"
TEXT_RENDER_PREFIX = "https://r.jina.ai/http://"
ARTICLE_ID_PATTERN = re.compile(r"/Article/(\d+)", re.IGNORECASE)
SERVICE_HEADING_PATTERN = re.compile(r"^\*\*([A-Z][A-Z &./'()-]{2,})\*\*$")
CONTRACT_ID_PATTERNS = (
    re.compile(r"\b[A-Z0-9]{5,10}-\d{2,4}-[A-Z0-9]-[A-Z0-9]{3,10}\b"),
    re.compile(r"\b[A-Z]{1,8}\d[A-Z0-9]{8,20}\b"),
)
AMOUNT_PATTERN = re.compile(
    r"\$\s*([\d,]+(?:\.\d+)?)\s*(billion|million|thousand)?",
    re.IGNORECASE,
)
AWARD_MARKERS = re.compile(
    r"\b(?:has been|have been|is being|are being|was|were|is|are|will be)\s+"
    r"(?:awarded|modified)|\bwill compete for each order\b",
    re.IGNORECASE,
)


def _fetch(url: str, timeout: int = 90) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": DIRECT_USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def discover_articles(xml_body: bytes, since: datetime) -> List[Dict[str, str]]:
    root = ET.fromstring(xml_body)
    articles: List[Dict[str, str]] = []
    for item in root.findall("./channel/item"):
        title = " ".join((item.findtext("title") or "").split())
        url = (item.findtext("link") or "").strip()
        published_text = (item.findtext("pubDate") or "").strip()
        if not title.lower().startswith("contracts for ") or not url:
            continue
        published_at = datetime.strptime(
            published_text, "%a, %d %b %Y %H:%M:%S %Z"
        ).replace(tzinfo=timezone.utc)
        if published_at < since:
            continue
        article_match = ARTICLE_ID_PATTERN.search(url)
        if not article_match:
            continue
        articles.append(
            {
                "article_id": article_match.group(1),
                "title": title,
                "url": url,
                "published_at": published_at.isoformat(),
            }
        )
    return articles


def fetch_article_text(url: str, allow_text_renderer: bool = True) -> Tuple[str, str, str]:
    try:
        body = _fetch(url)
        text = body.decode("utf-8", errors="replace")
        # The parser consumes plain text/Markdown. If the official endpoint returns
        # HTML, use the rendering fallback while retaining the official source URL.
        if not re.search(r"<(?:!doctype|html|body)\b", text[:4096], re.IGNORECASE):
            return text, url, "official_page"
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
        if not allow_text_renderer:
            raise
    if not allow_text_renderer:
        raise ValueError("official contract page did not return parseable plain text")
    source_without_scheme = re.sub(r"^https?://", "", url)
    fetch_url = TEXT_RENDER_PREFIX + source_without_scheme
    body = _fetch(fetch_url)
    return body.decode("utf-8", errors="replace"), fetch_url, "text_renderer"


def _money_value(match: re.Match[str]) -> float:
    value = float(match.group(1).replace(",", ""))
    scale = (match.group(2) or "").lower()
    if scale == "billion":
        value *= 1_000_000_000
    elif scale == "million":
        value *= 1_000_000
    elif scale == "thousand":
        value *= 1_000
    return value


def _sentence_containing(text: str, marker: str) -> str | None:
    for sentence in re.split(r"(?<=[.!?])\s+", text):
        if marker in sentence.lower():
            return sentence.strip()
    return None


def _obligated_at_announcement(text: str) -> float | None:
    values: List[float] = []
    explicit_deferred = False
    for sentence in re.split(r"(?<=[.!?])\s+", text):
        if "obligat" not in sentence.lower():
            continue
        if re.search(
            r"\bno funds? (?:will be|are|is) obligated\b",
            sentence,
            re.IGNORECASE,
        ):
            explicit_deferred = True
            continue
        marker = re.search(
            r"\b(?:are|is|were|was|will be|being)?\s*obligated\s+"
            r"at\s+(?:the\s+)?time\s+of\s+(?:the\s+)?award\b",
            sentence,
            re.IGNORECASE,
        )
        if marker:
            funding_text = sentence[: marker.start()]
            funding_cue = re.search(
                r"\b(?:fiscal\s+20\d{2}|funds?\s+in\s+the\s+amount\s+of)\b",
                funding_text,
                re.IGNORECASE,
            )
            if funding_cue:
                funding_text = funding_text[funding_cue.start() :]
            amounts = list(AMOUNT_PATTERN.finditer(funding_text))
            if amounts:
                values.append(sum(_money_value(match) for match in amounts))
            continue
        if re.search(
            r"\bobligated\s+(?:at\s+)?(?:a\s+)?later\s+time\b|"
            r"\bobligated\s+when\s+funds\s+are\s+available\b",
            sentence,
            re.IGNORECASE,
        ):
            explicit_deferred = True
    if values:
        return sum(values)
    return 0.0 if explicit_deferred else None


def _contract_ids(text: str) -> List[str]:
    positioned: List[Tuple[int, str]] = []
    for pattern in CONTRACT_ID_PATTERNS:
        for match in pattern.finditer(text.upper()):
            value = match.group(0)
            if value.startswith("P000"):
                continue
            positioned.append((match.start(), value))
    values: List[str] = []
    for _, value in sorted(positioned):
        if value not in values:
            values.append(value)
    return values


def _recipient_text(text: str) -> str | None:
    marker = AWARD_MARKERS.search(text)
    if not marker:
        return None
    recipient = text[: marker.start()].strip(" ,;*")
    return recipient or None


def _entry_type(text: str) -> str:
    upper = text.upper().lstrip()
    if upper.startswith("CORRECTION:"):
        return "correction"
    if upper.startswith("UPDATE:"):
        return "update"
    return "award"


def parse_announcement(
    article: Dict[str, str],
    page_text: str,
    *,
    fetch_url: str,
    fetch_method: str,
    retrieved_at: datetime,
) -> List[Dict[str, Any]]:
    content = page_text.split("Markdown Content:", 1)[-1]
    paragraphs = [
        " ".join(paragraph.split())
        for paragraph in re.split(r"\n\s*\n", content)
        if paragraph.strip()
    ]
    source_hash = hashlib.sha256(page_text.encode("utf-8")).hexdigest()
    current_service: str | None = None
    records: List[Dict[str, Any]] = []
    for paragraph in paragraphs:
        heading = SERVICE_HEADING_PATTERN.fullmatch(paragraph)
        if heading:
            current_service = heading.group(1).strip()
            continue
        if not current_service:
            continue
        entry_type = _entry_type(paragraph)
        contract_ids = _contract_ids(paragraph)
        if (
            entry_type == "award"
            and not contract_ids
            and not AWARD_MARKERS.search(paragraph)
        ):
            continue
        amounts = list(AMOUNT_PATTERN.finditer(paragraph))
        obligated_value = _obligated_at_announcement(paragraph)
        announced_value = _money_value(amounts[0]) if amounts else None
        work_sentence = _sentence_containing(paragraph, "work will be performed")
        if work_sentence is None:
            work_sentence = _sentence_containing(paragraph, "work is being performed")
        completion_sentence = _sentence_containing(paragraph, "completed")
        if completion_sentence is None:
            completion_sentence = _sentence_containing(paragraph, "complete")
        competition_sentence = next(
            (
                sentence.strip()
                for sentence in re.split(r"(?<=[.!?])\s+", paragraph)
                if re.search(r"\b(?:competitive|sole.source|offers? received|bids? received)\b", sentence, re.IGNORECASE)
            ),
            None,
        )
        contracting_sentence = _sentence_containing(paragraph, "contracting activity")
        entry_index = len(records) + 1
        announcement_id = hashlib.sha256(
            f"{article['article_id']}|{current_service}|{entry_index}|{paragraph}".encode()
        ).hexdigest()[:24]
        records.append(
            {
                "announcement_id": announcement_id,
                "announcement_date": article["published_at"][:10],
                "service": current_service,
                "entry_index": entry_index,
                "entry_type": entry_type,
                "recipient_text": _recipient_text(paragraph),
                "primary_contract_id": contract_ids[0] if contract_ids else None,
                "contract_ids": contract_ids,
                "announced_value_usd": announced_value,
                "obligated_at_announcement_usd": obligated_value,
                "work_locations": work_sentence,
                "completion_text": completion_sentence,
                "competition_text": competition_sentence,
                "contracting_activity": contracting_sentence,
                "description": paragraph,
                "search_text": paragraph.upper(),
                "source_article_id": article["article_id"],
                "source_title": article["title"],
                "source_url": article["url"],
                "source_published_at": article["published_at"],
                "source_fetch_url": fetch_url,
                "source_fetch_method": fetch_method,
                "source_content_sha256": source_hash,
                "retrieved_at": retrieved_at.isoformat(),
                "raw_text": paragraph,
            }
        )
    return records


def _merge_records(output: Path, records: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    incoming = pd.DataFrame(list(records))
    if output.exists():
        existing = pd.read_parquet(output)
        incoming = pd.concat([existing, incoming], ignore_index=True)
    if incoming.empty:
        return incoming
    # The first observed copy is the immutable record. If Defense.gov changes an
    # entry, its content-derived ID changes and the corrected text is retained as
    # a separate record instead of rewriting the original observation.
    incoming = incoming.drop_duplicates(subset=["announcement_id"], keep="first")
    return incoming.sort_values(
        ["announcement_date", "source_article_id", "entry_index"],
        ascending=[False, False, True],
    )


def _upload(
    output_dir: Path,
    parquet_path: Path,
    manifest_path: Path,
    raw_paths: Iterable[Path],
    *,
    bucket: str,
    profile: str | None,
    release_id: str,
) -> None:
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client("s3", region_name="us-east-1")
    for raw_path in raw_paths:
        relative = raw_path.relative_to(output_dir)
        s3.upload_file(
            str(raw_path),
            bucket,
            f"bronze/dod/contract_announcements/{relative.as_posix()}",
        )
    for path in (parquet_path, manifest_path):
        stable_key = f"silver/dod/ref_contract_announcements/{path.name}"
        release_key = (
            "silver/dod/ref_contract_announcements/releases/"
            f"{release_id}/{path.name}"
        )
        s3.upload_file(str(path), bucket, release_key)
        s3.upload_file(str(path), bucket, stable_key)


def ingest(
    output_dir: Path,
    *,
    since_days: int = 45,
    max_articles: int = 100,
    allow_text_renderer: bool = True,
    bucket: str | None = None,
    profile: str | None = None,
    fail_on_fetch_error: bool = False,
) -> Dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    retrieved_at = datetime.now(timezone.utc)
    stamp = retrieved_at.strftime("%Y%m%dT%H%M%SZ")
    release_id = f"dod-contract-announcements-{stamp}"
    rss_body = _fetch(RSS_URL)
    rss_path = output_dir / "raw" / "rss" / f"contracts-{stamp}.xml"
    rss_path.parent.mkdir(parents=True, exist_ok=True)
    rss_path.write_bytes(rss_body)
    articles = discover_articles(
        rss_body,
        retrieved_at - timedelta(days=max(int(since_days), 1)),
    )[: max(int(max_articles), 1)]
    records: List[Dict[str, Any]] = []
    raw_paths = [rss_path]
    failures: List[Dict[str, str]] = []
    for article in articles:
        try:
            page_text, fetch_url, fetch_method = fetch_article_text(
                article["url"], allow_text_renderer=allow_text_renderer
            )
        except Exception as exc:
            failures.append(
                {
                    "article_id": article["article_id"],
                    "source_url": article["url"],
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )
            continue
        raw_path = output_dir / "raw" / "articles" / f"{article['article_id']}-{stamp}.md"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(page_text)
        raw_paths.append(raw_path)
        records.extend(
            parse_announcement(
                article,
                page_text,
                fetch_url=fetch_url,
                fetch_method=fetch_method,
                retrieved_at=retrieved_at,
            )
        )
    parquet_path = output_dir / "dod_contract_announcements.parquet"
    merged = _merge_records(parquet_path, records)
    if not merged.empty:
        merged.to_parquet(parquet_path, index=False, compression="zstd")
    manifest = {
        "release_id": release_id,
        "generated_at": retrieved_at.isoformat(),
        "official_feed_url": RSS_URL,
        "articles_discovered": len(articles),
        "articles_ingested": len(articles) - len(failures),
        "entries_parsed_this_run": len(records),
        "total_entries": len(merged),
        "failures": failures,
        "financial_treatment": (
            "Announcement values and amounts explicitly obligated at announcement remain "
            "separate enrichment fields. They are not added to USAspending obligations."
        ),
        "parquet_sha256": (
            hashlib.sha256(parquet_path.read_bytes()).hexdigest()
            if parquet_path.exists()
            else None
        ),
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    if failures and fail_on_fetch_error:
        raise RuntimeError(
            f"{len(failures)} of {len(articles)} DoD announcement pages failed to load"
        )
    if bucket and parquet_path.exists():
        _upload(
            output_dir,
            parquet_path,
            manifest_path,
            raw_paths,
            bucket=bucket,
            profile=profile,
            release_id=release_id,
        )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--since-days", type=int, default=45)
    parser.add_argument("--max-articles", type=int, default=100)
    parser.add_argument("--bucket")
    parser.add_argument("--profile")
    parser.add_argument("--direct-only", action="store_true")
    parser.add_argument("--fail-on-fetch-error", action="store_true")
    args = parser.parse_args()
    result = ingest(
        args.output_dir.resolve(),
        since_days=args.since_days,
        max_articles=args.max_articles,
        allow_text_renderer=not args.direct_only,
        bucket=args.bucket,
        profile=args.profile,
        fail_on_fetch_error=args.fail_on_fetch_error,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
