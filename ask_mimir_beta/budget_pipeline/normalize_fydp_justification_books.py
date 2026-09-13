"""Normalize FYDP resource summaries from DoD procurement justification books.

The parser intentionally reads only first-page Exhibit P-40 resource summaries. It
preserves every published period and measure, including explicit blanks and
"Continuing" values, so a missing projection is never converted to zero.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

CALCULATION_VERSION = "mimir-fydp-p40-normalizer-2026-09-v1"

PERIODS = [
    ("prior_years", None, "prior_years"),
    ("fy2025", 2025, "actual"),
    ("fy2026", 2026, "enacted"),
    ("fy2027_base", 2027, "base_request"),
    ("fy2027_ooc", 2027, "ooc_request"),
    ("fy2027_total", 2027, "total_request"),
    ("fy2028", 2028, "projected"),
    ("fy2029", 2029, "projected"),
    ("fy2030", 2030, "projected"),
    ("fy2031", 2031, "projected"),
    ("to_complete", None, "to_complete"),
    ("program_total", None, "program_total"),
]

SIMPLE_PERIODS = [
    ("prior_years", None, "prior_years"),
    ("fy2025", 2025, "actual"),
    ("fy2026", 2026, "enacted"),
    ("fy2027_total", 2027, "total_request"),
    ("fy2028", 2028, "projected"),
    ("fy2029", 2029, "projected"),
    ("fy2030", 2030, "projected"),
    ("fy2031", 2031, "projected"),
    ("to_complete", None, "to_complete"),
    ("program_total", None, "program_total"),
]

MEASURES = [
    ("procurement_quantity", "Procurement Quantity (Units in Each)", "quantity", 1.0),
    (
        "gross_weapon_system_cost",
        "Gross/Weapon System Cost ($ in Millions)",
        "amount_usd",
        1_000_000.0,
    ),
    (
        "less_prior_year_advance_procurement",
        "Less PY Advance Procurement ($ in Millions)",
        "amount_usd",
        1_000_000.0,
    ),
    (
        "net_procurement_p1",
        "Net Procurement (P-1) ($ in Millions)",
        "amount_usd",
        1_000_000.0,
    ),
    (
        "plus_current_year_advance_procurement",
        "Plus CY Advance Procurement ($ in Millions)",
        "amount_usd",
        1_000_000.0,
    ),
    (
        "total_obligation_authority",
        "Total Obligation Authority ($ in Millions)",
        "amount_usd",
        1_000_000.0,
    ),
]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", value).strip()
    return cleaned or None


def source_tokens(value: str) -> List[str]:
    normalized = (
        value.replace("\u00a0", " ")
        .replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u2212", "-")
    )
    return re.findall(
        r"Continuing|\*+\.\*+|\(?-?[\d,]+(?:\.\d+)?\)?|-",
        normalized,
    )


def parse_source_value(value: str) -> tuple[float | None, str]:
    clean = value.strip()
    if clean == "-":
        return None, "NOT_PUBLISHED"
    if clean.lower() == "continuing":
        return None, "CONTINUING"
    if "*" in clean:
        return None, "UNDISCLOSED"
    negative = clean.startswith("(") and clean.endswith(")")
    number = float(clean.strip("()").replace(",", ""))
    return (-number if negative else number), "PUBLISHED"


def extract_between(text: str, start: str, end: str) -> str | None:
    match = re.search(
        re.escape(start) + r"\s*(.*?)\s*" + re.escape(end),
        text,
        flags=re.DOTALL,
    )
    return clean_text(match.group(1)) if match else None


def parse_page(
    text: str,
    *,
    page_number: int,
    source: Dict[str, Any],
) -> Iterable[Dict[str, Any]]:
    if "Exhibit P-40" not in text or not re.search(
        r"Page\s+1\s+of\s+\d+\s+P-1\s+Line\s+#", text
    ):
        return []
    if "Resource Summary" not in text or "FY 2031" not in text:
        return []

    joined = clean_text(text) or ""
    line_match = re.search(r"P-1\s+Line\s+#\s*(\d+)", joined)
    item_value = None
    raw_lines = text.splitlines()
    for index, line in enumerate(raw_lines):
        header = "P-1 Line Item Number / Title:"
        if header not in line:
            continue
        column_start = line.index(header)
        column_rows = []
        for candidate in raw_lines[index + 1 : index + 5]:
            if "ID Code" in candidate:
                break
            if len(candidate) > column_start:
                value = candidate[column_start:].strip()
                if value:
                    column_rows.append(value)
        candidate_value = clean_text(" ".join(column_rows))
        if candidate_value and "/" in candidate_value:
            item_value = candidate_value
            break
    if item_value is None:
        item_match = re.search(
            r"P-1 Line Item Number / Title:\s*(.*?)\s*ID Code",
            joined,
            flags=re.DOTALL,
        )
        item_value = clean_text(item_match.group(1)) if item_match else None
    if not line_match or not item_value or "/" not in item_value:
        return []
    line_item, line_title = [part.strip() for part in item_value.split("/", 1)]

    appropriation_match = re.search(
        r"Appropriation / Budget Activity / Budget Sub Activity:\s*"
        r"([^:]+):\s*(.*?)\s*/\s*BA\s*([^:]+):\s*(.*?)\s*/\s*"
        r"BSA\s*([^:]+):\s*(.*?)\s*P-1 Line Item Number",
        joined,
        flags=re.DOTALL,
    )
    appropriation = {}
    if appropriation_match:
        appropriation = {
            "appropriation_code": clean_text(appropriation_match.group(1)),
            "appropriation_title": clean_text(appropriation_match.group(2)),
            "budget_activity_code": clean_text(appropriation_match.group(3)),
            "budget_activity_title": clean_text(appropriation_match.group(4)),
            "budget_subactivity_code": clean_text(appropriation_match.group(5)),
            "budget_subactivity_title": clean_text(appropriation_match.group(6)),
        }

    id_match = re.search(
        r"ID Code \(A=Service Ready, B=Not Service Ready\):\s*([AB])?\s*Program Elements",
        joined,
    )
    pe_match = re.search(r"Program Elements for Code B Items:\s*(.*?)\s*Other Related", joined)
    mdap_match = re.search(r"Line Item MDAP/MAIS Code:\s*(.*?)\s*Resource Summary", joined)
    description = extract_between(joined, "Description:", "Secondary Distribution")
    if description is None:
        description_match = re.search(r"Description:\s*(.*?)(?:Volume \d|UNCLASSIFIED$)", joined)
        description = clean_text(description_match.group(1)) if description_match else None

    page_lines = [(clean_text(line) or "").strip() for line in text.splitlines()]
    rows: List[Dict[str, Any]] = []
    for measure_name, source_label, value_kind, multiplier in MEASURES:
        row_line = next((line for line in page_lines if line.startswith(source_label)), None)
        if row_line is None:
            continue
        tokens = source_tokens(row_line[len(source_label) :])
        periods = SIMPLE_PERIODS if len(tokens) == len(SIMPLE_PERIODS) else PERIODS
        if len(tokens) != len(periods):
            raise ValueError(
                f"{source['source_id']} page {page_number} {measure_name}: "
                f"expected {len(PERIODS)} or {len(SIMPLE_PERIODS)} values, "
                f"found {len(tokens)}: {tokens}"
            )
        for (period_key, fiscal_year, funding_status), token in zip(periods, tokens):
            numeric_value, availability_status = parse_source_value(token)
            rows.append(
                {
                    "submission_fiscal_year": 2027,
                    "component": source.get("component"),
                    "appropriation_category": "procurement",
                    "p1_line_number": int(line_match.group(1)),
                    "budget_line_item": line_item,
                    "budget_line_item_title": line_title,
                    "is_advance_procurement_exhibit": "Advance Procurement Budget Line Item" in joined,
                    "id_code": clean_text(id_match.group(1)) if id_match else None,
                    "program_elements": clean_text(pe_match.group(1)) if pe_match else None,
                    "mdap_mais_code": clean_text(mdap_match.group(1)) if mdap_match else None,
                    **appropriation,
                    "measure_type": measure_name,
                    "value_kind": value_kind,
                    "period_key": period_key,
                    "fiscal_year": fiscal_year,
                    "funding_status": funding_status,
                    "source_value": token,
                    "availability_status": availability_status,
                    "value_source_units": numeric_value,
                    "amount_usd": (
                        numeric_value * multiplier
                        if numeric_value is not None and value_kind == "amount_usd"
                        else None
                    ),
                    "quantity": (
                        numeric_value
                        if numeric_value is not None and value_kind == "quantity"
                        else None
                    ),
                    "description_excerpt": description[:2000] if description else None,
                    "source_id": source.get("source_id"),
                    "source_document_title": source.get("document_title"),
                    "source_page_number": page_number,
                    "source_landing_page": source.get("landing_page"),
                    "source_download_url": source.get("download_url"),
                    "source_s3_uri": source.get("s3_uri"),
                    "source_sha256": source.get("sha256"),
                    "source_locator": f"P-1 line {line_match.group(1)}, PDF page {page_number}",
                    "extraction_method": "deterministic_p40_resource_summary_parser",
                    "calculation_version": CALCULATION_VERSION,
                }
            )
    return rows


def extract_pdf_pages(path: Path) -> tuple[List[str], str]:
    pdftotext = os.getenv("MIMIR_PDFTOTEXT_BIN") or shutil.which("pdftotext")
    if pdftotext:
        result = subprocess.run(
            [pdftotext, "-layout", str(path), "-"],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.split("\f"), "pdftotext_layout"
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise RuntimeError(
            "PDF normalization requires pdftotext or the optional pypdf dependency."
        ) from exc
    reader = PdfReader(str(path))
    return [page.extract_text() or "" for page in reader.pages], "pypdf"


def normalize(input_dir: Path, manifest_path: Path) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    manifest = json.loads(manifest_path.read_text())
    records: List[Dict[str, Any]] = []
    source_results: List[Dict[str, Any]] = []
    for source in manifest["sources"]:
        path = input_dir / source["local_filename"]
        if not path.exists():
            raise FileNotFoundError(path)
        actual_hash = sha256_file(path)
        expected_hash = source.get("sha256")
        if expected_hash and expected_hash != actual_hash:
            raise ValueError(f"source hash mismatch for {path.name}")
        source = {**source, "sha256": actual_hash}
        pages, extraction_engine = extract_pdf_pages(path)
        source_rows = 0
        source_pages = 0
        for page_number, page_text in enumerate(pages, start=1):
            page_rows = list(parse_page(page_text, page_number=page_number, source=source))
            if page_rows:
                source_pages += 1
                source_rows += len(page_rows)
                records.extend(page_rows)
        source_results.append(
            {
                "source_id": source["source_id"],
                "source_file": path.name,
                "sha256": actual_hash,
                "pdf_pages": len(pages),
                "normalized_p40_pages": source_pages,
                "normalized_fact_rows": source_rows,
                "extraction_engine": extraction_engine,
            }
        )
    return records, source_results


def main() -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    records, sources = normalize(args.input_dir, args.manifest)
    output_path = args.output_dir / "dod_fydp_budget_facts.parquet"
    pq.write_table(pa.Table.from_pylist(records), output_path, compression="zstd")

    outyear_rows = [
        row
        for row in records
        if row["fiscal_year"] in {2028, 2029, 2030, 2031}
        and row["availability_status"] == "PUBLISHED"
    ]
    summary = {
        "dataset_id": "dod_pb_fy2027_p40_fydp_resource_summaries",
        "calculation_version": CALCULATION_VERSION,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "record_count": len(records),
        "program_exhibit_count": len(
            {(row["source_id"], row["p1_line_number"]) for row in records}
        ),
        "published_outyear_fact_count": len(outyear_rows),
        "outyear_fiscal_years": sorted(
            {row["fiscal_year"] for row in outyear_rows if row["fiscal_year"]}
        ),
        "output": output_path.name,
        "output_sha256": sha256_file(output_path),
        "sources": sources,
        "methodology": [
            "Reads first-page Exhibit P-40 resource summaries only.",
            "Retains FY2027 base, OOC and total request as separate facts.",
            "Retains zero, not-published and Continuing values as distinct states.",
            "Treats FY2028-FY2031 values as public planning projections, not enacted appropriations.",
            "Does not combine net procurement, total obligation authority or advance-procurement rows.",
        ],
    }
    (args.output_dir / "manifest.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
