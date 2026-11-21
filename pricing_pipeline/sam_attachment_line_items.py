"""Deterministic SAM attachment text and CLIN-line extraction helpers.

This module deliberately does not require a platform or system match.  Its grain is
one source row in one immutable attachment.  A row may carry quantity-only evidence,
a source-stated unit price, or a unit price derived from an extended line amount.
Nothing is promoted to a customer unit price until notice and document scope have
also been validated.
"""

from __future__ import annotations

import csv
import hashlib
import io
import os
import re
import shutil
import subprocess
import tempfile
import zipfile
from dataclasses import asdict, dataclass, replace
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, List, Optional


def non_procurement_document_class(attachment_sha256, attachment_name, source_row_text=""):
    """Document meaning takes precedence over valid numeric row arithmetic.

    The exact reviewed hash remains excluded if renamed. Filename matches flag
    similar property documents for review, without claiming a verified value
    basis for a different source. A property clause in an executed line alone
    is not enough to reject a genuine purchase.
    """
    import re

    reviewed = {
        "84075394258b3f88d553c39b5950b58e045113c016aaaf4f270b5005f268a967":
            "GOVERNMENT_FURNISHED_PROPERTY_INVENTORY",
        "aa8abebbd6e303032eb32a1a63568275147577139783e2b2558cb212c7454169":
            "PREAWARD_PLANNING_ESTIMATE_REFERENCE",
        "6f2e0fc4bb73993ba7c0f0cd226a38bd85bc9dca33a67c55efe27de1999b14bf":
            "VALUE_BASIS_UNESTABLISHED_REFERENCE",
    }
    if attachment_sha256 in reviewed:
        return reviewed[attachment_sha256]
    name = re.sub(r"[^A-Z0-9]+", " ", str(attachment_name or "").upper()).strip()
    row = re.sub(r"[^A-Z0-9]+", " ", str(source_row_text or "").upper()).strip()
    if re.search(r"\bGOVERNMENT FURNISHED PROPERTY\b|\bGFP\b", name):
        return "NON_PROCUREMENT_DOCUMENT_REQUIRES_REVIEW"
    if re.search(r"\b(INVENTORY|ASSET REGISTER|PROPERTY REGISTER)\b", name) and re.search(
        r"\b(IN STOCK|IN REPAIR|UNIT COST|EXTENDED COST|SERIAL NBR|BATCH VALUATION)\b", row
    ):
        return "NON_PROCUREMENT_DOCUMENT_REQUIRES_REVIEW"
    if re.search(r"\b(PRE ?SOLICITATION|PLANNING ESTIMATE|IGCE)\b", name):
        return "PREAWARD_DOCUMENT_VALUE_REQUIRES_REVIEW"
    return None

def document_value_basis(document_class):
    """Economic meaning is separate from the unchanged extraction price_basis."""
    if document_class == "GOVERNMENT_FURNISHED_PROPERTY_INVENTORY":
        return "GOVERNMENT_PROPERTY_ACCOUNTING_UNIT_COST"
    if document_class == "PREAWARD_PLANNING_ESTIMATE_REFERENCE":
        return "SOURCE_PLANNING_ESTIMATE_UNIT_PRICE"
    if document_class:
        return "SOURCE_VALUE_BASIS_UNESTABLISHED"
    return "OBSERVED_SOURCE_LINE_PRICE_UNREVIEWED"

def retain_document_clin(document_class):
    """An estimate can have real schedule CLINs; inventory item codes cannot."""
    return document_class in (None, "PREAWARD_PLANNING_ESTIMATE_REFERENCE", "PREAWARD_DOCUMENT_VALUE_REQUIRES_REVIEW")

def document_quality_status(document_class):
    if document_class in ("PREAWARD_PLANNING_ESTIMATE_REFERENCE", "VALUE_BASIS_UNESTABLISHED_REFERENCE", "PREAWARD_DOCUMENT_VALUE_REQUIRES_REVIEW"):
        return "REFERENCE_ONLY_SOURCE_VALUE"
    if document_class:
        return "EXCLUDED_NON_PROCUREMENT_DOCUMENT"
    return "AUTO_ADMITTED_BY_COHORT_RULE"


PARSER_VERSION = "mimir-sam-attachment-clin-2026-11-v9-source-value-basis"
# The accepted-items tables on pages 3-8 of this immutable award split item
# descriptions and numeric columns into separate PDF text lines. Its five-cell
# table extraction was audited in full: 47 physical Each rows reconcile exactly;
# Job/service and credit rows are excluded. Do not generalize this document-
# specific layout to other award or solicitation attachments.
AUDITED_ADEC_AWARD_SHA256 = (
    "5f35757152d12af6734cd318c35ece9cb211bdf40017e8b20ee55f0dbb8a8891"
)
AUDITED_ADEC_TABLE_PAGES = {3, 4, 5, 6, 7, 8}
AUDITED_ADEC_ITEM_RE = re.compile(r"^\d[\dA-Z.]*\s*-\s*\S", re.IGNORECASE)
AUDITED_ADEC_MONEY_RE = re.compile(r"^\$([0-9][0-9,]*\.[0-9]{2})$")
SUPPORTED_DOCUMENT_SUFFIXES = {
    ".txt", ".csv", ".tsv", ".html", ".htm", ".xml", ".pdf", ".xlsx",
    ".xlsm", ".docx", ".zip",
}
MAX_ZIP_MEMBERS = 250
MAX_ZIP_MEMBER_BYTES = 100_000_000
MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES = 500_000_000
MAX_ZIP_DEPTH = 2
QUANTITY_UNITS = {
    "EA", "EACH", "UNIT", "UNITS", "UN", "SET", "SETS", "KIT", "KITS",
    "LO", "LOT", "LOTS", "PK", "PACK", "BOX", "PAIR", "PR", "JB", "JOB", "SV",
    "SERVICE", "LS", "LUMP", "MO", "MONTH",
    "MONTHS", "YR", "YEAR", "YEARS", "HR", "HOUR", "HOURS", "DAY", "DAYS",
    "GAL", "GALS", "GALLON", "GALLONS",
}
CLIN_RE = re.compile(
    r"^\s*(?:CLIN\s*[:#-]?\s*)?"
    r"(?P<clin>(?:\d{4}[A-Z0-9]{0,4}|[A-Z]{1,3}\d{3,7}[A-Z0-9-]*))\b",
    re.IGNORECASE,
)
LABELED_QUANTITY_RE = re.compile(
    r"\b(?:QTY|QUANTITY)\s*[:#=-]?\s*(?P<quantity>[\d,]+(?:\.\d+)?)"
    r"(?:\s*(?P<unit>[A-Z]{1,12}))?\b",
    re.IGNORECASE,
)
MULTIPLICATION_QUANTITY_RE = re.compile(
    r"(?:\bx|\u00d7)\s*(?P<quantity>[\d,]+(?:\.\d+)?)\s*=",
    re.IGNORECASE,
)
LEADING_QUANTITY_ITEM_RE = re.compile(
    r"^\s*(?P<quantity>[\d,]+(?:\.\d+)?)\s*"
    r"(?P<unit>EA|EACH|UNIT|UNITS|SET|SETS|KIT|KITS|LOT|LOTS|PK|PACK|BOX|PAIR|PR)\b"
    r"\s*(?:[-\u2013\u2014\u0016:]+\s*)?(?P<description>.+?)\s*$",
    re.IGNORECASE,
)
MONEY_RE = re.compile(
    r"(?<![A-Z0-9])(?:USD\s*)?\$\s*(?P<dollar>[\d,]+(?:\.\d{1,6})?)\s*(?P<magnitude>[KMB])?"
    r"|(?<![A-Z0-9$])(?P<plain>[\d]{1,3}(?:,[\d]{3})+(?:\.\d{1,6})?|\d+\.\d{2,6})(?![A-Z0-9])",
    re.IGNORECASE,
)
MULTILINE_CLIN_RE = re.compile(
    r"\bCLIN[ \t]*\n[ \t]*Number[ \t]*\n[ \t]*"
    r"(?P<clin>\d{4}[A-Z0-9]{0,4})[ \t]*\n(?:[ \t]*\n)*"
    r"[ \t]*(?P<quantity>[\d,]+(?:\.\d+)?)\s+"
    r"(?P<unit>EA|EACH|UNIT|UNITS|SET|SETS|KIT|KITS|LO|LOT|LOTS|PK|PACK|BOX|PAIR|PR|JB|JOB|SV|SERVICE|LS|LUMP)[ \t]*\n"
    r"\s*(?P<description>[^\n]+)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class DocumentChunk:
    source_locator: str
    text: str


@dataclass(frozen=True)
class SamAttachmentLineCandidate:
    line_candidate_id: str
    notice_id: str
    notice_version_id: Optional[str]
    solicitation_id: Optional[str]
    award_number: Optional[str]
    notice_type: Optional[str]
    attachment_id: str
    resource_id: str
    attachment_sha256: str
    attachment_s3_uri: str
    attachment_name: str
    source_locator: str
    source_row_text: str
    source_row_sha256: str
    clin: Optional[str]
    item_description: str
    quantity: Optional[Decimal]
    quantity_unit: Optional[str]
    source_unit_price_usd: Optional[Decimal]
    extended_line_amount_usd: Optional[Decimal]
    derived_unit_price_usd: Optional[Decimal]
    unit_price_method: Optional[str]
    arithmetic_validation_status: str
    evidence_type: str
    promotion_status: str
    parser_version: str
    document_role: str = "UNVERIFIED_ATTACHMENT_ROW"
    source_value_basis: str = "OBSERVED_SOURCE_LINE_PRICE_UNREVIEWED"

    def to_dict(self) -> dict:
        result = asdict(self)
        for key in (
            "quantity", "source_unit_price_usd", "extended_line_amount_usd",
            "derived_unit_price_usd",
        ):
            if result[key] is not None:
                result[key] = str(result[key])
        return result


def _decimal(value: str) -> Optional[Decimal]:
    try:
        return Decimal(value.replace(",", "").strip())
    except (InvalidOperation, AttributeError):
        return None


def _normalise_row(value: str) -> str:
    value = (
        value.replace("\u00a0", " ")
        .replace("\x00", " ")
        .replace("\x96", "-")
    )
    return re.sub(r"[ \t]+", " ", value).strip()


def _flatten_table_cell(value: object) -> str:
    """Keep one source table row on one parser row, even when cells wrap."""
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def _money_values(row: str) -> list[Decimal]:
    values: list[Decimal] = []
    for match in MONEY_RE.finditer(row):
        explicit_currency = match.group("dollar") is not None
        following = row[match.end():match.end() + 16]
        preceding = row[max(0, match.start() - 16):match.start()]
        # FAR/DFARS citations such as ``252.211-7003`` resemble unmarked
        # decimal currency.  They are regulatory identifiers, never prices.
        if not explicit_currency and (
            re.match(r"\s*-\s*\d", following)
            or re.search(r"\b(?:DFARS|FAR|CFR|PGI)\s*$", preceding, re.IGNORECASE)
        ):
            continue
        # Unmarked decimals and comma-separated integers also occur in model
        # numbers, dimensions, pitches, quantities and regulatory text. Retain
        # them as money only when the source labels the immediately preceding
        # field as an amount. Currency-marked values do not need this heuristic.
        if not explicit_currency and not re.search(
            r"(?:UNIT\s+PRICE|EXTENDED\s+(?:PRICE|AMOUNT)|TOTAL\s+PRICE|AMOUNT|COST)"
            r"\s*[:=]?\s*$",
            preceding,
            re.IGNORECASE,
        ):
            continue
        if not explicit_currency and (
            re.match(r"\s*(?:" + "|".join(sorted(QUANTITY_UNITS, key=len, reverse=True)) + r")\b", following, re.IGNORECASE)
            or re.search(r"(?:QTY|QUANTITY)\s*[:#=-]?\s*$", preceding, re.IGNORECASE)
        ):
            continue
        parsed = _decimal(match.group("dollar") or match.group("plain"))
        if parsed is not None and parsed >= 0:
            magnitude = (match.group("magnitude") or "").upper()
            parsed *= {"": Decimal("1"), "K": Decimal("1000"), "M": Decimal("1000000"), "B": Decimal("1000000000")}[magnitude]
            values.append(parsed)
    return values


def _quantity(row: str, clin_end: int) -> tuple[Optional[Decimal], Optional[str]]:
    labeled = LABELED_QUANTITY_RE.search(row)
    if labeled:
        quantity = _decimal(labeled.group("quantity"))
        unit = (labeled.group("unit") or "").upper() or None
        return quantity, unit

    multiplied = MULTIPLICATION_QUANTITY_RE.search(row)
    if multiplied:
        return _decimal(multiplied.group("quantity")), None

    # Common schedule layout: CLIN | description | 12 | EA | $unit | $amount.
    tokens = re.split(r"\s+", row[clin_end:].strip())
    candidates: list[tuple[int, Decimal, str]] = []
    for index in range(1, len(tokens)):
        unit = re.sub(r"[^A-Z]", "", tokens[index].upper())
        if unit not in QUANTITY_UNITS:
            continue
        number = re.sub(r"[^0-9.,]", "", tokens[index - 1])
        quantity = _decimal(number)
        if quantity is not None and quantity > 0:
            candidates.append((index, quantity, unit))
    if candidates:
        # A description can itself contain a duration (for example, "3 year
        # OEM warranty") before the schedule's actual Quantity/Unit columns.
        # In flattened table rows the structural quantity is the rightmost
        # number/unit pair, so selecting the first pair turns durations into
        # false equipment quantities.
        _, quantity, unit = candidates[-1]
        return quantity, unit
    return None, None


def _description(row: str, clin_match: re.Match[str]) -> str:
    value = row[clin_match.end():]
    value = LABELED_QUANTITY_RE.sub(" ", value)
    value = MONEY_RE.sub(" ", value)
    value = re.sub(
        r"\s+[\d,]+(?:\.\d+)?\s+(?:"
        + "|".join(sorted(QUANTITY_UNITS, key=len, reverse=True))
        + r")\s*$",
        " ",
        value,
        flags=re.IGNORECASE,
    )
    value = re.sub(r"\s+", " ", value).strip(" |-:\t")
    return value or row


def _candidate_rows(chunk: DocumentChunk) -> Iterable[tuple[str, str]]:
    """Yield physical rows plus schedule entries split across PDF text lines."""
    requirement_section: Optional[str] = None
    for row_number, raw_row in enumerate(chunk.text.splitlines(), 1):
        heading = _normalise_row(raw_row).upper().rstrip(":")
        if heading == "FUNDED":
            requirement_section = "FUNDED"
        elif heading in {"UNFUNDED", "OPTION", "OPTIONAL"}:
            requirement_section = "UNFUNDED_OR_OPTION"
        locator = f"row={row_number}"
        if requirement_section:
            locator += f";section={requirement_section}"
        yield locator, raw_row

    # Standard Forms frequently place CLIN, quantity, unit and description on
    # separate PDF text lines.  Treat the matched block as one source-scoped
    # row; otherwise a valid schedule such as ``0001 / 2 Each / Base MATLAB``
    # is invisible to a line-oriented parser.
    for block_number, match in enumerate(MULTILINE_CLIN_RE.finditer(chunk.text), 1):
        row = " ".join(
            (
                match.group("clin"),
                match.group("description").strip(),
                match.group("quantity"),
                match.group("unit"),
            )
        )
        yield f"block={block_number}", row


def parse_line_candidates(
    chunks: Iterable[DocumentChunk],
    *,
    notice_id: str,
    notice_version_id: Optional[str],
    solicitation_id: Optional[str],
    award_number: Optional[str],
    notice_type: Optional[str],
    attachment_id: str,
    resource_id: str,
    attachment_sha256: str,
    attachment_s3_uri: str,
    attachment_name: str,
) -> List[SamAttachmentLineCandidate]:
    """Extract traceable CLIN rows without allocating notice or award totals."""
    results: list[SamAttachmentLineCandidate] = []
    seen: set[str] = set()
    for chunk in chunks:
        chunk_header = _normalise_row(chunk.text[:500]).upper()
        if "DELIVERY SCHEDULE" in chunk_header and "ADDRESS AND POC" in chunk_header:
            # Delivery tables repeat procurement CLINs and quantities. Keeping
            # both inflates evidence counts and can make one procurement look
            # like several independent quantity observations.
            continue
        table_page = re.fullmatch(r"page=(\d+);table=\d+", chunk.source_locator)
        if (
            attachment_sha256 == AUDITED_ADEC_AWARD_SHA256
            and notice_type == "Award Notice"
            and table_page is not None
            and int(table_page.group(1)) in AUDITED_ADEC_TABLE_PAGES
        ):
            for row_number, raw_row in enumerate(chunk.text.splitlines(), 1):
                cells = [_normalise_row(cell) for cell in raw_row.split("\t")]
                if len(cells) != 5 or not AUDITED_ADEC_ITEM_RE.match(cells[0]):
                    continue
                if cells[0] == "5744 - Oregon 5744 ASST Cart":
                    # This row was already captured and governed from PDF text
                    # at page=8#row=11 in v6; keep one physical observation.
                    continue
                if cells[2].upper() != "EACH" or not re.fullmatch(r"[1-9][0-9]*", cells[1]):
                    continue
                unit_match = AUDITED_ADEC_MONEY_RE.fullmatch(cells[3])
                amount_match = AUDITED_ADEC_MONEY_RE.fullmatch(cells[4])
                if unit_match is None or amount_match is None:
                    continue
                quantity = Decimal(cells[1])
                unit_price = _decimal(unit_match.group(1))
                line_amount = _decimal(amount_match.group(1))
                if (
                    unit_price is None or line_amount is None
                    or unit_price <= 0 or quantity * unit_price != line_amount
                ):
                    continue
                row = "\t".join(cells)
                source_locator = f"{chunk.source_locator}#row={row_number}"
                row_sha = hashlib.sha256(row.encode("utf-8")).hexdigest()
                identity = "\x1f".join((notice_id, attachment_sha256, source_locator, "", row_sha))
                line_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()
                if line_id in seen:
                    continue
                seen.add(line_id)
                results.append(SamAttachmentLineCandidate(
                    line_candidate_id=line_id,
                    notice_id=notice_id,
                    notice_version_id=notice_version_id,
                    solicitation_id=solicitation_id,
                    award_number=award_number,
                    notice_type=notice_type,
                    attachment_id=attachment_id,
                    resource_id=resource_id,
                    attachment_sha256=attachment_sha256,
                    attachment_s3_uri=attachment_s3_uri,
                    attachment_name=attachment_name,
                    source_locator=source_locator,
                    source_row_text=row,
                    source_row_sha256=row_sha,
                    clin=None,
                    item_description=cells[0],
                    quantity=quantity,
                    quantity_unit="EACH",
                    source_unit_price_usd=unit_price,
                    extended_line_amount_usd=line_amount,
                    derived_unit_price_usd=None,
                    unit_price_method="SOURCE_STATED_UNIT_PRICE",
                    arithmetic_validation_status="VALIDATED_WITHIN_TOLERANCE",
                    evidence_type="SOURCE_UNIT_PRICE",
                    promotion_status="PENDING_NOTICE_AND_DOCUMENT_SCOPE_VALIDATION",
                    parser_version=PARSER_VERSION,
                ))
            # Preserve the five physical table cells as one source line. The
            # generic text parser can misread the leading item code as a CLIN.
            continue
        for row_locator, raw_row in _candidate_rows(chunk):
            row = _normalise_row(raw_row)
            clin_match = CLIN_RE.match(row)
            leading_item_match = LEADING_QUANTITY_ITEM_RE.match(row)
            if (not clin_match and not leading_item_match) or len(row) < 6:
                continue
            clin = clin_match.group("clin").upper() if clin_match else None
            if leading_item_match and not clin_match:
                quantity = _decimal(leading_item_match.group("quantity"))
                quantity_unit = leading_item_match.group("unit").upper()
                item_description = leading_item_match.group("description").strip()
            else:
                quantity, quantity_unit = _quantity(row, clin_match.end())
                item_description = _description(row, clin_match)
            money = _money_values(row)
            if (
                quantity is None
                and "$" not in row
                and not re.search(r"\b(?:PRICE|AMOUNT|COST|TOTAL)\b", row, re.IGNORECASE)
            ):
                # An unlabeled decimal in a schedule is often a quantity or
                # duration. Do not manufacture amount-only evidence from it.
                money = []

            source_unit_price: Optional[Decimal] = None
            extended_amount: Optional[Decimal] = None
            derived_unit_price: Optional[Decimal] = None
            unit_price_method: Optional[str] = None
            arithmetic_status = "NOT_TESTED"
            ambiguous_multiple_amounts = False

            if len(money) >= 2:
                if quantity and quantity > 0:
                    source_unit_price, extended_amount = money[-2], money[-1]
                    unit_price_method = "SOURCE_STATED_UNIT_PRICE"
                    expected = source_unit_price * quantity
                    tolerance = max(Decimal("0.02"), abs(extended_amount) * Decimal("0.005"))
                    arithmetic_status = (
                        "VALIDATED_WITHIN_TOLERANCE"
                        if abs(expected - extended_amount) <= tolerance
                        else "SOURCE_VALUES_DO_NOT_RECONCILE"
                    )
                elif re.search(r"\b(?:UNIT\s+PRICE|PRICE\s+PER)\b", row, re.IGNORECASE):
                    source_unit_price = money[0]
                    unit_price_method = "SOURCE_STATED_UNIT_PRICE_WITHOUT_QUANTITY"
                else:
                    ambiguous_multiple_amounts = True
            elif len(money) == 1:
                extended_amount = money[0]
                if quantity and quantity > 0:
                    derived_unit_price = (extended_amount / quantity).quantize(Decimal("0.000001"))
                    unit_price_method = "DERIVED_FROM_EXTENDED_LINE_AMOUNT"
                    arithmetic_status = "EXACT_ARITHMETIC_DERIVATION"

            if ambiguous_multiple_amounts:
                evidence_type = "AMBIGUOUS_MULTIPLE_AMOUNTS_NO_QUANTITY"
            elif source_unit_price is not None:
                evidence_type = "SOURCE_UNIT_PRICE"
            elif derived_unit_price is not None:
                evidence_type = "DERIVED_UNIT_PRICE_CANDIDATE"
            elif quantity is not None:
                evidence_type = "QUANTITY_ONLY"
            elif extended_amount is not None:
                evidence_type = "LINE_AMOUNT_ONLY"
            else:
                continue

            source_locator = f"{chunk.source_locator}#{row_locator}"
            row_sha = hashlib.sha256(row.encode("utf-8")).hexdigest()
            identity = "\x1f".join(
                (notice_id, attachment_sha256, source_locator, clin or "", row_sha)
            )
            line_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()
            if line_id in seen:
                continue
            seen.add(line_id)
            excluded_document = non_procurement_document_class(
                attachment_sha256, attachment_name, row,
            )
            results.append(
                SamAttachmentLineCandidate(
                    line_candidate_id=line_id,
                    notice_id=notice_id,
                    notice_version_id=notice_version_id,
                    solicitation_id=solicitation_id,
                    award_number=award_number,
                    notice_type=notice_type,
                    attachment_id=attachment_id,
                    resource_id=resource_id,
                    attachment_sha256=attachment_sha256,
                    attachment_s3_uri=attachment_s3_uri,
                    attachment_name=attachment_name,
                    source_locator=source_locator,
                    source_row_text=row,
                    source_row_sha256=row_sha,
                    clin=clin if retain_document_clin(excluded_document) else None,
                    item_description=item_description,
                    quantity=quantity,
                    quantity_unit=quantity_unit,
                    source_unit_price_usd=source_unit_price,
                    extended_line_amount_usd=extended_amount,
                    derived_unit_price_usd=derived_unit_price,
                    unit_price_method=unit_price_method,
                    arithmetic_validation_status=arithmetic_status,
                    evidence_type="NON_PROCUREMENT_DOCUMENT_REFERENCE" if excluded_document else evidence_type,
                    promotion_status="EXCLUDED_NON_PROCUREMENT_DOCUMENT" if excluded_document else "PENDING_NOTICE_AND_DOCUMENT_SCOPE_VALIDATION",
                    parser_version=PARSER_VERSION,
                    document_role=excluded_document or "UNVERIFIED_ATTACHMENT_ROW",
                )
            )
    reviewed_results = []
    for candidate in results:
        excluded_document = non_procurement_document_class(
            candidate.attachment_sha256, candidate.attachment_name, candidate.source_row_text,
        )
        if excluded_document:
            candidate = replace(
                candidate, clin=candidate.clin if retain_document_clin(excluded_document) else None,
                evidence_type=("SOURCE_PLANNING_ESTIMATE_REFERENCE" if excluded_document == "PREAWARD_PLANNING_ESTIMATE_REFERENCE"
                               else "SOURCE_VALUE_REQUIRES_REVIEW" if excluded_document == "VALUE_BASIS_UNESTABLISHED_REFERENCE"
                               else "NON_PROCUREMENT_DOCUMENT_REFERENCE"),
                promotion_status=document_quality_status(excluded_document),
                document_role=excluded_document,
                source_value_basis=document_value_basis(excluded_document),
            )
        reviewed_results.append(candidate)
    return reviewed_results


def extract_document_chunks(
    content: bytes,
    filename: str,
    *,
    extract_pdf_tables: bool = True,
    _archive_depth: int = 0,
) -> List[DocumentChunk]:
    """Extract text/table rows while retaining page, sheet, or file locators."""
    suffix = Path(filename).suffix.lower()
    if suffix == ".zip":
        if _archive_depth >= MAX_ZIP_DEPTH:
            raise ValueError(f"ZIP nesting exceeds {MAX_ZIP_DEPTH} levels")
        chunks: list[DocumentChunk] = []
        total_uncompressed = 0
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            members = [member for member in archive.infolist() if not member.is_dir()]
            if len(members) > MAX_ZIP_MEMBERS:
                raise ValueError(
                    f"ZIP contains too many members: {len(members)} > {MAX_ZIP_MEMBERS}"
                )
            for member in members:
                member_suffix = Path(member.filename).suffix.lower()
                if member_suffix not in SUPPORTED_DOCUMENT_SUFFIXES:
                    continue
                if member.flag_bits & 0x1:
                    continue
                if member.file_size > MAX_ZIP_MEMBER_BYTES:
                    continue
                total_uncompressed += member.file_size
                if total_uncompressed > MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES:
                    raise ValueError(
                        "ZIP uncompressed content exceeds safety cap: "
                        f"{total_uncompressed} > {MAX_ZIP_TOTAL_UNCOMPRESSED_BYTES}"
                    )
                member_content = archive.read(member)
                member_sha256 = hashlib.sha256(member_content).hexdigest()
                member_chunks = extract_document_chunks(
                    member_content,
                    member.filename,
                    extract_pdf_tables=extract_pdf_tables,
                    _archive_depth=_archive_depth + 1,
                )
                member_name = re.sub(r"[\r\n]+", " ", member.filename).strip()
                for chunk in member_chunks:
                    chunks.append(
                        DocumentChunk(
                            source_locator=(
                                f"zip_member={member_name};member_sha256={member_sha256}"
                                f"#{chunk.source_locator}"
                            ),
                            text=chunk.text,
                        )
                    )
        if not chunks:
            raise ValueError("ZIP contains no supported, readable document members")
        return chunks

    if suffix in {".txt", ".csv", ".tsv", ".html", ".htm", ".xml"}:
        decoded = content.decode("utf-8", errors="replace")
        if suffix in {".csv", ".tsv"}:
            delimiter = "\t" if suffix == ".tsv" else ","
            rows = csv.reader(io.StringIO(decoded), delimiter=delimiter)
            decoded = "\n".join("\t".join(cell.strip() for cell in row) for row in rows)
        return [DocumentChunk(source_locator="file", text=decoded)]

    if suffix == ".pdf":
        chunks: list[DocumentChunk] = []
        page_texts: list[str] = []
        pdftotext = os.getenv("MIMIR_PDFTOTEXT_BIN") or shutil.which("pdftotext")
        if pdftotext:
            temporary_path = None
            try:
                with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temporary:
                    temporary.write(content)
                    temporary_path = temporary.name
                result = subprocess.run(
                    [pdftotext, "-layout", temporary_path, "-"],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                page_texts = result.stdout.split("\f")
                chunks = [
                    DocumentChunk(source_locator=f"page={page_number}", text=text)
                    for page_number, text in enumerate(page_texts, 1)
                    if text.strip()
                ]
            finally:
                if temporary_path:
                    Path(temporary_path).unlink(missing_ok=True)
        try:
            from pypdf import PdfReader
        except ImportError as exc:  # pragma: no cover - deployment dependency
            raise RuntimeError("pypdf is required to extract PDF attachments") from exc
        reader = PdfReader(io.BytesIO(content))
        table_candidate_pages: list[int] = []
        table_signal = re.compile(
            r"\b(CLIN|CONTRACT LINE|QTY|QUANTITY|UNIT PRICE|EXTENDED (?:PRICE|AMOUNT)|TOTAL PRICE)\b",
            re.IGNORECASE,
        )
        for page_number, page in enumerate(reader.pages, 1):
            if page_number <= len(page_texts):
                text = page_texts[page_number - 1]
            else:
                text = page.extract_text() or ""
                chunks.append(DocumentChunk(source_locator=f"page={page_number}", text=text))
            if table_signal.search(text):
                table_candidate_pages.append(page_number)

        # Continuation pages of this audited award omit the column header, so
        # the generic page-level signal misses their otherwise complete tables.
        if hashlib.sha256(content).hexdigest() == AUDITED_ADEC_AWARD_SHA256:
            table_candidate_pages = sorted(
                set(table_candidate_pages) | AUDITED_ADEC_TABLE_PAGES
            )

        # Table detection is materially slower than text extraction. Restrict it
        # to pages already carrying line-item signals, while retaining every page
        # as text evidence above.
        if extract_pdf_tables and table_candidate_pages:
            try:
                import pdfplumber

                with pdfplumber.open(io.BytesIO(content)) as document:
                    for page_number in table_candidate_pages:
                        page = document.pages[page_number - 1]
                        for table_number, table in enumerate(page.extract_tables() or [], 1):
                            rows = ["\t".join(_flatten_table_cell(cell) for cell in row) for row in table]
                            chunks.append(
                                DocumentChunk(
                                    source_locator=f"page={page_number};table={table_number}",
                                    text="\n".join(rows),
                                )
                            )
            except ImportError:
                pass
        return chunks

    if suffix in {".xlsx", ".xlsm"}:
        try:
            from openpyxl import load_workbook
        except ImportError as exc:  # pragma: no cover - deployment dependency
            raise RuntimeError("openpyxl is required to extract Excel attachments") from exc
        workbook = load_workbook(io.BytesIO(content), read_only=True, data_only=True)
        chunks = []
        for worksheet in workbook.worksheets:
            rows = []
            for row in worksheet.iter_rows(values_only=True):
                rows.append("\t".join("" if value is None else str(value) for value in row))
            chunks.append(DocumentChunk(source_locator=f"sheet={worksheet.title}", text="\n".join(rows)))
        return chunks

    if suffix == ".docx":
        try:
            from docx import Document
        except ImportError as exc:  # pragma: no cover - deployment dependency
            raise RuntimeError("python-docx is required to extract Word attachments") from exc
        document = Document(io.BytesIO(content))
        rows = [paragraph.text for paragraph in document.paragraphs]
        for table in document.tables:
            rows.extend(
                "\t".join(_flatten_table_cell(cell.text) for cell in row.cells)
                for row in table.rows
            )
        return [DocumentChunk(source_locator="document", text="\n".join(rows))]

    raise ValueError(f"Unsupported attachment type: {suffix or '<none>'}")
