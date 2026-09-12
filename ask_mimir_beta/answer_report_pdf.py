"""Lightweight branded PDF export for completed Ask Mimir answers."""

from __future__ import annotations

import html
import re
from io import BytesIO
from pathlib import Path
from typing import Iterable

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from reportlab.platypus import (
    ListFlowable,
    ListItem,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


NAVY = colors.HexColor("#12283A")
CYAN = colors.HexColor("#2E9FCB")
LIGHT_CYAN = colors.HexColor("#EAF5FA")
INK = colors.HexColor("#1D2933")
MUTED = colors.HexColor("#62717D")
RULE = colors.HexColor("#D9E2E8")
PAPER = colors.HexColor("#FFFFFF")
SOFT = colors.HexColor("#F4F7F9")


def _register_fonts() -> tuple[str, str]:
    """Use a clean bundled font when present, otherwise ReportLab's Helvetica."""
    candidates = (
        (
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ),
        (
            "/System/Library/Fonts/Supplemental/Arial.ttf",
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
        ),
    )
    for regular_path, bold_path in candidates:
        if not Path(regular_path).is_file() or not Path(bold_path).is_file():
            continue
        try:
            pdfmetrics.registerFont(TTFont("MimirSans", regular_path))
            pdfmetrics.registerFont(TTFont("MimirSansBold", bold_path))
            return "MimirSans", "MimirSansBold"
        except Exception:
            continue
    return "Helvetica", "Helvetica-Bold"


def _inline_markup(value: str) -> str:
    """Convert the small inline Markdown subset used by Ask Mimir to safe XML."""
    escaped = html.escape(value.strip(), quote=True)
    escaped = re.sub(
        r"\[([^\]]+)\]\((https?://[^)]+)\)",
        r'<link href="\2" color="#287FA5">\1</link>',
        escaped,
    )
    escaped = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", escaped)
    escaped = re.sub(r"(?<!\*)\*([^*]+)\*(?!\*)", r"<i>\1</i>", escaped)
    escaped = re.sub(r"`([^`]+)`", r'<font name="Courier">\1</font>', escaped)
    return escaped


def _is_table_separator(line: str) -> bool:
    cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
    return bool(cells) and all(re.fullmatch(r":?-{3,}:?", cell) for cell in cells)


def _table_cells(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def _answer_blocks(answer: str) -> Iterable[tuple[str, object]]:
    """Yield headings, paragraphs, bullets and GitHub-style tables."""
    lines = answer.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    index = 0
    paragraph: list[str] = []

    def flush_paragraph() -> Iterable[tuple[str, object]]:
        if paragraph:
            text = " ".join(part.strip() for part in paragraph if part.strip())
            paragraph.clear()
            if text:
                yield ("paragraph", text)

    while index < len(lines):
        line = lines[index].rstrip()
        stripped = line.strip()
        if not stripped:
            yield from flush_paragraph()
            index += 1
            continue

        heading = re.match(r"^(#{1,4})\s+(.+)$", stripped)
        if heading:
            yield from flush_paragraph()
            yield ("heading", (len(heading.group(1)), heading.group(2)))
            index += 1
            continue

        if (
            "|" in stripped
            and index + 1 < len(lines)
            and _is_table_separator(lines[index + 1])
        ):
            yield from flush_paragraph()
            rows = [_table_cells(stripped)]
            index += 2
            while index < len(lines) and "|" in lines[index] and lines[index].strip():
                rows.append(_table_cells(lines[index]))
                index += 1
            yield ("table", rows)
            continue

        bullet = re.match(r"^[-*+]\s+(.+)$", stripped)
        if bullet:
            yield from flush_paragraph()
            items: list[str] = []
            while index < len(lines):
                match = re.match(r"^\s*[-*+]\s+(.+)$", lines[index])
                if not match:
                    break
                items.append(match.group(1).strip())
                index += 1
            yield ("bullets", items)
            continue

        paragraph.append(stripped)
        index += 1

    yield from flush_paragraph()


def _styles(regular_font: str, bold_font: str) -> dict[str, ParagraphStyle]:
    sample = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "MimirTitle",
            parent=sample["Title"],
            fontName=bold_font,
            fontSize=22,
            leading=26,
            textColor=NAVY,
            alignment=TA_LEFT,
            spaceAfter=12,
        ),
        "question": ParagraphStyle(
            "MimirQuestion",
            parent=sample["BodyText"],
            fontName=regular_font,
            fontSize=9.5,
            leading=14,
            textColor=INK,
        ),
        "body": ParagraphStyle(
            "MimirBody",
            parent=sample["BodyText"],
            fontName=regular_font,
            fontSize=9.2,
            leading=14,
            textColor=INK,
            spaceAfter=7,
        ),
        "h1": ParagraphStyle(
            "MimirHeading1",
            parent=sample["Heading1"],
            fontName=bold_font,
            fontSize=14,
            leading=18,
            textColor=NAVY,
            spaceBefore=13,
            spaceAfter=6,
        ),
        "h2": ParagraphStyle(
            "MimirHeading2",
            parent=sample["Heading2"],
            fontName=bold_font,
            fontSize=11.5,
            leading=15,
            textColor=NAVY,
            spaceBefore=11,
            spaceAfter=5,
        ),
        "table_header": ParagraphStyle(
            "MimirTableHeader",
            parent=sample["BodyText"],
            fontName=bold_font,
            fontSize=7.2,
            leading=9,
            textColor=PAPER,
        ),
        "table_body": ParagraphStyle(
            "MimirTableBody",
            parent=sample["BodyText"],
            fontName=regular_font,
            fontSize=7,
            leading=9,
            textColor=INK,
        ),
    }


def _draw_page(canvas, document) -> None:
    canvas.saveState()
    width, height = LETTER
    canvas.setFillColor(NAVY)
    canvas.rect(0, height - 26, width, 26, stroke=0, fill=1)
    canvas.setFillColor(CYAN)
    canvas.rect(0, height - 29, width, 3, stroke=0, fill=1)
    canvas.setFont("Helvetica-Bold", 7.5)
    canvas.setFillColor(colors.white)
    canvas.drawString(document.leftMargin, height - 17, "MIMIR ADVISORS")
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(MUTED)
    canvas.drawString(document.leftMargin, 24, "Ask Mimir research brief")
    canvas.drawRightString(width - document.rightMargin, 24, f"Page {document.page}")
    canvas.setStrokeColor(RULE)
    canvas.setLineWidth(0.5)
    canvas.line(document.leftMargin, 34, width - document.rightMargin, 34)
    canvas.restoreState()


def _title(scope_name: str | None) -> str:
    value = re.sub(r"\s+", " ", str(scope_name or "")).strip()
    return value or "Defense market research brief"


def answer_report_filename(scope_name: str | None, question: str) -> str:
    stem = _title(scope_name) if scope_name else question
    stem = re.sub(r"[^A-Za-z0-9]+", "-", stem).strip("-").lower()[:72]
    return f"mimir-{stem or 'research-brief'}.pdf"


def build_branded_answer_pdf(
    *,
    question: str,
    answer: str,
    scope_name: str | None = None,
) -> bytes:
    """Return a compact, shareable PDF containing one completed Ask Mimir answer."""
    regular_font, bold_font = _register_fonts()
    styles = _styles(regular_font, bold_font)
    output = BytesIO()
    document = SimpleDocTemplate(
        output,
        pagesize=LETTER,
        leftMargin=0.58 * inch,
        rightMargin=0.58 * inch,
        topMargin=0.62 * inch,
        bottomMargin=0.58 * inch,
        title=_title(scope_name),
        author="Mimir Advisors",
        subject="Ask Mimir research brief",
    )

    story: list[object] = [
        Spacer(1, 3),
        Paragraph("ASK MIMIR · RESEARCH BRIEF", ParagraphStyle(
            "MimirKicker",
            fontName=bold_font,
            fontSize=7.5,
            leading=10,
            textColor=CYAN,
            spaceAfter=5,
        )),
        Paragraph(_inline_markup(_title(scope_name)), styles["title"]),
        Table(
            [[Paragraph("<b>QUESTION</b>", ParagraphStyle(
                "MimirQuestionLabel",
                fontName=bold_font,
                fontSize=7,
                leading=10,
                textColor=CYAN,
            )), Paragraph(_inline_markup(question), styles["question"])]],
            colWidths=[0.92 * inch, document.width - 0.92 * inch],
            style=TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), LIGHT_CYAN),
                ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#BBDDEA")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 9),
                ("RIGHTPADDING", (0, 0), (-1, -1), 9),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ]),
        ),
        Spacer(1, 12),
    ]

    for block_type, content in _answer_blocks(answer):
        if block_type == "heading":
            level, value = content
            story.append(Paragraph(_inline_markup(str(value)), styles["h1" if level == 1 else "h2"]))
        elif block_type == "paragraph":
            story.append(Paragraph(_inline_markup(str(content)), styles["body"]))
        elif block_type == "bullets":
            story.append(ListFlowable(
                [
                    ListItem(Paragraph(_inline_markup(item), styles["body"]), leftIndent=9)
                    for item in content
                ],
                bulletType="bullet",
                leftIndent=17,
                bulletFontName=regular_font,
                bulletFontSize=6,
                bulletColor=CYAN,
                spaceAfter=7,
            ))
        elif block_type == "table":
            rows = content
            widest = max(len(row) for row in rows)
            normalized = [row + [""] * (widest - len(row)) for row in rows]
            cells = [
                [
                    Paragraph(_inline_markup(cell), styles["table_header" if row_index == 0 else "table_body"])
                    for cell in row
                ]
                for row_index, row in enumerate(normalized)
            ]
            table = Table(
                cells,
                colWidths=[document.width / widest] * widest,
                repeatRows=1,
                hAlign="LEFT",
            )
            table.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), NAVY),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [PAPER, SOFT]),
                ("GRID", (0, 0), (-1, -1), 0.35, RULE),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]))
            story.extend([Spacer(1, 3), table, Spacer(1, 8)])

    document.build(story, onFirstPage=_draw_page, onLaterPages=_draw_page)
    return output.getvalue()
