from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

# Defaults
DEFAULT_OUTPUT_PATH = Path("tmp/pdfs/kafka-expert-app-summary.pdf").resolve()

SECTION_HEADINGS = (
    "What it is",
    "Who it is for",
    "What it does",
    "How it works",
    "How to run",
    "Code-level note",
)


def _resolve_output_path(output_path: str | Path | None) -> Path:
    """Return an absolute output path (defaults to DEFAULT_OUTPUT_PATH)."""
    if output_path is None:
        return DEFAULT_OUTPUT_PATH
    return Path(output_path)


def build_summary_payload() -> List[Tuple[str, List[str]]]:
    """Return (heading, bullet lines) tuples for the PDF summary."""
    payload: List[Tuple[str, List[str]]] = []
    for heading in SECTION_HEADINGS:
        lines: List[str] = [f"Not found in repo: {heading} content placeholder."]
        payload.append((heading, lines))
    return payload


# -------- PDF writer (minimal, dependency-free) --------

def _pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _build_content_stream(lines: Iterable[str]) -> bytes:
    parts = [
        "BT",
        "/F1 12 Tf",
        "14 TL",
        "50 780 Td",
    ]
    for line in lines:
        parts.append(f"({_pdf_escape(line)}) Tj")
        parts.append("T*")
    parts.append("ET")
    return ("\n".join(parts) + "\n").encode("latin-1")


def _build_pdf_bytes(lines: Sequence[str]) -> bytes:
    content_bytes = _build_content_stream(lines)

    buffer = io.BytesIO()
    buffer.write(b"%PDF-1.4\n%\xE2\xE3\xCF\xD3\n")

    offsets: list[int] = [0]  # object 0 placeholder

    def write_obj(obj_num: int, body: bytes) -> None:
        offsets.append(buffer.tell())
        buffer.write(f"{obj_num} 0 obj\n".encode("latin-1"))
        buffer.write(body)
        buffer.write(b"\nendobj\n")

    write_obj(1, b"<< /Type /Catalog /Pages 2 0 R >>")
    write_obj(2, b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    write_obj(
        3,
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] "
        b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
    )
    # Content stream
    offsets.append(buffer.tell())
    buffer.write(b"4 0 obj\n")
    buffer.write(f"<< /Length {len(content_bytes)} >>\nstream\n".encode("latin-1"))
    buffer.write(content_bytes)
    buffer.write(b"endstream\nendobj\n")
    # Font
    write_obj(5, b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    xref_start = buffer.tell()
    size = len(offsets)
    buffer.write(f"xref\n0 {size}\n".encode("latin-1"))
    buffer.write(b"0000000000 65535 f \n")
    for pos in offsets[1:]:
        buffer.write(f"{pos:010d} 00000 n \n".encode("latin-1"))

    buffer.write(
        b"trailer\n"
        + f"<< /Size {size} /Root 1 0 R >>\n".encode("latin-1")
        + b"startxref\n"
        + f"{xref_start}\n".encode("latin-1")
        + b"%%EOF\n"
    )
    return buffer.getvalue()


def generate_pdf(output_path: str | Path | None = None) -> Path:
    """Generate a single-page PDF with summary content; return the resolved path."""
    out_path = _resolve_output_path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    payload = build_summary_payload()
    lines: list[str] = ["Kafka Expert Demo - App Summary", ""]
    for heading, bullets in payload:
        lines.append(heading)
        for bullet in bullets:
            lines.append(f"- {bullet}")
        lines.append("")

    pdf_bytes = _build_pdf_bytes(lines)
    out_path.write_bytes(pdf_bytes)
    return out_path


if __name__ == "__main__":
    generate_pdf()
    print(f"Generated: {DEFAULT_OUTPUT_PATH}")
