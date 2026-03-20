from __future__ import annotations

import importlib
import sys
import pdfplumber
from pathlib import Path


def _load_module():
    pdfs_path = Path(__file__).resolve().parents[1] / "tmp" / "pdfs"
    if str(pdfs_path) not in sys.path:
        sys.path.insert(0, str(pdfs_path))
    if "generate_app_summary_pdf" in sys.modules:
        del sys.modules["generate_app_summary_pdf"]
    module = importlib.import_module("generate_app_summary_pdf")
    return module


def test_resolve_output_path_defaults_and_override():
    module = _load_module()
    assert module._resolve_output_path(None) == module.DEFAULT_OUTPUT_PATH
    assert module._resolve_output_path("custom-output.pdf") == Path("custom-output.pdf")


def test_payload_contains_expected_sections_and_warnings():
    module = _load_module()
    payload = module.build_summary_payload()
    expected_sections = {
        "What it is",
        "Who it is for",
        "What it does",
        "How it works",
        "How to run",
        "Code-level note",
    }
    headings = {heading for heading, _ in payload}
    assert headings == expected_sections
    assert any("Not found in repo" in line for _, lines in payload for line in lines)


def test_generate_pdf_single_page(tmp_path):
    module = _load_module()
    output_pdf = tmp_path / "kafka-expert-app-summary-test.pdf"
    generated = module.generate_pdf(output_pdf)

    assert generated == output_pdf.resolve()
    assert generated.exists()
    with pdfplumber.open(generated) as pdf:
        assert len(pdf.pages) == 1
        text = pdf.pages[0].extract_text() or ""
        assert "Kafka Expert Demo - App Summary" in text
        assert "What it is" in text
        assert "How it works" in text
        assert "Not found in repo" in text
