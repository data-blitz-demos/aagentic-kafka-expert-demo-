"""Build a complete, inspectable Monitor-to-Doer diagnostic handoff."""

__author__ = "Paul Harvener"
__company__ = "Data-Blitz Inc."

import re
from typing import Any


def monitor_reported_reason(issue_line: str) -> str:
    """Return the Monitor's diagnosis from a rendered finding without its action clause."""
    reason = re.sub(r"^\s*\d+\.\s*", "", issue_line.strip())
    reason = re.sub(r"^\[(?:GOOD|WARN|BAD)\]\s*", "", reason, flags=re.IGNORECASE)
    reason = re.split(r"\s+(?:Fix|Action|Next step):\s*", reason, maxsplit=1, flags=re.IGNORECASE)[0]
    return reason.strip().rstrip(".") or "The Monitor did not provide a separate diagnosis."


def source_monitor_output(issue_line: str, snapshot: dict[str, Any]) -> str:
    """Find the complete Monitor response that contained the selected finding."""
    candidate = issue_line.strip()
    for message in reversed(snapshot.get("chat", [])):
        if not isinstance(message, dict) or message.get("role") not in {"agent", "monitor"}:
            continue
        text = str(message.get("text", "")).strip()
        if candidate and candidate in {line.strip() for line in text.splitlines()}:
            return text

    status_text = str(snapshot.get("status_text", "")).strip()
    if candidate and candidate in {line.strip() for line in status_text.splitlines()}:
        return status_text
    return candidate


def build_monitor_handoff(issue_line: str, snapshot: dict[str, Any]) -> dict[str, str]:
    """Build the lossless diagnostic fields sent from Monitor to Doer.

    Keep the short selected finding and the complete Monitor response separate:
    the former is actionable, while the latter contains the evidence and causal
    explanation that can be lost when a UI finding is reduced to one line.
    """
    selected = issue_line.strip()
    return {
        "issue_line": selected,
        "monitor_reason": monitor_reported_reason(selected),
        "monitor_context": source_monitor_output(selected, snapshot),
    }
