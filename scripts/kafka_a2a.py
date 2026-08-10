#!/usr/bin/env python3
"""Small A2A 1.0 HTTP+JSON client helpers for the Kafka agent handoff."""

__author__ = "Paul Harvener"
__company__ = "Data-Blitz Inc."

import json
import urllib.request
import uuid
from typing import Any


PROTOCOL_VERSION = "1.0"


def build_send_message_request(prompt: str, context_id: str, message_id: str) -> dict[str, Any]:
    """Build an A2A SendMessageRequest using one text part."""
    return {
        "message": {
            "messageId": message_id,
            "contextId": context_id,
            "role": "ROLE_USER",
            "parts": [{"text": prompt}],
        },
        "configuration": {"acceptedOutputModes": ["text/plain"]},
    }


def extract_task_update(
    body: dict[str, Any],
    context_id: str,
) -> tuple[str, dict[str, Any], str, str]:
    """Extract artifacts, trace, identity, and state from any valid A2A task update."""
    task = body.get("task") if isinstance(body, dict) else None
    if not isinstance(task, dict):
        raise RuntimeError("Kafka Remediation Agent returned an invalid A2A task response.")
    state_value = str(task.get("status", {}).get("state", ""))
    detail = ""
    status_message = task.get("status", {}).get("message", {})
    if isinstance(status_message, dict):
        for part in status_message.get("parts", []):
            if isinstance(part, dict) and part.get("text"):
                detail = str(part["text"])
                break
    text_parts: list[str] = []
    for artifact in task.get("artifacts", []):
        if not isinstance(artifact, dict):
            continue
        for part in artifact.get("parts", []):
            if isinstance(part, dict) and part.get("text"):
                text_parts.append(str(part["text"]))
    answer = "\n".join(text_parts).strip()
    if len(text_parts) > 1:
        # The Doer preserves the original request, then appends the plan and
        # final result as artifacts. Consumers display the latest artifact.
        answer = text_parts[-1].strip()
    metadata = task.get("metadata", {}) if isinstance(task.get("metadata"), dict) else {}
    trace = dict(metadata.get("trace", {})) if isinstance(metadata.get("trace"), dict) else {}
    trace["a2a"] = {
        "protocol_version": PROTOCOL_VERSION,
        "task_id": str(task.get("id", "")),
        "context_id": str(task.get("contextId", context_id)),
        "transport": "HTTP+JSON",
        "task_state": state_value,
    }
    terminal_error_states = {
        "TASK_STATE_FAILED",
        "TASK_STATE_CANCELED",
        "TASK_STATE_REJECTED",
    }
    if state_value in terminal_error_states:
        raise RuntimeError(detail or answer or f"A2A remediation task ended in {state_value}.")
    if state_value == "TASK_STATE_COMPLETED" and not answer:
        raise RuntimeError("Kafka Remediation Agent completed without a text artifact.")
    if not answer:
        answer = detail or f"A2A remediation task is in {state_value or 'an unknown state'}."
    return answer, trace, str(task.get("id", "")), state_value


def extract_completed_task(body: dict[str, Any], context_id: str) -> tuple[str, dict[str, Any], str]:
    """Extract the text artifact and trace from a completed A2A task response."""
    answer, trace, task_id, state_value = extract_task_update(body, context_id)
    if state_value != "TASK_STATE_COMPLETED":
        raise RuntimeError(f"A2A remediation task ended in {state_value or 'an unknown state'}.")
    return answer, trace, task_id


def send_message(
    *,
    base_url: str,
    token: str,
    prompt: str,
    context_id: str,
    timeout_seconds: float,
) -> tuple[str, dict[str, Any], str]:
    """Send one synchronous A2A 1.0 message and return its completed task artifact."""
    message_id = uuid.uuid4().hex
    payload = build_send_message_request(prompt, context_id, message_id)
    req = urllib.request.Request(
        url=f"{base_url.rstrip('/')}/message:send",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Accept": "application/a2a+json",
            "Content-Type": "application/a2a+json",
            "A2A-Version": PROTOCOL_VERSION,
            "Authorization": f"Bearer {token}",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
        body = json.loads(response.read().decode("utf-8"))
    answer, trace, task_id, _state = extract_task_update(body, context_id)
    trace["a2a"]["message_id"] = message_id
    return answer, trace, task_id


def get_task(
    *,
    base_url: str,
    token: str,
    task_id: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Get the current A2A 1.0 task state for Monitor-side polling."""
    req = urllib.request.Request(
        url=f"{base_url.rstrip('/')}/tasks/{task_id}",
        method="GET",
        headers={
            "Accept": "application/a2a+json",
            "A2A-Version": PROTOCOL_VERSION,
            "Authorization": f"Bearer {token}",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
        body = json.loads(response.read().decode("utf-8"))
    if not isinstance(body, dict):
        raise RuntimeError("Kafka Remediation Agent returned an invalid Get Task response.")
    return body
