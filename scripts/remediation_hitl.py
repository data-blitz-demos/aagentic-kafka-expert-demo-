"""Thread-safe Human-in-the-Loop task state for the isolated Doer runtime."""

__author__ = "Paul Harvener"
__company__ = "Data-Blitz Inc."

import json
import re
import threading
from datetime import datetime, timezone
from typing import Any


INPUT_REQUIRED = "TASK_STATE_INPUT_REQUIRED"
WORKING = "TASK_STATE_WORKING"
COMPLETED = "TASK_STATE_COMPLETED"
FAILED = "TASK_STATE_FAILED"
REJECTED = "TASK_STATE_REJECTED"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def extract_issue_from_handoff(a2a_message: str) -> str:
    """Extract the selected Monitor finding, with a safe fallback for direct A2A requests."""
    marker = "Issue line:"
    if marker in a2a_message:
        remainder = a2a_message.split(marker, 1)[1]
        return remainder.split("\n\n", 1)[0].strip()
    return a2a_message.strip().splitlines()[0] if a2a_message.strip() else "No issue text supplied."


def extract_diagnosis_from_handoff(a2a_message: str) -> str:
    """Extract the Monitor's causal explanation for the operator's first review card."""
    match = re.search(
        r"BEGIN MONITOR DIAGNOSIS.*?\n(.*?)\nEND MONITOR DIAGNOSIS",
        a2a_message,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match and match.group(1).strip():
        return match.group(1).strip()
    return extract_issue_from_handoff(a2a_message)


def format_human_plan(text: str, *, max_items: int = 20) -> str:
    """Normalize an LLM plan without adding operational severity or generic Fix clauses."""
    items: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip().lstrip("#").strip()
        if line.startswith(("-", "*", "•")):
            line = line[1:].strip()
        line = re.sub(r"^\d+[\.\)]\s*", "", line)
        if line:
            items.append(line)
    if not items and text.strip():
        items = [text.strip()]
    return "\n".join(f"{index}. {item}" for index, item in enumerate(items[:max_items], start=1))


def build_review_context(
    *,
    protocol_version: str,
    endpoint: str,
    task_id: str,
    context_id: str,
    message_id: str,
    a2a_message: str,
    plan: str,
    planning_system_prompt: str,
    execution_system_prompt: str,
    tool_names: list[str],
    require_approval: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build the human decision copy and inspectable, secret-free Doer context."""
    issue = extract_issue_from_handoff(a2a_message)
    diagnosis = extract_diagnosis_from_handoff(a2a_message)
    lowered_message = a2a_message.lower()
    read_only_request = any(
        marker in lowered_message
        for marker in ("do not mutate", "do not change", "read tools only", "read-only")
    )
    approval_effect = (
        "Approve & Run authorizes read-only verification of this finding. This request explicitly "
        "forbids Kafka mutations."
        if read_only_request
        else (
            "Approve & Run authorizes the Doer to verify this finding and use the smallest "
            "justified Kafka Admin mutation described by the conditional plan."
        )
    )
    human_review = {
        "title": "Approve this Kafka remediation?",
        "issue": issue,
        "diagnosis": diagnosis,
        "approval_effect": approval_effect,
        "safety_boundary": (
            "Approval does not authorize unrelated changes. The Doer must verify evidence first, "
            "avoid destructive internal-topic operations, and verify or roll back any change."
        ),
        "rejection_effect": "Reject ends this A2A task without running tools or changing Kafka.",
    }
    full_context = {
        "a2a_transport": {
            "protocol_version": protocol_version,
            "endpoint": endpoint,
            "headers": {
                "A2A-Version": protocol_version,
                "Authorization": "Bearer [redacted]",
                "Content-Type": "application/a2a+json",
            },
            "request": {
                "message": {
                    "messageId": message_id,
                    "contextId": context_id,
                    "role": "ROLE_USER",
                    "parts": [{"text": a2a_message}],
                },
                "configuration": {"acceptedOutputModes": ["text/plain"]},
            },
        },
        "task": {
            "task_id": task_id,
            "human_approval_required": require_approval,
            "read_only_requested": read_only_request,
            "proposed_plan": plan,
        },
        "planning_llm_context": {
            "system_prompt": planning_system_prompt,
            "user_prompt": a2a_message,
            "tool_access": [],
        },
        "approved_execution_llm_context": {
            "system_prompt_file": "prompts/remediation_system_prompt.txt",
            "system_prompt": execution_system_prompt,
            "user_prompt": a2a_message,
            "thread_id": f"a2a:{context_id}:{task_id}",
            "prompt_source": "a2a_remediation",
            "available_tool_names": sorted(tool_names),
        },
    }
    return human_review, full_context


class RemediationHitlStore:
    """Own the Doer's bounded HITL task history and valid state transitions."""

    def __init__(self, *, enabled: bool = True, max_tasks: int = 40) -> None:
        self._enabled = bool(enabled)
        self._max_tasks = max(1, int(max_tasks))
        self._tasks: dict[str, dict[str, Any]] = {}
        self._order: list[str] = []
        self._lock = threading.Lock()

    def enabled(self) -> bool:
        with self._lock:
            return self._enabled

    def set_enabled(self, enabled: bool) -> bool:
        with self._lock:
            self._enabled = bool(enabled)
            return self._enabled

    def create(
        self,
        *,
        task_id: str,
        context_id: str,
        message_id: str,
        a2a_message: str,
        plan: str,
        require_approval: bool,
        trace: dict[str, Any] | None = None,
        human_review: dict[str, Any] | None = None,
        full_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = utc_now()
        record = {
            "task_id": task_id,
            "context_id": context_id,
            "message_id": message_id,
            "a2a_message": a2a_message,
            "plan": plan,
            "human_review": human_review or {},
            "full_context": full_context or {},
            "require_approval": bool(require_approval),
            "decision": "",
            "decision_note": "",
            "task_state": INPUT_REQUIRED if require_approval else WORKING,
            "result": "",
            "error": "",
            "trace": trace or {},
            "created_at_utc": now,
            "updated_at_utc": now,
        }
        with self._lock:
            self._tasks[task_id] = record
            self._order.insert(0, task_id)
            for stale_id in self._order[self._max_tasks :]:
                self._tasks.pop(stale_id, None)
            del self._order[self._max_tasks :]
            return self._copy(record)

    def decide(self, task_id: str, *, decision: str, note: str = "") -> dict[str, Any]:
        normalized = decision.strip().lower()
        if normalized not in {"approve", "reject"}:
            raise ValueError("decision must be approve or reject")
        with self._lock:
            record = self._require(task_id)
            if record["task_state"] != INPUT_REQUIRED:
                raise ValueError("task is not waiting for human approval")
            record["decision"] = normalized
            record["decision_note"] = note.strip()
            record["task_state"] = WORKING if normalized == "approve" else REJECTED
            if normalized == "reject":
                record["error"] = note.strip() or "Rejected by human operator."
            record["updated_at_utc"] = utc_now()
            return self._copy(record)

    def complete(self, task_id: str, *, result: str, trace: dict[str, Any]) -> dict[str, Any]:
        return self._finish(task_id, state=COMPLETED, result=result, trace=trace, error="")

    def fail(self, task_id: str, *, error: str, trace: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._finish(task_id, state=FAILED, result="", trace=trace or {}, error=error)

    def get(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            record = self._tasks.get(task_id)
            return self._copy(record) if record is not None else None

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "enabled": self._enabled,
                "tasks": [self._copy(self._tasks[task_id]) for task_id in self._order],
            }

    def _finish(
        self,
        task_id: str,
        *,
        state: str,
        result: str,
        trace: dict[str, Any],
        error: str,
    ) -> dict[str, Any]:
        with self._lock:
            record = self._require(task_id)
            record["task_state"] = state
            record["result"] = result
            record["trace"] = trace
            record["error"] = error
            record["updated_at_utc"] = utc_now()
            return self._copy(record)

    def _require(self, task_id: str) -> dict[str, Any]:
        record = self._tasks.get(task_id)
        if record is None:
            raise KeyError(f"unknown HITL task: {task_id}")
        return record

    @staticmethod
    def _copy(record: dict[str, Any]) -> dict[str, Any]:
        return json.loads(json.dumps(record))
