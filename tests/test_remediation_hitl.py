import importlib
import sys
from pathlib import Path

import pytest


scripts_path = Path(__file__).resolve().parents[1] / "scripts"
if str(scripts_path) not in sys.path:
    sys.path.insert(0, str(scripts_path))
hitl = importlib.import_module("remediation_hitl")


def create_task(store, task_id="task-1", require_approval=True):
    return store.create(
        task_id=task_id,
        context_id="context-1",
        message_id="message-1",
        a2a_message="Fix broker latency.",
        plan="1. Verify metrics.\n2. Apply smallest repair.",
        require_approval=require_approval,
        human_review={"title": "Approve?", "issue": "Broker latency."},
        full_context={"a2a_transport": {"protocol_version": "1.0"}},
    )


def test_toggle_and_input_required_task_snapshot_are_isolated() -> None:
    store = hitl.RemediationHitlStore(enabled=True, max_tasks=2)
    created = create_task(store)
    created["plan"] = "mutated outside store"
    created["full_context"]["a2a_transport"]["protocol_version"] = "mutated"

    assert store.enabled() is True
    assert store.set_enabled(False) is False
    snapshot = store.snapshot()
    assert snapshot["enabled"] is False
    assert snapshot["tasks"][0]["task_state"] == hitl.INPUT_REQUIRED
    assert snapshot["tasks"][0]["plan"].startswith("1. Verify")
    assert snapshot["tasks"][0]["full_context"]["a2a_transport"]["protocol_version"] == "1.0"


def test_approval_moves_task_to_working_then_completed() -> None:
    store = hitl.RemediationHitlStore()
    create_task(store)

    approved = store.decide("task-1", decision="approve", note="Proceed carefully.")
    assert approved["task_state"] == hitl.WORKING
    assert approved["decision_note"] == "Proceed carefully."

    completed = store.complete("task-1", result="Fixed.", trace={"tools": 2})
    assert completed["task_state"] == hitl.COMPLETED
    assert completed["result"] == "Fixed."
    assert store.get("task-1")["trace"] == {"tools": 2}


def test_rejection_is_terminal_with_default_or_custom_note() -> None:
    store = hitl.RemediationHitlStore()
    create_task(store, "task-default")
    create_task(store, "task-custom")

    assert store.decide("task-default", decision="reject")["error"] == "Rejected by human operator."
    rejected = store.decide("task-custom", decision="reject", note="Maintenance freeze.")
    assert rejected["task_state"] == hitl.REJECTED
    assert rejected["error"] == "Maintenance freeze."


def test_automatic_task_starts_working_and_can_fail() -> None:
    store = hitl.RemediationHitlStore()
    automatic = create_task(store, require_approval=False)
    assert automatic["task_state"] == hitl.WORKING
    failed = store.fail("task-1", error="tool failed")
    assert failed["task_state"] == hitl.FAILED
    assert failed["error"] == "tool failed"


def test_invalid_decision_state_and_unknown_task_are_rejected() -> None:
    store = hitl.RemediationHitlStore()
    create_task(store)
    with pytest.raises(ValueError, match="decision must"):
        store.decide("task-1", decision="maybe")
    store.decide("task-1", decision="approve")
    with pytest.raises(ValueError, match="not waiting"):
        store.decide("task-1", decision="reject")
    with pytest.raises(KeyError, match="unknown HITL task"):
        store.complete("missing", result="", trace={})
    assert store.get("missing") is None


def test_history_is_bounded_and_minimum_capacity_is_one() -> None:
    store = hitl.RemediationHitlStore(max_tasks=0)
    create_task(store, "task-1")
    create_task(store, "task-2")
    snapshot = store.snapshot()
    assert [task["task_id"] for task in snapshot["tasks"]] == ["task-2"]


def test_extract_issue_from_structured_or_direct_handoff() -> None:
    structured = "Intro\n\nIssue line:\n3. [BAD] Broker offline Fix: restore it\n\nObjective:\n- verify"
    assert hitl.extract_issue_from_handoff(structured) == "3. [BAD] Broker offline Fix: restore it"
    assert hitl.extract_issue_from_handoff("Direct validation request\nSecond line") == "Direct validation request"
    assert hitl.extract_issue_from_handoff("   ") == "No issue text supplied."


def test_extract_diagnosis_from_handoff() -> None:
    handoff_text = (
        "Issue line:\n[WARN] ISR below target. Fix: inspect replicas\n\n"
        "BEGIN MONITOR DIAGNOSIS (REQUIRED CONTEXT)\n"
        "Broker 2 is offline, causing replica loss.\n"
        "END MONITOR DIAGNOSIS"
    )
    assert hitl.extract_diagnosis_from_handoff(handoff_text) == (
        "Broker 2 is offline, causing replica loss."
    )


def test_format_human_plan_preserves_decision_content_without_fix_injection() -> None:
    plan = hitl.format_human_plan(
        "## Intended outcome\n- READ ONLY: inspect ISR\n2. CONDITIONAL MUTATION: increase replicas\n* Roll back if ISR falls",
        max_items=3,
    )
    assert plan == (
        "1. Intended outcome\n"
        "2. READ ONLY: inspect ISR\n"
        "3. CONDITIONAL MUTATION: increase replicas"
    )
    assert "Fix:" not in plan
    assert hitl.format_human_plan("-") == "1. -"
    assert hitl.format_human_plan("   ") == ""


def test_build_review_context_exposes_exact_prompts_but_redacts_token() -> None:
    review, context = hitl.build_review_context(
        protocol_version="1.0",
        endpoint="http://doer/a2a/remediation/message:send",
        task_id="task-1",
        context_id="context-1",
        message_id="message-1",
        a2a_message="Issue line:\n[BAD] ISR below minimum\n\nObjective:\n- repair",
        plan="1. READ ONLY: verify ISR.",
        planning_system_prompt="planning prompt",
        execution_system_prompt="execution prompt",
        tool_names=["write_tool", "read_tool"],
        require_approval=True,
    )

    assert review["issue"] == "[BAD] ISR below minimum"
    assert "Approve & Run" in review["approval_effect"]
    assert context["a2a_transport"]["headers"]["Authorization"] == "Bearer [redacted]"
    assert context["a2a_transport"]["request"]["message"]["parts"][0]["text"].startswith("Issue line:")
    assert context["planning_llm_context"]["system_prompt"] == "planning prompt"
    assert context["approved_execution_llm_context"]["available_tool_names"] == [
        "read_tool",
        "write_tool",
    ]

    read_only_review, read_only_context = hitl.build_review_context(
        protocol_version="1.0",
        endpoint="http://doer/a2a/remediation/message:send",
        task_id="task-read",
        context_id="context-read",
        message_id="message-read",
        a2a_message="Inspect ISR with read tools only. Do not change Kafka.",
        plan="1. READ ONLY: inspect ISR.",
        planning_system_prompt="planning prompt",
        execution_system_prompt="execution prompt",
        tool_names=["read_tool"],
        require_approval=True,
    )
    assert "forbids Kafka mutations" in read_only_review["approval_effect"]
    assert read_only_context["task"]["read_only_requested"] is True
