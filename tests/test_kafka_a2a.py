import importlib
import sys
from pathlib import Path

import pytest


scripts_path = Path(__file__).resolve().parents[1] / "scripts"
if str(scripts_path) not in sys.path:
    sys.path.insert(0, str(scripts_path))
kafka_a2a = importlib.import_module("kafka_a2a")
build_send_message_request = kafka_a2a.build_send_message_request
extract_completed_task = kafka_a2a.extract_completed_task


def test_build_send_message_request_uses_a2a_role_and_text_part() -> None:
    payload = build_send_message_request("repair it", "context-1", "message-1")

    assert payload["message"] == {
        "messageId": "message-1",
        "contextId": "context-1",
        "role": "ROLE_USER",
        "parts": [{"text": "repair it"}],
    }
    assert payload["configuration"]["acceptedOutputModes"] == ["text/plain"]


def test_extract_completed_task_returns_artifact_and_trace() -> None:
    answer, trace, task_id = extract_completed_task(
        {
            "task": {
                "id": "task-1",
                "contextId": "context-1",
                "status": {"state": "TASK_STATE_COMPLETED"},
                "artifacts": [{"parts": [{"text": "fixed and verified"}]}],
                "metadata": {"trace": {"agent_role": "remediation"}},
            }
        },
        "fallback-context",
    )

    assert answer == "fixed and verified"
    assert task_id == "task-1"
    assert trace["agent_role"] == "remediation"
    assert trace["a2a"]["protocol_version"] == "1.0"
    assert trace["a2a"]["context_id"] == "context-1"


def test_extract_completed_task_surfaces_failed_task_message() -> None:
    with pytest.raises(RuntimeError, match="mutation was rejected"):
        extract_completed_task(
            {
                "task": {
                    "id": "task-2",
                    "status": {
                        "state": "TASK_STATE_FAILED",
                        "message": {"parts": [{"text": "mutation was rejected"}]},
                    },
                }
            },
            "context-2",
        )
