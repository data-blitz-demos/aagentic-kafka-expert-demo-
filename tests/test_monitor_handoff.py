import importlib
import sys
from pathlib import Path


scripts_path = Path(__file__).resolve().parents[1] / "scripts"
if str(scripts_path) not in sys.path:
    sys.path.insert(0, str(scripts_path))
handoff = importlib.import_module("monitor_handoff")


def test_build_monitor_handoff_preserves_reason_and_full_diagnostic() -> None:
    issue = "2. [BAD] Under-replicated partitions increased because broker 2 is offline. Fix: restore broker"
    snapshot = {
        "status_text": "",
        "chat": [
            {
                "role": "agent",
                "text": (
                    "1. [GOOD] Prometheus is reachable.\n"
                    f"{issue}\n"
                    "3. [WARN] Controller failover is delayed. Fix: inspect controller logs"
                ),
            }
        ],
    }

    result = handoff.build_monitor_handoff(issue, snapshot)

    assert result["issue_line"] == issue
    assert result["monitor_reason"] == (
        "Under-replicated partitions increased because broker 2 is offline"
    )
    assert "Prometheus is reachable" in result["monitor_context"]
    assert "Controller failover is delayed" in result["monitor_context"]


def test_build_monitor_handoff_falls_back_to_selected_finding() -> None:
    result = handoff.build_monitor_handoff(
        "[WARN] ISR is below target. Fix: inspect replicas", {"chat": []}
    )

    assert result == {
        "issue_line": "[WARN] ISR is below target. Fix: inspect replicas",
        "monitor_reason": "ISR is below target",
        "monitor_context": "[WARN] ISR is below target. Fix: inspect replicas",
    }
