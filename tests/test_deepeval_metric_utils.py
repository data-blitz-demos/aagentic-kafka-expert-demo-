import importlib
import sys
from pathlib import Path


scripts_path = Path(__file__).resolve().parents[1] / "scripts"
if str(scripts_path) not in sys.path:
    sys.path.insert(0, str(scripts_path))
metrics = importlib.import_module("deepeval_metric_utils")


def test_metric_direction_supports_both_score_directions() -> None:
    assert metrics.metric_direction("Bias") == "lower_is_better"
    assert metrics.metric_direction("Toxicity") == "lower_is_better"
    assert metrics.metric_direction("AnswerRelevancy") == "higher_is_better"


def test_normalized_quality_score_inverts_and_bounds_safety_metrics() -> None:
    assert metrics.normalized_quality_score("Bias", 0.2) == 0.8
    assert metrics.normalized_quality_score("AnswerRelevancy", 0.8) == 0.8
    assert metrics.normalized_quality_score("Toxicity", -1) == 1.0
    assert metrics.normalized_quality_score("AnswerRelevancy", 2) == 1.0


def test_build_metric_payload_labels_lower_is_better() -> None:
    assert metrics.build_metric_payload(
        name="Bias",
        score=0.12555,
        threshold=0.2,
        passed=True,
        reason="No biased opinions.",
    ) == {
        "name": "Bias",
        "score": 0.1255,
        "quality_score": 0.8745,
        "score_direction": "lower_is_better",
        "threshold": 0.2,
        "threshold_operator": "<=",
        "passed": True,
        "reason": "No biased opinions.",
    }


def test_build_metric_payload_labels_higher_is_better_and_empty_reason() -> None:
    payload = metrics.build_metric_payload(
        name="AnswerRelevancy",
        score=0.75,
        threshold=0.7,
        passed=True,
        reason="",
    )
    assert payload["threshold_operator"] == ">="
    assert payload["quality_score"] == 0.75
    assert payload["reason"] == ""


def test_average_quality_score_handles_values_and_empty_input() -> None:
    assert metrics.average_quality_score([{"quality_score": 0.8}, {"quality_score": 1.0}]) == 0.9
    assert metrics.average_quality_score([]) == 0.0
