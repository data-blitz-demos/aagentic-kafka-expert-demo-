"""Helpers for consistently presenting mixed-direction DeepEval metrics."""

__author__ = "Paul Harvener"
__company__ = "Data-Blitz Inc."

from collections.abc import Iterable
from typing import Any


LOWER_IS_BETTER = {"Bias", "Toxicity"}


def metric_direction(name: str) -> str:
    """Return the score direction used by a DeepEval metric."""
    return "lower_is_better" if name in LOWER_IS_BETTER else "higher_is_better"


def normalized_quality_score(name: str, score: float) -> float:
    """Convert a raw metric score to a common higher-is-better quality score."""
    bounded = min(1.0, max(0.0, float(score)))
    return 1.0 - bounded if metric_direction(name) == "lower_is_better" else bounded


def build_metric_payload(
    *,
    name: str,
    score: float,
    threshold: float,
    passed: bool,
    reason: str,
) -> dict[str, Any]:
    """Build the stable metric representation returned by the judge API."""
    direction = metric_direction(name)
    rounded_score = round(float(score), 4)
    return {
        "name": name,
        "score": rounded_score,
        "quality_score": round(normalized_quality_score(name, rounded_score), 4),
        "score_direction": direction,
        "threshold": float(threshold),
        "threshold_operator": "<=" if direction == "lower_is_better" else ">=",
        "passed": bool(passed),
        "reason": str(reason or ""),
    }


def average_quality_score(metrics: Iterable[dict[str, Any]]) -> float:
    """Average normalized quality scores, returning zero for an empty set."""
    scores = [float(metric.get("quality_score", 0.0)) for metric in metrics]
    return round(sum(scores) / len(scores), 4) if scores else 0.0
