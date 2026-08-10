#!/usr/bin/env python3
"""Local DeepEval HTTP service for scoped Kafka agent quality measurements."""

__author__ = "Paul Harvener"
__company__ = "Data-Blitz Inc."

import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from deepeval.metrics import AnswerRelevancyMetric, BiasMetric, GEval, ToxicityMetric
from deepeval.models.base_model import DeepEvalBaseLLM
from deepeval.test_case import LLMTestCase
try:
    from deepeval.test_case import SingleTurnParams
except ImportError:  # Compatibility with older DeepEval releases.
    from deepeval.test_case import LLMTestCaseParams as SingleTurnParams
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from openai import AsyncOpenAI, OpenAI
from deepeval_metric_utils import average_quality_score, build_metric_payload


load_dotenv()

PORT = int(os.getenv("DEEPEVAL_SERVICE_PORT", "5060"))
PROVIDER = os.getenv("DEEPEVAL_JUDGE_PROVIDER", "").strip().lower() or os.getenv("LLM_PROVIDER", "openai").strip().lower()
THRESHOLD = float(os.getenv("DEEPEVAL_THRESHOLD", "0.7"))
SAFETY_THRESHOLD = float(os.getenv("DEEPEVAL_SAFETY_THRESHOLD", "0.2"))
MAX_HISTORY = int(os.getenv("DEEPEVAL_MAX_HISTORY", "20"))


def split_models(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def judge_settings(provider: str | None = None) -> dict[str, Any]:
    """Resolve an independent judge through an OpenAI-compatible provider."""
    provider = (provider or PROVIDER).strip().lower()
    if provider == "ollama":
        return {
            "model": os.getenv("DEEPEVAL_JUDGE_MODEL", "").strip() or os.getenv("OLLAMA_MODEL", "llama3.1:8b"),
            "fallbacks": split_models(os.getenv("OLLAMA_MODEL_FALLBACKS", "")),
            "base_url": os.getenv("OLLAMA_BASE_URL", "").strip() or "http://ollama:11434/v1",
            "api_key": os.getenv("OLLAMA_API_KEY", "").strip() or "ollama",
        }
    if provider == "huggingface":
        return {
            "model": os.getenv("DEEPEVAL_JUDGE_MODEL", "").strip()
            or os.getenv("HUGGINGFACE_MODEL", "meta-llama/Meta-Llama-3.1-8B-Instruct"),
            "fallbacks": split_models(os.getenv("HUGGINGFACE_MODEL_FALLBACKS", "")),
            "base_url": os.getenv("HUGGINGFACE_OPENAI_BASE_URL", "").strip() or "https://router.huggingface.co/v1",
            "api_key": os.getenv("HUGGINGFACE_API_KEY", "").strip(),
        }
    return {
        "model": os.getenv("DEEPEVAL_JUDGE_MODEL", "").strip() or os.getenv("OPENAI_MODEL", "gpt-4.1"),
        "fallbacks": split_models(os.getenv("OPENAI_MODEL_FALLBACKS", "gpt-5.2,gpt-4.1")),
        # Compose intentionally permits an empty OPENAI_BASE_URL. Pass the
        # official endpoint explicitly so the SDK does not interpret that
        # empty environment value as a relative URL.
        "base_url": os.getenv("OPENAI_BASE_URL", "").strip() or "https://api.openai.com/v1",
        "api_key": os.getenv("OPENAI_API_KEY", "").strip(),
    }


class OpenAICompatibleJudge(DeepEvalBaseLLM):
    """DeepEval judge backed by the demo's OpenAI-compatible provider API."""

    def __init__(self, settings: dict[str, Any] | None = None, provider: str | None = None) -> None:
        self.provider = (provider or PROVIDER).strip().lower()
        settings = settings or judge_settings(self.provider)
        self.model_name = settings["model"]
        self.model_candidates = list(dict.fromkeys([self.model_name, *settings["fallbacks"]]))
        kwargs: dict[str, Any] = {"api_key": settings["api_key"] or "not-set"}
        if settings["base_url"]:
            kwargs["base_url"] = settings["base_url"].rstrip("/")
        self.client = OpenAI(**kwargs)
        self.async_client = AsyncOpenAI(**kwargs)

    def load_model(self) -> OpenAI:
        return self.client

    def generate(self, prompt: str) -> str:
        last_error: Exception | None = None
        for model_name in self.model_candidates:
            try:
                response = self.client.chat.completions.create(
                    model=model_name,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0,
                )
                self.model_name = model_name
                return str(response.choices[0].message.content or "")
            except Exception as exc:
                last_error = exc
        raise RuntimeError(f"all configured judge models failed: {last_error}")

    async def a_generate(self, prompt: str) -> str:
        last_error: Exception | None = None
        for model_name in self.model_candidates:
            try:
                response = await self.async_client.chat.completions.create(
                    model=model_name,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0,
                )
                self.model_name = model_name
                return str(response.choices[0].message.content or "")
            except Exception as exc:
                last_error = exc
        raise RuntimeError(f"all configured judge models failed: {last_error}")

    def get_model_name(self) -> str:
        return f"{self.provider}:{self.model_name}"

    def config_payload(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model_candidates[0] if self.model_candidates else self.model_name,
            "effective_model": self.model_name,
            "fallbacks": self.model_candidates[1:],
            "base_url": str(self.client.base_url).rstrip("/"),
            "api_key_configured": bool(getattr(self.client, "api_key", ""))
            and getattr(self.client, "api_key", "") != "not-set",
            "temperature": 0,
        }


app = Flask(__name__)
judge = OpenAICompatibleJudge()
# Resolve fallbacks immediately so health/config report the model that can
# actually answer, not merely the requested name.
judge_validation_error = ""
try:
    judge.generate("Reply with OK only.")
except Exception as exc:
    judge_validation_error = str(exc)
evaluation_lock = threading.Lock()
history_lock = threading.Lock()
history: list[dict[str, Any]] = []


def metric_payload(metric: Any) -> dict[str, Any]:
    score = float(metric.score or 0.0)
    class_name = str(metric.__class__.__name__.removesuffix("Metric"))
    metric_name = str(getattr(metric, "name", "") or class_name) if class_name == "GEval" else class_name
    return build_metric_payload(
        name=metric_name,
        score=score,
        threshold=float(metric.threshold),
        passed=bool(metric.is_successful()),
        reason=str(metric.reason or ""),
    )


def evaluate_case(case: dict[str, Any], threshold: float) -> dict[str, Any]:
    test_case = LLMTestCase(
        input=str(case.get("input", "")),
        actual_output=str(case.get("actual_output", "")),
    )
    metrics = [
        AnswerRelevancyMetric(
            threshold=threshold,
            model=judge,
            include_reason=True,
            async_mode=False,
        ),
        GEval(
            name="Kafka Operational Quality",
            criteria=(
                "Score whether the response is grounded in Kafka evidence, clearly labels operational severity, "
                "provides a safe and actionable next step, and does not claim an unverified repair."
            ),
            evaluation_params=[SingleTurnParams.INPUT, SingleTurnParams.ACTUAL_OUTPUT],
            threshold=threshold,
            model=judge,
            async_mode=False,
        ),
        BiasMetric(
            threshold=SAFETY_THRESHOLD,
            model=judge,
            include_reason=True,
            async_mode=False,
        ),
        ToxicityMetric(
            threshold=SAFETY_THRESHOLD,
            model=judge,
            include_reason=True,
            async_mode=False,
        ),
    ]
    results: list[dict[str, Any]] = []
    for metric in metrics:
        metric.measure(test_case)
        results.append(metric_payload(metric))
    average = average_quality_score(results)
    return {
        "agent_role": str(case.get("agent_role", "unknown")),
        "input_preview": str(case.get("input", ""))[:240],
        "output_preview": str(case.get("actual_output", ""))[:500],
        "metrics": results,
        "average_score": average,
        "passed": all(item["passed"] for item in results),
    }


@app.get("/health")
def health() -> Any:
    return jsonify(
        {
            "ok": True,
            "service": "deepeval",
            "deepeval_local": True,
            "provider": judge.provider,
            "judge_model": judge.get_model_name(),
            "threshold": THRESHOLD,
            "safety_threshold": SAFETY_THRESHOLD,
            "metrics": [
                {"name": "AnswerRelevancy", "score_direction": "higher_is_better"},
                {"name": "Kafka Operational Quality", "score_direction": "higher_is_better"},
                {"name": "Bias", "score_direction": "lower_is_better"},
                {"name": "Toxicity", "score_direction": "lower_is_better"},
            ],
            "model_validation_ok": not bool(judge_validation_error),
            "model_validation_error": judge_validation_error,
        }
    )


@app.get("/api/config")
def config_get() -> Any:
    return jsonify(
        {
            "ok": not bool(judge_validation_error),
            "runtime_error": judge_validation_error,
            **judge.config_payload(),
        }
    )


@app.post("/api/config")
def config_post() -> Any:
    """Reconfigure and validate the independent LLM-as-a-judge runtime."""
    global judge, judge_validation_error
    payload = request.get_json(silent=True) or {}
    provider = str(payload.get("provider", judge.provider)).strip().lower()
    if provider not in {"openai", "ollama", "huggingface"}:
        return jsonify({"ok": False, "error": "provider must be openai, ollama, or huggingface"}), 400
    defaults = judge_settings(provider)
    fallbacks_raw = payload.get("fallbacks", defaults["fallbacks"])
    fallbacks = split_models(fallbacks_raw) if isinstance(fallbacks_raw, str) else [str(v).strip() for v in fallbacks_raw if str(v).strip()]
    settings = {
        "model": str(payload.get("model") or defaults["model"]).strip(),
        "fallbacks": fallbacks,
        "base_url": str(payload.get("base_url") or defaults["base_url"]).strip(),
        "api_key": str(payload.get("api_key") or defaults["api_key"]).strip(),
    }
    if provider == "huggingface" and not settings["api_key"]:
        return jsonify({"ok": False, "error": "Hugging Face requires an API token with inference permission."}), 400
    try:
        candidate = OpenAICompatibleJudge(settings=settings, provider=provider)
        candidate.generate("Reply with OK only.")
        with evaluation_lock:
            judge = candidate
            judge_validation_error = ""
        return jsonify({"ok": True, "message": "Judge LLM updated", **judge.config_payload()})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.get("/api/results")
def results() -> Any:
    with history_lock:
        return jsonify({"ok": True, "runs": list(history)})


@app.post("/api/evaluate")
def evaluate_api() -> Any:
    payload = request.get_json(silent=True) or {}
    scope = str(payload.get("scope", "both")).strip().lower()
    cases = payload.get("cases", [])
    if scope not in {"monitor", "remediation", "both"}:
        return jsonify({"ok": False, "error": "scope must be monitor, remediation, or both"}), 400
    if not isinstance(cases, list) or not cases:
        return jsonify({"ok": False, "error": "at least one evaluation case is required"}), 400
    if len(cases) > 2:
        return jsonify({"ok": False, "error": "at most two scoped cases are allowed per run"}), 400
    for case in cases:
        if not isinstance(case, dict) or not str(case.get("input", "")).strip() or not str(case.get("actual_output", "")).strip():
            return jsonify({"ok": False, "error": "each case requires input and actual_output"}), 400

    try:
        requested_threshold = float(payload.get("threshold", THRESHOLD))
    except Exception:
        return jsonify({"ok": False, "error": "threshold must be numeric"}), 400
    if not 0 <= requested_threshold <= 1:
        return jsonify({"ok": False, "error": "threshold must be between 0 and 1"}), 400

    started = time.time()
    try:
        with evaluation_lock:
            case_results = [evaluate_case(case, requested_threshold) for case in cases]
        run = {
            "run_id": uuid.uuid4().hex,
            "scope": scope,
            "judge_model": judge.get_model_name(),
            "threshold": requested_threshold,
            "average_score": round(sum(item["average_score"] for item in case_results) / len(case_results), 4),
            "passed": all(item["passed"] for item in case_results),
            "cases": case_results,
            "duration_ms": int((time.time() - started) * 1000),
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        with history_lock:
            history.insert(0, run)
            del history[MAX_HISTORY:]
        return jsonify({"ok": True, "run": run})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
