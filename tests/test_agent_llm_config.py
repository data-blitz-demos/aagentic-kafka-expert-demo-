import importlib
import sys
from pathlib import Path

import pytest


scripts_path = Path(__file__).resolve().parents[1] / "scripts"
if str(scripts_path) not in sys.path:
    sys.path.insert(0, str(scripts_path))
config = importlib.import_module("agent_llm_config")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("", "openai"),
        (" OpenAI ", "openai"),
        ("hf", "huggingface"),
        ("open source", "ollama"),
        ("ollama", "ollama"),
    ],
)
def test_normalize_provider(raw: str, expected: str) -> None:
    assert config.normalize_provider(raw) == expected


def test_dedupe_models_removes_blanks_and_duplicates() -> None:
    assert config.dedupe_models([" gemma3:4b ", "", "gemma3:4b", "qwen3:8b"]) == [
        "gemma3:4b",
        "qwen3:8b",
    ]


def test_monitor_provider_is_independent() -> None:
    assert config.resolve_agent_provider(
        "monitor",
        default_provider="openai",
        monitor_provider="ollama",
        remediation_provider="huggingface",
    ) == "ollama"


def test_remediation_provider_is_independent() -> None:
    assert config.resolve_agent_provider(
        "remediation",
        default_provider="ollama",
        monitor_provider="ollama",
        remediation_provider="hf",
    ) == "huggingface"


def test_blank_role_provider_uses_default() -> None:
    assert config.resolve_agent_provider("monitor", default_provider="HuggingFace") == "huggingface"


def test_invalid_provider_role_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unsupported agent role"):
        config.resolve_agent_provider("judge", default_provider="openai")


def test_monitor_models_override_provider_defaults() -> None:
    assert config.resolve_agent_models(
        "monitor",
        provider_models=["gpt-5.2"],
        monitor_model="gemma3:4b",
        monitor_fallbacks=["kafka-qwen3:8b", "gemma3:4b"],
        remediation_model="gpt-5.2",
    ) == ["gemma3:4b", "kafka-qwen3:8b"]


def test_remediation_models_override_provider_defaults() -> None:
    assert config.resolve_agent_models(
        "remediation",
        provider_models=["gemma3:4b"],
        monitor_model="gemma3:4b",
        remediation_model="gpt-5.2",
        remediation_fallbacks=["gpt-4.1"],
    ) == ["gpt-5.2", "gpt-4.1"]


def test_blank_role_models_use_provider_defaults() -> None:
    assert config.resolve_agent_models(
        "monitor",
        provider_models=["gpt-5.2", "", "gpt-5.2", "gpt-4.1"],
    ) == ["gpt-5.2", "gpt-4.1"]


def test_invalid_model_role_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unsupported agent role"):
        config.resolve_agent_models("judge", provider_models=["gpt-5.2"])
