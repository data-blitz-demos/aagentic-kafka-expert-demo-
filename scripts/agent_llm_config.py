"""Pure configuration helpers for independent Monitor and Doer LLM runtimes."""

from collections.abc import Iterable


SUPPORTED_AGENT_ROLES = {"monitor", "remediation"}
PROVIDER_ALIASES = {"hf": "huggingface", "open source": "ollama"}


def normalize_provider(provider: str) -> str:
    """Normalize a configured provider name."""
    normalized = (provider or "openai").strip().lower()
    return PROVIDER_ALIASES.get(normalized, normalized)


def dedupe_models(models: Iterable[str]) -> list[str]:
    """Remove blank and duplicate model names while preserving order."""
    seen: set[str] = set()
    result: list[str] = []
    for model in models:
        cleaned = str(model).strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            result.append(cleaned)
    return result


def resolve_agent_provider(
    role: str,
    *,
    default_provider: str,
    monitor_provider: str = "",
    remediation_provider: str = "",
) -> str:
    """Resolve an agent provider without coupling the two assignments."""
    if role not in SUPPORTED_AGENT_ROLES:
        raise ValueError(f"Unsupported agent role: {role}")
    role_provider = monitor_provider if role == "monitor" else remediation_provider
    return normalize_provider(role_provider or default_provider)


def resolve_agent_models(
    role: str,
    *,
    provider_models: Iterable[str],
    monitor_model: str = "",
    monitor_fallbacks: Iterable[str] = (),
    remediation_model: str = "",
    remediation_fallbacks: Iterable[str] = (),
) -> list[str]:
    """Resolve role-specific models, falling back to the provider defaults."""
    if role not in SUPPORTED_AGENT_ROLES:
        raise ValueError(f"Unsupported agent role: {role}")
    configured = (
        [monitor_model, *monitor_fallbacks]
        if role == "monitor"
        else [remediation_model, *remediation_fallbacks]
    )
    role_models = dedupe_models(configured)
    return role_models or dedupe_models(provider_models)
