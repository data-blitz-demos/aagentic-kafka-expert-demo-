#!/usr/bin/env python3
"""Isolated Graph RAG extraction call, designed to be killed on a hard deadline."""

import json
import os
import sys

from langchain_openai import ChatOpenAI


def main() -> int:
    payload = json.loads(sys.stdin.read() or "{}")
    provider = str(payload.get("provider", "openai")).strip().lower()
    kwargs = {
        "model": str(payload["model"]),
        "temperature": float(payload.get("temperature", 0)),
        "api_key": os.getenv("GRAPH_RAG_WORKER_API_KEY", "") or "not-required",
    }
    base_url = str(payload.get("base_url", "")).strip()
    if base_url:
        kwargs["base_url"] = base_url.rstrip("/")
    if provider == "ollama":
        kwargs["reasoning_effort"] = "none"
    response = ChatOpenAI(**kwargs).bind(
        max_tokens=int(payload.get("max_tokens", 320)),
        response_format={"type": "json_object"},
    ).invoke(str(payload["prompt"]))
    content = response.content
    if not isinstance(content, str):
        content = json.dumps(content, ensure_ascii=False)
    sys.stdout.write(json.dumps({"content": content}, ensure_ascii=False))
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised by the container subprocess
    raise SystemExit(main())
