#!/usr/bin/env python3
"""MCP tools backed by Confluent Kafka REST Proxy."""

__author__ = "Paul Harvener"
__company__ = "Data-Blitz Inc."

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

from mcp.server.fastmcp import FastMCP


KAFKA_REST_PROXY_URL = os.getenv("KAFKA_REST_PROXY_URL", "http://kafka-rest-proxy:8082").rstrip("/")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("KAFKA_REST_REQUEST_TIMEOUT_SECONDS", "15"))

mcp = FastMCP(
    "kafka-rest-proxy-tools",
    instructions=(
        "Kafka REST Proxy tools for topic metadata, partition inspection, and JSON message production. "
        "Use live Kafka Admin and Prometheus tools for authoritative cluster state."
    ),
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _request(path: str, *, method: str = "GET", payload: Any = None, content_type: str = "application/json") -> Any:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{KAFKA_REST_PROXY_URL}/{path.lstrip('/')}",
        data=body,
        method=method,
        headers={"Accept": "application/json", "Content-Type": content_type},
    )
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Kafka REST Proxy HTTP {exc.code}: {detail[:500]}") from exc


@mcp.tool()
def kafka_rest_proxy_health() -> dict[str, Any]:
    """Check REST Proxy reachability and return its configured endpoint."""
    topics = _request("topics")
    return {
        "ok": True,
        "rest_proxy_url": KAFKA_REST_PROXY_URL,
        "topic_count": len(topics) if isinstance(topics, list) else None,
        "checked_at_utc": _utc_now_iso(),
    }


@mcp.tool()
def kafka_rest_list_topics() -> dict[str, Any]:
    """List topics visible through Kafka REST Proxy."""
    topics = _request("topics")
    return {"rest_proxy_url": KAFKA_REST_PROXY_URL, "topics": topics if isinstance(topics, list) else []}


@mcp.tool()
def kafka_rest_topic_metadata(topic: str) -> dict[str, Any]:
    """Return REST Proxy metadata for one topic, including partition endpoints."""
    topic_name = str(topic or "").strip()
    if not topic_name:
        raise ValueError("topic is required")
    encoded = urllib.parse.quote(topic_name, safe="")
    metadata = _request(f"topics/{encoded}")
    partitions = _request(f"topics/{encoded}/partitions")
    return {
        "rest_proxy_url": KAFKA_REST_PROXY_URL,
        "topic": topic_name,
        "metadata": metadata,
        "partitions": partitions,
    }


@mcp.tool()
def kafka_rest_produce_json(topic: str, value_json: str, key: str = "") -> dict[str, Any]:
    """Produce one JSON value through REST Proxy (Doer authorization required by the agent runtime)."""
    topic_name = str(topic or "").strip()
    if not topic_name:
        raise ValueError("topic is required")
    value = json.loads(value_json)
    record: dict[str, Any] = {"value": value}
    if str(key).strip():
        record["key"] = str(key)
    encoded = urllib.parse.quote(topic_name, safe="")
    result = _request(
        f"topics/{encoded}",
        method="POST",
        payload={"records": [record]},
        content_type="application/vnd.kafka.json.v2+json",
    )
    return {"rest_proxy_url": KAFKA_REST_PROXY_URL, "topic": topic_name, "result": result}


if __name__ == "__main__":
    mcp.run(transport=os.getenv("MCP_TRANSPORT", "stdio"))
