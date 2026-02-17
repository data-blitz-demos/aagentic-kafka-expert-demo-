#!/usr/bin/env python3
"""
Kafka Expert Demo
Copyright (c) 2026 Paul Harvener, Data-Blitz Inc
SPDX-License-Identifier: MIT
"""

import json
import os
from datetime import datetime, timezone
from typing import Any

from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewPartitions,
    NewTopic,
    RESOURCE_TOPIC,
)
from mcp.server.fastmcp import FastMCP


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")
KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS = float(os.getenv("KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS", "15"))
KAFKA_ADMIN_OPERATION_TIMEOUT_SECONDS = float(os.getenv("KAFKA_ADMIN_OPERATION_TIMEOUT_SECONDS", "15"))

mcp = FastMCP(
    "kafka-admin-tools",
    instructions=(
        "Kafka Admin tools for cluster metadata inspection and cluster mutations. "
        "These tools can change Kafka state (topics, partitions, configs, consumer groups)."
    ),
)


def _utc_now_iso() -> str:
    """Return current UTC timestamp as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _admin_client() -> AdminClient:
    """Build an AdminClient configured for the demo Kafka cluster."""
    return AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def _error_string(value: Any) -> str:
    """Convert an error-like object into a stable human-readable string."""
    if value is None:
        return ""
    return str(value)


def _parse_config_json(config_json: str) -> dict[str, str]:
    """Parse a JSON object string into a flat string-to-string config map."""
    raw = (config_json or "").strip()
    if not raw:
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("config_json must be a JSON object (for example: {\"retention.ms\":\"604800000\"}).")
    out: dict[str, str] = {}
    for key, value in parsed.items():
        out[str(key)] = str(value)
    return out


def _parse_csv_values(values_csv: str) -> list[str]:
    """Split comma-separated values into a trimmed non-empty list."""
    out: list[str] = []
    for raw in (values_csv or "").split(","):
        value = raw.strip()
        if value:
            out.append(value)
    return out


def _node_to_dict(node: Any) -> dict[str, Any]:
    """Serialize a Kafka node object into plain JSON-compatible fields."""
    return {
        "id": int(getattr(node, "id", -1)),
        "host": str(getattr(node, "host", "")),
        "port": int(getattr(node, "port", 0)),
        "rack": getattr(node, "rack", None),
    }


def _partition_to_dict(partition: Any) -> dict[str, Any]:
    """Serialize one topic partition metadata object into JSON-compatible fields."""
    return {
        "partition_id": int(getattr(partition, "id", -1)),
        "leader_id": int(getattr(partition, "leader", -1)),
        "replicas": [int(replica) for replica in list(getattr(partition, "replicas", []))],
        "isr": [int(replica) for replica in list(getattr(partition, "isrs", []))],
        "error": _error_string(getattr(partition, "error", None)),
    }


def _resolve_future_map(
    future_map: dict[Any, Any],
    key_to_name: callable,
    timeout_seconds: float = KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS + 10,
) -> dict[str, dict[str, Any]]:
    """Resolve AdminClient future maps and return per-resource success/error payloads."""
    out: dict[str, dict[str, Any]] = {}
    for key, fut in future_map.items():
        name = key_to_name(key)
        try:
            fut.result(timeout=timeout_seconds)
            out[name] = {"ok": True, "error": ""}
        except Exception as exc:  # noqa: BLE001
            out[name] = {"ok": False, "error": str(exc)}
    return out


@mcp.tool()
def kafka_admin_capabilities() -> dict[str, Any]:
    """List the Kafka Admin MCP tools available for reads and mutations."""
    return {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "queried_at_utc": _utc_now_iso(),
        "read_tools": [
            "kafka_describe_cluster",
            "kafka_list_topics",
            "kafka_describe_topic",
            "kafka_list_consumer_groups",
            "kafka_describe_consumer_groups",
            "kafka_describe_topic_configs",
        ],
        "mutation_tools": [
            "kafka_create_topic",
            "kafka_delete_topic",
            "kafka_create_partitions",
            "kafka_set_topic_config",
            "kafka_delete_topic_config_keys",
            "kafka_delete_consumer_groups",
        ],
    }


@mcp.tool()
def kafka_describe_cluster() -> dict[str, Any]:
    """Return cluster ID, controller, and broker node metadata."""
    admin = _admin_client()
    result = admin.describe_cluster(request_timeout=KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS).result(
        timeout=KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS + 10
    )
    nodes = [_node_to_dict(node) for node in list(getattr(result, "nodes", []))]
    controller = _node_to_dict(getattr(result, "controller", None)) if getattr(result, "controller", None) else None
    return {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "queried_at_utc": _utc_now_iso(),
        "cluster_id": str(getattr(result, "cluster_id", "")),
        "controller": controller,
        "nodes": nodes,
    }


@mcp.tool()
def kafka_list_topics(include_internal: bool = True, include_partition_details: bool = True) -> dict[str, Any]:
    """List topics with partition counts and optional full partition metadata."""
    admin = _admin_client()
    metadata = admin.list_topics(timeout=KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS)
    topics_out: list[dict[str, Any]] = []
    for topic_name, topic_meta in sorted(metadata.topics.items()):
        is_internal = topic_name.startswith("__")
        if not include_internal and is_internal:
            continue
        partition_map = dict(topic_meta.partitions)
        item: dict[str, Any] = {
            "topic": topic_name,
            "is_internal": is_internal,
            "partition_count": len(partition_map),
            "error": _error_string(getattr(topic_meta, "error", None)),
        }
        if include_partition_details:
            item["partitions"] = [
                _partition_to_dict(partition)
                for _, partition in sorted(partition_map.items(), key=lambda pair: int(pair[0]))
            ]
        topics_out.append(item)
    return {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "queried_at_utc": _utc_now_iso(),
        "topic_count": len(topics_out),
        "topics": topics_out,
    }


@mcp.tool()
def kafka_describe_topic(topic: str) -> dict[str, Any]:
    """Describe one topic in detail, including partition-level metadata."""
    topic_name = str(topic).strip()
    if not topic_name:
        raise ValueError("topic is required.")
    admin = _admin_client()
    metadata = admin.list_topics(topic=topic_name, timeout=KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS)
    topic_meta = metadata.topics.get(topic_name)
    if topic_meta is None:
        return {
            "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "queried_at_utc": _utc_now_iso(),
            "topic": topic_name,
            "exists": False,
            "error": "Topic not found.",
        }
    partitions = [
        _partition_to_dict(partition)
        for _, partition in sorted(topic_meta.partitions.items(), key=lambda pair: int(pair[0]))
    ]
    return {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "queried_at_utc": _utc_now_iso(),
        "topic": topic_name,
        "exists": True,
        "is_internal": topic_name.startswith("__"),
        "partition_count": len(partitions),
        "error": _error_string(getattr(topic_meta, "error", None)),
        "partitions": partitions,
    }


@mcp.tool()
def kafka_create_topic(
    topic: str,
    num_partitions: int = 3,
    replication_factor: int = 3,
    config_json: str = "{}",
) -> dict[str, Any]:
    """Create a topic with optional config overrides provided as a JSON object string."""
    topic_name = str(topic).strip()
    if not topic_name:
        raise ValueError("topic is required.")
    if num_partitions < 1:
        raise ValueError("num_partitions must be >= 1.")
    if replication_factor < 1:
        raise ValueError("replication_factor must be >= 1.")

    config = _parse_config_json(config_json)
    admin = _admin_client()
    if config:
        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=int(num_partitions),
            replication_factor=int(replication_factor),
            config=config,
        )
    else:
        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=int(num_partitions),
            replication_factor=int(replication_factor),
        )
    futures = admin.create_topics(
        [new_topic],
        request_timeout=KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS,
        operation_timeout=KAFKA_ADMIN_OPERATION_TIMEOUT_SECONDS,
    )
    results = _resolve_future_map(futures, key_to_name=lambda key: str(key))
    return {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "applied_at_utc": _utc_now_iso(),
        "operation": "create_topic",
        "requested": {
            "topic": topic_name,
            "num_partitions": int(num_partitions),
            "replication_factor": int(replication_factor),
            "config": config,
        },
        "results": results,
    }


@mcp.tool()
def kafka_delete_topic(topic: str) -> dict[str, Any]:
    """Delete one topic by name."""
    topic_name = str(topic).strip()
    if not topic_name:
        raise ValueError("topic is required.")
    admin = _admin_client()
    futures = admin.delete_topics(
        [topic_name],
        request_timeout=KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS,
        operation_timeout=KAFKA_ADMIN_OPERATION_TIMEOUT_SECONDS,
    )
    results = _resolve_future_map(futures, key_to_name=lambda key: str(key))
    return {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "applied_at_utc": _utc_now_iso(),
        "operation": "delete_topic",
        "requested": {"topic": topic_name},
        "results": results,
    }


@mcp.tool()
def kafka_create_partitions(topic: str, new_total_count: int) -> dict[str, Any]:
    """Increase partition count for one topic to a new total partition count."""
    topic_name = str(topic).strip()
    if not topic_name:
        raise ValueError("topic is required.")
    if new_total_count < 1:
        raise ValueError("new_total_count must be >= 1.")
    admin = _admin_client()
    spec = NewPartitions(topic_name, int(new_total_count))
    futures = admin.create_partitions(
        [spec],
        request_timeout=KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS,
        operation_timeout=KAFKA_ADMIN_OPERATION_TIMEOUT_SECONDS,
    )
    results = _resolve_future_map(futures, key_to_name=lambda key: str(key))
    return {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "applied_at_utc": _utc_now_iso(),
        "operation": "create_partitions",
        "requested": {"topic": topic_name, "new_total_count": int(new_total_count)},
        "results": results,
    }


@mcp.tool()
def kafka_list_consumer_groups(include_simple_groups: bool = True) -> dict[str, Any]:
    """List consumer groups and their high-level state/type."""
    admin = _admin_client()
    result = admin.list_consumer_groups(request_timeout=KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS).result(
        timeout=KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS + 10
    )
    valid_groups = list(getattr(result, "valid", []))
    groups_out: list[dict[str, Any]] = []
    for group in sorted(valid_groups, key=lambda item: str(item.group_id)):
        is_simple = bool(getattr(group, "is_simple_consumer_group", False))
        if not include_simple_groups and is_simple:
            continue
        groups_out.append(
            {
                "group_id": str(getattr(group, "group_id", "")),
                "state": getattr(getattr(group, "state", None), "name", str(getattr(group, "state", ""))),
                "type": getattr(getattr(group, "type", None), "name", str(getattr(group, "type", ""))),
                "is_simple_consumer_group": is_simple,
            }
        )
    errors = [str(err) for err in list(getattr(result, "errors", []))]
    return {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "queried_at_utc": _utc_now_iso(),
        "group_count": len(groups_out),
        "groups": groups_out,
        "errors": errors,
    }


@mcp.tool()
def kafka_describe_consumer_groups(group_ids_csv: str = "") -> dict[str, Any]:
    """Describe one or more consumer groups in detail, including member assignments."""
    admin = _admin_client()
    group_ids = _parse_csv_values(group_ids_csv)
    if not group_ids:
        listing = admin.list_consumer_groups(request_timeout=KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS).result(
            timeout=KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS + 10
        )
        group_ids = [str(getattr(group, "group_id", "")).strip() for group in list(getattr(listing, "valid", []))]
        group_ids = [group_id for group_id in group_ids if group_id]

    if not group_ids:
        return {
            "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "queried_at_utc": _utc_now_iso(),
            "group_count": 0,
            "groups": [],
            "note": "No consumer groups available to describe.",
        }

    future_map = admin.describe_consumer_groups(group_ids, request_timeout=KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS)
    groups_out: list[dict[str, Any]] = []
    for group_id in group_ids:
        fut = future_map[group_id]
        try:
            desc = fut.result(timeout=KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS + 10)
            members_out: list[dict[str, Any]] = []
            for member in list(getattr(desc, "members", [])):
                assigned = []
                assignment = getattr(member, "assignment", None)
                topic_partitions = list(getattr(assignment, "topic_partitions", [])) if assignment else []
                for tp in topic_partitions:
                    assigned.append(
                        {
                            "topic": str(getattr(tp, "topic", "")),
                            "partition": int(getattr(tp, "partition", -1)),
                            "offset": int(getattr(tp, "offset", -1)),
                        }
                    )
                members_out.append(
                    {
                        "member_id": str(getattr(member, "member_id", "")),
                        "client_id": str(getattr(member, "client_id", "")),
                        "host": str(getattr(member, "host", "")),
                        "group_instance_id": getattr(member, "group_instance_id", None),
                        "assigned_topic_partitions": assigned,
                    }
                )
            coordinator = getattr(desc, "coordinator", None)
            groups_out.append(
                {
                    "group_id": str(getattr(desc, "group_id", group_id)),
                    "state": getattr(getattr(desc, "state", None), "name", str(getattr(desc, "state", ""))),
                    "type": getattr(getattr(desc, "type", None), "name", str(getattr(desc, "type", ""))),
                    "is_simple_consumer_group": bool(getattr(desc, "is_simple_consumer_group", False)),
                    "partition_assignor": str(getattr(desc, "partition_assignor", "")),
                    "coordinator": _node_to_dict(coordinator) if coordinator else None,
                    "member_count": len(members_out),
                    "members": members_out,
                    "error": "",
                }
            )
        except Exception as exc:  # noqa: BLE001
            groups_out.append({"group_id": group_id, "error": str(exc)})

    return {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "queried_at_utc": _utc_now_iso(),
        "group_count": len(groups_out),
        "groups": groups_out,
    }


@mcp.tool()
def kafka_delete_consumer_groups(group_ids_csv: str) -> dict[str, Any]:
    """Delete one or more consumer groups (comma-separated group IDs)."""
    group_ids = _parse_csv_values(group_ids_csv)
    if not group_ids:
        raise ValueError("group_ids_csv is required (comma-separated consumer group IDs).")
    admin = _admin_client()
    future_map = admin.delete_consumer_groups(group_ids, request_timeout=KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS)
    results = _resolve_future_map(future_map, key_to_name=lambda key: str(key))
    return {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "applied_at_utc": _utc_now_iso(),
        "operation": "delete_consumer_groups",
        "requested": {"group_ids": group_ids},
        "results": results,
    }


@mcp.tool()
def kafka_describe_topic_configs(topic: str) -> dict[str, Any]:
    """Describe topic-level configuration entries for one topic."""
    topic_name = str(topic).strip()
    if not topic_name:
        raise ValueError("topic is required.")
    admin = _admin_client()
    resource = ConfigResource(RESOURCE_TOPIC, topic_name)
    future_map = admin.describe_configs([resource], request_timeout=KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS)
    fut = future_map[resource]
    config_entries = fut.result(timeout=KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS + 10)
    config_out: dict[str, Any] = {}
    for key, entry in sorted(config_entries.items()):
        config_out[str(key)] = {
            "value": None if getattr(entry, "is_sensitive", False) else str(getattr(entry, "value", "")),
            "is_default": bool(getattr(entry, "is_default", False)),
            "is_read_only": bool(getattr(entry, "is_read_only", False)),
            "is_sensitive": bool(getattr(entry, "is_sensitive", False)),
            "source": getattr(getattr(entry, "source", None), "name", str(getattr(entry, "source", ""))),
        }
    return {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "queried_at_utc": _utc_now_iso(),
        "topic": topic_name,
        "config_count": len(config_out),
        "configs": config_out,
    }


@mcp.tool()
def kafka_set_topic_config(topic: str, config_json: str) -> dict[str, Any]:
    """Set topic config keys using incremental config SET operations."""
    topic_name = str(topic).strip()
    if not topic_name:
        raise ValueError("topic is required.")
    config_map = _parse_config_json(config_json)
    if not config_map:
        raise ValueError("config_json must include at least one key/value.")

    entries = [
        ConfigEntry(key, value, incremental_operation=AlterConfigOpType.SET)
        for key, value in sorted(config_map.items())
    ]
    resource = ConfigResource(RESOURCE_TOPIC, topic_name, incremental_configs=entries)
    admin = _admin_client()
    future_map = admin.incremental_alter_configs([resource], request_timeout=KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS)
    results = _resolve_future_map(future_map, key_to_name=lambda key: str(getattr(key, "name", topic_name)))
    return {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "applied_at_utc": _utc_now_iso(),
        "operation": "set_topic_config",
        "requested": {"topic": topic_name, "config": config_map},
        "results": results,
    }


@mcp.tool()
def kafka_delete_topic_config_keys(topic: str, config_keys_csv: str) -> dict[str, Any]:
    """Delete topic config keys using incremental config DELETE operations."""
    topic_name = str(topic).strip()
    if not topic_name:
        raise ValueError("topic is required.")
    keys = _parse_csv_values(config_keys_csv)
    if not keys:
        raise ValueError("config_keys_csv is required (comma-separated topic config keys).")

    entries = [ConfigEntry(key, None, incremental_operation=AlterConfigOpType.DELETE) for key in sorted(keys)]
    resource = ConfigResource(RESOURCE_TOPIC, topic_name, incremental_configs=entries)
    admin = _admin_client()
    future_map = admin.incremental_alter_configs([resource], request_timeout=KAFKA_ADMIN_REQUEST_TIMEOUT_SECONDS)
    results = _resolve_future_map(future_map, key_to_name=lambda key: str(getattr(key, "name", topic_name)))
    return {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "applied_at_utc": _utc_now_iso(),
        "operation": "delete_topic_config_keys",
        "requested": {"topic": topic_name, "config_keys": keys},
        "results": results,
    }


if __name__ == "__main__":
    mcp.run(transport=os.getenv("MCP_TRANSPORT", "stdio"))
