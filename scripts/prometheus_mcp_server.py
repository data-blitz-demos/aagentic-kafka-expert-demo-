#!/usr/bin/env python3
"""
Kafka Expert Demo
Copyright (c) 2026 Paul Harvener, Data-Blitz Inc
SPDX-License-Identifier: MIT
"""

import os
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

from mcp.server.fastmcp import FastMCP


PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090").rstrip("/")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081").rstrip("/")

mcp = FastMCP(
    "prometheus-kafka-tools",
    instructions=(
        "Prometheus tools for Kafka cluster diagnostics. "
        "Use these tools to query broker health and Kafka JMX state."
    ),
)

TOPIC_ACTIVITY_BASE = (
    'label_replace('
    'kafka_jmx_attribute_value{domain="kafka.server",'
    'mbean=~"kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=.*",'
    'attribute="OneMinuteRate"},'
    '"topic","$1","mbean",".*topic=([^,]+).*$"'
    ")"
)

TOPIC_INVENTORY_BASE = (
    'label_replace('
    'kafka_jmx_attribute_value{domain="kafka.log",'
    'mbean=~"kafka.log:type=Log,name=Size,topic=.*,partition=.*",'
    'attribute="Value"},'
    '"topic","$1","mbean",".*topic=([^,]+),partition=.*$"'
    ")"
)

PARTITION_INVENTORY_BASE = (
    'label_replace('
    'label_replace('
    'kafka_jmx_attribute_value{domain="kafka.log",'
    'mbean=~"kafka.log:type=Log,name=Size,topic=.*,partition=.*",'
    'attribute="Value"},'
    '"topic","$1","mbean",".*topic=([^,]+),partition=.*$"),'
    '"partition","$1","mbean",".*partition=([0-9]+).*$"'
    ")"
)


def _utc_now_iso() -> str:
    """Return current UTC timestamp as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _prom_get(path: str, params: dict[str, str]) -> dict[str, Any]:
    """Execute a Prometheus HTTP GET and return metadata plus raw JSON response."""
    query = urllib.parse.urlencode(params)
    url = f"{PROMETHEUS_URL}{path}?{query}"
    req = urllib.request.Request(url=url, method="GET")
    with urllib.request.urlopen(req, timeout=20) as response:
        payload = response.read().decode("utf-8")
    return {"queried_at_utc": _utc_now_iso(), "url": url, "raw_json": payload}


def _http_get_json(url: str) -> Any:
    """Execute an HTTP GET and parse JSON body."""
    req = urllib.request.Request(url=url, method="GET")
    with urllib.request.urlopen(req, timeout=20) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


@mcp.tool()
def prometheus_query(query: str, time: str | None = None) -> dict[str, Any]:
    """Run an instant PromQL query. Returns raw JSON payload from Prometheus."""
    params = {"query": query}
    if time:
        params["time"] = time
    return _prom_get("/api/v1/query", params)


@mcp.tool()
def prometheus_range_query(query: str, start: str, end: str, step: str = "30s") -> dict[str, Any]:
    """Run a range PromQL query. start/end must be RFC3339 or Unix timestamps."""
    return _prom_get(
        "/api/v1/query_range",
        {
            "query": query,
            "start": start,
            "end": end,
            "step": step,
        },
    )


@mcp.tool()
def kafka_cluster_state_snapshot() -> dict[str, Any]:
    """Collect a broad Kafka cluster state snapshot from Prometheus for diagnosis."""
    queries = {
        "prometheus_target_up": 'up{job="kafka_jmx"}',
        "node_exporter_target_up": 'up{job="node_exporter"}',
        "broker_poll_success": "kafka_jmx_exporter_last_poll_success",
        "broker_last_error": "kafka_jmx_exporter_last_error",
        "broker_poll_duration_seconds": "kafka_jmx_exporter_poll_duration_seconds",
        "broker_numeric_attributes": "kafka_jmx_exporter_numeric_attributes",
        "node_disk_used_percent": (
            'max by (instance, mountpoint) (100 * (1 - ('
            'node_filesystem_avail_bytes{job="node_exporter",fstype!~"tmpfs|overlay|squashfs|nsfs|proc|sysfs|cgroup2fs|tracefs|ramfs"} / '
            'node_filesystem_size_bytes{job="node_exporter",fstype!~"tmpfs|overlay|squashfs|nsfs|proc|sysfs|cgroup2fs|tracefs|ramfs"}'
            ")))"
        ),
        "jvm_gc_time_ms_per_second": (
            'sum by (broker) (rate(kafka_jmx_attribute_value{domain="java.lang",'
            'mbean=~"java.lang:name=G1 (Young|Old) Generation,type=GarbageCollector",'
            'attribute="CollectionTime"}[5m]))'
        ),
        "produce_request_p99_ms": (
            'max by (broker) (kafka_jmx_attribute_value{domain="kafka.network",'
            'mbean=~"kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce.*",'
            'attribute="99thPercentile"})'
        ),
        "fetch_consumer_request_p99_ms": (
            'max by (broker) (kafka_jmx_attribute_value{domain="kafka.network",'
            'mbean=~"kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer.*",'
            'attribute="99thPercentile"})'
        ),
        "active_controller_count": (
            'sum by (broker) (kafka_jmx_attribute_value{domain="kafka.controller",mbean=~"kafka.controller:type=KafkaController,name=ActiveControllerCount.*",attribute="Value"})'
        ),
        "offline_partitions_count": (
            'sum by (broker) (kafka_jmx_attribute_value{domain="kafka.controller",mbean=~"kafka.controller:type=KafkaController,name=OfflinePartitionsCount.*",attribute="Value"})'
        ),
        "under_replicated_partitions": (
            'sum by (broker) (kafka_jmx_attribute_value{domain="kafka.server",mbean=~"kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions.*",attribute="Value"})'
        ),
        "isr_shrinks_per_sec": (
            'max by (broker) (kafka_jmx_attribute_value{domain="kafka.server",'
            'mbean=~"kafka.server:type=ReplicaManager,name=IsrShrinksPerSec.*",attribute="OneMinuteRate"})'
        ),
        "preferred_replica_imbalance_count": (
            'max by (broker) (kafka_jmx_attribute_value{domain="kafka.controller",'
            'mbean=~"kafka.controller:type=KafkaController,name=PreferredReplicaImbalanceCount.*",attribute="Value"})'
        ),
        "request_queue_size": (
            'sum by (broker) (kafka_jmx_attribute_value{domain="kafka.network",mbean=~"kafka.network:type=RequestChannel,name=RequestQueueSize.*",attribute="Value"})'
        ),
        "produce_request_rate_per_broker": (
            'sum by (broker) (kafka_jmx_attribute_value{domain="kafka.network",mbean=~"kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Produce.*",attribute="OneMinuteRate"})'
        ),
        "fetch_consumer_rate_per_broker": (
            'sum by (broker) (kafka_jmx_attribute_value{domain="kafka.network",mbean=~"kafka.network:type=RequestMetrics,name=RequestsPerSec,request=FetchConsumer.*",attribute="OneMinuteRate"})'
        ),
        "topic_inventory_count": f"count(count by (topic) ({TOPIC_INVENTORY_BASE}))",
        "topic_inventory_topics": f"count by (topic) (count by (topic, partition) ({PARTITION_INVENTORY_BASE}))",
        "topic_activity_count": f"count(count by (topic) ({TOPIC_ACTIVITY_BASE}))",
        "topic_activity_topics": f"sum by (topic) ({TOPIC_ACTIVITY_BASE})",
        "partition_inventory_total": f"count(count by (topic, partition) ({PARTITION_INVENTORY_BASE}))",
        "partition_inventory_by_topic": f"count by (topic) (count by (topic, partition) ({PARTITION_INVENTORY_BASE}))",
        "consumer_group_count_total": (
            'sum(kafka_jmx_attribute_value{domain="kafka.coordinator.group",'
            'mbean=~"kafka.coordinator.group:type=GroupMetadataManager,name=NumGroups$",attribute="Value"})'
        ),
        "consumer_group_stable": (
            'sum(kafka_jmx_attribute_value{domain="kafka.coordinator.group",'
            'mbean=~"kafka.coordinator.group:type=GroupMetadataManager,name=NumGroupsStable$",attribute="Value"})'
        ),
        "consumer_group_empty": (
            'sum(kafka_jmx_attribute_value{domain="kafka.coordinator.group",'
            'mbean=~"kafka.coordinator.group:type=GroupMetadataManager,name=NumGroupsEmpty$",attribute="Value"})'
        ),
        "consumer_group_preparing_rebalance": (
            'sum(kafka_jmx_attribute_value{domain="kafka.coordinator.group",'
            'mbean=~"kafka.coordinator.group:type=GroupMetadataManager,name=NumGroupsPreparingRebalance$",'
            'attribute="Value"})'
        ),
        "consumer_group_completing_rebalance": (
            'sum(kafka_jmx_attribute_value{domain="kafka.coordinator.group",'
            'mbean=~"kafka.coordinator.group:type=GroupMetadataManager,name=NumGroupsCompletingRebalance$",'
            'attribute="Value"})'
        ),
    }

    results: dict[str, Any] = {}
    for name, promql in queries.items():
        results[name] = _prom_get("/api/v1/query", {"query": promql})

    return {
        "prometheus_url": PROMETHEUS_URL,
        "collected_at_utc": _utc_now_iso(),
        "queries": queries,
        "results": results,
    }


@mcp.tool()
def kafka_topic_inventory() -> dict[str, Any]:
    """Return Kafka topic inventory from log metrics plus active-topic visibility from traffic metrics."""
    queries = {
        "inventory_topic_count": f"count(count by (topic) ({TOPIC_INVENTORY_BASE}))",
        "inventory_topics": f"count by (topic) (count by (topic, partition) ({PARTITION_INVENTORY_BASE}))",
        "active_topic_count": f"count(count by (topic) ({TOPIC_ACTIVITY_BASE}))",
        "active_topics": f"sum by (topic) ({TOPIC_ACTIVITY_BASE})",
    }

    results: dict[str, Any] = {}
    for name, promql in queries.items():
        results[name] = _prom_get("/api/v1/query", {"query": promql})

    return {
        "prometheus_url": PROMETHEUS_URL,
        "collected_at_utc": _utc_now_iso(),
        "queries": queries,
        "results": results,
    }


@mcp.tool()
def kafka_partition_inventory() -> dict[str, Any]:
    """Return partition inventory totals and per-topic partition counts from Kafka log metrics."""
    queries = {
        "partition_inventory_total": f"count(count by (topic, partition) ({PARTITION_INVENTORY_BASE}))",
        "partition_inventory_by_topic": f"count by (topic) (count by (topic, partition) ({PARTITION_INVENTORY_BASE}))",
    }

    results: dict[str, Any] = {}
    for name, promql in queries.items():
        results[name] = _prom_get("/api/v1/query", {"query": promql})

    return {
        "prometheus_url": PROMETHEUS_URL,
        "collected_at_utc": _utc_now_iso(),
        "queries": queries,
        "results": results,
    }


@mcp.tool()
def kafka_consumer_group_summary() -> dict[str, Any]:
    """Return consumer-group totals and rebalance state counts from coordinator metrics."""
    queries = {
        "consumer_group_count_total": (
            'sum(kafka_jmx_attribute_value{domain="kafka.coordinator.group",'
            'mbean=~"kafka.coordinator.group:type=GroupMetadataManager,name=NumGroups$",attribute="Value"})'
        ),
        "consumer_group_stable": (
            'sum(kafka_jmx_attribute_value{domain="kafka.coordinator.group",'
            'mbean=~"kafka.coordinator.group:type=GroupMetadataManager,name=NumGroupsStable$",attribute="Value"})'
        ),
        "consumer_group_empty": (
            'sum(kafka_jmx_attribute_value{domain="kafka.coordinator.group",'
            'mbean=~"kafka.coordinator.group:type=GroupMetadataManager,name=NumGroupsEmpty$",attribute="Value"})'
        ),
        "consumer_group_preparing_rebalance": (
            'sum(kafka_jmx_attribute_value{domain="kafka.coordinator.group",'
            'mbean=~"kafka.coordinator.group:type=GroupMetadataManager,name=NumGroupsPreparingRebalance$",'
            'attribute="Value"})'
        ),
        "consumer_group_completing_rebalance": (
            'sum(kafka_jmx_attribute_value{domain="kafka.coordinator.group",'
            'mbean=~"kafka.coordinator.group:type=GroupMetadataManager,name=NumGroupsCompletingRebalance$",'
            'attribute="Value"})'
        ),
    }

    results: dict[str, Any] = {}
    for name, promql in queries.items():
        results[name] = _prom_get("/api/v1/query", {"query": promql})

    return {
        "prometheus_url": PROMETHEUS_URL,
        "collected_at_utc": _utc_now_iso(),
        "queries": queries,
        "results": results,
    }


@mcp.tool()
def schema_registry_inventory(max_subjects: int = 50) -> dict[str, Any]:
    """Return Schema Registry status, subject list, and latest contract metadata per subject."""
    base = SCHEMA_REGISTRY_URL.rstrip("/")
    subjects: list[str] = _http_get_json(f"{base}/subjects")
    global_config = _http_get_json(f"{base}/config")

    details: list[dict[str, Any]] = []
    for subject in sorted(subjects)[:max_subjects]:
        encoded = urllib.parse.quote(subject, safe="")
        versions = _http_get_json(f"{base}/subjects/{encoded}/versions")
        latest = _http_get_json(f"{base}/subjects/{encoded}/versions/latest")
        schema_text = str(latest.get("schema", ""))
        schema_type = str(latest.get("schemaType", "AVRO"))
        field_count = None
        if schema_type.upper() == "AVRO" and schema_text:
            try:
                schema_obj = json.loads(schema_text)
                fields = schema_obj.get("fields", []) if isinstance(schema_obj, dict) else []
                field_count = len(fields) if isinstance(fields, list) else None
            except Exception:
                field_count = None
        details.append(
            {
                "subject": subject,
                "versions": versions,
                "latest_version": latest.get("version"),
                "schema_id": latest.get("id"),
                "schema_type": schema_type,
                "field_count": field_count,
            }
        )

    return {
        "schema_registry_url": SCHEMA_REGISTRY_URL,
        "collected_at_utc": _utc_now_iso(),
        "subject_count": len(subjects),
        "subjects": sorted(subjects),
        "global_config": global_config,
        "subject_details": details,
        "max_subjects": max_subjects,
        "truncated": len(subjects) > max_subjects,
    }


if __name__ == "__main__":
    mcp.run(transport=os.getenv("MCP_TRANSPORT", "stdio"))
