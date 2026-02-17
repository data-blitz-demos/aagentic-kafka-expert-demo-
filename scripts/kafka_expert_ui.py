#!/usr/bin/env python3
"""
Kafka Expert Demo
Copyright (c) 2026 Paul Harvener, Data-Blitz Inc
SPDX-License-Identifier: MIT
"""

import asyncio
import io
import json
import os
import re
import threading
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from langchain_core.messages import AIMessage
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
from neo4j import GraphDatabase
from pypdf import PdfReader


load_dotenv()

UI_PORT = int(os.getenv("KAFKA_EXPERT_UI_PORT", "5052"))
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092"
)
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.3")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "").strip()
OPENAI_MODEL_FALLBACKS = [
    model.strip()
    for model in os.getenv("OPENAI_MODEL_FALLBACKS", "gpt-5.2,gpt-4.1").split(",")
    if model.strip()
]
KAFKA_UI_PUBLIC_URL = os.getenv("KAFKA_UI_PUBLIC_URL", "http://localhost:8080").strip()
KAFKA_PRODUCER_UI_PUBLIC_URL = os.getenv(
    "KAFKA_PRODUCER_UI_PUBLIC_URL", "http://localhost:5050"
).strip()
KAFKA_CONSUMER_UI_PUBLIC_URL = os.getenv(
    "KAFKA_CONSUMER_UI_PUBLIC_URL", "http://localhost:5051"
).strip()
GRAFANA_PUBLIC_URL = os.getenv(
    "GRAFANA_PUBLIC_URL", "http://localhost:3000/d/kafka-jmx-overview/kafka-jmx-overview?orgId=1&theme=dark"
).strip()
NEO4J_BROWSER_PUBLIC_URL = os.getenv(
    "NEO4J_BROWSER_PUBLIC_URL", "http://localhost:7474/browser/"
).strip()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687").strip()
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j").strip()
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "").strip()
NEO4J_AUTH = os.getenv("NEO4J_AUTH", "").strip()
NEO4J_AUTH_DISABLED = NEO4J_AUTH.lower() == "none"
NEO4J_BROWSER_LOGIN_HINT = (
    "Auto-login mode is enabled for demo use: no username/password required."
    if NEO4J_AUTH_DISABLED
    else "Login with `NEO4J_USER` and `NEO4J_PASSWORD` from your `.env`."
)
GRAPH_RAG_MAX_CHUNKS = int(os.getenv("GRAPH_RAG_MAX_CHUNKS", "20"))

KAFKA_ONLY_MESSAGE = (
    "1. [WARN] I can only answer questions about this Kafka cluster and its Prometheus telemetry.\n"
    "2. [GOOD] Next step: ask a Kafka-specific question about brokers, topics, partitions, consumer groups, "
    "Schema Registry subjects, data contracts, lag, throughput, or health."
)

KAFKA_TERMS = (
    "kafka",
    "broker",
    "topic",
    "partition",
    "replica",
    "isr",
    "leader",
    "prometheus",
    "consumer",
    "producer",
    "consumer group",
    "subjects",
    "subject",
    "schema",
    "data contract",
    "contract",
    "avro",
    "schema registry",
    "cluster",
    "lag",
    "throughput",
    "latency",
    "jmx",
)

SYSTEM_PROMPT = """You are Kafka Expert, a Kafka-only operations agent.
Rules:
1) You must answer only Kafka cluster state, Kafka metrics, Kafka brokers, topics, partitions, replicas, producers, consumers, Schema Registry, or Prometheus telemetry for Kafka.
2) If a question is not Kafka-related, refuse with a short response.
3) Tool resources available:
   - Prometheus MCP telemetry tools (cluster state and health metrics).
   - Kafka Admin MCP tools (cluster metadata and cluster mutations).
   - Schema Registry MCP inventory tool.
4) For cluster state analysis, call Prometheus tools first, especially kafka_cluster_state_snapshot.
5) For topic inventory/count questions, call kafka_topic_inventory first.
6) For partition inventory questions, call kafka_partition_inventory.
7) For consumer-group questions, call kafka_consumer_group_summary and/or Kafka Admin group tools.
8) For Schema Registry, subjects, and data contract questions, call schema_registry_inventory.
9) For admin metadata questions (exact topics/partitions/groups), prefer Kafka Admin MCP tools over inferred telemetry.
10) For cluster mutations (create/delete topic, increase partitions, alter topic configs, delete groups):
    - Only execute mutation tools when the user explicitly asks for a change.
    - Do not run destructive operations implicitly.
    - After each mutation, run a verification read tool and report the post-change state.
11) Never delete internal topics unless the user explicitly names and requests deletion.
12) Ground conclusions in queried metrics and cited values.
13) Keep responses concise and operational: status, evidence, risks, next actions.
14) Output format is mandatory:
   - Use only 1-based numbered bullets (`1.`, `2.`, ...).
   - No markdown headings.
   - No paragraph prose outside bullets.
   - Keep 4-6 bullets, one short sentence per bullet.
   - Use plain language for non-expert readers.
   - Prefix each bullet with one severity tag: `[GOOD]`, `[WARN]`, or `[BAD]`.
   - For every `[WARN]` or `[BAD]` bullet, include a short `Fix:` action in the same bullet.
"""

CLUSTER_STATE_PROMPT = """Query Prometheus for a full Kafka cluster state assessment.
Required:
- Use kafka_cluster_state_snapshot first.
- If needed, run additional prometheus_query calls for clarification.
- Determine one overall status: HEALTHY, DEGRADED, or UNHEALTHY.
- Return:
  1. Overall status
  2. Broker-by-broker state
  3. Key evidence metrics
  4. Risks or gaps
  5. Recommended next actions
Formatting constraints:
- Must be 1-based numbered bullets only.
- Keep each bullet concise and easy to understand.
- Prefix each bullet with `[GOOD]`, `[WARN]`, or `[BAD]`.
- If a bullet is `[WARN]` or `[BAD]`, include `Fix:` with what to do next.
"""

AUTO_FIX_PROMPT_TEMPLATE = """A user clicked a `Fix:` action in Kafka Expert UI.

Issue line:
{issue_line}

Objective:
- Attempt to remediate this issue now using all available tools:
  - Prometheus MCP telemetry tools
  - Kafka Admin MCP tools (including mutation tools)
  - Schema Registry MCP inventory tools

Rules:
1) You have explicit user authorization to attempt this fix.
2) If mutation is required, perform the smallest safe change and then verify with read tools.
3) Report concrete evidence after the change attempt (metrics/metadata values).
4) If full remediation is not possible with available tools/access, explain exactly what remains.
5) Output only 1-based numbered bullets with `[GOOD]`, `[WARN]`, `[BAD]`.
6) Include `Fix:` only for unresolved warning/bad bullets.
7) Keep response concise and operational.
"""

TOPIC_INVENTORY_SERIES_PROMQL = (
    'count by (topic) (label_replace('
    'kafka_jmx_attribute_value{domain="kafka.log",'
    'mbean=~"kafka.log:type=Log,name=Size,topic=.*,partition=.*",'
    'attribute="Value"},'
    '"topic","$1","mbean",".*topic=([^,]+),partition=.*$"'
    "))"
)

TOPIC_ACTIVITY_SERIES_PROMQL = (
    'sum by (topic) (label_replace('
    'kafka_jmx_attribute_value{domain="kafka.server",'
    'mbean=~"kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=.*",'
    'attribute="OneMinuteRate"},'
    '"topic","$1","mbean",".*topic=([^,]+).*$"'
    "))"
)


def utc_now_iso() -> str:
    """Return current UTC time in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def is_kafka_question(text: str) -> bool:
    """Heuristically determine whether a user prompt is Kafka-related."""
    normalized = text.lower()
    return any(term in normalized for term in KAFKA_TERMS)


def is_topic_inventory_question(text: str) -> bool:
    """Detect user prompts asking for topic count/list/inventory completeness."""
    normalized = text.lower()
    if "topic" not in normalized:
        return False
    topic_inventory_terms = (
        "how many",
        "count",
        "inventory",
        "list",
        "which topic",
        "what topic",
        "full inventory",
    )
    return any(term in normalized for term in topic_inventory_terms)


def is_cluster_inventory_question(text: str) -> bool:
    """Detect prompts asking for full inventory across topics, partitions, and consumer groups."""
    normalized = text.lower()
    inventory_terms = (
        "inventory",
        "collect",
        "collection",
        "list",
        "count",
        "how many",
        "everything",
    )
    entity_terms = (
        "topic",
        "topics",
        "partition",
        "partitions",
        "consumer group",
        "consumer groups",
    )
    has_inventory_intent = any(term in normalized for term in inventory_terms)
    if not has_inventory_intent:
        # Avoid false positives like "called" containing "all".
        has_inventory_intent = bool(
            re.search(r"\ball\s+(topics?|partitions?|consumer\s+groups?)\b", normalized)
        )
    has_entity = any(term in normalized for term in entity_terms)
    return has_inventory_intent and has_entity


def is_schema_registry_question(text: str) -> bool:
    """Detect prompts asking about Schema Registry, subjects, or data contracts."""
    normalized = text.lower()
    schema_terms = (
        "schema registry",
        "subject",
        "subjects",
        "data contract",
        "contract",
        "compatibility",
        "avro",
    )
    if any(term in normalized for term in schema_terms):
        return True
    return "schema" in normalized and ("kafka" in normalized or "subject" in normalized)


def format_backtick_list(items: list[str], max_items: int = 10) -> str:
    """Render list items in backticks with truncation for readability."""
    if not items:
        return "none"
    sorted_items = sorted(items)
    shown = sorted_items[:max_items]
    rendered = ", ".join(f"`{item}`" for item in shown)
    remaining = len(sorted_items) - len(shown)
    if remaining > 0:
        rendered += f", ... (+{remaining} more)"
    return rendered


def fetch_json(url: str) -> Any:
    """Fetch one HTTP JSON payload using a short timeout."""
    req = urllib.request.Request(url=url, method="GET")
    with urllib.request.urlopen(req, timeout=20) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def build_cluster_inventory_answer() -> str:
    """Build deterministic inventory response for topics, partitions, and consumer groups."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(timeout=15)

    topic_partition_counts: dict[str, int] = {}
    for topic_name, topic_meta in metadata.topics.items():
        if topic_meta.error is not None:
            continue
        topic_partition_counts[topic_name] = len(topic_meta.partitions)

    topics = sorted(topic_partition_counts.keys())
    total_partitions = sum(topic_partition_counts.values())
    partitions_per_topic = ", ".join(
        f"`{topic}`={count}" for topic, count in sorted(topic_partition_counts.items())
    )

    groups_future = admin.list_consumer_groups(request_timeout=10)
    groups_result = groups_future.result(timeout=20)
    group_listings = sorted(groups_result.valid, key=lambda item: item.group_id)
    consumer_groups = [group.group_id for group in group_listings if group.group_id]
    state_counts: dict[str, int] = {}
    for group in group_listings:
        state = getattr(group.state, "name", str(group.state)).upper()
        state_counts[state] = state_counts.get(state, 0) + 1

    if not topics:
        return (
            "1. [WARN] Topic inventory returned zero topics from Kafka Admin metadata; Fix: verify broker "
            "connectivity and ACLs for metadata access.\n"
            "2. [WARN] Partition inventory is unavailable because topic metadata is empty; Fix: check broker "
            "health and metadata APIs, then retry.\n"
            "3. [WARN] Consumer group inventory may be incomplete when metadata access fails; Fix: validate "
            "broker endpoints and credentials used by Kafka Expert.\n"
            "4. [GOOD] Retry after metadata is restored to get full topic/partition/group inventory."
        )

    state_summary = ", ".join(f"{state}={count}" for state, count in sorted(state_counts.items()))
    groups_display = format_backtick_list(consumer_groups, max_items=12)
    topics_display = format_backtick_list(topics, max_items=12)

    return (
        f"1. [GOOD] Full topic inventory is collected from Kafka Admin metadata: {len(topics)} topics "
        f"({topics_display})\n"
        f"2. [GOOD] Full partition inventory is collected from topic metadata: {total_partitions} partitions "
        f"total ({partitions_per_topic})\n"
        f"3. [GOOD] Consumer group inventory is collected from broker coordinators: {len(consumer_groups)} "
        f"groups ({groups_display})\n"
        f"4. [GOOD] Consumer group states summary: {state_summary if state_summary else 'none reported'}"
    )


def _avro_contract_summary(schema_text: str) -> tuple[str, str]:
    """Extract concise contract summary and field count text from an Avro schema string."""
    try:
        schema_obj = json.loads(schema_text)
    except Exception:
        return ("unknown", "unknown")

    if isinstance(schema_obj, dict):
        schema_kind = str(schema_obj.get("type", "unknown"))
        fields = schema_obj.get("fields", [])
        field_count = str(len(fields)) if isinstance(fields, list) else "unknown"
        return (schema_kind, field_count)
    return ("unknown", "unknown")


def build_schema_registry_answer() -> str:
    """Build deterministic Schema Registry/subjects/data-contract response."""
    base = SCHEMA_REGISTRY_URL.rstrip("/")
    subjects: list[str] = fetch_json(f"{base}/subjects")
    global_config = fetch_json(f"{base}/config")
    compatibility = str(global_config.get("compatibilityLevel", "UNKNOWN"))

    subject_details: list[str] = []
    for subject in sorted(subjects):
        encoded = urllib.parse.quote(subject, safe="")
        latest = fetch_json(f"{base}/subjects/{encoded}/versions/latest")
        schema_type = str(latest.get("schemaType", "AVRO"))
        latest_version = latest.get("version", "?")
        schema_id = latest.get("id", "?")
        schema_kind = "unknown"
        field_count = "unknown"
        if schema_type.upper() == "AVRO":
            schema_kind, field_count = _avro_contract_summary(str(latest.get("schema", "")))
        subject_details.append(
            f"`{subject}` v{latest_version} (id={schema_id}, type={schema_type}, "
            f"contract={schema_kind}, fields={field_count})"
        )

    if not subjects:
        return (
            "1. [WARN] Schema Registry is reachable but no subjects are registered; Fix: register subject schemas "
            "before producing governed data.\n"
            "2. [GOOD] Global compatibility is "
            f"{compatibility}.\n"
            "3. [WARN] No data contracts are active because subject list is empty; Fix: publish and register "
            "initial Avro/JSON/Protobuf contracts."
        )

    subjects_display = format_backtick_list(subjects, max_items=20)
    contracts_display = "; ".join(subject_details[:8])
    hidden_contracts = len(subject_details) - min(len(subject_details), 8)
    if hidden_contracts > 0:
        contracts_display += f"; ... (+{hidden_contracts} more subjects)"

    return (
        f"1. [GOOD] Schema Registry is reachable and global compatibility is `{compatibility}`\n"
        f"2. [GOOD] Subject inventory is collected: {len(subjects)} subjects ({subjects_display})\n"
        f"3. [GOOD] Latest data contract snapshot: {contracts_display}\n"
        "4. [GOOD] Subject versions and schema IDs confirm active contract governance in Schema Registry"
    )


def parse_create_topic_request(text: str) -> tuple[str, int, int] | None:
    """Parse explicit create-topic requests and return (topic, partitions, replication_factor)."""
    normalized = text.strip().lower()
    create_match = re.search(
        r"\b(?:create|add|provision)\s+(?:a\s+|an\s+)?(?:new\s+)?(?:kafka\s+)?topic"
        r"(?:\s+(?:called|named))?\s+['\"`]?([a-zA-Z0-9._-]+)['\"`]?",
        normalized,
    )
    if not create_match:
        return None

    topic = create_match.group(1).strip()
    if not topic:
        return None

    partitions = 3
    replication_factor = 3

    partitions_match = re.search(r"\b(\d+)\s+partitions?\b", normalized)
    if partitions_match:
        try:
            partitions = max(1, int(partitions_match.group(1)))
        except ValueError:
            partitions = 3

    replication_match = re.search(
        r"\b(?:rf|replication(?:\s*factor)?)\s*(?:=|:|of|is)?\s*(\d+)\b",
        normalized,
    )
    if replication_match:
        try:
            replication_factor = max(1, int(replication_match.group(1)))
        except ValueError:
            replication_factor = 3

    return (topic, partitions, replication_factor)


def build_create_topic_answer(topic: str, partitions: int, replication_factor: int) -> str:
    """Create Kafka topic deterministically via AdminClient and return verification bullets."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    metadata_before = admin.list_topics(timeout=15)

    if topic in metadata_before.topics and metadata_before.topics[topic].error is None:
        existing_partitions = len(metadata_before.topics[topic].partitions)
        return (
            f"1. [GOOD] Topic `{topic}` already exists, so no create change was needed.\n"
            f"2. [GOOD] Current topic state: partitions={existing_partitions}.\n"
            "3. [GOOD] Verification complete from Kafka Admin metadata."
        )

    if replication_factor > 3:
        return (
            f"1. [WARN] Requested replication factor `{replication_factor}` is greater than available brokers (3); "
            "Fix: use replication factor 1-3 and retry.\n"
            f"2. [GOOD] No topic was created for `{topic}` because the request is invalid."
        )

    new_topic = NewTopic(topic=topic, num_partitions=partitions, replication_factor=replication_factor)
    futures = admin.create_topics([new_topic], request_timeout=20)
    future = futures[topic]
    try:
        future.result(timeout=30)
    except Exception as exc:
        err = str(exc)
        if "TOPIC_ALREADY_EXISTS" not in err and "already exists" not in err.lower():
            return (
                f"1. [BAD] Topic create failed for `{topic}`: {err}; "
                "Fix: check broker/controller health and ACLs, then retry create topic.\n"
                "2. [WARN] Verify the request parameters and Kafka Admin permissions before retrying."
            )

    topic_meta = None
    for _ in range(8):
        metadata_after = admin.list_topics(timeout=15)
        candidate = metadata_after.topics.get(topic)
        if candidate is not None and candidate.error is None:
            topic_meta = candidate
            break
        time.sleep(1.0)

    if topic_meta is None:
        return (
            f"1. [WARN] Topic create command was submitted for `{topic}`, but verification metadata does not show it yet; "
            "Fix: wait a few seconds and re-check topic list.\n"
            "2. [WARN] If it still does not appear, check controller logs and admin ACLs."
        )

    created_partitions = len(topic_meta.partitions)
    return (
        f"1. [GOOD] Topic `{topic}` was created successfully.\n"
        f"2. [GOOD] Topic settings: partitions={created_partitions}, replication_factor={replication_factor}.\n"
        "3. [GOOD] Verification complete from Kafka Admin metadata."
    )


def prometheus_query_json(query: str) -> dict[str, Any]:
    """Run a Prometheus instant query and return parsed JSON response payload."""
    encoded = urllib.parse.urlencode({"query": query})
    url = f"{PROMETHEUS_URL.rstrip('/')}/api/v1/query?{encoded}"
    req = urllib.request.Request(url=url, method="GET")
    with urllib.request.urlopen(req, timeout=20) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def extract_topic_values(result_json: dict[str, Any]) -> dict[str, float]:
    """Extract topic->value map from a Prometheus vector response."""
    data = result_json.get("data", {})
    result = data.get("result", [])
    out: dict[str, float] = {}
    for item in result:
        metric = item.get("metric", {})
        topic = str(metric.get("topic", "")).strip()
        value = item.get("value", [None, "0"])
        raw_num = value[1] if len(value) > 1 else "0"
        if not topic:
            continue
        try:
            out[topic] = float(raw_num)
        except (TypeError, ValueError):
            out[topic] = 0.0
    return out


def build_topic_inventory_answer() -> str:
    """Build a deterministic topic inventory response from Prometheus metrics."""
    inventory_json = prometheus_query_json(TOPIC_INVENTORY_SERIES_PROMQL)
    activity_json = prometheus_query_json(TOPIC_ACTIVITY_SERIES_PROMQL)
    inventory_topics_map = extract_topic_values(inventory_json)
    activity_topics_map = extract_topic_values(activity_json)

    inventory_topics = sorted(inventory_topics_map.keys())
    active_topics = sorted(activity_topics_map.keys())
    idle_topics = sorted(set(inventory_topics) - set(active_topics))

    if not inventory_topics:
        return (
            "1. [WARN] Full topic inventory is unavailable because Prometheus returned zero "
            "topic series from kafka.log metrics; Fix: verify kafka.log JMX metrics are exported and scraped.\n"
            "2. [WARN] Active topic visibility is also empty from BrokerTopicMetrics; Fix: verify kafka.server "
            "BrokerTopicMetrics are exposed and not filtered.\n"
            "3. [GOOD] Once topic series return, Kafka Expert will report exact inventory count and topic names."
        )

    inventory_list = ", ".join(f"`{topic}`" for topic in inventory_topics)
    active_list = ", ".join(f"`{topic}`" for topic in active_topics) if active_topics else "none"
    if idle_topics:
        idle_note = ", ".join(f"`{topic}`" for topic in idle_topics)
        activity_summary = (
            f"Active traffic topics: {len(active_topics)} ({active_list}); idle topics right now: "
            f"{len(idle_topics)} ({idle_note})"
        )
    else:
        activity_summary = f"Active traffic topics: {len(active_topics)} ({active_list})"

    return (
        f"1. [GOOD] Full Kafka topic inventory count is {len(inventory_topics)} from kafka.log "
        "topic/partition metrics.\n"
        f"2. [GOOD] Inventory topics: {inventory_list}.\n"
        f"3. [GOOD] {activity_summary}.\n"
        "4. [GOOD] Inventory completeness is confirmed from per-topic/per-partition log metrics, not only "
        "traffic metrics."
    )


def prom_result_count(result_json: dict[str, Any]) -> int:
    """Return vector sample count from a Prometheus instant-query response."""
    data = result_json.get("data", {})
    result = data.get("result", [])
    if not isinstance(result, list):
        return 0
    return len(result)


def is_prometheus_scrape_fix_issue(issue_line: str) -> bool:
    """Detect whether an auto-fix issue line asks for Prometheus scrape/alert coverage remediation."""
    normalized = issue_line.lower()
    required = ("prometheus", "fix:")
    if not all(token in normalized for token in required):
        return False
    coverage_terms = (
        "node_exporter",
        "jvm",
        "disk",
        "latency",
        "isr",
        "leader",
        "scraping",
        "alerts",
    )
    return any(term in normalized for term in coverage_terms)


def build_prometheus_scrape_fix_answer() -> str:
    """Build deterministic verification/fix guidance for Prometheus scrape + alert coverage."""
    kafka_jmx_up = prometheus_query_json('up{job="kafka_jmx"}')
    node_exporter_up = prometheus_query_json('up{job="node_exporter"}')
    produce_p99 = prometheus_query_json(
        'max by (broker) (kafka_jmx_attribute_value{domain="kafka.network",'
        'mbean=~"kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce.*",'
        'attribute="99thPercentile"})'
    )
    fetch_p99 = prometheus_query_json(
        'max by (broker) (kafka_jmx_attribute_value{domain="kafka.network",'
        'mbean=~"kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer.*",'
        'attribute="99thPercentile"})'
    )
    isr_metrics = prometheus_query_json(
        'max by (broker) (kafka_jmx_attribute_value{domain="kafka.server",'
        'mbean=~"kafka.server:type=ReplicaManager,name=IsrShrinksPerSec.*",'
        'attribute="OneMinuteRate"})'
    )
    leader_metrics = prometheus_query_json(
        'sum(kafka_jmx_attribute_value{domain="kafka.controller",'
        'mbean=~"kafka.controller:type=KafkaController,name=ActiveControllerCount.*",'
        'attribute="Value"})'
    )
    disk_metrics = prometheus_query_json(
        'max by (instance, mountpoint) (100 * (1 - ('
        'node_filesystem_avail_bytes{job="node_exporter",'
        'fstype!~"tmpfs|overlay|squashfs|nsfs|proc|sysfs|cgroup2fs|tracefs|ramfs"} / '
        'node_filesystem_size_bytes{job="node_exporter",'
        'fstype!~"tmpfs|overlay|squashfs|nsfs|proc|sysfs|cgroup2fs|tracefs|ramfs"}'
        ")))"
    )

    rules_json = fetch_json(f"{PROMETHEUS_URL.rstrip('/')}/api/v1/rules")
    groups = rules_json.get("data", {}).get("groups", [])
    loaded_alert_names: set[str] = set()
    for group in groups:
        for rule in group.get("rules", []):
            name = rule.get("name")
            if isinstance(name, str) and name:
                loaded_alert_names.add(name)

    required_alerts = {
        "NodeExporterTargetDown",
        "KafkaHostDiskUsageHigh",
        "KafkaJvmGcTimeHigh",
        "KafkaFileDescriptorUsageHigh",
        "KafkaProduceRequestLatencyP99High",
        "KafkaFetchConsumerRequestLatencyP99High",
        "KafkaRequestLatencyMetricsMissing",
        "KafkaIsrShrinkRateNonZero",
        "KafkaActiveControllerCountInvalid",
        "KafkaOfflinePartitionsNonZero",
        "KafkaUnderReplicatedPartitionsNonZero",
        "KafkaPreferredReplicaImbalanceNonZero",
        "KafkaIsrLeaderMetricsMissing",
    }
    missing_alerts = sorted(required_alerts - loaded_alert_names)

    has_kafka_jmx = prom_result_count(kafka_jmx_up) > 0
    has_node_exporter = prom_result_count(node_exporter_up) > 0
    has_latency = prom_result_count(produce_p99) > 0 and prom_result_count(fetch_p99) > 0
    has_isr_leader = prom_result_count(isr_metrics) > 0 and prom_result_count(leader_metrics) > 0
    has_disk = prom_result_count(disk_metrics) > 0

    scrape_tag = "GOOD" if has_kafka_jmx and has_node_exporter else "WARN"
    latency_tag = "GOOD" if has_latency else "WARN"
    isr_tag = "GOOD" if has_isr_leader else "WARN"
    disk_tag = "GOOD" if has_disk else "WARN"
    alerts_tag = "GOOD" if not missing_alerts else "WARN"

    scrape_fix = (
        ""
        if scrape_tag == "GOOD"
        else " Fix: ensure Prometheus `scrape_configs` include `kafka_jmx` and `node_exporter`, then reload Prometheus."
    )
    latency_fix = (
        ""
        if latency_tag == "GOOD"
        else " Fix: verify Kafka network RequestMetrics `TotalTimeMs` p99 series are exported by JMX and scraped."
    )
    isr_fix = (
        ""
        if isr_tag == "GOOD"
        else " Fix: verify ReplicaManager/Controller JMX MBeans for ISR and controller count are exposed and scraped."
    )
    disk_fix = (
        ""
        if disk_tag == "GOOD"
        else " Fix: verify node_exporter is running and filesystem metrics are not filtered out."
    )
    alerts_fix = (
        ""
        if alerts_tag == "GOOD"
        else f" Fix: load missing alert rules: {', '.join(f'`{name}`' for name in missing_alerts)}."
    )

    return (
        f"1. [{scrape_tag}] Prometheus scrape targets: kafka_jmx="
        f"{prom_result_count(kafka_jmx_up)} series and node_exporter={prom_result_count(node_exporter_up)} series.{scrape_fix}\n"
        f"2. [{disk_tag}] Host disk telemetry via node_exporter has "
        f"{prom_result_count(disk_metrics)} filesystem series available.{disk_fix}\n"
        f"3. [{latency_tag}] Kafka request latency p99 telemetry has "
        f"{prom_result_count(produce_p99)} Produce series and {prom_result_count(fetch_p99)} FetchConsumer series.{latency_fix}\n"
        f"4. [{isr_tag}] ISR/leader telemetry has {prom_result_count(isr_metrics)} ISR-shrink series "
        f"and {prom_result_count(leader_metrics)} ActiveControllerCount aggregate series.{isr_fix}\n"
        f"5. [{alerts_tag}] Prometheus alert rules coverage: "
        f"{len(required_alerts) - len(missing_alerts)}/{len(required_alerts)} required alerts loaded.{alerts_fix}"
)


def _is_model_not_found_error(exc: Exception) -> bool:
    """Return True when an exception indicates the requested model is unavailable for the account."""
    text = str(exc).lower()
    return "model_not_found" in text or "does not exist" in text or "you do not have access" in text


def _build_openai_model_kwargs(model_name: str) -> dict[str, Any]:
    """Build ChatOpenAI kwargs using shared environment configuration."""
    model_kwargs: dict[str, Any] = {
        "model": model_name,
        "api_key": OPENAI_API_KEY,
        "temperature": 0,
    }
    if OPENAI_BASE_URL:
        model_kwargs["base_url"] = OPENAI_BASE_URL
    return model_kwargs


def build_chat_openai_with_fallback(component: str) -> tuple[ChatOpenAI, str]:
    """Create a ChatOpenAI client, trying fallback models if the requested one is unavailable."""
    candidates: list[str] = []
    seen: set[str] = set()
    for model_name in [OPENAI_MODEL, *OPENAI_MODEL_FALLBACKS]:
        trimmed = model_name.strip()
        if not trimmed or trimmed in seen:
            continue
        seen.add(trimmed)
        candidates.append(trimmed)

    if not candidates:
        raise RuntimeError(f"{component}: no OPENAI model configured.")

    unavailable_errors: list[str] = []
    for model_name in candidates:
        llm = ChatOpenAI(**_build_openai_model_kwargs(model_name))
        try:
            # Probe once so startup chooses a model that the account can actually use.
            llm.invoke("Reply with OK.")
            return llm, model_name
        except Exception as exc:
            if _is_model_not_found_error(exc):
                unavailable_errors.append(f"{model_name}: {exc}")
                continue
            raise RuntimeError(f"{component}: model '{model_name}' validation failed: {exc}") from exc

    tried = ", ".join(candidates)
    errors = " | ".join(unavailable_errors) if unavailable_errors else "model unavailable"
    raise RuntimeError(f"{component}: no available model from [{tried}]. Details: {errors}")


def normalize_content(content: Any) -> str:
    """Normalize model content payload (string/list blocks) into plain text."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(str(item.get("text", "")))
            else:
                parts.append(str(item))
        return "\n".join(p for p in parts if p).strip()
    return str(content)


def extract_ai_text(agent_result: dict[str, Any]) -> str:
    """Extract the latest assistant message text from a LangGraph agent result."""
    messages = agent_result.get("messages", [])
    for msg in reversed(messages):
        if isinstance(msg, AIMessage):
            return normalize_content(msg.content).strip()
    return "No response produced by model."


def infer_severity(line: str) -> str:
    """Infer severity label from a bullet line when model output is ambiguous."""
    lower = line.lower()
    if "[bad]" in lower:
        return "BAD"
    if "[warn]" in lower:
        return "WARN"
    if "[good]" in lower:
        return "GOOD"

    bad_markers = ("unhealthy", "failed", "failure", "error", "critical", "down")
    warn_markers = ("degraded", "risk", "gap", "missing", "cannot", "not fully", "warn")

    if any(token in lower for token in bad_markers):
        return "BAD"
    if any(token in lower for token in warn_markers):
        return "WARN"
    return "GOOD"


def strip_severity_tag(line: str) -> str:
    """Remove leading severity marker if present."""
    return re.sub(r"^\[(good|warn|bad)\]\s*", "", line, flags=re.IGNORECASE).strip()


def ensure_fix_clause(line: str, severity: str) -> str:
    """Ensure warning/bad lines provide a concrete remediation action."""
    if severity == "GOOD":
        return line
    lower = line.lower()
    if "fix:" in lower or "action:" in lower or "next step:" in lower:
        return line
    return (
        f"{line} Fix: Check the related Kafka metric and broker logs, "
        "apply the needed configuration or capacity fix, then re-run the cluster check."
    )


def to_numbered_bullets(text: str, max_items: int = 6) -> str:
    """Normalize text into concise 1-based bullet lines for UI readability."""
    raw_lines = [line.strip() for line in text.splitlines() if line.strip()]
    cleaned: list[str] = []
    for line in raw_lines:
        # Drop markdown heading markers and list markers if present.
        line = line.lstrip("#").strip()
        if line.startswith(("-", "*", "•")):
            line = line[1:].strip()
        if len(line) >= 3 and line[0].isdigit() and line[1] == ".":
            line = line[2:].strip()
        if line:
            cleaned.append(line)

    # Fallback for paragraph-only model output.
    if not cleaned:
        cleaned = [seg.strip() for seg in text.replace("\n", " ").split(".") if seg.strip()]

    concise = cleaned[:max_items]
    out_lines: list[str] = []
    for idx, raw_item in enumerate(concise, start=1):
        item = strip_severity_tag(raw_item).rstrip(".")
        severity = infer_severity(raw_item)
        item = ensure_fix_clause(item, severity)
        out_lines.append(f"{idx}. [{severity}] {item}")
    return "\n".join(out_lines)


class GraphRAGRuntime:
    """Neo4j-backed PDF Graph RAG runtime for knowledge graph ingest and graph-grounded QA."""

    def __init__(self) -> None:
        """Initialize Neo4j driver, constraints, and an extraction/answering LLM."""
        if not OPENAI_API_KEY:
            raise RuntimeError("OPENAI_API_KEY is not configured for Graph RAG.")
        if not NEO4J_AUTH_DISABLED and not NEO4J_PASSWORD:
            raise RuntimeError("NEO4J_PASSWORD is not configured.")

        self._lock = threading.Lock()
        if NEO4J_AUTH_DISABLED:
            self._driver = GraphDatabase.driver(NEO4J_URI, auth=None)
        else:
            self._driver = GraphDatabase.driver(
                NEO4J_URI,
                auth=(NEO4J_USERNAME, NEO4J_PASSWORD),
            )
        with self._driver.session() as session:
            session.run("RETURN 1 AS ok").single()
            self._ensure_schema(session)

        self._llm, self.model_name = build_chat_openai_with_fallback("Graph RAG")

    def _ensure_schema(self, session: Any) -> None:
        """Create id/name uniqueness constraints required for graph upserts."""
        session.run(
            "CREATE CONSTRAINT graph_doc_id IF NOT EXISTS FOR (d:Document) REQUIRE d.doc_id IS UNIQUE"
        ).consume()
        session.run(
            "CREATE CONSTRAINT graph_chunk_id IF NOT EXISTS FOR (c:Chunk) REQUIRE c.chunk_id IS UNIQUE"
        ).consume()
        session.run(
            "CREATE CONSTRAINT graph_entity_name IF NOT EXISTS FOR (e:Entity) REQUIRE e.name IS UNIQUE"
        ).consume()

    @staticmethod
    def _extract_pdf_text(pdf_bytes: bytes) -> str:
        """Extract text from all pages of a PDF binary payload."""
        reader = PdfReader(io.BytesIO(pdf_bytes))
        parts: list[str] = []
        for page in reader.pages:
            page_text = page.extract_text() or ""
            page_text = page_text.strip()
            if page_text:
                parts.append(page_text)
        return "\n\n".join(parts).strip()

    @staticmethod
    def _chunk_text(text: str, max_chars: int = 1800, overlap: int = 240) -> list[str]:
        """Split text into overlapping chunks for relation extraction stability."""
        if not text:
            return []
        chunks: list[str] = []
        start = 0
        while start < len(text):
            end = min(start + max_chars, len(text))
            piece = text[start:end].strip()
            if piece:
                chunks.append(piece)
            if end == len(text):
                break
            start = max(0, end - overlap)
        return chunks

    @staticmethod
    def _normalize_edge_value(value: Any, max_len: int = 120) -> str:
        """Normalize one edge value into a compact graph-safe string."""
        out = re.sub(r"\s+", " ", str(value or "")).strip()
        out = out.strip("`\"' ")
        return out[:max_len]

    def _extract_edges(self, chunk_text: str) -> list[dict[str, str]]:
        """Extract relation edges from one chunk using deterministic JSON output."""
        prompt = (
            "Extract a concise knowledge graph from this text.\n"
            "Return JSON only, with this exact schema:\n"
            '{"edges":[{"source":"...", "relation":"...", "target":"...", "evidence":"..."}]}\n'
            "Rules:\n"
            "- Use short entity names.\n"
            "- relation should be verb-like snake_case (for example: has_component, uses, stores).\n"
            "- Keep max 20 edges.\n"
            "- Do not invent facts outside the text.\n"
            f"Text:\n{chunk_text}"
        )
        raw = normalize_content(self._llm.invoke(prompt).content).strip()
        payload: dict[str, Any] = {}
        try:
            payload = json.loads(raw)
        except Exception:
            match = re.search(r"\{[\s\S]*\}", raw)
            if match:
                payload = json.loads(match.group(0))

        raw_edges = payload.get("edges", []) if isinstance(payload, dict) else []
        if not isinstance(raw_edges, list):
            return []

        dedupe: set[tuple[str, str, str]] = set()
        edges: list[dict[str, str]] = []
        for edge in raw_edges:
            if not isinstance(edge, dict):
                continue
            source = self._normalize_edge_value(edge.get("source"), max_len=140)
            relation = self._normalize_edge_value(edge.get("relation"), max_len=80).lower()
            relation = re.sub(r"[^a-z0-9_]+", "_", relation).strip("_")
            target = self._normalize_edge_value(edge.get("target"), max_len=140)
            evidence = self._normalize_edge_value(edge.get("evidence"), max_len=300)
            if not source or not relation or not target:
                continue
            key = (source.lower(), relation, target.lower())
            if key in dedupe:
                continue
            dedupe.add(key)
            edges.append(
                {
                    "source": source,
                    "relation": relation,
                    "target": target,
                    "evidence": evidence,
                }
            )
            if len(edges) >= 20:
                break
        return edges

    def ingest_pdf(self, filename: str, pdf_bytes: bytes) -> dict[str, Any]:
        """Ingest one PDF file into Neo4j and create entity/relation edges."""
        if not pdf_bytes:
            raise ValueError("PDF payload is empty.")
        text = self._extract_pdf_text(pdf_bytes)
        if not text:
            raise ValueError("PDF contains no extractable text.")

        chunks = self._chunk_text(text)
        if not chunks:
            raise ValueError("Unable to create text chunks from PDF.")
        limited_chunks = chunks[: max(1, GRAPH_RAG_MAX_CHUNKS)]

        doc_id = str(uuid.uuid4())
        total_edges = 0
        with self._lock:
            with self._driver.session() as session:
                session.run(
                    "MERGE (d:Document {doc_id:$doc_id}) "
                    "ON CREATE SET d.created_at=datetime() "
                    "SET d.source_file=$source_file, d.title=$title, d.updated_at=datetime()",
                    {
                        "doc_id": doc_id,
                        "source_file": filename,
                        "title": filename,
                    },
                ).consume()

                for idx, chunk_text in enumerate(limited_chunks):
                    edges = self._extract_edges(chunk_text)
                    total_edges += len(edges)
                    chunk_id = f"{doc_id}:{idx}"
                    session.run(
                        """
                        MATCH (d:Document {doc_id:$doc_id})
                        MERGE (c:Chunk {chunk_id:$chunk_id})
                        SET c.text=$chunk_text, c.chunk_index=$chunk_index
                        MERGE (d)-[:HAS_CHUNK]->(c)
                        WITH d, c, $edges AS edges
                        UNWIND edges AS edge
                        MERGE (s:Entity {name: edge.source})
                        MERGE (t:Entity {name: edge.target})
                        MERGE (s)-[r:RELATES {relation: edge.relation, doc_id: d.doc_id}]->(t)
                        ON CREATE SET r.weight=1, r.evidence=edge.evidence, r.last_seen_at=datetime()
                        ON MATCH SET r.weight=coalesce(r.weight,0)+1, r.last_seen_at=datetime()
                        MERGE (c)-[:MENTIONS]->(s)
                        MERGE (c)-[:MENTIONS]->(t)
                        """,
                        {
                            "doc_id": doc_id,
                            "chunk_id": chunk_id,
                            "chunk_index": idx,
                            "chunk_text": chunk_text,
                            "edges": edges,
                        },
                    ).consume()

        return {
            "doc_id": doc_id,
            "source_file": filename,
            "chunks_processed": len(limited_chunks),
            "edges_created": total_edges,
        }

    def _status_counts(self) -> dict[str, int]:
        """Return graph cardinality counters for UI status and diagnostics."""
        with self._driver.session() as session:
            row = session.run(
                """
                MATCH (d:Document)
                WITH count(d) AS docs
                MATCH (c:Chunk)
                WITH docs, count(c) AS chunks
                MATCH (e:Entity)
                WITH docs, chunks, count(e) AS entities
                MATCH ()-[r:RELATES]->()
                RETURN docs, chunks, entities, count(r) AS relations
                """
            ).single()
        return {
            "documents": int(row["docs"]) if row else 0,
            "chunks": int(row["chunks"]) if row else 0,
            "entities": int(row["entities"]) if row else 0,
            "relations": int(row["relations"]) if row else 0,
        }

    def status(self) -> dict[str, Any]:
        """Return runtime availability and graph counters."""
        return {
            "neo4j_uri": NEO4J_URI,
            "neo4j_auth_mode": "none" if NEO4J_AUTH_DISABLED else "password",
            "openai_model": self.model_name,
            "counts": self._status_counts(),
        }

    @staticmethod
    def _question_terms(question: str) -> list[str]:
        """Extract candidate entity terms from a natural language question."""
        tokens = re.findall(r"[a-zA-Z][a-zA-Z0-9_-]{2,}", question.lower())
        stop = {
            "what",
            "which",
            "where",
            "when",
            "does",
            "with",
            "about",
            "from",
            "into",
            "this",
            "that",
            "kafka",
            "topic",
            "broker",
            "cluster",
        }
        terms: list[str] = []
        for token in tokens:
            if token in stop:
                continue
            if token in terms:
                continue
            terms.append(token)
            if len(terms) >= 8:
                break
        return terms or ["kafka"]

    def query(self, question: str) -> str:
        """Answer a question using graph-local context retrieved from Neo4j edges."""
        terms = self._question_terms(question)
        with self._lock:
            with self._driver.session() as session:
                rows = session.run(
                    """
                    UNWIND $terms AS term
                    MATCH (e:Entity)
                    WHERE toLower(e.name) CONTAINS term
                    WITH collect(DISTINCT e)[0..15] AS seed
                    UNWIND seed AS s
                    OPTIONAL MATCH (s)-[r:RELATES]->(t:Entity)
                    RETURN s.name AS source, coalesce(r.relation, "related_to") AS relation,
                           coalesce(t.name, "") AS target, coalesce(r.weight, 0) AS weight
                    ORDER BY weight DESC
                    LIMIT 80
                    """,
                    {"terms": terms},
                ).data()
        if not rows:
            return (
                "1. [WARN] No graph context matched that question yet.\n"
                "2. [GOOD] Next step: ingest a Kafka PDF into Graph RAG, then ask again."
            )

        context_lines: list[str] = []
        for row in rows[:40]:
            source = str(row.get("source", "")).strip()
            relation = str(row.get("relation", "related_to")).strip()
            target = str(row.get("target", "")).strip()
            weight = row.get("weight", 0)
            if not source or not target:
                continue
            context_lines.append(f"- {source} --{relation}--> {target} (weight={weight})")
        graph_context = "\n".join(context_lines)

        prompt = (
            "You are answering from a Neo4j graph built from PDF knowledge.\n"
            "Use only the graph context below.\n"
            "If context is weak, say so.\n"
            "Output only 1-based bullets with [GOOD]/[WARN]/[BAD].\n\n"
            f"Question: {question}\n\nGraph context:\n{graph_context}"
        )
        answer = normalize_content(self._llm.invoke(prompt).content).strip()
        return to_numbered_bullets(answer)


class KafkaExpertRuntime:
    """Thread-safe wrapper around LangChain agent and MCP tools (Prometheus + Kafka Admin)."""

    def __init__(self) -> None:
        """Initialize MCP-backed tools and construct the Kafka expert agent."""
        if not OPENAI_API_KEY:
            raise RuntimeError("OPENAI_API_KEY is not configured.")

        self._lock = threading.Lock()
        self._mcp_client = MultiServerMCPClient(
            {
                "prometheus": {
                    "transport": "stdio",
                    "command": "python",
                    "args": ["/app/scripts/prometheus_mcp_server.py"],
                    "env": {
                        "PROMETHEUS_URL": PROMETHEUS_URL,
                        "SCHEMA_REGISTRY_URL": SCHEMA_REGISTRY_URL,
                        "MCP_TRANSPORT": "stdio",
                    },
                },
                "kafka_admin": {
                    "transport": "stdio",
                    "command": "python",
                    "args": ["/app/scripts/kafka_admin_mcp_server.py"],
                    "env": {
                        "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS,
                        "MCP_TRANSPORT": "stdio",
                    },
                },
            }
        )
        prometheus_tools = asyncio.run(self._mcp_client.get_tools(server_name="prometheus"))
        kafka_admin_tools = asyncio.run(self._mcp_client.get_tools(server_name="kafka_admin"))
        tools = [*prometheus_tools, *kafka_admin_tools]

        llm, self.model_name = build_chat_openai_with_fallback("Kafka Expert")
        self._agent = create_react_agent(model=llm, tools=tools, prompt=SYSTEM_PROMPT)

    async def _ask_async(self, user_prompt: str) -> dict[str, Any]:
        """Run one async agent turn and return the full LangGraph response payload."""
        return await self._agent.ainvoke(
            {"messages": [("user", user_prompt)]},
            config={"recursion_limit": 25},
        )

    def ask(self, user_prompt: str) -> str:
        """Run one agent turn and return normalized response text."""
        with self._lock:
            # MCP-backed tools are async-only; use async agent invocation path.
            result = asyncio.run(self._ask_async(user_prompt))
        return to_numbered_bullets(extract_ai_text(result))


class AppState:
    """In-memory state for cluster status text and chat transcript."""

    def __init__(self) -> None:
        """Initialize default status message, error field, and chat history."""
        self.lock = threading.Lock()
        self.status_text = "Click 'Query Full Cluster State' to run Kafka Expert."
        self.status_updated_at = ""
        self.last_error = ""
        self.chat: list[dict[str, str]] = []

    def set_status(self, text: str, error: str = "") -> None:
        """Update cluster status text, timestamp, and optional error message."""
        with self.lock:
            self.status_text = text
            self.status_updated_at = utc_now_iso()
            self.last_error = error

    def add_chat(self, role: str, text: str) -> None:
        """Append a chat entry and enforce bounded chat history."""
        with self.lock:
            self.chat.append({"role": role, "text": text, "at_utc": utc_now_iso()})
            self.chat = self.chat[-80:]

    def clear_chat(self) -> None:
        """Clear chat transcript while preserving latest cluster-status text."""
        with self.lock:
            self.chat = []

    def snapshot(self) -> dict[str, Any]:
        """Return a thread-safe copy of current app state for API responses."""
        with self.lock:
            return {
                "status_text": self.status_text,
                "status_updated_at": self.status_updated_at,
                "last_error": self.last_error,
                "chat": list(self.chat),
            }


app = Flask(__name__)
state = AppState()
runtime: KafkaExpertRuntime | None = None
runtime_error = ""
graph_runtime: GraphRAGRuntime | None = None
graph_runtime_error = ""
graph_runtime_lock = threading.Lock()

try:
    runtime = KafkaExpertRuntime()
except Exception as exc:
    runtime_error = str(exc)
    state.set_status(
        "Kafka Expert is waiting for model credentials. Set OPENAI_API_KEY and refresh.",
        error=runtime_error,
    )

try:
    graph_runtime = GraphRAGRuntime()
except Exception as exc:
    graph_runtime_error = str(exc)


def ensure_graph_runtime() -> tuple[GraphRAGRuntime | None, str]:
    """Lazily initialize Graph RAG runtime so startup ordering doesn't block Neo4j usage."""
    global graph_runtime, graph_runtime_error
    if graph_runtime is not None:
        return graph_runtime, ""

    with graph_runtime_lock:
        if graph_runtime is not None:
            return graph_runtime, ""
        try:
            graph_runtime = GraphRAGRuntime()
            graph_runtime_error = ""
            return graph_runtime, ""
        except Exception as exc:
            graph_runtime_error = str(exc)
            return None, graph_runtime_error


@app.get("/")
def index() -> str:
    """Serve the Kafka Expert single-page UI."""
    page = """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Kafka Expert</title>
  <style>
    :root {
      --bg: #0b1117;
      --panel: #111a23;
      --ink: #e6eff6;
      --ink-dim: #9ab0c1;
      --line: #2b3d4d;
      --accent: #0da37f;
      --accent-2: #0a6a53;
      --warn: #ff6b6b;
      --link: #79c0ff;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 12% 8%, #143f38 0%, #143f38 14%, transparent 46%),
        radial-gradient(circle at 86% 2%, #1b2f49 0%, #1b2f49 16%, transparent 40%),
        var(--bg);
    }
    a { color: var(--link); }
    .wrap {
      max-width: 1100px;
      margin: 20px auto 36px auto;
      padding: 0 14px;
    }
    .title {
      margin: 0 0 14px 0;
      font-size: 30px;
      letter-spacing: 0.2px;
    }
    .subtitle {
      margin: 0 0 20px 0;
      color: var(--ink-dim);
      font-size: 14px;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 14px;
    }
    .hidden { display: none; }
    .tabs {
      display: flex;
      gap: 8px;
      margin-bottom: 12px;
      flex-wrap: wrap;
    }
    .tab-btn {
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 8px 14px;
      background: #162430;
      color: var(--ink);
      font-weight: 700;
      cursor: pointer;
    }
    .tab-btn.active {
      background: var(--accent-2);
      color: #ffffff;
      border-color: var(--accent-2);
    }
    .panel h2 {
      margin: 0 0 10px 0;
      font-size: 18px;
    }
    button {
      border: 0;
      border-radius: 10px;
      padding: 10px 14px;
      background: var(--accent);
      color: #fff;
      font-weight: 700;
      cursor: pointer;
    }
    button.secondary {
      background: #2e4f67;
    }
    button:disabled { opacity: 0.6; cursor: not-allowed; }
    .actions {
      display: flex;
      align-items: center;
      gap: 10px;
      margin-bottom: 10px;
      flex-wrap: wrap;
    }
    .chat-log {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #0d161f;
      min-height: 420px;
      max-height: 620px;
      overflow: auto;
      padding: 10px;
      margin-bottom: 10px;
    }
    .msg { margin: 0 0 10px 0; padding: 8px; border-radius: 8px; }
    .msg.user { background: #1a2a38; border: 1px solid #34506a; }
    .msg.agent { background: #17232c; border: 1px solid #2f4454; }
    .msg.cluster { background: #162126; border: 1px solid #2a5a6f; }
    .meta { font-size: 12px; color: var(--ink-dim); margin-bottom: 4px; }
    .text { white-space: pre-wrap; font-size: 13px; line-height: 1.45; }
    .status-lines {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .status-line {
      border-radius: 8px;
      border: 2px solid transparent;
      padding: 8px 10px;
      font-size: 13px;
      line-height: 1.45;
      white-space: pre-wrap;
      font-weight: 400;
    }
    .status-line.good {
      background: #22c55e;
      border-color: #15803d;
      color: #04210f;
    }
    .status-line.warn {
      background: #fde047;
      border-color: #ca8a04;
      color: #352300;
    }
    .status-line.bad {
      background: #ef4444;
      border-color: #b91c1c;
      color: #ffffff;
    }
    button.fix-action {
      border: 0;
      border-radius: 0;
      padding: 0;
      margin: 0;
      background: transparent;
      color: inherit;
      cursor: pointer;
      font: inherit;
      text-align: left;
      text-decoration: underline;
      text-underline-offset: 2px;
    }
    button.fix-action:disabled {
      opacity: 0.65;
      cursor: wait;
    }
    .short-answer {
      margin-top: 8px;
      border-radius: 8px;
      border: 1px solid #355063;
      background: #12212b;
      color: #d9edf7;
      padding: 8px 10px;
      font-size: 13px;
      line-height: 1.4;
    }
    .chat-input-row { display: flex; gap: 8px; }
    #chatInput {
      flex: 1;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px;
      font-size: 14px;
      background: #0e1821;
      color: var(--ink);
    }
    #chatInput::placeholder { color: #7890a3; }
    .statusline {
      margin: 0;
      font-size: 12px;
      color: var(--ink-dim);
    }
    .error { color: var(--warn); }
    .kafka-ui-launch {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #0f1a24;
      padding: 16px;
    }
    .embed-frame-wrap {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #0f1a24;
      overflow: hidden;
      margin-top: 8px;
    }
    .embed-frame {
      width: 100%;
      min-height: 760px;
      border: 0;
      background: #0f1a24;
      display: block;
    }
    .launch-row {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
      margin-bottom: 8px;
    }
    .file-input {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 8px;
      background: #0e1821;
      color: var(--ink);
      font-size: 13px;
    }
    .graph-status {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 8px 10px;
      background: #0f1a24;
      color: var(--ink-dim);
      font-size: 13px;
      margin-bottom: 8px;
    }
    @media (max-width: 980px) {
      .chat-log { min-height: 300px; }
      .embed-frame { min-height: 620px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <h1 class="title">Kafka Expert</h1>
    <p class="subtitle">LangChain + MCP agent for Kafka-only cluster diagnostics using Prometheus telemetry.</p>

    <div class="tabs">
      <button id="tabExpertBtn" class="tab-btn active" onclick="switchTab('expert')">Kafka Expert</button>
      <button id="tabGraphRagBtn" class="tab-btn" onclick="switchTab('graph_rag')">Graph RAG</button>
      <button id="tabProducerBtn" class="tab-btn" onclick="switchTab('producer')">Kafka Producer</button>
      <button id="tabConsumerBtn" class="tab-btn" onclick="switchTab('consumer')">Kafka Consumer</button>
      <button id="tabGrafanaBtn" class="tab-btn" onclick="switchTab('grafana')">Grafana</button>
      <button id="tabNeo4jBrowserBtn" class="tab-btn" onclick="switchTab('neo4j_browser')">Neo4j Browser</button>
      <button id="tabKafkaUiBtn" class="tab-btn" onclick="switchTab('kafka_ui')">Kafka UI</button>
    </div>

    <section id="expertPanel" class="panel">
      <h2>Kafka Expert Chat</h2>
      <div class="actions">
        <button id="refreshStateBtn" onclick="refreshClusterState()">Query Full Cluster State</button>
        <button id="clearChatBtn" class="secondary" onclick="clearChat()">Clear Chat</button>
        <p class="statusline">Cluster State Updated: <span id="statusUpdated">n/a</span></p>
      </div>
      <div class="chat-log" id="chatLog"></div>
      <div class="chat-input-row">
        <input id="chatInput" type="text" placeholder="Ask Kafka cluster questions only..." />
        <button id="chatSendBtn" onclick="sendChat()">Send</button>
      </div>
      <p class="statusline">Non-Kafka questions are refused by policy.</p>
      <p class="statusline error" id="statusError"></p>
    </section>

    <section id="graphRagPanel" class="panel hidden">
      <h2>Graph RAG (Neo4j)</h2>
      <p class="statusline">Upload a PDF to build a Kafka knowledge graph (entities + edges) in Neo4j, then ask graph-grounded questions.</p>
      <div class="graph-status" id="graphRagStatus">Loading Graph RAG status...</div>
      <div class="launch-row">
        <input id="graphPdfInput" class="file-input" type="file" accept=".pdf,application/pdf" />
        <button id="graphIngestBtn" type="button" onclick="ingestGraphPdf()">Ingest PDF to Neo4j</button>
      </div>
      <div class="chat-input-row">
        <input id="graphQuestionInput" type="text" placeholder="Ask a question from the ingested PDF graph..." />
        <button id="graphAskBtn" type="button" onclick="askGraphRag()">Ask Graph</button>
      </div>
      <div class="chat-log" id="graphRagLog"></div>
    </section>

    <section id="kafkaUiPanel" class="panel hidden">
      <h2>Kafka UI</h2>
      <p class="statusline">Kafka UI blocks iframe embedding by security policy (`X-Frame-Options: DENY`).</p>
      <div class="kafka-ui-launch">
        <button type="button" onclick="openKafkaUi()">Open Kafka UI</button>
        <p class="statusline">URL: <a href="__KAFKA_UI_PUBLIC_URL__" target="_blank" rel="noopener noreferrer">__KAFKA_UI_PUBLIC_URL__</a></p>
      </div>
    </section>

    <section id="neo4jBrowserPanel" class="panel hidden">
      <h2>Neo4j Browser</h2>
      <p class="statusline">Neo4j Browser blocks iframe embedding by security policy (`X-Frame-Options: DENY`, `frame-ancestors 'none'`).</p>
      <div class="kafka-ui-launch">
        <button type="button" onclick="openNeo4jBrowser()">Open Neo4j Browser</button>
        <button type="button" class="secondary" onclick="openNeo4jBrowserHere()">Open Neo4j Browser Here</button>
        <p class="statusline">URL: <a href="__NEO4J_BROWSER_PUBLIC_URL__" target="_blank" rel="noopener noreferrer">__NEO4J_BROWSER_PUBLIC_URL__</a></p>
        <p class="statusline">__NEO4J_BROWSER_LOGIN_HINT__</p>
        <p class="statusline">If nothing opens, your browser/webview blocked new tabs; use <strong>Open Neo4j Browser Here</strong>.</p>
      </div>
      <div class="graph-status">
        <div>Topology query starter:</div>
        <pre style="margin:6px 0 0 0; white-space:pre-wrap;">MATCH p=()-[r:RELATES]-() RETURN p LIMIT 200;</pre>
      </div>
    </section>

    <section id="producerPanel" class="panel hidden">
      <h2>Kafka Producer UI</h2>
      <p class="statusline">Embedded Producer UI for publishing Kafka test data.</p>
      <div class="launch-row">
        <button type="button" onclick="openProducerUi()">Open Producer UI</button>
        <p class="statusline">URL: <a href="__KAFKA_PRODUCER_UI_PUBLIC_URL__" target="_blank" rel="noopener noreferrer">__KAFKA_PRODUCER_UI_PUBLIC_URL__</a></p>
      </div>
      <div class="embed-frame-wrap">
        <iframe
          id="producerFrame"
          class="embed-frame"
          src="__KAFKA_PRODUCER_UI_PUBLIC_URL__"
          title="Kafka Producer UI"
          loading="lazy"
          referrerpolicy="no-referrer"
        ></iframe>
      </div>
    </section>

    <section id="consumerPanel" class="panel hidden">
      <h2>Kafka Consumer UI</h2>
      <p class="statusline">Embedded Consumer UI for live consumed-message feed and latency visibility.</p>
      <div class="launch-row">
        <button type="button" onclick="openConsumerUi()">Open Consumer UI</button>
        <p class="statusline">URL: <a href="__KAFKA_CONSUMER_UI_PUBLIC_URL__" target="_blank" rel="noopener noreferrer">__KAFKA_CONSUMER_UI_PUBLIC_URL__</a></p>
      </div>
      <div class="embed-frame-wrap">
        <iframe
          id="consumerFrame"
          class="embed-frame"
          src="__KAFKA_CONSUMER_UI_PUBLIC_URL__"
          title="Kafka Consumer UI"
          loading="lazy"
          referrerpolicy="no-referrer"
        ></iframe>
      </div>
    </section>

    <section id="grafanaPanel" class="panel hidden">
      <h2>Grafana (Dark)</h2>
      <p class="statusline">Embedded Grafana in dark mode for Kafka dashboards.</p>
      <div class="launch-row">
        <button type="button" onclick="openGrafana()">Open Grafana</button>
        <p class="statusline">URL: <a href="__GRAFANA_PUBLIC_URL__" target="_blank" rel="noopener noreferrer">__GRAFANA_PUBLIC_URL__</a></p>
      </div>
      <div class="embed-frame-wrap">
        <iframe
          id="grafanaFrame"
          class="embed-frame"
          src="__GRAFANA_PUBLIC_URL__"
          title="Grafana"
          loading="lazy"
          referrerpolicy="no-referrer"
        ></iframe>
      </div>
    </section>
  </div>

  <script>
    function openKafkaUi() {
      window.open('__KAFKA_UI_PUBLIC_URL__', '_blank', 'noopener,noreferrer');
    }

    function openNeo4jBrowser() {
      const w = window.open('__NEO4J_BROWSER_PUBLIC_URL__', '_blank', 'noopener,noreferrer');
      if (!w) {
        window.location.assign('__NEO4J_BROWSER_PUBLIC_URL__');
      }
    }

    function openNeo4jBrowserHere() {
      window.location.assign('__NEO4J_BROWSER_PUBLIC_URL__');
    }

    function openProducerUi() {
      window.open('__KAFKA_PRODUCER_UI_PUBLIC_URL__', '_blank', 'noopener,noreferrer');
    }

    function openConsumerUi() {
      window.open('__KAFKA_CONSUMER_UI_PUBLIC_URL__', '_blank', 'noopener,noreferrer');
    }

    function openGrafana() {
      window.open('__GRAFANA_PUBLIC_URL__', '_blank', 'noopener,noreferrer');
    }

    function appendGraphLog(role, text) {
      const log = document.getElementById('graphRagLog');
      const cls = role === 'user' ? 'user' : 'agent';
      const body = renderSeverityLines(text || '') || esc(text || '');
      log.insertAdjacentHTML(
        'beforeend',
        '<div class="msg ' + cls + '">' +
          '<div class="meta">' + esc(role.toUpperCase()) + ' • ' + new Date().toISOString() + '</div>' +
          '<div class="text">' + body + '</div>' +
        '</div>'
      );
      log.scrollTop = log.scrollHeight;
    }

    async function loadGraphRagStatus() {
      const statusEl = document.getElementById('graphRagStatus');
      try {
        const r = await fetch('/api/graphrag/status');
        const data = await r.json();
        if (!data.ok) {
          statusEl.textContent = 'Graph RAG unavailable: ' + (data.error || 'unknown error');
          return;
        }
        const c = data.counts || {};
        statusEl.textContent =
          'Neo4j: ' + (data.neo4j_uri || 'n/a') +
          ' | Documents: ' + (c.documents ?? 0) +
          ' | Chunks: ' + (c.chunks ?? 0) +
          ' | Entities: ' + (c.entities ?? 0) +
          ' | Relations: ' + (c.relations ?? 0);
      } catch (e) {
        statusEl.textContent = 'Graph RAG status error: ' + String(e);
      }
    }

    async function ingestGraphPdf() {
      const input = document.getElementById('graphPdfInput');
      const btn = document.getElementById('graphIngestBtn');
      if (!input || !input.files || input.files.length === 0) {
        appendGraphLog('agent', '1. [WARN] Choose a PDF file first. Fix: select a Kafka PDF and click ingest again.');
        return;
      }
      const file = input.files[0];
      const formData = new FormData();
      formData.append('pdf', file, file.name);
      btn.disabled = true;
      appendGraphLog('user', 'Ingest PDF: ' + file.name);
      try {
        const r = await fetch('/api/graphrag/ingest_pdf', { method: 'POST', body: formData });
        const data = await r.json();
        if (!data.ok) {
          appendGraphLog('agent', '1. [BAD] PDF ingest failed. Fix: ' + (data.error || 'check Neo4j/OpenAI and retry.'));
        } else {
          appendGraphLog('agent', data.answer || '1. [GOOD] PDF ingest completed.');
        }
      } catch (e) {
        appendGraphLog('agent', '1. [BAD] Ingest request failed. Fix: ' + String(e));
      } finally {
        btn.disabled = false;
        await loadGraphRagStatus();
      }
    }

    async function askGraphRag() {
      const input = document.getElementById('graphQuestionInput');
      const btn = document.getElementById('graphAskBtn');
      const question = (input.value || '').trim();
      if (!question) return;
      btn.disabled = true;
      appendGraphLog('user', question);
      try {
        const r = await fetch('/api/graphrag/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ question })
        });
        const data = await r.json();
        if (!data.ok) {
          appendGraphLog('agent', '1. [BAD] Graph query failed. Fix: ' + (data.error || 'retry.'));
        } else {
          appendGraphLog('agent', data.answer || '1. [WARN] Graph query returned no answer.');
        }
        input.value = '';
      } catch (e) {
        appendGraphLog('agent', '1. [BAD] Graph query request failed. Fix: ' + String(e));
      } finally {
        btn.disabled = false;
      }
    }

    function switchTab(tab) {
      const expertPanel = document.getElementById('expertPanel');
      const graphRagPanel = document.getElementById('graphRagPanel');
      const kafkaUiPanel = document.getElementById('kafkaUiPanel');
      const neo4jBrowserPanel = document.getElementById('neo4jBrowserPanel');
      const producerPanel = document.getElementById('producerPanel');
      const consumerPanel = document.getElementById('consumerPanel');
      const grafanaPanel = document.getElementById('grafanaPanel');
      const tabExpertBtn = document.getElementById('tabExpertBtn');
      const tabGraphRagBtn = document.getElementById('tabGraphRagBtn');
      const tabProducerBtn = document.getElementById('tabProducerBtn');
      const tabConsumerBtn = document.getElementById('tabConsumerBtn');
      const tabGrafanaBtn = document.getElementById('tabGrafanaBtn');
      const tabNeo4jBrowserBtn = document.getElementById('tabNeo4jBrowserBtn');
      const tabKafkaUiBtn = document.getElementById('tabKafkaUiBtn');
      const isExpert = tab === 'expert';
      const isGraphRag = tab === 'graph_rag';
      const isProducer = tab === 'producer';
      const isConsumer = tab === 'consumer';
      const isGrafana = tab === 'grafana';
      const isNeo4jBrowser = tab === 'neo4j_browser';
      const isKafkaUi = tab === 'kafka_ui';

      expertPanel.classList.toggle('hidden', !isExpert);
      graphRagPanel.classList.toggle('hidden', !isGraphRag);
      producerPanel.classList.toggle('hidden', !isProducer);
      consumerPanel.classList.toggle('hidden', !isConsumer);
      grafanaPanel.classList.toggle('hidden', !isGrafana);
      neo4jBrowserPanel.classList.toggle('hidden', !isNeo4jBrowser);
      kafkaUiPanel.classList.toggle('hidden', !isKafkaUi);
      tabExpertBtn.classList.toggle('active', isExpert);
      tabGraphRagBtn.classList.toggle('active', isGraphRag);
      tabProducerBtn.classList.toggle('active', isProducer);
      tabConsumerBtn.classList.toggle('active', isConsumer);
      tabGrafanaBtn.classList.toggle('active', isGrafana);
      tabNeo4jBrowserBtn.classList.toggle('active', isNeo4jBrowser);
      tabKafkaUiBtn.classList.toggle('active', isKafkaUi);
    }

    function esc(s) {
      return String(s)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;');
    }

    function escAttr(s) {
      return String(s)
        .replaceAll('&', '&amp;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;');
    }

    function formatLineWithFix(line) {
      const raw = String(line || '');
      const lower = raw.toLowerCase();
      const idx = lower.indexOf('fix:');
      if (idx === -1) {
        return esc(raw);
      }
      const before = raw.slice(0, idx);
      const fix = raw.slice(idx);
      return (
        esc(before) +
        '<button class="fix-action" type="button" data-issue="' + escAttr(raw) + '" onclick="runFixFromLine(this)">' +
          '<strong>' + esc(fix) + '</strong>' +
        '</button>'
      );
    }

    async function runFixFromLine(btn) {
      const issueLine = btn.getAttribute('data-issue') || '';
      if (!issueLine) return;

      const sendBtn = document.getElementById('chatSendBtn');
      const refreshBtn = document.getElementById('refreshStateBtn');
      const clearBtn = document.getElementById('clearChatBtn');
      btn.disabled = true;
      if (sendBtn) sendBtn.disabled = true;
      if (refreshBtn) refreshBtn.disabled = true;
      if (clearBtn) clearBtn.disabled = true;
      document.getElementById('statusError').textContent = '';

      try {
        const r = await fetch('/api/auto_fix', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ issue_line: issueLine })
        });
        const data = await r.json();
        if (!data.ok) {
          document.getElementById('statusError').textContent = data.error || 'Auto-fix request failed';
        }
        await loadState();
      } catch (e) {
        document.getElementById('statusError').textContent = String(e);
      } finally {
        if (sendBtn) sendBtn.disabled = false;
        if (refreshBtn) refreshBtn.disabled = false;
        if (clearBtn) clearBtn.disabled = false;
      }
    }

    function statusClass(line) {
      const lower = String(line).toLowerCase();
      if (lower.includes('[bad]')) return 'bad';
      if (lower.includes('[warn]')) return 'warn';
      if (lower.includes('[good]')) return 'good';
      if (
        lower.includes('unhealthy') ||
        lower.includes('failed') ||
        lower.includes('failure') ||
        lower.includes('error') ||
        lower.includes('critical') ||
        lower.includes('down')
      ) {
        return 'bad';
      }
      if (
        lower.includes('degraded') ||
        lower.includes('risk') ||
        lower.includes('gap') ||
        lower.includes('missing') ||
        lower.includes('cannot')
      ) {
        return 'warn';
      }
      return 'good';
    }

    function renderSeverityLines(text) {
      const lines = String(text || '').split('\\n').map(x => x.trim()).filter(Boolean);
      if (lines.length === 0) return '';
      let out = '';
      for (const line of lines) {
        const cls = statusClass(line);
        out += '<div class="status-line ' + cls + '">' + formatLineWithFix(line) + '</div>';
      }
      return '<div class="status-lines">' + out + '</div>';
    }

    function cleanBulletText(line) {
      return String(line || '')
        .replace(/^\\d+\\.\\s*/, '')
        .replace(/^\\[(good|warn|bad)\\]\\s*/i, '')
        .replace(/\\s+fix:.*$/i, '')
        .trim();
    }

    function deriveShortAnswer(text) {
      const lines = String(text || '').split('\\n').map(x => x.trim()).filter(Boolean);
      if (lines.length === 0) return '';
      const first = cleanBulletText(lines[0]);
      if (!first) return '';
      const maxLen = 180;
      if (first.length <= maxLen) return first;
      return first.slice(0, maxLen - 3).trim() + '...';
    }

    async function loadState() {
      const r = await fetch('/api/state');
      const data = await r.json();
      document.getElementById('statusUpdated').textContent = data.status_updated_at || 'n/a';
      document.getElementById('statusError').textContent = data.last_error || '';

      const log = document.getElementById('chatLog');
      log.innerHTML = '';
      const statusText = String(data.status_text || '').trim();
      if (statusText) {
        const clusterHtml = renderSeverityLines(statusText);
        log.insertAdjacentHTML(
          'beforeend',
          '<div class="msg cluster">' +
            '<div class="meta">CLUSTER STATE • ' + esc(data.status_updated_at || '') + '</div>' +
            '<div class="text">' + clusterHtml + '</div>' +
          '</div>'
        );
      }
      for (const m of data.chat || []) {
        const cls = m.role === 'user' ? 'user' : 'agent';
        let text = '';
        if (m.role === 'agent') {
          const severityHtml = renderSeverityLines(m.text || '');
          const shortAnswer = deriveShortAnswer(m.text || '');
          if (severityHtml) {
            text = severityHtml;
            if (shortAnswer) {
              text += '<div class="short-answer">Short answer: ' + esc(shortAnswer) + '</div>';
            }
          } else {
            text = esc(m.text || '');
          }
        } else {
          text = esc(m.text || '');
        }
        log.insertAdjacentHTML(
          'beforeend',
          '<div class="msg ' + cls + '">' +
            '<div class="meta">' + esc(m.role.toUpperCase()) + ' • ' + esc(m.at_utc || '') + '</div>' +
            '<div class="text">' + text + '</div>' +
          '</div>'
        );
      }
      log.scrollTop = log.scrollHeight;
    }

    async function refreshClusterState() {
      const btn = document.getElementById('refreshStateBtn');
      btn.disabled = true;
      document.getElementById('statusError').textContent = '';
      try {
        const r = await fetch('/api/cluster_state', { method: 'POST', headers: { 'Content-Type': 'application/json' }});
        const data = await r.json();
        document.getElementById('statusUpdated').textContent = data.status_updated_at || 'n/a';
        document.getElementById('statusError').textContent = data.last_error || '';
        await loadState();
      } catch (e) {
        document.getElementById('statusError').textContent = String(e);
      } finally {
        btn.disabled = false;
      }
    }

    async function sendChat() {
      const input = document.getElementById('chatInput');
      const btn = document.getElementById('chatSendBtn');
      const message = input.value.trim();
      if (!message) return;

      btn.disabled = true;
      try {
        const r = await fetch('/api/chat', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ message })
        });
        const data = await r.json();
        if (!data.ok) {
          document.getElementById('statusError').textContent = data.error || 'Chat request failed';
        }
        input.value = '';
        await loadState();
      } catch (e) {
        document.getElementById('statusError').textContent = String(e);
      } finally {
        btn.disabled = false;
      }
    }

    async function clearChat() {
      const clearBtn = document.getElementById('clearChatBtn');
      const sendBtn = document.getElementById('chatSendBtn');
      const refreshBtn = document.getElementById('refreshStateBtn');
      clearBtn.disabled = true;
      if (sendBtn) sendBtn.disabled = true;
      if (refreshBtn) refreshBtn.disabled = true;
      document.getElementById('statusError').textContent = '';
      try {
        const r = await fetch('/api/chat/clear', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' }
        });
        const data = await r.json();
        if (!data.ok) {
          document.getElementById('statusError').textContent = data.error || 'Clear chat failed';
        }
        await loadState();
      } catch (e) {
        document.getElementById('statusError').textContent = String(e);
      } finally {
        clearBtn.disabled = false;
        if (sendBtn) sendBtn.disabled = false;
        if (refreshBtn) refreshBtn.disabled = false;
      }
    }

    document.getElementById('chatInput').addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        sendChat();
      }
    });
    document.getElementById('graphQuestionInput').addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        askGraphRag();
      }
    });

    switchTab('expert');
    loadGraphRagStatus();
    loadState();
  </script>
</body>
</html>"""
    return (
        page.replace("__KAFKA_UI_PUBLIC_URL__", KAFKA_UI_PUBLIC_URL).replace(
            "__KAFKA_PRODUCER_UI_PUBLIC_URL__", KAFKA_PRODUCER_UI_PUBLIC_URL
        ).replace(
            "__KAFKA_CONSUMER_UI_PUBLIC_URL__", KAFKA_CONSUMER_UI_PUBLIC_URL
        ).replace(
            "__GRAFANA_PUBLIC_URL__", GRAFANA_PUBLIC_URL
        ).replace(
            "__NEO4J_BROWSER_PUBLIC_URL__", NEO4J_BROWSER_PUBLIC_URL
        ).replace(
            "__NEO4J_BROWSER_LOGIN_HINT__", NEO4J_BROWSER_LOGIN_HINT
        )
    )


@app.get("/api/health")
def health() -> Any:
    """Expose runtime health and model configuration visibility for diagnostics."""
    graph_rt, graph_err = ensure_graph_runtime()
    effective_model = OPENAI_MODEL
    if runtime is not None and getattr(runtime, "model_name", ""):
        effective_model = runtime.model_name
    elif graph_rt is not None and getattr(graph_rt, "model_name", ""):
        effective_model = graph_rt.model_name
    return jsonify(
        {
            "ok": runtime is not None,
            "graph_rag_ok": graph_rt is not None,
            "openai_model": effective_model,
            "openai_model_requested": OPENAI_MODEL,
            "openai_model_fallbacks": OPENAI_MODEL_FALLBACKS,
            "openai_api_key_configured": bool(OPENAI_API_KEY),
            "prometheus_url": PROMETHEUS_URL,
            "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "schema_registry_url": SCHEMA_REGISTRY_URL,
            "neo4j_uri": NEO4J_URI,
            "neo4j_auth_mode": "none" if NEO4J_AUTH_DISABLED else "password",
            "neo4j_browser_public_url": NEO4J_BROWSER_PUBLIC_URL,
            "runtime_error": runtime_error,
            "graph_runtime_error": graph_err,
        }
    )


@app.get("/api/state")
def api_state() -> Any:
    """Return current status text, errors, and chat history snapshot."""
    return jsonify(state.snapshot())


@app.get("/api/graphrag/status")
def api_graphrag_status() -> Any:
    """Return Neo4j Graph RAG runtime health and graph cardinality counters."""
    graph_rt, graph_err = ensure_graph_runtime()
    if graph_rt is None:
        return jsonify({"ok": False, "error": graph_err or "Graph RAG runtime unavailable."}), 500
    try:
        return jsonify({"ok": True, **graph_rt.status()})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.post("/api/graphrag/ingest_pdf")
def api_graphrag_ingest_pdf() -> Any:
    """Ingest one uploaded PDF into Neo4j and create graph entities/relations."""
    graph_rt, graph_err = ensure_graph_runtime()
    if graph_rt is None:
        return jsonify({"ok": False, "error": graph_err or "Graph RAG runtime unavailable."}), 500

    uploaded = request.files.get("pdf")
    if uploaded is None:
        return jsonify({"ok": False, "error": "pdf file is required (multipart field name: pdf)"}), 400

    filename = (uploaded.filename or "uploaded.pdf").strip() or "uploaded.pdf"
    if not filename.lower().endswith(".pdf"):
        return jsonify({"ok": False, "error": "Only PDF files are supported."}), 400

    try:
        payload = uploaded.read()
        result = graph_rt.ingest_pdf(filename=filename, pdf_bytes=payload)
        answer = (
            f"1. [GOOD] PDF `{result['source_file']}` ingested into Neo4j Graph RAG.\n"
            f"2. [GOOD] Chunks processed: {result['chunks_processed']}.\n"
            f"3. [GOOD] Edges extracted and merged: {result['edges_created']}.\n"
            "4. [GOOD] Next step: ask a Kafka question in the Graph RAG tab."
        )
        return jsonify({"ok": True, "answer": answer, **result})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.post("/api/graphrag/query")
def api_graphrag_query() -> Any:
    """Answer one question using Neo4j graph context built from ingested PDFs."""
    graph_rt, graph_err = ensure_graph_runtime()
    if graph_rt is None:
        return jsonify({"ok": False, "error": graph_err or "Graph RAG runtime unavailable."}), 500

    payload = request.get_json(silent=True) or {}
    question = str(payload.get("question", "")).strip()
    if not question:
        return jsonify({"ok": False, "error": "question is required"}), 400

    try:
        answer = graph_rt.query(question)
        return jsonify({"ok": True, "answer": answer})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.post("/api/cluster_state")
def api_cluster_state() -> Any:
    """Run a full cluster-state assessment prompt and persist the result."""
    if runtime is None:
        err = runtime_error or "Kafka Expert runtime is not available."
        state.set_status(
            "Kafka Expert could not run cluster assessment because runtime initialization failed.",
            error=err,
        )
        return jsonify({"ok": False, **state.snapshot()}), 500

    try:
        answer = runtime.ask(CLUSTER_STATE_PROMPT)
        state.set_status(answer, error="")
        state.add_chat("agent", answer)
        return jsonify({"ok": True, **state.snapshot()})
    except Exception as exc:
        err = str(exc)
        state.set_status(state.snapshot().get("status_text", ""), error=err)
        return jsonify({"ok": False, "error": err, **state.snapshot()}), 500


@app.post("/api/chat")
def api_chat() -> Any:
    """Answer one Kafka-focused chat question with guardrails and tool reasoning."""
    payload = request.get_json(silent=True) or {}
    message = str(payload.get("message", "")).strip()
    if not message:
        return jsonify({"ok": False, "error": "message is required"}), 400

    state.add_chat("user", message)

    if not is_kafka_question(message):
        state.add_chat("agent", KAFKA_ONLY_MESSAGE)
        return jsonify({"ok": True, "answer": KAFKA_ONLY_MESSAGE, **state.snapshot()})

    create_request = parse_create_topic_request(message)
    if create_request is not None:
        topic, partitions, replication_factor = create_request
        try:
            answer = build_create_topic_answer(topic, partitions, replication_factor)
            state.add_chat("agent", answer)
            return jsonify({"ok": True, "answer": answer, **state.snapshot()})
        except Exception as exc:
            err = str(exc)
            state.add_chat("agent", f"Error: {err}")
            return jsonify({"ok": False, "error": err, **state.snapshot()}), 500

    if is_schema_registry_question(message):
        try:
            answer = build_schema_registry_answer()
            state.add_chat("agent", answer)
            return jsonify({"ok": True, "answer": answer, **state.snapshot()})
        except Exception as exc:
            err = str(exc)
            state.add_chat("agent", f"Error: {err}")
            return jsonify({"ok": False, "error": err, **state.snapshot()}), 500

    if is_cluster_inventory_question(message):
        try:
            answer = build_cluster_inventory_answer()
            state.add_chat("agent", answer)
            return jsonify({"ok": True, "answer": answer, **state.snapshot()})
        except Exception as exc:
            err = str(exc)
            state.add_chat("agent", f"Error: {err}")
            return jsonify({"ok": False, "error": err, **state.snapshot()}), 500

    if is_topic_inventory_question(message):
        try:
            answer = build_topic_inventory_answer()
            state.add_chat("agent", answer)
            return jsonify({"ok": True, "answer": answer, **state.snapshot()})
        except Exception as exc:
            err = str(exc)
            state.add_chat("agent", f"Error: {err}")
            return jsonify({"ok": False, "error": err, **state.snapshot()}), 500

    if runtime is None:
        err = runtime_error or "Kafka Expert runtime is not available."
        state.add_chat("agent", f"Kafka Expert is unavailable: {err}")
        return jsonify({"ok": False, "error": err, **state.snapshot()}), 500

    try:
        answer = runtime.ask(message)
        state.add_chat("agent", answer)
        return jsonify({"ok": True, "answer": answer, **state.snapshot()})
    except Exception as exc:
        err = str(exc)
        state.add_chat("agent", f"Error: {err}")
        return jsonify({"ok": False, "error": err, **state.snapshot()}), 500


@app.post("/api/auto_fix")
def api_auto_fix() -> Any:
    """Attempt automated remediation for one clicked Fix line using all available MCP tools."""
    payload = request.get_json(silent=True) or {}
    issue_line = str(payload.get("issue_line", "")).strip()
    if not issue_line:
        return jsonify({"ok": False, "error": "issue_line is required"}), 400

    state.add_chat("user", f"Auto-fix requested: {issue_line}")

    if is_prometheus_scrape_fix_issue(issue_line):
        try:
            answer = build_prometheus_scrape_fix_answer()
            state.add_chat("agent", answer)
            return jsonify({"ok": True, "answer": answer, **state.snapshot()})
        except Exception as exc:
            err = str(exc)
            state.add_chat("agent", f"Error: {err}")
            return jsonify({"ok": False, "error": err, **state.snapshot()}), 500

    if runtime is None:
        err = runtime_error or "Kafka Expert runtime is not available."
        state.add_chat("agent", f"Kafka Expert is unavailable: {err}")
        return jsonify({"ok": False, "error": err, **state.snapshot()}), 500

    try:
        prompt = AUTO_FIX_PROMPT_TEMPLATE.format(issue_line=issue_line)
        answer = runtime.ask(prompt)
        state.add_chat("agent", answer)
        return jsonify({"ok": True, "answer": answer, **state.snapshot()})
    except Exception as exc:
        err = str(exc)
        state.add_chat("agent", f"Error: {err}")
        return jsonify({"ok": False, "error": err, **state.snapshot()}), 500


@app.post("/api/chat/clear")
def api_chat_clear() -> Any:
    """Clear all chat entries while keeping the cluster status panel unchanged."""
    state.clear_chat()
    return jsonify({"ok": True, **state.snapshot()})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=UI_PORT, debug=False)
