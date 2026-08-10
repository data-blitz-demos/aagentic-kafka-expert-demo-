#!/usr/bin/env python3
"""Role-based Kafka Admin tool selection for the two-agent demo."""

__author__ = "Paul Harvener"
__company__ = "Data-Blitz Inc."

from typing import Any


READ_ONLY_KAFKA_ADMIN_TOOLS = {
    "kafka_admin_capabilities",
    "kafka_describe_cluster",
    "kafka_list_topics",
    "kafka_describe_topic",
    "kafka_list_consumer_groups",
    "kafka_describe_consumer_groups",
    "kafka_describe_topic_configs",
}

READ_ONLY_KAFKA_REST_TOOLS = {
    "kafka_rest_proxy_health",
    "kafka_rest_list_topics",
    "kafka_rest_topic_metadata",
}


def select_kafka_admin_tools(tools: list[Any], *, allow_mutations: bool) -> list[Any]:
    """Give the monitor only allowlisted reads; remediation receives the full tool set."""
    if allow_mutations:
        return list(tools)
    return [tool for tool in tools if str(getattr(tool, "name", "")) in READ_ONLY_KAFKA_ADMIN_TOOLS]


def select_kafka_rest_tools(tools: list[Any], *, allow_mutations: bool) -> list[Any]:
    """Give both agents REST reads; only the Doer receives REST production."""
    if allow_mutations:
        return list(tools)
    return [tool for tool in tools if str(getattr(tool, "name", "")) in READ_ONLY_KAFKA_REST_TOOLS]
