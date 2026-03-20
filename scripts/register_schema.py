#!/usr/bin/env python3
"""
Kafka Expert Demo
Copyright (c) 2026 Paul Harvener, Data-Blitz Inc
SPDX-License-Identifier: MIT
"""

import os
import json
import urllib.request
from urllib.error import HTTPError
from pathlib import Path

from dotenv import load_dotenv
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient


def main() -> None:
    """Register the configured Avro schema under the Kafka topic value subject."""
    # Load runtime configuration from .env so local and Compose runs share config.
    load_dotenv()

    # Resolve Schema Registry endpoint, topic, and schema file path.
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    topic = os.getenv("KAFKA_TOPIC", "credit-card-purchases")
    subject = f"{topic}-value"
    schema_path = Path(os.getenv("SCHEMA_FILE", "schemas/credit_card_purchase.avsc"))

    # Read Avro schema from disk and register it under <topic>-value subject.
    schema_str = schema_path.read_text(encoding="utf-8")
    client = SchemaRegistryClient({"url": schema_registry_url})
    schema = Schema(schema_str, schema_type="AVRO")
    _set_subject_compatibility_none(subject, schema_registry_url)

    # Print schema id so operators can verify the active version quickly.
    schema_id = client.register_schema(subject_name=subject, schema=schema)
    print(f"Registered schema subject={subject} id={schema_id}")


def _set_subject_compatibility_none(
    subject: str, schema_registry_url: str
) -> None:
    """Set the subject compatibility to NONE so bootstrap can recover from old local schema versions."""
    url = f"{schema_registry_url.rstrip('/')}/config/{subject}"
    req = urllib.request.Request(
        url=url,
        data=json.dumps({"compatibility": "NONE"}).encode("utf-8"),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="PUT",
    )
    try:
        with urllib.request.urlopen(req, timeout=5):
            pass
    except HTTPError:
        # Compatibility endpoint may be unavailable in older registry builds.
        # Keep bootstrap resilient for existing demo environments; schema registration
        # will fail with an explicit error if server-side restrictions are enforced.
        pass


if __name__ == "__main__":
    main()
