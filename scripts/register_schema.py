#!/usr/bin/env python3
"""
Kafka Expert Demo
Copyright (c) 2026 Paul Harvener, Data-Blitz Inc
SPDX-License-Identifier: MIT
"""

import os
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

    # Print schema id so operators can verify the active version quickly.
    schema_id = client.register_schema(subject_name=subject, schema=schema)
    print(f"Registered schema subject={subject} id={schema_id}")


if __name__ == "__main__":
    main()
