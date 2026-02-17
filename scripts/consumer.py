#!/usr/bin/env python3
"""
Kafka Expert Demo
Copyright (c) 2026 Paul Harvener, Data-Blitz Inc
SPDX-License-Identifier: MIT
"""

import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from confluent_kafka import DeserializingConsumer
from confluent_kafka.error import ConsumeError
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from couchdb_writer import CouchDBWriter


def main() -> None:
    """Run a Kafka Avro consumer and persist each consumed record to CouchDB."""
    # Load runtime config for brokers, schema registry, topic, group, and CouchDB sink.
    load_dotenv()

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092,localhost:29092,localhost:39092")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    topic = os.getenv("KAFKA_TOPIC", "credit-card-purchases")
    group_id = os.getenv("KAFKA_GROUP_ID", "credit-card-consumer")
    couchdb = CouchDBWriter.from_env()

    # Configure Avro deserializer backed by Schema Registry.
    schema_client = SchemaRegistryClient({"url": schema_registry_url})
    avro_deserializer = AvroDeserializer(schema_registry_client=schema_client)

    # Create Kafka consumer that reads string keys and Avro values.
    consumer = DeserializingConsumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": avro_deserializer,
        }
    )

    consumer.subscribe([topic])

    print(f"Consuming from topic={topic}; writing consumed messages to CouchDB db={couchdb.db_name}")
    print("Press Ctrl+C to stop")

    try:
        while True:
            try:
                # Poll continuously for records.
                msg = consumer.poll(1.0)
            except ConsumeError as exc:
                # Topic may not exist yet during startup; keep waiting.
                if "UNKNOWN_TOPIC_OR_PART" in str(exc):
                    continue
                raise
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            # Convert Avro payload and persist to CouchDB.
            value = msg.value()
            consumed_at = datetime.now(timezone.utc).isoformat()
            kafka_timestamp_type, kafka_timestamp_ms = msg.timestamp()
            if kafka_timestamp_ms is not None and kafka_timestamp_ms < 0:
                kafka_timestamp_ms = None

            # Also persist each consumed message into CouchDB.
            couchdb.write_message(
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                key=msg.key(),
                consumed_at=str(consumed_at),
                latency_ms=None,
                kafka_timestamp_ms=kafka_timestamp_ms,
                kafka_timestamp_type=kafka_timestamp_type,
                message=value,
            )

            print(
                f"Consumed topic={msg.topic()} partition={msg.partition()} "
                f"offset={msg.offset()} key={msg.key()}"
            )
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
