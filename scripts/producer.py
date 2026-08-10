#!/usr/bin/env python3
"""CLI Kafka producer for credit-card Avro or synthetic PHR JSON events.

Set ``PRODUCER_PROFILE=phr`` to publish synthetic personal health records to
``PHR_TOPIC`` (``personal-health-records`` by default).
"""

__author__ = "Paul Harvener"
__company__ = "Data-Blitz Inc."

import os
import random
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
from confluent_kafka import Producer, SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from phr_generator import generate_personal_health_record


PURCHASE_CURRENCIES = ["USD", "CNY"]


def delivery_report(err, msg) -> None:
    """Log Kafka delivery success/failure for produced records."""
    if err is not None:
        print(f"Delivery failed: {err}")
        return
    print(
        f"Produced topic={msg.topic()} partition={msg.partition()} "
        f"offset={msg.offset()} key={msg.key()}"
    )


def make_purchase() -> dict:
    """Generate a mock credit-card purchase payload matching the Avro schema."""
    # Build a realistic mock purchase payload aligned to the Avro schema fields.
    brands = ["VISA", "MASTERCARD", "AMEX", "DISCOVER"]
    merchants = ["Amazon", "Walmart", "Target", "Costco", "Best Buy", "Shell"]
    categories = ["grocery", "fuel", "retail", "travel", "electronics", "food"]
    cities = ["Austin", "Seattle", "Denver", "Boston", "Atlanta", "Phoenix"]
    states = ["TX", "WA", "CO", "MA", "GA", "AZ"]

    return {
        "purchase_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "card_last4": f"{random.randint(0, 9999):04d}",
        "card_brand": random.choice(brands),
        "merchant": random.choice(merchants),
        "category": random.choice(categories),
        "city": random.choice(cities),
        "state": random.choice(states),
        "amount": round(random.uniform(3.5, 1200.0), 2),
        "currency": random.choice(PURCHASE_CURRENCIES),
        "is_card_present": random.choice([True, False]),
    }


def main() -> None:
    """Run the continuous Kafka producer loop until interrupted."""
    # Load .env settings for Kafka endpoints, schema registry, topic, and send rate.
    load_dotenv()

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092,localhost:29092,localhost:39092")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    profile = os.getenv("PRODUCER_PROFILE", "credit_card").strip().lower().replace("-", "_")
    if profile in {"personal_health_record", "personal_health_records", "health"}:
        profile = "phr"
    topic = os.getenv("PHR_TOPIC" if profile == "phr" else "KAFKA_TOPIC", "personal-health-records" if profile == "phr" else "credit-card-purchases")
    schema_path = Path(os.getenv("SCHEMA_FILE", "schemas/credit_card_purchase.avsc"))
    rate_seconds = float(os.getenv("PRODUCER_RATE_SECONDS", "1.0"))

    if profile == "phr":
        # PHR records follow schemas/ph-schema.json and are sent as JSON bytes;
        # JSON Schema is intentionally not registered as an Avro subject.
        producer = Producer({"bootstrap.servers": bootstrap_servers})
    else:
        # Load the schema used by AvroSerializer and connect to Schema Registry.
        schema_str = schema_path.read_text(encoding="utf-8")
        schema_client = SchemaRegistryClient({"url": schema_registry_url})
        avro_serializer = AvroSerializer(
            schema_registry_client=schema_client,
            schema_str=schema_str,
            conf={"auto.register.schemas": False},
        )
        producer = SerializingProducer(
            {
                "bootstrap.servers": bootstrap_servers,
                "key.serializer": StringSerializer("utf_8"),
                "value.serializer": avro_serializer,
            }
        )

    # Ensure topic exists before publishing so first run does not fail.
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    topic_config = NewTopic(topic=topic, num_partitions=3, replication_factor=3)
    create_futures = admin.create_topics([topic_config])
    topic_future = create_futures.get(topic)
    if topic_future is not None:
        try:
            topic_future.result()
            print(f"Created topic={topic}")
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise

    print(f"Producing profile={profile} to topic={topic} every {rate_seconds} second(s)")
    print("Press Ctrl+C to stop")

    try:
        while True:
            # Generate one record, publish it, then wait based on configured interval.
            record = generate_personal_health_record() if profile == "phr" else make_purchase()
            key = record["recordId"] if profile == "phr" else record["purchase_id"]
            value = json.dumps(record).encode("utf-8") if profile == "phr" else record
            producer.produce(topic=topic, key=key, value=value, on_delivery=delivery_report)
            producer.poll(0)
            time.sleep(rate_seconds)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
