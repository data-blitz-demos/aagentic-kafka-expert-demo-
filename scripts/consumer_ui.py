#!/usr/bin/env python3
"""
Kafka Expert Demo
Copyright (c) 2026 Paul Harvener, Data-Blitz Inc
SPDX-License-Identifier: MIT
"""

import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from flask import Flask, jsonify
from confluent_kafka import DeserializingConsumer
from confluent_kafka.error import ConsumeError
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from couchdb_writer import CouchDBWriter


load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092,localhost:29092,localhost:39092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.getenv("KAFKA_TOPIC", "credit-card-purchases")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "credit-card-consumer")
UI_GROUP_ID = os.getenv("CONSUMER_UI_GROUP_ID", f"{GROUP_ID}-ui")
UI_PORT = int(os.getenv("CONSUMER_UI_PORT", "5051"))
MAX_MESSAGES = int(os.getenv("CONSUMER_UI_MAX_MESSAGES", "200"))


class ConsumerFeed:
    """Background Kafka consumer feeding a UI-friendly in-memory message stream."""

    def __init__(self) -> None:
        """Initialize runtime counters, in-memory feed, and CouchDB writer."""
        # Keep an in-memory ring buffer for UI display and operational counters.
        self.lock = threading.Lock()
        self.messages: list[dict[str, Any]] = []
        self.total_consumed = 0
        self.poll_count = 0
        self.last_poll_at = ""
        self.last_error = ""
        self.running = False
        self.thread: threading.Thread | None = None
        self.couchdb = CouchDBWriter.from_env()

    def start(self) -> None:
        """Start the background consumer thread if it is not already running."""
        # Start the background consumer thread once.
        with self.lock:
            if self.running:
                return
            self.running = True
            self.thread = threading.Thread(target=self._consume_loop, daemon=True)
            self.thread.start()

    def _consume_loop(self) -> None:
        """Continuously poll Kafka, compute latency, cache records, and persist to CouchDB."""
        # Consume Avro records continuously, persist to CouchDB, and cache for UI.
        try:
            schema_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
            avro_deserializer = AvroDeserializer(schema_registry_client=schema_client)
            consumer = DeserializingConsumer(
                {
                    "bootstrap.servers": BOOTSTRAP_SERVERS,
                    "group.id": UI_GROUP_ID,
                    "auto.offset.reset": "earliest",
                    "key.deserializer": StringDeserializer("utf_8"),
                    "value.deserializer": avro_deserializer,
                }
            )
            consumer.subscribe([TOPIC])

            while True:
                try:
                    # Poll Kafka and track poll heartbeat for diagnostics.
                    msg = consumer.poll(1.0)
                    with self.lock:
                        self.poll_count += 1
                        self.last_poll_at = datetime.now(timezone.utc).isoformat()
                except ConsumeError as exc:
                    if "UNKNOWN_TOPIC_OR_PART" in str(exc):
                        continue
                    with self.lock:
                        self.last_error = str(exc)
                    continue

                try:
                    if msg is None:
                        continue
                    if msg.error():
                        with self.lock:
                            self.last_error = str(msg.error())
                        continue

                    # Compute end-to-end latency using event_time from producer payload.
                    value = msg.value()
                    consumed_at = datetime.now(timezone.utc)
                    latency_ms = self._latency_ms(value, consumed_at)
                    kafka_timestamp_type, kafka_timestamp_ms = msg.timestamp()
                    if kafka_timestamp_ms is not None and kafka_timestamp_ms < 0:
                        kafka_timestamp_ms = None

                    # Push latest message to top of in-memory feed shown in UI.
                    entry = {
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "key": msg.key(),
                        "consumed_at": consumed_at.isoformat(),
                        "latency_ms": latency_ms,
                        "kafka_timestamp_type": kafka_timestamp_type,
                        "kafka_timestamp_ms": kafka_timestamp_ms,
                        "message": value,
                    }

                    # Persist consumed payload to CouchDB for queryable storage.
                    self.couchdb.write_message(
                        topic=entry["topic"],
                        partition=entry["partition"],
                        offset=entry["offset"],
                        key=entry["key"],
                        consumed_at=entry["consumed_at"],
                        latency_ms=entry["latency_ms"],
                        kafka_timestamp_ms=entry["kafka_timestamp_ms"],
                        kafka_timestamp_type=entry["kafka_timestamp_type"],
                        message=value,
                    )

                    with self.lock:
                        self.total_consumed += 1
                        self.messages.insert(0, entry)
                        if len(self.messages) > MAX_MESSAGES:
                            self.messages = self.messages[:MAX_MESSAGES]
                except Exception as exc:
                    with self.lock:
                        self.last_error = f"message-processing: {exc}"
        except Exception as exc:
            with self.lock:
                self.last_error = f"consumer-loop: {exc}"
        finally:
            try:
                consumer.close()
            except Exception:
                pass

    def _latency_ms(self, message: dict[str, Any], consumed_at: datetime) -> int | None:
        """Compute non-negative producer-to-consumer latency in milliseconds."""
        # Parse producer event_time and return non-negative latency in milliseconds.
        event_time = message.get("event_time")
        if not isinstance(event_time, str):
            return None
        try:
            produced_at = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
            if produced_at.tzinfo is None:
                produced_at = produced_at.replace(tzinfo=timezone.utc)
            delta = consumed_at - produced_at
            return max(0, int(delta.total_seconds() * 1000))
        except Exception:
            return None

    def snapshot(self) -> dict[str, Any]:
        """Return an atomic snapshot consumed by the browser polling endpoint."""
        # Return atomic snapshot consumed by API/UI polling.
        with self.lock:
            return {
                "running": self.running,
                "thread_alive": bool(self.thread and self.thread.is_alive()),
                "topic": TOPIC,
                "group_id": UI_GROUP_ID,
                "total_consumed": self.total_consumed,
                "poll_count": self.poll_count,
                "last_poll_at": self.last_poll_at,
                "last_error": self.last_error,
                "couchdb_enabled": self.couchdb.enabled,
                "couchdb_url": self.couchdb.url,
                "couchdb_db": self.couchdb.db_name,
                "couchdb_writes_ok": self.couchdb.writes_ok,
                "couchdb_last_error": self.couchdb.last_error,
                "messages": list(self.messages),
            }


app = Flask(__name__)
feed = ConsumerFeed()
feed.start()


@app.get("/")
def index() -> str:
    """Serve the single-page consumer monitoring UI."""
    # Single-page UI that renders status + pretty JSON message feed.
    return """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Kafka Consumer UI</title>
  <style>
    :root {
      --bg: #0b1117;
      --panel: #111a23;
      --ink: #e6eff6;
      --ink-dim: #9ab0c1;
      --line: #2b3d4d;
    }
    body {
      font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
      margin: 24px;
      background:
        radial-gradient(circle at 12% 8%, #143f38 0%, #143f38 14%, transparent 46%),
        radial-gradient(circle at 86% 2%, #1b2f49 0%, #1b2f49 16%, transparent 40%),
        var(--bg);
      color: var(--ink);
    }
    h1 { margin-top: 0; }
    .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 16px; margin-bottom: 16px; }
    .status { font-size: 14px; line-height: 1.6; color: var(--ink-dim); }
    .msg { border: 1px solid var(--line); border-radius: 8px; background: #12212d; margin-bottom: 12px; }
    .msg-head { padding: 10px 12px; background: #162430; border-bottom: 1px solid var(--line); font-size: 13px; color: var(--ink-dim); }
    .msg-body { margin: 0; padding: 12px; background: #0d161f; color: #e5e7eb; border-radius: 0 0 8px 8px; overflow: auto; }
    code { background: #162430; color: #d9edf7; padding: 2px 5px; border-radius: 4px; }
  </style>
</head>
<body>
  <h1>Kafka Avro Consumer UI</h1>
  <div class="panel">
    <div class="status" id="status">Loading...</div>
  </div>
  <div id="messages"></div>

  <script>
    function esc(s) {
      return String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');
    }

    async function refresh() {
      const r = await fetch('/api/messages');
      const data = await r.json();
      document.getElementById('status').innerHTML =
        'Running: <code>' + data.running + '</code> | ' +
        'Topic: <code>' + data.topic + '</code> | ' +
        'Group: <code>' + data.group_id + '</code> | ' +
        'Total consumed: <code>' + data.total_consumed + '</code> | ' +
        'CouchDB: <code>' + (data.couchdb_enabled ? (data.couchdb_db + ' (' + data.couchdb_writes_ok + ')') : 'disabled') + '</code> | ' +
        'CouchDB error: <code>' + (data.couchdb_last_error || 'none') + '</code> | ' +
        'Last error: <code>' + (data.last_error || 'none') + '</code>';

      const container = document.getElementById('messages');
      container.innerHTML = '';
      for (const m of data.messages) {
        const head =
          'partition=' + m.partition +
          ' offset=' + m.offset +
          ' key=' + m.key +
          ' consumed_at=' + m.consumed_at +
          ' latency_ms=' + (m.latency_ms === null ? 'n/a' : m.latency_ms);
        const msgHtml =
          '<div class="msg">' +
            '<div class="msg-head">' + esc(head) + '</div>' +
            '<pre class="msg-body">' + esc(JSON.stringify(m.message, null, 2)) + '</pre>' +
          '</div>';
        container.insertAdjacentHTML('beforeend', msgHtml);
      }
    }

    refresh();
    setInterval(refresh, 1000);
  </script>
</body>
</html>"""


@app.get("/api/messages")
def api_messages():
    """Return the current message feed and runtime status as JSON."""
    # Return current feed snapshot including latency and metadata.
    return jsonify(feed.snapshot())


if __name__ == "__main__":
    print(f"Consumer UI listening on http://localhost:{UI_PORT}")
    app.run(host="0.0.0.0", port=UI_PORT, debug=False)
