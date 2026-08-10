#!/usr/bin/env python3
"""Flask producer UI with Avro schema editing and rate-controlled publishing.

``ProducerService`` owns Schema Registry registration, schema-aware sample
generation, one-shot sends, and the background rate loop.
"""

__author__ = "Paul Harvener"
__company__ = "Data-Blitz Inc."

import json
import os
import random
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import request as urlrequest

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from confluent_kafka import Producer, SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

try:
    from phr_generator import generate_personal_health_record
except ImportError:  # pragma: no cover - supports importing as scripts.producer_ui
    from scripts.phr_generator import generate_personal_health_record


load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092,localhost:29092,localhost:39092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.getenv("KAFKA_TOPIC", "credit-card-purchases")
SCHEMA_FILE = Path(os.getenv("SCHEMA_FILE", "schemas/credit_card_purchase.avsc"))
PHR_TOPIC = os.getenv("PHR_TOPIC", "personal-health-records")
PHR_SCHEMA_FILE = Path(os.getenv("PHR_SCHEMA_FILE", "schemas/ph-schema.json"))
DEFAULT_RATE_SECONDS = float(os.getenv("PRODUCER_RATE_SECONDS", "1.0"))
UI_PORT = int(os.getenv("PRODUCER_UI_PORT", "5050"))

SUBJECT = f"{TOPIC}-value"


def _unwrap_union(avro_type: Any) -> Any:
    """Return the first non-null branch for an Avro union value definition."""
    # Avro unions are represented as lists; pick first non-null type for generation.
    if isinstance(avro_type, list):
        non_null = [t for t in avro_type if t != "null"]
        return non_null[0] if non_null else "null"
    return avro_type


def _random_for_type(avro_type: Any, field_name: str = "") -> Any:
    """Generate a schema-compatible random value for a given Avro type node."""
    # Generate schema-compatible random values recursively for primitives and complex types.
    avro_type = _unwrap_union(avro_type)

    if isinstance(avro_type, str):
        if avro_type == "string":
            if "id" in field_name:
                return str(uuid.uuid4())
            if "time" in field_name:
                return datetime.now(timezone.utc).isoformat()
            if field_name == "currency":
                return random.choice(["USD", "CNY"])
            return f"{field_name or 'value'}-{random.randint(1000, 9999)}"
        if avro_type == "int":
            return random.randint(1, 1000)
        if avro_type == "long":
            return int(time.time() * 1000)
        if avro_type == "float":
            return round(random.uniform(1.0, 999.0), 2)
        if avro_type == "double":
            return round(random.uniform(1.0, 2000.0), 2)
        if avro_type == "boolean":
            return random.choice([True, False])
        if avro_type == "bytes":
            return b"payload"
        if avro_type == "null":
            return None
        raise ValueError(f"Unsupported primitive type: {avro_type}")

    if isinstance(avro_type, dict):
        t = _unwrap_union(avro_type.get("type"))

        if t == "record":
            out: dict[str, Any] = {}
            for field in avro_type.get("fields", []):
                out[field["name"]] = _random_for_type(field["type"], field["name"])
            return out
        if t == "array":
            item_type = avro_type["items"]
            size = random.randint(1, 3)
            return [_random_for_type(item_type, field_name) for _ in range(size)]
        if t == "map":
            value_type = avro_type["values"]
            return {
                "k1": _random_for_type(value_type, field_name),
                "k2": _random_for_type(value_type, field_name),
            }
        if t == "enum":
            symbols = avro_type.get("symbols", [])
            if not symbols:
                raise ValueError("Enum has no symbols")
            return random.choice(symbols)
        if t == "fixed":
            size = int(avro_type.get("size", 4))
            return bytes([65]) * size

        if isinstance(t, str):
            return _random_for_type(t, field_name)

    raise ValueError(f"Unsupported Avro type shape: {avro_type}")


class ProducerService:
    """Stateful producer service used by the Flask UI endpoints."""

    def __init__(self) -> None:
        """Initialize clients, ensure topic existence, and prepare serializer/producer."""
        # Initialize clients/state once and build producer from current schema.
        self.schema_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        self.admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
        self.lock = threading.Lock()
        self.running = False
        self.rate_seconds = DEFAULT_RATE_SECONDS
        self.profile = "credit_card"
        self.topic = TOPIC
        self.sent_count = 0
        self.last_error = ""
        self.last_message: dict[str, Any] | None = None
        self.last_sent_at = ""
        self._schema_mtime = 0.0
        self._schema_str = ""
        self._schema_dict: dict[str, Any] = {}
        self._producer: SerializingProducer | None = None
        self._json_producer: Producer | None = None
        self._worker: threading.Thread | None = None
        self.ensure_topic()
        self.reload_schema_and_producer(force=True)

    def ensure_topic(self, topic: str | None = None) -> None:
        """Create the demo topic if needed, tolerating already-exists responses."""
        # Create topic on first run; ignore already-exists condition.
        topic_name = topic or self.topic
        topic_cfg = NewTopic(topic=topic_name, num_partitions=3, replication_factor=3)
        fut = self.admin.create_topics([topic_cfg]).get(topic_name)
        if fut is not None:
            try:
                fut.result()
            except Exception as exc:  # topic exists is expected in most runs
                if "TOPIC_ALREADY_EXISTS" not in str(exc):
                    raise

    def schema_text(self) -> str:
        """Read the active profile's schema/documentation text from disk."""
        path = SCHEMA_FILE if self.profile == "credit_card" else PHR_SCHEMA_FILE
        return path.read_text(encoding="utf-8")

    def save_schema(self, schema_text: str) -> None:
        """Persist edited schema JSON and rebuild producer/serializer state."""
        # Persist schema updates from UI then hot-reload serializer/producer.
        parsed = json.loads(schema_text)
        if not isinstance(parsed, dict):
            raise ValueError("Schema must be a JSON object")
        path = SCHEMA_FILE if self.profile == "credit_card" else PHR_SCHEMA_FILE
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(parsed, indent=2) + "\n", encoding="utf-8")
        self.reload_schema_and_producer(force=True)

    def register_schema(self) -> int:
        """Register the current schema under `<topic>-value` and return schema id."""
        if self.profile != "credit_card":
            raise ValueError("Personal Health Record payloads are JSON and do not use Schema Registry")
        # Register current schema as the next version for this subject.
        self._set_subject_compatibility_none()
        schema = Schema(self._schema_str, schema_type="AVRO")
        return self.schema_client.register_schema(subject_name=SUBJECT, schema=schema)

    def _set_subject_compatibility_none(self) -> None:
        """Set Schema Registry compatibility to NONE for rapid demo iterations."""
        # Allow rapid schema edits in the demo by disabling compatibility checks for subject.
        url = f"{SCHEMA_REGISTRY_URL.rstrip('/')}/config/{SUBJECT}"
        data = json.dumps({"compatibility": "NONE"}).encode("utf-8")
        req = urlrequest.Request(
            url=url,
            data=data,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            method="PUT",
        )
        with urlrequest.urlopen(req, timeout=5):
            pass

    def reload_schema_and_producer(self, force: bool = False) -> None:
        """Recreate the active profile's serializer/producer when its schema changes."""
        # Recreate Avro serializer/producer when schema changes on disk.
        with self.lock:
            schema_path = SCHEMA_FILE if self.profile == "credit_card" else PHR_SCHEMA_FILE
            mtime = schema_path.stat().st_mtime if schema_path.exists() else 0.0
            if not force and mtime == self._schema_mtime and self._producer is not None:
                return
            self._schema_mtime = mtime
            self._schema_str = self.schema_text()
            self._schema_dict = json.loads(self._schema_str)

            if self.profile == "phr":
                self._producer = None
                if self._json_producer is None:
                    self._json_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
                return

            serializer = AvroSerializer(
                schema_registry_client=self.schema_client,
                schema_str=self._schema_str,
                conf={"auto.register.schemas": False},
            )
            self._producer = SerializingProducer(
                {
                    "bootstrap.servers": BOOTSTRAP_SERVERS,
                    "key.serializer": StringSerializer("utf_8"),
                    "value.serializer": serializer,
                }
            )

    def set_profile(self, profile: str) -> None:
        """Switch between credit-card Avro and synthetic PHR JSON publishing."""
        normalized = (profile or "").strip().lower().replace("-", "_")
        if normalized in {"personal_health_record", "personal_health_records", "health", "phr"}:
            normalized = "phr"
        if normalized not in {"credit_card", "phr"}:
            raise ValueError("profile must be credit_card or phr")
        with self.lock:
            self.profile = normalized
            self.topic = PHR_TOPIC if normalized == "phr" else TOPIC
            self._schema_mtime = 0.0
        self.ensure_topic(self.topic)
        self.reload_schema_and_producer(force=True)

    def generate_message(self) -> dict[str, Any]:
        """Generate one record payload for the active producer profile."""
        if self.profile == "phr":
            return generate_personal_health_record()
        # Generate one payload that conforms to current top-level record schema.
        root_type = self._schema_dict.get("type")
        if root_type != "record":
            raise ValueError("Top-level Avro schema must be a record")
        return _random_for_type(self._schema_dict, self._schema_dict.get("name", "record"))

    def send_one(self) -> dict[str, Any]:
        """Produce one message synchronously and return the published payload."""
        # Produce exactly one message and block until delivery completes.
        self.reload_schema_and_producer()
        message = self.generate_message()
        key = str(message.get("recordId" if self.profile == "phr" else "purchase_id", uuid.uuid4()))

        def _delivery(err, msg) -> None:
            """Capture asynchronous delivery errors for UI status reporting."""
            if err is not None:
                self.last_error = str(err)

        with self.lock:
            if self.profile == "phr":
                assert self._json_producer is not None
                self._json_producer.produce(
                    topic=self.topic,
                    key=key,
                    value=json.dumps(message).encode("utf-8"),
                    on_delivery=_delivery,
                )
                self._json_producer.flush(10)
            else:
                assert self._producer is not None
                self._producer.produce(topic=self.topic, key=key, value=message, on_delivery=_delivery)
                self._producer.flush(10)
            self.sent_count += 1
            self.last_message = message
            self.last_sent_at = datetime.now(timezone.utc).isoformat()
        return message

    def start(self, rate_seconds: float) -> None:
        """Start continuous production at the requested seconds interval."""
        # Start background publishing loop at user-selected interval.
        with self.lock:
            self.rate_seconds = max(0.05, float(rate_seconds))
            if self.running:
                return
            self.running = True
            self._worker = threading.Thread(target=self._run_loop, daemon=True)
            self._worker.start()

    def stop(self) -> None:
        """Stop continuous production loop."""
        # Stop background publishing loop.
        with self.lock:
            self.running = False

    def _run_loop(self) -> None:
        """Background publisher loop used by start/stop rate controls."""
        # Background loop used for continuous rate-based publishing.
        while True:
            with self.lock:
                if not self.running:
                    break
                rate = self.rate_seconds
            try:
                self.send_one()
            except Exception as exc:
                self.last_error = str(exc)
            time.sleep(rate)

    def status(self) -> dict[str, Any]:
        """Return producer runtime state consumed by UI polling."""
        with self.lock:
            return {
                "running": self.running,
                "rate_seconds": self.rate_seconds,
                "sent_count": self.sent_count,
                "last_error": self.last_error,
                "last_message": self.last_message,
                "last_sent_at": self.last_sent_at,
                "profile": self.profile,
                "topic": self.topic,
                "subject": f"{self.topic}-value" if self.profile == "credit_card" else None,
                "schema_file": str(SCHEMA_FILE if self.profile == "credit_card" else PHR_SCHEMA_FILE),
            }


app = Flask(__name__)
svc = ProducerService()


@app.get("/")
def index() -> str:
    """Serve the single-page producer control and schema editor UI."""
    # Single-page UI with send controls, schema editor, and preview panel.
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Kafka Producer UI</title>
  <style>
    :root {{
      --bg: #0b1117;
      --panel: #111a23;
      --ink: #e6eff6;
      --ink-dim: #9ab0c1;
      --line: #2b3d4d;
    }}
    body {{
      font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
      margin: 24px;
      background:
        radial-gradient(circle at 12% 8%, #143f38 0%, #143f38 14%, transparent 46%),
        radial-gradient(circle at 86% 2%, #1b2f49 0%, #1b2f49 16%, transparent 40%),
        var(--bg);
      color: var(--ink);
    }}
    h1 {{ margin-top: 0; }}
    .row {{ display: flex; gap: 16px; align-items: center; flex-wrap: wrap; margin-bottom: 12px; }}
    .panel {{ background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 16px; margin-bottom: 16px; }}
    .green {{ background: #16a34a; color: #fff; border: none; padding: 10px 14px; border-radius: 6px; font-weight: 600; cursor: pointer; }}
    .blue {{ background: #2563eb; color: #fff; border: none; padding: 8px 12px; border-radius: 6px; cursor: pointer; }}
    .red {{ background: #dc2626; color: #fff; border: none; padding: 8px 12px; border-radius: 6px; cursor: pointer; }}
    textarea {{
      width: 100%;
      height: 320px;
      font-family: Menlo, monospace;
      font-size: 12px;
      background: #0d161f;
      color: var(--ink);
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 10px;
    }}
    input[type=number] {{
      width: 120px;
      padding: 6px;
      background: #0d161f;
      color: var(--ink);
      border: 1px solid var(--line);
      border-radius: 6px;
    }}
    code {{ background: #162430; color: #d9edf7; padding: 2px 5px; border-radius: 4px; }}
    pre {{ background: #111827; color: #e5e7eb; padding: 12px; border-radius: 8px; overflow: auto; max-height: 300px; }}
    .status {{ font-size: 14px; color: var(--ink-dim); }}
  </style>
</head>
<body>
  <h1>Kafka Producer UI</h1>
  <div class="panel">
    <div class="row">
      <label>Record profile:
        <select id="profile" onchange="setProfile()">
          <option value="credit_card">Credit card purchase (Avro)</option>
          <option value="phr">Synthetic personal health record (JSON)</option>
        </select>
      </label>
      <button class="green" onclick="sendOne()">Send One Message</button>
      <label>Rate (seconds): <input id="rate" type="number" step="0.1" min="0.05" value="{DEFAULT_RATE_SECONDS}"></label>
      <button class="blue" onclick="startRate()">Start Rate Send</button>
      <button class="red" onclick="stopRate()">Stop</button>
    </div>
    <div class="status" id="status">Loading status...</div>
  </div>

  <div class="panel" id="schemaPanel">
    <h3>Schema Editor</h3>
    <div class="status">Credit-card messages use Avro + Schema Registry. PHR messages are synthetic JSON and contain no real patient data.</div>
    <div class="row">
      <button id="saveSchemaButton" class="blue" onclick="saveSchema()">Save + Register Schema</button>
      <button class="blue" onclick="previewMessage()">Preview Generated Message</button>
    </div>
    <textarea id="schema"></textarea>
  </div>

  <div class="panel">
    <h3>Latest Generated Message</h3>
    <pre id="message">(none)</pre>
  </div>

  <script>
    async function loadSchema() {{
      const r = await fetch('/api/schema');
      const data = await r.json();
      document.getElementById('schema').value = data.schema;
    }}

    async function loadStatus() {{
      const r = await fetch('/api/status');
      const s = await r.json();
      document.getElementById('status').innerHTML =
        'Profile: <code>' + (s.profile || 'credit_card') + '</code> | ' +
        'Running: <code>' + s.running + '</code> | ' +
        'Rate: <code>' + s.rate_seconds + 's</code> | ' +
        'Sent: <code>' + s.sent_count + '</code> | ' +
        'Topic: <code>' + s.topic + '</code> | ' +
        'Subject: <code>' + s.subject + '</code> | ' +
        'Last sent at: <code>' + (s.last_sent_at || 'n/a') + '</code> | ' +
        'Last error: <code>' + (s.last_error || 'none') + '</code>';
      const profile = document.getElementById('profile');
      if (profile && profile.value !== (s.profile || 'credit_card')) profile.value = s.profile || 'credit_card';
      applyProfileUi(s.profile || 'credit_card');
      if (s.last_message) {{
        document.getElementById('message').textContent = JSON.stringify(s.last_message, null, 2);
      }}
    }}

    async function sendOne() {{
      const r = await fetch('/api/send_once', {{method:'POST'}});
      const data = await r.json();
      if (!r.ok) {{ alert(data.error || 'Failed to send message'); return; }}
      document.getElementById('message').textContent = JSON.stringify(data.message || data, null, 2);
      loadStatus();
    }}

    async function setProfile() {{
      const profile = document.getElementById('profile').value;
      const r = await fetch('/api/profile', {{
        method:'POST', headers: {{'Content-Type':'application/json'}},
        body: JSON.stringify({{profile}})
      }});
      const data = await r.json();
      if (!r.ok) {{ alert(data.error || 'Failed to select profile'); return; }}
      await loadSchema();
      loadStatus();
    }}

    function applyProfileUi(profile) {{
      const saveButton = document.getElementById('saveSchemaButton');
      if (!saveButton) return;
      const isPhr = profile === 'phr';
      saveButton.disabled = isPhr;
      saveButton.title = isPhr ? 'PHR payloads are JSON and are not registered in Schema Registry' : '';
    }}

    async function startRate() {{
      const rate = parseFloat(document.getElementById('rate').value);
      await fetch('/api/start', {{
        method:'POST',
        headers: {{'Content-Type':'application/json'}},
        body: JSON.stringify({{rate_seconds: rate}})
      }});
      loadStatus();
    }}

    async function stopRate() {{
      await fetch('/api/stop', {{method:'POST'}});
      loadStatus();
    }}

    async function saveSchema() {{
      const schema = document.getElementById('schema').value;
      const r = await fetch('/api/schema', {{
        method:'POST',
        headers: {{'Content-Type':'application/json'}},
        body: JSON.stringify({{schema}})
      }});
      const data = await r.json();
      if (!r.ok) {{
        alert(data.error || 'Failed to save schema');
      }}
      loadStatus();
    }}

    async function previewMessage() {{
      const r = await fetch('/api/preview');
      const data = await r.json();
      document.getElementById('message').textContent = JSON.stringify(data.message || data, null, 2);
    }}

    loadSchema();
    loadStatus();
    setInterval(loadStatus, 1000);
  </script>
</body>
</html>"""


@app.get("/api/schema")
def get_schema():
    """Return current schema text for editor initialization and refresh."""
    # Return current schema text for editor initialization/refresh.
    try:
        return jsonify({"schema": svc.schema_text()})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.post("/api/profile")
def set_profile():
    """Select the credit-card Avro or synthetic PHR publishing profile."""
    payload = request.get_json(silent=True) or {}
    try:
        svc.set_profile(str(payload.get("profile", "")))
        return jsonify({"ok": True, **svc.status()})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


@app.post("/api/schema")
def set_schema():
    """Persist edited schema and register a new schema version in Schema Registry."""
    # Save edited schema, register it, and return resulting schema id.
    payload = request.get_json(silent=True) or {}
    schema_text = payload.get("schema", "")
    try:
        if svc.profile != "credit_card":
            raise ValueError("PHR payloads are JSON and are not registered in Schema Registry")
        svc.save_schema(schema_text)
        schema_id = svc.register_schema()
        return jsonify({"ok": True, "schema_id": schema_id, "subject": SUBJECT})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


@app.get("/api/preview")
def preview():
    """Generate and return a sample message without publishing to Kafka."""
    # Generate sample payload from current schema without publishing.
    try:
        svc.reload_schema_and_producer()
        msg = svc.generate_message()
        return jsonify({"message": msg})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


@app.post("/api/send_once")
def send_once():
    """Publish one Avro message and return the sent payload."""
    # Produce a single Avro message immediately.
    try:
        msg = svc.send_one()
        return jsonify({"ok": True, "message": msg})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.post("/api/start")
def start():
    """Enable continuous producer mode with the requested interval."""
    # Start continuous producer mode using requested rate in seconds.
    payload = request.get_json(silent=True) or {}
    rate_seconds = payload.get("rate_seconds", DEFAULT_RATE_SECONDS)
    try:
        svc.start(float(rate_seconds))
        return jsonify({"ok": True, **svc.status()})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400


@app.post("/api/stop")
def stop():
    """Disable continuous producer mode."""
    # Stop continuous producer mode.
    svc.stop()
    return jsonify({"ok": True, **svc.status()})


@app.get("/api/status")
def status():
    """Return producer runtime status used by the browser polling loop."""
    # Expose runtime status for UI polling.
    return jsonify(svc.status())


if __name__ == "__main__":
    print(f"Producer UI listening on http://localhost:{UI_PORT}")
    app.run(host="0.0.0.0", port=UI_PORT, debug=False)
