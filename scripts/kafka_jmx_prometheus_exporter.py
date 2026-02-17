#!/usr/bin/env python3
"""
Poll Kafka JMX via JmxTool and expose numeric attributes as Prometheus metrics.
"""

import csv
import io
import os
import subprocess
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional


def utc_now_iso() -> str:
    """Return current UTC timestamp as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def parse_targets(raw_targets: str) -> list[str]:
    """Parse comma-separated JMX targets (`host:port`) from configuration."""
    targets: list[str] = []
    for item in raw_targets.split(","):
        entry = item.strip()
        if not entry:
            continue
        if ":" not in entry:
            raise ValueError(f"Invalid JMX target '{entry}'. Expected host:port.")
        targets.append(entry)
    if not targets:
        raise ValueError("At least one JMX target is required.")
    return targets


def coerce_value(value: Optional[str]) -> Any:
    """Convert JMX CSV string values into primitive Python types when possible."""
    if value is None:
        return None
    stripped = value.strip()
    if stripped == "":
        return None
    lowered = stripped.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    try:
        return int(stripped)
    except ValueError:
        pass
    try:
        return float(stripped)
    except ValueError:
        return stripped


def parse_jmx_csv_rows(stdout: str) -> dict[str, Any]:
    """Parse Kafka JmxTool CSV output into a dictionary of attribute values."""
    attributes: dict[str, Any] = {}
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("Trying to connect to JMX url:"):
            continue
        if "," not in line:
            continue

        split_idx = line.rfind(',"')
        if split_idx == -1:
            split_idx = line.rfind(",")
        if split_idx == -1:
            continue

        key = line[:split_idx].strip()
        raw_value = line[split_idx + 1 :]
        value_row = next(csv.reader(io.StringIO(raw_value)), [])
        value = value_row[0] if value_row else raw_value
        if key:
            attributes[key] = coerce_value(value)

    if not attributes:
        raise ValueError("Unable to parse JmxTool CSV rows.")
    return attributes


def jmx_url_for_target(target: str) -> str:
    """Build a JMX RMI URL for a broker target host/port pair."""
    return f"service:jmx:rmi:///jndi/rmi://{target}/jmxrmi"


def query_target(target: str) -> dict[str, Any]:
    """Run Kafka JmxTool against one broker and return parsed attributes."""
    cmd = [
        "kafka-run-class",
        "org.apache.kafka.tools.JmxTool",
        "--jmx-url",
        jmx_url_for_target(target),
        "--report-format",
        "csv",
        "--one-time",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=45, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"JmxTool failed for {target} (exit {result.returncode}) "
            f"stderr={result.stderr.strip()!r} stdout={result.stdout.strip()!r}"
        )
    return parse_jmx_csv_rows(result.stdout)


def escape_label_value(value: str) -> str:
    """Escape string content for safe inclusion in Prometheus label values."""
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def metric_num(value: Any) -> Optional[float]:
    """Convert primitive values to float when exportable as Prometheus samples."""
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return None


def parse_key(raw_key: str) -> tuple[str, str, str]:
    """Split raw JMX key into domain, object-name (mbean), and attribute."""
    if ":" not in raw_key:
        return ("unknown", raw_key, "Value")
    object_name, attribute = raw_key.rsplit(":", 1)
    domain = object_name.split(":", 1)[0]
    return (domain, object_name, attribute)


class ExporterState:
    """Thread-safe state cache of the latest poll result per Kafka broker."""

    def __init__(self) -> None:
        """Initialize empty broker state and synchronization primitives."""
        self.lock = threading.Lock()
        self.brokers: dict[str, dict[str, Any]] = {}

    def set_broker(
        self,
        broker: str,
        success: bool,
        collected_at_unix: float,
        poll_duration_seconds: float,
        numeric_samples: list[tuple[str, str, str, float]],
        error_message: str,
    ) -> None:
        """Store the latest poll result payload for one broker."""
        with self.lock:
            self.brokers[broker] = {
                "success": success,
                "collected_at_unix": collected_at_unix,
                "poll_duration_seconds": poll_duration_seconds,
                "samples": numeric_samples,
                "error_message": error_message,
            }

    def render_metrics(self) -> str:
        """Render current cached state into Prometheus text exposition format."""
        with self.lock:
            brokers_copy = dict(self.brokers)

        lines: list[str] = []
        lines.append("# HELP kafka_jmx_attribute_value Numeric Kafka JMX attribute value.")
        lines.append("# TYPE kafka_jmx_attribute_value gauge")

        lines.append("# HELP kafka_jmx_exporter_last_poll_success 1 if last poll succeeded, else 0.")
        lines.append("# TYPE kafka_jmx_exporter_last_poll_success gauge")

        lines.append("# HELP kafka_jmx_exporter_last_poll_timestamp_seconds Unix timestamp of last poll.")
        lines.append("# TYPE kafka_jmx_exporter_last_poll_timestamp_seconds gauge")

        lines.append("# HELP kafka_jmx_exporter_poll_duration_seconds Duration of last JMX poll.")
        lines.append("# TYPE kafka_jmx_exporter_poll_duration_seconds gauge")

        lines.append("# HELP kafka_jmx_exporter_numeric_attributes Number of numeric attributes exported.")
        lines.append("# TYPE kafka_jmx_exporter_numeric_attributes gauge")

        lines.append("# HELP kafka_jmx_exporter_last_error 1 if last poll had an error, else 0.")
        lines.append("# TYPE kafka_jmx_exporter_last_error gauge")

        for broker, info in sorted(brokers_copy.items()):
            success = 1 if info.get("success") else 0
            collected_at = float(info.get("collected_at_unix", 0.0))
            poll_duration = float(info.get("poll_duration_seconds", 0.0))
            samples: list[tuple[str, str, str, float]] = info.get("samples", [])
            has_error = 1 if info.get("error_message") else 0

            lines.append(f'kafka_jmx_exporter_last_poll_success{{broker="{escape_label_value(broker)}"}} {success}')
            lines.append(
                f'kafka_jmx_exporter_last_poll_timestamp_seconds{{broker="{escape_label_value(broker)}"}} {collected_at}'
            )
            lines.append(
                f'kafka_jmx_exporter_poll_duration_seconds{{broker="{escape_label_value(broker)}"}} {poll_duration}'
            )
            lines.append(
                f'kafka_jmx_exporter_numeric_attributes{{broker="{escape_label_value(broker)}"}} {len(samples)}'
            )
            lines.append(f'kafka_jmx_exporter_last_error{{broker="{escape_label_value(broker)}"}} {has_error}')

            for domain, mbean, attribute, value in samples:
                labels = (
                    f'broker="{escape_label_value(broker)}",'
                    f'domain="{escape_label_value(domain)}",'
                    f'mbean="{escape_label_value(mbean)}",'
                    f'attribute="{escape_label_value(attribute)}"'
                )
                lines.append(f"kafka_jmx_attribute_value{{{labels}}} {value}")

        lines.append("")
        return "\n".join(lines)


class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP handler that serves exporter metrics for Prometheus scraping."""

    state: Optional[ExporterState] = None

    def do_GET(self) -> None:  # noqa: N802
        """Serve metrics for `/metrics` and `/`; return 404 for other paths."""
        if self.path not in ("/metrics", "/"):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not found")
            return

        assert self.state is not None
        payload = self.state.render_metrics().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt: str, *args: Any) -> None:
        """Suppress default HTTP request logging for cleaner container logs."""
        return


def poll_loop(state: ExporterState, targets: list[str], interval_seconds: float) -> None:
    """Continuously poll broker JMX endpoints and update shared exporter state."""
    while True:
        cycle_started = time.monotonic()
        for target in targets:
            broker = target.split(":", 1)[0]
            poll_started = time.monotonic()
            collected_unix = time.time()
            try:
                attrs = query_target(target)
                samples: list[tuple[str, str, str, float]] = []
                for raw_key, raw_value in attrs.items():
                    num = metric_num(raw_value)
                    if num is None:
                        continue
                    domain, mbean, attribute = parse_key(raw_key)
                    samples.append((domain, mbean, attribute, num))

                state.set_broker(
                    broker=broker,
                    success=True,
                    collected_at_unix=collected_unix,
                    poll_duration_seconds=time.monotonic() - poll_started,
                    numeric_samples=samples,
                    error_message="",
                )
            except Exception as exc:
                state.set_broker(
                    broker=broker,
                    success=False,
                    collected_at_unix=collected_unix,
                    poll_duration_seconds=time.monotonic() - poll_started,
                    numeric_samples=[],
                    error_message=str(exc),
                )

        elapsed = time.monotonic() - cycle_started
        time.sleep(max(0.0, interval_seconds - elapsed))


def main() -> None:
    """Start poll thread and HTTP metrics endpoint for Prometheus scraping."""
    targets = parse_targets(os.getenv("JMX_TARGETS", "kafka1:9991,kafka2:9992,kafka3:9993"))
    interval_seconds = float(os.getenv("JMX_POLL_INTERVAL_SECONDS", "5"))
    port = int(os.getenv("JMX_EXPORTER_PORT", "9404"))

    if interval_seconds <= 0:
        raise ValueError("JMX_POLL_INTERVAL_SECONDS must be greater than 0.")

    state = ExporterState()
    thread = threading.Thread(target=poll_loop, args=(state, targets, interval_seconds), daemon=True)
    thread.start()

    MetricsHandler.state = state
    server = ThreadingHTTPServer(("0.0.0.0", port), MetricsHandler)
    print(
        f"Kafka JMX Prometheus exporter started at :{port}/metrics "
        f"targets={targets} interval_seconds={interval_seconds} started_at={utc_now_iso()}"
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
