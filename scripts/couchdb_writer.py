#!/usr/bin/env python3
"""
Lightweight CouchDB writer for consumed Kafka messages.
"""

import json
import os
import time
from datetime import datetime, timezone
from typing import Any
from urllib import error, parse, request


class CouchDBWriter:
    """Persist consumed Kafka records into CouchDB with retry-aware writes."""

    def __init__(
        self,
        url: str,
        user: str,
        password: str,
        db_name: str,
        enabled: bool = True,
        timeout_seconds: float = 5.0,
    ) -> None:
        """Initialize CouchDB connection settings and in-memory write counters."""
        self.url = url.rstrip("/")
        self.user = user
        self.password = password
        self.db_name = db_name
        self.enabled = enabled
        self.timeout_seconds = timeout_seconds
        self.ready = False
        self.writes_ok = 0
        self.last_error = ""
        self._next_retry_at = 0.0

    @classmethod
    def from_env(cls) -> "CouchDBWriter":
        """Build a writer from environment variables used by the demo stack."""
        return cls(
            url=os.getenv("COUCHDB_URL", "http://localhost:5984"),
            user=os.getenv("COUCHDB_USER", "admin"),
            password=os.getenv("COUCHDB_PASSWORD", "demo"),
            db_name=os.getenv("COUCHDB_DB", "kafka_expert_consumed"),
            enabled=os.getenv("CONSUMER_WRITE_COUCHDB", "true").lower() in {"1", "true", "yes", "on"},
            timeout_seconds=float(os.getenv("COUCHDB_TIMEOUT_SECONDS", "5.0")),
        )

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
        """Execute an authenticated JSON request against CouchDB and return status/body."""
        url = f"{self.url}{path}"
        body = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = request.Request(url=url, data=body, method=method, headers=headers)
        req.add_header("Authorization", self._basic_auth())
        with request.urlopen(req, timeout=self.timeout_seconds) as resp:
            raw = resp.read().decode("utf-8") if resp.length != 0 else "{}"
            data = json.loads(raw) if raw else {}
            return resp.status, data

    def _basic_auth(self) -> str:
        """Encode CouchDB basic auth credentials for HTTP Authorization header."""
        import base64

        auth = f"{self.user}:{self.password}".encode("utf-8")
        return f"Basic {base64.b64encode(auth).decode('ascii')}"

    def _ensure_db(self) -> None:
        """Create the target database on first use if it does not already exist."""
        if not self.enabled or self.ready:
            return
        try:
            status, _ = self._request("PUT", f"/{parse.quote(self.db_name, safe='')}")
            if status not in {200, 201, 202}:
                raise RuntimeError(f"Unexpected status creating db: {status}")
            self.ready = True
            self.last_error = ""
        except error.HTTPError as exc:
            if exc.code == 412:
                # Database already exists.
                self.ready = True
                self.last_error = ""
                return
            raise

    def _doc_id(self, topic: str, partition: int, offset: int) -> str:
        """Build deterministic CouchDB document id from Kafka coordinates."""
        return f"{topic}:{partition}:{offset}"

    def write_message(
        self,
        topic: str,
        partition: int,
        offset: int,
        key: str | None,
        consumed_at: str,
        latency_ms: int | None,
        kafka_timestamp_ms: int | None,
        kafka_timestamp_type: int | None,
        message: dict[str, Any],
    ) -> None:
        """Write one consumed Kafka message to CouchDB with lightweight backoff."""
        if not self.enabled:
            return

        now = time.time()
        if now < self._next_retry_at:
            return

        try:
            self._ensure_db()
            doc_id = self._doc_id(topic, partition, offset)
            payload = {
                "_id": doc_id,
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "key": key,
                "consumed_at": consumed_at,
                "written_at_utc": datetime.now(timezone.utc).isoformat(),
                "latency_ms": latency_ms,
                "kafka_timestamp_ms": kafka_timestamp_ms,
                "kafka_timestamp_type": kafka_timestamp_type,
                "message": message,
            }
            status, _ = self._request(
                "PUT",
                f"/{parse.quote(self.db_name, safe='')}/{parse.quote(doc_id, safe='')}",
                payload=payload,
            )
            if status not in {200, 201, 202}:
                raise RuntimeError(f"Unexpected status writing doc: {status}")
            self.writes_ok += 1
            self.last_error = ""
        except error.HTTPError as exc:
            if exc.code == 409:
                # Document already exists for this topic/partition/offset.
                self.writes_ok += 1
                self.last_error = ""
                return
            self.last_error = f"http-{exc.code}: {exc.reason}"
            self._next_retry_at = now + 3.0
            raise
        except Exception as exc:
            self.last_error = str(exc)
            self._next_retry_at = now + 3.0
            raise
