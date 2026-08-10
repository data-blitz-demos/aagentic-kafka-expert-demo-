#!/usr/bin/env python3
"""Main Flask UI and orchestration runtime for the Agentic Kafka demo.

Owns the Monitor/Doer browser panels, agent APIs, prompt loading, LLM
configuration, Graph RAG, A2A/HITL workflows, observability, and MLX
fine-tuning preparation/job controls.
"""

__author__ = "Paul Harvener"
__company__ = "Data-Blitz Inc."

import asyncio
import base64
import html
import io
import json
import os
import re
import subprocess
import threading
import time
from pathlib import Path
from string import Template
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from langchain_core.messages import AIMessage
from langchain_core.tools import StructuredTool
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import create_react_agent
from neo4j import GraphDatabase
from pypdf import PdfReader
from agent_llm_config import resolve_agent_models, resolve_agent_provider
from graph_ingest_utils import fallback_graph_edges
from kafka_a2a import PROTOCOL_VERSION as KAFKA_A2A_PROTOCOL_VERSION
from kafka_a2a import extract_task_update as extract_kafka_a2a_task_update
from kafka_a2a import get_task as get_kafka_a2a_task
from kafka_a2a import send_message as send_kafka_a2a_message
from kafka_agent_tools import select_kafka_admin_tools, select_kafka_rest_tools
from monitor_handoff import build_monitor_handoff
from neo4j_agent_rag import retrieve_neo4j_knowledge
from remediation_hitl import (
    COMPLETED as HITL_COMPLETED,
    FAILED as HITL_FAILED,
    INPUT_REQUIRED as HITL_INPUT_REQUIRED,
    REJECTED as HITL_REJECTED,
    WORKING as HITL_WORKING,
    RemediationHitlStore,
    build_review_context,
    format_human_plan,
)
from mlx_finetuning import build_mlx_lora_args, build_mlx_lora_command, estimate_mlx_duration, mlx_runtime_status


load_dotenv()

FINETUNING_JOBS: dict[str, dict[str, Any]] = {}


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


UI_PORT = int(os.getenv("KAFKA_EXPERT_UI_PORT", "5052"))
REMEDIATION_PORT = int(os.getenv("KAFKA_REMEDIATION_AGENT_PORT", "5053"))
AGENT_PROCESS_ROLE = os.getenv("KAFKA_AGENT_PROCESS_ROLE", "monitor").strip().lower()
if AGENT_PROCESS_ROLE not in {"monitor", "remediation"}:
    raise RuntimeError("KAFKA_AGENT_PROCESS_ROLE must be 'monitor' or 'remediation'.")
SERVER_PORT = UI_PORT if AGENT_PROCESS_ROLE == "monitor" else REMEDIATION_PORT
A2A_PROTOCOL_VERSION = KAFKA_A2A_PROTOCOL_VERSION
A2A_REMEDIATION_BASE_URL = os.getenv(
    "A2A_REMEDIATION_BASE_URL",
    f"http://127.0.0.1:{REMEDIATION_PORT}/a2a/remediation",
).strip().rstrip("/")
A2A_REMEDIATION_PUBLIC_URL = os.getenv(
    "A2A_REMEDIATION_PUBLIC_URL",
    f"http://localhost:{REMEDIATION_PORT}/a2a/remediation",
).strip().rstrip("/")
A2A_REMEDIATION_HEALTH_URL = os.getenv(
    "A2A_REMEDIATION_HEALTH_URL",
    f"http://127.0.0.1:{REMEDIATION_PORT}/api/health",
).strip()
A2A_REMEDIATION_TOKEN = os.getenv("A2A_REMEDIATION_TOKEN", "").strip() or uuid.uuid4().hex
A2A_REQUEST_TIMEOUT_SECONDS = float(os.getenv("A2A_REQUEST_TIMEOUT_SECONDS", "120"))
REMEDIATION_HITL_ENABLED = os.getenv("REMEDIATION_HITL_ENABLED", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
DEEPEVAL_URL = os.getenv("DEEPEVAL_URL", "http://deepeval:5060").strip().rstrip("/")
DEEPEVAL_AUTO_EVALUATE = os.getenv("DEEPEVAL_AUTO_EVALUATE", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092"
)
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
COUCHDB_URL = os.getenv("COUCHDB_URL", "http://couchdb:5984").strip()
COUCHDB_USER = os.getenv("COUCHDB_USER", "admin").strip()
COUCHDB_PASSWORD = os.getenv("COUCHDB_PASSWORD", "demo").strip()
COUCHDB_DB = os.getenv("COUCHDB_DB", "kafka_expert_consumed").strip()
COUCHDB_CONTEXT_DB = os.getenv("COUCHDB_CONTEXT_DB", f"{COUCHDB_DB}_context").strip()
CONTEXT_PERSIST_TO_COUCHDB = os.getenv("CONTEXT_PERSIST_TO_COUCHDB", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
COUCHDB_TIMEOUT_SECONDS = float(os.getenv("COUCHDB_TIMEOUT_SECONDS", "5.0"))
MONITOR_LLM_PROVIDER = os.getenv("MONITOR_LLM_PROVIDER", "").strip()
REMEDIATION_LLM_PROVIDER = os.getenv("REMEDIATION_LLM_PROVIDER", "").strip()
LLM_PROVIDER = resolve_agent_provider(
    AGENT_PROCESS_ROLE,
    default_provider=os.getenv("LLM_PROVIDER", "openai"),
    monitor_provider=MONITOR_LLM_PROVIDER,
    remediation_provider=REMEDIATION_LLM_PROVIDER,
)
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0").strip() or "0")
MONITOR_MODEL = os.getenv("MONITOR_MODEL", "").strip()
MONITOR_MODEL_FALLBACKS = _split_csv(os.getenv("MONITOR_MODEL_FALLBACKS", "").strip())
REMEDIATION_MODEL = os.getenv("REMEDIATION_MODEL", "").strip()
REMEDIATION_MODEL_FALLBACKS = _split_csv(os.getenv("REMEDIATION_MODEL_FALLBACKS", "").strip())
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "").strip()
OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY", "").strip()
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3:8b").strip()
OLLAMA_MODEL_FALLBACKS = _split_csv(os.getenv("OLLAMA_MODEL_FALLBACKS", "").strip())
OLLAMA_MODELS_DIR = os.getenv("OLLAMA_MODELS_DIR", "/root/.ollama/models").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.3")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "").strip()
OPENAI_MODEL_FALLBACKS = _split_csv(os.getenv("OPENAI_MODEL_FALLBACKS", "gpt-5.2,gpt-4.1"))
HUGGINGFACE_OPENAI_BASE_URL = os.getenv("HUGGINGFACE_OPENAI_BASE_URL", "").strip() or "https://router.huggingface.co/v1"
HUGGINGFACE_API_KEY = os.getenv("HUGGINGFACE_API_KEY", "").strip()
HUGGINGFACE_MODEL = os.getenv("HUGGINGFACE_MODEL", "meta-llama/Meta-Llama-3.1-8B-Instruct").strip()
HUGGINGFACE_MODEL_FALLBACKS = _split_csv(os.getenv("HUGGINGFACE_MODEL_FALLBACKS", "").strip())
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
GRAPH_RAG_EXTRACT_MAX_TOKENS = max(
    64, int(os.getenv("GRAPH_RAG_EXTRACT_MAX_TOKENS", "320"))
)
GRAPH_RAG_LLM_TIMEOUT_SECONDS = max(
    10.0, float(os.getenv("GRAPH_RAG_LLM_TIMEOUT_SECONDS", "30"))
)
GRAPH_RAG_MAX_EDGES_PER_CHUNK = max(
    1, int(os.getenv("GRAPH_RAG_MAX_EDGES_PER_CHUNK", "8"))
)
MLX_TRAINING_DATA_DIR = os.getenv("MLX_TRAINING_DATA_DIR", "output/fine-tuning").strip() or "output/fine-tuning"
try:
    AI_TOKEN_BUDGET = max(1, int((os.getenv("AI_TOKEN_BUDGET", "250000") or "250000").strip()))
except Exception:
    AI_TOKEN_BUDGET = 250000
PROMPT_DIR = Path(__file__).resolve().parents[1] / "prompts"


def _load_prompt_template(name: str) -> str:
    """Load one prompt template from repo-level prompts directory."""
    path = PROMPT_DIR / name
    return path.read_text(encoding="utf-8").strip()


def render_prompt(name: str, **kwargs: Any) -> str:
    """Render a string.Template payload so prompts can be updated without code changes."""
    template = Template(_load_prompt_template(name))
    return template.safe_substitute(**kwargs)


class CouchDBContextStore:
    """Tiny CouchDB JSON document store used for Kafka Expert context persistence."""

    def __init__(
        self,
        *,
        url: str,
        user: str,
        password: str,
        db_name: str,
        enabled: bool,
        timeout_seconds: float,
    ) -> None:
        self.url = url.rstrip("/")
        self.user = user
        self.password = password
        self.db_name = db_name
        self.enabled = enabled
        self.timeout_seconds = timeout_seconds
        self.ready = False
        self.last_error = ""
        self._next_retry_at = 0.0
        self._lock = threading.Lock()
        self._rev_cache: dict[str, str] = {}

    @classmethod
    def from_env(cls) -> "CouchDBContextStore":
        """Construct the context store from environment variables."""
        return cls(
            url=COUCHDB_URL,
            user=COUCHDB_USER,
            password=COUCHDB_PASSWORD,
            db_name=COUCHDB_CONTEXT_DB,
            enabled=CONTEXT_PERSIST_TO_COUCHDB,
            timeout_seconds=COUCHDB_TIMEOUT_SECONDS,
        )

    def _basic_auth(self) -> str:
        auth = f"{self.user}:{self.password}".encode("utf-8")
        return f"Basic {base64.b64encode(auth).decode('ascii')}"

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
        url = f"{self.url}{path}"
        body = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url=url, data=body, method=method, headers=headers)
        req.add_header("Authorization", self._basic_auth())
        with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
            raw = resp.read().decode("utf-8") if getattr(resp, "length", 0) != 0 else "{}"
            parsed = json.loads(raw) if raw else {}
            return int(getattr(resp, "status", 200)), parsed

    def _db_path(self) -> str:
        return f"/{urllib.parse.quote(self.db_name, safe='')}"

    def _mark_failure(self, error_text: str) -> None:
        self.last_error = error_text
        self._next_retry_at = time.time() + 3.0

    def _ensure_db(self) -> bool:
        if not self.enabled:
            return False
        if self.ready:
            return True
        if time.time() < self._next_retry_at:
            return False
        try:
            status, _ = self._request("PUT", self._db_path())
            if status not in {200, 201, 202}:
                self._mark_failure(f"unexpected-status-{status}")
                return False
            self.ready = True
            self.last_error = ""
            return True
        except urllib.error.HTTPError as exc:
            if exc.code == 412:
                self.ready = True
                self.last_error = ""
                return True
            self._mark_failure(f"http-{exc.code}: {exc.reason}")
            return False
        except Exception as exc:
            self._mark_failure(str(exc))
            return False

    def get_doc(self, doc_id: str) -> dict[str, Any] | None:
        if not self._ensure_db():
            return None
        encoded_id = urllib.parse.quote(doc_id, safe="")
        try:
            _, data = self._request("GET", f"{self._db_path()}/{encoded_id}")
            rev = str(data.get("_rev", "")).strip()
            if rev:
                self._rev_cache[doc_id] = rev
            self.last_error = ""
            return data
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            self._mark_failure(f"http-{exc.code}: {exc.reason}")
            return None
        except Exception as exc:
            self._mark_failure(str(exc))
            return None

    def upsert_doc(self, doc_id: str, body: dict[str, Any]) -> bool:
        if not self._ensure_db():
            return False
        encoded_id = urllib.parse.quote(doc_id, safe="")
        for _ in range(2):
            rev = self._rev_cache.get(doc_id, "")
            if not rev:
                existing = self.get_doc(doc_id)
                if existing is not None:
                    rev = str(existing.get("_rev", "")).strip()

            payload = {"_id": doc_id, **body}
            if rev:
                payload["_rev"] = rev
            try:
                status, data = self._request("PUT", f"{self._db_path()}/{encoded_id}", payload=payload)
                if status not in {200, 201, 202}:
                    self._mark_failure(f"unexpected-status-{status}")
                    return False
                new_rev = str(data.get("rev", "")).strip()
                if new_rev:
                    self._rev_cache[doc_id] = new_rev
                self.last_error = ""
                return True
            except urllib.error.HTTPError as exc:
                if exc.code == 409:
                    self._rev_cache.pop(doc_id, None)
                    continue
                self._mark_failure(f"http-{exc.code}: {exc.reason}")
                return False
            except Exception as exc:
                self._mark_failure(str(exc))
                return False
        self._mark_failure(f"conflict-upserting-{doc_id}")
        return False

    def delete_doc(self, doc_id: str) -> bool:
        if not self._ensure_db():
            return False
        doc = self.get_doc(doc_id)
        if doc is None:
            self._rev_cache.pop(doc_id, None)
            return True
        rev = str(doc.get("_rev", "")).strip()
        if not rev:
            return False
        encoded_id = urllib.parse.quote(doc_id, safe="")
        encoded_rev = urllib.parse.quote(rev, safe="")
        try:
            self._request("DELETE", f"{self._db_path()}/{encoded_id}?rev={encoded_rev}")
            self._rev_cache.pop(doc_id, None)
            self.last_error = ""
            return True
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                self._rev_cache.pop(doc_id, None)
                return True
            self._mark_failure(f"http-{exc.code}: {exc.reason}")
            return False
        except Exception as exc:
            self._mark_failure(str(exc))
            return False

    def list_docs_by_prefix(self, prefix: str) -> list[dict[str, Any]]:
        if not self._ensure_db():
            return []
        start_key = json.dumps(prefix)
        end_key = json.dumps(f"{prefix}\ufff0")
        query = urllib.parse.urlencode(
            {
                "include_docs": "true",
                "startkey": start_key,
                "endkey": end_key,
            }
        )
        try:
            _, payload = self._request("GET", f"{self._db_path()}/_all_docs?{query}")
            rows = payload.get("rows", [])
            docs: list[dict[str, Any]] = []
            for row in rows:
                if isinstance(row, dict) and isinstance(row.get("doc"), dict):
                    docs.append(row["doc"])
            self.last_error = ""
            return docs
        except Exception as exc:
            self._mark_failure(str(exc))
            return []


class CouchDBBackedMemorySaver(MemorySaver):
    """LangGraph in-memory checkpointer mirrored to CouchDB for persistence."""

    DOC_PREFIX = "context::langgraph_thread::"

    def __init__(self, context_store: CouchDBContextStore) -> None:
        super().__init__()
        self._context_store = context_store
        self.last_persist_error = ""
        self._hydrate_from_couchdb()

    @staticmethod
    def _encode_typed(value: tuple[str, bytes]) -> dict[str, str]:
        return {"type": value[0], "b64": base64.b64encode(value[1]).decode("ascii")}

    @staticmethod
    def _decode_typed(payload: dict[str, Any]) -> tuple[str, bytes]:
        if not isinstance(payload, dict):
            return "bytes", b""
        kind = str(payload.get("type", "bytes"))
        try:
            raw = base64.b64decode(str(payload.get("b64", "")).encode("ascii"))
        except Exception:
            raw = b""
        return kind, raw

    @staticmethod
    def _encode_version(version: str | int | float) -> dict[str, Any]:
        if isinstance(version, bool):
            return {"kind": "str", "value": str(version)}
        if isinstance(version, int):
            return {"kind": "int", "value": version}
        if isinstance(version, float):
            return {"kind": "float", "value": version}
        return {"kind": "str", "value": str(version)}

    @staticmethod
    def _decode_version(payload: dict[str, Any]) -> str | int | float:
        kind = str(payload.get("kind", "str")).strip().lower()
        value = payload.get("value")
        try:
            if kind == "int":
                return int(value)
            if kind == "float":
                return float(value)
        except Exception:
            return str(value)
        return str(value)

    @classmethod
    def _doc_id(cls, thread_id: str) -> str:
        return f"{cls.DOC_PREFIX}{thread_id}"

    def _hydrate_from_couchdb(self) -> None:
        if not self._context_store.enabled:
            return
        docs = self._context_store.list_docs_by_prefix(self.DOC_PREFIX)
        for doc in docs:
            try:
                thread_id = str(doc.get("thread_id", "")).strip()
                if not thread_id:
                    continue
                for ns_item in doc.get("storage", []):
                    if not isinstance(ns_item, dict):
                        continue
                    checkpoint_ns = str(ns_item.get("checkpoint_ns", "")).strip()
                    for cp in ns_item.get("checkpoints", []):
                        if not isinstance(cp, dict):
                            continue
                        checkpoint_id = str(cp.get("checkpoint_id", "")).strip()
                        if not checkpoint_id:
                            continue
                        checkpoint_typed = self._decode_typed(cp.get("checkpoint", {}))
                        metadata_typed = self._decode_typed(cp.get("metadata", {}))
                        parent_id = cp.get("parent_checkpoint_id")
                        parent_checkpoint_id = str(parent_id).strip() if parent_id is not None else None
                        self.storage[thread_id][checkpoint_ns][checkpoint_id] = (
                            checkpoint_typed,
                            metadata_typed,
                            parent_checkpoint_id,
                        )

                for write_item in doc.get("writes", []):
                    if not isinstance(write_item, dict):
                        continue
                    checkpoint_ns = str(write_item.get("checkpoint_ns", "")).strip()
                    checkpoint_id = str(write_item.get("checkpoint_id", "")).strip()
                    task_id = str(write_item.get("task_id", "")).strip()
                    idx = int(write_item.get("write_index", 0))
                    channel = str(write_item.get("channel", "")).strip()
                    task_path = str(write_item.get("task_path", "")).strip()
                    value_typed = self._decode_typed(write_item.get("value", {}))
                    if checkpoint_id and task_id and channel:
                        self.writes[(thread_id, checkpoint_ns, checkpoint_id)][(task_id, idx)] = (
                            task_id,
                            channel,
                            value_typed,
                            task_path,
                        )

                for blob_item in doc.get("blobs", []):
                    if not isinstance(blob_item, dict):
                        continue
                    checkpoint_ns = str(blob_item.get("checkpoint_ns", "")).strip()
                    channel = str(blob_item.get("channel", "")).strip()
                    version = self._decode_version(blob_item.get("version", {}))
                    value_typed = self._decode_typed(blob_item.get("value", {}))
                    if channel:
                        self.blobs[(thread_id, checkpoint_ns, channel, version)] = value_typed
            except Exception:
                continue

    def _persist_thread(self, thread_id: str) -> None:
        if not self._context_store.enabled:
            return

        storage_payload: list[dict[str, Any]] = []
        for checkpoint_ns, checkpoints in self.storage.get(thread_id, {}).items():
            checkpoint_items: list[dict[str, Any]] = []
            for checkpoint_id, values in checkpoints.items():
                checkpoint_typed, metadata_typed, parent_checkpoint_id = values
                checkpoint_items.append(
                    {
                        "checkpoint_id": checkpoint_id,
                        "checkpoint": self._encode_typed(checkpoint_typed),
                        "metadata": self._encode_typed(metadata_typed),
                        "parent_checkpoint_id": parent_checkpoint_id,
                    }
                )
            storage_payload.append({"checkpoint_ns": checkpoint_ns, "checkpoints": checkpoint_items})

        writes_payload: list[dict[str, Any]] = []
        for (t_id, checkpoint_ns, checkpoint_id), write_map in self.writes.items():
            if t_id != thread_id:
                continue
            for (task_id, write_index), (saved_task_id, channel, value_typed, task_path) in write_map.items():
                writes_payload.append(
                    {
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": checkpoint_id,
                        "task_id": task_id,
                        "write_index": int(write_index),
                        "saved_task_id": saved_task_id,
                        "channel": channel,
                        "value": self._encode_typed(value_typed),
                        "task_path": task_path,
                    }
                )

        blobs_payload: list[dict[str, Any]] = []
        for (t_id, checkpoint_ns, channel, version), value_typed in self.blobs.items():
            if t_id != thread_id:
                continue
            blobs_payload.append(
                {
                    "checkpoint_ns": checkpoint_ns,
                    "channel": channel,
                    "version": self._encode_version(version),
                    "value": self._encode_typed(value_typed),
                }
            )

        ok = self._context_store.upsert_doc(
            self._doc_id(thread_id),
            {
                "type": "langgraph_thread_checkpoint",
                "thread_id": thread_id,
                "storage": storage_payload,
                "writes": writes_payload,
                "blobs": blobs_payload,
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            },
        )
        if not ok:
            self.last_persist_error = self._context_store.last_error or "couchdb-upsert-failed"
        else:
            self.last_persist_error = ""

    def put(self, config: Any, checkpoint: Any, metadata: Any, new_versions: Any) -> Any:
        result = super().put(config, checkpoint, metadata, new_versions)
        thread_id = str(config.get("configurable", {}).get("thread_id", "")).strip()
        if thread_id:
            self._persist_thread(thread_id)
        return result

    def put_writes(self, config: Any, writes: Any, task_id: str, task_path: str = "") -> None:
        super().put_writes(config, writes, task_id, task_path)
        thread_id = str(config.get("configurable", {}).get("thread_id", "")).strip()
        if thread_id:
            self._persist_thread(thread_id)

    def delete_thread(self, thread_id: str) -> None:
        super().delete_thread(thread_id)
        if thread_id:
            self._context_store.delete_doc(self._doc_id(thread_id))

def _normalize_llm_provider(provider: str) -> str:
    """Normalize provider name into a supported key."""
    provider = (provider or "openai").strip().lower()
    aliases = {
        "hf": "huggingface",
        "open source": "ollama",
    }
    return aliases.get(provider, provider)


def _validate_llm_provider_configuration(component: str) -> None:
    """Validate required credentials/base URLs for the configured LLM provider."""
    provider = _normalize_llm_provider(LLM_PROVIDER)
    if provider == "openai":
        if not OPENAI_API_KEY:
            raise RuntimeError(
                f"{component}: OPENAI_API_KEY is required when LLM_PROVIDER=openai. "
                f"{_llm_error_message_for_provider(provider)}"
            )
        return
    if provider == "ollama":
        return
    if provider == "huggingface":
        if not HUGGINGFACE_OPENAI_BASE_URL:
            raise RuntimeError(
                f"{component}: Hugging Face provider requires HUGGINGFACE_OPENAI_BASE_URL. "
                f"{_llm_error_message_for_provider(provider)}"
            )
        if "router.huggingface.co" in HUGGINGFACE_OPENAI_BASE_URL and not HUGGINGFACE_API_KEY:
            raise RuntimeError(
                f"{component}: HUGGINGFACE_API_KEY is required for the Hugging Face router. "
                "Create a token with inference permission or use an unauthenticated local HUGS/TGI endpoint."
            )
        return
    raise RuntimeError(
        f"{component}: Unsupported LLM_PROVIDER={provider}. Expected one of: openai, ollama, huggingface."
    )


def _dedupe_models(models: list[str]) -> list[str]:
    """Remove empty/duplicate models while preserving declaration order."""
    seen: set[str] = set()
    out: list[str] = []
    for model_name in models:
        trimmed = model_name.strip()
        if not trimmed or trimmed in seen:
            continue
        seen.add(trimmed)
        out.append(trimmed)
    return out


def _model_candidates_for_provider(provider: str) -> list[str]:
    """Resolve primary + fallback model names for the selected provider."""
    provider = _normalize_llm_provider(provider)
    if provider == "openai":
        return _dedupe_models([OPENAI_MODEL, *OPENAI_MODEL_FALLBACKS])
    if provider == "ollama":
        return _dedupe_models([OLLAMA_MODEL, *OLLAMA_MODEL_FALLBACKS])
    if provider == "huggingface":
        return _dedupe_models([HUGGINGFACE_MODEL, *HUGGINGFACE_MODEL_FALLBACKS])
    raise RuntimeError(f"Unsupported LLM_PROVIDER={provider}. Expected one of: openai, ollama, huggingface.")


def _requested_model_for_provider(provider: str) -> str:
    """Return the primary model configured for the provider."""
    candidates = _model_candidates_for_provider(provider)
    return candidates[0] if candidates else ""


def _llm_base_url_for_provider(provider: str) -> str:
    """Return base URL for ChatOpenAI-compatible provider backends."""
    provider = _normalize_llm_provider(provider)
    if provider == "openai":
        return OPENAI_BASE_URL
    if provider == "ollama":
        # Prefer the in-cluster service name so Docker Compose users don't need to override.
        return OLLAMA_BASE_URL or "http://ollama:11434/v1"
    if provider == "huggingface":
        return HUGGINGFACE_OPENAI_BASE_URL or "https://router.huggingface.co/v1"
    raise RuntimeError(f"Unsupported LLM_PROVIDER={provider}.")


def _llm_api_key_for_provider(provider: str) -> str:
    """Return provider-specific API token when required."""
    provider = _normalize_llm_provider(provider)
    if provider == "openai":
        return OPENAI_API_KEY
    if provider == "ollama":
        return OLLAMA_API_KEY or "ollama"
    if provider == "huggingface":
        return HUGGINGFACE_API_KEY
    raise RuntimeError(f"Unsupported LLM_PROVIDER={provider}.")


def _llm_error_message_for_provider(provider: str) -> str:
    """Return a human-readable config requirement for the selected provider."""
    provider = _normalize_llm_provider(provider)
    if provider == "openai":
        return "Set OPENAI_API_KEY (or configure OPENAI_BASE_URL with valid credentials)."
    if provider == "ollama":
        return "Set OLLAMA_BASE_URL (default http://ollama:11434/v1) and ensure ollama service is reachable."
    if provider == "huggingface":
        return (
            "Use https://router.huggingface.co/v1 with a Hugging Face inference token, "
            "or set an OpenAI-compatible local HUGS/TGI endpoint."
        )
    return f"Unsupported LLM_PROVIDER={provider}."


def _build_chat_model_kwargs(model_name: str, provider: str) -> dict[str, Any]:
    """Build ChatOpenAI kwargs for OpenAI-compatible provider endpoints."""
    model_kwargs: dict[str, Any] = {
        "model": model_name,
        "temperature": LLM_TEMPERATURE,
    }
    base_url = _llm_base_url_for_provider(provider)
    if base_url:
        model_kwargs["base_url"] = base_url.rstrip("/")
    api_key = _llm_api_key_for_provider(provider)
    if api_key:
        model_kwargs["api_key"] = api_key
    if _normalize_llm_provider(provider) == "ollama":
        # Thinking models can spend minutes reasoning before a tool call on CPU.
        # Ollama's OpenAI-compatible endpoint supports disabling that phase.
        model_kwargs["reasoning_effort"] = "none"
    return model_kwargs

KAFKA_ONLY_MESSAGE = (
    _load_prompt_template("kafka_only_message.txt")
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

SYSTEM_PROMPT = _load_prompt_template("system_prompt.txt")

REMEDIATION_SYSTEM_PROMPT = _load_prompt_template("remediation_system_prompt.txt")

REMEDIATION_PLANNING_SYSTEM_PROMPT = (
    "You are the non-mutating planning phase of the Kafka Remediation Agent. "
    "Write for a human operator deciding whether to authorize execution. "
    "Do not call tools and do not claim that verification or mutation has occurred. "
        "Start with one sentence describing the intended outcome. Then produce concise numbered steps. "
        "When more than one safe repair is plausible, present 2-5 distinct suggested repair paths "
        "that an operator can choose between, with the evidence and risk for each. "
        "Label every step READ ONLY or CONDITIONAL MUTATION. State the evidence to inspect, "
    "the exact smallest conditional change that may be attempted, its blast radius and risk, "
    "rollback instructions, and measurable post-change success criteria. "
    "If the evidence cannot yet justify a mutation, say that approval authorizes verification "
    "but does not require a change."
)

CLUSTER_STATE_PROMPT = _load_prompt_template("cluster_state_prompt.txt")

AUTO_FIX_PROMPT_TEMPLATE = _load_prompt_template("auto_fix_prompt_template.txt")

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


def new_graph_metrics() -> dict[str, Any]:
    """Return default Graph RAG metrics payload used by API/UI dashboards."""
    return {
        "ingest": {
            "runs": 0,
            "success": 0,
            "failure": 0,
            "total_duration_ms": 0,
            "total_chunks": 0,
            "total_edges": 0,
            "last": {},
        },
        "query": {
            "runs": 0,
            "success": 0,
            "failure": 0,
            "total_duration_ms": 0,
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "total_tokens": 0,
            "total_context_items": 0,
            "total_context_chars": 0,
            "zero_context_runs": 0,
            "last": {},
        },
        "recent_runs": [],
        "updated_at_utc": "",
    }


def new_chat_thread_id() -> str:
    """Return a unique thread id for LangGraph checkpointed chat memory."""
    return f"kafka-expert-{uuid.uuid4().hex}"


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
    return (
        "model_not_found" in text
        or "not_found_error" in text
        or "model not found" in text
        or ("model '" in text and "not found" in text)
        or "does not exist" in text
        or "you do not have access" in text
    )


def build_chat_with_fallback(
    component: str,
    *,
    model_candidates: list[str] | None = None,
) -> tuple[ChatOpenAI, str]:
    """Create a ChatOpenAI-compatible client, trying fallback models if the requested one is unavailable."""
    provider = _normalize_llm_provider(LLM_PROVIDER)
    candidates = _dedupe_models(model_candidates or _model_candidates_for_provider(provider))
    _validate_llm_provider_configuration(component)

    if not candidates:
        raise RuntimeError(
            f"{component}: no model configured for provider '{provider}'. "
            f"{_llm_error_message_for_provider(provider)}"
        )

    unavailable_errors: list[str] = []
    for model_name in candidates:
        base_url = _llm_base_url_for_provider(provider)
        if provider != "openai" and not base_url:
            raise RuntimeError(
                f"{component}: base URL is not configured for provider '{provider}'. "
                f"{_llm_error_message_for_provider(provider)}"
            )

        llm = ChatOpenAI(**_build_chat_model_kwargs(model_name=model_name, provider=provider))
        try:
            if provider == "ollama":
                # Model metadata validation is immediate and avoids running a
                # full CPU inference twice while Monitor + Graph RAG rebuild.
                model_url = f"{base_url.rstrip('/')}/models/{urllib.parse.quote(model_name, safe='')}"
                req = urllib.request.Request(model_url, headers={"Authorization": "Bearer ollama"})
                with urllib.request.urlopen(req, timeout=10.0):
                    pass
            else:
                # Probe hosted providers so fallbacks resolve to an accessible model.
                llm.bind(max_tokens=8).invoke("Reply with OK only.")
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


def _token_usage_from_ai_message(message: AIMessage) -> dict[str, int]:
    """Extract token usage from one AIMessage metadata payload."""
    input_tokens = 0
    output_tokens = 0
    total_tokens = 0
    source = ""

    usage_metadata = getattr(message, "usage_metadata", None)
    if isinstance(usage_metadata, dict):
        input_tokens = int(usage_metadata.get("input_tokens") or usage_metadata.get("prompt_tokens") or 0)
        output_tokens = int(usage_metadata.get("output_tokens") or usage_metadata.get("completion_tokens") or 0)
        total_tokens = int(usage_metadata.get("total_tokens") or (input_tokens + output_tokens))
        source = "usage_metadata"
    else:
        response_metadata = getattr(message, "response_metadata", None)
        if isinstance(response_metadata, dict):
            token_usage = response_metadata.get("token_usage")
            if isinstance(token_usage, dict):
                input_tokens = int(token_usage.get("prompt_tokens") or token_usage.get("input_tokens") or 0)
                output_tokens = int(token_usage.get("completion_tokens") or token_usage.get("output_tokens") or 0)
                total_tokens = int(token_usage.get("total_tokens") or (input_tokens + output_tokens))
                source = "response_metadata.token_usage"

    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "source": source,
    }


def _sum_token_usage(messages: list[Any]) -> dict[str, Any]:
    """Aggregate token usage across all AI messages in one run."""
    totals = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    counted_messages = 0
    sources: list[str] = []
    for msg in messages:
        if not isinstance(msg, AIMessage):
            continue
        usage = _token_usage_from_ai_message(msg)
        if usage["total_tokens"] <= 0 and usage["input_tokens"] <= 0 and usage["output_tokens"] <= 0:
            continue
        totals["input_tokens"] += int(usage["input_tokens"])
        totals["output_tokens"] += int(usage["output_tokens"])
        totals["total_tokens"] += int(usage["total_tokens"])
        counted_messages += 1
        if usage["source"]:
            sources.append(str(usage["source"]))
    return {
        **totals,
        "ai_messages_with_usage": counted_messages,
        "sources": sorted(set(sources)),
    }


def _mcp_usage_summary(messages: list[Any]) -> dict[str, Any]:
    """Summarize MCP/tool usage from LangGraph messages."""
    tool_call_names: list[str] = []
    tool_result_names: list[str] = []
    for msg in messages:
        if isinstance(msg, AIMessage):
            tool_calls = getattr(msg, "tool_calls", None)
            if isinstance(tool_calls, list):
                for call in tool_calls:
                    if not isinstance(call, dict):
                        continue
                    name = str(call.get("name", "")).strip()
                    if name:
                        tool_call_names.append(name)
            continue
        if msg.__class__.__name__ == "ToolMessage":
            name = str(getattr(msg, "name", "")).strip()
            if name:
                tool_result_names.append(name)

    tool_call_counts: dict[str, int] = {}
    for name in tool_call_names:
        tool_call_counts[name] = tool_call_counts.get(name, 0) + 1
    tool_result_counts: dict[str, int] = {}
    for name in tool_result_names:
        tool_result_counts[name] = tool_result_counts.get(name, 0) + 1

    return {
        "tool_call_count": len(tool_call_names),
        "tool_calls_by_name": tool_call_counts,
        "tool_result_count": len(tool_result_names),
        "tool_results_by_name": tool_result_counts,
        "unique_tools_called": sorted(set(tool_call_names)),
    }


def build_agent_trace(
    *,
    prompt_source: str,
    agent_role: str,
    system_prompt_file: str,
    system_prompt: str,
    user_prompt: str,
    agent_result: dict[str, Any],
    duration_ms: int,
) -> dict[str, Any]:
    """Build structured run-trace payload for Kafka Expert agent turns."""
    messages = agent_result.get("messages", [])
    if not isinstance(messages, list):
        messages = []
    return {
        "prompt_source": prompt_source,
        "agent_role": agent_role,
        "prompts": {
            "system_prompt_file": system_prompt_file,
            "system_prompt_chars": len(system_prompt),
            "user_prompt_chars": len(user_prompt),
            "user_prompt_preview": user_prompt[:500],
        },
        "rag_content": {
            "kind": "none",
            "items": 0,
            "chars": 0,
        },
        "mcp_usage": _mcp_usage_summary(messages),
        "token_usage": _sum_token_usage(messages),
        "message_count": len(messages),
        "duration_ms": int(max(duration_ms, 0)),
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def build_static_trace(
    *,
    prompt_source: str,
    user_prompt: str,
    reason: str,
) -> dict[str, Any]:
    """Build a trace record for deterministic/non-LLM branches."""
    return {
        "prompt_source": prompt_source,
        "agent_role": "deterministic",
        "prompts": {
            "system_prompt_file": "n/a",
            "system_prompt_chars": 0,
            "user_prompt_chars": len(user_prompt),
            "user_prompt_preview": user_prompt[:500],
        },
        "rag_content": {"kind": "none", "items": 0, "chars": 0},
        "mcp_usage": {
            "tool_call_count": 0,
            "tool_calls_by_name": {},
            "tool_result_count": 0,
            "tool_results_by_name": {},
            "unique_tools_called": [],
        },
        "token_usage": {
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "ai_messages_with_usage": 0,
            "sources": [],
        },
        "message_count": 0,
        "duration_ms": 0,
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "note": reason,
    }


def send_a2a_remediation_message(prompt: str, *, context_id: str) -> tuple[str, dict[str, Any], str]:
    """Send a synchronous A2A 1.0 message to the Kafka Remediation Agent."""
    return send_kafka_a2a_message(
        base_url=A2A_REMEDIATION_BASE_URL,
        token=A2A_REMEDIATION_TOKEN,
        prompt=prompt,
        context_id=context_id,
        timeout_seconds=A2A_REQUEST_TIMEOUT_SECONDS,
    )


def deepeval_request(
    path: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout_seconds: float = 180.0,
) -> dict[str, Any]:
    """Call the local DeepEval client API over the Compose network."""
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        url=f"{DEEPEVAL_URL}{path}",
        data=body,
        method=method,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
            result = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            detail = json.loads(exc.read().decode("utf-8"))
            raise RuntimeError(str(detail.get("error") or detail)) from exc
        except (ValueError, AttributeError):
            raise RuntimeError(f"DeepEval returned HTTP {exc.code}") from exc
    if not isinstance(result, dict):
        raise RuntimeError("DeepEval returned an invalid response.")
    return result


def llm_service_request(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout_seconds: float = 180.0,
) -> dict[str, Any]:
    """Call another runtime's LLM configuration API."""
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        url=url,
        data=body,
        method=method,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
            result = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            detail = json.loads(exc.read().decode("utf-8"))
            raise RuntimeError(str(detail.get("error") or detail)) from exc
        except (ValueError, AttributeError):
            raise RuntimeError(f"LLM service returned HTTP {exc.code}") from exc
    if not isinstance(result, dict):
        raise RuntimeError("LLM service returned an invalid response.")
    return result


def remediation_llm_config_url() -> str:
    parsed = urllib.parse.urlsplit(A2A_REMEDIATION_HEALTH_URL)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "/api/llm/config", "", ""))


def queue_deepeval_observation(agent_role: str, input_text: str, actual_output: str) -> None:
    """Evaluate one live agent response asynchronously so user requests are not blocked by the judge."""
    if not DEEPEVAL_AUTO_EVALUATE or AGENT_PROCESS_ROLE not in {"monitor", "remediation"}:
        return

    def _run() -> None:
        try:
            deepeval_request(
                "/api/evaluate",
                method="POST",
                payload={
                    "scope": agent_role,
                    "cases": [
                        {
                            "agent_role": agent_role,
                            "input": input_text,
                            "actual_output": actual_output,
                        }
                    ],
                },
            )
        except Exception as exc:
            print(f"DeepEval observation failed for {agent_role}: {exc}")

    threading.Thread(target=_run, name=f"deepeval-{agent_role}", daemon=True).start()


def infer_severity(line: str) -> str:
    """Infer severity label from a bullet line when model output is ambiguous."""
    lower = line.lower()
    if "[bad]" in lower:
        return "BAD"
    if "[warn]" in lower:
        return "WARN"
    if "[good]" in lower:
        return "GOOD"

    # Do not turn healthy phrases such as "no errors" into BAD findings.
    scored = re.sub(
        r"\bno\s+(?:critical\s+)?(?:errors?|failures?|failed checks?|issues?|brokers? down)\b",
        "",
        lower,
    )
    bad_markers = ("unhealthy", "failed", "failure", "error", "critical", "down")
    warn_markers = ("degraded", "risk", "gap", "missing", "cannot", "not fully", "warn")

    if any(re.search(rf"\b{re.escape(token)}\b", scored) for token in bad_markers):
        return "BAD"
    if any(re.search(rf"\b{re.escape(token)}\b", scored) for token in warn_markers):
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


def is_current_actionable_issue(issue_line: str, snapshot: dict[str, Any]) -> bool:
    """Accept only a current monitor-produced WARN/BAD line with a visible Fix action."""
    candidate = issue_line.strip()
    lower = candidate.lower()
    if "fix:" not in lower or not ("[warn]" in lower or "[bad]" in lower):
        return False
    current_lines: set[str] = set()
    for line in str(snapshot.get("status_text", "")).splitlines():
        if line.strip():
            current_lines.add(line.strip())
    for message in snapshot.get("chat", []):
        if not isinstance(message, dict) or message.get("role") not in {"agent", "monitor"}:
            continue
        for line in str(message.get("text", "")).splitlines():
            if line.strip():
                current_lines.add(line.strip())
    return candidate in current_lines


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


class GraphIngestCanceled(RuntimeError):
    """Raised between chunks when a user cancels asynchronous graph ingestion."""


class GraphRAGRuntime:
    """Neo4j-backed PDF Graph RAG runtime for knowledge graph ingest and graph-grounded QA."""

    def __init__(self) -> None:
        """Initialize Neo4j driver, constraints, and an extraction/answering LLM."""
        _validate_llm_provider_configuration("Graph RAG")
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

        self._llm, self.model_name = build_chat_with_fallback("Graph RAG")

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

    def _extract_edges(self, chunk_text: str) -> tuple[list[dict[str, str]], str, str]:
        """Extract edges with a bounded LLM call and deterministic timeout fallback."""
        prompt = render_prompt("graphrag_extract_prompt.txt", chunk_text=chunk_text)
        try:
            worker_env = dict(os.environ)
            worker_env["GRAPH_RAG_WORKER_API_KEY"] = _llm_api_key_for_provider(LLM_PROVIDER)
            completed = subprocess.run(
                ["python", "/app/scripts/graphrag_extract_worker.py"],
                input=json.dumps(
                    {
                        "provider": _normalize_llm_provider(LLM_PROVIDER),
                        "model": self.model_name,
                        "base_url": _llm_base_url_for_provider(LLM_PROVIDER),
                        "temperature": LLM_TEMPERATURE,
                        "max_tokens": GRAPH_RAG_EXTRACT_MAX_TOKENS,
                        "prompt": prompt,
                    }
                ),
                text=True,
                capture_output=True,
                timeout=GRAPH_RAG_LLM_TIMEOUT_SECONDS,
                check=False,
                env=worker_env,
            )
            if completed.returncode != 0:
                raise RuntimeError(completed.stderr.strip() or "Graph extraction worker failed.")
            worker_payload = json.loads(completed.stdout)
            raw = str(worker_payload.get("content", "")).strip()
        except Exception as exc:
            return (
                fallback_graph_edges(chunk_text, max_edges=GRAPH_RAG_MAX_EDGES_PER_CHUNK),
                "deterministic_fallback",
                str(exc),
            )
        payload: dict[str, Any] = {}
        try:
            payload = json.loads(raw)
        except Exception:
            match = re.search(r"\{[\s\S]*\}", raw)
            if match:
                try:
                    payload = json.loads(match.group(0))
                except Exception:
                    payload = {}

        raw_edges = payload.get("edges", []) if isinstance(payload, dict) else []
        if not isinstance(raw_edges, list):
            return (
                fallback_graph_edges(chunk_text, max_edges=GRAPH_RAG_MAX_EDGES_PER_CHUNK),
                "deterministic_fallback",
                "The extraction LLM returned an invalid edges payload.",
            )

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
            if len(edges) >= GRAPH_RAG_MAX_EDGES_PER_CHUNK:
                break
        if edges:
            return edges, "llm", ""
        return (
            fallback_graph_edges(chunk_text, max_edges=GRAPH_RAG_MAX_EDGES_PER_CHUNK),
            "deterministic_fallback",
            "The extraction LLM returned no valid edges.",
        )

    def ingest_pdf(
        self,
        filename: str,
        pdf_bytes: bytes,
        progress_callback: Any = None,
        should_cancel: Any = None,
    ) -> dict[str, Any]:
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
        if progress_callback:
            progress_callback(0, len(limited_chunks), phase="preparing")

        doc_id = str(uuid.uuid4())
        total_edges = 0
        fallback_chunks = 0
        llm_enabled = True
        last_fallback_reason = ""
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
                    if should_cancel and should_cancel():
                        raise GraphIngestCanceled("Graph ingestion canceled by the user.")
                    if progress_callback:
                        progress_callback(
                            idx,
                            len(limited_chunks),
                            phase="extracting",
                            current_chunk=idx + 1,
                            extraction_mode="llm" if llm_enabled else "deterministic_fallback",
                        )
                    if llm_enabled:
                        edges, extraction_mode, fallback_reason = self._extract_edges(chunk_text)
                        if extraction_mode != "llm":
                            llm_enabled = False
                            last_fallback_reason = fallback_reason
                    else:
                        edges = fallback_graph_edges(
                            chunk_text,
                            max_edges=GRAPH_RAG_MAX_EDGES_PER_CHUNK,
                        )
                        extraction_mode = "deterministic_fallback"
                    if should_cancel and should_cancel():
                        raise GraphIngestCanceled("Graph ingestion canceled by the user.")
                    if extraction_mode != "llm":
                        fallback_chunks += 1
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
                    if progress_callback:
                        progress_callback(
                            idx + 1,
                            len(limited_chunks),
                            phase="persisted",
                            current_chunk=idx + 1,
                            extraction_mode=extraction_mode,
                            edges_created=total_edges,
                            fallback_chunks=fallback_chunks,
                            fallback_reason=last_fallback_reason,
                        )

        return {
            "doc_id": doc_id,
            "source_file": filename,
            "chunks_processed": len(limited_chunks),
            "edges_created": total_edges,
            "fallback_chunks": fallback_chunks,
            "extraction_mode": "llm" if fallback_chunks == 0 else "hybrid_fallback",
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
            "llm_provider": _normalize_llm_provider(LLM_PROVIDER),
            "llm_model": self.model_name,
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

    def query_with_trace(self, question: str) -> tuple[str, dict[str, Any]]:
        """Answer a question using graph-local context and return trace details."""
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
            answer = (
                "1. [WARN] No graph context matched that question yet.\n"
                "2. [GOOD] Next step: ingest a Kafka PDF into Graph RAG, then ask again."
            )
            trace = build_static_trace(
                prompt_source="graphrag_query",
                user_prompt=question,
                reason="no graph context matched query terms",
            )
            trace["rag_content"] = {"kind": "graph_rag", "items": 0, "chars": 0}
            return answer, trace

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

        prompt = render_prompt(
            "graphrag_query_prompt.txt", question=question, graph_context=graph_context
        )
        started = time.time()
        response = self._llm.invoke(prompt)
        duration_ms = int((time.time() - started) * 1000)
        answer = normalize_content(response.content).strip()

        usage = _token_usage_from_ai_message(response) if isinstance(response, AIMessage) else {
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "source": "",
        }
        trace = {
            "prompt_source": "graphrag_query",
            "prompts": {
                "system_prompt_file": "prompts/graphrag_query_prompt.txt",
                "system_prompt_chars": len(_load_prompt_template("graphrag_query_prompt.txt")),
                "user_prompt_chars": len(question),
                "user_prompt_preview": question[:500],
                "rendered_prompt_chars": len(prompt),
            },
            "rag_content": {
                "kind": "graph_rag",
                "items": len(context_lines),
                "chars": len(graph_context),
                "query_terms": terms,
            },
            "mcp_usage": {
                "tool_call_count": 0,
                "tool_calls_by_name": {},
                "tool_result_count": 0,
                "tool_results_by_name": {},
                "unique_tools_called": [],
            },
            "token_usage": {
                "input_tokens": int(usage["input_tokens"]),
                "output_tokens": int(usage["output_tokens"]),
                "total_tokens": int(usage["total_tokens"]),
                "ai_messages_with_usage": 1 if int(usage["total_tokens"]) > 0 else 0,
                "sources": [usage["source"]] if usage["source"] else [],
            },
            "message_count": 1,
            "duration_ms": duration_ms,
            "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        return to_numbered_bullets(answer), trace

    def query(self, question: str) -> str:
        """Answer a question using graph-local context retrieved from Neo4j edges."""
        answer, _trace = self.query_with_trace(question)
        return answer


class KafkaExpertRuntime:
    """One isolated monitor or remediation LLM agent with role-specific tools and memory."""

    def __init__(
        self,
        context_store: CouchDBContextStore | None = None,
        *,
        agent_role: str = "monitor",
    ) -> None:
        """Initialize an MCP-backed agent with a strict monitor/remediation boundary."""
        if agent_role not in {"monitor", "remediation"}:
            raise ValueError(f"Unsupported Kafka agent role: {agent_role}")
        self.agent_role = agent_role
        self.system_prompt = SYSTEM_PROMPT if agent_role == "monitor" else REMEDIATION_SYSTEM_PROMPT
        self.system_prompt_file = (
            "prompts/system_prompt.txt"
            if agent_role == "monitor"
            else "prompts/remediation_system_prompt.txt"
        )
        component = "Kafka Monitor Agent" if agent_role == "monitor" else "Kafka Remediation Agent"
        _validate_llm_provider_configuration(component)

        self._lock = threading.Lock()
        if context_store is not None and context_store.enabled:
            self._checkpointer = CouchDBBackedMemorySaver(context_store)
        else:
            self._checkpointer = MemorySaver()
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
                "kafka_rest": {
                    "transport": "stdio",
                    "command": "python",
                    "args": ["/app/scripts/kafka_rest_mcp_server.py"],
                    "env": {
                        "KAFKA_REST_PROXY_URL": os.getenv("KAFKA_REST_PROXY_URL", "http://kafka-rest-proxy:8082"),
                        "MCP_TRANSPORT": "stdio",
                    },
                },
            }
        )
        prometheus_tools = asyncio.run(self._mcp_client.get_tools(server_name="prometheus"))
        kafka_admin_tools = asyncio.run(self._mcp_client.get_tools(server_name="kafka_admin"))
        kafka_rest_tools = asyncio.run(self._mcp_client.get_tools(server_name="kafka_rest"))

        def neo4j_graph_rag_search(question: str) -> dict[str, Any]:
            """Search PDF-derived Kafka knowledge in Neo4j for causes, risks, and repair guidance."""
            return retrieve_neo4j_knowledge(
                question,
                uri=NEO4J_URI,
                username=NEO4J_USERNAME,
                password=NEO4J_PASSWORD,
                auth_disabled=NEO4J_AUTH_DISABLED,
            )

        graph_rag_tool = StructuredTool.from_function(
            func=neo4j_graph_rag_search,
            name="neo4j_graph_rag_search",
            description=(
                "Search the shared Neo4j Graph RAG knowledge built from ingested Kafka PDFs. "
                "Use it for failure causes, operational risks, and remediation guidance; verify "
                "live cluster state with Prometheus or Kafka Admin tools."
            ),
        )
        tools = [
            *prometheus_tools,
            *select_kafka_admin_tools(
                kafka_admin_tools,
                allow_mutations=agent_role == "remediation",
            ),
            *select_kafka_rest_tools(
                kafka_rest_tools,
                allow_mutations=agent_role == "remediation",
            ),
            graph_rag_tool,
        ]
        self._tools_by_name = {tool.name: tool for tool in tools}

        role_candidates = resolve_agent_models(
            agent_role,
            provider_models=_model_candidates_for_provider(LLM_PROVIDER),
            monitor_model=MONITOR_MODEL,
            monitor_fallbacks=MONITOR_MODEL_FALLBACKS,
            remediation_model=REMEDIATION_MODEL,
            remediation_fallbacks=REMEDIATION_MODEL_FALLBACKS,
        )
        llm, self.model_name = build_chat_with_fallback(
            component,
            model_candidates=role_candidates,
        )
        self._llm = llm
        self._agent = create_react_agent(
            model=llm,
            tools=tools,
            prompt=self.system_prompt,
            checkpointer=self._checkpointer,
        )

    async def _ask_async(self, user_prompt: str, thread_id: str) -> dict[str, Any]:
        """Run one async agent turn and return the full LangGraph response payload."""
        return await self._agent.ainvoke(
            {"messages": [("user", user_prompt)]},
            config={
                "recursion_limit": 25,
                "configurable": {"thread_id": thread_id},
            },
        )

    def ask_with_trace(
        self,
        user_prompt: str,
        *,
        thread_id: str | None = None,
        prompt_source: str = "chat",
    ) -> tuple[str, dict[str, Any]]:
        """Run one agent turn and return answer plus structured trace payload."""
        with self._lock:
            effective_thread_id = (thread_id or "").strip() or "kafka-expert-default"
            started = time.time()
            # MCP-backed tools are async-only; use async agent invocation path.
            result = asyncio.run(self._ask_async(user_prompt, effective_thread_id))
            duration_ms = int((time.time() - started) * 1000)
        answer = to_numbered_bullets(extract_ai_text(result))
        trace = build_agent_trace(
            prompt_source=prompt_source,
            agent_role=self.agent_role,
            system_prompt_file=self.system_prompt_file,
            system_prompt=self.system_prompt,
            user_prompt=user_prompt,
            agent_result=result,
            duration_ms=duration_ms,
        )
        return answer, trace

    def ask(self, user_prompt: str, *, thread_id: str | None = None) -> str:
        """Run one agent turn and return normalized response text."""
        answer, _trace = self.ask_with_trace(user_prompt, thread_id=thread_id)
        return answer

    def plan_with_trace(self, user_prompt: str) -> tuple[str, dict[str, Any]]:
        """Draft a repair plan without exposing tools or performing mutations."""
        with self._lock:
            started = time.time()
            response = self._llm.bind(max_tokens=450).invoke(
                [("system", REMEDIATION_PLANNING_SYSTEM_PROMPT), ("user", user_prompt)]
            )
            duration_ms = int((time.time() - started) * 1000)
        result = {"messages": [response]}
        plan = format_human_plan(extract_ai_text(result))
        trace = build_agent_trace(
            prompt_source="a2a_remediation_plan",
            agent_role=self.agent_role,
            system_prompt_file="inline:hitl_planning",
            system_prompt=REMEDIATION_PLANNING_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            agent_result=result,
            duration_ms=duration_ms,
        )
        return plan, trace

    async def _cluster_state_grounded_async(self) -> tuple[Any, Any]:
        """Fetch the required snapshot first, then ask the model to interpret verified evidence."""
        snapshot_tool = self._tools_by_name.get("kafka_cluster_state_snapshot")
        if snapshot_tool is None:
            raise RuntimeError("kafka_cluster_state_snapshot tool is unavailable")
        raw_evidence = await snapshot_tool.ainvoke({})
        evidence: Any = raw_evidence
        if isinstance(raw_evidence, str):
            try:
                evidence = json.loads(raw_evidence)
            except Exception:
                evidence = {"raw_text": raw_evidence}
        elif isinstance(raw_evidence, list) and raw_evidence:
            first = raw_evidence[0]
            text_value = first.get("text", "") if isinstance(first, dict) else getattr(first, "text", "")
            if text_value:
                try:
                    evidence = json.loads(text_value)
                except Exception:
                    evidence = {"raw_text": text_value}
        elif isinstance(raw_evidence, dict) and "content" in raw_evidence and "results" not in raw_evidence:
            content_value = raw_evidence.get("content")
            if isinstance(content_value, str):
                try:
                    evidence = json.loads(content_value)
                except Exception:
                    evidence = raw_evidence
        compact_results: dict[str, Any] = {}
        for metric_name, result in (evidence.get("results", {}) if isinstance(evidence, dict) else {}).items():
            try:
                rows = json.loads(result["raw_json"]).get("data", {}).get("result", [])
                compact_results[metric_name] = [
                    {
                        **(row.get("metric", {}) if isinstance(row, dict) else {}),
                        "value": (row.get("value", [None, None])[-1] if isinstance(row, dict) else None),
                    }
                    for row in rows
                ]
            except Exception:
                compact_results[metric_name] = result
        compact_evidence = {
            "collected_at_utc": evidence.get("collected_at_utc", "") if isinstance(evidence, dict) else "",
            "results": compact_results,
        }
        evidence_lines = [f"collected_at_utc={compact_evidence['collected_at_utc']}"]
        for metric_name, rows in compact_results.items():
            if not isinstance(rows, list):
                evidence_lines.append(f"{metric_name}={str(rows)[:300]}")
                continue
            def _row_magnitude(row: Any) -> float:
                try:
                    return abs(float(row.get("value", 0) or 0)) if isinstance(row, dict) else 0.0
                except Exception:
                    return 0.0

            ranked_rows = sorted(rows, key=_row_magnitude, reverse=True)[:8]
            row_texts: list[str] = []
            for row in ranked_rows:
                if not isinstance(row, dict):
                    row_texts.append(str(row)[:120])
                    continue
                labels = [
                    f"{key}={row[key]}"
                    for key in ("broker", "topic", "partition", "group", "state", "instance", "job", "mountpoint")
                    if key in row
                ]
                labels.append(f"value={row.get('value')}")
                row_texts.append(" ".join(labels))
            evidence_lines.append(f"{metric_name}: {'; '.join(row_texts) if row_texts else 'no series'}")
        evidence_text = "\n".join(evidence_lines)
        grounded_prompt = (
            f"{CLUSTER_STATE_PROMPT}\n\n"
            "The required kafka_cluster_state_snapshot tool has already been called. "
            "Use only the verified compact evidence below; do not invent values.\n"
            f"VERIFIED_KAFKA_SNAPSHOT:\n{evidence_text}"
        )
        response = await self._llm.bind(max_tokens=192).ainvoke(
            [("system", self.system_prompt), ("user", grounded_prompt)]
        )
        return response, compact_evidence

    def cluster_state_with_trace(self) -> tuple[str, dict[str, Any]]:
        """Run a provider-neutral, evidence-first cluster assessment."""
        with self._lock:
            started = time.time()
            response, evidence = asyncio.run(self._cluster_state_grounded_async())
            duration_ms = int((time.time() - started) * 1000)
        result = {"messages": [response]}
        answer = to_numbered_bullets(extract_ai_text(result))
        trace = build_agent_trace(
            prompt_source="cluster_state",
            agent_role=self.agent_role,
            system_prompt_file=self.system_prompt_file,
            system_prompt=self.system_prompt,
            user_prompt=CLUSTER_STATE_PROMPT,
            agent_result=result,
            duration_ms=duration_ms,
        )
        trace["mcp_usage"] = {
            "tool_call_count": 1,
            "tool_calls_by_name": {"kafka_cluster_state_snapshot": 1},
            "tool_result_count": 1,
            "tool_results_by_name": {"kafka_cluster_state_snapshot": 1},
            "unique_tools_called": ["kafka_cluster_state_snapshot"],
            "snapshot_chars": len(json.dumps(evidence, ensure_ascii=False, default=str)),
        }
        return answer, trace

    def clear_thread(self, thread_id: str) -> None:
        """Delete one thread from LangGraph checkpointer memory."""
        value = (thread_id or "").strip()
        if not value:
            return
        with self._lock:
            self._checkpointer.delete_thread(value)


class AppState:
    """In-memory state for cluster status text and chat transcript."""

    DOC_ID = "context::app_state"

    def __init__(
        self,
        context_store: CouchDBContextStore | None = None,
        *,
        doc_id: str | None = None,
    ) -> None:
        """Initialize default status message, error field, and chat history."""
        self.lock = threading.Lock()
        self._context_store = context_store
        self.doc_id = (doc_id or self.DOC_ID).strip()
        self.status_text = "Click 'Query Full Cluster State' to run Kafka Expert."
        self.status_updated_at = ""
        self.last_error = ""
        self.chat: list[dict[str, str]] = []
        self.chat_epoch = 0
        self.chat_thread_id = new_chat_thread_id()
        self.context_persist_error = ""
        self.last_trace: dict[str, Any] = {}
        self.graph_metrics: dict[str, Any] = new_graph_metrics()
        self._load_from_couchdb()

    def _load_from_couchdb(self) -> None:
        """Hydrate state from CouchDB if persisted context is available."""
        if self._context_store is None or not self._context_store.enabled:
            return
        payload = self._context_store.get_doc(self.doc_id)
        if not isinstance(payload, dict):
            return

        status_text = str(payload.get("status_text", self.status_text))
        status_updated_at = str(payload.get("status_updated_at", self.status_updated_at))
        last_error = str(payload.get("last_error", self.last_error))
        chat_epoch = payload.get("chat_epoch", self.chat_epoch)
        chat_thread_id = str(payload.get("chat_thread_id", self.chat_thread_id)).strip() or new_chat_thread_id()
        chat_items = payload.get("chat", [])
        last_trace = payload.get("last_trace", {})
        graph_metrics = payload.get("graph_metrics", {})

        restored_chat: list[dict[str, str]] = []
        if isinstance(chat_items, list):
            for item in chat_items[-80:]:
                if not isinstance(item, dict):
                    continue
                role = str(item.get("role", "")).strip()
                text = str(item.get("text", "")).strip()
                at_utc = str(item.get("at_utc", "")).strip()
                if not role:
                    continue
                restored_chat.append({"role": role, "text": text, "at_utc": at_utc})

        with self.lock:
            self.status_text = status_text
            self.status_updated_at = status_updated_at
            self.last_error = last_error
            try:
                self.chat_epoch = int(chat_epoch)
            except Exception:
                self.chat_epoch = 0
            self.chat_thread_id = chat_thread_id
            self.chat = restored_chat
            self.last_trace = last_trace if isinstance(last_trace, dict) else {}
            if isinstance(graph_metrics, dict):
                merged = new_graph_metrics()
                for top_key in ("ingest", "query"):
                    candidate = graph_metrics.get(top_key)
                    if isinstance(candidate, dict):
                        merged[top_key].update(candidate)
                recent_runs = graph_metrics.get("recent_runs", [])
                if isinstance(recent_runs, list):
                    merged["recent_runs"] = [item for item in recent_runs[-40:] if isinstance(item, dict)]
                merged["updated_at_utc"] = str(graph_metrics.get("updated_at_utc", "")).strip()
                self.graph_metrics = merged
            else:
                self.graph_metrics = new_graph_metrics()

    def _persist_locked(self) -> None:
        """Persist state snapshot to CouchDB while holding lock."""
        if self._context_store is None or not self._context_store.enabled:
            return
        payload = {
            "type": "kafka_expert_app_state",
            "status_text": self.status_text,
            "status_updated_at": self.status_updated_at,
            "last_error": self.last_error,
            "chat": list(self.chat),
            "chat_epoch": self.chat_epoch,
            "chat_thread_id": self.chat_thread_id,
            "last_trace": self.last_trace,
            "graph_metrics": self.graph_metrics,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        ok = self._context_store.upsert_doc(self.doc_id, payload)
        self.context_persist_error = "" if ok else (self._context_store.last_error or "couchdb-upsert-failed")

    def set_status(self, text: str, error: str = "", *, epoch: int | None = None) -> None:
        """Update cluster status text, timestamp, and optional error message."""
        with self.lock:
            if epoch is not None and epoch != self.chat_epoch:
                return
            self.status_text = text
            self.status_updated_at = utc_now_iso()
            self.last_error = error
            self._persist_locked()

    def add_chat(self, role: str, text: str, *, epoch: int | None = None) -> None:
        """Append a chat entry and enforce bounded chat history."""
        with self.lock:
            if epoch is not None and epoch != self.chat_epoch:
                return
            self.chat.append({"role": role, "text": text, "at_utc": utc_now_iso()})
            self.chat = self.chat[-80:]
            self._persist_locked()

    def current_chat_epoch(self) -> int:
        """Return the current chat epoch used to suppress stale in-flight writes."""
        with self.lock:
            return self.chat_epoch

    def current_thread_id(self) -> str:
        """Return current LangGraph thread id used for checkpointed memory."""
        with self.lock:
            return self.chat_thread_id

    def clear_chat(self, clear_status: bool = False) -> None:
        """Clear chat transcript and optionally clear the cluster-status panel text."""
        with self.lock:
            self.chat_epoch += 1
            self.chat = []
            self.chat_thread_id = new_chat_thread_id()
            self.last_trace = {}
            if clear_status:
                self.status_text = ""
                self.status_updated_at = ""
                self.last_error = ""
            self._persist_locked()

    def set_last_trace(self, trace: dict[str, Any], *, epoch: int | None = None) -> None:
        """Persist the latest pipeline trace payload for UI/API diagnostics."""
        with self.lock:
            if epoch is not None and epoch != self.chat_epoch:
                return
            self.last_trace = trace if isinstance(trace, dict) else {}
            self._persist_locked()

    def _append_graph_recent_locked(self, row: dict[str, Any]) -> None:
        """Append one Graph RAG run summary row while preserving a bounded history."""
        recent = self.graph_metrics.get("recent_runs", [])
        if not isinstance(recent, list):
            recent = []
        recent.append(row)
        self.graph_metrics["recent_runs"] = recent[-40:]

    def record_graph_ingest(
        self,
        *,
        ok: bool,
        duration_ms: int,
        source_file: str = "",
        chunks_processed: int = 0,
        edges_created: int = 0,
        error: str = "",
    ) -> None:
        """Record one Graph RAG PDF ingest run for dashboard metrics."""
        with self.lock:
            metrics = self.graph_metrics.get("ingest", {})
            if not isinstance(metrics, dict):
                metrics = {}
            metrics["runs"] = int(metrics.get("runs", 0)) + 1
            if ok:
                metrics["success"] = int(metrics.get("success", 0)) + 1
                metrics["total_chunks"] = int(metrics.get("total_chunks", 0)) + max(0, int(chunks_processed))
                metrics["total_edges"] = int(metrics.get("total_edges", 0)) + max(0, int(edges_created))
            else:
                metrics["failure"] = int(metrics.get("failure", 0)) + 1
            metrics["total_duration_ms"] = int(metrics.get("total_duration_ms", 0)) + max(0, int(duration_ms))
            metrics["last"] = {
                "ok": bool(ok),
                "duration_ms": max(0, int(duration_ms)),
                "source_file": str(source_file or ""),
                "chunks_processed": max(0, int(chunks_processed)),
                "edges_created": max(0, int(edges_created)),
                "error": str(error or ""),
                "at_utc": utc_now_iso(),
            }
            self.graph_metrics["ingest"] = metrics
            self._append_graph_recent_locked(
                {
                    "kind": "ingest",
                    "ok": bool(ok),
                    "duration_ms": max(0, int(duration_ms)),
                    "tokens": 0,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "context_items": 0,
                    "chunks_processed": max(0, int(chunks_processed)),
                    "edges_created": max(0, int(edges_created)),
                    "note": str(source_file or ""),
                    "error": str(error or ""),
                    "at_utc": utc_now_iso(),
                }
            )
            self.graph_metrics["updated_at_utc"] = utc_now_iso()
            self._persist_locked()

    def record_graph_query(
        self,
        *,
        ok: bool,
        question: str = "",
        duration_ms: int = 0,
        token_usage: dict[str, Any] | None = None,
        rag_content: dict[str, Any] | None = None,
        error: str = "",
    ) -> None:
        """Record one Graph RAG question-answer run for dashboard metrics."""
        usage = token_usage if isinstance(token_usage, dict) else {}
        rag = rag_content if isinstance(rag_content, dict) else {}
        input_tokens = max(0, int(usage.get("input_tokens", 0) or 0))
        output_tokens = max(0, int(usage.get("output_tokens", 0) or 0))
        total_tokens = max(0, int(usage.get("total_tokens", 0) or (input_tokens + output_tokens)))
        context_items = max(0, int(rag.get("items", 0) or 0))
        context_chars = max(0, int(rag.get("chars", 0) or 0))
        safe_duration = max(0, int(duration_ms))

        with self.lock:
            metrics = self.graph_metrics.get("query", {})
            if not isinstance(metrics, dict):
                metrics = {}
            metrics["runs"] = int(metrics.get("runs", 0)) + 1
            if ok:
                metrics["success"] = int(metrics.get("success", 0)) + 1
            else:
                metrics["failure"] = int(metrics.get("failure", 0)) + 1
            metrics["total_duration_ms"] = int(metrics.get("total_duration_ms", 0)) + safe_duration
            metrics["total_input_tokens"] = int(metrics.get("total_input_tokens", 0)) + input_tokens
            metrics["total_output_tokens"] = int(metrics.get("total_output_tokens", 0)) + output_tokens
            metrics["total_tokens"] = int(metrics.get("total_tokens", 0)) + total_tokens
            metrics["total_context_items"] = int(metrics.get("total_context_items", 0)) + context_items
            metrics["total_context_chars"] = int(metrics.get("total_context_chars", 0)) + context_chars
            if context_items <= 0:
                metrics["zero_context_runs"] = int(metrics.get("zero_context_runs", 0)) + 1
            metrics["last"] = {
                "ok": bool(ok),
                "duration_ms": safe_duration,
                "question_preview": str(question or "")[:220],
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "total_tokens": total_tokens,
                "context_items": context_items,
                "context_chars": context_chars,
                "error": str(error or ""),
                "at_utc": utc_now_iso(),
            }
            self.graph_metrics["query"] = metrics
            self._append_graph_recent_locked(
                {
                    "kind": "query",
                    "ok": bool(ok),
                    "duration_ms": safe_duration,
                    "tokens": total_tokens,
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "context_items": context_items,
                    "chunks_processed": 0,
                    "edges_created": 0,
                    "note": str(question or "")[:120],
                    "error": str(error or ""),
                    "at_utc": utc_now_iso(),
                }
            )
            self.graph_metrics["updated_at_utc"] = utc_now_iso()
            self._persist_locked()

    def graph_metrics_snapshot(self) -> dict[str, Any]:
        """Return a safe copy of Graph RAG metrics for API responses."""
        with self.lock:
            return json.loads(json.dumps(self.graph_metrics))

    def reset_graph_metrics(self) -> None:
        """Reset Graph RAG metrics counters/history and persist immediately."""
        with self.lock:
            self.graph_metrics = new_graph_metrics()
            self.graph_metrics["updated_at_utc"] = utc_now_iso()
            self._persist_locked()

    def snapshot(self) -> dict[str, Any]:
        """Return a thread-safe copy of current app state for API responses."""
        with self.lock:
            return {
                "status_text": self.status_text,
                "status_updated_at": self.status_updated_at,
                "last_error": self.last_error,
                "chat": list(self.chat),
                "chat_epoch": self.chat_epoch,
                "chat_thread_id": self.chat_thread_id,
                "context_persist_error": self.context_persist_error,
                "last_trace": dict(self.last_trace),
                "graph_metrics": json.loads(json.dumps(self.graph_metrics)),
            }


app = Flask(__name__)
context_store = CouchDBContextStore.from_env()
state = AppState(
    context_store=context_store if context_store.enabled else None,
    doc_id=(
        AppState.DOC_ID
        if AGENT_PROCESS_ROLE == "monitor"
        else f"{AppState.DOC_ID}::remediation"
    ),
)
remediation_hitl = RemediationHitlStore(enabled=REMEDIATION_HITL_ENABLED)
cluster_state_lock = threading.Lock()
cluster_state_running = False
completed_a2a_tasks: set[str] = set()
completed_a2a_tasks_lock = threading.Lock()
pending_a2a_prompts: dict[str, str] = {}
runtime: KafkaExpertRuntime | None = None
runtime_error = ""
remediation_runtime: KafkaExpertRuntime | None = None
remediation_runtime_error = ""
graph_runtime: GraphRAGRuntime | None = None
GRAPH_INGEST_JOBS: dict[str, dict[str, Any]] = {}
GRAPH_INGEST_JOBS_LOCK = threading.Lock()
graph_runtime_error = ""
graph_runtime_lock = threading.Lock()
runtime_reconfig_lock = threading.Lock()

if AGENT_PROCESS_ROLE == "monitor":
    try:
        runtime = KafkaExpertRuntime(
            context_store=context_store if context_store.enabled else None,
            agent_role="monitor",
        )
    except Exception as exc:
        runtime_error = str(exc)
        state.set_status(
            "Kafka Monitor Agent is waiting for LLM provider configuration. Update LLM_* settings and refresh.",
            error=runtime_error,
        )

    try:
        graph_runtime = GraphRAGRuntime()
    except Exception as exc:
        graph_runtime_error = str(exc)
else:
    try:
        remediation_runtime = KafkaExpertRuntime(
            context_store=context_store if context_store.enabled else None,
            agent_role="remediation",
        )
        state.set_status(
            "1. [GOOD] Kafka Remediation Agent is ready and waiting for an authorized A2A task."
        )
    except Exception as exc:
        remediation_runtime_error = str(exc)
        state.set_status(
            "1. [BAD] Kafka Remediation Agent is unavailable. Fix: Configure its LLM and reload.",
            error=remediation_runtime_error,
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
  <title>__PAGE_TITLE__</title>
  <style>
    :root {
      --bg: #0b1117;
      --panel: #111a23;
      --ink: #e6eff6;
      --ink-dim: #9ab0c1;
      --line: #2b3d4d;
      --accent: #0da37f;
      --accent-2: #0a6a53;
      --good: #22c55e;
      --warn: #fde047;
      --bad: #991b1b;
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
    .llm-hero {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 14px;
      border-radius: 14px;
      border: 1px solid #1d3a4a;
      background: linear-gradient(135deg, rgba(13,163,127,0.18), rgba(57,109,242,0.14));
      box-shadow: 0 18px 40px rgba(0,0,0,0.28);
    }
    .llm-hero h2 { margin: 0 0 6px 0; }
    .llm-hero .eyebrow {
      letter-spacing: 0.4px;
      text-transform: uppercase;
      font-size: 11px;
      color: var(--ink-dim);
      margin: 0 0 6px 0;
    }
    .llm-chips {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 8px;
    }
    .llm-chip {
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(13,163,127,0.15);
      border: 1px solid rgba(13,163,127,0.5);
      font-size: 12px;
      letter-spacing: 0.3px;
    }
    .llm-toggle {
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 10px 16px;
      background: #0f1f2c;
      color: var(--ink);
      font-weight: 700;
      cursor: pointer;
      box-shadow: inset 0 0 0 1px rgba(255,255,255,0.04);
      transition: all 160ms ease;
    }
    .llm-toggle:hover { transform: translateY(-1px); box-shadow: 0 10px 22px rgba(0,0,0,0.35); }
    .llm-menu {
      margin-top: 12px;
      overflow: hidden;
      max-height: 0;
      opacity: 0;
      transition: max-height 240ms ease, opacity 240ms ease, padding 240ms ease;
      padding: 0 4px;
      border-radius: 12px;
      border: 1px solid transparent;
    }
    .llm-menu.open {
      padding: 14px 12px 10px 12px;
      max-height: 1200px;
      opacity: 1;
      border-color: #1d3a4a;
      background: linear-gradient(145deg, rgba(15,26,36,0.95), rgba(19,33,46,0.95));
      box-shadow: 0 20px 36px rgba(0,0,0,0.32);
    }
    .llm-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 12px;
      margin-bottom: 10px;
    }
    .llm-field {
      display: flex;
      flex-direction: column;
      gap: 6px;
      font-size: 13px;
    }
    .llm-field label { color: var(--ink-dim); font-weight: 600; letter-spacing: 0.2px; }
    .llm-field input,
    .llm-field select {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px;
      background: #0e1821;
      color: var(--ink);
      font-size: 13px;
    }
    .llm-slider-row {
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 10px;
      border-radius: 999px;
      background: #13212b;
      border: 1px solid #234052;
      font-size: 12px;
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
    button.llm-chip {
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(13,163,127,0.15);
      border: 1px solid rgba(13,163,127,0.5);
      color: var(--ink);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.3px;
    }
    button.llm-chip:hover {
      background: rgba(13,163,127,0.28);
      transform: translateY(-1px);
    }
    button.llm-chip.active {
      background: var(--accent-2);
      border-color: #2dd4bf;
      color: #ffffff;
      box-shadow: 0 0 0 2px rgba(45,212,191,0.2);
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
    .severity-legend {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin: 0 0 10px 0;
    }
    .severity-key {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border-radius: 999px;
      padding: 5px 9px;
      font-size: 12px;
      font-weight: 800;
      border: 1px solid transparent;
    }
    .severity-key.good { background: var(--good); border-color: #15803d; color: #04210f; }
    .severity-key.warn { background: var(--warn); border-color: #ca8a04; color: #352300; }
    .severity-key.bad { background: var(--bad); border-color: #7f1d1d; color: #ffffff; }
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
      background: var(--good);
      border-color: #15803d;
      color: #04210f;
    }
    .status-line.warn {
      background: var(--warn);
      border-color: #ca8a04;
      color: #352300;
    }
    .status-line.bad {
      background: var(--bad);
      border-color: #7f1d1d;
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
    .hitl-panel {
      margin: 12px 0;
      border: 1px solid #486274;
      border-radius: 12px;
      background: #0d1821;
      padding: 14px;
    }
    .hitl-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      flex-wrap: wrap;
    }
    .hitl-header h3 { margin: 0 0 4px 0; }
    .hitl-mode {
      display: inline-flex;
      align-items: center;
      gap: 9px;
      font-size: 13px;
      font-weight: 700;
    }
    .hitl-switch {
      position: relative;
      display: inline-block;
      width: 48px;
      height: 26px;
    }
    .hitl-switch input {
      width: 0;
      height: 0;
      opacity: 0;
    }
    .hitl-slider {
      position: absolute;
      inset: 0;
      cursor: pointer;
      border-radius: 999px;
      background: #405261;
      transition: 160ms ease;
    }
    .hitl-slider::before {
      content: "";
      position: absolute;
      width: 20px;
      height: 20px;
      left: 3px;
      top: 3px;
      border-radius: 50%;
      background: #f2f7fa;
      transition: 160ms ease;
    }
    .hitl-switch input:checked + .hitl-slider { background: #0d9b74; }
    .hitl-switch input:checked + .hitl-slider::before { transform: translateX(22px); }
    .hitl-task {
      margin-top: 12px;
      border: 1px solid #355063;
      border-radius: 10px;
      background: #111f2a;
      padding: 12px;
    }
    .hitl-task-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
      margin-bottom: 10px;
      font-size: 12px;
      color: var(--ink-dim);
    }
    .hitl-review-prompt {
      border: 1px solid #a16207;
      border-radius: 10px;
      background: #2b1d07;
      padding: 12px;
      margin-bottom: 12px;
    }
    .hitl-review-prompt h4 {
      margin: 0 0 8px 0;
      color: #fde68a;
      font-size: 16px;
    }
    .hitl-review-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
      gap: 8px;
    }
    .hitl-review-item {
      border: 1px solid #5e481c;
      border-radius: 8px;
      background: #1c160b;
      padding: 9px;
      font-size: 12px;
      line-height: 1.45;
    }
    .hitl-review-item strong {
      display: block;
      margin-bottom: 4px;
      color: #fef3c7;
    }
    .hitl-what-wrong {
      border: 1px solid #b45309;
      border-radius: 9px;
      background: #211507;
      color: #ffedd5;
      padding: 11px;
      font-size: 14px;
      line-height: 1.5;
    }
    .hitl-fix-options {
      display: grid;
      gap: 8px;
      margin-top: 8px;
    }
    .hitl-fix-option {
      display: flex;
      align-items: flex-start;
      gap: 9px;
      width: 100%;
      border: 1px solid #386078;
      border-radius: 8px;
      background: #102431;
      color: #d9edf7;
      padding: 10px;
      text-align: left;
      cursor: pointer;
      font: inherit;
      line-height: 1.4;
    }
    .hitl-fix-option:hover { border-color: #38bdf8; background: #15364a; }
    .hitl-fix-option:disabled { opacity: 0.6; cursor: wait; }
    .hitl-fix-number { color: #7dd3fc; font-weight: 800; min-width: 22px; }
    .hitl-badge {
      border-radius: 999px;
      padding: 4px 8px;
      border: 1px solid #a16207;
      background: #422006;
      color: #fde68a;
      font-weight: 700;
    }
    .hitl-badge.working { border-color: #2563eb; background: #172554; color: #bfdbfe; }
    .hitl-badge.completed { border-color: #15803d; background: #052e16; color: #bbf7d0; }
    .hitl-badge.rejected,
    .hitl-badge.failed { border-color: #991b1b; background: #450a0a; color: #fecaca; }
    .hitl-block-label {
      margin: 9px 0 5px;
      color: #9fc5db;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.2px;
      text-transform: uppercase;
    }
    .hitl-block {
      margin: 0;
      max-height: 230px;
      overflow: auto;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      border: 1px solid #2e4657;
      border-radius: 8px;
      background: #09131b;
      color: #d9edf7;
      padding: 10px;
      font: 12px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace;
    }
    .hitl-decision-row {
      display: grid;
      grid-template-columns: minmax(180px, 1fr) auto auto;
      gap: 8px;
      margin-top: 10px;
    }
    .hitl-note {
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 9px;
      background: #0e1821;
      color: var(--ink);
    }
    .hitl-context-details {
      margin-top: 10px;
      border: 1px solid #355063;
      border-radius: 8px;
      background: #0b151d;
      overflow: hidden;
    }
    .hitl-context-details summary {
      cursor: pointer;
      padding: 10px;
      color: #9fc5db;
      font-size: 13px;
      font-weight: 700;
    }
    .hitl-context-details[open] summary {
      border-bottom: 1px solid #355063;
    }
    .hitl-context-details .hitl-block {
      max-height: 520px;
      border: 0;
      border-radius: 0;
    }
    button.hitl-reject { background: #7f1d1d; }
    .hitl-empty {
      margin: 12px 0 0;
      color: var(--ink-dim);
      font-size: 13px;
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
    .usage-strip {
      margin: 8px 0 8px 0;
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
    }
    .usage-pill {
      display: inline-flex;
      align-items: center;
      padding: 6px 10px;
      border-radius: 999px;
      border: 1px solid #304858;
      background: #101d27;
      color: #c7dfef;
      font-size: 12px;
      letter-spacing: 0.2px;
    }
    .usage-budget {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #101a23;
      padding: 8px 10px;
      margin-bottom: 10px;
    }
    .usage-budget-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
      margin-bottom: 6px;
      font-size: 12px;
      color: var(--ink-dim);
    }
    .usage-meter {
      height: 8px;
      border-radius: 999px;
      background: #1b2a36;
      overflow: hidden;
      border: 1px solid #2b4355;
      margin-bottom: 6px;
    }
    .usage-meter-fill {
      display: block;
      height: 100%;
      width: 0%;
      background: linear-gradient(90deg, #0da37f, #3dc7a5);
      transition: width 180ms ease;
    }
    .usage-meter-fill.warn {
      background: linear-gradient(90deg, #f59e0b, #fde047);
    }
    .usage-meter-fill.over {
      background: linear-gradient(90deg, #dc2626, #ef4444);
    }
    .busy-indicator {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 6px 10px;
      border-radius: 999px;
      border: 1px solid #355063;
      background: #12212b;
      color: #d9edf7;
      font-size: 12px;
      letter-spacing: 0.2px;
    }
    .busy-indicator.ai {
      border-color: #355063;
      background: #12212b;
    }
    .busy-indicator.processing {
      border-color: #3b4b39;
      background: #152315;
    }
    .busy-indicator.hidden { display: none; }
    .busy-spinner {
      width: 12px;
      height: 12px;
      border-radius: 50%;
      border: 2px solid #4d6b80;
      border-top-color: #d9edf7;
      animation: spin 900ms linear infinite;
      flex: 0 0 12px;
    }
    @keyframes spin {
      from { transform: rotate(0deg); }
      to { transform: rotate(360deg); }
    }
    .error { color: var(--bad); }
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
    .finetune-card {
      border: 1px solid #355063;
      border-radius: 10px;
      background: #0f1a24;
      padding: 14px;
      margin-bottom: 10px;
    }
    .finetune-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 10px;
      margin: 10px 0;
    }
    .finetune-grid label { display: grid; gap: 5px; color: var(--ink-dim); font-size: 12px; }
    .finetune-grid input, .finetune-grid select {
      border: 1px solid var(--line); border-radius: 8px; padding: 9px;
      background: #0e1821; color: var(--ink);
    }
    .finetune-status { border: 1px solid #516273; border-radius: 9px; padding: 11px 13px; margin: 10px 0; font-weight: 600; }
    .finetune-status.available { border-color: #2f9e68; background: rgba(47, 158, 104, 0.14); color: #8ff0b9; }
    .finetune-status.unavailable { border-color: #b36a55; background: rgba(179, 106, 85, 0.14); color: #ffb19b; }
    .finetune-live { display: flex; align-items: center; gap: 8px; margin: 8px 0; color: var(--ink-dim); }
    .finetune-live-icon { color: #77d6ff; font-size: 18px; }
    .finetune-live-icon.running { animation: finetunePulse 1.1s ease-in-out infinite; }
    .finetune-time-track { height: 6px; flex: 1; background: #22313e; border-radius: 99px; overflow: hidden; }
    .finetune-time-fill { height: 100%; width: 0%; background: #45b7e8; transition: width .3s ease; }
    @keyframes finetunePulse { 50% { opacity: .35; transform: scale(1.25); } }
    .rag-metrics-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 10px;
      margin: 10px 0 12px 0;
    }
    .rag-metric-card {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #0f1a24;
      padding: 10px;
    }
    .rag-metric-label {
      font-size: 12px;
      color: var(--ink-dim);
      margin-bottom: 6px;
      letter-spacing: 0.2px;
    }
    .rag-metric-value {
      font-size: 20px;
      font-weight: 700;
      line-height: 1.2;
      color: var(--ink);
    }
    .rag-metric-sub {
      margin-top: 5px;
      font-size: 12px;
      color: var(--ink-dim);
    }
    .rag-recent-table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 8px;
      font-size: 12px;
    }
    .rag-recent-table th,
    .rag-recent-table td {
      border: 1px solid var(--line);
      padding: 6px 8px;
      text-align: left;
      vertical-align: top;
    }
    .rag-recent-table th {
      background: #13202c;
      color: var(--ink-dim);
      font-weight: 700;
    }
    .rag-recent-table td {
      background: #0d161f;
      color: var(--ink);
    }
    .rag-status-badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 46px;
      border-radius: 999px;
      padding: 2px 8px;
      font-weight: 700;
      font-size: 11px;
      letter-spacing: 0.2px;
      border: 1px solid transparent;
    }
    .rag-status-badge.ok {
      color: #052013;
      background: #22c55e;
      border-color: #15803d;
    }
    .rag-status-badge.fail {
      color: #ffffff;
      background: #ef4444;
      border-color: #b91c1c;
    }
    @media (max-width: 980px) {
      .chat-log { min-height: 300px; }
      .embed-frame { min-height: 620px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <h1 class="title">__PAGE_HEADING__</h1>
    <p class="subtitle">__PAGE_SUBTITLE__</p>

    <div class="tabs">
      <button id="tabExpertBtn" class="tab-btn active" onclick="switchTab('expert')">__EXPERT_TAB_LABEL__</button>
      <button id="tabGraphRagBtn" class="tab-btn" onclick="switchTab('graph_rag')">Graph RAG</button>
      <button id="tabRagMetricsBtn" class="tab-btn" onclick="switchTab('rag_metrics')">RAG Metrics</button>
      <button id="tabLlmBtn" class="tab-btn" onclick="switchTab('llm')">LLM</button>
      <button id="tabDeepEvalBtn" class="tab-btn" onclick="switchTab('deepeval')">DeepEval Judge</button>
      <button id="tabFineTuningBtn" class="tab-btn" onclick="switchTab('fine_tuning')">Fine Tuning</button>
      <button id="tabProducerBtn" class="tab-btn" onclick="switchTab('producer')">Kafka Producer</button>
      <button id="tabConsumerBtn" class="tab-btn" onclick="switchTab('consumer')">Kafka Consumer</button>
      <button id="tabGrafanaBtn" class="tab-btn" onclick="switchTab('grafana')">Grafana</button>
      <button id="tabNeo4jBrowserBtn" class="tab-btn" onclick="switchTab('neo4j_browser')">Neo4j Browser</button>
      <button id="tabKafkaUiBtn" class="tab-btn" onclick="switchTab('kafka_ui')">Kafka UI</button>
    </div>

    <section id="expertPanel" class="panel">
      <h2>__AGENT_PANEL_HEADING__</h2>
      <div class="severity-legend" aria-label="Finding severity colors">
        <span class="severity-key good">Green = GOOD</span>
        <span class="severity-key warn">Yellow = WARN</span>
        <span class="severity-key bad">Red = BAD</span>
      </div>
      <div id="hitlPanel" class="hitl-panel hidden">
        <div class="hitl-header">
          <div>
            <h3>Human-in-the-Loop Approval</h3>
            <p class="statusline">Review the Monitor's A2A message and the Doer's proposed repair plan before execution.</p>
          </div>
          <label class="hitl-mode" for="hitlToggle">
            <span id="hitlModeLabel">Human approval ON</span>
            <span class="hitl-switch">
              <input id="hitlToggle" type="checkbox" onchange="toggleHitlMode()" />
              <span class="hitl-slider"></span>
            </span>
          </label>
        </div>
        <p class="statusline" id="hitlStatus">Loading approval mode...</p>
        <div id="hitlTasks"></div>
      </div>
      <div class="actions">
        <button id="refreshStateBtn" onclick="refreshClusterState()">__REFRESH_LABEL__</button>
        <button id="clearChatBtn" class="secondary" onclick="clearChat()">Clear Chat</button>
        <p class="statusline">__STATUS_LABEL__: <span id="statusUpdated">n/a</span></p>
      </div>
      <div id="expertBusy" class="busy-indicator hidden" role="status" aria-live="polite">
        <span class="busy-spinner" aria-hidden="true"></span>
        <span id="expertBusyText">Processing (00:00)</span>
      </div>
      <div class="usage-strip">
        <span class="usage-pill" id="usageRunTokens">Run tokens: 0</span>
        <span class="usage-pill" id="usageSessionTokens">Session tokens: 0</span>
        <span class="usage-pill" id="usageLastRuntime">Last AI runtime: 00:00</span>
      </div>
      <div class="usage-budget">
        <div class="usage-budget-row">
          <span>AI token budget</span>
          <span id="usageBudgetValue">0 / 0 tokens (0.0%)</span>
        </div>
        <div class="usage-meter">
          <span id="usageBudgetFill" class="usage-meter-fill"></span>
        </div>
        <p class="statusline" id="usageSourceLine">Token usage updates after each AI response.</p>
      </div>
      <div class="chat-log" id="chatLog"></div>
      <div class="chat-input-row" id="directChatRow">
        <input id="chatInput" type="text" placeholder="Ask Kafka cluster questions only..." />
        <button id="chatSendBtn" onclick="sendChat()">Send</button>
      </div>
      <p class="statusline" id="agentPolicyText">Non-Kafka questions are refused by policy.</p>
      <p class="statusline error" id="statusError"></p>
    </section>

    <section id="graphRagPanel" class="panel hidden">
      <h2>Graph RAG (Neo4j)</h2>
      <p class="statusline">Upload a PDF to build a Kafka knowledge graph (entities + edges) in Neo4j, then ask graph-grounded questions.</p>
      <div class="graph-status" id="graphRagStatus">Loading Graph RAG status...</div>
      <div id="graphBusy" class="busy-indicator hidden" role="status" aria-live="polite">
        <span class="busy-spinner" aria-hidden="true"></span>
        <span id="graphBusyText">Processing (00:00)</span>
      </div>
      <div class="launch-row">
        <input id="graphPdfInput" class="file-input" type="file" accept=".pdf,application/pdf" />
        <button id="graphIngestBtn" type="button" onclick="ingestGraphPdf()">Ingest PDF to Neo4j</button>
        <button id="graphCancelBtn" class="secondary hidden" type="button" onclick="cancelGraphIngest()">Cancel Ingest</button>
      </div>
      <div class="chat-input-row">
        <input id="graphQuestionInput" type="text" placeholder="Ask a question from the ingested PDF graph..." />
        <button id="graphAskBtn" type="button" onclick="askGraphRag()">Ask Graph</button>
      </div>
      <div class="chat-log" id="graphRagLog"></div>
    </section>

    <section id="ragMetricsPanel" class="panel hidden">
      <h2>RAG Metrics</h2>
      <p class="statusline">Graph RAG quality and performance telemetry for ingest + query workflows.</p>
      <div class="actions">
        <button type="button" onclick="loadRagMetrics()">Refresh Metrics</button>
        <button type="button" class="secondary" onclick="resetRagMetrics()">Reset Metrics</button>
        <p class="statusline">Updated: <span id="ragMetricsUpdated">n/a</span></p>
      </div>
      <div class="graph-status" id="ragMetricsRuntime">Loading RAG runtime stats...</div>
      <div class="rag-metrics-grid">
        <div class="rag-metric-card">
          <div class="rag-metric-label">Ingest Runs</div>
          <div class="rag-metric-value" id="ragIngestRuns">0</div>
          <div class="rag-metric-sub" id="ragIngestSuccessRate">Success rate: 0.0%</div>
        </div>
        <div class="rag-metric-card">
          <div class="rag-metric-label">Ingest Latency</div>
          <div class="rag-metric-value" id="ragIngestAvgMs">0 ms</div>
          <div class="rag-metric-sub" id="ragIngestAvgPayload">Avg chunks 0 • avg edges 0</div>
        </div>
        <div class="rag-metric-card">
          <div class="rag-metric-label">Query Runs</div>
          <div class="rag-metric-value" id="ragQueryRuns">0</div>
          <div class="rag-metric-sub" id="ragQuerySuccessRate">Success rate: 0.0%</div>
        </div>
        <div class="rag-metric-card">
          <div class="rag-metric-label">Query Latency</div>
          <div class="rag-metric-value" id="ragQueryAvgMs">0 ms</div>
          <div class="rag-metric-sub" id="ragQueryAvgContext">Avg context items 0 • zero-context runs 0</div>
        </div>
        <div class="rag-metric-card">
          <div class="rag-metric-label">Query Tokens</div>
          <div class="rag-metric-value" id="ragQueryAvgTokens">0</div>
          <div class="rag-metric-sub" id="ragQueryTokenTotals">Input 0 • Output 0 • Total 0</div>
        </div>
      </div>
      <div class="graph-status">
        <div>Recent runs</div>
        <table class="rag-recent-table">
          <thead>
            <tr>
              <th>When (UTC)</th>
              <th>Kind</th>
              <th>Status</th>
              <th>Duration</th>
              <th>Tokens</th>
              <th>Context</th>
              <th>Details</th>
            </tr>
          </thead>
          <tbody id="ragRecentRunsBody">
            <tr><td colspan="7">No Graph RAG runs yet.</td></tr>
          </tbody>
        </table>
      </div>
      <p class="statusline error" id="ragMetricsError"></p>
    </section>

    <section id="llmPanel" class="panel hidden">
      <div class="llm-hero">
        <div>
          <p class="eyebrow">Model Router</p>
          <h2>LLM Control</h2>
          <p class="statusline">Assign OpenAI, Ollama, or Hugging Face independently to every runtime.</p>
          <div class="llm-chips">
            <button class="llm-chip" id="llmMonitorChip" type="button" onclick="selectLlmTarget('monitor')">Monitor: --</button>
            <button class="llm-chip" id="llmRemediationChip" type="button" onclick="selectLlmTarget('remediation')">Doer: --</button>
            <button class="llm-chip" id="llmJudgeChip" type="button" onclick="selectLlmTarget('judge')">Judge: --</button>
            <span class="llm-chip pill" id="llmTempChip">Temp: --</span>
          </div>
        </div>
        <button id="llmToggleBtn" class="llm-toggle" type="button" onclick="toggleLlmMenu()">LLM Config ▼</button>
      </div>
      <div id="llmMenu" class="llm-menu">
        <div class="llm-grid">
          <div class="llm-field">
            <label for="llmTargetSelect">Runtime assignment</label>
            <select id="llmTargetSelect" onchange="selectLlmTarget(this.value)">
              <option id="llmTargetMonitor" value="monitor">Monitor Agent</option>
              <option id="llmTargetRemediation" value="remediation">Doer / Remediation Agent</option>
              <option id="llmTargetJudge" value="judge">DeepEval LLM Judge</option>
            </select>
          </div>
          <div class="llm-field">
            <label for="llmProviderSelect">Provider</label>
            <select id="llmProviderSelect" onchange="updateLlmProviderDefaults(this.value)">
              <option value="openai">OpenAI</option>
              <option value="ollama">Ollama (on-cluster)</option>
              <option value="huggingface">Hugging Face (OpenAI-compatible)</option>
            </select>
          </div>
          <div class="llm-field">
            <label for="llmModelInput">Primary model</label>
            <input id="llmModelInput" type="text" placeholder="gpt-4.1 / llama3.1:8b / meta-llama/..." list="llmModelOptions" />
            <datalist id="llmModelOptions"></datalist>
          </div>
          <div class="llm-field">
            <label for="llmModelSelect">Available models</label>
            <select id="llmModelSelect" onchange="selectLlmModelFromDropdown(this.value)">
              <option value="">Select downloaded model...</option>
            </select>
          </div>
          <div class="llm-field">
            <label for="llmFallbacksInput">Fallback models (comma)</label>
            <input id="llmFallbacksInput" type="text" placeholder="gpt-3.5-turbo, llama3.2:3b" />
          </div>
          <div class="llm-field">
            <label>&nbsp;</label>
            <button type="button" class="secondary" onclick="loadLlmModels()">Load provider models</button>
          </div>
          <div class="llm-field">
            <label for="llmBaseUrlInput">Base URL</label>
            <input id="llmBaseUrlInput" type="text" placeholder="http://ollama:11434/v1" />
          </div>
          <div class="llm-field">
            <label for="llmApiKeyInput">API key (optional)</label>
            <input id="llmApiKeyInput" type="password" autocomplete="off" placeholder="sk-..." />
          </div>
          <div class="llm-field">
            <label for="llmTemperatureInput">Temperature</label>
            <div class="llm-slider-row">
              <input id="llmTemperatureInput" type="range" min="0" max="1" step="0.05" value="0" oninput="updateLlmTempValue(this.value)" />
              <span class="pill" id="llmTemperatureValue">0.00</span>
            </div>
          </div>
        </div>
        <div class="actions">
          <button id="llmSaveBtn" type="button" onclick="saveLlmConfig()">Apply + Reload</button>
          <button type="button" class="secondary" onclick="loadLlmConfig()">Reset</button>
          <div id="llmBusy" class="busy-indicator hidden" role="status" aria-live="polite">
            <span class="busy-spinner" aria-hidden="true"></span>
            <span id="llmBusyText">Processing (00:00)</span>
          </div>
          <p class="statusline" id="llmStatusLine"></p>
        </div>
      </div>
    </section>

    <section id="deepevalPanel" class="panel hidden">
      <h2>DeepEval LLM-as-a-Judge</h2>
      <p class="statusline">Local DeepEval watches both agents for relevancy, Kafka operational quality, bias, and toxicity.</p>
      <div class="llm-grid">
        <div class="llm-field">
          <label for="deepevalScope">Evaluation scope</label>
          <select id="deepevalScope" onchange="renderDeepEvalRuns()">
            <option value="both">Monitor + Doer</option>
            <option value="monitor">Monitor Agent</option>
            <option value="remediation">Doer Agent</option>
          </select>
        </div>
        <div class="llm-field">
          <label for="deepevalThreshold">Pass threshold (0–1)</label>
          <input id="deepevalThreshold" type="number" min="0" max="1" step="0.05" value="0.70" />
        </div>
      </div>
      <div class="actions">
        <button id="deepevalRunBtn" type="button" onclick="runDeepEval()">Evaluate Latest Outputs</button>
        <button type="button" class="secondary" onclick="loadDeepEvalStatus()">Refresh Results</button>
        <p class="statusline" id="deepevalStatus">Loading local judge...</p>
      </div>
      <div class="rag-metrics-grid">
        <div class="rag-metric-card">
          <div class="rag-metric-label">Evaluated Cases</div>
          <div class="rag-metric-value" id="deepevalCaseCount">0</div>
          <div class="rag-metric-sub" id="deepevalRunCount">Across 0 runs</div>
        </div>
        <div class="rag-metric-card">
          <div class="rag-metric-label">Metric Pass Rate</div>
          <div class="rag-metric-value" id="deepevalPassRate">0.0%</div>
          <div class="rag-metric-sub" id="deepevalMetricCount">0 metric results</div>
        </div>
        <div class="rag-metric-card">
          <div class="rag-metric-label">Normalized Quality</div>
          <div class="rag-metric-value" id="deepevalQualityScore">0.000</div>
          <div class="rag-metric-sub">Higher is better across mixed metric directions</div>
        </div>
        <div class="rag-metric-card">
          <div class="rag-metric-label">Average Evaluation Time</div>
          <div class="rag-metric-value" id="deepevalAvgDuration">0 ms</div>
          <div class="rag-metric-sub">Judge latency per evaluation run</div>
        </div>
        <div class="rag-metric-card">
          <div class="rag-metric-label">Latest Answer Relevancy</div>
          <div class="rag-metric-value" id="deepevalAnswerRelevancy">n/a</div>
          <div class="rag-metric-sub">Higher is better</div>
        </div>
        <div class="rag-metric-card">
          <div class="rag-metric-label">Latest Operational Quality</div>
          <div class="rag-metric-value" id="deepevalOperationalQuality">n/a</div>
          <div class="rag-metric-sub">Higher is better</div>
        </div>
        <div class="rag-metric-card">
          <div class="rag-metric-label">Latest Bias</div>
          <div class="rag-metric-value" id="deepevalBias">n/a</div>
          <div class="rag-metric-sub">Lower is better</div>
        </div>
        <div class="rag-metric-card">
          <div class="rag-metric-label">Latest Toxicity</div>
          <div class="rag-metric-value" id="deepevalToxicity">n/a</div>
          <div class="rag-metric-sub">Lower is better</div>
        </div>
      </div>
      <h3>Original DeepEval Quality Output</h3>
      <p class="statusline">The original Answer Relevancy and Kafka Operational Quality scores and judge reasons are preserved here.</p>
      <div class="rag-table-wrap">
        <table class="rag-table">
          <thead>
            <tr><th>Time</th><th>Agent</th><th>Metric</th><th>Score</th><th>Pass</th><th>Judge reason</th></tr>
          </thead>
          <tbody id="deepevalOriginalResultsBody"><tr><td colspan="6">No original quality evaluations yet.</td></tr></tbody>
        </table>
      </div>
      <h3>Additional DeepEval Safety Output</h3>
      <p class="statusline">Bias and Toxicity retain their raw lower-is-better scores and independent safety threshold.</p>
      <div class="rag-table-wrap">
        <table class="rag-table">
          <thead>
            <tr><th>Time</th><th>Agent</th><th>Metric</th><th>Raw score</th><th>Direction</th><th>Pass</th><th>Judge reason</th></tr>
          </thead>
          <tbody id="deepevalSafetyResultsBody"><tr><td colspan="7">No safety evaluations yet.</td></tr></tbody>
        </table>
      </div>
      <p class="statusline error" id="deepevalError"></p>
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

    <section id="fineTuningPanel" class="panel hidden">
      <h2>Fine Tuning (MLX on Mac)</h2>
      <p class="statusline">Prepare an MLX-LM fine-tuning run on Apple Silicon. Training executes on the Mac host, not inside the Linux Docker agents.</p>
      <div id="fineTuningStatus" class="finetune-status" role="status" aria-live="polite">Checking MLX runtime...</div>
      <div class="finetune-card">
        <h3>Training configuration</h3>
        <div class="finetune-grid">
          <label>Base model<select id="fineTuneModel" onchange="updateFineTuningEstimate()"><option value="mlx-community/Llama-3.2-3B-Instruct-4bit">Llama 3.2 3B Instruct (4-bit)</option><option value="mlx-community/Qwen2.5-3B-Instruct-4bit">Qwen 2.5 3B Instruct (4-bit)</option></select></label>
          <label>Training data directory<input id="fineTuneData" type="text" value="__MLX_TRAINING_DATA_DIR__" placeholder="/Users/name/training-data or output/fine-tuning" /></label>
          <label>Output adapter path<select id="fineTuneOutput"><option value="models/kafka-monitor-lora">models/kafka-monitor-lora</option><option value="models/kafka-doer-lora">models/kafka-doer-lora</option></select></label>
          <label>Iterations<input id="fineTuneIterations" type="number" min="1" step="1" value="600" oninput="updateFineTuningEstimate()" /></label>
          <label>LoRA rank estimate<input id="fineTuneRank" type="number" min="1" step="1" value="8" oninput="updateFineTuningEstimate()" /></label>
        </div>
        <div id="fineTuningEstimate" class="graph-status">Estimated runtime: calculating...</div>
        <div class="launch-row">
          <button type="button" onclick="refreshFineTuningStatus()">Refresh MLX Status</button>
          <button type="button" onclick="validateFineTuningDataset()">Validate Dataset Directory</button>
          <button type="button" onclick="startFineTuning()">Prepare MLX Fine-Tuning Run</button>
          <button type="button" onclick="submitFineTuningJob()">Submit Fine-Tuning Job</button>
          <span id="fineTuningElapsed" class="graph-status">Elapsed: 0.0s</span>
          <span id="fineTuningProgress" class="graph-status">Progress: 0 / 0 iterations</span>
        </div>
        <div class="finetune-live"><span id="fineTuningLiveIcon" class="finetune-live-icon">○</span><span id="fineTuningLiveLabel">Idle</span><span class="finetune-time-track"><span id="fineTuningTimeFill" class="finetune-time-fill"></span></span></div>
        <pre id="fineTuneCommand" class="hitl-block" hidden></pre>
        <button id="fineTuneCopyCommand" type="button" onclick="copyFineTuningCommand()" hidden>Copy Mac MLX Command</button>
        <p id="fineTuningError" class="statusline error"></p>
      </div>
      <div class="finetune-card">
        <h3>Expected MLX-LM dataset format</h3>
        <p class="statusline">Each dataset directory must contain all three files:</p>
        <pre class="hitl-block">data/finetuning/kafka-traces/
├── train.jsonl
├── valid.jsonl
└── test.jsonl</pre>
        <pre class="hitl-block">{"prompt":"Diagnose under-replicated partitions","completion":"Check ISR and broker health before proposing a repair."}</pre>
        <p class="statusline">Before running fine tuning on the Mac host, install the MLX runtime:</p>
        <pre class="hitl-block">python -m pip install mlx mlx-lm</pre>
        <p class="statusline">The Docker UI validates configuration and emits the host command; it does not install MLX inside the container. The installed MLX-LM CLI uses its configured/default LoRA rank; the rank field is used only for duration planning.</p>
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
    const agentProcessRole = '__AGENT_PROCESS_ROLE__';
    const isDoerRuntime = agentProcessRole === 'remediation';
    const llmProviderDefaults = __LLM_PROVIDER_DEFAULTS_JSON__;

    function configureRoleUi() {
      if (!isDoerRuntime) return;
      const directChatRow = document.getElementById('directChatRow');
      if (directChatRow) directChatRow.classList.add('hidden');
      const hitlPanel = document.getElementById('hitlPanel');
      if (hitlPanel) hitlPanel.classList.remove('hidden');
      const policy = document.getElementById('agentPolicyText');
      if (policy) {
        policy.textContent = 'Repairs are accepted only as authorized A2A tasks from the Monitor Agent.';
      }
      const target = document.getElementById('llmTargetSelect');
      if (target) {
        target.value = 'remediation';
        target.disabled = true;
      }
      for (const id of ['llmTargetMonitor', 'llmTargetJudge', 'llmMonitorChip', 'llmJudgeChip']) {
        const element = document.getElementById(id);
        if (element) element.classList.add('hidden');
      }
    }

    function hitlStateLabel(taskState) {
      const labels = {
        TASK_STATE_INPUT_REQUIRED: 'AWAITING APPROVAL',
        TASK_STATE_WORKING: 'WORKING',
        TASK_STATE_COMPLETED: 'COMPLETED',
        TASK_STATE_REJECTED: 'REJECTED',
        TASK_STATE_FAILED: 'FAILED'
      };
      return labels[taskState] || String(taskState || 'UNKNOWN').replace('TASK_STATE_', '');
    }

    function hitlStateClass(taskState) {
      return String(taskState || '').replace('TASK_STATE_', '').toLowerCase();
    }

    function renderHitlTasks(tasks) {
      const container = document.getElementById('hitlTasks');
      if (!container) return;
      if (!Array.isArray(tasks) || tasks.length === 0) {
        container.innerHTML = '<p class="hitl-empty">No A2A remediation tasks have arrived yet.</p>';
        return;
      }
      container.innerHTML = tasks.map(task => {
        const taskId = String(task.task_id || '');
        const stateValue = String(task.task_state || '');
        const waiting = stateValue === 'TASK_STATE_INPUT_REQUIRED';
        const review = task.human_review || {};
        const reviewPrompt =
          '<section class="hitl-review-prompt" aria-label="Human approval decision">' +
            '<h4>' + esc(review.title || 'Review this Kafka remediation') + '</h4>' +
            '<div class="hitl-block-label">What\\'s wrong</div>' +
            '<div class="hitl-what-wrong">' + esc(review.diagnosis || review.issue || task.a2a_message || 'No diagnosis supplied.') + '</div>' +
            '<div class="hitl-block-label">Suggested ways to fix it</div>' +
            '<div class="hitl-fix-options">' + renderHitlFixOptions(task, waiting) + '</div>' +
            '<div class="hitl-review-grid">' +
              '<div class="hitl-review-item"><strong>Issue selected by Monitor</strong>' + esc(review.issue || task.a2a_message || '') + '</div>' +
              '<div class="hitl-review-item"><strong>What approval authorizes</strong>' + esc(review.approval_effect || 'Run the proposed Doer plan.') + '</div>' +
              '<div class="hitl-review-item"><strong>Safety boundary</strong>' + esc(review.safety_boundary || 'Only the selected issue is in scope.') + '</div>' +
              '<div class="hitl-review-item"><strong>If you reject</strong>' + esc(review.rejection_effect || 'The task ends without execution.') + '</div>' +
            '</div>' +
          '</section>';
        const fullContext = JSON.stringify(task.full_context || {}, null, 2);
        const decision = task.decision
          ? '<p class="statusline">Human decision: ' + esc(task.decision.toUpperCase()) +
              (task.decision_note ? ' — ' + esc(task.decision_note) : '') + '</p>'
          : '';
        const outcome = task.result
          ? '<div class="hitl-block-label">Execution Result</div><pre class="hitl-block">' + esc(task.result) + '</pre>'
          : (task.error
              ? '<div class="hitl-block-label">Task Outcome</div><pre class="hitl-block">' + esc(task.error) + '</pre>'
              : '');
        const controls = waiting
          ? '<div class="hitl-decision-row">' +
              '<input class="hitl-note" id="hitl-note-' + escAttr(taskId) + '" type="text" ' +
                'placeholder="Optional constraints or decision note" />' +
              '<button type="button" onclick="decideHitlTask(\\'' + escAttr(taskId) + '\\', \\'approve\\')">Approve Plan &amp; Run Doer</button>' +
              '<button type="button" class="hitl-reject" onclick="decideHitlTask(\\'' + escAttr(taskId) + '\\', \\'reject\\')">Reject Without Running</button>' +
            '</div>'
          : '';
        return '<article class="hitl-task">' +
          '<div class="hitl-task-head">' +
            '<span>Task ' + esc(taskId.slice(0, 12)) + ' • ' + esc(task.updated_at_utc || task.created_at_utc || '') + '</span>' +
            '<span class="hitl-badge ' + escAttr(hitlStateClass(stateValue)) + '">' + esc(hitlStateLabel(stateValue)) + '</span>' +
          '</div>' +
          reviewPrompt +
          '<div class="hitl-block-label">A2A Message From Monitor</div>' +
          '<pre class="hitl-block">' + esc(task.a2a_message || '') + '</pre>' +
          '<div class="hitl-block-label">Doer Plan To Fix The Issue</div>' +
          '<pre class="hitl-block">' + esc(task.plan || 'No plan was generated.') + '</pre>' +
          '<details class="hitl-context-details">' +
            '<summary>View entire context sent to the Doer</summary>' +
            '<pre class="hitl-block">' + esc(fullContext) + '</pre>' +
          '</details>' +
          decision + outcome + controls +
        '</article>';
      }).join('');
    }

    function renderHitlFixOptions(task, waiting) {
      const lines = String(task.plan || '').split(/\\n+/).map(line => line.trim()).filter(Boolean);
      if (!lines.length) return '<p class="hitl-empty">The Doer did not generate suggested fixes.</p>';
      // Legacy inline handler retained for reference; replaced by JSON-safe action construction.
      /* return lines.map((line, index) => waiting
        ? '<button type="button" class="hitl-fix-option" onclick="decideHitlTask(\'' + escAttr(String(task.task_id || '')) + '\', \'approve\', \'Suggested fix ' + escAttr(String(index + 1)) + ': ' + escAttr(line) + '\')">' +
            '<span class="hitl-fix-number">' + String(index + 1) + '.</span><span>' + esc(line.replace(/^\\d+[.)]\\s* /, '')) + '</span></button>'
        : '<div class="hitl-fix-option"><span class="hitl-fix-number">' + String(index + 1) + '.</span><span>' + esc(line.replace(/^\\d+[.)]\\s* /, '')) + '</span></div>'
      ).join(''); */
      return lines.map((line, index) => {
        const cleanLine = line.replace(/^\\d+[.)]\\s*/, '');
        const label = 'Suggested fix ' + String(index + 1) + ': ' + cleanLine;
        if (!waiting) {
          return '<div class="hitl-fix-option"><span class="hitl-fix-number">' + String(index + 1) + '.</span><span>' + esc(cleanLine) + '</span></div>';
        }
        const action = 'decideHitlTask(' + JSON.stringify(String(task.task_id || '')) + ', ' +
          JSON.stringify('approve') + ', ' + JSON.stringify(label) + ')';
        return '<button type="button" class="hitl-fix-option" onclick="' + escAttr(action) + '">' +
          '<span class="hitl-fix-number">' + String(index + 1) + '.</span><span>' + esc(cleanLine) + '</span></button>';
      }).join('');
    }

    async function loadHitlState() {
      if (!isDoerRuntime) return;
      const status = document.getElementById('hitlStatus');
      try {
        const response = await fetch('/api/remediation/hitl');
        const data = await response.json();
        if (!data.ok) throw new Error(data.error || 'Could not load HITL state');
        const toggle = document.getElementById('hitlToggle');
        const label = document.getElementById('hitlModeLabel');
        if (toggle) toggle.checked = Boolean(data.enabled);
        if (label) label.textContent = data.enabled ? 'Human approval ON' : 'Automatic execution ON';
        if (status) {
          status.textContent = data.enabled
            ? String(data.pending_count || 0) + ' task(s) waiting. New A2A repairs pause for approval.'
            : 'New A2A repairs execute automatically. Existing pending tasks still require a decision.';
        }
        renderHitlTasks(data.tasks || []);
      } catch (error) {
        if (status) status.textContent = 'Approval screen unavailable: ' + String(error);
      }
    }

    async function refreshFineTuningStatus() {
      const status = document.getElementById('fineTuningStatus');
      if (!status) return;
      try {
        const response = await fetch('/api/finetuning/status');
        const data = await response.json();
        status.classList.toggle('available', Boolean(data.mlx_available && data.mlx_lm_available));
        status.classList.toggle('unavailable', !(data.mlx_available && data.mlx_lm_available));
        status.textContent = data.mlx_available && data.mlx_lm_available
          ? `MLX AVAILABLE — ${data.platform}. Host training is ready.`
          : data.platform.toLowerCase().includes('linux')
            ? `HOST MLX REQUIRED — UI runtime is ${data.platform}. Run MLX on the Mac host.`
            : `MLX UNAVAILABLE — ${data.platform}. Install mlx and mlx-lm in the Mac host Python environment.`;
      } catch (error) {
        status.classList.remove('available');
        status.classList.add('unavailable');
        status.textContent = 'MLX status unavailable: ' + String(error);
      }
    }

    async function validateFineTuningDataset() {
      const directory = document.getElementById('fineTuneData').value.trim();
      const status = document.getElementById('fineTuningStatus');
      try {
        const response = await fetch('/api/finetuning/validate', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({data: directory})});
        const data = await response.json();
        if (!response.ok || !data.ok) throw new Error(data.error || response.statusText);
        if (!data.valid && !data.host_required) throw new Error('Missing: ' + data.missing_files.join(', '));
        status.textContent = data.host_required
          ? 'Dataset directory accepted for Mac host: ' + directory + ' (Docker cannot inspect host files). Required: train.jsonl, valid.jsonl, test.jsonl.'
          : 'Dataset directory is valid: ' + directory;
      } catch (error) {
        status.textContent = 'Dataset validation failed: ' + String(error);
      }
    }

async function startFineTuning() {
      const error = document.getElementById('fineTuningError');
      if (error) error.textContent = '';
      const elapsed = document.getElementById('fineTuningElapsed');
      const startedAt = performance.now();
      if (window.fineTuneElapsedTimer) clearInterval(window.fineTuneElapsedTimer);
      const updateElapsed = () => {
        if (elapsed) elapsed.textContent = 'Elapsed: ' + ((performance.now() - startedAt) / 1000).toFixed(1) + 's';
      };
      updateElapsed();
      window.fineTuneElapsedTimer = setInterval(updateElapsed, 100);
  try {
    const response = await fetch('/api/finetuning/prepare', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            model: document.getElementById('fineTuneModel').value,
        data: document.getElementById('fineTuneData').value.trim(),
            output: document.getElementById('fineTuneOutput').value,
            iterations: Number(document.getElementById('fineTuneIterations').value || 600),
            rank: Number(document.getElementById('fineTuneRank').value || 8)
          })
        });
        const responseText = await response.text();
        let data;
        try { data = JSON.parse(responseText); } catch (_) { data = {ok: false, error: responseText || 'Empty response from Fine Tuning API'}; }
        if (!response.ok || !data.ok) throw new Error(data.error || response.statusText);
        document.getElementById('fineTuningStatus').textContent = 'Prepared host MLX command: ' + data.command;
        if (data.estimate) renderFineTuningEstimate(data.estimate);
      } catch (err) {
        if (error) error.textContent = String(err);
        const status = document.getElementById('fineTuningStatus');
        if (status) status.textContent = 'Fine Tuning request failed: ' + String(err);
      } finally {
        clearInterval(window.fineTuneElapsedTimer);
        window.fineTuneElapsedTimer = null;
        updateElapsed();
      }
    }

    function renderFineTuningEstimate(estimate) {
      const target = document.getElementById('fineTuningEstimate');
      if (target && estimate) target.textContent = 'Estimated runtime: ' + estimate.label + ' — ' + estimate.basis;
    }

    function updateFineTuningEstimate() {
      const model = document.getElementById('fineTuneModel')?.value || '';
      const iterations = Number(document.getElementById('fineTuneIterations')?.value || 600);
      const rank = Number(document.getElementById('fineTuneRank')?.value || 8);
      const modelFactor = /(?:7b|8b|13b)/i.test(model) ? 2.2 : 1.0;
      const minutes = (Math.max(1, iterations) * 0.45 * modelFactor * Math.max(1, rank) / 8) / 60;
      const target = document.getElementById('fineTuningEstimate');
      if (target) target.textContent = 'Estimated runtime: about ' + minutes.toFixed(1) + ' minutes (planning estimate; actual time varies by Mac, sequence length, and dataset).';
    }

async function submitFineTuningJob() {
      const status = document.getElementById('fineTuningStatus');
      const liveIcon = document.getElementById('fineTuningLiveIcon');
      const liveLabel = document.getElementById('fineTuningLiveLabel');
      try {
        const response = await fetch('/api/finetuning/run', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({
          model: document.getElementById('fineTuneModel').value,
          data: document.getElementById('fineTuneData').value,
          output: document.getElementById('fineTuneOutput').value,
          iterations: Number(document.getElementById('fineTuneIterations').value || 600),
          rank: Number(document.getElementById('fineTuneRank').value || 8)
        })});
        const data = await response.json();
        if (!response.ok || !data.ok) throw new Error(data.error || response.statusText);
        if (data.status === 'host_required') {
          status.textContent = data.message + ' Run the command below from the Mac project directory.';
          const command = document.getElementById('fineTuneCommand');
          const copy = document.getElementById('fineTuneCopyCommand');
          if (command) { command.hidden = false; command.textContent = data.command; }
          if (copy) copy.hidden = false;
          const liveIcon = document.getElementById('fineTuningLiveIcon');
          const liveLabel = document.getElementById('fineTuningLiveLabel');
          if (liveIcon) { liveIcon.textContent = '●'; liveIcon.classList.add('running'); }
          if (liveLabel) liveLabel.textContent = 'Submitted — waiting for Mac host';
          return;
        }
        status.textContent = 'Fine-tuning job submitted: ' + data.job_id;
        window.fineTuneJobStartedAt = Date.now();
        liveIcon.textContent = '●'; liveIcon.classList.add('running'); liveLabel.textContent = 'Processing';
        pollFineTuningJob(data.job_id);
      } catch (error) {
        status.textContent = 'Fine-tuning submission failed: ' + String(error);
      }
    }

    async function copyFineTuningCommand() {
      const command = document.getElementById('fineTuneCommand')?.textContent || '';
      if (!command) return;
      await navigator.clipboard.writeText(command);
      document.getElementById('fineTuningStatus').textContent = 'MLX command copied. Run it on the Mac host to start training.';
    }

    async function pollFineTuningJob(jobId) {
      const progress = document.getElementById('fineTuningProgress');
      const status = document.getElementById('fineTuningStatus');
      const poll = async () => {
        try {
          const response = await fetch('/api/finetuning/jobs/' + encodeURIComponent(jobId));
          const data = await response.json();
          if (!response.ok || !data.ok) throw new Error(data.error || response.statusText);
          if (progress) progress.textContent = 'Progress: ' + (data.progress || 0) + ' / ' + (data.total || 0) + ' iterations';
          const estimateSeconds = Math.max(1, Number(document.getElementById('fineTuneIterations')?.value || 600) * 0.45);
          const elapsedSeconds = Math.max(0, (Date.now() - (window.fineTuneJobStartedAt || Date.now())) / 1000);
          const fill = document.getElementById('fineTuningTimeFill');
          if (fill) fill.style.width = Math.min(100, (elapsedSeconds / estimateSeconds) * 100).toFixed(1) + '%';
          if (data.status === 'running') {
            setTimeout(poll, 1000);
          } else if (status) {
            status.textContent = 'Fine-tuning job ' + data.status + ': ' + jobId;
            const liveIcon = document.getElementById('fineTuningLiveIcon');
            const liveLabel = document.getElementById('fineTuningLiveLabel');
            if (liveIcon) { liveIcon.textContent = data.status === 'completed' ? '✓' : '!'; liveIcon.classList.remove('running'); }
            if (liveLabel) liveLabel.textContent = data.status === 'completed' ? 'Completed' : 'Failed';
            if (fill && data.status === 'completed') fill.style.width = '100%';
          }
        } catch (error) {
          if (status) status.textContent = 'Fine-tuning progress unavailable: ' + String(error);
        }
      };
      poll();
    }

    async function toggleHitlMode() {
      const toggle = document.getElementById('hitlToggle');
      const requested = Boolean(toggle && toggle.checked);
      if (toggle) toggle.disabled = true;
      try {
        const response = await fetch('/api/remediation/hitl/config', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ enabled: requested })
        });
        const data = await response.json();
        if (!data.ok) throw new Error(data.error || 'Could not update HITL mode');
      } catch (error) {
        document.getElementById('statusError').textContent = String(error);
      } finally {
        if (toggle) toggle.disabled = false;
        await loadHitlState();
      }
    }

    async function decideHitlTask(taskId, decision, suggestedFix = '') {
      const noteInput = document.getElementById('hitl-note-' + taskId);
      const operatorNote = noteInput ? noteInput.value.trim() : '';
      const note = [suggestedFix, operatorNote].filter(Boolean).join(' | ');
      document.getElementById('statusError').textContent = '';
      startExpertBusy(
        decision === 'approve' ? 'Starting approved remediation...' : 'Rejecting remediation task...',
        'processing'
      );
      try {
        const response = await fetch('/api/remediation/hitl/tasks/' + encodeURIComponent(taskId) + '/decision', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ decision, note })
        });
        const data = await response.json();
        if (!data.ok) throw new Error(data.error || 'Could not record human decision');
        await Promise.all([loadHitlState(), loadState()]);
      } catch (error) {
        document.getElementById('statusError').textContent = String(error);
      } finally {
        stopExpertBusy();
      }
    }

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
      startGraphBusy('Loading Graph RAG status...', 'processing');
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
      } finally {
        stopGraphBusy();
      }
    }

    let activeGraphIngestJobId = '';

    function updateGraphBusy(message, kind) {
      const state = busyState.graph;
      if (!state) return;
      state.message = message || state.message;
      if (kind) state.kind = kind === 'ai' ? 'ai' : 'processing';
      renderBusy('graph');
    }

    async function cancelGraphIngest() {
      if (!activeGraphIngestJobId) return;
      const cancelBtn = document.getElementById('graphCancelBtn');
      if (cancelBtn) cancelBtn.disabled = true;
      try {
        const response = await fetch(
          '/api/graphrag/ingest_pdf/cancel/' + encodeURIComponent(activeGraphIngestJobId),
          {method: 'POST', headers: {'Content-Type': 'application/json'}}
        );
        const data = await response.json();
        if (!response.ok || !data.ok) throw new Error(data.error || 'Cancel request failed');
        updateGraphBusy(data.message || 'Cancellation requested...', 'processing');
        appendGraphLog('agent', '1. [WARN] Cancellation requested. The current bounded AI call will stop at its timeout; no additional chunks will run.');
      } catch (error) {
        appendGraphLog('agent', '1. [BAD] Could not cancel graph ingest. Fix: ' + String(error));
        if (cancelBtn) cancelBtn.disabled = false;
      }
    }

    async function ingestGraphPdf() {
      const input = document.getElementById('graphPdfInput');
      const btn = document.getElementById('graphIngestBtn');
      const cancelBtn = document.getElementById('graphCancelBtn');
      if (!input || !input.files || input.files.length === 0) {
        appendGraphLog('agent', '1. [WARN] Choose a PDF file first. Fix: select a Kafka PDF and click ingest again.');
        return;
      }
      const file = input.files[0];
      const formData = new FormData();
      formData.append('pdf', file, file.name);
      btn.disabled = true;
      startGraphBusy('Uploading and queuing PDF...', 'processing');
      appendGraphLog('user', 'Ingest PDF: ' + file.name);
      try {
        const r = await fetch('/api/graphrag/ingest_pdf', { method: 'POST', body: formData });
        const data = await r.json();
        if (!data.ok) {
          appendGraphLog('agent', '1. [BAD] PDF ingest failed. Fix: ' + (data.error || 'check Neo4j and LLM settings and retry.'));
        } else if (data.pending) {
          activeGraphIngestJobId = data.job_id;
          if (cancelBtn) {
            cancelBtn.disabled = false;
            cancelBtn.classList.remove('hidden');
          }
          appendGraphLog('agent', '1. [GOOD] PDF ingest queued as job ' + data.job_id + '. Extracting ontology edges in the background...');
          let finished = false;
          while (!finished) {
            await new Promise(resolve => setTimeout(resolve, 2000));
            const statusResponse = await fetch('/api/graphrag/ingest_pdf/status/' + encodeURIComponent(data.job_id));
            const job = await statusResponse.json();
            if (!job.ok) throw new Error(job.error || 'Unable to read ingest status');
            const done = Number(job.chunks_processed || 0);
            const total = Number(job.total_chunks || 0);
            const current = Number(job.current_chunk || Math.min(total, done + 1));
            const pct = Number(job.progress || 0);
            const elapsed = formatElapsedMs(Number(job.elapsed_ms || 0));
            const eta = job.estimated_remaining_ms == null
              ? 'calculating ETA'
              : 'ETA ' + formatElapsedMs(Number(job.estimated_remaining_ms || 0));
            const mode = job.extraction_mode === 'deterministic_fallback'
              ? 'fast deterministic fallback'
              : 'bounded AI extraction';
            const progressText = job.cancel_requested
              ? `Canceling after current chunk ${current}/${total} • elapsed ${elapsed}`
              : `Chunk ${current}/${total} • ${pct}% • ${mode} • elapsed ${elapsed} • ${eta}`;
            updateGraphBusy(progressText, job.extraction_mode === 'llm' ? 'ai' : 'processing');
            document.getElementById('graphRagStatus').textContent =
              'PDF ingest job ' + data.job_id.slice(0, 10) + ': ' + progressText;
            if (job.status === 'completed') { appendGraphLog('agent', job.answer); finished = true; }
            if (job.status === 'failed') { appendGraphLog('agent', '1. [BAD] PDF ingest failed. Fix: ' + job.error); finished = true; }
            if (job.status === 'canceled') { appendGraphLog('agent', '1. [WARN] PDF ingest canceled without processing additional chunks.'); finished = true; }
          }
        } else {
          appendGraphLog('agent', data.answer || '1. [GOOD] PDF ingest completed.');
        }
      } catch (e) {
        appendGraphLog('agent', '1. [BAD] Ingest request failed. Fix: ' + String(e));
      } finally {
        activeGraphIngestJobId = '';
        if (cancelBtn) {
          cancelBtn.disabled = false;
          cancelBtn.classList.add('hidden');
        }
        btn.disabled = false;
        await loadGraphRagStatus();
        await loadRagMetrics();
        const elapsedMs = stopGraphBusy();
        if (elapsedMs > 0) {
          appendGraphLog('agent', `1. [GOOD] Ingest processing time: ${formatElapsedMs(elapsedMs)}.`);
        }
      }
    }

    async function askGraphRag() {
      const input = document.getElementById('graphQuestionInput');
      const btn = document.getElementById('graphAskBtn');
      const question = (input.value || '').trim();
      if (!question) return;
      btn.disabled = true;
      startGraphBusy('Querying Graph RAG...', 'ai');
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
          applyTraceUsage(data.trace || {});
        }
        input.value = '';
      } catch (e) {
        appendGraphLog('agent', '1. [BAD] Graph query request failed. Fix: ' + String(e));
      } finally {
        btn.disabled = false;
        await loadRagMetrics();
        const elapsedMs = stopGraphBusy();
        if (elapsedMs > 0) {
          appendGraphLog('agent', `1. [GOOD] Graph query processing time: ${formatElapsedMs(elapsedMs)}.`);
        }
      }
    }

    function ragMetricInt(value) {
      const n = Number.parseInt(String(value ?? 0), 10);
      return Number.isFinite(n) ? Math.max(0, n) : 0;
    }

    function ragFmtInt(value) {
      return ragMetricInt(value).toLocaleString('en-US');
    }

    function ragPct(numer, denom) {
      const d = ragMetricInt(denom);
      if (d <= 0) return '0.0%';
      return ((ragMetricInt(numer) / d) * 100).toFixed(1) + '%';
    }

    function setRagText(id, text) {
      const el = document.getElementById(id);
      if (el) el.textContent = text;
    }

    function renderRagRecentRuns(rows) {
      const body = document.getElementById('ragRecentRunsBody');
      if (!body) return;
      const entries = Array.isArray(rows) ? rows : [];
      if (entries.length === 0) {
        body.innerHTML = '<tr><td colspan="7">No Graph RAG runs yet.</td></tr>';
        return;
      }
      let html = '';
      for (const row of entries.slice().reverse()) {
        const ok = !!row.ok;
        const badgeClass = ok ? 'ok' : 'fail';
        const badgeText = ok ? 'PASS' : 'FAIL';
        const kind = String(row.kind || 'run').toUpperCase();
        const when = esc(row.at_utc || '');
        const duration = ragFmtInt(row.duration_ms) + ' ms';
        const tokens = ragFmtInt(row.tokens);
        const contextItems = ragFmtInt(row.context_items);
        const note = esc((row.note || row.error || '').toString().slice(0, 140));
        html += '<tr>' +
          '<td>' + when + '</td>' +
          '<td>' + esc(kind) + '</td>' +
          '<td><span class="rag-status-badge ' + badgeClass + '">' + badgeText + '</span></td>' +
          '<td>' + duration + '</td>' +
          '<td>' + tokens + '</td>' +
          '<td>' + contextItems + '</td>' +
          '<td>' + note + '</td>' +
        '</tr>';
      }
      body.innerHTML = html;
    }

    function renderRagMetrics(data) {
      const metrics = (data && data.metrics && typeof data.metrics === 'object') ? data.metrics : {};
      const ingest = (metrics.ingest && typeof metrics.ingest === 'object') ? metrics.ingest : {};
      const query = (metrics.query && typeof metrics.query === 'object') ? metrics.query : {};
      const ingestRuns = ragMetricInt(ingest.runs);
      const ingestSuccess = ragMetricInt(ingest.success);
      const queryRuns = ragMetricInt(query.runs);
      const querySuccess = ragMetricInt(query.success);
      const ingestAvgMs = ingestRuns > 0 ? Math.round(ragMetricInt(ingest.total_duration_ms) / ingestRuns) : 0;
      const queryAvgMs = queryRuns > 0 ? Math.round(ragMetricInt(query.total_duration_ms) / queryRuns) : 0;
      const ingestAvgChunks = ingestSuccess > 0 ? (ragMetricInt(ingest.total_chunks) / ingestSuccess).toFixed(1) : '0.0';
      const ingestAvgEdges = ingestSuccess > 0 ? (ragMetricInt(ingest.total_edges) / ingestSuccess).toFixed(1) : '0.0';
      const queryAvgTokens = queryRuns > 0 ? Math.round(ragMetricInt(query.total_tokens) / queryRuns) : 0;
      const queryAvgContext = queryRuns > 0 ? (ragMetricInt(query.total_context_items) / queryRuns).toFixed(1) : '0.0';
      const zeroContextRuns = ragMetricInt(query.zero_context_runs);

      setRagText('ragIngestRuns', ragFmtInt(ingestRuns));
      setRagText('ragIngestSuccessRate', `Success rate: ${ragPct(ingestSuccess, ingestRuns)}`);
      setRagText('ragIngestAvgMs', `${ragFmtInt(ingestAvgMs)} ms`);
      setRagText('ragIngestAvgPayload', `Avg chunks ${ingestAvgChunks} • avg edges ${ingestAvgEdges}`);

      setRagText('ragQueryRuns', ragFmtInt(queryRuns));
      setRagText('ragQuerySuccessRate', `Success rate: ${ragPct(querySuccess, queryRuns)}`);
      setRagText('ragQueryAvgMs', `${ragFmtInt(queryAvgMs)} ms`);
      setRagText('ragQueryAvgTokens', ragFmtInt(queryAvgTokens));
      setRagText(
        'ragQueryTokenTotals',
        `Input ${ragFmtInt(query.total_input_tokens)} • Output ${ragFmtInt(query.total_output_tokens)} • Total ${ragFmtInt(query.total_tokens)}`
      );
      setRagText('ragQueryAvgContext', `Avg context items ${queryAvgContext} • zero-context runs ${ragFmtInt(zeroContextRuns)}`);
      setRagText('ragMetricsUpdated', metrics.updated_at_utc || 'n/a');

      const graphStatus = (data && data.graph_status && typeof data.graph_status === 'object') ? data.graph_status : {};
      const counts = (graphStatus.counts && typeof graphStatus.counts === 'object') ? graphStatus.counts : {};
      setRagText(
        'ragMetricsRuntime',
        `Runtime: ${data.graph_runtime_ok ? 'UP' : 'DOWN'} | LLM: ${graphStatus.llm_model || 'n/a'} | Documents: ${counts.documents ?? 0} | Chunks: ${counts.chunks ?? 0} | Entities: ${counts.entities ?? 0} | Relations: ${counts.relations ?? 0}`
      );
      renderRagRecentRuns(metrics.recent_runs || []);
    }

    async function loadRagMetrics() {
      const errorEl = document.getElementById('ragMetricsError');
      if (errorEl) errorEl.textContent = '';
      try {
        const r = await fetch('/api/graphrag/metrics');
        const data = await r.json();
        if (!r.ok || !data.ok) {
          throw new Error(data.error || data.graph_runtime_error || r.statusText);
        }
        renderRagMetrics(data);
      } catch (e) {
        if (errorEl) errorEl.textContent = 'RAG metrics load failed: ' + String(e);
      }
    }

    async function resetRagMetrics() {
      const errorEl = document.getElementById('ragMetricsError');
      if (errorEl) errorEl.textContent = '';
      try {
        const r = await fetch('/api/graphrag/metrics/reset', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({})
        });
        const data = await r.json();
        if (!r.ok || !data.ok) {
          throw new Error(data.error || r.statusText);
        }
        await loadRagMetrics();
      } catch (e) {
        if (errorEl) errorEl.textContent = 'RAG metrics reset failed: ' + String(e);
      }
    }

    let llmMenuOpen = false;

    function updateLlmTempValue(val) {
      const chip = document.getElementById('llmTempChip');
      const readout = document.getElementById('llmTemperatureValue');
      const v = parseFloat(val || 0);
      if (readout) readout.textContent = v.toFixed(2);
      if (chip) chip.textContent = `Temp: ${v.toFixed(2)}`;
    }

    function toggleLlmMenu(force) {
      const menu = document.getElementById('llmMenu');
      const btn = document.getElementById('llmToggleBtn');
      if (!menu || !btn) return;
      llmMenuOpen = typeof force === 'boolean' ? force : !menu.classList.contains('open');
      menu.classList.toggle('open', llmMenuOpen);
      btn.textContent = llmMenuOpen ? 'Close LLM Config ▲' : 'LLM Config ▼';
    }

    let llmAssignments = {};

    function llmAssignmentText(label, cfg) {
      if (!cfg || cfg.error) return `${label}: unavailable`;
      const provider = (cfg.provider || '--').toUpperCase();
      const model = cfg.effective_model || cfg.model || 'n/a';
      return `${label}: ${provider} / ${model}`;
    }

    function renderLlmAssignmentChips() {
      document.getElementById('llmMonitorChip').textContent = llmAssignmentText('Monitor', llmAssignments.monitor);
      document.getElementById('llmRemediationChip').textContent = llmAssignmentText('Doer', llmAssignments.remediation);
      document.getElementById('llmJudgeChip').textContent = llmAssignmentText('Judge', llmAssignments.judge);
    }

    function selectLlmTarget(value) {
      const target = value || (isDoerRuntime ? 'remediation' : 'monitor');
      const targetSelect = document.getElementById('llmTargetSelect');
      if (targetSelect && targetSelect.value !== target) {
        targetSelect.value = target;
      }
      const chipTargets = {
        monitor: 'llmMonitorChip',
        remediation: 'llmRemediationChip',
        judge: 'llmJudgeChip'
      };
      for (const [chipTarget, chipId] of Object.entries(chipTargets)) {
        const chip = document.getElementById(chipId);
        if (chip) {
          chip.classList.toggle('active', chipTarget === target);
          chip.setAttribute('aria-pressed', chipTarget === target ? 'true' : 'false');
        }
      }
      const data = llmAssignments[target] || {};
      document.getElementById('llmProviderSelect').value = data.provider || 'openai';
      document.getElementById('llmModelInput').value = data.model || '';
      document.getElementById('llmFallbacksInput').value = (data.fallbacks || []).join(', ');
      document.getElementById('llmBaseUrlInput').value = data.base_url || '';
      document.getElementById('llmApiKeyInput').value = '';
      document.getElementById('llmApiKeyInput').placeholder = data.api_key_configured ? '•••••••• (configured)' : 'Provider API token';
      document.getElementById('llmTemperatureInput').value = data.temperature ?? 0;
      updateLlmTempValue(data.temperature ?? 0);
      const statusLine = document.getElementById('llmStatusLine');
      if (statusLine) statusLine.textContent = data.error || data.runtime_error || `Editing ${target} assignment`;
      if ((data.provider || '').toLowerCase() === 'ollama') loadLlmModels();
    }

    function updateLlmProviderDefaults(provider) {
      const baseInput = document.getElementById('llmBaseUrlInput');
      const modelInput = document.getElementById('llmModelInput');
      const fallbacksInput = document.getElementById('llmFallbacksInput');
      const defaults = llmProviderDefaults[provider] || {};
      if (baseInput) baseInput.value = defaults.base_url || '';
      if (modelInput) modelInput.value = defaults.model || '';
      if (fallbacksInput) fallbacksInput.value = (defaults.fallbacks || []).join(', ');
      if (['ollama', 'huggingface'].includes(provider)) loadLlmModels();
    }

    async function loadLlmConfig() {
      const statusLine = document.getElementById('llmStatusLine');
      startLlmBusy('Loading LLM config...', 'processing');
      if (statusLine) statusLine.textContent = 'Loading LLM config...';
      try {
        const res = await fetch(isDoerRuntime ? '/api/llm/config' : '/api/llm/assignments');
        if (!res.ok) throw new Error(await res.text());
        const data = await res.json();
        llmAssignments = isDoerRuntime ? {remediation: data} : (data.assignments || {});
        renderLlmAssignmentChips();
        selectLlmTarget(
          isDoerRuntime
            ? 'remediation'
            : (document.getElementById('llmTargetSelect').value || 'monitor')
        );
        const panel = document.getElementById('llmPanel');
        const panelVisible = panel && !panel.classList.contains('hidden');
        if (panelVisible && !llmMenuOpen) toggleLlmMenu(true);
      } catch (e) {
        if (statusLine) statusLine.textContent = 'Load failed: ' + String(e);
      } finally {
        const elapsedMs = stopLlmBusy();
        if (statusLine && elapsedMs > 0) {
          statusLine.textContent = `${statusLine.textContent} (took ${formatElapsedMs(elapsedMs)})`;
        }
      }
    }

    function selectLlmModelFromDropdown(value) {
      const chosen = (value || '').trim();
      if (!chosen) return;
      const input = document.getElementById('llmModelInput');
      if (input) input.value = chosen;
    }

    async function saveLlmConfig() {
      const btn = document.getElementById('llmSaveBtn');
      const statusLine = document.getElementById('llmStatusLine');
      if (btn) btn.disabled = true;
      startLlmBusy('Applying LLM config...', 'processing');
      if (statusLine) statusLine.textContent = 'Applying...';
      try {
        const payload = {
          provider: document.getElementById('llmProviderSelect').value,
          model: document.getElementById('llmModelInput').value,
          fallbacks: document.getElementById('llmFallbacksInput').value,
          base_url: document.getElementById('llmBaseUrlInput').value,
          api_key: document.getElementById('llmApiKeyInput').value || undefined,
          temperature: document.getElementById('llmTemperatureInput').value,
        };
        const target = isDoerRuntime
          ? 'remediation'
          : (document.getElementById('llmTargetSelect').value || 'monitor');
        const endpoint = isDoerRuntime
          ? '/api/llm/config'
          : `/api/llm/assignments/${encodeURIComponent(target)}`;
        const res = await fetch(endpoint, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        const data = await res.json();
        if (!res.ok || !data.ok) {
          throw new Error(data.error || res.statusText);
        }
        updateLlmTempValue(data.temperature ?? payload.temperature ?? 0);
        if (statusLine) statusLine.textContent = data.message || 'LLM updated';
        await loadLlmConfig();
        await loadState();
        if (!isDoerRuntime) await loadGraphRagStatus();
      } catch (e) {
        if (statusLine) statusLine.textContent = 'Apply failed: ' + String(e);
      } finally {
        if (btn) btn.disabled = false;
        const elapsedMs = stopLlmBusy();
        if (statusLine && elapsedMs > 0) {
          statusLine.textContent = `${statusLine.textContent} (took ${formatElapsedMs(elapsedMs)})`;
        }
      }
    }

    async function loadLlmModels() {
      const provider = document.getElementById('llmProviderSelect').value;
      const statusLine = document.getElementById('llmStatusLine');
      startLlmBusy('Loading provider models...', 'processing');
      if (!['ollama', 'huggingface'].includes(provider)) {
        document.getElementById('llmStatusLine').textContent = 'Model listing is available for Ollama and Hugging Face.';
        const elapsedMsEarly = stopLlmBusy();
        if (statusLine && elapsedMsEarly > 0) {
          statusLine.textContent = `${statusLine.textContent} (took ${formatElapsedMs(elapsedMsEarly)})`;
        }
        return;
      }
      if (statusLine) statusLine.textContent = `Loading ${provider} models...`;
      try {
        const res = await fetch(`/api/llm/models?provider=${encodeURIComponent(provider)}`);
        const data = await res.json();
        if (!res.ok || !data.ok) {
          throw new Error(data.error || res.statusText);
        }
        const list = document.getElementById('llmModelOptions');
        const select = document.getElementById('llmModelSelect');
        list.innerHTML = '';
        if (select) {
          select.innerHTML = '';
          const placeholder = document.createElement('option');
          placeholder.value = '';
          placeholder.textContent = 'Select available model...';
          select.appendChild(placeholder);
        }
        for (const m of data.models || []) {
          const opt = document.createElement('option');
          opt.value = m;
          list.appendChild(opt);
          if (select) {
            const pick = document.createElement('option');
            pick.value = m;
            pick.textContent = m;
            select.appendChild(pick);
          }
        }
        if ((document.getElementById('llmModelInput').value || '').trim() === '' && data.models && data.models.length > 0) {
          document.getElementById('llmModelInput').value = data.models[0];
        }
        if (statusLine) statusLine.textContent = `Loaded ${data.models.length} ${provider} model(s) from ${data.source || 'provider endpoint'}.`;
      } catch (e) {
        if (statusLine) statusLine.textContent = 'Model load failed: ' + String(e);
      } finally {
        const elapsedMs = stopLlmBusy();
        if (statusLine && elapsedMs > 0) {
          statusLine.textContent = `${statusLine.textContent} (took ${formatElapsedMs(elapsedMs)})`;
        }
      }
    }

    let deepEvalRuns = [];

    function renderDeepEvalSummary(scope) {
      const scopedRuns = [];
      const scopedCases = [];
      const metricResults = [];
      const latestByName = {};
      for (const run of deepEvalRuns) {
        let runMatches = false;
        for (const evalCase of run.cases || []) {
          if (scope !== 'both' && evalCase.agent_role !== scope) continue;
          runMatches = true;
          scopedCases.push(evalCase);
          for (const metric of evalCase.metrics || []) {
            metricResults.push(metric);
            if (!latestByName[metric.name]) latestByName[metric.name] = metric;
          }
        }
        if (runMatches) scopedRuns.push(run);
      }
      const passed = metricResults.filter(metric => metric.passed).length;
      const qualityTotal = metricResults.reduce((total, metric) => {
        const raw = Number(metric.score || 0);
        const quality = metric.quality_score === undefined
          ? (metric.score_direction === 'lower_is_better' ? 1 - raw : raw)
          : Number(metric.quality_score || 0);
        return total + quality;
      }, 0);
      const averageQuality = metricResults.length ? qualityTotal / metricResults.length : 0;
      const averageDuration = scopedRuns.length
        ? scopedRuns.reduce((total, run) => total + Number(run.duration_ms || 0), 0) / scopedRuns.length
        : 0;
      const latestScore = name => latestByName[name] ? Number(latestByName[name].score || 0).toFixed(3) : 'n/a';

      setRagText('deepevalCaseCount', scopedCases.length.toLocaleString('en-US'));
      setRagText('deepevalRunCount', `Across ${scopedRuns.length.toLocaleString('en-US')} runs`);
      setRagText('deepevalPassRate', metricResults.length ? ((passed / metricResults.length) * 100).toFixed(1) + '%' : '0.0%');
      setRagText('deepevalMetricCount', `${metricResults.length.toLocaleString('en-US')} metric results`);
      setRagText('deepevalQualityScore', averageQuality.toFixed(3));
      setRagText('deepevalAvgDuration', `${Math.round(averageDuration).toLocaleString('en-US')} ms`);
      setRagText('deepevalAnswerRelevancy', latestScore('AnswerRelevancy'));
      setRagText('deepevalOperationalQuality', latestScore('Kafka Operational Quality'));
      setRagText('deepevalBias', latestScore('Bias'));
      setRagText('deepevalToxicity', latestScore('Toxicity'));
    }

    function renderDeepEvalRuns() {
      const originalBody = document.getElementById('deepevalOriginalResultsBody');
      const safetyBody = document.getElementById('deepevalSafetyResultsBody');
      const scope = document.getElementById('deepevalScope').value;
      const originalRows = [];
      const safetyRows = [];
      const originalMetricNames = new Set(['AnswerRelevancy', 'Kafka Operational Quality', 'GEval']);
      for (const run of deepEvalRuns) {
        for (const evalCase of run.cases || []) {
          if (scope !== 'both' && evalCase.agent_role !== scope) continue;
          for (const metric of evalCase.metrics || []) {
            const commonStart =
              '<tr>' +
                '<td>' + esc(run.created_at_utc || '') + '</td>' +
                '<td>' + esc(String(evalCase.agent_role || '').toUpperCase()) + '</td>' +
                '<td>' + esc(metric.name || '') + '</td>' +
                '<td>' + esc(Number(metric.score || 0).toFixed(3)) + '</td>';
            const commonEnd =
                '<td><span class="rag-status-badge ' + (metric.passed ? 'ok' : 'fail') + '">' + (metric.passed ? 'PASS' : 'FAIL') + '</span></td>' +
                '<td>' + esc(metric.reason || '') + '</td>' +
              '</tr>';
            if (originalMetricNames.has(metric.name)) {
              originalRows.push(commonStart + commonEnd);
            } else {
              const direction = metric.score_direction === 'lower_is_better' ? 'Lower ↓' : 'Higher ↑';
              safetyRows.push(commonStart + '<td>' + direction + '</td>' + commonEnd);
            }
          }
        }
      }
      originalBody.innerHTML = originalRows.length
        ? originalRows.join('')
        : '<tr><td colspan="6">No original quality evaluations for this scope yet.</td></tr>';
      safetyBody.innerHTML = safetyRows.length
        ? safetyRows.join('')
        : '<tr><td colspan="7">No additional safety evaluations for this scope yet.</td></tr>';
      renderDeepEvalSummary(scope);
    }

    async function loadDeepEvalStatus() {
      const statusLine = document.getElementById('deepevalStatus');
      const errorLine = document.getElementById('deepevalError');
      errorLine.textContent = '';
      try {
        const response = await fetch('/api/deepeval/status');
        const data = await response.json();
        if (!data.ok) throw new Error(data.error || 'DeepEval is unavailable');
        deepEvalRuns = data.runs || [];
        const health = data.health || {};
        statusLine.textContent = `Local judge: ${health.judge_model || 'unknown'} | Auto-watch: ${data.auto_evaluate ? 'ON' : 'OFF'} | Quality threshold: ${health.threshold ?? 0.7} | Safety maximum: ${health.safety_threshold ?? 0.2} | Runs: ${deepEvalRuns.length}`;
        renderDeepEvalRuns();
      } catch (error) {
        errorLine.textContent = String(error);
      }
    }

    async function runDeepEval() {
      const button = document.getElementById('deepevalRunBtn');
      const errorLine = document.getElementById('deepevalError');
      button.disabled = true;
      errorLine.textContent = '';
      document.getElementById('deepevalStatus').textContent = 'DeepEval judge is scoring the selected scope...';
      try {
        const response = await fetch('/api/deepeval/evaluate', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            scope: document.getElementById('deepevalScope').value,
            threshold: Number(document.getElementById('deepevalThreshold').value || 0.7)
          })
        });
        const data = await response.json();
        if (!data.ok) throw new Error(data.error || 'DeepEval run failed');
        await loadDeepEvalStatus();
      } catch (error) {
        errorLine.textContent = String(error);
      } finally {
        button.disabled = false;
      }
    }

    function switchTab(tab) {
      const tabs = {
        expert: ['expertPanel', 'tabExpertBtn'],
        graph_rag: ['graphRagPanel', 'tabGraphRagBtn'],
        rag_metrics: ['ragMetricsPanel', 'tabRagMetricsBtn'],
        llm: ['llmPanel', 'tabLlmBtn'],
        deepeval: ['deepevalPanel', 'tabDeepEvalBtn'],
        fine_tuning: ['fineTuningPanel', 'tabFineTuningBtn'],
        producer: ['producerPanel', 'tabProducerBtn'],
        consumer: ['consumerPanel', 'tabConsumerBtn'],
        grafana: ['grafanaPanel', 'tabGrafanaBtn'],
        neo4j_browser: ['neo4jBrowserPanel', 'tabNeo4jBrowserBtn'],
        kafka_ui: ['kafkaUiPanel', 'tabKafkaUiBtn']
      };
      // Keep tab switching total: a hidden role-specific control must not
      // throw and prevent every other tab from responding.
      Object.values(tabs).forEach(([panelId, buttonId]) => {
        const panel = document.getElementById(panelId);
        const button = document.getElementById(buttonId);
        if (panel) panel.classList.toggle('hidden', panelId !== (tabs[tab] || [])[0]);
        if (button) button.classList.toggle('active', buttonId === (tabs[tab] || [])[1]);
      });
      const isLlm = tab === 'llm';
      const isRagMetrics = tab === 'rag_metrics';
      const isDeepEval = tab === 'deepeval';

      if (isLlm) {
        loadLlmConfig();
        toggleLlmMenu(true);
      } else {
        toggleLlmMenu(false);
      }
      if (isRagMetrics) {
        loadRagMetrics();
      }
      if (isDeepEval) {
        loadDeepEvalStatus();
      }
if (tab === 'fine_tuning') {
  refreshFineTuningStatus();
  updateFineTuningEstimate();
}
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
      if (idx === -1 || (!lower.includes('[warn]') && !lower.includes('[bad]'))) {
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

    const AI_USAGE_STORAGE_KEY = 'kafka_expert_ai_usage';
    const DEFAULT_AI_TOKEN_BUDGET = __AI_TOKEN_BUDGET__;

    function toSafeInt(value, fallback = 0) {
      const parsed = Number.parseInt(String(value ?? fallback), 10);
      return Number.isFinite(parsed) ? parsed : fallback;
    }

    function tokenInt(value) {
      return Math.max(0, toSafeInt(value, 0));
    }

    function formatCount(value) {
      return tokenInt(value).toLocaleString('en-US');
    }

    function loadAiUsageState() {
      const base = {
        session_input_tokens: 0,
        session_output_tokens: 0,
        session_total_tokens: 0,
        runs_with_usage: 0,
        last_run_input_tokens: 0,
        last_run_output_tokens: 0,
        last_run_total_tokens: 0,
        last_run_duration_ms: 0,
        last_trace_id: '',
        token_budget: tokenInt(DEFAULT_AI_TOKEN_BUDGET) || 250000
      };
      try {
        const raw = window.localStorage.getItem(AI_USAGE_STORAGE_KEY);
        if (!raw) return base;
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed !== 'object') return base;
        return {
          session_input_tokens: tokenInt(parsed.session_input_tokens),
          session_output_tokens: tokenInt(parsed.session_output_tokens),
          session_total_tokens: tokenInt(parsed.session_total_tokens),
          runs_with_usage: tokenInt(parsed.runs_with_usage),
          last_run_input_tokens: tokenInt(parsed.last_run_input_tokens),
          last_run_output_tokens: tokenInt(parsed.last_run_output_tokens),
          last_run_total_tokens: tokenInt(parsed.last_run_total_tokens),
          last_run_duration_ms: tokenInt(parsed.last_run_duration_ms),
          last_trace_id: String(parsed.last_trace_id || ''),
          token_budget: Math.max(1, tokenInt(parsed.token_budget || base.token_budget))
        };
      } catch (_) {
        return base;
      }
    }

    const aiUsageState = loadAiUsageState();

    function saveAiUsageState() {
      try {
        window.localStorage.setItem(AI_USAGE_STORAGE_KEY, JSON.stringify(aiUsageState));
      } catch (_) {
        // Ignore storage errors so UX remains responsive.
      }
    }

    function usageBudgetPercent() {
      const budget = Math.max(1, tokenInt(aiUsageState.token_budget));
      return (tokenInt(aiUsageState.session_total_tokens) / budget) * 100;
    }

    function traceIdentity(trace) {
      if (!trace || typeof trace !== 'object') return '';
      const captured = String(trace.captured_at_utc || '');
      const source = String(trace.prompt_source || '');
      const duration = tokenInt(trace.duration_ms);
      const messages = tokenInt(trace.message_count);
      if (!captured && !source && duration <= 0 && messages <= 0) return '';
      return `${captured}|${source}|${duration}|${messages}`;
    }

    function usageBudgetInline() {
      const budget = Math.max(1, tokenInt(aiUsageState.token_budget));
      const used = tokenInt(aiUsageState.session_total_tokens);
      const pct = (used / budget) * 100;
      return `Tokens ${formatCount(used)}/${formatCount(budget)} (${pct.toFixed(1)}%)`;
    }

    function renderAiUsagePanel() {
      const runTokensEl = document.getElementById('usageRunTokens');
      const sessionTokensEl = document.getElementById('usageSessionTokens');
      const lastRuntimeEl = document.getElementById('usageLastRuntime');
      const budgetValueEl = document.getElementById('usageBudgetValue');
      const budgetFillEl = document.getElementById('usageBudgetFill');
      const sourceLineEl = document.getElementById('usageSourceLine');
      const runInput = tokenInt(aiUsageState.last_run_input_tokens);
      const runOutput = tokenInt(aiUsageState.last_run_output_tokens);
      const runTotal = tokenInt(aiUsageState.last_run_total_tokens);
      const sessionTotal = tokenInt(aiUsageState.session_total_tokens);
      const budget = Math.max(1, tokenInt(aiUsageState.token_budget));
      const pct = usageBudgetPercent();
      if (runTokensEl) {
        runTokensEl.textContent = `Run tokens: ${formatCount(runTotal)} (in ${formatCount(runInput)} / out ${formatCount(runOutput)})`;
      }
      if (sessionTokensEl) {
        sessionTokensEl.textContent = `Session tokens: ${formatCount(sessionTotal)} across ${formatCount(aiUsageState.runs_with_usage)} AI run(s)`;
      }
      if (lastRuntimeEl) {
        lastRuntimeEl.textContent = `Last AI runtime: ${formatElapsedMs(aiUsageState.last_run_duration_ms)}`;
      }
      if (budgetValueEl) {
        budgetValueEl.textContent = `${formatCount(sessionTotal)} / ${formatCount(budget)} tokens (${pct.toFixed(1)}%)`;
      }
      if (budgetFillEl) {
        budgetFillEl.style.width = `${Math.min(100, Math.max(0, pct))}%`;
        budgetFillEl.classList.toggle('warn', pct >= 80 && pct < 100);
        budgetFillEl.classList.toggle('over', pct >= 100);
      }
      if (sourceLineEl) {
        const overText = pct >= 100 ? 'Budget exceeded.' : 'Budget remaining:';
        sourceLineEl.textContent = `${overText} ${formatCount(Math.max(0, budget - sessionTotal))} tokens.`;
      }
    }

    function applyTraceUsage(trace) {
      const traceId = traceIdentity(trace);
      if (!traceId) {
        renderAiUsagePanel();
        return;
      }
      const tokenUsage = (trace && typeof trace.token_usage === 'object') ? trace.token_usage : {};
      const runInput = tokenInt(tokenUsage.input_tokens);
      const runOutput = tokenInt(tokenUsage.output_tokens);
      const runTotal = tokenInt(tokenUsage.total_tokens);
      const runDurationMs = tokenInt(trace.duration_ms);
      aiUsageState.last_run_input_tokens = runInput;
      aiUsageState.last_run_output_tokens = runOutput;
      aiUsageState.last_run_total_tokens = runTotal;
      aiUsageState.last_run_duration_ms = runDurationMs;
      if (aiUsageState.last_trace_id !== traceId) {
        aiUsageState.last_trace_id = traceId;
        if (runTotal > 0 || runInput > 0 || runOutput > 0) {
          aiUsageState.session_input_tokens = tokenInt(aiUsageState.session_input_tokens + runInput);
          aiUsageState.session_output_tokens = tokenInt(aiUsageState.session_output_tokens + runOutput);
          aiUsageState.session_total_tokens = tokenInt(aiUsageState.session_total_tokens + runTotal);
          aiUsageState.runs_with_usage = tokenInt(aiUsageState.runs_with_usage + 1);
        }
      }
      saveAiUsageState();
      renderAiUsagePanel();
    }

    function resetAiUsageSession() {
      aiUsageState.session_input_tokens = 0;
      aiUsageState.session_output_tokens = 0;
      aiUsageState.session_total_tokens = 0;
      aiUsageState.runs_with_usage = 0;
      aiUsageState.last_run_input_tokens = 0;
      aiUsageState.last_run_output_tokens = 0;
      aiUsageState.last_run_total_tokens = 0;
      aiUsageState.last_run_duration_ms = 0;
      aiUsageState.last_trace_id = '';
      saveAiUsageState();
      renderAiUsagePanel();
    }

    const busyScopes = {
      expert: { chipId: 'expertBusy', textId: 'expertBusyText', onToggle: setExpertControlsDisabled },
      graph: { chipId: 'graphBusy', textId: 'graphBusyText' },
      llm: { chipId: 'llmBusy', textId: 'llmBusyText' }
    };
    const busyState = {};
    const BUSY_DURATION_STORAGE_KEY = 'kafka_expert_busy_durations';

    function loadBusyDurations() {
      try {
        const raw = window.localStorage.getItem(BUSY_DURATION_STORAGE_KEY);
        if (!raw) return {};
        const parsed = JSON.parse(raw);
        return parsed && typeof parsed === 'object' ? parsed : {};
      } catch (_) {
        return {};
      }
    }

    const storedBusyDurations = loadBusyDurations();

    function saveBusyDuration(scope, elapsedMs) {
      try {
        storedBusyDurations[scope] = {
          elapsed_ms: Math.max(0, Math.floor(elapsedMs || 0)),
          completed_at_utc: new Date().toISOString()
        };
        window.localStorage.setItem(BUSY_DURATION_STORAGE_KEY, JSON.stringify(storedBusyDurations));
      } catch (_) {
        // Ignore storage errors so UX remains responsive.
      }
    }

    function formatElapsed(seconds) {
      const safe = Math.max(0, Math.floor(seconds || 0));
      const mins = Math.floor(safe / 60);
      const secs = safe % 60;
      return String(mins).padStart(2, '0') + ':' + String(secs).padStart(2, '0');
    }

    function formatElapsedMs(ms) {
      return formatElapsed((ms || 0) / 1000);
    }

    function busyLabel(kind) {
      return kind === 'ai' ? 'Waiting on AI' : 'Processing';
    }

    function renderBusy(scope) {
      const cfg = busyScopes[scope];
      const state = busyState[scope];
      if (!cfg || !state) return;
      const chip = document.getElementById(cfg.chipId);
      const text = document.getElementById(cfg.textId);
      if (!chip || !text) return;
      const elapsedSeconds = state.startedAtMs > 0 ? (Date.now() - state.startedAtMs) / 1000 : 0;
      const usageSnapshot = state.kind === 'ai' ? usageBudgetInline() : '';
      text.textContent = usageSnapshot
        ? `${busyLabel(state.kind)} (${formatElapsed(elapsedSeconds)}) • ${state.message} • ${usageSnapshot}`
        : `${busyLabel(state.kind)} (${formatElapsed(elapsedSeconds)}) • ${state.message}`;
      chip.classList.toggle('ai', state.kind === 'ai');
      chip.classList.toggle('processing', state.kind !== 'ai');
    }

    function setExpertControlsDisabled(disabled) {
      const ids = ['chatInput', 'chatSendBtn', 'refreshStateBtn', 'clearChatBtn'];
      for (const id of ids) {
        const el = document.getElementById(id);
        if (el) el.disabled = disabled;
      }
      for (const fixBtn of document.querySelectorAll('button.fix-action')) {
        fixBtn.disabled = disabled;
      }
    }

    function startTimedBusy(scope, message, kind) {
      const cfg = busyScopes[scope];
      if (!cfg) return;
      if (!busyState[scope]) {
        busyState[scope] = {
          depth: 0,
          startedAtMs: 0,
          timer: null,
          kind: 'processing',
          message: 'Working...',
          lastElapsedMs: Number((storedBusyDurations[scope] || {}).elapsed_ms || 0)
        };
      }
      const state = busyState[scope];
      state.depth += 1;
      state.kind = String(kind || 'processing').toLowerCase() === 'ai' ? 'ai' : 'processing';
      state.message = message || 'Working...';
      if (state.startedAtMs === 0) {
        state.startedAtMs = Date.now();
      }
      renderBusy(scope);
      const chip = document.getElementById(cfg.chipId);
      if (chip) chip.classList.remove('hidden');
      if (state.timer === null) {
        state.timer = window.setInterval(() => renderBusy(scope), 1000);
      }
      if (typeof cfg.onToggle === 'function') {
        cfg.onToggle(true);
      }
    }

    function stopTimedBusy(scope) {
      const cfg = busyScopes[scope];
      const state = busyState[scope];
      if (!cfg || !state) return 0;
      state.depth = Math.max(0, state.depth - 1);
      if (state.depth > 0) return 0;
      const elapsedMs = state.startedAtMs > 0 ? (Date.now() - state.startedAtMs) : 0;
      state.lastElapsedMs = Math.max(0, Math.floor(elapsedMs));
      saveBusyDuration(scope, state.lastElapsedMs);
      const chip = document.getElementById(cfg.chipId);
      if (chip) chip.classList.add('hidden');
      if (state.timer !== null) {
        window.clearInterval(state.timer);
        state.timer = null;
      }
      state.startedAtMs = 0;
      state.kind = 'processing';
      state.message = 'Working...';
      if (typeof cfg.onToggle === 'function') {
        cfg.onToggle(false);
      }
      return state.lastElapsedMs;
    }

    function startExpertBusy(message, kind) {
      startTimedBusy('expert', message, kind);
    }

    function stopExpertBusy() {
      return stopTimedBusy('expert');
    }

    function startGraphBusy(message, kind) {
      startTimedBusy('graph', message, kind);
    }

    function stopGraphBusy() {
      return stopTimedBusy('graph');
    }

    function startLlmBusy(message, kind) {
      startTimedBusy('llm', message, kind);
    }

    function stopLlmBusy() {
      return stopTimedBusy('llm');
    }

    async function pollRemediationTask(taskId) {
      const deadline = Date.now() + (10 * 60 * 1000);
      while (Date.now() < deadline) {
        await new Promise(resolve => window.setTimeout(resolve, 2000));
        const busyText = document.getElementById('expertBusyText');
        if (busyText) busyText.textContent = 'Waiting for a human decision in the Doer UI...';
        const response = await fetch('/api/auto_fix/tasks/' + encodeURIComponent(taskId));
        const data = await response.json();
        await loadState();
        if (!data.ok && data.terminal) {
          throw new Error(data.error || 'The remediation task did not run.');
        }
        if (!data.ok) {
          throw new Error(data.error || 'Could not read the remediation task.');
        }
        if (data.terminal) return data;
      }
      throw new Error('Timed out waiting for the Doer approval decision.');
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
      startExpertBusy('Sending finding to Kafka Remediation Agent over A2A...', 'ai');

      try {
        const r = await fetch('/api/auto_fix', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ issue_line: issueLine })
        });
        const data = await r.json();
        if (!data.ok) {
          document.getElementById('statusError').textContent = data.error || 'Auto-fix request failed';
        } else if (data.pending_approval && data.a2a_task_id) {
          await loadState();
          await pollRemediationTask(data.a2a_task_id);
        }
        await loadState();
      } catch (e) {
        document.getElementById('statusError').textContent = String(e);
      } finally {
        if (sendBtn) sendBtn.disabled = false;
        if (refreshBtn) refreshBtn.disabled = false;
        if (clearBtn) clearBtn.disabled = false;
        stopExpertBusy();
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
      applyTraceUsage(data.last_trace || {});

      const log = document.getElementById('chatLog');
      log.innerHTML = '';
      const statusText = String(data.status_text || '').trim();
      if (statusText) {
        const clusterHtml = renderSeverityLines(statusText);
        log.insertAdjacentHTML(
          'beforeend',
          '<div class="msg cluster">' +
            '<div class="meta">' + (isDoerRuntime ? 'DOER STATE' : 'CLUSTER STATE') +
              ' • ' + esc(data.status_updated_at || '') + '</div>' +
            '<div class="text">' + clusterHtml + '</div>' +
          '</div>'
        );
      }
      for (const m of data.chat || []) {
        const cls = m.role === 'user' ? 'user' : 'agent';
        let text = '';
        if (m.role === 'agent' || m.role === 'monitor' || m.role === 'remediation') {
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
        const roleLabel = m.role === 'remediation' ? 'REMEDIATION AGENT' :
          (m.role === 'agent' || m.role === 'monitor' ? 'MONITOR AGENT' : m.role.toUpperCase());
        log.insertAdjacentHTML(
          'beforeend',
          '<div class="msg ' + cls + '">' +
            '<div class="meta">' + esc(roleLabel) + ' • ' + esc(m.at_utc || '') + '</div>' +
            '<div class="text">' + text + '</div>' +
          '</div>'
        );
      }
      log.scrollTop = log.scrollHeight;
    }

    async function refreshClusterState() {
      const btn = document.getElementById('refreshStateBtn');
      if (btn.disabled) return;
      if (isDoerRuntime) {
        await loadState();
        return;
      }
      btn.disabled = true;
      document.getElementById('statusError').textContent = '';
        startExpertBusy('Querying full cluster state...', 'ai');
      try {
        const r = await fetch('/api/cluster_state', { method: 'POST', headers: { 'Content-Type': 'application/json' }});
        const data = await r.json();
        if (!r.ok || data.ok === false) throw new Error(data.error || data.last_error || r.statusText || 'Cluster-state query failed');
        if (data.pending) {
          document.getElementById('statusError').textContent = 'Assessment is running; waiting for the verified result...';
          const deadline = Date.now() + 120000;
          while (Date.now() < deadline) {
            await new Promise(resolve => window.setTimeout(resolve, 2000));
            const poll = await fetch('/api/cluster_state/status');
            const pollData = await poll.json();
            if (!pollData.running) {
              if (pollData.error) throw new Error(pollData.error);
              break;
            }
          }
          if (Date.now() >= deadline) throw new Error('Cluster-state assessment timed out; check the status panel and logs.');
        }
        await loadState();
      } catch (e) {
        document.getElementById('statusError').textContent = String(e);
      } finally {
        btn.disabled = false;
        stopExpertBusy();
      }
    }

    async function sendChat() {
      const input = document.getElementById('chatInput');
      const btn = document.getElementById('chatSendBtn');
      const message = input.value.trim();
      if (!message) return;
      if (btn.disabled) return;

      btn.disabled = true;
      startExpertBusy('Thinking...', 'ai');
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
        stopExpertBusy();
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
      startExpertBusy('Clearing chat...', 'processing');
      try {
        const r = await fetch('/api/chat/clear', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' }
        });
        const data = await r.json();
        if (!data.ok) {
          document.getElementById('statusError').textContent = data.error || 'Clear chat failed';
        } else {
          resetAiUsageSession();
        }
        await loadState();
      } catch (e) {
        document.getElementById('statusError').textContent = String(e);
      } finally {
        clearBtn.disabled = false;
        if (sendBtn) sendBtn.disabled = false;
        if (refreshBtn) refreshBtn.disabled = false;
        stopExpertBusy();
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

    configureRoleUi();
    renderAiUsagePanel();
    switchTab('expert');
    if (!isDoerRuntime) {
      loadGraphRagStatus();
      loadRagMetrics();
    }
    loadState();
    loadLlmConfig();
    if (isDoerRuntime) {
      loadHitlState();
      window.setInterval(loadState, 5000);
      window.setInterval(loadHitlState, 2500);
    }
  </script>
</body>
</html>"""
    is_doer = AGENT_PROCESS_ROLE == "remediation"
    role_copy = {
        "page_title": "Kafka Remediation Agent" if is_doer else "Kafka Monitor Agent",
        "page_heading": "Kafka Remediation Agent" if is_doer else "Kafka Operations Agents",
        "page_subtitle": (
            "Independent Doer runtime: receive authorized A2A tasks, inspect repair history, and provision its LLM."
            if is_doer
            else "A read-only Monitor Agent hands approved fixes to a Remediation Agent over A2A 1.0."
        ),
        "expert_tab_label": "Doer Agent" if is_doer else "Kafka Expert",
        "agent_panel_heading": "Kafka Remediation Agent" if is_doer else "Kafka Monitor Agent",
        "refresh_label": "Refresh Doer State" if is_doer else "Query Full Cluster State",
        "status_label": "Doer State Updated" if is_doer else "Cluster State Updated",
    }
    provider_defaults = {
        "openai": {
            "model": OPENAI_MODEL,
            "fallbacks": OPENAI_MODEL_FALLBACKS,
            "base_url": OPENAI_BASE_URL,
        },
        "ollama": {
            "model": OLLAMA_MODEL,
            "fallbacks": OLLAMA_MODEL_FALLBACKS,
            "base_url": OLLAMA_BASE_URL,
        },
        "huggingface": {
            "model": HUGGINGFACE_MODEL,
            "fallbacks": HUGGINGFACE_MODEL_FALLBACKS,
            "base_url": HUGGINGFACE_OPENAI_BASE_URL,
        },
    }
    return (
        page.replace("__PAGE_TITLE__", role_copy["page_title"]).replace(
            "__PAGE_HEADING__", role_copy["page_heading"]
        ).replace(
            "__PAGE_SUBTITLE__", role_copy["page_subtitle"]
        ).replace(
            "__EXPERT_TAB_LABEL__", role_copy["expert_tab_label"]
        ).replace(
            "__AGENT_PANEL_HEADING__", role_copy["agent_panel_heading"]
        ).replace(
            "__REFRESH_LABEL__", role_copy["refresh_label"]
        ).replace(
            "__STATUS_LABEL__", role_copy["status_label"]
        ).replace(
            "__AGENT_PROCESS_ROLE__", AGENT_PROCESS_ROLE
        ).replace(
            "__LLM_PROVIDER_DEFAULTS_JSON__", json.dumps(provider_defaults)
        ).replace(
            "__AI_TOKEN_BUDGET__", str(AI_TOKEN_BUDGET)
        ).replace(
            "__MLX_TRAINING_DATA_DIR__", html.escape(MLX_TRAINING_DATA_DIR, quote=True)
        ).replace(
            "__KAFKA_UI_PUBLIC_URL__", KAFKA_UI_PUBLIC_URL
        ).replace(
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


def _current_llm_config() -> dict[str, Any]:
    """Return active LLM configuration for UI display."""
    provider = _normalize_llm_provider(LLM_PROVIDER)
    candidates = resolve_agent_models(
        AGENT_PROCESS_ROLE,
        provider_models=_model_candidates_for_provider(provider),
        monitor_model=MONITOR_MODEL,
        monitor_fallbacks=MONITOR_MODEL_FALLBACKS,
        remediation_model=REMEDIATION_MODEL,
        remediation_fallbacks=REMEDIATION_MODEL_FALLBACKS,
    )
    model = candidates[0] if candidates else ""
    fallbacks = [m for m in candidates if m != model]
    local_runtime = runtime if AGENT_PROCESS_ROLE == "monitor" else remediation_runtime
    return {
        "target": AGENT_PROCESS_ROLE,
        "provider": provider,
        "model": model,
        "effective_model": getattr(local_runtime, "model_name", "") if local_runtime is not None else "",
        "fallbacks": fallbacks,
        "base_url": _llm_base_url_for_provider(provider),
        "temperature": LLM_TEMPERATURE,
        "api_key_configured": bool(_llm_api_key_for_provider(provider)),
        "ok": local_runtime is not None,
    }


def _apply_llm_config(
    provider: str,
    model: str | None,
    fallbacks: list[str] | None,
    base_url: str | None,
    api_key: str | None,
    temperature: float | None,
) -> tuple[bool, str]:
    """Update in-memory LLM configuration and rebuild runtimes."""
    global LLM_PROVIDER
    global LLM_TEMPERATURE
    global OPENAI_MODEL, OPENAI_MODEL_FALLBACKS, OPENAI_BASE_URL, OPENAI_API_KEY
    global OLLAMA_MODEL, OLLAMA_MODEL_FALLBACKS, OLLAMA_BASE_URL, OLLAMA_API_KEY
    global HUGGINGFACE_MODEL, HUGGINGFACE_MODEL_FALLBACKS, HUGGINGFACE_OPENAI_BASE_URL, HUGGINGFACE_API_KEY
    global MONITOR_MODEL, MONITOR_MODEL_FALLBACKS
    global REMEDIATION_MODEL, REMEDIATION_MODEL_FALLBACKS

    provider = _normalize_llm_provider(provider or LLM_PROVIDER)
    if provider not in {"openai", "ollama", "huggingface"}:
        return False, f"Unsupported provider '{provider}'."

    previous = {
        "provider": LLM_PROVIDER,
        "temperature": LLM_TEMPERATURE,
        "openai_model": OPENAI_MODEL,
        "openai_fallbacks": list(OPENAI_MODEL_FALLBACKS),
        "openai_base_url": OPENAI_BASE_URL,
        "openai_api_key": OPENAI_API_KEY,
        "ollama_model": OLLAMA_MODEL,
        "ollama_fallbacks": list(OLLAMA_MODEL_FALLBACKS),
        "ollama_base_url": OLLAMA_BASE_URL,
        "ollama_api_key": OLLAMA_API_KEY,
        "huggingface_model": HUGGINGFACE_MODEL,
        "huggingface_fallbacks": list(HUGGINGFACE_MODEL_FALLBACKS),
        "huggingface_base_url": HUGGINGFACE_OPENAI_BASE_URL,
        "huggingface_api_key": HUGGINGFACE_API_KEY,
        "monitor_model": MONITOR_MODEL,
        "monitor_fallbacks": list(MONITOR_MODEL_FALLBACKS),
        "remediation_model": REMEDIATION_MODEL,
        "remediation_fallbacks": list(REMEDIATION_MODEL_FALLBACKS),
    }

    if temperature is not None:
        try:
            LLM_TEMPERATURE = float(temperature)
        except Exception:
            return False, "LLM temperature must be numeric."

    if provider == "openai":
        if model:
            OPENAI_MODEL = model.strip()
        if fallbacks is not None:
            OPENAI_MODEL_FALLBACKS = _dedupe_models(fallbacks)
        if base_url is not None:
            OPENAI_BASE_URL = base_url.strip()
        if api_key is not None:
            OPENAI_API_KEY = api_key.strip()
    elif provider == "ollama":
        if model:
            OLLAMA_MODEL = model.strip()
        if fallbacks is not None:
            OLLAMA_MODEL_FALLBACKS = _dedupe_models(fallbacks)
        if base_url is not None:
            OLLAMA_BASE_URL = base_url.strip()
        if api_key is not None:
            OLLAMA_API_KEY = api_key.strip()
    elif provider == "huggingface":
        if model:
            HUGGINGFACE_MODEL = model.strip()
        if fallbacks is not None:
            HUGGINGFACE_MODEL_FALLBACKS = _dedupe_models(fallbacks)
        if base_url is not None:
            HUGGINGFACE_OPENAI_BASE_URL = base_url.strip()
        if api_key is not None:
            HUGGINGFACE_API_KEY = api_key.strip()

    if AGENT_PROCESS_ROLE == "monitor":
        if model:
            MONITOR_MODEL = model.strip()
        if fallbacks is not None:
            MONITOR_MODEL_FALLBACKS = _dedupe_models(fallbacks)
    else:
        if model:
            REMEDIATION_MODEL = model.strip()
        if fallbacks is not None:
            REMEDIATION_MODEL_FALLBACKS = _dedupe_models(fallbacks)

    LLM_PROVIDER = provider
    ok, error = rebuild_runtimes(provider)
    if ok:
        return True, ""

    LLM_PROVIDER = str(previous["provider"])
    LLM_TEMPERATURE = float(previous["temperature"])
    OPENAI_MODEL = str(previous["openai_model"])
    OPENAI_MODEL_FALLBACKS = list(previous["openai_fallbacks"])
    OPENAI_BASE_URL = str(previous["openai_base_url"])
    OPENAI_API_KEY = str(previous["openai_api_key"])
    OLLAMA_MODEL = str(previous["ollama_model"])
    OLLAMA_MODEL_FALLBACKS = list(previous["ollama_fallbacks"])
    OLLAMA_BASE_URL = str(previous["ollama_base_url"])
    OLLAMA_API_KEY = str(previous["ollama_api_key"])
    HUGGINGFACE_MODEL = str(previous["huggingface_model"])
    HUGGINGFACE_MODEL_FALLBACKS = list(previous["huggingface_fallbacks"])
    HUGGINGFACE_OPENAI_BASE_URL = str(previous["huggingface_base_url"])
    HUGGINGFACE_API_KEY = str(previous["huggingface_api_key"])
    MONITOR_MODEL = str(previous["monitor_model"])
    MONITOR_MODEL_FALLBACKS = list(previous["monitor_fallbacks"])
    REMEDIATION_MODEL = str(previous["remediation_model"])
    REMEDIATION_MODEL_FALLBACKS = list(previous["remediation_fallbacks"])
    restored, restore_error = rebuild_runtimes(LLM_PROVIDER)
    if not restored:
        return False, f"{error} Previous LLM configuration could not be restored: {restore_error}"
    return False, f"{error} Previous working LLM configuration was restored."


def _ollama_model_dirs() -> list[Path]:
    """Return candidate directories where Ollama stores downloaded model manifests."""
    candidates: list[Path] = []
    for raw in [OLLAMA_MODELS_DIR, str(Path.home() / ".ollama" / "models"), "/root/.ollama/models"]:
        value = (raw or "").strip()
        if not value:
            continue
        path = Path(value).expanduser()
        if path not in candidates:
            candidates.append(path)
    return candidates


def _model_from_ollama_manifest(manifest_root: Path, manifest_file: Path) -> str | None:
    """Translate one Ollama manifest path into a model id (for example llama3.1:8b)."""
    try:
        rel_parts = manifest_file.relative_to(manifest_root).parts
    except Exception:
        return None
    if len(rel_parts) < 3:
        return None
    repo_parts = list(rel_parts[1:-1])
    tag = rel_parts[-1].strip()
    if not repo_parts or not tag:
        return None
    if repo_parts[0] == "library":
        repo_parts = repo_parts[1:]
    if not repo_parts:
        return None
    return f"{'/'.join(repo_parts)}:{tag}"


def _fetch_ollama_models_from_directory() -> tuple[list[str], str]:
    """Return model ids from Ollama's local manifest tree."""
    for models_dir in _ollama_model_dirs():
        manifest_root = models_dir / "manifests"
        if not manifest_root.exists() or not manifest_root.is_dir():
            continue
        models: list[str] = []
        for manifest_file in sorted(path for path in manifest_root.rglob("*") if path.is_file()):
            model_id = _model_from_ollama_manifest(manifest_root, manifest_file)
            if model_id:
                models.append(model_id)
        if models:
            return _dedupe_models(models), str(manifest_root)
    return [], ""


def _fetch_huggingface_models() -> tuple[list[str], str]:
    """List chat models exposed by the Hugging Face OpenAI-compatible router."""
    base_url = _llm_base_url_for_provider("huggingface").rstrip("/")
    api_key = _llm_api_key_for_provider("huggingface")
    if "router.huggingface.co" in base_url and not api_key:
        raise RuntimeError("Enter a Hugging Face API token with inference permission before loading models.")
    headers = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    req = urllib.request.Request(f"{base_url}/models", headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=30.0) as response:
        payload = json.loads(response.read().decode("utf-8"))
    rows = payload.get("data", []) if isinstance(payload, dict) else []
    models = _dedupe_models([str(row.get("id", "")) for row in rows if isinstance(row, dict)])
    return models[:500], f"{base_url}/models"


def rebuild_runtimes(provider: str) -> tuple[bool, str]:
    """Reinitialize only the agent runtime owned by this process."""
    global runtime, runtime_error, remediation_runtime, remediation_runtime_error
    global graph_runtime, graph_runtime_error, context_store
    with runtime_reconfig_lock:
        local_error = ""
        if AGENT_PROCESS_ROLE == "remediation":
            try:
                remediation_runtime = KafkaExpertRuntime(
                    context_store=context_store if context_store.enabled else None,
                    agent_role="remediation",
                )
                remediation_runtime_error = ""
            except Exception as exc:
                remediation_runtime = None
                remediation_runtime_error = str(exc)
                local_error = remediation_runtime_error
            try:
                graph_runtime = GraphRAGRuntime()
                graph_runtime_error = ""
            except Exception as exc:
                graph_runtime = None
                graph_runtime_error = str(exc)
                if not local_error:
                    local_error = graph_runtime_error
            return remediation_runtime is not None, local_error

        try:
            runtime = KafkaExpertRuntime(
                context_store=context_store if context_store.enabled else None,
                agent_role="monitor",
            )
            runtime_error = ""
        except Exception as exc:
            runtime = None
            runtime_error = str(exc)
            local_error = runtime_error

        try:
            graph_runtime = GraphRAGRuntime()
            graph_runtime_error = ""
        except Exception as exc:
            graph_runtime = None
            graph_runtime_error = str(exc)
            if not local_error:
                local_error = graph_runtime_error

        if runtime is not None:
            state.set_status(f"LLM set to {provider.upper()} ({_requested_model_for_provider(provider)})")
        else:
            state.set_status(
                "Kafka Monitor Agent is waiting for LLM provider configuration. Update LLM settings and retry.",
                error=local_error,
            )
        return runtime is not None, local_error


def a2a_json(payload: dict[str, Any], status: int = 200) -> Any:
    """Return an A2A media-type JSON response from Flask."""
    response = jsonify(payload)
    response.status_code = status
    response.headers["Content-Type"] = "application/a2a+json"
    response.headers["A2A-Version"] = A2A_PROTOCOL_VERSION
    return response


def remediation_task_payload(record: dict[str, Any]) -> dict[str, Any]:
    """Render one Doer task record as an A2A 1.0 Task response."""
    state_value = str(record.get("task_state", HITL_FAILED))
    status_messages = {
        HITL_INPUT_REQUIRED: "Human approval is required in the Kafka Remediation Agent UI.",
        HITL_WORKING: "The approved Kafka remediation task is running.",
        HITL_COMPLETED: "The Kafka remediation task completed.",
        HITL_REJECTED: str(record.get("error") or "The human operator rejected this remediation task."),
        HITL_FAILED: str(record.get("error") or "The Kafka remediation task failed."),
    }
    artifacts: list[dict[str, Any]] = []
    if record.get("a2a_message"):
        artifacts.append(
            {
                "artifactId": f"{record['task_id']}-request",
                "name": "A2A message from Kafka Monitor Agent",
                "parts": [{"text": str(record["a2a_message"])}],
            }
        )
    if record.get("plan"):
        artifacts.append(
            {
                "artifactId": f"{record['task_id']}-plan",
                "name": "Proposed Kafka remediation plan",
                "parts": [{"text": str(record["plan"])}],
            }
        )
    if record.get("result"):
        artifacts.append(
            {
                "artifactId": f"{record['task_id']}-result",
                "name": "Kafka remediation result",
                "parts": [{"text": str(record["result"])}],
            }
        )
    return {
        "task": {
            "id": str(record["task_id"]),
            "contextId": str(record.get("context_id", "")),
            "status": {
                "state": state_value,
                "message": {
                    "messageId": uuid.uuid4().hex,
                    "contextId": str(record.get("context_id", "")),
                    "taskId": str(record["task_id"]),
                    "role": "ROLE_AGENT",
                    "parts": [{"text": status_messages.get(state_value, state_value)}],
                },
                "timestamp": str(record.get("updated_at_utc", utc_now_iso())),
            },
            "artifacts": artifacts,
            "metadata": {
                "trace": dict(record.get("trace") or {}),
                "hitl": {
                    "enabled": bool(record.get("require_approval")),
                    "decision": str(record.get("decision", "")),
                    "decision_note": str(record.get("decision_note", "")),
                },
            },
        }
    }


def execute_remediation_task(task_id: str) -> None:
    """Execute one approved/automatic Doer task and persist its terminal state."""
    record = remediation_hitl.get(task_id)
    if record is None or remediation_runtime is None:
        return
    try:
        execution_prompt = str(record["a2a_message"])
        selected_fix = str(record.get("decision_note", "")).strip()
        if selected_fix:
            execution_prompt += (
                "\n\nHUMAN-SELECTED SUGGESTED FIX (use this as the preferred path, "
                "subject to live verification):\n" + selected_fix
            )
        answer, trace = remediation_runtime.ask_with_trace(
            execution_prompt,
            thread_id=f"a2a:{record['context_id']}:{task_id}",
            prompt_source="a2a_remediation",
        )
        remediation_hitl.complete(task_id, result=answer, trace=trace)
        state.set_last_trace(trace)
        state.set_status(answer)
        state.add_chat("remediation", answer)
        queue_deepeval_observation("remediation", execution_prompt, answer)
    except Exception as exc:
        error_text = f"1. [BAD] A2A task {task_id[:12]} failed. Fix: {exc}"
        remediation_hitl.fail(task_id, error=str(exc))
        state.set_status(error_text, error=str(exc))
        state.add_chat("remediation", error_text)


@app.get("/.well-known/agent-card.json")
def remediation_agent_card() -> Any:
    """Publish the Kafka doer agent's A2A 1.0 discovery document."""
    return jsonify(
        {
            "name": "Kafka Remediation Agent",
            "description": "Verifies and repairs an authorized Kafka cluster health finding.",
            "supportedInterfaces": [
                {
                    "url": A2A_REMEDIATION_PUBLIC_URL,
                    "protocolBinding": "HTTP+JSON",
                    "protocolVersion": A2A_PROTOCOL_VERSION,
                }
            ],
            "provider": {"organization": "Data-Blitz Inc"},
            "version": "1.0.0",
            "capabilities": {"streaming": False, "pushNotifications": False},
            "securitySchemes": {
                "bearer": {
                    "httpAuthSecurityScheme": {
                        "scheme": "bearer",
                        "description": "Internal monitor-to-remediation authorization token.",
                    }
                }
            },
            "securityRequirements": [{"schemes": {"bearer": {"list": []}}}],
            "defaultInputModes": ["text/plain"],
            "defaultOutputModes": ["text/plain"],
            "skills": [
                {
                    "id": "kafka-cluster-remediation",
                    "name": "Kafka Cluster Remediation",
                    "description": "Apply the smallest safe repair for one verified Kafka warning or bad finding.",
                    "tags": ["kafka", "operations", "remediation"],
                    "examples": ["Repair this under-replicated partition finding and verify the result."],
                    "inputModes": ["text/plain"],
                    "outputModes": ["text/plain"],
                }
            ],
        }
    )


@app.post("/a2a/remediation/message:send")
def a2a_remediation_send_message() -> Any:
    """Implement the A2A 1.0 synchronous Send Message operation for the doer agent."""
    requested_version = request.headers.get("A2A-Version", "0.3").strip()
    if requested_version != A2A_PROTOCOL_VERSION:
        return jsonify(
            {
                "type": "https://a2a-protocol.org/errors/version-not-supported",
                "title": "Protocol Version Not Supported",
                "status": 400,
                "detail": f"A2A protocol version {requested_version} is not supported.",
                "supportedVersions": [A2A_PROTOCOL_VERSION],
            }
        ), 400
    if request.headers.get("Authorization", "") != f"Bearer {A2A_REMEDIATION_TOKEN}":
        return jsonify({"title": "Unauthorized", "status": 401}), 401
    if remediation_runtime is None:
        return jsonify({"title": "Remediation Agent Unavailable", "status": 503}), 503

    payload = request.get_json(silent=True) or {}
    message = payload.get("message") if isinstance(payload, dict) else None
    if not isinstance(message, dict) or message.get("role") != "ROLE_USER":
        return jsonify({"title": "Invalid A2A message", "status": 400}), 400
    prompt = "\n".join(
        str(part.get("text", ""))
        for part in message.get("parts", [])
        if isinstance(part, dict) and part.get("text")
    ).strip()
    if not prompt:
        return jsonify({"title": "A text message part is required", "status": 400}), 400

    task_id = uuid.uuid4().hex
    context_id = str(message.get("contextId", "")).strip() or uuid.uuid4().hex
    message_id = str(message.get("messageId", "")).strip() or uuid.uuid4().hex
    task_label = f"A2A task {task_id[:12]} received (context {context_id[:20]})."
    state.add_chat("user", f"{task_label}\n{prompt}")
    require_approval = remediation_hitl.enabled()
    plan = ""
    plan_trace: dict[str, Any] = {}
    if require_approval:
        try:
            plan, plan_trace = remediation_runtime.plan_with_trace(prompt)
        except Exception as exc:
            human_review, full_context = build_review_context(
                protocol_version=A2A_PROTOCOL_VERSION,
                endpoint=f"{A2A_REMEDIATION_PUBLIC_URL}/message:send",
                task_id=task_id,
                context_id=context_id,
                message_id=message_id,
                a2a_message=prompt,
                plan="",
                planning_system_prompt=REMEDIATION_PLANNING_SYSTEM_PROMPT,
                execution_system_prompt=REMEDIATION_SYSTEM_PROMPT,
                tool_names=list(remediation_runtime._tools_by_name),
                require_approval=False,
            )
            record = remediation_hitl.create(
                task_id=task_id,
                context_id=context_id,
                message_id=message_id,
                a2a_message=prompt,
                plan="",
                require_approval=False,
                human_review=human_review,
                full_context=full_context,
            )
            remediation_hitl.fail(task_id, error=f"Could not generate remediation plan: {exc}")
            return a2a_json(remediation_task_payload(remediation_hitl.get(task_id) or record))
    human_review, full_context = build_review_context(
        protocol_version=A2A_PROTOCOL_VERSION,
        endpoint=f"{A2A_REMEDIATION_PUBLIC_URL}/message:send",
        task_id=task_id,
        context_id=context_id,
        message_id=message_id,
        a2a_message=prompt,
        plan=plan,
        planning_system_prompt=REMEDIATION_PLANNING_SYSTEM_PROMPT,
        execution_system_prompt=REMEDIATION_SYSTEM_PROMPT,
        tool_names=list(remediation_runtime._tools_by_name),
        require_approval=require_approval,
    )
    record = remediation_hitl.create(
        task_id=task_id,
        context_id=context_id,
        message_id=message_id,
        a2a_message=prompt,
        plan=plan,
        require_approval=require_approval,
        trace=plan_trace,
        human_review=human_review,
        full_context=full_context,
    )
    if require_approval:
        state.set_last_trace(plan_trace)
        state.set_status(
            f"1. [WARN] A2A task {task_id[:12]} is waiting for human approval in the Doer UI."
        )
        return a2a_json(remediation_task_payload(record))

    execute_remediation_task(task_id)
    return a2a_json(remediation_task_payload(remediation_hitl.get(task_id) or record))


@app.get("/a2a/remediation/tasks/<task_id>")
def a2a_remediation_get_task(task_id: str) -> Any:
    """Implement A2A Get Task for Monitor polling after HITL interruption."""
    if request.headers.get("A2A-Version", "") != A2A_PROTOCOL_VERSION:
        return jsonify({"title": "Protocol Version Not Supported", "status": 400}), 400
    if request.headers.get("Authorization", "") != f"Bearer {A2A_REMEDIATION_TOKEN}":
        return jsonify({"title": "Unauthorized", "status": 401}), 401
    record = remediation_hitl.get(task_id)
    if record is None:
        return jsonify({"title": "Task Not Found", "status": 404}), 404
    return a2a_json(remediation_task_payload(record))


@app.get("/api/remediation/hitl")
def remediation_hitl_get() -> Any:
    """Return the Doer's HITL mode and bounded task history for its UI."""
    if AGENT_PROCESS_ROLE != "remediation":
        return jsonify({"ok": False, "error": "HITL state is owned by the Doer runtime."}), 404
    snapshot = remediation_hitl.snapshot()
    return jsonify(
        {
            "ok": True,
            **snapshot,
            "pending_count": sum(
                1 for task in snapshot["tasks"] if task["task_state"] == HITL_INPUT_REQUIRED
            ),
        }
    )


@app.post("/api/remediation/hitl/config")
def remediation_hitl_config() -> Any:
    """Enable or disable human approval for future Doer A2A tasks."""
    if AGENT_PROCESS_ROLE != "remediation":
        return jsonify({"ok": False, "error": "HITL configuration is owned by the Doer runtime."}), 404
    payload = request.get_json(silent=True) or {}
    if "enabled" not in payload or not isinstance(payload["enabled"], bool):
        return jsonify({"ok": False, "error": "enabled must be a boolean"}), 400
    enabled = remediation_hitl.set_enabled(payload["enabled"])
    state.add_chat(
        "user",
        f"Human-in-the-Loop mode changed to {'ON' if enabled else 'OFF'} for future A2A tasks.",
    )
    return jsonify({"ok": True, "enabled": enabled})


@app.post("/api/remediation/hitl/tasks/<task_id>/decision")
def remediation_hitl_decision(task_id: str) -> Any:
    """Approve or reject one proposed Doer repair from the local HITL screen."""
    if AGENT_PROCESS_ROLE != "remediation":
        return jsonify({"ok": False, "error": "HITL decisions are owned by the Doer runtime."}), 404
    payload = request.get_json(silent=True) or {}
    decision = str(payload.get("decision", "")).strip().lower()
    note = str(payload.get("note", "")).strip()
    try:
        record = remediation_hitl.decide(task_id, decision=decision, note=note)
    except KeyError:
        return jsonify({"ok": False, "error": "HITL task not found"}), 404
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    if decision == "approve":
        state.add_chat("user", f"Human approved A2A task {task_id[:12]}. {note}".strip())
        threading.Thread(
            target=execute_remediation_task,
            args=(task_id,),
            name=f"hitl-remediation-{task_id[:8]}",
            daemon=True,
        ).start()
    else:
        rejection_text = note or "Rejected by human operator."
        state.set_status(
            f"1. [WARN] A2A task {task_id[:12]} was rejected by the human operator: {rejection_text}"
        )
        state.add_chat("user", f"Human rejected A2A task {task_id[:12]}: {rejection_text}")
    return jsonify({"ok": True, "task": record})


def remote_remediation_health() -> tuple[dict[str, Any], str]:
    """Read health from the independent remediation process without sharing runtime state."""
    if AGENT_PROCESS_ROLE != "monitor":
        return {}, ""
    try:
        req = urllib.request.Request(
            A2A_REMEDIATION_HEALTH_URL,
            method="GET",
            headers={"Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=3.0) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return payload if isinstance(payload, dict) else {}, ""
    except Exception as exc:
        return {}, str(exc)


@app.get("/api/health")
def health() -> Any:
    """Expose runtime health and model configuration visibility for diagnostics."""
    provider = _normalize_llm_provider(LLM_PROVIDER)
    graph_rt, graph_err = ensure_graph_runtime()
    remote_health, remote_health_error = remote_remediation_health()
    monitor_ok = runtime is not None if AGENT_PROCESS_ROLE == "monitor" else False
    remediation_ok = (
        bool(remote_health.get("remediation_agent_ok"))
        if AGENT_PROCESS_ROLE == "monitor"
        else remediation_runtime is not None
    )
    remediation_model = (
        str(remote_health.get("remediation_agent_model", ""))
        if AGENT_PROCESS_ROLE == "monitor"
        else (getattr(remediation_runtime, "model_name", "") if remediation_runtime is not None else "")
    )
    requested_model = _requested_model_for_provider(provider)
    effective_model = requested_model
    if runtime is not None and getattr(runtime, "model_name", ""):
        effective_model = runtime.model_name
    elif remediation_runtime is not None and getattr(remediation_runtime, "model_name", ""):
        effective_model = remediation_runtime.model_name
    elif graph_rt is not None and getattr(graph_rt, "model_name", ""):
        effective_model = graph_rt.model_name
    fallback_models = _model_candidates_for_provider(provider)
    hitl_snapshot = remediation_hitl.snapshot() if AGENT_PROCESS_ROLE == "remediation" else {}
    return jsonify(
        {
            "ok": monitor_ok if AGENT_PROCESS_ROLE == "monitor" else remediation_ok,
            "agent_process_role": AGENT_PROCESS_ROLE,
            "server_port": SERVER_PORT,
            "monitor_agent_ok": monitor_ok,
            "monitor_agent_model": getattr(runtime, "model_name", "") if runtime is not None else "",
            "remediation_agent_ok": remediation_ok,
            "remediation_agent_model": remediation_model,
            "remediation_runtime_location": "remote-a2a" if AGENT_PROCESS_ROLE == "monitor" else "local",
            "a2a_protocol_version": A2A_PROTOCOL_VERSION,
            "a2a_remediation_url": A2A_REMEDIATION_PUBLIC_URL,
            "remediation_hitl_enabled": hitl_snapshot.get("enabled"),
            "remediation_hitl_pending_count": sum(
                1
                for task in hitl_snapshot.get("tasks", [])
                if task.get("task_state") == HITL_INPUT_REQUIRED
            ),
            "graph_rag_ok": graph_rt is not None,
            "llm_provider": provider,
            "llm_model": effective_model,
            "llm_model_requested": requested_model,
            "llm_model_fallbacks": fallback_models,
            "llm_api_key_configured": bool(_llm_api_key_for_provider(provider)),
            "llm_base_url_configured": bool(_llm_base_url_for_provider(provider)),
            "llm_base_url": _llm_base_url_for_provider(provider),
            "openai_model": effective_model,
            "openai_model_requested": requested_model,
            "openai_model_fallbacks": fallback_models,
            "openai_api_key_configured": bool(_llm_api_key_for_provider(provider)),
            "openai_base_url_configured": bool(_llm_base_url_for_provider(provider)),
            "openai_base_url": _llm_base_url_for_provider(provider),
            "prometheus_url": PROMETHEUS_URL,
            "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "schema_registry_url": SCHEMA_REGISTRY_URL,
            "neo4j_uri": NEO4J_URI,
            "neo4j_auth_mode": "none" if NEO4J_AUTH_DISABLED else "password",
            "neo4j_browser_public_url": NEO4J_BROWSER_PUBLIC_URL,
            "runtime_error": runtime_error,
            "monitor_runtime_error": runtime_error,
            "remediation_runtime_error": remediation_runtime_error or remote_health_error,
            "graph_runtime_error": graph_err,
            "context_persist_enabled": context_store.enabled,
            "context_db": context_store.db_name,
            "context_store_error": context_store.last_error,
            "state_context_persist_error": state.context_persist_error,
            "ai_token_budget": AI_TOKEN_BUDGET,
        }
    )


@app.get("/api/llm/config")
def llm_config_get() -> Any:
    """Return current LLM configuration for the UI control."""
    cfg = _current_llm_config()
    cfg["ok"] = runtime is not None if AGENT_PROCESS_ROLE == "monitor" else remediation_runtime is not None
    cfg["runtime_error"] = runtime_error if AGENT_PROCESS_ROLE == "monitor" else remediation_runtime_error
    return jsonify(cfg)


@app.post("/api/llm/config")
def llm_config_post() -> Any:
    """Update LLM provider/model settings at runtime."""
    payload = request.get_json(silent=True) or {}
    provider = payload.get("provider", LLM_PROVIDER)
    model = payload.get("model")
    fallbacks_raw = payload.get("fallbacks")
    fallbacks: list[str] | None = None
    if isinstance(fallbacks_raw, str):
        fallbacks = _split_csv(fallbacks_raw)
    elif isinstance(fallbacks_raw, list):
        fallbacks = _dedupe_models([str(item) for item in fallbacks_raw])
    base_url = payload.get("base_url")
    api_key = payload.get("api_key")
    temperature = payload.get("temperature")
    ok, err = _apply_llm_config(
        provider=provider,
        model=model,
        fallbacks=fallbacks,
        base_url=base_url,
        api_key=api_key,
        temperature=temperature,
    )
    status = 200 if ok else 400
    cfg = _current_llm_config()
    cfg.update({"ok": ok, "message": f"LLM set to {cfg['provider'].upper()} ({cfg['model'] or 'n/a'})"})
    if not ok:
        cfg["error"] = err
    return jsonify(cfg), status


@app.get("/api/finetuning/status")
def finetuning_status() -> Any:
    """Report MLX availability for the environment serving this UI."""
    status = mlx_runtime_status()
    status["ok"] = True
    status["host_execution_note"] = (
        "Training runs on the Mac host; Docker only prepares the command."
    )
    status["default_data_directory"] = MLX_TRAINING_DATA_DIR
    return jsonify(status)


@app.post("/api/finetuning/validate")
def finetuning_validate() -> Any:
    """Validate an absolute host or project-relative MLX dataset directory."""
    payload = request.get_json(silent=True) or {}
    directory = str(payload.get("data", "")).strip()
    if not directory or directory.startswith("~") or directory.lower().endswith(".jsonl"):
        return jsonify({"ok": False, "error": "Enter an absolute or project-relative dataset directory, not an individual JSONL file. Expand '~' first."}), 400
    required = ["train.jsonl", "valid.jsonl", "test.jsonl"]
    local_path = Path(directory)
    present = [name for name in required if (local_path / name).is_file()]
    missing = [name for name in required if name not in present]
    return jsonify({"ok": True, "directory": directory, "required_files": required, "present_files": present, "missing_files": missing, "valid": not missing, "host_required": not local_path.exists()})


@app.post("/api/finetuning/prepare")
def finetuning_prepare() -> Any:
    """Validate fine-tuning settings and return a host-side MLX-LM command."""
    payload = request.get_json(silent=True) or {}
    try:
        iterations = int(payload.get("iterations", 600))
        rank = int(payload.get("rank", 8))
        command = build_mlx_lora_command(
            payload.get("model", ""),
            payload.get("data", ""),
            payload.get("output", ""),
            iterations=iterations,
            rank=rank,
        )
    except (TypeError, ValueError) as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    status = mlx_runtime_status()
    return jsonify(
        {
            "ok": True,
            "command": command,
            **status,
            "estimate": estimate_mlx_duration(payload.get("model", ""), iterations, rank),
            "message": (
                "Command prepared for the Mac host. Install mlx and mlx-lm there before running it."
                if not status["mlx_lm_available"]
                else "Command prepared; run it from the Mac host with the selected dataset."
            ),
        }
    )


@app.post("/api/finetuning/run")
def finetuning_run() -> Any:
    """Launch MLX-LM in the current host environment when available."""
    payload = request.get_json(silent=True) or {}
    try:
        iterations = int(payload.get("iterations", 600))
        rank = int(payload.get("rank", 8))
        args = build_mlx_lora_args(payload.get("model", ""), payload.get("data", ""), payload.get("output", ""), iterations, rank)
    except (TypeError, ValueError) as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    status = mlx_runtime_status()
    if not status["mlx_lm_available"]:
        return jsonify({
            "ok": True,
            "status": "host_required",
            "command": " ".join(args),
            "message": "Docker cannot execute MLX. Run this command from the Mac host.",
            **status,
        })
    job_id = uuid.uuid4().hex
    try:
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    except OSError as exc:
        return jsonify({"ok": False, "error": f"Could not start MLX-LM: {exc}"}), 500
    FINETUNING_JOBS[job_id] = {"job_id": job_id, "status": "running", "pid": process.pid, "command": " ".join(args), "progress": 0, "total": iterations}

    def collect() -> None:
        lines = []
        for line in process.stdout or ():
            lines.append(line)
            match = re.search(r"(?:iter(?:ation)?|step)\s*[:=]?\s*(\d+)", line, re.IGNORECASE)
            if match:
                FINETUNING_JOBS[job_id]["progress"] = min(iterations, int(match.group(1)))
        process.wait()
        FINETUNING_JOBS[job_id].update({"status": "completed" if process.returncode == 0 else "failed", "returncode": process.returncode, "output": "".join(lines)[-12000:]})

    threading.Thread(target=collect, daemon=True).start()
    return jsonify({"ok": True, "job_id": job_id, "status": "running", "command": " ".join(args)})


@app.get("/api/finetuning/jobs/<job_id>")
def finetuning_job(job_id: str) -> Any:
    job = FINETUNING_JOBS.get(job_id)
    if not job:
        return jsonify({"ok": False, "error": "Fine-tuning job not found"}), 404
    return jsonify({"ok": True, **job})


@app.get("/api/llm/models")
def llm_models() -> Any:
    """List models for Ollama or the configured Hugging Face endpoint."""
    provider = _normalize_llm_provider(request.args.get("provider") or LLM_PROVIDER)
    try:
        if provider == "ollama":
            models, source = _fetch_ollama_models_from_directory()
        elif provider == "huggingface":
            models, source = _fetch_huggingface_models()
        else:
            return jsonify({"ok": False, "error": "Model listing is supported for Ollama and Hugging Face."}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    if not models:
        checked = (
            ", ".join(str(path / "manifests") for path in _ollama_model_dirs())
            if provider == "ollama"
            else _llm_base_url_for_provider("huggingface")
        )
        return jsonify(
            {
                "ok": False,
                "error": (
                    f"No {provider} models were returned. "
                    f"Checked: {checked}. "
                    "Check the endpoint, credentials, and model availability."
                ),
            }
        ), 404
    return jsonify({"ok": True, "models": models, "source": source})


@app.get("/api/llm/assignments")
def llm_assignments_get() -> Any:
    """Show the actual provider/model assignment for each independent LLM runtime."""
    if AGENT_PROCESS_ROLE != "monitor":
        return jsonify({"ok": False, "error": "Assignments are exposed by the Monitor UI runtime."}), 404
    assignments: dict[str, Any] = {"monitor": _current_llm_config()}
    errors: dict[str, str] = {}
    try:
        assignments["remediation"] = llm_service_request(remediation_llm_config_url(), timeout_seconds=8.0)
    except Exception as exc:
        errors["remediation"] = str(exc)
        assignments["remediation"] = {"target": "remediation", "ok": False, "error": str(exc)}
    try:
        assignments["judge"] = deepeval_request("/api/config", timeout_seconds=8.0)
    except Exception as exc:
        errors["judge"] = str(exc)
        assignments["judge"] = {"target": "judge", "ok": False, "error": str(exc)}
    return jsonify({"ok": not errors, "assignments": assignments, "errors": errors})


@app.post("/api/llm/assignments/<target>")
def llm_assignment_post(target: str) -> Any:
    """Configure Monitor, Doer, or Judge without sharing their runtime objects."""
    if AGENT_PROCESS_ROLE != "monitor":
        return jsonify({"ok": False, "error": "Assignments are controlled by the Monitor UI runtime."}), 404
    target = target.strip().lower()
    payload = request.get_json(silent=True) or {}
    if target == "remediation":
        try:
            result = llm_service_request(
                remediation_llm_config_url(), method="POST", payload=payload, timeout_seconds=240.0
            )
            return jsonify(result), 200 if result.get("ok") else 400
        except Exception as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
    if target == "judge":
        try:
            result = deepeval_request("/api/config", method="POST", payload=payload, timeout_seconds=240.0)
            return jsonify(result), 200 if result.get("ok") else 400
        except Exception as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
    if target != "monitor":
        return jsonify({"ok": False, "error": "target must be monitor, remediation, or judge"}), 400

    fallbacks_raw = payload.get("fallbacks")
    fallbacks = _split_csv(fallbacks_raw) if isinstance(fallbacks_raw, str) else (
        _dedupe_models([str(item) for item in fallbacks_raw]) if isinstance(fallbacks_raw, list) else None
    )
    ok, err = _apply_llm_config(
        provider=str(payload.get("provider", LLM_PROVIDER)),
        model=payload.get("model"),
        fallbacks=fallbacks,
        base_url=payload.get("base_url"),
        api_key=payload.get("api_key"),
        temperature=payload.get("temperature"),
    )
    cfg = _current_llm_config()
    cfg.update({"ok": ok, "message": f"Monitor LLM set to {cfg['provider'].upper()} ({cfg['effective_model'] or cfg['model']})"})
    if not ok:
        cfg["error"] = err
    return jsonify(cfg), 200 if ok else 400


@app.get("/api/state")
def api_state() -> Any:
    """Return current status text, errors, and chat history snapshot."""
    return jsonify(state.snapshot())


@app.get("/api/observability")
def api_observability() -> Any:
    """Return the complete agent harness view for either Monitor or Doer.

    This intentionally combines runtime identity/configuration, available tools,
    latest structured trace, persisted conversation, Graph RAG health, HITL/A2A
    state, and DeepEval history behind one role-local endpoint.
    """
    local_runtime = runtime if AGENT_PROCESS_ROLE == "monitor" else remediation_runtime
    graph_rt, graph_err = ensure_graph_runtime()
    graph_status: dict[str, Any] = {}
    if graph_rt is not None:
        try:
            graph_status = graph_rt.status()
        except Exception as exc:
            graph_status = {"error": str(exc)}
    judge_status: dict[str, Any] = {}
    try:
        judge_status = {
            "health": deepeval_request("/health", timeout_seconds=5.0),
            "runs": deepeval_request("/api/results", timeout_seconds=5.0).get("runs", []),
        }
    except Exception as exc:
        judge_status = {"error": str(exc)}
    return jsonify(
        {
            "ok": True,
            "agent_process_role": AGENT_PROCESS_ROLE,
            "runtime": {
                "available": local_runtime is not None,
                "provider": _normalize_llm_provider(LLM_PROVIDER),
                "model": getattr(local_runtime, "model_name", "") if local_runtime else "",
                "system_prompt_file": getattr(local_runtime, "system_prompt_file", "") if local_runtime else "",
                "system_prompt_chars": len(getattr(local_runtime, "system_prompt", "")) if local_runtime else 0,
                "available_tools": sorted(getattr(local_runtime, "_tools_by_name", {}).keys()) if local_runtime else [],
                "runtime_error": runtime_error if AGENT_PROCESS_ROLE == "monitor" else remediation_runtime_error,
            },
            "state": state.snapshot(),
            "graph_rag": {"ok": graph_rt is not None, "error": graph_err, "status": graph_status},
            "hitl": remediation_hitl.snapshot() if AGENT_PROCESS_ROLE == "remediation" else None,
            "deepeval": judge_status,
        }
    )


def latest_deepeval_cases(scope: str) -> list[dict[str, str]]:
    """Build UI-requested DeepEval cases from the latest persisted outputs for each agent."""
    snapshot = state.snapshot()
    cases: list[dict[str, str]] = []
    if scope in {"monitor", "both"}:
        monitor_output = str(snapshot.get("status_text", "")).strip()
        if monitor_output:
            cases.append(
                {
                    "agent_role": "monitor",
                    "input": CLUSTER_STATE_PROMPT,
                    "actual_output": monitor_output,
                }
            )
    if scope in {"remediation", "both"}:
        chat = snapshot.get("chat", [])
        remediation_index = -1
        for index in range(len(chat) - 1, -1, -1):
            if isinstance(chat[index], dict) and chat[index].get("role") == "remediation":
                remediation_index = index
                break
        if remediation_index >= 0:
            remediation_output = str(chat[remediation_index].get("text", "")).strip()
            remediation_input = "Remediate the latest Kafka warning or bad finding."
            for index in range(remediation_index - 1, -1, -1):
                item = chat[index]
                if isinstance(item, dict) and item.get("role") == "user":
                    remediation_input = str(item.get("text", remediation_input)).strip()
                    break
            if remediation_output:
                cases.append(
                    {
                        "agent_role": "remediation",
                        "input": remediation_input,
                        "actual_output": remediation_output,
                    }
                )
    return cases


@app.get("/api/deepeval/status")
def api_deepeval_status() -> Any:
    """Return local judge health and recent evaluation runs for the UI."""
    try:
        health_payload = deepeval_request("/health", timeout_seconds=5.0)
        results_payload = deepeval_request("/api/results", timeout_seconds=5.0)
        return jsonify(
            {
                "ok": True,
                "health": health_payload,
                "runs": results_payload.get("runs", []),
                "auto_evaluate": DEEPEVAL_AUTO_EVALUATE,
            }
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "auto_evaluate": DEEPEVAL_AUTO_EVALUATE}), 503


@app.post("/api/deepeval/evaluate")
def api_deepeval_evaluate() -> Any:
    """Run a manual DeepEval judgement for the UI-selected agent scope."""
    payload = request.get_json(silent=True) or {}
    scope = str(payload.get("scope", "both")).strip().lower()
    if scope not in {"monitor", "remediation", "both"}:
        return jsonify({"ok": False, "error": "scope must be monitor, remediation, or both"}), 400
    cases = latest_deepeval_cases(scope)
    expected_roles = {scope} if scope != "both" else {"monitor", "remediation"}
    available_roles = {case["agent_role"] for case in cases}
    missing = sorted(expected_roles - available_roles)
    if missing:
        return jsonify({"ok": False, "error": f"No current output is available for: {', '.join(missing)}"}), 400
    try:
        result = deepeval_request(
            "/api/evaluate",
            method="POST",
            payload={"scope": scope, "threshold": payload.get("threshold", 0.7), "cases": cases},
        )
        return jsonify(result), 200 if result.get("ok") else 500
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


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


@app.get("/api/graphrag/metrics")
def api_graphrag_metrics() -> Any:
    """Return persisted Graph RAG metrics used by the dedicated dashboard tab."""
    graph_rt, graph_err = ensure_graph_runtime()
    graph_status: dict[str, Any] = {}
    if graph_rt is not None:
        try:
            graph_status = graph_rt.status()
        except Exception as exc:
            graph_status = {"error": str(exc)}
    return jsonify(
        {
            "ok": True,
            "metrics": state.graph_metrics_snapshot(),
            "graph_runtime_ok": graph_rt is not None,
            "graph_runtime_error": graph_err if graph_rt is None else "",
            "graph_status": graph_status,
        }
    )


@app.post("/api/graphrag/metrics/reset")
def api_graphrag_metrics_reset() -> Any:
    """Reset Graph RAG metrics counters/history while keeping graph data intact."""
    state.reset_graph_metrics()
    return jsonify({"ok": True, "metrics": state.graph_metrics_snapshot()})


@app.post("/api/graphrag/ingest_pdf")
def api_graphrag_ingest_pdf() -> Any:
    """Queue one uploaded PDF for asynchronous Neo4j ontology extraction."""
    graph_rt, graph_err = ensure_graph_runtime()
    if graph_rt is None:
        return jsonify({"ok": False, "error": graph_err or "Graph RAG runtime unavailable."}), 500

    uploaded = request.files.get("pdf")
    if uploaded is None:
        return jsonify({"ok": False, "error": "pdf file is required (multipart field name: pdf)"}), 400

    filename = (uploaded.filename or "uploaded.pdf").strip() or "uploaded.pdf"
    if not filename.lower().endswith(".pdf"):
        return jsonify({"ok": False, "error": "Only PDF files are supported."}), 400

    payload = uploaded.read()
    if not payload:
        return jsonify({"ok": False, "error": "PDF payload is empty."}), 400
    job_id = uuid.uuid4().hex
    with GRAPH_INGEST_JOBS_LOCK:
        GRAPH_INGEST_JOBS[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "source_file": filename,
            "progress": 0,
            "phase": "queued",
            "chunks_processed": 0,
            "cancel_requested": False,
            "_started_at_epoch": time.time(),
        }

    def run_ingest() -> None:
        started = time.time()
        with GRAPH_INGEST_JOBS_LOCK:
            GRAPH_INGEST_JOBS[job_id].update({"status": "running", "phase": "extracting_pdf"})
        try:
            def progress(done: int, total: int, **details: Any) -> None:
                with GRAPH_INGEST_JOBS_LOCK:
                    job = GRAPH_INGEST_JOBS[job_id]
                    current_chunk = int(details.get("current_chunk", 0) or 0)
                    if current_chunk and current_chunk != int(job.get("current_chunk", 0) or 0):
                        job["_chunk_started_at_epoch"] = time.time()
                    job.update(
                        {
                            "progress": round((done / total) * 100) if total else 0,
                            "chunks_processed": done,
                            "total_chunks": total,
                            **details,
                        }
                    )

            def should_cancel() -> bool:
                with GRAPH_INGEST_JOBS_LOCK:
                    return bool(GRAPH_INGEST_JOBS.get(job_id, {}).get("cancel_requested"))

            result = graph_rt.ingest_pdf(
                filename=filename,
                pdf_bytes=payload,
                progress_callback=progress,
                should_cancel=should_cancel,
            )
            duration_ms = int((time.time() - started) * 1000)
            state.record_graph_ingest(ok=True, duration_ms=duration_ms, source_file=str(result.get("source_file", filename)), chunks_processed=int(result.get("chunks_processed", 0) or 0), edges_created=int(result.get("edges_created", 0) or 0))
            answer = (
                f"1. [GOOD] PDF `{result['source_file']}` ingested into Neo4j Graph RAG.\n"
                f"2. [GOOD] Chunks processed: {result['chunks_processed']}.\n"
                f"3. [GOOD] Edges extracted and merged: {result['edges_created']}.\n"
                f"4. [GOOD] Extraction mode: {result['extraction_mode']} "
                f"({result['fallback_chunks']} fallback chunks).\n"
                "5. [GOOD] Next step: ask a Kafka question in the Graph RAG tab."
            )
            with GRAPH_INGEST_JOBS_LOCK:
                GRAPH_INGEST_JOBS[job_id].update({"status": "completed", "phase": "completed", "progress": 100, "duration_ms": duration_ms, "answer": answer, **result})
        except GraphIngestCanceled as exc:
            duration_ms = int((time.time() - started) * 1000)
            with GRAPH_INGEST_JOBS_LOCK:
                GRAPH_INGEST_JOBS[job_id].update(
                    {"status": "canceled", "phase": "canceled", "duration_ms": duration_ms, "error": str(exc)}
                )
        except Exception as exc:
            duration_ms = int((time.time() - started) * 1000)
            state.record_graph_ingest(ok=False, duration_ms=duration_ms, source_file=filename, error=str(exc))
            with GRAPH_INGEST_JOBS_LOCK:
                GRAPH_INGEST_JOBS[job_id].update({"status": "failed", "phase": "failed", "duration_ms": duration_ms, "error": str(exc)})

    threading.Thread(target=run_ingest, name=f"graphrag-ingest-{job_id[:8]}", daemon=True).start()
    return jsonify({"ok": True, "pending": True, "job_id": job_id, "source_file": filename}), 202


@app.get("/api/graphrag/ingest_pdf/status/<job_id>")
def api_graphrag_ingest_status(job_id: str) -> Any:
    """Return asynchronous PDF ontology extraction progress/result."""
    with GRAPH_INGEST_JOBS_LOCK:
        job = dict(GRAPH_INGEST_JOBS.get(job_id, {}))
    if not job:
        return jsonify({"ok": False, "error": "Graph ingest job not found."}), 404
    now = time.time()
    started_at = float(job.pop("_started_at_epoch", now) or now)
    chunk_started_at = float(job.pop("_chunk_started_at_epoch", now) or now)
    job["elapsed_ms"] = max(0, int((now - started_at) * 1000))
    job["current_chunk_elapsed_ms"] = max(0, int((now - chunk_started_at) * 1000))
    completed = int(job.get("chunks_processed", 0) or 0)
    total = int(job.get("total_chunks", 0) or 0)
    job["estimated_remaining_ms"] = (
        max(0, int((job["elapsed_ms"] / completed) * (total - completed)))
        if completed > 0 and total > completed
        else None
    )
    return jsonify({"ok": True, **job})


@app.post("/api/graphrag/ingest_pdf/cancel/<job_id>")
def api_graphrag_ingest_cancel(job_id: str) -> Any:
    """Request cooperative cancellation; active LLM calls stop at the configured timeout."""
    with GRAPH_INGEST_JOBS_LOCK:
        job = GRAPH_INGEST_JOBS.get(job_id)
        if not job:
            return jsonify({"ok": False, "error": "Graph ingest job not found."}), 404
        if job.get("status") in {"completed", "failed", "canceled"}:
            return jsonify({"ok": False, "error": f"Graph ingest job is already {job.get('status')}."}), 409
        job.update({"cancel_requested": True, "phase": "cancel_requested"})
    return jsonify(
        {
            "ok": True,
            "job_id": job_id,
            "status": "cancel_requested",
            "message": (
                "Cancellation requested. The current LLM chunk may run until its timeout, "
                "then no additional chunks will be processed."
            ),
        }
    )


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

    started = time.time()
    try:
        answer, trace = graph_rt.query_with_trace(question)
        state.record_graph_query(
            ok=True,
            question=question,
            duration_ms=int(trace.get("duration_ms", 0) or 0),
            token_usage=trace.get("token_usage", {}),
            rag_content=trace.get("rag_content", {}),
            error="",
        )
        return jsonify({"ok": True, "answer": answer, "trace": trace})
    except Exception as exc:
        state.record_graph_query(
            ok=False,
            question=question,
            duration_ms=int((time.time() - started) * 1000),
            token_usage={},
            rag_content={},
            error=str(exc),
        )
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.post("/api/cluster_state")
def api_cluster_state() -> Any:
    """Start a full cluster-state assessment without blocking the browser request."""
    global cluster_state_running
    if runtime is None:
        err = runtime_error or "Kafka Expert runtime is not available."
        trace = build_static_trace(
            prompt_source="cluster_state",
            user_prompt=CLUSTER_STATE_PROMPT,
            reason=f"runtime-unavailable: {err}",
        )
        state.set_last_trace(trace, epoch=chat_epoch)
        state.set_status(
            "Kafka Expert could not run cluster assessment because runtime initialization failed.",
            error=err,
        )
        return jsonify({"ok": False, "trace": trace, **state.snapshot()}), 500
    with cluster_state_lock:
        if cluster_state_running:
            return jsonify({"ok": True, "pending": True, "message": "Cluster-state assessment is already running."}), 202
        cluster_state_running = True
    state.set_status("Collecting verified full-cluster state...", error="")

    def run_assessment() -> None:
        global cluster_state_running
        try:
            answer, trace = runtime.cluster_state_with_trace()
            state.set_last_trace(trace)
            state.set_status(answer, error="")
            state.add_chat("agent", answer)
            queue_deepeval_observation("monitor", CLUSTER_STATE_PROMPT, answer)
        except Exception as exc:
            err = str(exc)
            trace = build_static_trace(
                prompt_source="cluster_state",
                user_prompt=CLUSTER_STATE_PROMPT,
                reason=f"runtime-error: {err}",
            )
            state.set_last_trace(trace)
            state.set_status("Cluster-state assessment failed.", error=err)
        finally:
            with cluster_state_lock:
                cluster_state_running = False

    threading.Thread(target=run_assessment, name="cluster-state-assessment", daemon=True).start()
    return jsonify({"ok": True, "pending": True, "status": state.snapshot()}), 202


@app.get("/api/cluster_state/status")
def api_cluster_state_status() -> Any:
    """Return background full-cluster assessment progress and latest error."""
    with cluster_state_lock:
        running = cluster_state_running
    snapshot = state.snapshot()
    return jsonify({"ok": True, "running": running, "error": snapshot.get("last_error", "")})


@app.post("/api/chat")
def api_chat() -> Any:
    """Answer one Kafka-focused chat question with guardrails and tool reasoning."""
    payload = request.get_json(silent=True) or {}
    message = str(payload.get("message", "")).strip()
    if not message:
        return jsonify({"ok": False, "error": "message is required"}), 400

    chat_epoch = state.current_chat_epoch()
    thread_id = state.current_thread_id()
    state.add_chat("user", message, epoch=chat_epoch)

    if not is_kafka_question(message):
        trace = build_static_trace(
            prompt_source="chat_guardrail",
            user_prompt=message,
            reason="non-kafka question refused by policy",
        )
        state.set_last_trace(trace, epoch=chat_epoch)
        state.add_chat("agent", KAFKA_ONLY_MESSAGE, epoch=chat_epoch)
        return jsonify({"ok": True, "answer": KAFKA_ONLY_MESSAGE, "trace": trace, **state.snapshot()})

    if is_schema_registry_question(message):
        try:
            answer = build_schema_registry_answer()
            trace = build_static_trace(
                prompt_source="chat_schema_registry",
                user_prompt=message,
                reason="deterministic schema registry inventory path",
            )
            state.set_last_trace(trace, epoch=chat_epoch)
            state.add_chat("agent", answer, epoch=chat_epoch)
            return jsonify({"ok": True, "answer": answer, "trace": trace, **state.snapshot()})
        except Exception as exc:
            err = str(exc)
            trace = build_static_trace(
                prompt_source="chat_schema_registry",
                user_prompt=message,
                reason=f"deterministic error: {err}",
            )
            state.set_last_trace(trace, epoch=chat_epoch)
            state.add_chat("agent", f"Error: {err}", epoch=chat_epoch)
            return jsonify({"ok": False, "error": err, "trace": trace, **state.snapshot()}), 500

    if is_cluster_inventory_question(message):
        try:
            answer = build_cluster_inventory_answer()
            trace = build_static_trace(
                prompt_source="chat_cluster_inventory",
                user_prompt=message,
                reason="deterministic cluster inventory path",
            )
            state.set_last_trace(trace, epoch=chat_epoch)
            state.add_chat("agent", answer, epoch=chat_epoch)
            return jsonify({"ok": True, "answer": answer, "trace": trace, **state.snapshot()})
        except Exception as exc:
            err = str(exc)
            trace = build_static_trace(
                prompt_source="chat_cluster_inventory",
                user_prompt=message,
                reason=f"deterministic error: {err}",
            )
            state.set_last_trace(trace, epoch=chat_epoch)
            state.add_chat("agent", f"Error: {err}", epoch=chat_epoch)
            return jsonify({"ok": False, "error": err, "trace": trace, **state.snapshot()}), 500

    if is_topic_inventory_question(message):
        try:
            answer = build_topic_inventory_answer()
            trace = build_static_trace(
                prompt_source="chat_topic_inventory",
                user_prompt=message,
                reason="deterministic topic inventory path",
            )
            state.set_last_trace(trace, epoch=chat_epoch)
            state.add_chat("agent", answer, epoch=chat_epoch)
            return jsonify({"ok": True, "answer": answer, "trace": trace, **state.snapshot()})
        except Exception as exc:
            err = str(exc)
            trace = build_static_trace(
                prompt_source="chat_topic_inventory",
                user_prompt=message,
                reason=f"deterministic error: {err}",
            )
            state.set_last_trace(trace, epoch=chat_epoch)
            state.add_chat("agent", f"Error: {err}", epoch=chat_epoch)
            return jsonify({"ok": False, "error": err, "trace": trace, **state.snapshot()}), 500

    if runtime is None:
        err = runtime_error or "Kafka Expert runtime is not available."
        trace = build_static_trace(
            prompt_source="chat",
            user_prompt=message,
            reason=f"runtime-unavailable: {err}",
        )
        state.set_last_trace(trace, epoch=chat_epoch)
        state.add_chat("agent", f"Kafka Expert is unavailable: {err}", epoch=chat_epoch)
        return jsonify({"ok": False, "error": err, "trace": trace, **state.snapshot()}), 500

    try:
        answer, trace = runtime.ask_with_trace(
            message,
            thread_id=thread_id,
            prompt_source="chat",
        )
        state.set_last_trace(trace, epoch=chat_epoch)
        state.add_chat("agent", answer, epoch=chat_epoch)
        queue_deepeval_observation("monitor", message, answer)
        return jsonify({"ok": True, "answer": answer, "trace": trace, **state.snapshot()})
    except Exception as exc:
        err = str(exc)
        trace = build_static_trace(
            prompt_source="chat",
            user_prompt=message,
            reason=f"runtime-error: {err}",
        )
        state.set_last_trace(trace, epoch=chat_epoch)
        state.add_chat("agent", f"Error: {err}", epoch=chat_epoch)
        return jsonify({"ok": False, "error": err, "trace": trace, **state.snapshot()}), 500


@app.post("/api/auto_fix")
def api_auto_fix() -> Any:
    """Hand one current monitor finding to the isolated remediation LLM agent."""
    payload = request.get_json(silent=True) or {}
    issue_line = str(payload.get("issue_line", "")).strip()
    if not issue_line:
        return jsonify({"ok": False, "error": "issue_line is required"}), 400

    if not is_current_actionable_issue(issue_line, state.snapshot()):
        return jsonify(
            {
                "ok": False,
                "error": "The fix must be a current [WARN] or [BAD] finding produced by Kafka Monitor Agent.",
            }
        ), 400

    chat_epoch = state.current_chat_epoch()
    thread_id = state.current_thread_id()
    state.add_chat("user", f"Fix handoff to Kafka Remediation Agent: {issue_line}", epoch=chat_epoch)

    try:
        snapshot = state.snapshot()
        handoff = build_monitor_handoff(issue_line, snapshot)
        prompt = Template(AUTO_FIX_PROMPT_TEMPLATE).safe_substitute(
            issue_line=handoff["issue_line"],
            monitor_reason=handoff["monitor_reason"],
            monitor_context=handoff["monitor_context"],
        )
        a2a_context_id = f"{thread_id}-remediation-{uuid.uuid4().hex}"
        answer, trace, a2a_task_id = send_a2a_remediation_message(
            prompt,
            context_id=a2a_context_id,
        )
        task_state = str((trace.get("a2a") or {}).get("task_state", ""))
        # Keep the extracted diagnosis visible to the Monitor UI even when the
        # Doer is waiting for HITL approval or returns only a plan artifact.
        trace["monitor_handoff"] = handoff
        state.set_last_trace(trace, epoch=chat_epoch)
        if task_state == HITL_INPUT_REQUIRED:
            with completed_a2a_tasks_lock:
                pending_a2a_prompts[a2a_task_id] = prompt
            state.add_chat(
                "remediation",
                f"Human approval required in the Doer UI.\n\nProposed plan:\n{answer}",
                epoch=chat_epoch,
            )
        else:
            state.add_chat("remediation", answer, epoch=chat_epoch)
        return jsonify(
            {
                "ok": True,
                "answer": answer,
                "trace": trace,
                "a2a_task_id": a2a_task_id,
                "task_state": task_state,
                "pending_approval": task_state == HITL_INPUT_REQUIRED,
                **state.snapshot(),
            }
        )
    except Exception as exc:
        err = str(exc)
        trace = build_static_trace(
            prompt_source="auto_fix",
            user_prompt=issue_line,
            reason=f"runtime-error: {err}",
        )
        state.set_last_trace(trace, epoch=chat_epoch)
        state.add_chat("remediation", f"Error: {err}", epoch=chat_epoch)
        return jsonify({"ok": False, "error": err, "trace": trace, **state.snapshot()}), 500


@app.get("/api/auto_fix/tasks/<task_id>")
def api_auto_fix_task(task_id: str) -> Any:
    """Proxy A2A Get Task so the Monitor can follow a Doer HITL task to completion."""
    if AGENT_PROCESS_ROLE != "monitor":
        return jsonify({"ok": False, "error": "A2A task polling is owned by the Monitor runtime."}), 404
    try:
        body = get_kafka_a2a_task(
            base_url=A2A_REMEDIATION_BASE_URL,
            token=A2A_REMEDIATION_TOKEN,
            task_id=task_id,
            timeout_seconds=A2A_REQUEST_TIMEOUT_SECONDS,
        )
        task = body.get("task") if isinstance(body, dict) else None
        task_status = task.get("status", {}) if isinstance(task, dict) else {}
        task_state = str(task_status.get("state", ""))
        terminal = task_state in {HITL_COMPLETED, HITL_FAILED, HITL_REJECTED}
        if task_state in {HITL_FAILED, HITL_REJECTED}:
            message = task_status.get("message", {}) if isinstance(task_status, dict) else {}
            parts = message.get("parts", []) if isinstance(message, dict) else []
            detail = next(
                (
                    str(part.get("text"))
                    for part in parts
                    if isinstance(part, dict) and part.get("text")
                ),
                f"A2A remediation task ended in {task_state}.",
            )
            with completed_a2a_tasks_lock:
                pending_a2a_prompts.pop(task_id, None)
            state.add_chat("remediation", f"Task {task_id[:12]} ended: {detail}")
            return jsonify(
                {
                    "ok": False,
                    "terminal": True,
                    "task_state": task_state,
                    "error": detail,
                }
            )

        answer, trace, parsed_task_id, parsed_state = extract_kafka_a2a_task_update(body, "")
        if parsed_state == HITL_COMPLETED:
            should_record = False
            prompt = ""
            with completed_a2a_tasks_lock:
                if parsed_task_id not in completed_a2a_tasks:
                    completed_a2a_tasks.add(parsed_task_id)
                    should_record = True
                pending_a2a_prompts.pop(parsed_task_id, None)
            if should_record:
                state.set_last_trace(trace)
                state.add_chat("remediation", answer)
        return jsonify(
            {
                "ok": True,
                "answer": answer,
                "trace": trace,
                "a2a_task_id": parsed_task_id,
                "task_state": parsed_state,
                "pending_approval": parsed_state == HITL_INPUT_REQUIRED,
                "terminal": terminal,
                **state.snapshot(),
            }
        )
    except Exception as exc:
        return jsonify({"ok": False, "terminal": False, "error": str(exc)}), 502


@app.post("/api/chat/clear")
def api_chat_clear() -> Any:
    """Clear all chat entries and clear the rendered cluster status panel content."""
    previous_thread_id = state.current_thread_id()
    state.clear_chat(clear_status=True)
    if runtime is not None:
        try:
            runtime.clear_thread(previous_thread_id)
        except Exception:
            # Clearing memory is best-effort and should not fail user-visible chat clear.
            pass
    if remediation_runtime is not None:
        # Remediation runs use per-click thread IDs, so there is no shared repair history to retain.
        pass
    return jsonify({"ok": True, **state.snapshot()})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=SERVER_PORT, debug=False, threaded=True)
