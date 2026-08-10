# Agentic Kafka Expert Demo — Internal Documentation

**Author:** Paul Harvener  
**Company:** Data-Blitz Inc.  
**Document status:** Internal working reference  
**Repository:** `agentic-kafka-expert-demo`

## Purpose

This document inventories the repository files and explains how they fit
together. It is intended for Data-Blitz engineering, operations, and demo
support teams. Paths are relative to the repository root.

## System at a glance

The project runs a Kafka cluster with Schema Registry, Kafka UI, Kafka REST
Proxy, Prometheus, Grafana, Neo4j, CouchDB, Ollama, and two isolated agent
runtimes. The Monitor agent observes Kafka and proposes findings. The Doer
agent receives authenticated A2A repair requests, verifies evidence, and can
apply authorized Kafka changes through its mutation-capable tools. Both agents
have Prometheus and Neo4j RAG access. DeepEval evaluates agent outputs.

The browser-facing Monitor UI is exposed on port 5052 and the Doer UI on 5053.
Fine tuning is prepared by the UI but MLX execution belongs on the Mac host.

## Root files

| File | Responsibility |
|---|---|
| `README.md` | Public-facing setup, architecture, operations, API, and troubleshooting guide. |
| `AGENT.md` | Repository-specific agent/development guidance. |
| `LICENSE` | Project license terms. |
| `docker-compose.yml` | Main local orchestration for Kafka, agents, observability, storage, and supporting services. |
| `Dockerfile.app` | Shared Python application image for Monitor, Doer, and related Python services. |
| `Dockerfile.deepeval` | DeepEval service image. |
| `requirements.txt` | Python dependencies for the application image. |
| `requirements-deepeval.txt` | Python dependencies for the DeepEval image. |

## Infrastructure and observability

### Prometheus

| File | Responsibility |
|---|---|
| `monitoring/prometheus/prometheus.yml` | Prometheus scrape configuration for Kafka JMX, node exporter, and service metrics. |
| `monitoring/prometheus/kafka-alerts.yml` | Kafka and infrastructure alert rules. |

### Grafana dashboards

All dashboards are provisioned through `monitoring/grafana/provisioning/dashboards/dashboards.yml` and use the Prometheus datasource configured in `monitoring/grafana/provisioning/datasources/prometheus.yml`.

| File | Responsibility |
|---|---|
| `kafka-jmx-overview.json` | High-level Kafka JMX health and activity overview. |
| `kafka-jmx-attribute-gauges.json` | Dynamic gauge panels for numeric JMX attributes. |
| `kafka-jmx-all-broker-network.json` | Broker and network metrics. |
| `kafka-jmx-all-control-cluster.json` | Controller and cluster-control metrics. |
| `kafka-jmx-all-storage-coordination.json` | Storage, log, and coordination metrics. |
| `kafka-jmx-jvm-os.json` | JVM and operating-system metrics. |
| `kafka-jmx-replication-health.json` | Replication, ISR, and under-replication indicators. |
| `kafka-jmx-request-latency.json` | Request latency and request-rate metrics. |
| `kafka-jmx-throughput.json` | Producer, consumer, and broker throughput metrics. |
| `kafka-observability-control-plane.json` | Agent/control-plane observability panels. |

## Prompts

Prompt files are runtime inputs and should be reviewed like code. `prompts/README.md` describes prompt ownership and editing conventions.

| File | Responsibility |
|---|---|
| `prompts/system_prompt.txt` | Monitor agent system instructions and Kafka-only guardrails. |
| `prompts/remediation_system_prompt.txt` | Doer agent instructions, safety boundaries, and repair behavior. |
| `prompts/cluster_state_prompt.txt` | Full-cluster assessment reasoning prompt. |
| `prompts/auto_fix_prompt_template.txt` | Context template for A2A repair handoff. |
| `prompts/kafka_only_message.txt` | Standard refusal for non-Kafka requests. |
| `prompts/graphrag_extract_prompt.txt` | PDF-to-knowledge-graph extraction instructions. |
| `prompts/graphrag_query_prompt.txt` | Neo4j Graph RAG question-answering instructions. |

## Application scripts

| File | Responsibility |
|---|---|
| `scripts/kafka_expert_ui.py` | Flask Monitor/Doer UI, tab rendering, APIs, chat, cluster assessment, HITL, A2A, observability, and fine-tuning controls. |
| `scripts/agent_llm_config.py` | Provider/model resolution and independent Monitor, Doer, and Judge configuration. |
| `scripts/kafka_a2a.py` | A2A protocol message, task, authentication, and task-state helpers. |
| `scripts/monitor_handoff.py` | Builds validated Monitor-to-Doer repair handoff context. |
| `scripts/remediation_hitl.py` | Human-in-the-loop state, approval, rejection, and review context. |
| `scripts/kafka_agent_tools.py` | Selects role-specific Kafka Admin and REST Proxy tools. |
| `scripts/kafka_admin_mcp_server.py` | Kafka Admin MCP server for metadata, diagnostics, and authorized mutations. |
| `scripts/kafka_rest_mcp_server.py` | Kafka REST Proxy MCP server for health, topics, metadata, and Doer JSON production. |
| `scripts/prometheus_mcp_server.py` | Prometheus MCP server for metric queries and snapshots. |
| `scripts/neo4j_agent_rag.py` | Neo4j graph ingestion, retrieval, and RAG tooling. |
| `scripts/deepeval_service.py` | Local DeepEval judge HTTP service. |
| `scripts/deepeval_metric_utils.py` | Metric normalization and quality/safety scoring helpers. |
| `scripts/mlx_finetuning.py` | MLX runtime detection, relative-path validation, command construction, and duration estimates. |
| `scripts/phr_generator.py` | Synthetic personal health record generator that follows `schemas/ph-schema.json` and emits only fictional identifiers. |
| `scripts/producer.py` | Kafka producer workload generator for credit-card Avro or synthetic PHR JSON profiles. |
| `scripts/producer_ui.py` | Producer browser UI/API with credit-card Avro and synthetic PHR JSON profiles. |
| `scripts/consumer.py` | Kafka consumer workload and persistence integration. |
| `scripts/consumer_ui.py` | Consumer browser UI and message feed. |
| `scripts/couchdb_writer.py` | Consumed-message persistence in CouchDB. |
| `scripts/register_schema.py` | Schema Registry bootstrap/registration utility. |
| `scripts/kafka_jmx_prometheus_exporter.py` | Kafka JMX-to-Prometheus exporter support. |

## Data and model configuration

| File | Responsibility |
|---|---|
| `schemas/credit_card_purchase.avsc` | Avro schema used by the producer and Schema Registry bootstrap. |
| `schemas/ph-schema.json` | JSON Schema contract for synthetic personal health records. |
| `ollama/Modelfile.kafka-qwen3` | Local Ollama model configuration for Kafka-focused inference. |
| `draw.io/kafkaExpert.drawio` | Editable architecture diagram source. |
| `draw.io/images/kafka.svg` | Kafka diagram asset. |

## Tests

| File | Coverage |
|---|---|
| `tests/test_agent_llm_config.py` | Provider/model selection and fallback behavior. |
| `tests/test_deepeval_metric_utils.py` | Judge metric normalization and aggregation. |
| `tests/test_generate_app_summary_pdf.py` | Application summary PDF generation behavior. |
| `tests/test_kafka_a2a.py` | A2A protocol and task-state helpers. |
| `tests/test_kafka_agent_tools.py` | Role-specific tool selection. |
| `tests/test_kafka_rest_mcp_server.py` | REST Proxy MCP behavior. |
| `tests/test_mlx_finetuning.py` | MLX command safety, relative paths, and duration estimates. |
| `tests/test_phr_generator.py` | PHR schema validation, synthetic identifier safety, temporal consistency, and producer profile wiring. |
| `tests/test_monitor_handoff.py` | Monitor-to-Doer handoff validation. |
| `tests/test_prometheus_mcp_server.py` | Prometheus MCP query behavior. |
| `tests/test_remediation_hitl.py` | HITL approval/rejection lifecycle. |
| `tests/test_ui_tabs.py` | Tab/panel wiring and non-blocking cluster assessment. |

## Operational notes

- Run the full test suite with `pytest -q`.
- Rebuild changed services with `docker compose up -d --build kafka-expert-ui kafka-remediation-agent`.
- Fine tuning uses relative paths such as `data/finetuning/example.jsonl` and `models/kafka-monitor-lora`.
- Install MLX on the Mac host with `python -m pip install mlx mlx-lm`; Docker Linux containers cannot execute Apple Silicon MLX jobs.
- Keep secrets in environment configuration; do not commit API keys, tokens, or credentials.
- Changes to prompts, MCP tools, A2A behavior, or dashboards should include corresponding tests or verification notes.

## Python source reference

This section documents the Python source files at module/function level. Private
helpers are included where they define validation, persistence, or protocol
behavior.

### `scripts/agent_llm_config.py`

Normalizes provider names, removes duplicate model names, resolves the provider
for Monitor/Doer, and builds the primary/fallback model list for each agent.

### `scripts/consumer.py`

CLI consumer entry point. `main()` consumes configured Kafka records and writes
them through the configured persistence path.

### `scripts/consumer_ui.py`

`ConsumerFeed` owns the background Kafka consumer, message buffer, latency
calculation, and snapshot API. Flask routes `index()` and `api_messages()` serve
the dark consumer feed and JSON message endpoint.

### `scripts/couchdb_writer.py`

`CouchDBWriter` manages authenticated CouchDB requests, database creation,
stable topic/partition/offset document IDs, and message writes. `from_env()`
constructs configuration from environment variables.

### `scripts/deepeval_metric_utils.py`

Defines metric direction, converts raw judge scores into normalized quality
scores, builds metric payloads, and calculates aggregate quality.

### `scripts/deepeval_service.py`

Implements the local judge service. `OpenAICompatibleJudge` adapts configured
OpenAI-compatible providers to DeepEval. Flask endpoints expose health,
configuration, stored results, and evaluation. `evaluate_case()` scores one
Monitor/Doer output.

### `scripts/kafka_a2a.py`

Builds A2A Send Message requests, extracts task updates and completed tasks,
and performs authenticated HTTP send/get operations for Monitor-to-Doer repair
handoffs.

### `scripts/kafka_admin_mcp_server.py`

MCP wrapper around Kafka AdminClient. Read operations describe clusters, topics,
partitions, consumer groups, and topic configs. Mutation operations create or
delete topics, resize partitions, delete groups, and set/delete topic configs.
Parsing helpers normalize CSV/config inputs and convert Kafka objects to JSON.

### `scripts/kafka_agent_tools.py`

Filters discovered MCP tools by role. Monitor receives read-only Kafka tools;
Doer receives read tools plus explicitly authorized mutation tools. REST Proxy
tool selection follows the same policy.

### `scripts/kafka_expert_ui.py`

Primary application module. It contains:

- Prompt loading/rendering and Kafka-only request guards.
- CouchDB-backed LangGraph checkpoint persistence.
- LLM provider/model configuration and fallback initialization.
- Deterministic Kafka, Schema Registry, Prometheus, and topic-inventory answers.
- Chat, cluster-state assessment, A2A repair, HITL, Graph RAG, DeepEval, and
  observability APIs.
- HTML/JavaScript for Monitor, Doer, Graph RAG, LLM, DeepEval, Fine Tuning,
  Producer, Consumer, Grafana, Neo4j Browser, and Kafka UI tabs.
- Fine Tuning preparation, relative-path validation, MLX duration estimates,
  host-required handling, job submission, progress polling, and elapsed-time UI.

`CouchDBContextStore`, `CouchDBBackedMemorySaver`, and `GraphRAGRuntime` are
the main stateful classes. HTTP functions near the end of the module are Flask
route handlers.

### `scripts/kafka_jmx_prometheus_exporter.py`

Polls Kafka JMX endpoints, parses CSV output, converts values to Prometheus
metrics, and serves `/metrics` through `MetricsHandler`. `ExporterState` keeps
the latest broker snapshots; `poll_loop()` refreshes them.

### `scripts/kafka_rest_mcp_server.py`

MCP tools for REST Proxy health, topic listing, topic metadata, and JSON
production. `_request()` centralizes URL construction, timeout handling, and
JSON decoding.

### `scripts/mlx_finetuning.py`

Reports MLX/MLX-LM import availability, validates JSONL and relative paths,
builds shell-safe command strings/argv, and estimates duration from model size,
iterations, and LoRA rank. It intentionally does not execute training.

### `scripts/monitor_handoff.py`

Extracts the Monitor’s reason and source evidence from an issue line/snapshot
and produces the structured context sent to the Doer over A2A.

### `scripts/neo4j_agent_rag.py`

Extracts meaningful question terms and retrieves relevant Neo4j graph knowledge
for both agent runtimes. It is the shared Graph RAG access layer, not a UI.

### `scripts/producer.py`

CLI Kafka producer. `make_purchase()` creates a sample purchase event. Set
`PRODUCER_PROFILE=phr` to call `generate_personal_health_record()` and publish
JSON bytes to `PHR_TOPIC`; the default profile remains Avro credit-card data.
`delivery_report()` handles asynchronous Kafka delivery results.

### `scripts/phr_generator.py`

`generate_personal_health_record()` creates a realistic but fictional
longitudinal record with patient demographics, conditions, allergies,
medications, observations, encounters, procedures, care plans, insurance,
consents, and provenance. It uses `example.invalid` email addresses and 555
phone numbers and accepts injectable clock/randomness for deterministic tests.

### `scripts/producer_ui.py`

Provides schema-aware message generation and producer controls. `ProducerService`
manages topic/schema lifecycle, Avro value generation, synthetic PHR JSON
generation, one-shot sends, profile switching, and the rate loop. Flask routes
expose profile selection, schema editing, preview, send, start, stop, and status
operations. PHR values are published as JSON bytes without Avro Schema Registry
registration.

### `scripts/prometheus_mcp_server.py`

MCP adapter for Prometheus instant/range queries and deterministic Kafka,
partition, consumer-group, and Schema Registry inventory snapshots. Cluster
state queries run in bounded parallel workers.

### `scripts/register_schema.py`

Bootstrap utility that registers `schemas/credit_card_purchase.avsc` and sets
the desired Schema Registry subject compatibility.

### `scripts/remediation_hitl.py`

Extracts issue/diagnosis text, formats a human-readable repair plan, and stores
Doer tasks in `RemediationHitlStore`. The store handles enabled state, task
creation, approval/rejection, completion/failure, bounded history, and safe
copies for API responses.

### `tmp/pdfs/generate_app_summary_pdf.py`

Standalone utility for creating a compact PDF application summary. It resolves
the output path, builds summary sections, escapes PDF text, writes a content
stream, and emits a minimal valid PDF. It is a temporary/support artifact and
is covered by `tests/test_generate_app_summary_pdf.py`.

## Test-to-source map

The test suite is intentionally source-oriented: configuration tests cover
`agent_llm_config`, protocol tests cover `kafka_a2a`, tool tests cover both MCP
servers and role filtering, lifecycle tests cover HITL/handoff, UI tests cover
tab wiring and asynchronous cluster assessment, and MLX tests cover command
safety and runtime estimation. Run `pytest -q` after changes to any source file.
