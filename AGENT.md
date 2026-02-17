# AGENT.md

## Ownership
- Owner: `Paul Harvener`
- Organization: `Data-Blitz Inc`
- Project: `kafka-expert` demo

## License
- Controlled by the `MIT License`
- License file: `/Users/paulharvener/workspace/kafka-expert/LICENSE`

## Objective
Set up a Docker Compose-based Confluent Kafka cluster compatible with local development, including:
- 3 Kafka brokers (KRaft mode)
- Confluent Schema Registry
- Kafka UI
- CouchDB for consumed-message persistence
- Kafka JMX instrumentation
- node_exporter host/filesystem instrumentation
- Prometheus for JMX metric storage
- Grafana dashboard for JMX monitoring
- Neo4j graph database for Graph RAG knowledge edges
- Python consumer using Avro with Schema Registry
- Producer Web UI for manual and rate-based publishing with editable schema
- Kafka Expert Agentic UI using LangChain
- Prometheus access exposed to agent through MCP tools
- Kafka Admin access exposed to agent through MCP tools for metadata and cluster mutations
- Kafka-only chatbot behavior and cluster-state assessment workflow
- All executables run via Docker Compose services only

## Artifacts Created

### 1) Docker Compose stack
Created:
- `/Users/paulharvener/workspace/kafka-expert/docker-compose.yml`

Contains:
- `kafka1`, `kafka2`, `kafka3` using `confluentinc/cp-kafka:${CP_VERSION}`
- `schema-registry` using `confluentinc/cp-schema-registry:${CP_VERSION}`
- `kafka-ui` using `provectuslabs/kafka-ui:${KAFKA_UI_VERSION}`
- `couchdb` using `couchdb:3.3`
- `kafka-jmx-prom-exporter` using `confluentinc/cp-kafka:${CP_VERSION}`
- `node-exporter` using `prom/node-exporter:v1.8.2`
- `prometheus` using `prom/prometheus:v2.54.1`
- `grafana` using `grafana/grafana:10.4.7`
- `neo4j` using `neo4j:5.26.0`
- `kafka-expert-ui` using local Python app image (`Dockerfile.app`)
- Named volumes:
  - `couchdb-data`
  - `prometheus-data`
  - `grafana-data`
  - `kafka1-data`
  - `kafka2-data`
  - `kafka3-data`

Ports:
- Kafka brokers (host listeners):
  - `19092` -> `kafka1`
  - `29092` -> `kafka2`
  - `39092` -> `kafka3`
- Schema Registry:
  - `8081`
- Kafka UI:
  - `8080`
- CouchDB:
  - `5984`
- JMX Exporter:
  - `9404`
- node_exporter:
  - `9100`
- Prometheus:
  - `9090`
- Grafana:
  - `3000`
- Neo4j:
  - `7474` (browser)
  - `7687` (bolt)
- Kafka Expert UI:
  - `5052`

### 2) Environment file
Created:
- `/Users/paulharvener/workspace/kafka-expert/.env`

Values:
- `CP_VERSION=7.6.1`
- `KAFKA_UI_VERSION=v0.7.2`
- `KAFKA_CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk`
- `KAFKA_BOOTSTRAP_SERVERS=localhost:19092,localhost:29092,localhost:39092`
- `SCHEMA_REGISTRY_URL=http://localhost:8081`
- `KAFKA_TOPIC=credit-card-purchases`
- `KAFKA_GROUP_ID=credit-card-consumer`
- `SCHEMA_FILE=schemas/credit_card_purchase.avsc`
- `PRODUCER_RATE_SECONDS=1.0`
- `PRODUCER_UI_PORT=5050`
- `CONSUMER_UI_PORT=5051`
- `CONSUMER_UI_GROUP_ID=credit-card-consumer-ui`
- `COUCHDB_USER=admin`
- `COUCHDB_PASSWORD=demo`
- `COUCHDB_DB=kafka_expert_consumed`
- `CONSUMER_WRITE_COUCHDB=true`
- `KAFKA_JMX_POLL_INTERVAL_SECONDS=5`
- `JMX_EXPORTER_PORT=9404`
- `PROMETHEUS_PORT=9090`
- `GRAFANA_PORT=3000`
- `GRAFANA_ADMIN_USER=admin`
- `GRAFANA_ADMIN_PASSWORD=demo`
- `KAFKA_EXPERT_UI_PORT=5052`
- `PROMETHEUS_URL=http://prometheus:9090`
- `OPENAI_MODEL=gpt-5.3`
- `OPENAI_MODEL_FALLBACKS=gpt-5.2,gpt-4.1`
- `OPENAI_BASE_URL=` (optional)
- `OPENAI_API_KEY=` (required for live model responses)
- `NEO4J_USER=neo4j`
- `NEO4J_PASSWORD=<set-secure-password>`
- `NEO4J_AUTH=none` (demo auto-login mode; disables Neo4j credential prompts)
- `NEO4J_BROWSER_PUBLIC_URL=http://localhost:7474/browser/`
- `GRAPH_RAG_MAX_CHUNKS=20`

### 3) Avro schema
Created:
- `/Users/paulharvener/workspace/kafka-expert/schemas/credit_card_purchase.avsc`

Schema type:
- Avro record `CreditCardPurchase` in namespace `com.example.payments`

Fields:
- `purchase_id`, `event_time`, `card_last4`, `card_brand`, `merchant`
- `category`, `city`, `state`, `amount`, `currency`, `is_card_present`

### 4) Python app scripts
Created:
- `/Users/paulharvener/workspace/kafka-expert/scripts/register_schema.py`
- `/Users/paulharvener/workspace/kafka-expert/scripts/producer.py`
- `/Users/paulharvener/workspace/kafka-expert/scripts/producer_ui.py`
- `/Users/paulharvener/workspace/kafka-expert/scripts/consumer.py`
- `/Users/paulharvener/workspace/kafka-expert/scripts/consumer_ui.py`
- `/Users/paulharvener/workspace/kafka-expert/scripts/couchdb_writer.py`
- `/Users/paulharvener/workspace/kafka-expert/scripts/kafka_jmx_prometheus_exporter.py`
- `/Users/paulharvener/workspace/kafka-expert/scripts/prometheus_mcp_server.py`
- `/Users/paulharvener/workspace/kafka-expert/scripts/kafka_admin_mcp_server.py`
- `/Users/paulharvener/workspace/kafka-expert/scripts/kafka_expert_ui.py`
- `/Users/paulharvener/workspace/kafka-expert/requirements.txt`

Responsibilities:
- `register_schema.py`
  - Registers `schemas/credit_card_purchase.avsc` to subject `credit-card-purchases-value`
- `producer.py`
  - Produces mock credit card purchase events
  - Uses Avro serializer + Schema Registry
  - Requires pre-registered schema (`auto.register.schemas=false`)
  - Uses `PRODUCER_RATE_SECONDS` from `.env` for message cadence
  - Creates topic `credit-card-purchases` if missing
- `producer_ui.py`
  - Serves a browser UI on `http://localhost:5050`
  - Green button sends one message immediately
  - Start/Stop controls publish continuously at configurable seconds rate
  - Shows current schema and allows editing/saving schema
  - Registers schema updates to Schema Registry
  - Generates payloads directly from current Avro schema
- `consumer.py`
  - Consumes Avro messages using Schema Registry deserializer
  - Converts each message to JSON envelope with consume/Kafka timestamps
  - Persists one CouchDB document per consumed message (`topic:partition:offset` id)
- `consumer_ui.py`
  - Serves a browser UI on `http://localhost:5051`
  - Displays each consumed JSON message in pretty format
  - Shows consume latency in milliseconds (`consumed_at - event_time`)
  - Writes one CouchDB document per consumed message (`topic:partition:offset` id)
- `couchdb_writer.py`
  - Creates the configured CouchDB database when missing
  - Writes consumed message documents with metadata (topic/partition/offset/key/latency/message)
  - Adds `consumed_at`, `written_at_utc`, `kafka_timestamp_type`, and `kafka_timestamp_ms`
  - Retries on transient write failures with short backoff
- `kafka_jmx_prometheus_exporter.py`
  - Polls all broker JMX endpoints every `KAFKA_JMX_POLL_INTERVAL_SECONDS`
  - Exposes numeric JMX attributes at `/metrics` for Prometheus scraping
  - Exposes exporter health metrics (`kafka_jmx_exporter_*`)
- `prometheus_mcp_server.py`
  - Exposes MCP tools for Prometheus instant/range queries
  - Provides a `kafka_cluster_state_snapshot` tool that executes broad Kafka-health PromQL set
  - Includes scrape-health checks for both `kafka_jmx` and `node_exporter`
  - Includes host disk utilization telemetry from `node_filesystem_*` metrics
  - Includes request-latency p99 telemetry for `Produce` and `FetchConsumer`
  - Includes ISR/leader telemetry (`IsrShrinksPerSec`, `PreferredReplicaImbalanceCount`)
  - Includes JVM GC pressure telemetry via `java.lang` GC `CollectionTime` rate
  - Collects controller/replication/backpressure metrics including `ActiveControllerCount`, `OfflinePartitionsCount`, `UnderReplicatedPartitions`, and `RequestQueueSize`
  - Provides `kafka_topic_inventory` tool with:
    - full inventory from `kafka.log` topic/partition metrics
    - active-topic view from `BrokerTopicMetrics`
  - Provides `kafka_partition_inventory` tool with total and per-topic partition counts
  - Provides `kafka_consumer_group_summary` tool with total/stable/empty/rebalancing counts
  - Provides `schema_registry_inventory` tool with subject inventory and latest contract metadata
- `kafka_admin_mcp_server.py`
  - Exposes Kafka Admin MCP tools for cluster metadata reads and cluster state mutations
  - Supports topic operations (list/describe/create/delete/create_partitions)
  - Supports topic config operations (describe/set/delete keys)
  - Supports consumer-group operations (list/describe/delete groups)
  - Supports cluster metadata introspection (`kafka_describe_cluster`)
- `kafka_expert_ui.py`
  - Serves Kafka Expert UI at `http://localhost:5052`
  - Uses LangChain agent with MCP-backed Prometheus + Kafka Admin tools for reasoning and actions
  - Provides `Query Full Cluster State` action writing assessment to status panel
  - Provides seven UI tabs: `Kafka Expert`, `Graph RAG`, `Kafka Producer`, `Kafka Consumer`, `Grafana`, `Neo4j Browser`, and `Kafka UI`
  - Adds Neo4j Graph RAG workflow:
    - upload PDF
    - extract entity/relation edges with LLM
    - persist graph to Neo4j
    - answer graph-grounded questions from Neo4j context
  - Embeds Producer UI (`http://localhost:5050`) inside `Kafka Producer` tab for in-context message publishing
  - Embeds Consumer UI (`http://localhost:5051`) inside `Kafka Consumer` tab with dark-mode styling
  - Embeds Grafana dashboards in dark mode inside `Grafana` tab
  - Launches Neo4j Browser in a new browser tab from `Neo4j Browser` tab (not iframe) because Neo4j Browser sets `X-Frame-Options: DENY` and `frame-ancestors 'none'`
  - Supports Neo4j Browser auto-login UX in demo mode (`NEO4J_AUTH=none`) so end users are not prompted for username/password
  - Launches Kafka UI in a new browser tab (not iframe) because Kafka UI sets `X-Frame-Options: DENY`
  - Provides Kafka-only chatbot and refuses non-Kafka questions
  - Handles explicit create-topic requests deterministically (e.g., `add kafka topic called sally`) using Kafka Admin and immediate metadata verification
  - Normalizes outputs into concise `1.`-based bullets with `[GOOD]`, `[WARN]`, `[BAD]` tags
  - Renders status bullets with green/yellow/red backgrounds based on severity
  - Renders status text in regular weight; only `Fix:` text is bold
  - Makes `Fix:` text clickable and routes to backend auto-remediation
  - Provides a visible `Clear Chat` button and backend `POST /api/chat/clear` endpoint
  - Adds a short plain-language `Short answer:` summary under colored bullet blocks in agent chat messages
  - Uses deterministic Prometheus-based topic inventory responses for topic count/list questions
  - Uses deterministic Kafka Admin metadata responses for topics/partitions/consumer-group inventory questions
  - Uses deterministic Schema Registry REST responses for subjects and data-contract questions
  - Exposes `POST /api/auto_fix` to run LLM remediation attempts from clicked `Fix:` items using all available tools
  - Exposes Graph RAG endpoints:
    - `GET /api/graphrag/status`
    - `POST /api/graphrag/ingest_pdf`
    - `POST /api/graphrag/query`
  - Handles Prometheus scrape/alert coverage `Fix:` requests deterministically by validating Prometheus targets, metric series presence, and loaded alert rules
  - Includes prompt rules for safe admin mutations: only execute on explicit user request and verify post-change state
  - Uses GenAI credentials from env (`OPENAI_API_KEY`, `OPENAI_MODEL`, optional `OPENAI_BASE_URL`)
  - Validates requested OpenAI model on startup and auto-falls back via `OPENAI_MODEL_FALLBACKS` when the requested model is unavailable

### 5) Monitoring configuration
Created:
- `/Users/paulharvener/workspace/kafka-expert/monitoring/prometheus/prometheus.yml`
- `/Users/paulharvener/workspace/kafka-expert/monitoring/prometheus/kafka-alerts.yml`
- `/Users/paulharvener/workspace/kafka-expert/monitoring/grafana/provisioning/datasources/prometheus.yml`
- `/Users/paulharvener/workspace/kafka-expert/monitoring/grafana/provisioning/dashboards/dashboards.yml`
- `/Users/paulharvener/workspace/kafka-expert/monitoring/grafana/dashboards/kafka-jmx-overview.json`
- `/Users/paulharvener/workspace/kafka-expert/monitoring/grafana/dashboards/kafka-jmx-throughput.json`
- `/Users/paulharvener/workspace/kafka-expert/monitoring/grafana/dashboards/kafka-jmx-request-latency.json`
- `/Users/paulharvener/workspace/kafka-expert/monitoring/grafana/dashboards/kafka-jmx-replication-health.json`
- `/Users/paulharvener/workspace/kafka-expert/monitoring/grafana/dashboards/kafka-jmx-jvm-os.json`
- `/Users/paulharvener/workspace/kafka-expert/monitoring/grafana/dashboards/kafka-jmx-all-control-cluster.json`
- `/Users/paulharvener/workspace/kafka-expert/monitoring/grafana/dashboards/kafka-jmx-all-broker-network.json`
- `/Users/paulharvener/workspace/kafka-expert/monitoring/grafana/dashboards/kafka-jmx-all-storage-coordination.json`
- `/Users/paulharvener/workspace/kafka-expert/monitoring/grafana/dashboards/kafka-observability-control-plane.json`

Responsibilities:
- Prometheus scrapes:
  - `kafka-jmx-prom-exporter:9404`
  - `node-exporter:9100`
- Prometheus loads alerting rules from `kafka-alerts.yml` for:
  - broker log disk capacity
  - host filesystem disk utilization
  - request latency p99
  - ISR/leader health
  - JVM GC and file-descriptor pressure
  - missing request-latency and ISR/leader telemetry series
- Grafana auto-provisions a single Prometheus datasource (`uid=prometheus`).
- Grafana auto-loads JMX dashboards:
  - `Kafka JMX Overview`
  - `Kafka JMX Throughput`
  - `Kafka JMX Request Latency`
  - `Kafka JMX Replication Health`
  - `Kafka JMX JVM and OS`
  - `Kafka JMX All Attributes - Control and Cluster` (domains: `kafka.controller`, `kafka.cluster`, `kafka.utils`)
  - `Kafka JMX All Attributes - Broker and Network` (domains: `kafka.server`, `kafka.network`)
  - `Kafka JMX All Attributes - Storage and Coordination` (domains: `kafka.log`, `kafka.coordinator.group`, `kafka.coordinator.transaction`)
  - `Kafka Observability Control Plane` (node disk + JVM + latency p99 + ISR/leader signals)
  - These 3 `All Attributes` dashboards partition and cover all exported `kafka.*` numeric JMX attributes.

## Issues Encountered and Fixes Applied

### Issue 1: Kafka containers exited on startup
Symptom:
- Brokers exited with:
  - `CLUSTER_ID is required.`

Root cause:
- Compose used `KAFKA_CLUSTER_ID` environment variable inside broker services.
- Confluent KRaft startup script expects `CLUSTER_ID`.

Fix:
- Updated broker env from:
  - `KAFKA_CLUSTER_ID: ${KAFKA_CLUSTER_ID}`
- To:
  - `CLUSTER_ID: ${KAFKA_CLUSTER_ID}`

### Issue 2: Broker health checks never turned healthy
Symptom:
- Brokers showed `health: starting` for a long time.
- Health checks timed out / connection refused.

Root cause:
- Health check targeted `localhost:9092`, but brokers were not listening on localhost for that listener in this setup.

Fix:
- Changed health checks to host listener ports inside each container:
  - `kafka1`: `localhost:19092`
  - `kafka2`: `localhost:29092`
  - `kafka3`: `localhost:39092`

### Issue 3: Consumer exited if topic did not exist yet
Symptom:
- Consumer stopped with unknown topic/partition at startup.

Fix:
- Producer now creates the topic if needed.
- Consumer now tolerates transient `UNKNOWN_TOPIC_OR_PART` during bootstrap.

### Issue 4: Kafka Expert runtime unavailable without model credentials
Symptom:
- Kafka Expert `/api/health` showed `ok=false` with runtime error.
- Kafka Expert cluster-state and Kafka chat endpoints returned runtime-unavailable errors.

Root cause:
- `OPENAI_API_KEY` was not configured for LangChain model client initialization.

Fix:
- Added required env guidance in `.env`/README.
- Runtime now reports credential state via `/api/health` for clear diagnosis.

### Issue 5: Cluster-state trigger returned sync tool invocation error
Symptom:
- Clicking `Query Full Cluster State` returned:
  - `StructuredTool does not support sync invocation.`

Root cause:
- LangChain tool path attempted synchronous invocation for tools that only support async execution.

Fix:
- Updated the agent runtime to use async invocation (`ainvoke`) and execute via `asyncio.run(...)`.
- Re-tested `POST /api/cluster_state` and `POST /api/chat` successfully.

### Issue 6: Key Kafka health metrics returned empty series in snapshot
Symptom:
- Cluster assessments reported missing values for:
  - `ActiveControllerCount`
  - `OfflinePartitionsCount`
  - `UnderReplicatedPartitions`
  - `RequestQueueSize`

Root cause:
- PromQL filters in the MCP snapshot did not match the JMX exporter label shape for these MBeans.

Fix:
- Updated snapshot queries to match exporter labels (`attribute="Value"` and correct `name=...` MBean selectors).
- Verified series presence per broker for all four metrics.

### Issue 7: Operator output readability was too verbose/raw
Symptom:
- Agent output was difficult to scan quickly during operations.

Fix:
- Added response normalization to concise `1.`-based bullets.
- Added severity tags `[GOOD]`, `[WARN]`, `[BAD]`.
- Enforced `Fix:` actions for warning/bad bullets.
- Added UI severity color coding and bold rendering for `Fix:` content.

### Issue 8: Topic count warning claimed topic metrics were unavailable
Symptom:
- Agent responses sometimes stated topic count could not be determined from Prometheus.

Root cause:
- Cluster snapshot/tooling did not provide explicit topic inventory queries, so the model could miss available topic-labeled JMX series.

Fix:
- Added `kafka_topic_inventory` MCP tool with:
  - `inventory_topic_count`
  - `inventory_topics`
  - `active_topic_count`
  - `active_topics`
- Added inventory/activity topic queries to `kafka_cluster_state_snapshot`.
- Updated agent prompt to use topic inventory first for topic-count questions.

### Issue 9: Topic inventory responses still produced avoidable "not full inventory" warnings
Symptom:
- Some topic count/list responses still warned that inventory was incomplete.

Root cause:
- Topic answers still depended on model interpretation of activity-based metrics in some paths.

Fix:
- Added deterministic topic-inventory answer path in `kafka_expert_ui.py` for topic count/list/inventory questions.
- Full inventory now comes from `kafka.log` topic/partition metrics, while `BrokerTopicMetrics` is presented as traffic activity only.

### Issue 10: Needed full inventory coverage and schema-contract Q&A support
Symptom:
- User requested explicit collection/answers for topics, partitions, consumer groups, and Schema Registry subjects/data contracts.

Root cause:
- Prior flow relied mainly on model tool reasoning and did not guarantee deterministic inventory answers for all requested domains.

Fix:
- Added deterministic inventory path in `kafka_expert_ui.py`:
  - Kafka Admin metadata for topics and partitions.
  - Kafka consumer-group listings for group IDs and states.
- Added deterministic schema path in `kafka_expert_ui.py`:
  - Schema Registry subject list, compatibility, latest versions, and contract summaries.
- Added MCP tools:
  - `kafka_partition_inventory`
  - `kafka_consumer_group_summary`
  - `schema_registry_inventory`
- Extended `kafka_cluster_state_snapshot` to include partition and consumer-group metrics.

### Issue 11: Needed full Kafka Admin MCP integration for LLM-driven cluster changes
Symptom:
- User requested full Kafka Admin MCP exposure so Kafka Expert could both inspect and modify Kafka cluster state.

Root cause:
- Existing MCP integration focused on Prometheus telemetry; admin operations were not exposed as a dedicated MCP resource set.

Fix:
- Added `kafka_admin_mcp_server.py` with read and mutation tools for topics, partitions, consumer groups, and topic configs.
- Wired Kafka Expert runtime to load tools from both MCP servers:
  - `prometheus_mcp_server.py`
  - `kafka_admin_mcp_server.py`
- Updated system prompt to document new resources and mutation policy (explicit user intent + post-change verification).

### Issue 12: Needed explicit Prometheus alerting for disk, latency, ISR, and leader signals
Symptom:
- User requested concrete Prometheus collection/alert coverage for broker disk, request latency, and ISR/leader metrics.

Root cause:
- Prometheus scraped metrics, but threshold-based alerts were not defined in rule files.

Fix:
- Added `monitoring/prometheus/kafka-alerts.yml` and wired it via `rule_files` in `prometheus.yml`.
- Implemented alerts for:
  - Broker disk size threshold
  - Produce/FetchConsumer request latency p99
  - ISR shrink rate
  - Active controller count validity
  - Offline/under-replicated partitions
  - Preferred replica imbalance

### Issue 13: Needed clickable Fix workflow and focused visual emphasis
Symptom:
- User requested only `Fix:` text to be bold (not full status line) and wanted click-to-remediate behavior.

Root cause:
- UI formatting emphasized entire status lines and had no direct action binding on remediation text.

Fix:
- Updated status-card typography to regular body text.
- Kept bold styling only for `Fix:` segments.
- Made `Fix:` segments clickable.
- Added backend `POST /api/auto_fix` endpoint to trigger a remediation attempt with all available MCP tools and return verification output.

### Issue 14: Missing clear-chat action and incomplete Prometheus scrape/alert coverage for JVM+disk
Symptom:
- User could not find a `Clear Chat` button in Kafka Expert UI.
- Cluster health guidance requested explicit fix for Prometheus scraping/alerting coverage for JVM/disk (`node_exporter`), request latency percentiles, and ISR/leader metrics.

Root cause:
- UI had no clear-chat control or API endpoint.
- Prometheus config only scraped `kafka_jmx`; it did not scrape `node_exporter`.
- Alerts did not include host filesystem (`node_exporter`) and JVM pressure rules, or telemetry-missing alerts for latency/ISR-leader signals.

Fix:
- Added a visible `Clear Chat` button in Kafka Expert UI and backend endpoint:
  - `POST /api/chat/clear` clears chat transcript while preserving cluster status panel text.
- Added `node-exporter` service to Docker Compose and Prometheus scrape config:
  - New target: `node-exporter:9100`
- Added/updated Prometheus alerts for:
  - `NodeExporterTargetDown`
  - `KafkaHostDiskUsageHigh`
  - `KafkaJvmGcTimeHigh`
  - `KafkaFileDescriptorUsageHigh`
  - `KafkaRequestLatencyMetricsMissing`
  - `KafkaIsrLeaderMetricsMissing`
- Expanded MCP cluster snapshot queries to include:
  - `node_exporter` scrape status
  - host disk usage percent
  - request latency p99 (`Produce` / `FetchConsumer`)
  - ISR shrink rate and preferred replica imbalance
  - JVM GC time rate
- Added Grafana dashboard:
  - `Kafka Observability Control Plane` for the new disk/JVM/latency/ISR-leader views.

### Issue 15: Needed Neo4j Graph RAG for PDF-driven Kafka knowledge edges
Symptom:
- User requested Graph RAG using Neo4j that accepts a PDF document and generates graph edges for question answering.

Root cause:
- Existing stack had no graph database service and no PDF-to-graph ingestion workflow.

Fix:
- Added `neo4j` service (`7474` browser, `7687` bolt) to Docker Compose.
- Added Neo4j and PDF dependencies:
  - `neo4j`
  - `pypdf`
- Added Graph RAG runtime in `kafka_expert_ui.py`:
  - PDF text extraction
  - chunking
  - LLM-based edge extraction (`source`, `relation`, `target`, `evidence`)
  - Neo4j upserts for `Document`, `Chunk`, `Entity`, and `RELATES` edges
  - graph-grounded query answering
- Added Graph RAG API endpoints:
  - `GET /api/graphrag/status`
  - `POST /api/graphrag/ingest_pdf`
  - `POST /api/graphrag/query`
- Added `Graph RAG` tab to Kafka Expert UI with:
  - PDF upload + ingest action
  - graph status counters
  - graph question input and answer log

## Commands Executed (high level)
- `docker compose config` (validation)
- `docker compose up -d`
- `docker compose ps -a`
- `docker compose logs ...`
- `docker inspect ... .State.Health`
- `docker compose down && docker compose up -d`
- `docker compose up -d --force-recreate`
- `curl http://localhost:8081/subjects`
- `curl http://localhost:8080`
- `curl -u admin:demo http://localhost:5984/_up`
- `curl -u admin:demo "http://localhost:5984/kafka_expert_consumed/_all_docs?include_docs=true"`
- `curl http://localhost:9404/metrics`
- `curl http://localhost:9090/api/v1/targets`
- `curl -u admin:demo http://localhost:3000/api/health`
- `docker compose build`
- `docker compose up -d`
- `docker compose --profile rate-producer up -d producer-rate` (optional)
- `docker compose build kafka-expert-ui`
- `docker compose up -d kafka-expert-ui`
- `curl http://localhost:5052/api/health`
- `curl -X POST http://localhost:5052/api/cluster_state -H 'Content-Type: application/json' -d '{}'`
- `curl -X POST http://localhost:5052/api/chat -H 'Content-Type: application/json' -d '{"message":"Is the Kafka cluster healthy?"}'`
- `docker compose up -d --build --force-recreate kafka-expert-ui`
- `curl -I http://localhost:7474`
- `curl http://localhost:5052/api/graphrag/status`
- `curl -X POST http://localhost:5052/api/graphrag/ingest_pdf -F "pdf=@/path/to/kafka.pdf"`
- `curl -X POST http://localhost:5052/api/graphrag/query -H 'Content-Type: application/json' -d '{"question":"What does Kafka use partitions for?"}'`

## Final Runtime Status
Stack is up and running.

Verified:
- `docker compose ps` shows all services up.
- Brokers are healthy.
- Schema Registry responds:
  - `GET http://localhost:8081/subjects` includes `credit-card-purchases-value`
  - `GET http://localhost:8081/subjects/credit-card-purchases-value/versions` -> includes `[1]` and later versions on schema edits
- Kafka UI responds:
  - `GET http://localhost:8080` -> HTTP `200`
- Producer UI responds:
  - `GET http://localhost:5050/api/status` -> HTTP `200`
  - `POST http://localhost:5050/api/send_once` -> sends Avro message
  - `POST http://localhost:5050/api/schema` -> registers updated schema version
- Consumer UI responds:
  - `GET http://localhost:5051` -> HTTP `200`
  - `GET http://localhost:5051/api/messages` -> consumed messages + latency
- Kafka Expert UI responds:
  - `GET http://localhost:5052` -> HTTP `200`
  - `GET http://localhost:5052/api/health` returns model credential/runtime state
  - `POST http://localhost:5052/api/cluster_state` runs full Prometheus-backed cluster assessment when model credentials are configured
  - `POST http://localhost:5052/api/chat` answers Kafka questions; non-Kafka prompts are refused by policy
  - `POST http://localhost:5052/api/chat/clear` clears chat history while keeping cluster state text
  - Cluster assessment and agent responses are concise numbered bullets with severity tags
  - `[WARN]`/`[BAD]` bullets include `Fix:` guidance and UI renders those `Fix:` segments in bold
- CouchDB responds and stores consumed records:
  - `GET http://localhost:5984/_up` -> `{"status":"ok",...}`
  - `GET http://localhost:5984/_all_dbs` includes `kafka_expert_consumed`
  - `GET http://localhost:5984/kafka_expert_consumed/_all_docs?include_docs=true` returns consumed docs
- JMX Prometheus exporter responds:
  - `GET http://localhost:9404/metrics` -> `kafka_jmx_attribute_value` and `kafka_jmx_exporter_*`
- Prometheus responds and shows scrape targets:
  - `GET http://localhost:9090/api/v1/targets` shows `kafka_jmx` and `node_exporter` targets `up`
- Grafana responds and dashboard is provisioned:
  - `GET http://localhost:3000/api/health` -> `database":"ok"`
  - `GET http://localhost:3000/api/search?query=Kafka%20JMX` includes `Kafka JMX Overview`
- JMX data is exposed to Prometheus only (no JMX file snapshots).

## How to Operate
- Start:
  - `docker compose up -d`
- Stop:
  - `docker compose down`
- View logs:
  - `docker compose logs -f`
- Verify CouchDB health:
  - `curl -u admin:demo http://localhost:5984/_up`
- Inspect consumed docs in CouchDB:
  - `curl -u admin:demo "http://localhost:5984/kafka_expert_consumed/_all_docs?include_docs=true"`
- Verify JMX exporter metrics:
  - `curl http://localhost:9404/metrics`
- Verify node_exporter metrics:
  - `curl http://localhost:9100/metrics`
- Verify Prometheus target health:
  - `curl http://localhost:9090/api/v1/targets`
- Verify Kafka Expert runtime:
  - `curl http://localhost:5052/api/health`
- Open Grafana dashboard:
  - `http://localhost:3000/d/kafka-jmx-overview/kafka-jmx-overview`
  - `http://localhost:3000/d/kafka-observability-control-plane/kafka-observability-control-plane`
- Start optional fixed-rate producer container:
  - `docker compose --profile rate-producer up -d producer-rate`
- Stop optional fixed-rate producer container:
  - `docker compose stop producer-rate`

## Replay Steps
1. `cd /Users/paulharvener/workspace/kafka-expert`
2. Set GenAI credentials in `.env`:
   - `OPENAI_API_KEY=...`
   - `OPENAI_MODEL=gpt-5.3` (or another available model)
   - optional `OPENAI_BASE_URL=...`
3. `docker compose up -d --build`
4. Open `http://localhost:5050`
5. Open `http://localhost:5051`
6. Open `http://localhost:5052`
7. Click `Send One Message` (green button) or set rate and click `Start Rate Send` in Producer UI
8. Optional: edit schema in Producer UI and click `Save + Register Schema`
9. Verify Consumer UI shows pretty JSON + `latency_ms`
10. In Kafka Expert UI, click `Query Full Cluster State` and ask Kafka-specific questions in chat
11. Verify CouchDB updates:
   - `curl -u admin:demo "http://localhost:5984/kafka_expert_consumed/_all_docs?include_docs=true"`

## Compose-Only Execution Policy
- Do not run Python scripts directly on host.
- All runtime executables are launched by Docker Compose services:
  - `schema-bootstrap`
  - `consumer-ui`
  - `producer-ui`
  - `kafka-expert-ui`
  - `producer-rate` (optional profile)
- MCP tool servers run as internal subprocesses of `kafka-expert-ui` (not separate host-executed processes):
  - `prometheus_mcp_server.py`
  - `kafka_admin_mcp_server.py`
