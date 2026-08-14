# Flink Streaming & Data Infrastructure Platform

Real-time streaming platform powered by **Apache Flink**, **Flinkflow (YAML DSL)**, **Apache Kafka**, **Confluent Schema Registry**, **PostgreSQL (CDC)**, and **ClickHouse**.

## Quick Start & Running Pipelines

### 1. Start Infrastructure
```bash
mise run docker:start:flinkflow
```

### 2. Submit Flinkflow Pipelines
Submit stream processing pipelines defined in `flinkflow-jobs/`:
```bash
# Run any YAML pipeline (e.g. Java, Python, SQL, Hybrid)
mise run ff:run hybrid/omop/omop-vocab-mapping-demo

# Or run interactively using the pipeline picker
mise run ff:select
```

### 3. Stream & Inspect Logs
```bash
# Stream Flink TaskManager logs
mise run flink:logs

# Open Web UIs
mise run open:flink-ui         # Flink Dashboard (port 8081)
mise run open:kafka-ui         # Kafka UI (port 8080)
mise run open:schema-registry  # Schema Registry (port 8084)
```

## Infrastructure Setup

The project includes a Docker Compose setup with:

- PostgreSQL with logical replication enabled
- Kafka
- Kafka Connect with Debezium for Change Data Capture
- Kafka UI for monitoring

To start the infrastructure:

```bash
# Using Docker Compose directly
cd docker
docker-compose up -d

# Or using mise tasks
mise run docker:start:flinkflow  # For Kafka & Flink infrastructure
mise run docker:start:langfuse   # For Langfuse LLM monitoring infrastructure
```

### Connecting to PostgreSQL

You can connect to the PostgreSQL database using the following mise task:

```bash
mise run postgres:connect
```

This will open a psql session connected to the PostgreSQL instance running in Docker.

## Environment Configuration

All connection environment variables are centrally managed:

### Local Development (`env.local`)
Local connection details and topic names are defined in [`env.local`](env.local), which is automatically loaded by `mise` (via `[env]` in `mise.toml`) and passed to `ff-jobmanager` / `ff-taskmanager` via `docker-compose.yml`:

- `KAFKA_BOOTSTRAP_SERVERS`: `kafka:29092` (container) / `localhost:9092` (host)
- `SCHEMA_REGISTRY_URL`: `http://schema-registry:8081`
- `POSTGRES_URL`: `jdbc:postgresql://postgres:5432/outbox_demo`
- `VOCAB_SERVICE_URL`: `http://vocab-service:8082`

### Production (Kubernetes Pattern)
In Kubernetes deployments, variables are injected into Flinkflow pods via Secrets and ConfigMaps, and referenced in DSL YAMLs using standard placeholders (e.g., `${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}`) or `secret:secret-name/key`.

## Schema Registry & Data Contracts

All event streams are governed by formal schemas registered with **Confluent Schema Registry**:
- See the complete developer guide: [`schemas/README.md`](schemas/README.md)
- Register schemas: `mise run schema:register:omop`
- Web UI: `http://localhost:8084/subjects` or via Kafka UI at `http://localhost:8080`
