# Flink Fintech POC

The goal is to try build simple real-time pipeline with PostgreSQL, Kafka, Kafka Connect, Debezium and Flink. 

## Project Structure

- `docker/` - Docker Compose setup for PostgreSQL, Kafka, Kafka Connect (Debezium), and Kafka UI
- `flinkfintechpoc/` - Original Flink example with a simple word count job
- `flink-jobs/` - Flink jobs for real-time data processing

## Flink Jobs

### Customer Analytics Job

This job reads from the `customers` Kafka topic and calculates the number of customers created per minute.

#### How to Build

```bash
cd flink-jobs
mvn clean package
```

#### How to Run

You can run the job using one of the following methods:

1. Using Flink CLI:
```bash
flink run flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar
```

2. Using mise task (recommended):
```bash
mise run flink-customer-analytics
```
This command will automatically build the jar and then run the Flink job.

The job will connect to Kafka and start processing customer data in real-time, outputting the count of customers created per minute.

#### Viewing Results

You can view the job results in the Flink Dashboard at http://localhost:8081 after starting the job.

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
Local connection details and topic names are defined in [`env.local`](file:///t:/home/dmichael/0Talweg/OS/flink-kafka-infra/env.local), which is automatically loaded by `mise` (via `[env]` in `mise.toml`) and passed to `ff-jobmanager` / `ff-taskmanager` via `docker-compose.yml`:

- `KAFKA_BOOTSTRAP_SERVERS`: `kafka:29092` (container) / `localhost:9092` (host)
- `SCHEMA_REGISTRY_URL`: `http://schema-registry:8081`
- `POSTGRES_URL`: `jdbc:postgresql://postgres:5432/outbox_demo`
- `VOCAB_SERVICE_URL`: `http://vocab-service:8082`

### Production (Kubernetes Pattern)
In Kubernetes deployments, variables are injected into Flinkflow pods via Secrets and ConfigMaps, and referenced in DSL YAMLs using standard placeholders (e.g., `${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}`) or `secret:secret-name/key`.
