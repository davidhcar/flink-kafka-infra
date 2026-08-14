# 📜 Flink & Schema Registry Technical Architecture & Developer Guide

This document is a technical guide for creating, governing, and deploying **stream processing pipelines** in Apache Flink and Flinkflow with data contract governance powered by **Confluent Schema Registry**.

---

## 🏛️ System Architecture

```text
[ Data Generators / Ingestion Sources ]
                  │
                  ▼
      [ Kafka Input Topics ]
                  │
                  ▼
[ Apache Flink / Flinkflow Stream Processing ]
   ├── Schema-Enforced Typed Processing (SQL DDL / Schema Properties)
   ├── Event-Time Watermarking & Window Aggregations (Tumbling / Sliding)
   └── Stateful Business Logic (Java / Python / SQL)
                  │
                  ▼
      [ Kafka Output Topics ] ──(Subject: <topic>-value)──> [ Confluent Schema Registry ]
                  │                                            (Port 8081 Cluster / 8084 Host)
                  ▼
[ Downstream Microservices / Data Warehouse / Dashboards ]
```

### Core Infrastructure Endpoints

| Service | In-Cluster Network | Host Network | Purpose / UI |
| :--- | :--- | :--- | :--- |
| **Confluent Schema Registry** | `http://schema-registry:8081` | `http://localhost:8084` | REST API / Schema Catalog (`/subjects`) |
| **Apache Kafka Broker** | `kafka:29092` | `localhost:9092` | Distributed streaming backbone |
| **Kafka UI** | `http://kafka-ui:8080` | `http://localhost:8080` | Topic browser & consumer inspection |
| **Flink JobManager** | `http://ff-jobmanager:8081` | `http://localhost:8081` | Flink Web Dashboard & job topology |

---

## 📋 Confluent Subject Naming Strategy

By default, the system adheres to the standard **`TopicNameStrategy`**:
* **Message Value Schema**: `<topic-name>-value` (e.g. `order-events-value`, `telemetry-metrics-value`)
* **Message Key Schema** (Optional): `<topic-name>-key`

Schema files in the `schemas/` directory are mapped automatically to subjects based on their filename:
```text
schemas/
├── <topic_a>-value.json     ──> Subject: '<topic_a>-value' (SchemaType: JSON)
├── <topic_b>-value.avsc     ──> Subject: '<topic_b>-value' (SchemaType: AVRO)
└── <topic_c>-value.proto    ──> Subject: '<topic_c>-value' (SchemaType: PROTOBUF)
```

---

## 🛠️ Step-by-Step Pipeline Creation with Schema Governance

### Step 1: Define the Data Contract (`schemas/`)

Create your formal schema definition in the `schemas/` folder using **JSON Schema**, **Apache Avro**, or **Protobuf**.

#### Example A: JSON Schema (`schemas/order-events-value.json`)
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "OrderEvent",
  "description": "Standard eCommerce order transaction event",
  "type": "object",
  "required": ["order_id", "customer_id", "event_time", "amount", "currency", "status"],
  "properties": {
    "order_id": { "type": "string" },
    "customer_id": { "type": "string" },
    "event_time": { "type": "string", "description": "ISO timestamp (YYYY-MM-DD HH:MM:SS)" },
    "amount": { "type": "number" },
    "currency": { "type": "string", "enum": ["USD", "EUR", "GBP", "JPY"] },
    "status": { "type": "string", "enum": ["PENDING", "COMPLETED", "FAILED", "CANCELLED"] },
    "item_count": { "type": "integer" }
  }
}
```

#### Example B: Apache Avro Schema (`schemas/device-telemetry-value.avsc`)
```json
{
  "type": "record",
  "name": "DeviceTelemetry",
  "namespace": "com.company.iot",
  "doc": "IoT device sensor telemetry stream",
  "fields": [
    { "name": "device_id", "type": "string" },
    { "name": "sensor_type", "type": "string" },
    { "name": "reading_value", "type": "double" },
    { "name": "unit", "type": "string" },
    { "name": "timestamp", "type": "string" }
  ]
}
```

---

### Step 2: Register Schemas with Schema Registry

The universal registration tool ([`scripts/register_schemas.py`](../scripts/register_schemas.py)) automatically detects file extensions, generates valid payload envelopes, and posts them to Schema Registry.

#### Option 1: Auto-Discover & Register ALL Schemas in `schemas/`
```bash
# Using mise
mise run schema:register:all

# Or using Python directly
python scripts/register_schemas.py
```

#### Option 2: Register a Specific Schema File or Pattern
```bash
# Register a specific file
mise run schema:register "order-events-value.json"
python scripts/register_schemas.py "schemas/order-events-value.json"

# Register via glob pattern
mise run schema:register "device-*"
python scripts/register_schemas.py "device-*"
```

#### Option 3: Verify Registered Subjects
```bash
# List all registered subjects
mise run schema:list
# or: curl -s http://localhost:8084/subjects

# Fetch latest schema version for a subject
curl -s http://localhost:8084/subjects/order-events-value/versions/latest
```

---

### Step 3: Configure Flinkflow Job YAML with Schema & Watermarks

In your pipeline YAML (located in `flinkflow-jobs/`), declare explicit types under the `schema.<source_step>.<column>` properties and set event-time watermarking:

```yaml
name: "Order Processing & Windowed Aggregation Pipeline"
parallelism: 1

steps:
  # 1. Ingest from Kafka Source
  - type: source
    name: order_source
    connector: kafka-source
    properties:
      topic: "${ORDER_SOURCE_TOPIC:-order-events}"
      bootstrap.servers: "${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
      group.id: "flinkflow-order-processors"
      scan.startup.mode: "latest-offset"

  # 2. Schema Definition & Event-Time Window Aggregation in Flink SQL
  - type: sql
    name: windowed_aggregates
    inputs: [order_source]
    properties:
      # Explicit Column Types
      schema.order_source.order_id: "string"
      schema.order_source.customer_id: "string"
      schema.order_source.event_time: "timestamp"
      schema.order_source.amount: "double"
      schema.order_source.currency: "string"
      schema.order_source.status: "string"
      schema.order_source.item_count: "int"
      
      # Watermark Declaration (2-second bounded out-of-orderness)
      watermark.order_source.column: "event_time"
      watermark.order_source.delay: "2"

      # Tumbling Window Query
      query: |
        SELECT 
          customer_id,
          CAST(TUMBLE_START(event_time, INTERVAL '30' SECOND) AS STRING) AS window_start,
          CAST(TUMBLE_END(event_time, INTERVAL '30' SECOND) AS STRING) AS window_end,
          COUNT(*) AS total_orders,
          SUM(amount) AS total_spend,
          MAX(amount) AS max_single_order,
          AVG(amount) AS avg_order_value,
          CASE 
            WHEN SUM(amount) > 10000.0 THEN 'VIP_TIER_1'
            WHEN SUM(amount) > 2500.0 THEN 'PREFERRED_TIER_2'
            ELSE 'STANDARD'
          END AS customer_tier
        FROM order_source
        WHERE status = 'COMPLETED'
        GROUP BY 
          TUMBLE(event_time, INTERVAL '30' SECOND),
          customer_id

  # 3. Serialize to Clean JSON Payload (matching Schema Registry contract)
  - type: process
    name: format_output_payload
    language: python
    inputs: [windowed_aggregates]
    code: |
      import json
      try:
          d = json.loads(input) if isinstance(input, str) else input
          payload = {
              "customer_id": str(d.get("customer_id", "UNKNOWN")),
              "window_start": str(d.get("window_start", "")),
              "window_end": str(d.get("window_end", "")),
              "total_orders": int(d.get("total_orders", 0)),
              "total_spend": float(d.get("total_spend", 0.0)),
              "max_single_order": float(d.get("max_single_order", 0.0)),
              "avg_order_value": float(d.get("avg_order_value", 0.0)),
              "customer_tier": str(d.get("customer_tier", "STANDARD"))
          }
          return json.dumps(payload)
      except Exception:
          return json.dumps({"error": "Serialization failed", "raw": str(input)})

  # 4. Kafka Output Sink (Clean Structured Records)
  - type: sink
    name: output_kafka_sink
    connector: kafka-sink
    inputs: [format_output_payload]
    properties:
      topic: "${PROCESSED_ORDERS_TOPIC:-order-aggregates}"
      bootstrap.servers: "${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"

  # 5. Console Output Formatter (Human-Readable Visualization ONLY)
  - type: process
    name: format_console_card
    language: python
    inputs: [format_output_payload]
    code: |
      import json
      d = json.loads(input)
      cid = d.get("customer_id", "UNKNOWN")
      spend = f"${d.get('total_spend', 0.0):,.2f}"
      tier = d.get("customer_tier", "STANDARD")
      return f"\n\033[1;32m[AGGREGATE]\033[0m Customer: {cid:<12} | Spend: {spend:<12} | Tier: {tier}\n"

  # 6. Live Terminal Console Sink (stdout)
  - type: sink
    name: console_sink
    connector: console
    inputs: [format_console_card]
```

---

## 🛡️ Critical Design Pattern: Separating Kafka Payloads from Terminal UI

To ensure Schema Registry compliance and prevent corrupting downstream consumers:
1. **Never sink ANSI-formatted strings (terminal colors/cards) into Kafka topics.**
2. Always route raw SQL output into a **JSON serialization step** that outputs valid records conforming to the registered schema.
3. Use a secondary process step solely to transform that JSON into human-readable terminal output for the `console` sink.

---

## 🔄 Schema Evolution & Compatibility Management

Confluent Schema Registry enforces data compatibility rules as pipelines evolve over time.

### Compatibility Modes

| Mode | Changes Allowed | Verification Rule |
| :--- | :--- | :--- |
| **`BACKWARD`** *(Default)* | Add optional fields, delete optional fields | Consumers with new schema can read old data |
| **`FORWARD`** | Add required fields, delete optional fields | Consumers with old schema can read new data |
| **`FULL`** *(Recommended for microservices)* | Add optional fields with defaults, delete optional fields | Both backward and forward compatible |
| **`NONE`** | Any change allowed | Disables compatibility checks |

### Setting Compatibility via REST API
```bash
# Check current compatibility level for a subject
curl -s http://localhost:8084/config/<subject-name>

# Update subject to FULL compatibility
curl -X PUT -H "Content-Type: application/json" \
  --data '{"compatibility": "FULL"}' \
  http://localhost:8084/config/<subject-name>
```

### Schema Best Practices
1. **Always provide default values** for newly added fields.
2. **Never rename existing required fields** (use SQL `AS` aliases in the stream instead).
3. **Never change field data types** without creating a new topic/subject version.
4. **Use explicit type mappings** in Flink SQL DDL (`string`, `double`, `int`, `bigint`, `timestamp`, `boolean`).

---

## 🔍 Validation & Troubleshooting

### 1. View Registered Schemas in Kafka UI
Open Kafka UI at **`http://localhost:8080`** and click **Schema Registry** in the sidebar to view all subjects, version diffs, and compatibility settings.

### 2. Inspect Live Stream Payloads
```bash
# Consume raw JSON payloads directly from broker
docker compose -f docker/docker-compose.yml exec -T kafka \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic <topic-name> --from-beginning
```

### 3. Check Schema Compatibility with CLI
```bash
# Test if a modified schema is compatible before registering
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schemaType": "JSON", "schema": "..."}' \
  http://localhost:8084/compatibility/subjects/<subject-name>/versions/latest
```
