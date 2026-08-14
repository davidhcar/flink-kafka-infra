# Real-Time OHDSI OMOP Streaming ETL & Clinical Early Warning System (Flinkflow)

## 🎯 The Actual Use Case: "OMOP-StreamFlow"

### Real-Time Harmonization of Heterogeneous Healthcare Feeds & Instant Patient Alerting

Historically, the **OHDSI OMOP Common Data Model (CDM)** has been used for **retrospective research** on static relational databases (PostgreSQL, Snowflake, BigQuery). 

In acute clinical environments (ICUs, Emergency Departments), clinical data arrives continuously from **disparate, incompatible systems**:
1. Bedside telemetry monitors emitting local vendor strings.
2. Hospital EHR systems emitting LOINC lab codes.
3. Pharmacy dispense feeds emitting NDC drug codes.
4. EHR Billing/Diagnosis streams emitting ICD-10-CM codes.

**OMOP-StreamFlow** solves this problem by using **Talweg Flinkflow** to ingest raw heterogeneous streams, dynamically standardize them into standard OMOP Concept IDs using a low-latency Vocabulary Lookup Service, and fire **instant clinical alerts** in sub-2 milliseconds.

---

## 🏗️ 3-Step Stream Lifecycle & Destinations

```text
[4 DataGen Sources: Vitals, Labs, Meds, Dx] 
                     │
                     ▼
  [Step 2: Dynamic HTTP Vocab Lookup] ──> Kafka Topic: 'omop-standard-events'
        (map_to_omop_concept)
                     │
                     ▼
  [Step 4: Flink Tumbling Window (TUMBLE)]
    Aggregates all 4 feeds per Patient ID
                     │
                     ▼
  [Step 5: Multi-Source Clinical State & CDSS Alert] ──> Kafka Topic: 'omop-clinical-alerts' & Console Sink
```

| Pipeline Step | Description | Stream Destination |
| :--- | :--- | :--- |
| **Step 1: Multi-Source Generation** | Continuous event generation across 4 clinical feeds (`LOINC Vitals`, `Legacy Labs`, `Pharmacy NDC`, `EHR Diagnoses`) for an active inpatient cohort | Pipeline Stream |
| **Step 2: OMOP Standardization** | Real-time dynamic HTTP lookup against Vocabulary Service mapping raw codes to standard OMOP Concept IDs (`concept_id`, `domain_id`) | **Kafka Topic 1 (`omop-standard-events`)** |
| **Step 4: Windowed Clinical CDSS Engine** | Flink SQL 10-second Tumbling Window aggregating all feeds per patient, assessing multi-source clinical risk (Septic Shock, Sepsis Risk, Crises) | **Kafka Topic 2 (`omop-clinical-alerts`) & Console** |

---

## ⚡ How Data Streaming Works

### Stream Execution Architecture
1. **Asynchronous Push-Based Topology**: Flinkflow executes inside Flink TaskManager worker slots. Data generators push events into the stream continuously without blocking.
2. **Pure Flink SQL Alert Engine**: Clinical alert rules are evaluated natively inside Flink SQL (`clinical_alerts`), guaranteeing sub-millisecond execution and zero `null` record emissions.
3. **Automated Topic Reset**: Running `mise run omop:run-demo` automatically purges both `omop-standard-events` and `omop-clinical-alerts` topics prior to pipeline launch.

### Raw Healthcare Data Formats & Real-World EHR Standards

Each synthetic data generator simulates production clinical systems and is verified against official healthcare interoperability standards:

| Source Feed | Medical Domain Icon | Real-World Healthcare System & EHR Standard | Standard Vocabulary | Verified Clinical Codes & Domains |
| :--- | :---: | :--- | :--- | :--- |
| **Source A (`ehr_vitals`)** | 🩺 | **Epic EHR Bedside Vitals (HL7 FHIR `Observation`)** | **LOINC** | Vitals & Labs (`8480-6` Systolic BP, `2339-0` Glucose, `2518-8` Lactate) |
| **Source B (`legacy_lab`)** | 🧪 | **Legacy Hospital LIS Analyzer (HL7 v2.x ORU^R01)** | **`LEGACY_LAB_LOCAL`** | Proprietary Local LIS (`GLUC-STAT`, `K-PANEL`) |
| **Source C (`pharmacy_feed`)** | 💊 | **Automated Pharmacy Dispenser (NCPDP / FDA NDC)** | **NDC** | Inpatient Meds (`00093-7146-01` Ceftriaxone, `00186-0771-31` Vancomycin) |
| **Source D (`condition_feed`)** | 📋 | **Epic EHR Diagnosis Stream (HL7 FHIR `Condition`)** | **ICD10CM** | Diagnoses (`A41.9` Sepsis, `R65.20` Severe Sepsis) |

---

## 🚨 Real-Time Clinical Decision Support (CDSS) Alert Engine

### Unified Rule Evaluation on Standard OMOP Concepts
Standardizing raw codes into OMOP `standard_concept_id` simplifies alert logic:

| Raw Code Incoming | Vocabulary | Mapped OMOP Concept ID | OMOP Concept Name | Alert Trigger Condition | Clinical Condition |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `2339-0` | LOINC | **`3004501`** | Glucose in Blood | `value > 180` | **Severe Hyperglycemia** |
| `GLUC-STAT` | LEGACY_LAB_LOCAL | **`3004501`** | Glucose in Blood | `value > 180` | **Severe Hyperglycemia** |
| `8480-6` | LOINC | **`3027018`** | Systolic Blood Pressure | `value > 140` | **High Systolic Blood Pressure** |
| `2518-8` | LOINC | **`3006615`** | Blood Lactate | `value > 2.0` | **Hyperlactatemia / Sepsis Risk** |
| `A41.9` | ICD10CM | **`132302`** | Sepsis, unspecified | Active Diagnosis | **Active Sepsis Diagnosis** |

> 💡 **Why this matters**: Instead of writing separate alert rules for LOINC `2339-0`, Legacy `GLUC-STAT`, and proprietary EHR lab IDs, clinicians define **one rule on OMOP Concept `3004501`**.

---

## 📊 Sample Output per Step

### Step 1: Console Output (`stdout` with Medical Icons & ANSI Colors)
```text
🩺 [Source A (EHR Vitals Feed)]                              (Bright Cyan)
   Patient: MRN1001  | Vocab: LOINC            | Code: 8480-6       | Value: 155 mmHg
   Timestamp: 2026-07-29T02:17:00Z

🧪 [Source B (Legacy Lab Analyzer)]                           (Bright Yellow)
   Patient: P-77821  | Vocab: LEGACY_LAB_LOCAL | Code: GLUC-STAT    | Value: 185
   Timestamp: 2026-07-29T02:17:00Z

💊 [Source C (Pharmacy Dispense Feed)]                        (Bright Green)
   Patient: PAT-556  | Vocab: NDC              | Code: 00093-7146-01| Value: 1 dose
   Timestamp: 2026-07-29T02:17:00Z

📋 [Source D (EHR Diagnoses Feed)]                           (Bright Magenta)
   Patient: PAT-901  | Vocab: ICD10CM          | Code: A41.9        | Value: Sepsis, unspecified
   Timestamp: 2026-07-29T02:17:00Z
```

### Step 2: Converted OMOP Standard Data Payload (`omop-standard-events`)
```json
{
  "person_source_value": "MRN1001",
  "source_vocabulary": "LOINC",
  "source_code": "8480-6",
  "value": "155",
  "unit": "mmHg",
  "ts": "2026-07-29T01:51:00Z",
  "standard_concept_id": 3027018,
  "standard_concept_name": "Systolic blood pressure",
  "domain_id": "Measurement"
}
```

### Step 3: Flink Clinical Decision Support Alert Payload (`omop-clinical-alerts`)
```json
{
  "patient_id": "MRN1001",
  "source_vocabulary": "LOINC",
  "source_code": "8480-6",
  "observed_value": "155",
  "unit": "mmHg",
  "event_time": "2026-07-29T01:51:00Z",
  "standard_concept_id": 3027018,
  "standard_concept_name": "Systolic blood pressure",
  "domain_id": "Measurement",
  "clinical_condition": "High Systolic Blood Pressure",
  "clinical_finding": "Systolic BP exceeds threshold (> 140 mmHg)",
  "alert_severity": "CRITICAL",
  "natural_language_summary": "Patient MRN1001 exhibited High Systolic Blood Pressure with recorded value of 155 mmHg (OMOP Concept #3027018 'Systolic blood pressure').",
  "processing_latency": "SUB-MILLISECOND (<2ms)"
}
```

---

## 🛠 Project Structure

```
flinkflow-jobs/hybrid/omop/
├── omop-vocab-mapping-demo.yaml  # Flinkflow YAML pipeline (Data-Gen + Vocab Lookup + Flink SQL Alerts)
├── vocab_service.py              # Zero-dependency OMOP Vocabulary Lookup Service (Port 8082)
└── README.md                     # This documentation & demo guide
```

---

## 🚀 Commands to Run the Prototype

### 1. Start Infrastructure & OMOP Vocabulary Service
Starts Kafka, Flink, Postgres, and the OMOP Vocabulary Microservice (Port 8082):
```bash
mise run docker:start:flinkflow
```

### 2. Run the Flinkflow OMOP Pipeline
Executes the hybrid (Python + SQL + HTTP) inline data-gen, vocabulary mapping, and clinical alert pipeline:
```bash
mise run omop:run-demo
```

### 3. Verify Vocabulary Service Directly (Optional)
```bash
# Test LOINC lookup (Systolic BP)
curl "http://localhost:8082/maps-to?vocab=LOINC&code=8480-6"

# Test Legacy Lab lookup (GLUC-STAT)
curl "http://localhost:8082/maps-to?vocab=LEGACY_LAB_LOCAL&code=GLUC-STAT"

# Test NDC Drug lookup (Metformin)
curl "http://localhost:8082/maps-to?vocab=NDC&code=00093-7146-01"

# Test ICD-10-CM Condition lookup (Sepsis)
curl "http://localhost:8082/maps-to?vocab=ICD10CM&code=A41.9"
```
