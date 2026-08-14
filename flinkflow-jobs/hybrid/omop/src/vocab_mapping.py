"""
OHDSI OMOP Dynamic Concept Harmonizer & Normalizer
--------------------------------------------------
Queries the vocab-service microservice on port 8082 to map source vocabularies
and source codes to standard OMOP Concept IDs and Domains.
"""

import json
import os
import urllib.request
import urllib.parse
from datetime import datetime
from typing import Dict, Any, Tuple


def resolve_omop_concept(
    source_vocab: str,
    source_code: str,
    vocab_service_url: str = "http://vocab-service:8082",
    timeout: float = 2.0
) -> Tuple[int, str, str]:
    """Queries vocab service /maps-to endpoint. Returns (concept_id, concept_name, domain_id)."""
    default_id = 0
    default_name = f"Unmapped ({source_vocab}:{source_code})"
    default_domain = "Observation"

    if not source_vocab or not source_code:
        return default_id, default_name, default_domain

    clean_vocab = source_vocab.strip().upper()
    clean_code = source_code.strip()
    params = urllib.parse.urlencode({"vocab": clean_vocab, "code": clean_code})
    url = f"{vocab_service_url.rstrip('/')}/maps-to?{params}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Flinkflow-OMOP-Pipeline"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            cid = int(data.get("concept_id", data.get("standard_concept_id", default_id)))
            cname = str(data.get("concept_name", data.get("standard_concept_name", default_name)))
            domain = str(data.get("domain_id", default_domain))
            return cid, cname, domain
    except Exception:
        return default_id, default_name, default_domain


def infer_source_name(source_vocab: str, raw_name: str = "") -> str:
    """Infers standard descriptive source name based on vocabulary taxonomy."""
    clean_name = raw_name.strip()
    if clean_name and clean_name != "UNKNOWN":
        return clean_name

    vocab_upper = source_vocab.strip().upper()
    if vocab_upper == "LOINC":
        return "Source A (Bedside Vitals Telemetry)"
    elif vocab_upper in ("LEGACY_LAB_LOCAL", "LAB", "LOCAL_LAB"):
        return "Source B (LIS Blood Gas & Chemistry Analyzer)"
    elif vocab_upper == "NDC":
        return "Source C (Inpatient Pharmacy Dispenser)"
    elif vocab_upper == "ICD10CM":
        return "Source D (EHR Encounter Problem List)"
    return "Source A (Bedside Vitals Telemetry)"


def map_record(input_raw: str) -> str:
    """Flinkflow operator entrypoint: maps a raw event to a standard OMOP CDM event."""
    try:
        record: Dict[str, Any] = json.loads(input_raw)
    except Exception:
        record = {}

    source_vocab = str(record.get("source_vocabulary", "")).strip().upper()
    source_code = str(record.get("source_code", "")).strip()
    source_name = infer_source_name(source_vocab, str(record.get("source_name", "")))

    vocab_service_url = os.environ.get("VOCAB_SERVICE_URL", "http://vocab-service:8082")
    standard_concept_id, standard_concept_name, domain_id = resolve_omop_concept(
        source_vocab, source_code, vocab_service_url=vocab_service_url
    )

    raw_val = record.get("value", "0.0")
    try:
        val_num = float(raw_val)
    except (ValueError, TypeError):
        val_num = 0.0

    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    ev_time = str(record.get("event_time", "")).strip()
    if not ev_time:
        ev_time = now_str

    omop_event = {
        "person_source_value": str(record.get("person_source_value", "UNKNOWN")),
        "source_name": source_name,
        "source_vocabulary": source_vocab,
        "source_code": source_code,
        "value": str(raw_val),
        "value_num": val_num,
        "unit": str(record.get("unit", "")),
        "event_time": ev_time,
        "standard_concept_id": standard_concept_id,
        "standard_concept_name": standard_concept_name,
        "domain_id": domain_id
    }
    return json.dumps(omop_event)


if __name__ == "__main__":
    print(map_record("{}"))

