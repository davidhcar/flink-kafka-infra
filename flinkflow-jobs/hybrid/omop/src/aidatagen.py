"""
Multi-Source Healthcare Clinical Event Generator (Google Gemini + Deterministic Persona Fallback)
-------------------------------------------------------------------------------------------------
Generates balanced clinical events across Bedside Vitals (LOINC), LIS Labs (LEGACY_LAB_LOCAL),
Pharmacy Dispensers (NDC), and EHR Problem Lists (ICD-10-CM) for an acute ICU cohort.
"""

import json
import os
import random
import urllib.request
import urllib.error
from datetime import datetime
from typing import Dict, Any, List, Optional

COHORT: List[str] = ["PATIENT-101", "PATIENT-102", "PATIENT-103", "PATIENT-104", "PATIENT-105"]


def fetch_gemini_synthetic_batch(
    gemini_key: str,
    model: str = "gemini-2.0-flash",
    timeout: float = 4.0
) -> List[Dict[str, Any]]:
    """Calls Google AI Studio Gemini API to generate a batch of 12 balanced clinical events."""
    if not gemini_key:
        return []

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={gemini_key}"
    prompt = (
        "You are a Realistic Healthcare Clinical Data Simulator for an acute ICU cohort.\n"
        "Generate a JSON list of 12 clinical streaming events with an EQUAL, BALANCED mix across ALL 4 clinical sources:\n"
        "1. Vitals (3 items): source_vocabulary='LOINC', source_name='Source A (Bedside Vitals Telemetry)', source_code in ['8480-6', '8867-4'], values like '82', '185', '130', units 'mmHg' or 'bpm'\n"
        "2. Labs (3 items): source_vocabulary='LEGACY_LAB_LOCAL', source_name='Source B (LIS Blood Gas & Chemistry Analyzer)', source_code in ['LAB_LACT_01', 'LAB_POT_02', 'LAB_GLUC_03'], values like '4.2', '5.8', '260', units 'mmol/L' or 'mg/dL'\n"
        "3. Pharmacy (3 items): source_vocabulary='NDC', source_name='Source C (Inpatient Pharmacy Dispenser)', source_code in ['00069-3150-83', '00002-1433-61', '00409-7332-01'], values like '20', '8', '1', units 'mcg/min' or 'units' or 'dose'\n"
        "4. EHR Diagnoses (3 items): source_vocabulary='ICD10CM', source_name='Source D (EHR Encounter Problem List)', source_code in ['R65.21', 'I10', 'E11.9'], value='Active', unit='dx'\n\n"
        "Distribute across PATIENT-101 (Septic Shock), PATIENT-102 (Hypertensive Urgency), PATIENT-103 (Hyperkalemia), PATIENT-104 (Hyperglycemia), PATIENT-105 (Stable).\n"
        "Output strictly valid JSON array of objects with keys: person_source_value, source_name, source_vocabulary, source_code, value, unit."
    )
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"response_mime_type": "application/json", "temperature": 0.7}
    }
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            res_data = json.loads(resp.read().decode("utf-8"))
            text_out = res_data["candidates"][0]["content"]["parts"][0]["text"].strip()
            if text_out.startswith("```"):
                text_out = text_out.split("```")[1]
                if text_out.startswith("json"):
                    text_out = text_out[4:]
                text_out = text_out.strip()
            items = json.loads(text_out)
            if isinstance(items, list) and len(items) > 0:
                return items
    except Exception:
        pass
    return []


def generate_fallback_cohort_batch() -> List[Dict[str, Any]]:
    """Generates a balanced multi-source batch across all 5 ICU cohort patient personas."""
    batch: List[Dict[str, Any]] = []
    for pid in COHORT:
        if pid == "PATIENT-101":
            batch.append({"person_source_value": pid, "source_name": "Source A (Bedside Vitals Telemetry)", "source_vocabulary": "LOINC", "source_code": "8480-6", "value": str(random.randint(75, 86)), "unit": "mmHg"})
            batch.append({"person_source_value": pid, "source_name": "Source B (LIS Blood Gas & Chemistry Analyzer)", "source_vocabulary": "LEGACY_LAB_LOCAL", "source_code": "LAB_LACT_01", "value": f"{random.uniform(3.5, 5.2):.1f}", "unit": "mmol/L"})
            batch.append({"person_source_value": pid, "source_name": "Source C (Inpatient Pharmacy Dispenser)", "source_vocabulary": "NDC", "source_code": "00069-3150-83", "value": str(random.randint(15, 25)), "unit": "mcg/min"})
            batch.append({"person_source_value": pid, "source_name": "Source D (EHR Encounter Problem List)", "source_vocabulary": "ICD10CM", "source_code": "R65.21", "value": "Active", "unit": "dx"})
        elif pid == "PATIENT-102":
            batch.append({"person_source_value": pid, "source_name": "Source A (Bedside Vitals Telemetry)", "source_vocabulary": "LOINC", "source_code": "8480-6", "value": str(random.randint(175, 195)), "unit": "mmHg"})
            batch.append({"person_source_value": pid, "source_name": "Source D (EHR Encounter Problem List)", "source_vocabulary": "ICD10CM", "source_code": "I10", "value": "Active", "unit": "dx"})
        elif pid == "PATIENT-103":
            batch.append({"person_source_value": pid, "source_name": "Source B (LIS Blood Gas & Chemistry Analyzer)", "source_vocabulary": "LEGACY_LAB_LOCAL", "source_code": "LAB_POT_02", "value": f"{random.uniform(5.8, 6.6):.1f}", "unit": "mmol/L"})
            batch.append({"person_source_value": pid, "source_name": "Source C (Inpatient Pharmacy Dispenser)", "source_vocabulary": "NDC", "source_code": "00002-1433-61", "value": "8", "unit": "units"})
        elif pid == "PATIENT-104":
            batch.append({"person_source_value": pid, "source_name": "Source B (LIS Blood Gas & Chemistry Analyzer)", "source_vocabulary": "LEGACY_LAB_LOCAL", "source_code": "LAB_GLUC_03", "value": str(random.randint(245, 320)), "unit": "mg/dL"})
            batch.append({"person_source_value": pid, "source_name": "Source D (EHR Encounter Problem List)", "source_vocabulary": "ICD10CM", "source_code": "E11.9", "value": "Active", "unit": "dx"})
        else:
            batch.append({"person_source_value": pid, "source_name": "Source A (Bedside Vitals Telemetry)", "source_vocabulary": "LOINC", "source_code": "8480-6", "value": str(random.randint(118, 124)), "unit": "mmHg"})
            batch.append({"person_source_value": pid, "source_name": "Source B (LIS Blood Gas & Chemistry Analyzer)", "source_vocabulary": "LEGACY_LAB_LOCAL", "source_code": "LAB_LACT_01", "value": "1.1", "unit": "mmol/L"})
    random.shuffle(batch)
    return batch


def process_event(input_tick: str = "") -> str:
    """Flinkflow operator entrypoint: processes a tick and returns a serialized JSON event."""
    if not hasattr(process_event, "buffer"):
        process_event.buffer = []

    gemini_key = os.environ.get("GEMINI_API_KEY", "").strip()
    model = os.environ.get("LLM_MODEL", "gemini-2.0-flash")
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Refill buffer if empty
    if not process_event.buffer:
        if gemini_key:
            process_event.buffer.extend(fetch_gemini_synthetic_batch(gemini_key, model=model))

        if not process_event.buffer:
            process_event.buffer.extend(generate_fallback_cohort_batch())

    event = process_event.buffer.pop(0) if process_event.buffer else {
        "person_source_value": "PATIENT-101",
        "source_name": "Source A (Bedside Vitals Telemetry)",
        "source_vocabulary": "LOINC",
        "source_code": "8480-6",
        "value": "82",
        "unit": "mmHg"
    }
    event["event_time"] = now_str
    return json.dumps(event)


if __name__ == "__main__":
    print(process_event("{}"))


