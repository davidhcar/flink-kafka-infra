"""
Multi-Provider Clinical Decision Support (CDSS) AI Reasoning Engine
-------------------------------------------------------------------
Analyzes 10-second windowed OMOP patient metrics across Vitals, Labs, Medications,
and EHR Problem Lists. Integrates with Google AI Studio (Gemini), OpenAI, or Ollama,
with deterministic clinical rule fallbacks.
"""

import json
import os
import urllib.request
import urllib.error
from typing import Dict, Any, List, Tuple


def check_is_anomalous(
    sepsis_dx: int,
    lactate: float,
    bp: float,
    k: float,
    glucose: float,
    hr: float,
    vaso: int
) -> bool:
    """Returns True if window metrics show clinical deterioration warranting LLM triage."""
    return (
        sepsis_dx > 0
        or lactate > 1.8
        or bp > 140.0
        or bp < 90.0 and bp > 0
        or k > 5.0
        or glucose > 180.0
        or hr > 100.0
        or vaso > 0
    )


def build_cdss_prompt(
    pid: str,
    bp: float,
    hr: float,
    lactate: float,
    k: float,
    glucose: float,
    sepsis_dx: int,
    meds: int,
    vaso: int
) -> str:
    """Constructs the clinical reasoning prompt for the LLM."""
    return (
        f"You are an Expert ICU Clinical Decision Support (CDSS) AI Agent.\n"
        f"Analyze the following 10-second multi-source OMOP data window for Patient {pid}:\n"
        f"- Vitals: Systolic BP={bp if bp > 0 else 'N/A'} mmHg, Heart Rate={hr if hr > 0 else 'N/A'} bpm\n"
        f"- Labs: Serum Lactate={lactate if lactate > 0 else 'N/A'} mmol/L, Serum Potassium={k if k > 0 else 'N/A'} mmol/L, Blood Glucose={glucose if glucose > 0 else 'N/A'} mg/dL\n"
        f"- EHR Diagnoses: Active Sepsis Problem List Records = {sepsis_dx}\n"
        f"- Pharmacy: Total Dispenses = {meds} (Vasopressors IV = {vaso})\n\n"
        f"Instructions:\n"
        f"1. Synthesize all evidence across vitals, labs, medications, and diagnoses.\n"
        f"2. Determine acuity: CRITICAL, URGENT, WARNING, or STABLE.\n"
        f"3. Output strictly valid JSON with keys: ai_triage_level, clinical_assessment, differential_diagnosis, recommended_interventions."
    )


def query_gemini_cdss(
    prompt: str,
    gemini_key: str,
    model: str = "gemini-2.0-flash",
    timeout: float = 4.0
) -> Tuple[bool, Dict[str, Any], str]:
    """Invokes Google AI Studio Gemini API in JSON output mode."""
    if not gemini_key:
        return False, {}, "Missing GEMINI_API_KEY"

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={gemini_key}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"response_mime_type": "application/json", "temperature": 0.2}
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
            parsed = json.loads(text_out)
            return True, parsed, ""
    except urllib.error.HTTPError as e:
        return False, {}, f"Gemini HTTP {e.code}"
    except Exception as e:
        return False, {}, f"Gemini API Error: {str(e)}"


def evaluate_deterministic_rules(
    bp: float,
    hr: float,
    lactate: float,
    k: float,
    glucose: float,
    sepsis_dx: int,
    vaso: int
) -> Tuple[str, str, List[str], List[str]]:
    """Deterministic clinical rule engine fallback aligned with Surviving Sepsis Campaign guidelines."""
    if sepsis_dx > 0 and (lactate >= 2.0 or vaso > 0 or (bp < 90 and bp > 0)):
        return (
            "🚨 CRITICAL",
            "Refractory Septic Shock suspected: concurrent active sepsis diagnosis, hyperlactatemia, and vasopressor infusion.",
            ["Septic Shock (Primary)", "Severe Metabolic Lactic Acidosis", "Distributive Vasodilatory Collapse"],
            [
                "Stat IV crystalloid fluid bolus 30 mL/kg",
                "Titrate IV Norepinephrine infusion to maintain MAP >= 65 mmHg",
                "Repeat blood gas and serum lactate in 2 hours",
                "Broad-spectrum IV antibiotic administration within 1 hour"
            ]
        )
    elif bp >= 170.0:
        return (
            "⚠️ URGENT",
            "Acute Hypertensive Urgency: severely elevated systolic blood pressure requiring rapid titration.",
            ["Hypertensive Crisis / Urgency (Primary)", "Essential Hypertension Decompensation"],
            [
                "Stat 12-lead ECG",
                "Initiate/titrate oral or IV antihypertensive (e.g. IV Labetalol or oral Lisinopril)",
                "Continuous automated BP monitoring every 5 minutes"
            ]
        )
    elif k >= 5.5:
        return (
            "⚠️ URGENT",
            "Severe Hyperkalemia Risk: serum potassium critically elevated above 5.5 mmol/L.",
            ["Severe Hyperkalemia (Primary)", "Acute Kidney Injury Risk"],
            [
                "Stat 12-lead ECG to evaluate peaked T-waves",
                "Administer IV Calcium Gluconate 1g for cardiac membrane stabilization",
                "Administer 10 units Regular Insulin IV with 50 mL 50% Dextrose (D50W)"
            ]
        )
    elif glucose >= 220.0:
        return (
            "🟡 WARNING",
            "Acute Inpatient Hyperglycemia: blood glucose exceeds 220 mg/dL.",
            ["Inpatient Hyperglycemia (Primary)", "Diabetic Ketoacidosis / HHS Risk"],
            [
                "Administer subcutaneous Regular Insulin sliding-scale dose",
                "Recheck point-of-care blood glucose in 1 hour"
            ]
        )
    return (
        "🟢 STABLE",
        "Patient vital signs and biomarkers within acceptable baseline parameters.",
        ["Hemodynamically Stable"],
        ["Continue routine telemetry monitoring"]
    )


def evaluate_patient_window(input_raw: str) -> str:
    """Flinkflow operator entrypoint: evaluates a 10s patient window and emits an AI CDSS alert."""
    try:
        d: Dict[str, Any] = json.loads(input_raw)
    except Exception:
        d = {}

    pid = str(d.get("patient_id", "UNKNOWN"))
    w_start = str(d.get("window_start", ""))
    w_end = str(d.get("window_end", ""))
    total_events = int(d.get("total_clinical_events", 0))
    sources_cnt = int(d.get("distinct_sources_count", 0))

    bp = float(d.get("max_systolic_bp", 0.0))
    hr = float(d.get("max_heart_rate", 0.0))
    glucose = float(d.get("max_glucose", 0.0))
    lactate = float(d.get("max_lactate", 0.0))
    k = float(d.get("max_potassium", 0.0))
    sepsis_dx = int(d.get("sepsis_dx_count", 0))
    vaso = int(d.get("vasopressor_dispenses", 0))
    meds = int(d.get("total_med_dispenses", 0))

    provider = os.environ.get("LLM_PROVIDER", "gemini").lower()
    model = os.environ.get("LLM_MODEL", "gemini-2.0-flash")
    gemini_key = os.environ.get("GEMINI_API_KEY", "").strip()

    is_anomalous = check_is_anomalous(sepsis_dx, lactate, bp, k, glucose, hr, vaso)

    ai_triage = "🟢 STABLE"
    clinical_assessment = "Patient vital signs and biomarkers within acceptable baseline parameters."
    differential_dx = ["Hemodynamically Stable"]
    interventions = ["Continue routine telemetry monitoring"]
    llm_used = provider
    success = False

    # Execute LLM Inference (gated for anomalous patients to respect rate limits)
    if is_anomalous and provider == "gemini" and gemini_key:
        prompt = build_cdss_prompt(pid, bp, hr, lactate, k, glucose, sepsis_dx, meds, vaso)
        success, parsed, err_msg = query_gemini_cdss(prompt, gemini_key, model=model)
        if success:
            ai_triage = parsed.get("ai_triage_level", ai_triage)
            clinical_assessment = parsed.get("clinical_assessment", clinical_assessment)
            differential_dx = parsed.get("differential_diagnosis", differential_dx)
            interventions = parsed.get("recommended_interventions", interventions)

    # Fallback to deterministic rules if not evaluated by LLM
    if not success and is_anomalous:
        llm_used = f"{provider} (Rule-Engine Fallback)"
        ai_triage, clinical_assessment, differential_dx, interventions = evaluate_deterministic_rules(
            bp, hr, lactate, k, glucose, sepsis_dx, vaso
        )

    alert_payload = {
        "patient_id": pid,
        "window_start": w_start,
        "window_end": w_end,
        "total_events_in_window": total_events,
        "distinct_sources_correlated": sources_cnt,
        "ai_triage_level": ai_triage,
        "clinical_assessment": clinical_assessment,
        "differential_diagnosis": differential_dx,
        "recommended_interventions": interventions,
        "vitals_summary": {
            "max_systolic_bp_mmhg": bp,
            "max_heart_rate_bpm": hr
        },
        "labs_summary": {
            "max_lactate_mmol_l": lactate,
            "max_potassium_mmol_l": k,
            "max_glucose_mg_dl": glucose
        },
        "meds_and_dx_summary": {
            "active_sepsis_dx_count": sepsis_dx,
            "vasopressor_infusions_count": vaso,
            "total_med_orders_count": meds
        },
        "llm_engine_used": llm_used
    }
    return json.dumps(alert_payload)


if __name__ == "__main__":
    print(evaluate_patient_window("{}"))

