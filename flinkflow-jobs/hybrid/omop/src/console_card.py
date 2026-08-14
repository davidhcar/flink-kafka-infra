"""
Console Visual Triage Card Formatter
------------------------------------
Formats structured AI CDSS alert payloads into human-readable, color-coded ANSI terminal cards.
"""

import json
from typing import Dict, Any


def format_card(input_raw: str) -> str:
    """Flinkflow operator entrypoint: renders a color-coded triage card for stdout."""
    try:
        a: Dict[str, Any] = json.loads(input_raw)
    except Exception:
        a = {}

    pid = a.get("patient_id", "UNKNOWN")
    triage = a.get("ai_triage_level", "🟢 STABLE")
    assessment = a.get("clinical_assessment", "No assessment")
    diff_dx = ", ".join(a.get("differential_diagnosis", []))
    orders = "\n    • ".join(a.get("recommended_interventions", []))
    engine = a.get("llm_engine_used", "AI")

    v = a.get("vitals_summary", {})
    l = a.get("labs_summary", {})
    m = a.get("meds_and_dx_summary", {})

    card = (
        f"\n\033[1;36m┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓\033[0m\n"
        f"\033[1;36m┃\033[0m \033[1;37m🏥 OMOP AI CLINICAL DECISION SUPPORT (CDSS) ALERT: {pid:<25}\033[0m \033[1;36m┃\033[0m\n"
        f"\033[1;36m┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫\033[0m\n"
        f"  \033[1;33m► TRIAGE LEVEL:\033[0m {triage}\n"
        f"  \033[1;33m► AI ASSESSMENT:\033[0m {assessment}\n"
        f"  \033[1;33m► DIFFERENTIAL DX:\033[0m {diff_dx}\n"
        f"  \033[1;33m► RECOMMENDED ORDERS:\033[0m\n    • {orders}\n"
        f"  \033[1;30m──────────────────────────────────────────────────────────────────────────────\033[0m\n"
        f"  \033[1;34m► MULTI-SOURCE EVIDENCE:\033[0m BP: {v.get('max_systolic_bp_mmhg', 0)} mmHg | "
        f"Lactate: {l.get('max_lactate_mmol_l', 0)} mmol/L | K+: {l.get('max_potassium_mmol_l', 0)} mmol/L | "
        f"Sepsis Dx: {m.get('active_sepsis_dx_count', 0)} | Vasopressor: {m.get('vasopressor_infusions_count', 0)}\n"
        f"  \033[1;30m► ENGINE: {engine} | WINDOW: {a.get('window_start', '')} -> {a.get('window_end', '')}\033[0m\n"
        f"\033[1;36m┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\033[0m\n"
    )
    return card


if __name__ == "__main__":
    print(format_card("{}"))

