import unittest
import json
import sys
import os
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from ai_cdss_agent import (
    check_is_anomalous,
    build_cdss_prompt,
    query_gemini_cdss,
    evaluate_deterministic_rules,
    evaluate_patient_window
)


class TestAiCdssAgent(unittest.TestCase):

    def test_check_is_anomalous_stable_patient(self):
        """Verifies that stable vitals and biomarkers return False (bypassing LLM API call)."""
        self.assertFalse(check_is_anomalous(sepsis_dx=0, lactate=1.1, bp=120.0, k=4.1, glucose=105.0, hr=72.0, vaso=0))

    def test_check_is_anomalous_deteriorating_signals(self):
        """Verifies that acute conditions trigger True for LLM triage."""
        self.assertTrue(check_is_anomalous(sepsis_dx=1, lactate=3.5, bp=80.0, k=4.5, glucose=110.0, hr=125.0, vaso=1))
        self.assertTrue(check_is_anomalous(sepsis_dx=0, lactate=1.2, bp=185.0, k=4.2, glucose=110.0, hr=95.0, vaso=0))
        self.assertTrue(check_is_anomalous(sepsis_dx=0, lactate=1.0, bp=120.0, k=6.1, glucose=110.0, hr=70.0, vaso=0))

    def test_build_cdss_prompt(self):
        """Verifies prompt construction contains all patient metrics."""
        prompt = build_cdss_prompt(
            pid="PATIENT-101", bp=82.0, hr=128.0, lactate=4.5, k=4.2, glucose=110.0,
            sepsis_dx=1, meds=4, vaso=1
        )
        self.assertIn("PATIENT-101", prompt)
        self.assertIn("Systolic BP=82.0", prompt)
        self.assertIn("Heart Rate=128.0", prompt)
        self.assertIn("Lactate=4.5", prompt)
        self.assertIn("Active Sepsis Problem List Records = 1", prompt)

    def test_evaluate_deterministic_rules_septic_shock(self):
        """Verifies rule-engine fallback correctly identifies septic shock."""
        triage, assessment, diff_dx, orders = evaluate_deterministic_rules(
            bp=82.0, hr=125.0, lactate=4.5, k=4.2, glucose=110.0, sepsis_dx=1, vaso=1
        )
        self.assertEqual(triage, "🚨 CRITICAL")
        self.assertIn("Septic Shock", assessment)
        self.assertTrue(any("fluid bolus" in o.lower() for o in orders))

    def test_evaluate_deterministic_rules_hypertensive_urgency(self):
        """Verifies rule-engine fallback correctly identifies hypertensive urgency."""
        triage, assessment, diff_dx, orders = evaluate_deterministic_rules(
            bp=188.0, hr=95.0, lactate=1.1, k=4.1, glucose=110.0, sepsis_dx=0, vaso=0
        )
        self.assertEqual(triage, "⚠️ URGENT")
        self.assertIn("Hypertensive", assessment)

    def test_evaluate_patient_window_stable_bypasses_llm(self):
        """Verifies that stable patient window evaluates to STABLE without calling Gemini API."""
        stable_window = json.dumps({
            "patient_id": "PATIENT-105",
            "window_start": "2026-08-14 12:00:00",
            "window_end": "2026-08-14 12:00:10",
            "total_clinical_events": 8,
            "distinct_sources_count": 3,
            "max_systolic_bp": 120.0,
            "max_heart_rate": 72.0,
            "max_glucose": 105.0,
            "max_lactate": 1.1,
            "max_potassium": 4.1,
            "sepsis_dx_count": 0,
            "vasopressor_dispenses": 0,
            "total_med_dispenses": 1
        })

        out_raw = evaluate_patient_window(stable_window)
        alert = json.loads(out_raw)
        self.assertEqual(alert["patient_id"], "PATIENT-105")
        self.assertEqual(alert["ai_triage_level"], "🟢 STABLE")
        self.assertIn("baseline parameters", alert["clinical_assessment"])


if __name__ == "__main__":
    unittest.main()

