import unittest
import json
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from console_card import format_card


class TestConsoleCard(unittest.TestCase):

    def test_format_card_output(self):
        """Verifies that format_card renders ANSI escape sequences and contains alert metadata."""
        alert = json.dumps({
            "patient_id": "PATIENT-101",
            "window_start": "2026-08-14 12:00:00",
            "window_end": "2026-08-14 12:00:10",
            "ai_triage_level": "🚨 CRITICAL",
            "clinical_assessment": "Refractory Septic Shock",
            "differential_diagnosis": ["Septic Shock"],
            "recommended_interventions": ["Stat IV fluids"],
            "vitals_summary": {"max_systolic_bp_mmhg": 82.0},
            "labs_summary": {"max_lactate_mmol_l": 4.5},
            "meds_and_dx_summary": {"active_sepsis_dx_count": 1},
            "llm_engine_used": "Gemini 2.0 Flash"
        })

        card = format_card(alert)
        self.assertIn("PATIENT-101", card)
        self.assertIn("🚨 CRITICAL", card)
        self.assertIn("Refractory Septic Shock", card)
        self.assertIn("Gemini 2.0 Flash", card)
        self.assertIn("\033[", card)  # ANSI styling present


if __name__ == "__main__":
    unittest.main()

