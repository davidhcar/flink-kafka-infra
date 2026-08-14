import unittest
import json
import sys
import os
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from aidatagen import (
    fetch_gemini_synthetic_batch,
    generate_fallback_cohort_batch,
    process_event,
    COHORT
)


class TestAiDatagen(unittest.TestCase):

    def test_fallback_cohort_batch_distribution(self):
        """Verifies that fallback generator produces events across all cohort patients and 4 feeds."""
        batch = generate_fallback_cohort_batch()
        self.assertGreaterEqual(len(batch), 5)

        patients_in_batch = {e["person_source_value"] for e in batch}
        vocabs_in_batch = {e["source_vocabulary"] for e in batch}

        self.assertIn("PATIENT-101", patients_in_batch)
        self.assertIn("LOINC", vocabs_in_batch)
        self.assertIn("LEGACY_LAB_LOCAL", vocabs_in_batch)
        self.assertIn("NDC", vocabs_in_batch)
        self.assertIn("ICD10CM", vocabs_in_batch)

    def test_fetch_gemini_synthetic_batch_empty_key(self):
        """Verifies that empty API key returns empty list safely without exception."""
        result = fetch_gemini_synthetic_batch(gemini_key="")
        self.assertEqual(result, [])

    def test_fetch_gemini_synthetic_batch_mock_response(self):
        """Verifies parsing of structured Gemini API response."""
        mock_payload = {
            "candidates": [{
                "content": {
                    "parts": [{
                        "text": json.dumps([{
                            "person_source_value": "PATIENT-101",
                            "source_name": "Source A (Bedside Vitals Telemetry)",
                            "source_vocabulary": "LOINC",
                            "source_code": "8480-6",
                            "value": "80",
                            "unit": "mmHg"
                        }])
                    }]
                }
            }]
        }

        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(mock_payload).encode("utf-8")
        mock_resp.__enter__.return_value = mock_resp

        with patch("urllib.request.urlopen", return_value=mock_resp):
            items = fetch_gemini_synthetic_batch(gemini_key="test_fake_key")
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["person_source_value"], "PATIENT-101")
            self.assertEqual(items[0]["source_code"], "8480-6")

    def test_process_event_output_contract(self):
        """Verifies that process_event returns valid JSON conforming to the stream contract."""
        raw_output = process_event("{}")
        event = json.loads(raw_output)

        required_keys = ["person_source_value", "source_name", "source_vocabulary", "source_code", "value", "unit", "event_time"]
        for key in required_keys:
            self.assertIn(key, event, f"Missing required key: {key}")

        self.assertIn(event["person_source_value"], COHORT)
        self.assertEqual(len(event["event_time"]), 19)  # YYYY-MM-DD HH:MM:SS


if __name__ == "__main__":
    unittest.main()

