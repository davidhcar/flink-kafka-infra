import unittest
import json
import sys
import os
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from vocab_mapping import resolve_omop_concept, infer_source_name, map_record


class TestVocabMapping(unittest.TestCase):

    def test_infer_source_name(self):
        """Verifies that source names are correctly inferred from vocabulary taxonomy."""
        self.assertEqual(infer_source_name("LOINC"), "Source A (Bedside Vitals Telemetry)")
        self.assertEqual(infer_source_name("LEGACY_LAB_LOCAL"), "Source B (LIS Blood Gas & Chemistry Analyzer)")
        self.assertEqual(infer_source_name("NDC"), "Source C (Inpatient Pharmacy Dispenser)")
        self.assertEqual(infer_source_name("ICD10CM"), "Source D (EHR Encounter Problem List)")
        self.assertEqual(infer_source_name("UNKNOWN_VOCAB", "Custom Device"), "Custom Device")

    def test_resolve_omop_concept_empty_inputs(self):
        """Verifies safe fallback when vocabulary or code is missing."""
        cid, name, domain = resolve_omop_concept("", "")
        self.assertEqual(cid, 0)
        self.assertIn("Unmapped", name)
        self.assertEqual(domain, "Observation")

    def test_resolve_omop_concept_mock_http(self):
        """Verifies successful concept ID resolution from vocab service."""
        mock_data = {
            "concept_id": 3027018,
            "concept_name": "Systolic blood pressure",
            "domain_id": "Measurement"
        }
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(mock_data).encode("utf-8")
        mock_resp.__enter__.return_value = mock_resp

        with patch("urllib.request.urlopen", return_value=mock_resp):
            cid, name, domain = resolve_omop_concept("LOINC", "8480-6")
            self.assertEqual(cid, 3027018)
            self.assertEqual(name, "Systolic blood pressure")
            self.assertEqual(domain, "Measurement")

    def test_map_record_full_transformation(self):
        """Verifies end-to-end transformation from raw input JSON to standard OMOP JSON."""
        raw_event = json.dumps({
            "person_source_value": "PATIENT-101",
            "source_vocabulary": "LOINC",
            "source_code": "8480-6",
            "value": "135.5",
            "unit": "mmHg"
        })

        mock_data = {
            "concept_id": 3027018,
            "concept_name": "Systolic blood pressure",
            "domain_id": "Measurement"
        }
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(mock_data).encode("utf-8")
        mock_resp.__enter__.return_value = mock_resp

        with patch("urllib.request.urlopen", return_value=mock_resp):
            out_raw = map_record(raw_event)
            event = json.loads(out_raw)

            self.assertEqual(event["person_source_value"], "PATIENT-101")
            self.assertEqual(event["source_name"], "Source A (Bedside Vitals Telemetry)")
            self.assertEqual(event["standard_concept_id"], 3027018)
            self.assertEqual(event["standard_concept_name"], "Systolic blood pressure")
            self.assertEqual(event["value_num"], 135.5)
            self.assertEqual(len(event["event_time"]), 19)


if __name__ == "__main__":
    unittest.main()

