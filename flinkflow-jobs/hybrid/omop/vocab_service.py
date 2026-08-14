#!/usr/bin/env python3
"""
OHDSI OMOP Real-Time Vocabulary Lookup Microservice for Flinkflow
------------------------------------------------------------------
Serves /maps-to endpoint on port 8082 for Flinkflow http-lookup step.
Maps source vocabularies & source codes to standard OMOP Concept IDs.
Uses standard Python libraries (zero external dependencies).
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import urllib.parse
import sys

VOCABULARY_INDEX = {
    # Modern EHR Vitals & Labs (LOINC)
    ("LOINC", "8480-6"): {
        "concept_id": 3027018,
        "concept_name": "Systolic blood pressure",
        "domain_id": "Measurement"
    },
    ("LOINC", "2339-0"): {
        "concept_id": 3004501,
        "concept_name": "Glucose [Mass/volume] in Blood",
        "domain_id": "Measurement"
    },
    ("LOINC", "2518-8"): {
        "concept_id": 3006615,
        "concept_name": "Blood lactate",
        "domain_id": "Measurement"
    },
    ("LOINC", "8867-4"): {
        "concept_id": 3004249,
        "concept_name": "Heart rate",
        "domain_id": "Measurement"
    },

    # Legacy Lab Local Codes
    ("LEGACY_LAB_LOCAL", "GLUC-STAT"): {
        "concept_id": 3004501,
        "concept_name": "Glucose [Mass/volume] in Blood",
        "domain_id": "Measurement"
    },
    ("LEGACY_LAB_LOCAL", "K-PANEL"): {
        "concept_id": 3023103,
        "concept_name": "Potassium [Moles/volume] in Blood",
        "domain_id": "Measurement"
    },
    ("LEGACY_LAB_LOCAL", "HR_MONITOR"): {
        "concept_id": 3004249,
        "concept_name": "Heart rate",
        "domain_id": "Measurement"
    },

    # Pharmacy Feed (NDC Drug Codes)
    ("NDC", "00093-7146-01"): {
        "concept_id": 1503297,
        "concept_name": "Metformin hydrochloride 500 MG Oral Tablet",
        "domain_id": "Drug"
    },
    ("NDC", "00186-0771-31"): {
        "concept_id": 19019074,
        "concept_name": "Lisinopril 10 MG Oral Tablet",
        "domain_id": "Drug"
    },
    ("NDC", "00069-3150-83"): {
        "concept_id": 1337424,
        "concept_name": "Norepinephrine 1 MG/ML Injectable Solution",
        "domain_id": "Drug"
    },
    ("NDC", "00409-7332-01"): {
        "concept_id": 1705674,
        "concept_name": "Vancomycin 1000 MG Injection",
        "domain_id": "Drug"
    },

    # Diagnoses & Conditions (ICD-10-CM Codes)
    ("ICD10CM", "A41.9"): {
        "concept_id": 132302,
        "concept_name": "Sepsis, unspecified organism",
        "domain_id": "Condition"
    },
    ("ICD10CM", "R65.20"): {
        "concept_id": 4100676,
        "concept_name": "Severe sepsis without septic shock",
        "domain_id": "Condition"
    },
    ("ICD10CM", "R65.21"): {
        "concept_id": 4100677,
        "concept_name": "Severe sepsis with septic shock",
        "domain_id": "Condition"
    },
    ("ICD10CM", "I10"): {
        "concept_id": 320128,
        "concept_name": "Essential hypertension",
        "domain_id": "Condition"
    }
}

class VocabLookupHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        sys.stderr.write(f"[VocabService] {self.address_string()} - {format % args}\n")

    def do_GET(self):
        parsed_url = urllib.parse.urlparse(self.path)
        
        # Health check endpoint
        if parsed_url.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "UP"}).encode("utf-8"))
            return

        # Main Vocabulary Mapping Endpoint: /maps-to?vocab=...&code=...
        if parsed_url.path == "/maps-to" or parsed_url.path == "/api/v1/map":
            query_params = urllib.parse.parse_qs(parsed_url.query)
            vocab = query_params.get("vocab", [""])[0].strip().upper()
            code = query_params.get("code", [""])[0].strip()

            key = (vocab, code)
            match = VOCABULARY_INDEX.get(key)

            if match:
                response_data = {
                    "concept_id": match["concept_id"],
                    "concept_name": match["concept_name"],
                    "domain_id": match["domain_id"]
                }
            else:
                response_data = {
                    "concept_id": 0,
                    "concept_name": f"Unmapped ({vocab}:{code})",
                    "domain_id": "Observation"
                }

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(response_data).encode("utf-8"))
            return

        self.send_response(404)
        self.end_headers()

def run_server(port=8082):
    server_address = ("0.0.0.0", port)
    httpd = HTTPServer(server_address, VocabLookupHandler)
    print(f"✅ OHDSI OMOP Vocabulary Service running on http://0.0.0.0:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping Vocabulary Service...")
        httpd.server_close()

if __name__ == "__main__":
    port = 8082
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    run_server(port)
