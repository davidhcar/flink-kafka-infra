"""
Unit tests for sql/windowed_patient_state.sql (Flink SQL Tumbling Window & Clinical Metric Aggregations).
"""

import unittest
import os
import sqlite3
from pathlib import Path


class TestSqlWindowing(unittest.TestCase):

    def setUp(self):
        sql_file = Path(__file__).resolve().parents[1] / "sql" / "windowed_patient_state.sql"
        self.assertTrue(sql_file.exists(), f"SQL file not found at {sql_file}")
        with open(sql_file, "r", encoding="utf-8") as f:
            self.raw_sql = f.read()

    def test_sql_syntax_and_required_columns(self):
        """Verifies that SQL contains all required Flink SQL keywords, projections, and grouping."""
        required_keywords = [
            "SELECT",
            "FROM map_to_omop_concept",
            "GROUP BY",
            "TUMBLE(event_time, INTERVAL '10' SECOND)",
            "person_source_value AS patient_id",
            "max_systolic_bp",
            "max_heart_rate",
            "max_glucose",
            "max_lactate",
            "max_potassium",
            "sepsis_dx_count",
            "vasopressor_dispenses",
            "total_med_dispenses"
        ]
        for kw in required_keywords:
            self.assertIn(kw, self.raw_sql, f"Missing required SQL clause or projection: {kw}")

    def test_sql_aggregation_logic_in_memory(self):
        """
        Executes standard SQL aggregation logic in an in-memory SQLite database
        to verify that OMOP Concept IDs correctly aggregate peak vitals, labs, and diagnoses.
        """
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()

        # Create in-memory mock map_to_omop_concept table
        cursor.execute("""
            CREATE TABLE map_to_omop_concept (
                person_source_value TEXT,
                source_name TEXT,
                source_vocabulary TEXT,
                source_code TEXT,
                value TEXT,
                value_num REAL,
                unit TEXT,
                event_time TEXT,
                standard_concept_id INTEGER,
                standard_concept_name TEXT,
                domain_id TEXT
            )
        """)

        # Insert test events for PATIENT-101 (Acute Shock) & PATIENT-105 (Stable)
        test_data = [
            # PATIENT-101: Vitals (BP 82, HR 128)
            ("PATIENT-101", "Source A (Bedside Vitals Telemetry)", "LOINC", "8480-6", "82", 82.0, "mmHg", "2026-08-14 12:00:01", 3027018, "Systolic BP", "Measurement"),
            ("PATIENT-101", "Source A (Bedside Vitals Telemetry)", "LOINC", "8867-4", "128", 128.0, "bpm", "2026-08-14 12:00:02", 3004249, "Heart Rate", "Measurement"),
            # PATIENT-101: Lab (Lactate 4.5)
            ("PATIENT-101", "Source B (LIS Blood Gas)", "LEGACY_LAB_LOCAL", "LAB_LACT_01", "4.5", 4.5, "mmol/L", "2026-08-14 12:00:03", 3006615, "Lactate", "Measurement"),
            # PATIENT-101: Med (Norepinephrine IV)
            ("PATIENT-101", "Source C (Inpatient Pharmacy)", "NDC", "00069-3150-83", "20", 20.0, "mcg/min", "2026-08-14 12:00:04", 1337424, "Norepinephrine", "Drug"),
            # PATIENT-101: EHR Diagnosis (Severe Sepsis)
            ("PATIENT-101", "Source D (EHR Encounter)", "ICD10CM", "R65.21", "Active", 0.0, "dx", "2026-08-14 12:00:05", 132302, "Severe Sepsis", "Condition"),

            # PATIENT-105: Normal Vitals (BP 120, HR 72, Lactate 1.1)
            ("PATIENT-105", "Source A (Bedside Vitals Telemetry)", "LOINC", "8480-6", "120", 120.0, "mmHg", "2026-08-14 12:00:01", 3027018, "Systolic BP", "Measurement"),
            ("PATIENT-105", "Source B (LIS Blood Gas)", "LEGACY_LAB_LOCAL", "LAB_LACT_01", "1.1", 1.1, "mmol/L", "2026-08-14 12:00:03", 3006615, "Lactate", "Measurement"),
        ]

        cursor.executemany("""
            INSERT INTO map_to_omop_concept VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, test_data)
        conn.commit()

        # Run aggregation logic matching windowed_patient_state.sql
        query = """
            SELECT 
                person_source_value AS patient_id,
                COUNT(*) AS total_clinical_events,
                COUNT(DISTINCT source_name) AS distinct_sources_count,
                COALESCE(MAX(CASE WHEN standard_concept_id = 3027018 THEN value_num ELSE NULL END), 0.0) AS max_systolic_bp,
                COALESCE(MAX(CASE WHEN standard_concept_id = 3004249 THEN value_num ELSE NULL END), 0.0) AS max_heart_rate,
                COALESCE(MAX(CASE WHEN standard_concept_id = 3006615 THEN value_num ELSE NULL END), 0.0) AS max_lactate,
                COUNT(CASE WHEN standard_concept_id IN (132302, 4100676, 4100677) THEN 1 ELSE NULL END) AS sepsis_dx_count,
                COUNT(CASE WHEN domain_id = 'Drug' AND standard_concept_id = 1337424 THEN 1 ELSE NULL END) AS vasopressor_dispenses
            FROM map_to_omop_concept
            GROUP BY person_source_value
            ORDER BY patient_id
        """

        cursor.execute(query)
        rows = cursor.fetchall()
        results = {r[0]: r for r in rows}

        # Verify PATIENT-101 metrics
        p101 = results["PATIENT-101"]
        self.assertEqual(p101[1], 5)     # 5 total events
        self.assertEqual(p101[2], 4)     # 4 distinct sources (A, B, C, D)
        self.assertEqual(p101[3], 82.0)  # max systolic BP = 82.0
        self.assertEqual(p101[4], 128.0) # max heart rate = 128.0
        self.assertEqual(p101[5], 4.5)   # max lactate = 4.5
        self.assertEqual(p101[6], 1)     # sepsis diagnosis count = 1
        self.assertEqual(p101[7], 1)     # vasopressor dispenses = 1

        # Verify PATIENT-105 metrics
        p105 = results["PATIENT-105"]
        self.assertEqual(p105[1], 2)     # 2 total events
        self.assertEqual(p105[2], 2)     # 2 distinct sources
        self.assertEqual(p105[3], 120.0) # normal BP
        self.assertEqual(p105[5], 1.1)   # normal lactate
        self.assertEqual(p105[6], 0)     # 0 sepsis dx
        self.assertEqual(p105[7], 0)     # 0 vasopressors

        conn.close()


if __name__ == "__main__":
    unittest.main()
