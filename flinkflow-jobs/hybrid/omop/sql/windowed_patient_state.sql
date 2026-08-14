SELECT 
  person_source_value AS patient_id,
  CAST(TUMBLE_START(event_time, INTERVAL '10' SECOND) AS STRING) AS window_start,
  CAST(TUMBLE_END(event_time, INTERVAL '10' SECOND) AS STRING) AS window_end,
  COUNT(*) AS total_clinical_events,
  COUNT(DISTINCT source_name) AS distinct_sources_count,
  
  -- Peak Vitals & Labs
  COALESCE(MAX(CASE WHEN standard_concept_id = 3027018 THEN value_num ELSE NULL END), 0.0) AS max_systolic_bp,
  COALESCE(MAX(CASE WHEN standard_concept_id = 3004249 THEN value_num ELSE NULL END), 0.0) AS max_heart_rate,
  COALESCE(MAX(CASE WHEN standard_concept_id = 3004501 THEN value_num ELSE NULL END), 0.0) AS max_glucose,
  COALESCE(MAX(CASE WHEN standard_concept_id = 3006615 THEN value_num ELSE NULL END), 0.0) AS max_lactate,
  COALESCE(MAX(CASE WHEN standard_concept_id = 3023103 THEN value_num ELSE NULL END), 0.0) AS max_potassium,
  
  -- Diagnosis and Medication Counts
  COUNT(CASE WHEN standard_concept_id IN (132302, 4100676, 4100677) THEN 1 ELSE NULL END) AS sepsis_dx_count,
  COUNT(CASE WHEN domain_id = 'Drug' AND standard_concept_id = 1337424 THEN 1 ELSE NULL END) AS vasopressor_dispenses,
  COUNT(CASE WHEN domain_id = 'Drug' THEN 1 ELSE NULL END) AS total_med_dispenses
FROM map_to_omop_concept
GROUP BY 
  TUMBLE(event_time, INTERVAL '10' SECOND),
  person_source_value
