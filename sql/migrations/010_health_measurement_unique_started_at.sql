-- Migration 010: Add unique constraint on health_measurement.started_at
-- Required for ON CONFLICT upsert in ingest_health_snapshot.py
-- Date: 2026-03-27

ALTER TABLE health_measurement
ADD CONSTRAINT uq_health_measurement_started_at UNIQUE (started_at);