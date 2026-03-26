-- ============================================================
-- 013_health_measurement_reading.sql
-- Garmin Health Snapshot — per-second readings
-- Depends on: health_measurement
-- ============================================================

CREATE TABLE public.health_measurement_reading (
	health_measurement_reading_id serial4 NOT NULL,
	health_measurement_id int4 NOT NULL,
	recorded_at timestamptz NOT NULL,
	elapsed_seconds int4 NULL,
	heart_rate int4 NULL,
	rr_interval_ms numeric(6, 1) NULL,
	spo2_pct numeric(5, 2) NULL,
	respiration_rate numeric(4, 1) NULL,
	stress_level int4 NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT health_measurement_reading_pkey PRIMARY KEY (health_measurement_reading_id),
	CONSTRAINT uq_health_reading_per_second UNIQUE (health_measurement_id, recorded_at)
);

CREATE INDEX idx_health_reading_recorded_at ON public.health_measurement_reading USING btree (recorded_at);

ALTER TABLE public.health_measurement_reading ADD CONSTRAINT health_measurement_reading_health_measurement_id_fkey FOREIGN KEY (health_measurement_id) REFERENCES public.health_measurement(health_measurement_id);
