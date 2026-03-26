-- ============================================================
-- 012_health_measurement.sql
-- Garmin Health Snapshot — session-level summary
-- Depends on: day
-- ============================================================

CREATE TABLE public.health_measurement (
	health_measurement_id serial4 NOT NULL,
	day_id int4 NOT NULL,
	measurement_date date NOT NULL,
	measurement_type varchar(50) DEFAULT 'health_snapshot'::character varying NOT NULL,
	started_at timestamptz NOT NULL,
	duration_seconds int4 NULL,
	device_name varchar(100) NULL,
	resting_heart_rate int4 NULL,
	avg_heart_rate int4 NULL,
	min_heart_rate int4 NULL,
	max_heart_rate int4 NULL,
	hrv_rmssd_ms numeric(6, 1) NULL,
	avg_spo2_pct numeric(5, 2) NULL,
	min_spo2_pct numeric(5, 2) NULL,
	avg_respiration_rate numeric(4, 1) NULL,
	avg_stress_level int4 NULL,
	source_system varchar(50) NULL,
	source_object varchar(100) NULL,
	source_record_id varchar(100) NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	is_deleted bool DEFAULT false NOT NULL,
	CONSTRAINT health_measurement_pkey PRIMARY KEY (health_measurement_id)
);

CREATE INDEX idx_health_measurement_day_id ON public.health_measurement USING btree (day_id);
CREATE INDEX idx_health_measurement_measurement_date ON public.health_measurement USING btree (measurement_date);
CREATE INDEX idx_health_measurement_started_at ON public.health_measurement USING btree (started_at);

ALTER TABLE public.health_measurement ADD CONSTRAINT health_measurement_day_id_fkey FOREIGN KEY (day_id) REFERENCES public.day(day_id);
