-- ============================================================
-- 010_exercise_trackpoint.sql
-- Per-second GPS and biometric trackpoints
-- ============================================================

CREATE TABLE public.exercise_trackpoint (
	exercise_trackpoint_id serial4 NOT NULL,
	exercise_id int4 NOT NULL,
	recorded_at timestamptz NOT NULL,
	position_lat numeric(9, 6) NULL,
	position_long numeric(9, 6) NULL,
	altitude_meters numeric(7, 1) NULL,
	distance_meters numeric(8, 1) NULL,
	speed_ms numeric(6, 3) NULL,
	heart_rate int4 NULL,
	cadence numeric(5, 1) NULL,
	power int4 NULL,
	temperature_c numeric(4, 1) NULL,
	gps_accuracy_m numeric(6, 1) NULL,
	vertical_oscillation_mm numeric(6, 1) NULL,
	stance_time_ms numeric(6, 1) NULL,
	stance_time_pct numeric(5, 2) NULL,
	vertical_ratio_pct numeric(5, 2) NULL,
	ground_contact_balance_pct numeric(5, 2) NULL,
	performance_condition numeric(5, 1) NULL,
	absolute_pressure_pa int4 NULL,
	spo2_pct numeric(5, 2) NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT exercise_trackpoint_pkey PRIMARY KEY (exercise_trackpoint_id),
	CONSTRAINT uq_exercise_trackpoint UNIQUE (exercise_id, recorded_at)
);

CREATE INDEX idx_exercise_trackpoint_recorded_at ON public.exercise_trackpoint USING btree (recorded_at);

ALTER TABLE public.exercise_trackpoint ADD CONSTRAINT exercise_trackpoint_exercise_id_fkey FOREIGN KEY (exercise_id) REFERENCES public.exercise(exercise_id);
