-- ============================================================
-- 008_exercise.sql
-- Canonical workout/activity fact table
-- Depends on: day, week, workout_plan, route, activity_detail
-- ============================================================

CREATE TABLE public.exercise (
	exercise_id serial4 NOT NULL,
	day_id int4 NOT NULL,
	week_id int4 NOT NULL,
	activity_date date NOT NULL,
	type_of_activity varchar(100) NULL,
	subtype_of_activity varchar(100) NULL,
	distance_miles numeric(7, 2) NULL,
	duration_minutes numeric(6, 2) NULL,
	elevation_gain_feet numeric(7, 1) NULL,
	average_heart_rate numeric(5, 1) NULL,
	bad_hr_data bool DEFAULT false NOT NULL,
	sweat_rate_l_per_hr numeric(4, 2) NULL,
	tss_score numeric(6, 2) NULL,
	raw_notes text NULL,
	salesforce_id varchar(18) NULL,
	source_system varchar(100) NULL,
	source_object varchar(100) NULL,
	source_record_id varchar(100) NULL,
	source_org varchar(100) NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	last_synced_at timestamptz NULL,
	is_deleted bool DEFAULT false NOT NULL,
	workout_plan_id int4 NULL,
	activity_detail_id int4 NULL,
	calories int4 NULL,
	average_cadence numeric(5, 1) NULL,
	average_power numeric(6, 1) NULL,
	max_power numeric(6, 1) NULL,
	max_heart_rate int4 NULL,
	num_laps int4 NULL,
	total_elapsed_time numeric(8, 2) NULL,
	avg_vertical_ratio numeric(5, 2) NULL,
	avg_stance_time_balance numeric(5, 2) NULL,
	avg_step_length_mm numeric(7, 1) NULL,
	route_id int4 NULL,
	route_match_method varchar(10) NULL,
	route_match_confidence numeric(5, 2) NULL,
	CONSTRAINT exercise_pkey PRIMARY KEY (exercise_id)
);

CREATE INDEX idx_exercise_activity_detail ON public.exercise USING btree (activity_detail_id);
CREATE INDEX idx_exercise_route_id ON public.exercise USING btree (route_id);
CREATE INDEX idx_exercise_type_of_activity ON public.exercise USING btree (type_of_activity);

ALTER TABLE public.exercise ADD CONSTRAINT exercise_activity_detail_id_fkey FOREIGN KEY (activity_detail_id) REFERENCES public.activity_detail(activity_detail_id);
ALTER TABLE public.exercise ADD CONSTRAINT exercise_day_id_fkey FOREIGN KEY (day_id) REFERENCES public.day(day_id);
ALTER TABLE public.exercise ADD CONSTRAINT exercise_route_id_fkey FOREIGN KEY (route_id) REFERENCES public.route(route_id);
ALTER TABLE public.exercise ADD CONSTRAINT exercise_week_id_fkey FOREIGN KEY (week_id) REFERENCES public.week(week_id);
ALTER TABLE public.exercise ADD CONSTRAINT exercise_workout_plan_id_fkey FOREIGN KEY (workout_plan_id) REFERENCES public.workout_plan(workout_plan_id);
