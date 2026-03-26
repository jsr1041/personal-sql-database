-- ============================================================
-- 007_workout_plan.sql
-- Workout plan / template library
-- ============================================================

CREATE TABLE public.workout_plan (
	workout_plan_id int4 DEFAULT nextval('workout_template_workout_template_id_seq'::regclass) NOT NULL,
	title varchar(500) NOT NULL,
	type_of_activity varchar(100) NULL,
	warmup text NULL,
	the_work text NULL,
	cooldown text NULL,
	source varchar(255) NULL,
	url varchar(2000) NULL,
	notes text NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	is_deleted bool DEFAULT false NOT NULL,
	CONSTRAINT workout_template_pkey PRIMARY KEY (workout_plan_id)
);

CREATE INDEX idx_workout_plan_type ON public.workout_plan USING btree (type_of_activity);
