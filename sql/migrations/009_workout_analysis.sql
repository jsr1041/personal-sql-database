-- ============================================================
-- 009_workout_analysis.sql
-- Subjective workout analysis (1:1 with exercise)
-- Depends on: exercise
-- ============================================================

CREATE TABLE public.workout_analysis (
	workout_analysis_id serial4 NOT NULL,
	exercise_id int4 NOT NULL,
	analysis_date date NULL,
	perceived_level_of_effort numeric(4) NULL,
	rating numeric(3, 1) NULL,
	session_quality varchar(50) NULL,
	felt_strong bool NULL,
	felt_fatigued bool NULL,
	mental_state varchar(100) NULL,
	training_purpose varchar(100) NULL,
	analysis_notes text NULL,
	analysis_summary text NULL,
	salesforce_id varchar(18) NULL,
	source_system varchar(100) NULL,
	source_object varchar(100) NULL,
	source_record_id varchar(100) NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	last_synced_at timestamptz NULL,
	is_deleted bool DEFAULT false NOT NULL,
	CONSTRAINT uq_workout_analysis UNIQUE (exercise_id),
	CONSTRAINT workout_analysis_pkey PRIMARY KEY (workout_analysis_id)
);

ALTER TABLE public.workout_analysis ADD CONSTRAINT workout_analysis_exercise_id_fkey FOREIGN KEY (exercise_id) REFERENCES public.exercise(exercise_id);
