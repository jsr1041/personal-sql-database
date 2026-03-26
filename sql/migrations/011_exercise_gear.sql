-- ============================================================
-- 011_exercise_gear.sql
-- Junction table — exercise to gear
-- Depends on: exercise, gear
-- ============================================================

CREATE TABLE public.exercise_gear (
	exercise_gear_id serial4 NOT NULL,
	exercise_id int4 NOT NULL,
	gear_id int4 NOT NULL,
	usage_role varchar(100) NULL,
	is_primary_gear bool DEFAULT false NOT NULL,
	notes text NULL,
	salesforce_id varchar(18) NULL,
	source_system varchar(100) NULL,
	source_object varchar(100) NULL,
	source_record_id varchar(100) NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	last_synced_at timestamptz NULL,
	is_deleted bool DEFAULT false NOT NULL,
	CONSTRAINT exercise_gear_pkey PRIMARY KEY (exercise_gear_id),
	CONSTRAINT uq_exercise_gear UNIQUE (exercise_id, gear_id)
);

ALTER TABLE public.exercise_gear ADD CONSTRAINT exercise_gear_exercise_id_fkey FOREIGN KEY (exercise_id) REFERENCES public.exercise(exercise_id);
ALTER TABLE public.exercise_gear ADD CONSTRAINT exercise_gear_gear_id_fkey FOREIGN KEY (gear_id) REFERENCES public.gear(gear_id);
