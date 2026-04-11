-- ============================================================
-- 034_stg_day_spreadsheet.sql
-- Staging table for spreadsheet-sourced daily data
-- ============================================================

CREATE TABLE public.stg_day_spreadsheet (
	stg_id serial4 NOT NULL,
	date date NOT NULL,
	-- Metric columns mirrored from day (all nullable — raw landing zone)
	calories numeric(7, 2) NULL,
	protein numeric(6, 2) NULL,
	fat numeric(6, 2) NULL,
	carbs numeric(6, 2) NULL,
	daily_gallons_water numeric(5, 2) NULL,
	daily_liters_water numeric(5, 2) NULL,
	daily_alcoholic_drinks numeric(4, 1) NULL,
	daily_steps int4 NULL,
	daily_pushups int4 NULL,
	sleep_time numeric(4, 2) NULL,
	minutes_reading int4 NULL,
	minutes_tv int4 NULL,
	screen_time numeric(4, 2) NULL,
	calories_burned numeric(7, 2) NULL,
	daily_points_scored numeric(6, 2) NULL,
	total_daily_points numeric(6, 2) NULL,
	daily_rating numeric(3, 1) NULL,
	incomplete_food_log bool NULL,
	breakfast text NULL,
	lunch text NULL,
	dinner text NULL,
	-- Staging metadata
	source_file varchar(255) NULL,
	source_row int4 NULL,
	load_status varchar(20) DEFAULT 'pending' NOT NULL,
	error_message text NULL,
	loaded_at timestamptz DEFAULT now() NOT NULL,
	processed_at timestamptz NULL,
	day_id int4 NULL,
	CONSTRAINT stg_day_spreadsheet_pkey PRIMARY KEY (stg_id),
	CONSTRAINT stg_day_spreadsheet_date_source_file_key UNIQUE (date, source_file),
	CONSTRAINT stg_day_spreadsheet_load_status_check CHECK (load_status IN ('pending', 'processed', 'error', 'skipped'))
);

ALTER TABLE public.stg_day_spreadsheet
	ADD CONSTRAINT stg_day_spreadsheet_day_id_fkey
	FOREIGN KEY (day_id) REFERENCES public.day(day_id);
