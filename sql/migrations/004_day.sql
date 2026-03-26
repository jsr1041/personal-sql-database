-- ============================================================
-- 004_day.sql
-- Core daily fact table
-- ============================================================

CREATE TABLE public.day (
	day_id serial4 NOT NULL,
	date date NOT NULL,
	day_number int4 NULL,
	week_id int4 NOT NULL,
	month_id int4 NOT NULL,
	year_id int4 NOT NULL,
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
	incomplete_food_log bool DEFAULT false NOT NULL,
	breakfast text NULL,
	lunch text NULL,
	dinner text NULL,
	salesforce_id varchar(18) NULL,
	source_system varchar(100) NULL,
	source_object varchar(100) NULL,
	source_record_id varchar(100) NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	last_synced_at timestamptz NULL,
	is_deleted bool DEFAULT false NOT NULL,
	CONSTRAINT day_date_key UNIQUE (date),
	CONSTRAINT day_pkey PRIMARY KEY (day_id)
);

ALTER TABLE public.day ADD CONSTRAINT day_month_id_fkey FOREIGN KEY (month_id) REFERENCES public.month(month_id);
ALTER TABLE public.day ADD CONSTRAINT day_week_id_fkey FOREIGN KEY (week_id) REFERENCES public.week(week_id);
ALTER TABLE public.day ADD CONSTRAINT day_year_id_fkey FOREIGN KEY (year_id) REFERENCES public.year(year_id);
