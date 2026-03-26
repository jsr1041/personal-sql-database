-- ============================================================
-- 003_week.sql
-- Calendar dimension — week
-- ============================================================

CREATE TABLE public.week (
	week_id serial4 NOT NULL,
	calendar_year int4 NOT NULL,
	week_number int4 NOT NULL,
	week_start_date date NOT NULL,
	week_end_date date NOT NULL,
	start_month_id int4 NOT NULL,
	year_id int4 NOT NULL,
	weight numeric(5, 2) NULL,
	salesforce_id varchar(18) NULL,
	source_system varchar(100) NULL,
	source_object varchar(100) NULL,
	source_record_id varchar(100) NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	last_synced_at timestamptz NULL,
	is_deleted bool DEFAULT false NOT NULL,
	CONSTRAINT chk_week_number CHECK (((week_number >= 1) AND (week_number <= 53))),
	CONSTRAINT uq_week UNIQUE (calendar_year, week_number),
	CONSTRAINT week_pkey PRIMARY KEY (week_id)
);

ALTER TABLE public.week ADD CONSTRAINT week_start_month_id_fkey FOREIGN KEY (start_month_id) REFERENCES public.month(month_id);
ALTER TABLE public.week ADD CONSTRAINT week_year_id_fkey FOREIGN KEY (year_id) REFERENCES public.year(year_id);
