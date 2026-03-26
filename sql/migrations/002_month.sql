-- ============================================================
-- 002_month.sql
-- Calendar dimension — month
-- ============================================================

CREATE TABLE public.month (
	month_id serial4 NOT NULL,
	calendar_year int4 NOT NULL,
	month_number int4 NOT NULL,
	month_name varchar(10) NOT NULL,
	month_start_date date NOT NULL,
	month_end_date date NOT NULL,
	year_id int4 NOT NULL,
	salesforce_id varchar(18) NULL,
	source_system varchar(100) NULL,
	source_object varchar(100) NULL,
	source_record_id varchar(100) NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	last_synced_at timestamptz NULL,
	is_deleted bool DEFAULT false NOT NULL,
	CONSTRAINT chk_month_number CHECK (((month_number >= 1) AND (month_number <= 12))),
	CONSTRAINT month_pkey PRIMARY KEY (month_id),
	CONSTRAINT uq_month UNIQUE (calendar_year, month_number)
);

ALTER TABLE public.month ADD CONSTRAINT month_year_id_fkey FOREIGN KEY (year_id) REFERENCES public.year(year_id);
