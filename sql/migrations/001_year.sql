-- public."year" definition

-- Drop table

-- DROP TABLE public."year";

CREATE TABLE public."year" (
	year_id serial4 NOT NULL,
	calendar_year int4 NOT NULL,
	salesforce_id varchar(18) NULL,
	source_system varchar(100) NULL,
	source_object varchar(100) NULL,
	source_record_id varchar(100) NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	last_synced_at timestamptz NULL,
	is_deleted bool DEFAULT false NOT NULL,
	CONSTRAINT year_calendar_year_key UNIQUE (calendar_year),
	CONSTRAINT year_pkey PRIMARY KEY (year_id)
);