-- ============================================================
-- 005_gear.sql
-- Gear inventory
-- ============================================================

CREATE TABLE public.gear (
	gear_id serial4 NOT NULL,
	name varchar(100) NOT NULL,
	category varchar(100) NULL,
	sport varchar(100) NULL,
	brand varchar(100) NULL,
	model varchar(100) NULL,
	purchase_date date NULL,
	retirement_date date NULL,
	default_usage_metric varchar(50) NULL,
	notes text NULL,
	salesforce_id varchar(18) NULL,
	source_system varchar(100) NULL,
	source_object varchar(100) NULL,
	source_record_id varchar(100) NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	last_synced_at timestamptz NULL,
	is_deleted bool DEFAULT false NOT NULL,
	CONSTRAINT gear_pkey PRIMARY KEY (gear_id)
);
