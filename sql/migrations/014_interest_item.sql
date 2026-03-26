-- ============================================================
-- 014_interest_item.sql
-- Interest tracking hub table
-- ============================================================

CREATE TABLE public.interest_item (
	interest_item_id serial4 NOT NULL,
	category varchar(50) NOT NULL,
	title varchar(500) NOT NULL,
	status varchar(50) NULL,
	priority int4 NULL,
	source varchar(255) NULL,
	source_contact_id int4 NULL,
	notes text NULL,
	date_added date NULL,
	completed_date date NULL,
	last_activity_date date NULL,
	last_activity_type varchar(100) NULL,
	link varchar(2000) NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	is_deleted bool DEFAULT false NOT NULL,
	CONSTRAINT interest_item_pkey PRIMARY KEY (interest_item_id)
);

CREATE INDEX idx_interest_item_source_contact ON public.interest_item USING btree (source_contact_id);
