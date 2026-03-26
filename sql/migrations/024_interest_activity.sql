-- ============================================================
-- 024_interest_activity.sql
-- Activity log for interest items
-- Depends on: interest_item
-- ============================================================

CREATE TABLE public.interest_activity (
	interest_activity_id serial4 NOT NULL,
	interest_item_id int4 NOT NULL,
	activity_type varchar(50) NOT NULL,
	activity_date date NOT NULL,
	notes text NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT interest_activity_pkey PRIMARY KEY (interest_activity_id),
	CONSTRAINT interest_activity_interest_item_id_fkey FOREIGN KEY (interest_item_id) REFERENCES public.interest_item(interest_item_id)
);

CREATE INDEX idx_interest_activity_date ON public.interest_activity USING btree (activity_date);
