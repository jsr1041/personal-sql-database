-- ============================================================
-- 019_activity_detail.sql
-- Activity detail (hikes, events, experiences)
-- Depends on: interest_item, place_detail
-- ============================================================

CREATE TABLE public.activity_detail (
	activity_detail_id serial4 NOT NULL,
	interest_item_id int4 NOT NULL,
	activity_subtype varchar(50) NULL,
	place_detail_id int4 NULL,
	difficulty varchar(100) NULL,
	duration_estimate_hours numeric(5, 2) NULL,
	distance_miles numeric(8, 2) NULL,
	elevation_gain_feet numeric(8) NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT activity_detail_pkey PRIMARY KEY (activity_detail_id),
	CONSTRAINT uq_activity_detail_interest_item UNIQUE (interest_item_id),
	CONSTRAINT activity_detail_interest_item_id_fkey FOREIGN KEY (interest_item_id) REFERENCES public.interest_item(interest_item_id),
	CONSTRAINT activity_detail_place_detail_id_fkey FOREIGN KEY (place_detail_id) REFERENCES public.place_detail(place_detail_id)
);

CREATE INDEX idx_activity_detail_place ON public.activity_detail USING btree (place_detail_id);
