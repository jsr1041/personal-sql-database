-- ============================================================
-- 018_place_detail.sql
-- Place detail (restaurants, destinations, venues)
-- Depends on: interest_item
-- ============================================================

CREATE TABLE public.place_detail (
	place_detail_id serial4 NOT NULL,
	interest_item_id int4 NOT NULL,
	place_subtype varchar(50) NULL,
	address varchar(500) NULL,
	city varchar(100) NULL,
	state varchar(100) NULL,
	country varchar(100) NULL,
	region varchar(255) NULL,
	latitude numeric(9, 6) NULL,
	longitude numeric(9, 6) NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT place_detail_pkey PRIMARY KEY (place_detail_id),
	CONSTRAINT uq_place_detail_interest_item UNIQUE (interest_item_id),
	CONSTRAINT place_detail_interest_item_id_fkey FOREIGN KEY (interest_item_id) REFERENCES public.interest_item(interest_item_id)
);

CREATE INDEX idx_place_detail_interest_item ON public.place_detail USING btree (interest_item_id);
