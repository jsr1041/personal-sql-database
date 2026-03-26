-- ============================================================
-- 017_screen_detail.sql
-- Screen detail (movies, TV, video)
-- Depends on: interest_item
-- ============================================================

CREATE TABLE public.screen_detail (
	screen_detail_id serial4 NOT NULL,
	interest_item_id int4 NOT NULL,
	format varchar(100) NULL,
	creator varchar(255) NULL,
	release_year int4 NULL,
	platform varchar(100) NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT screen_detail_pkey PRIMARY KEY (screen_detail_id),
	CONSTRAINT uq_screen_detail_interest_item UNIQUE (interest_item_id),
	CONSTRAINT screen_detail_interest_item_id_fkey FOREIGN KEY (interest_item_id) REFERENCES public.interest_item(interest_item_id)
);

CREATE INDEX idx_screen_detail_interest_item ON public.screen_detail USING btree (interest_item_id);
