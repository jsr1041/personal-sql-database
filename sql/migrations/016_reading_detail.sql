-- ============================================================
-- 016_reading_detail.sql
-- Reading detail (books, articles)
-- Depends on: interest_item
-- ============================================================

CREATE TABLE public.reading_detail (
	reading_detail_id serial4 NOT NULL,
	interest_item_id int4 NOT NULL,
	reading_subtype varchar(50) NOT NULL,
	author varchar(255) NULL,
	publication varchar(255) NULL,
	published_date date NULL,
	isbn varchar(20) NULL,
	page_count int4 NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT reading_detail_pkey PRIMARY KEY (reading_detail_id),
	CONSTRAINT uq_reading_detail_interest_item UNIQUE (interest_item_id),
	CONSTRAINT reading_detail_interest_item_id_fkey FOREIGN KEY (interest_item_id) REFERENCES public.interest_item(interest_item_id)
);

CREATE INDEX idx_reading_detail_interest_item ON public.reading_detail USING btree (interest_item_id);
