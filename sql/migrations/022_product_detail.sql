-- ============================================================
-- 022_product_detail.sql
-- Product detail (gear, equipment, wishlist)
-- Depends on: interest_item
-- ============================================================

CREATE TABLE public.product_detail (
	product_detail_id serial4 NOT NULL,
	interest_item_id int4 NOT NULL,
	brand varchar(100) NULL,
	model varchar(255) NULL,
	category varchar(100) NULL,
	price_estimate numeric(8, 2) NULL,
	retailer varchar(255) NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT product_detail_pkey PRIMARY KEY (product_detail_id),
	CONSTRAINT uq_product_detail_interest_item UNIQUE (interest_item_id),
	CONSTRAINT product_detail_interest_item_id_fkey FOREIGN KEY (interest_item_id) REFERENCES public.interest_item(interest_item_id)
);

CREATE INDEX idx_product_detail_interest_item ON public.product_detail USING btree (interest_item_id);
