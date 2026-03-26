-- ============================================================
-- 020_recipe_detail.sql
-- Recipe detail
-- Depends on: interest_item
-- ============================================================

CREATE TABLE public.recipe_detail (
	recipe_detail_id serial4 NOT NULL,
	interest_item_id int4 NOT NULL,
	cuisine varchar(100) NULL,
	meal_type varchar(50) NULL,
	prep_time_minutes int4 NULL,
	cook_time_minutes int4 NULL,
	servings int4 NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT recipe_detail_pkey PRIMARY KEY (recipe_detail_id),
	CONSTRAINT uq_recipe_detail_interest_item UNIQUE (interest_item_id),
	CONSTRAINT recipe_detail_interest_item_id_fkey FOREIGN KEY (interest_item_id) REFERENCES public.interest_item(interest_item_id)
);

CREATE INDEX idx_recipe_detail_interest_item ON public.recipe_detail USING btree (interest_item_id);
