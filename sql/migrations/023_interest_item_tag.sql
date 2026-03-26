-- ============================================================
-- 023_interest_item_tag.sql
-- Junction table — interest_item to tag
-- Depends on: interest_item, tag
-- ============================================================

CREATE TABLE public.interest_item_tag (
	interest_item_tag_id serial4 NOT NULL,
	interest_item_id int4 NOT NULL,
	tag_id int4 NOT NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT interest_item_tag_pkey PRIMARY KEY (interest_item_tag_id),
	CONSTRAINT uq_interest_item_tag UNIQUE (interest_item_id, tag_id),
	CONSTRAINT interest_item_tag_interest_item_id_fkey FOREIGN KEY (interest_item_id) REFERENCES public.interest_item(interest_item_id),
	CONSTRAINT interest_item_tag_tag_id_fkey FOREIGN KEY (tag_id) REFERENCES public.tag(tag_id)
);

CREATE INDEX idx_interest_item_tag_tag ON public.interest_item_tag USING btree (tag_id);
