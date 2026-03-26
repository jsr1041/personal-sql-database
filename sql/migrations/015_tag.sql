-- ============================================================
-- 015_tag.sql
-- Tag taxonomy
-- ============================================================

CREATE TABLE public.tag (
	tag_id serial4 NOT NULL,
	tag_name varchar(100) NOT NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT tag_pkey PRIMARY KEY (tag_id),
	CONSTRAINT uq_tag_name UNIQUE (tag_name)
);
