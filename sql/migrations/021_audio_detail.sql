-- ============================================================
-- 021_audio_detail.sql
-- Audio detail (podcasts, music, audiobooks)
-- Depends on: interest_item
-- ============================================================

CREATE TABLE public.audio_detail (
	audio_detail_id serial4 NOT NULL,
	interest_item_id int4 NOT NULL,
	audio_subtype varchar(50) NOT NULL,
	creator varchar(255) NULL,
	genre varchar(100) NULL,
	release_year int4 NULL,
	episode_title varchar(500) NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT audio_detail_pkey PRIMARY KEY (audio_detail_id),
	CONSTRAINT uq_audio_detail_interest_item UNIQUE (interest_item_id),
	CONSTRAINT audio_detail_interest_item_id_fkey FOREIGN KEY (interest_item_id) REFERENCES public.interest_item(interest_item_id)
);

CREATE INDEX idx_audio_detail_interest_item ON public.audio_detail USING btree (interest_item_id);
