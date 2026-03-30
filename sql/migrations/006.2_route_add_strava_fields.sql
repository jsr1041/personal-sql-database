-- ============================================================
-- 006.1_route_add_strava_fields.sql
-- Add Strava sync fields to route table
-- ============================================================
-- strava_route_id   : Strava's internal route ID; used as the
--                     primary join key for API-based sync
-- strava_updated_at : Timestamp from Strava's updated_at field;
--                     used to detect changes and skip unchanged
--                     routes during incremental sync
-- ============================================================

ALTER TABLE public.route
    ADD COLUMN strava_route_id   BIGINT       NULL,
    ADD COLUMN strava_updated_at TIMESTAMPTZ  NULL;

CREATE UNIQUE INDEX idx_route_strava_route_id ON public.route (strava_route_id)
    WHERE strava_route_id IS NOT NULL;
