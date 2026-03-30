-- ============================================================
-- 032_route_add_activity_type.sql
-- Add activity_type to route table
-- ============================================================
-- Captures the Strava/GPX activity type at ingestion time
-- (e.g. 'running', 'trail_running', 'cycling')
-- Nullable: populated automatically from GPX <type> tag;
-- NULL for manually created routes or non-GPX sources.
-- ============================================================

ALTER TABLE public.route
    ADD COLUMN activity_type VARCHAR(50) NULL;
