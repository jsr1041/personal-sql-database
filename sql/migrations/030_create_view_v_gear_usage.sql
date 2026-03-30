-- =============================================================================
-- Migration: 030_create_view_v_gear_usage.sql
-- View:      v_gear_usage
-- Purpose:   Gear usage rollup per gear item across all linked exercises.
-- Depends:   gear, exercise_gear, exercise
-- Author:    Claude (overnight session 2026-03-27)
-- Updated:   2026-03-30 — PKs corrected (g.gear_id, e.exercise_id);
--            JOINs and GROUP BY fixed to use confirmed PKs.
--            ALTER added to backfill is_active column (no default; existing rows NULL).
-- =============================================================================

DROP VIEW IF EXISTS v_gear_usage;

CREATE OR REPLACE VIEW v_gear_usage AS
SELECT
    -- Gear identity
    g.gear_id,
    g.name,
    g.category,
    g.sport,
    g.brand,
    g.model,
    g.purchase_date,
    g.retirement_date,
    g.default_usage_metric,
    g.is_active,
    -- Usage aggregates (NULL-safe via COALESCE for gears with no exercises)
    COUNT(DISTINCT eg.exercise_id)          AS total_sessions,
    COALESCE(SUM(e.distance_miles), 0)      AS total_miles,
    COALESCE(SUM(e.duration_minutes), 0)    AS total_duration_minutes,
    COALESCE(SUM(e.elevation_gain_feet), 0) AS total_elevation_gain_feet,
    -- First / last use dates
    MIN(e.activity_date)                    AS first_use_date,
    MAX(e.activity_date)                    AS last_use_date,
    -- Days since last use (useful for retirement tracking)
    CURRENT_DATE - MAX(e.activity_date)     AS days_since_last_use
FROM gear g
LEFT JOIN exercise_gear eg ON eg.gear_id = g.gear_id
LEFT JOIN exercise e       ON e.exercise_id = eg.exercise_id
GROUP BY
    g.gear_id,
    g.name,
    g.category,
    g.sport,
    g.brand,
    g.model,
    g.purchase_date,
    g.retirement_date,
    g.default_usage_metric,
    g.is_active;

-- -----------------------------------------------------------------------------
-- Schema migration: add is_active to gear table
-- No default set — existing rows will be NULL until explicitly updated.
-- -----------------------------------------------------------------------------
ALTER TABLE gear
    ADD COLUMN IF NOT EXISTS is_active BOOLEAN;

COMMENT ON VIEW v_gear_usage IS
    'Per-gear usage rollup: total sessions, miles, duration, and elevation '
    'from exercise_gear → exercise. Cumulative usage is derived, not stored on gear.';
