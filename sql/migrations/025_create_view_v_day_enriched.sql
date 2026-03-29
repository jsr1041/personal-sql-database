-- =============================================================================
-- Migration: 025_create_view_v_day_enriched.sql
-- View:      v_day_enriched
-- Purpose:   Daily record plus day-level derived metrics (macros per kg,
--            daily score, completed_day flag).
-- Depends:   day, week
-- Author:    Claude (overnight session 2026-03-27)
-- Updated:   2026-03-29 — weight stored in lbs; divide by 2.205 for kg
--            conversion. Fallback: 175 lbs when week weight is NULL or 0
--            (mirrors Salesforce formula behavior).
--            PKs corrected: d.day_id, w.week_id (not d.id / w.id).
--            Comments removed from SELECT body for DBeaver compatibility.
-- =============================================================================

-- NOTE: On first run, skip this line and run the CREATE OR REPLACE block
-- directly. DBeaver may abort on DROP IF EXISTS when the view does not yet
-- exist. On subsequent re-runs the full script is safe to execute.
DROP VIEW IF EXISTS v_day_enriched;

CREATE OR REPLACE VIEW v_day_enriched AS
SELECT
    d.day_id,
    d.date,
    d.day_number,
    d.week_id,
    d.month_id,
    d.year_id,
    d.calories,
    d.protein,
    d.fat,
    d.carbs,
    d.daily_gallons_water,
    d.daily_liters_water,
    d.daily_alcoholic_drinks,
    d.daily_steps,
    d.daily_pushups,
    d.sleep_time,
    d.minutes_reading,
    d.minutes_tv,
    d.screen_time,
    d.calories_burned,
    d.daily_points_scored,
    d.total_daily_points,
    d.daily_rating,
    d.incomplete_food_log,
    w.weight AS week_weight_lbs,
    ROUND(d.protein::NUMERIC / (COALESCE(NULLIF(w.weight, 0), 175) / 2.205), 2) AS protein_per_kg,
    ROUND(d.fat::NUMERIC    / (COALESCE(NULLIF(w.weight, 0), 175) / 2.205), 2) AS fat_per_kg,
    ROUND(d.carbs::NUMERIC  / (COALESCE(NULLIF(w.weight, 0), 175) / 2.205), 2) AS carbs_per_kg,
    CASE
        WHEN d.total_daily_points IS NULL OR d.total_daily_points = 0 THEN NULL
        ELSE ROUND(d.daily_points_scored::NUMERIC / d.total_daily_points, 4)
    END AS daily_score,
    CASE
        WHEN d.daily_points_scored <> 0
         AND d.total_daily_points  <> 0
         AND d.daily_alcoholic_drinks IS NOT NULL
         AND d.daily_liters_water     IS NOT NULL
         AND d.screen_time            IS NOT NULL
         AND d.sleep_time             IS NOT NULL
         AND d.daily_steps            IS NOT NULL
         AND d.daily_pushups          IS NOT NULL
        THEN TRUE
        ELSE FALSE
    END AS completed_day
FROM day d
LEFT JOIN week w ON d.week_id = w.week_id;

COMMENT ON VIEW v_day_enriched IS
    'Daily record enriched with macro-per-kg normalization, daily score ratio, '
    'and completed_day flag. Joins day → week for weight context. '
    'week.weight stored in lbs; divided by 2.205 for kg conversion. '
    'Fallback weight of 175 lbs used when week weight is NULL or 0.';
