-- =============================================================================
-- Migration: 029_create_view_v_year_summary.sql
-- View:      v_year_summary
-- Purpose:   Yearly rollups from daily and exercise data.
-- Depends:   year, day, exercise
-- Author:    Claude (overnight session 2026-03-27)
-- Updated:   2026-03-30 — PKs corrected (d.day_id, e.exercise_id, y.year_id);
--            JOINs fixed to use confirmed PKs; running filter updated to
--            confirmed Strava type/subtype values.
-- =============================================================================

DROP VIEW IF EXISTS v_year_summary;

CREATE OR REPLACE VIEW v_year_summary AS
WITH day_agg AS (
    SELECT
        d.year_id,
        COUNT(d.day_id)                                             AS number_of_days,
        COUNT(CASE
            WHEN d.daily_points_scored <> 0
             AND d.total_daily_points  <> 0
             AND d.daily_alcoholic_drinks IS NOT NULL
             AND d.daily_liters_water     IS NOT NULL
             AND d.screen_time            IS NOT NULL
             AND d.sleep_time             IS NOT NULL
             AND d.daily_steps            IS NOT NULL
             AND d.daily_pushups          IS NOT NULL
            THEN 1
        END)                                                        AS number_of_complete_days,
        SUM(d.calories)                                             AS yearly_calories,
        SUM(d.daily_steps)                                          AS yearly_steps,
        SUM(d.sleep_time)                                           AS yearly_sleep,
        SUM(d.minutes_reading)                                      AS yearly_reading,
        SUM(d.daily_pushups)                                        AS yearly_pushups,
        SUM(d.daily_liters_water)                                   AS yearly_water_liters,
        SUM(d.daily_alcoholic_drinks)                               AS yearly_alcoholic_drinks,
        SUM(d.daily_points_scored)                                  AS yearly_points_scored,
        SUM(d.total_daily_points)                                   AS yearly_total_points
    FROM day d
    WHERE d.year_id IS NOT NULL
    GROUP BY d.year_id
),
exercise_agg AS (
    SELECT
        d.year_id,
        SUM(e.duration_minutes)                                     AS yearly_minutes_of_exercise,
        SUM(CASE
            WHEN e.type_of_activity = 'Run'
             AND e.subtype_of_activity IN ('Road Run', 'Trail Run', 'Treadmill')
            THEN e.distance_miles ELSE 0
        END)                                                        AS yearly_miles_run,
        SUM(e.distance_miles)                                       AS yearly_miles_all_activities,
        COUNT(e.exercise_id)                                        AS yearly_workouts
    FROM exercise e
    JOIN day d ON e.day_id = d.day_id
    WHERE d.year_id IS NOT NULL
    GROUP BY d.year_id
)
SELECT
    -- Identity
    y.year_id,
    y.calendar_year,
    -- Day counts
    da.number_of_days,
    da.number_of_complete_days,
    -- Nutrition & lifestyle
    da.yearly_calories,
    da.yearly_steps,
    da.yearly_sleep,
    da.yearly_reading,
    da.yearly_pushups,
    da.yearly_water_liters,
    da.yearly_alcoholic_drinks,
    -- Points
    da.yearly_points_scored,
    da.yearly_total_points,
    CASE
        WHEN da.yearly_total_points IS NULL OR da.yearly_total_points = 0 THEN NULL
        ELSE ROUND(da.yearly_points_scored::NUMERIC / da.yearly_total_points, 4)
    END                                                     AS yearly_score,
    -- Exercise
    COALESCE(ea.yearly_minutes_of_exercise, 0)              AS yearly_minutes_of_exercise,
    COALESCE(ea.yearly_miles_run, 0)                        AS yearly_miles_run,
    COALESCE(ea.yearly_miles_all_activities, 0)             AS yearly_miles_all_activities,
    COALESCE(ea.yearly_workouts, 0)                         AS yearly_workouts
FROM year y
LEFT JOIN day_agg da ON da.year_id = y.year_id
LEFT JOIN exercise_agg ea ON ea.year_id = y.year_id;

COMMENT ON VIEW v_year_summary IS
    'Yearly rollup derived from day.year_id. '
    'Covers nutrition, lifestyle, points, and exercise aggregates by calendar year.';
