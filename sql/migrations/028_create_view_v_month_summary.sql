-- =============================================================================
-- Migration: 028_create_view_v_month_summary.sql
-- View:      v_month_summary
-- Purpose:   Monthly rollups from daily and exercise data.
--            Aggregates via day.month_id (not week.start_month_id).
-- Depends:   month, day, exercise
-- Author:    Claude (overnight session 2026-03-27)
-- Updated:   2026-03-30 — PKs corrected (d.day_id, e.exercise_id, m.month_id);
--            JOINs fixed to use confirmed PKs; running filter updated to
--            confirmed Strava type/subtype values.
-- =============================================================================

DROP VIEW IF EXISTS v_month_summary;

CREATE OR REPLACE VIEW v_month_summary AS
WITH day_agg AS (
    SELECT
        d.month_id,
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
        COUNT(CASE WHEN d.incomplete_food_log = TRUE THEN 1 END)   AS number_of_incomplete_food_logs,
        SUM(d.calories)                                             AS monthly_calories,
        SUM(d.protein)                                              AS monthly_protein,
        SUM(d.fat)                                                  AS monthly_fat,
        SUM(d.carbs)                                                AS monthly_carbs,
        SUM(d.daily_steps)                                          AS monthly_steps,
        SUM(d.sleep_time)                                           AS monthly_sleep,
        SUM(d.minutes_reading)                                      AS monthly_reading_minutes,
        SUM(d.screen_time)                                          AS monthly_screen_time,
        SUM(d.daily_pushups)                                        AS monthly_pushups,
        SUM(d.daily_liters_water)                                   AS monthly_water_liters,
        SUM(d.daily_alcoholic_drinks)                               AS monthly_alcoholic_drinks,
        SUM(d.daily_points_scored)                                  AS monthly_points_scored,
        SUM(d.total_daily_points)                                   AS monthly_total_points
    FROM day d
    WHERE d.month_id IS NOT NULL
    GROUP BY d.month_id
),
exercise_agg AS (
    SELECT
        d.month_id,
        SUM(e.duration_minutes)                                     AS monthly_minutes_of_exercise,
        SUM(CASE
            WHEN e.type_of_activity = 'Run'
             AND e.subtype_of_activity IN ('Road Run', 'Trail Run', 'Treadmill')
            THEN e.distance_miles ELSE 0
        END)                                                        AS monthly_miles_run,
        SUM(e.distance_miles)                                       AS monthly_miles_all_activities,
        COUNT(e.exercise_id)                                        AS monthly_workout_count
    FROM exercise e
    JOIN day d ON e.day_id = d.day_id
    WHERE d.month_id IS NOT NULL
    GROUP BY d.month_id
)
SELECT
    m.month_id,
    m.calendar_year,
    m.month_number,
    m.month_name,
    m.month_start_date,
    m.month_end_date,
    m.year_id,
    -- Day counts
    da.number_of_days,
    da.number_of_complete_days,
    da.number_of_incomplete_food_logs,
    -- Nutrition totals
    da.monthly_calories,
    da.monthly_protein,
    da.monthly_fat,
    da.monthly_carbs,
    -- Lifestyle
    da.monthly_steps,
    da.monthly_sleep,
    da.monthly_reading_minutes,
    da.monthly_screen_time,
    da.monthly_pushups,
    da.monthly_water_liters,
    da.monthly_alcoholic_drinks,
    -- Points
    da.monthly_points_scored,
    da.monthly_total_points,
    CASE
        WHEN da.monthly_total_points IS NULL OR da.monthly_total_points = 0 THEN NULL
        ELSE ROUND(da.monthly_points_scored::NUMERIC / da.monthly_total_points, 4)
    END                                                     AS monthly_score,
    -- Exercise
    COALESCE(ea.monthly_minutes_of_exercise, 0)             AS monthly_minutes_of_exercise,
    COALESCE(ea.monthly_miles_run, 0)                       AS monthly_miles_run,
    COALESCE(ea.monthly_miles_all_activities, 0)            AS monthly_miles_all_activities,
    COALESCE(ea.monthly_workout_count, 0)                   AS monthly_workout_count
FROM month m
LEFT JOIN day_agg da ON da.month_id = m.month_id
LEFT JOIN exercise_agg ea ON ea.month_id = m.month_id;

COMMENT ON VIEW v_month_summary IS
    'Monthly rollup derived from day.month_id (not week.start_month_id). '
    'Covers nutrition, lifestyle, points, and exercise aggregates by calendar month.';
