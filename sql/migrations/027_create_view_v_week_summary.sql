-- =============================================================================
-- Migration: 027_create_view_v_week_summary.sql
-- View:      v_week_summary
-- Purpose:   Weekly rollups: nutrition, lifestyle, hydration, exercise, points.
--            Implements food_average_denominator logic from Derived Metric Rules.
-- Depends:   week, day, exercise
-- Author:    Claude (overnight session 2026-03-27)
-- Updated:   2026-03-30 — PKs corrected (d.day_id, e.exercise_id, w.week_id);
--            JOIN fixed to w.week_id; weight lbs→kg via /2.205 with 175 lb
--            fallback; running filter updated to confirmed Strava subtype values.
-- =============================================================================

DROP VIEW IF EXISTS v_week_summary;

CREATE OR REPLACE VIEW v_week_summary AS
WITH day_agg AS (
    -- Aggregate day-level facts per week
    SELECT
        d.week_id,
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
        SUM(d.protein)                                              AS total_protein,
        SUM(d.fat)                                                  AS total_fat,
        SUM(d.carbs)                                                AS total_carbs,
        SUM(d.calories)                                             AS total_calories,
        SUM(d.screen_time)                                          AS total_screen_time,
        SUM(d.sleep_time)                                           AS total_sleep_time,
        SUM(d.daily_steps)                                          AS total_weekly_steps,
        SUM(d.daily_pushups)                                        AS weekly_pushups,
        SUM(d.minutes_reading)                                      AS weekly_minutes_read,
        SUM(d.minutes_tv)                                           AS weekly_minutes_tv,
        SUM(d.daily_liters_water)                                   AS weekly_water_liters,
        SUM(d.daily_gallons_water)                                  AS weekly_water_gallons,
        SUM(d.daily_alcoholic_drinks)                               AS weekly_alcoholic_drinks,
        SUM(d.daily_points_scored)                                  AS weekly_points_scored,
        SUM(d.total_daily_points)                                   AS total_weekly_points
    FROM day d
    GROUP BY d.week_id
),
exercise_agg AS (
    SELECT
        e.week_id,
        SUM(e.duration_minutes)                                     AS minutes_of_exercise,
        SUM(CASE
            WHEN e.type_of_activity = 'Run'
             AND e.subtype_of_activity IN ('Road Run', 'Trail Run', 'Treadmill')
            THEN e.distance_miles ELSE 0
        END)                                                        AS weekly_miles_run,
        SUM(e.distance_miles)                                       AS weekly_miles_all_activities,
        COUNT(e.exercise_id)                                        AS weekly_workout_count
    FROM exercise e
    WHERE e.week_id IS NOT NULL
    GROUP BY e.week_id
)
SELECT
    w.week_id,
    w.calendar_year,
    w.week_number,
    w.week_start_date,
    w.week_end_date,
    w.start_month_id,
    w.year_id,
    COALESCE(NULLIF(w.weight, 0), 175) / 2.205             AS weight_kg,
    da.number_of_days,
    da.number_of_complete_days,
    da.number_of_incomplete_food_logs,
    da.total_protein,
    da.total_fat,
    da.total_carbs,
    da.total_calories,
    GREATEST(da.number_of_complete_days - da.number_of_incomplete_food_logs, 0)
                                                            AS food_avg_denominator,
    da.number_of_complete_days                              AS non_food_avg_denominator,
    CASE
        WHEN GREATEST(da.number_of_complete_days - da.number_of_incomplete_food_logs, 0) <= 0 THEN NULL
        ELSE ROUND(da.total_protein::NUMERIC / GREATEST(da.number_of_complete_days - da.number_of_incomplete_food_logs, 1), 1)
    END                                                     AS average_protein_per_day,
    CASE
        WHEN GREATEST(da.number_of_complete_days - da.number_of_incomplete_food_logs, 0) <= 0 THEN NULL
        ELSE ROUND(da.total_fat::NUMERIC / GREATEST(da.number_of_complete_days - da.number_of_incomplete_food_logs, 1), 1)
    END                                                     AS average_fat_per_day,
    CASE
        WHEN GREATEST(da.number_of_complete_days - da.number_of_incomplete_food_logs, 0) <= 0 THEN NULL
        ELSE ROUND(da.total_carbs::NUMERIC / GREATEST(da.number_of_complete_days - da.number_of_incomplete_food_logs, 1), 1)
    END                                                     AS average_carbs_per_day,
    CASE
        WHEN GREATEST(da.number_of_complete_days - da.number_of_incomplete_food_logs, 0) <= 0 THEN NULL
        ELSE ROUND(
            (da.total_protein::NUMERIC / GREATEST(da.number_of_complete_days - da.number_of_incomplete_food_logs, 1))
            / (COALESCE(NULLIF(w.weight, 0), 175) / 2.205), 3)
    END                                                     AS protein_per_kg_per_day,
    CASE
        WHEN GREATEST(da.number_of_complete_days - da.number_of_incomplete_food_logs, 0) <= 0 THEN NULL
        ELSE ROUND(
            (da.total_fat::NUMERIC / GREATEST(da.number_of_complete_days - da.number_of_incomplete_food_logs, 1))
            / (COALESCE(NULLIF(w.weight, 0), 175) / 2.205), 3)
    END                                                     AS fat_per_kg_per_day,
    CASE
        WHEN GREATEST(da.number_of_complete_days - da.number_of_incomplete_food_logs, 0) <= 0 THEN NULL
        ELSE ROUND(
            (da.total_carbs::NUMERIC / GREATEST(da.number_of_complete_days - da.number_of_incomplete_food_logs, 1))
            / (COALESCE(NULLIF(w.weight, 0), 175) / 2.205), 3)
    END                                                     AS carbs_per_kg_per_day,
    CASE
        WHEN da.number_of_complete_days <= 0 THEN NULL
        ELSE ROUND(da.total_screen_time::NUMERIC / da.number_of_complete_days, 2)
    END                                                     AS average_screen_time,
    CASE
        WHEN da.number_of_complete_days <= 0 THEN NULL
        ELSE ROUND(da.total_sleep_time::NUMERIC / da.number_of_complete_days, 2)
    END                                                     AS average_sleep_time,
    CASE
        WHEN da.number_of_complete_days <= 0 THEN NULL
        ELSE ROUND(da.total_weekly_steps::NUMERIC / da.number_of_complete_days, 0)
    END                                                     AS average_steps,
    da.total_screen_time,
    da.total_sleep_time,
    da.total_weekly_steps,
    da.weekly_pushups,
    da.weekly_minutes_read,
    da.weekly_minutes_tv,
    da.weekly_water_liters,
    da.weekly_water_gallons,
    da.weekly_alcoholic_drinks,
    da.weekly_points_scored,
    da.total_weekly_points,
    CASE
        WHEN da.total_weekly_points IS NULL OR da.total_weekly_points = 0 THEN NULL
        ELSE ROUND(da.weekly_points_scored::NUMERIC / da.total_weekly_points, 4)
    END                                                     AS weekly_score,
    COALESCE(ea.minutes_of_exercise, 0)                     AS minutes_of_exercise,
    COALESCE(ea.weekly_miles_run, 0)                        AS weekly_miles_run,
    COALESCE(ea.weekly_miles_all_activities, 0)             AS weekly_miles_all_activities,
    COALESCE(ea.weekly_workout_count, 0)                    AS weekly_workout_count
FROM week w
LEFT JOIN day_agg da ON da.week_id = w.week_id
LEFT JOIN exercise_agg ea ON ea.week_id = w.week_id;

COMMENT ON VIEW v_week_summary IS
    'Weekly rollup: nutrition totals/averages, macro-per-kg normalization, '
    'lifestyle totals/averages, hydration, points, exercise aggregates. '
    'Implements food_average_denominator logic from Derived Metric Rules.';
