-- =============================================================================
-- Migration: 026_create_view_v_exercise_enriched.sql
-- View:      v_exercise_enriched
-- Purpose:   Objective workout facts plus exercise-level derived metrics
--            (elevation gain per mile, speed/pace summaries).
-- Depends:   exercise
-- Author:    Claude (overnight session 2026-03-27)
-- Updated:   2026-03-29 — e.id → e.exercise_id (actual PK);
--            e.planned → (e.workout_plan_id IS NOT NULL) AS planned;
--            migration number corrected to 026;
--            activity_category CASE updated to match Salesforce picklist values
--            (Run, Bike, Gym, Walk, Hike, Ski, Climb, Swim, Rowing).
-- =============================================================================

DROP VIEW IF EXISTS v_exercise_enriched;

CREATE OR REPLACE VIEW v_exercise_enriched AS
SELECT
    e.exercise_id,
    e.day_id,
    e.week_id,
    e.activity_date,
    e.type_of_activity,
    e.subtype_of_activity,
    (e.workout_plan_id IS NOT NULL) AS planned,
    e.distance_miles,
    e.duration_minutes,
    e.total_elapsed_time,
    e.elevation_gain_feet,
    e.num_laps,
    e.average_heart_rate,
    e.max_heart_rate,
    e.bad_hr_data,
    e.average_cadence,
    e.average_power,
    e.max_power,
    e.avg_vertical_ratio,
    e.avg_stance_time_balance,
    e.avg_step_length_mm,
    e.tss_score,
    e.sweat_rate_l_per_hr,
    e.calories,
    e.source_system,
    e.source_object,
    CASE
        WHEN e.distance_miles IS NULL OR e.distance_miles = 0 THEN NULL
        ELSE ROUND(e.elevation_gain_feet::NUMERIC / e.distance_miles, 1)
    END AS elevation_gain_per_mile,
    CASE
        WHEN e.distance_miles IS NULL OR e.distance_miles = 0 THEN NULL
        WHEN e.duration_minutes IS NULL THEN NULL
        ELSE ROUND(e.duration_minutes::NUMERIC / e.distance_miles, 2)
    END AS avg_pace_min_per_mile,
    CASE
        WHEN e.duration_minutes IS NULL OR e.duration_minutes = 0 THEN NULL
        WHEN e.distance_miles IS NULL THEN NULL
        ELSE ROUND(e.distance_miles::NUMERIC / (e.duration_minutes / 60.0), 2)
    END AS avg_speed_mph,
    CASE
        WHEN lower(e.type_of_activity) = 'run'    THEN 'running'
        WHEN lower(e.type_of_activity) = 'bike'   THEN 'cycling'
        WHEN lower(e.type_of_activity) = 'gym'    THEN 'strength'
        WHEN lower(e.type_of_activity) = 'walk'   THEN 'walking'
        WHEN lower(e.type_of_activity) = 'hike'   THEN 'hiking'
        WHEN lower(e.type_of_activity) = 'ski'    THEN 'skiing'
        WHEN lower(e.type_of_activity) = 'climb'  THEN 'climbing'
        WHEN lower(e.type_of_activity) = 'swim'   THEN 'swimming'
        WHEN lower(e.type_of_activity) = 'rowing' THEN 'rowing'
        ELSE 'other'
    END AS activity_category
FROM exercise e;

COMMENT ON VIEW v_exercise_enriched IS
    'Exercise record enriched with elevation_gain_per_mile, avg_pace_min_per_mile, '
    'avg_speed_mph, and activity_category grouping. Source: exercise table only.';
