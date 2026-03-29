-- =============================================================================
-- Query: exercise_workout_summary
-- Description: Joins exercise, workout_analysis, and exercise_trackpoint to
--              produce a unified view of objective metrics, subjective analysis,
--              and trackpoint coverage per workout session.
-- Tables: exercise, workout_analysis, exercise_trackpoint
-- Repo: github.com/jsr1041/Personal-SQL-Database
-- Created: 2026-03-29
-- =============================================================================

SELECT
    e.exercise_id,
    wa.workout_analysis_id,
    e.activity_date,
    e.type_of_activity,
    e.subtype_of_activity,

    -- Objective metrics
    e.distance_miles,
    e.duration_minutes,
    e.average_heart_rate,
    e.max_heart_rate,
    e.elevation_gain_feet,
    e.calories,
    e.tss_score,
    e.average_cadence,
    e.average_power,
    e.avg_vertical_ratio,
    e.avg_stance_time_balance,
    e.avg_step_length_mm,

    -- Subjective analysis
    wa.perceived_level_of_effort,
    wa.rating,
    wa.session_quality,
    wa.felt_strong,
    wa.felt_fatigued,
    wa.mental_state,
    wa.training_purpose,
    wa.analysis_notes,
    wa.analysis_summary,
    wa.analysis_date,

    -- Trackpoint coverage
    COALESCE(tp.trackpoint_count, 0) AS trackpoint_count

FROM exercise e
LEFT JOIN workout_analysis wa
    ON wa.exercise_id = e.exercise_id
LEFT JOIN (
    SELECT exercise_id, COUNT(*) AS trackpoint_count
    FROM exercise_trackpoint
    GROUP BY exercise_id
) tp ON tp.exercise_id = e.exercise_id

ORDER BY e.activity_date DESC;
