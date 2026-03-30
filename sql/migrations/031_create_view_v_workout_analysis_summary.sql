-- =============================================================================
-- Migration: 031_create_view_v_workout_analysis_summary.sql
-- View:      v_workout_analysis_summary
-- Purpose:   Join workout_analysis to exercise and calendar hierarchy
--            for subjective-objective reporting and trend analysis.
-- Depends:   workout_analysis, exercise, day, week, month, year
-- Author:    Claude (overnight session 2026-03-27)
-- Updated:   2026-03-30 — PKs corrected (wa.workout_analysis_id, e.exercise_id,
--            d.day_id, w.week_id, mo.month_id, yr.year_id); all JOINs updated;
--            e.planned → (e.workout_plan_id IS NOT NULL) AS planned.
-- =============================================================================

DROP VIEW IF EXISTS v_workout_analysis_summary;

CREATE OR REPLACE VIEW v_workout_analysis_summary AS
SELECT
    -- Identity
    wa.workout_analysis_id,
    wa.exercise_id,
    e.activity_date,
    e.type_of_activity,
    e.subtype_of_activity,
    (e.workout_plan_id IS NOT NULL)     AS planned,
    -- Calendar context
    d.day_id,
    d.week_id,
    d.month_id,
    d.year_id,
    w.calendar_year,
    w.week_number,
    -- Objective facts (from exercise)
    e.distance_miles,
    e.duration_minutes,
    e.elevation_gain_feet,
    e.average_heart_rate,
    e.max_heart_rate,
    e.calories,
    e.tss_score,
    e.raw_notes,
    -- Subjective fields (from workout_analysis)
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
    wa.source_system,
    wa.source_object,
    -- -------------------------------------------------------------------------
    -- Derived: RPE vs HR ratio (subjective effort relative to objective HR)
    -- Only meaningful when both RPE and avg HR are present
    -- -------------------------------------------------------------------------
    CASE
        WHEN wa.perceived_level_of_effort IS NULL THEN NULL
        WHEN e.average_heart_rate IS NULL OR e.average_heart_rate = 0 THEN NULL
        ELSE ROUND(wa.perceived_level_of_effort::NUMERIC / e.average_heart_rate, 4)
    END                                 AS rpe_per_hr_unit,
    -- -------------------------------------------------------------------------
    -- Derived: elevation gain per mile (consistent with v_exercise_enriched)
    -- -------------------------------------------------------------------------
    CASE
        WHEN e.distance_miles IS NULL OR e.distance_miles = 0 THEN NULL
        ELSE ROUND(e.elevation_gain_feet::NUMERIC / e.distance_miles, 1)
    END                                 AS elevation_gain_per_mile
FROM workout_analysis wa
JOIN exercise e     ON e.exercise_id = wa.exercise_id
LEFT JOIN day d     ON d.day_id = e.day_id
LEFT JOIN week w    ON w.week_id = d.week_id
LEFT JOIN month mo  ON mo.month_id = d.month_id
LEFT JOIN year yr   ON yr.year_id = d.year_id;

COMMENT ON VIEW v_workout_analysis_summary IS
    'Combines workout_analysis (subjective) with exercise (objective) and '
    'full calendar hierarchy. Supports RPE trends, session quality reports, '
    'and subjective-vs-objective load comparisons.';
