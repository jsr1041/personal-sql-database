-- =============================================================================
-- migrate_add_fingerprint.sql
-- =============================================================================
-- Adds activity_fingerprint to the exercise table and tightens the source
-- uniqueness constraint.
--
-- Run order:
--   1. Patch nulls in source_record_id (Garmin rows only)
--   2. Add activity_fingerprint column
--   3. Add partial UNIQUE index on activity_fingerprint
--   4. Add partial UNIQUE index on (source_system, source_record_id)
--
-- Backfill of activity_fingerprint values is handled separately by
-- backfill_fingerprints.py — the column is intentionally left NULL here
-- so the migration is safe to run before the backfill.
--
-- Author: John Radcliffe
-- Date:   2026-04-06
-- =============================================================================


-- -----------------------------------------------------------------------------
-- STEP 1: Patch source_record_id for Garmin rows where it is NULL
--         (17 rows identified as of 2026-04-06)
-- -----------------------------------------------------------------------------

-- Preview first (comment out before running live)
-- SELECT exercise_id, garmin_fit_file, source_record_id
-- FROM exercise
-- WHERE source_record_id IS NULL
--   AND garmin_fit_file IS NOT NULL;

UPDATE exercise
SET source_record_id = REGEXP_REPLACE(garmin_fit_file, '\.[^.]+$', '')
WHERE source_record_id IS NULL
  AND garmin_fit_file IS NOT NULL
  AND is_deleted = FALSE;

-- Verify: should return 0 rows for Garmin-sourced records after patch
-- SELECT COUNT(*)
-- FROM exercise
-- WHERE garmin_fit_file IS NOT NULL
--   AND source_record_id IS NULL;


-- -----------------------------------------------------------------------------
-- STEP 2: Add activity_fingerprint column
-- -----------------------------------------------------------------------------

ALTER TABLE exercise
    ADD COLUMN IF NOT EXISTS activity_fingerprint TEXT;

COMMENT ON COLUMN exercise.activity_fingerprint IS
    'Cross-source dedup key. Format: YYYY-MM-DD|TypeOfActivity|D.D|MMM
     where distance is rounded to 0.1 mi and duration to 1 min.
     Example: 2024-03-15|Run|6.2|52
     Computed by backfill_fingerprints.py (historic) and
     batch_ingest_exercise.py / Strava ingest (new records).
     NULL only for records that lack date, type, distance, or duration.';


-- -----------------------------------------------------------------------------
-- STEP 3: Partial UNIQUE index on activity_fingerprint
--
-- Partial (WHERE NOT NULL) so that records missing enough data to form
-- a fingerprint can still be inserted without conflicting with each other.
-- -----------------------------------------------------------------------------

CREATE UNIQUE INDEX IF NOT EXISTS uix_exercise_fingerprint
    ON exercise (activity_fingerprint)
    WHERE activity_fingerprint IS NOT NULL;


-- -----------------------------------------------------------------------------
-- STEP 4: Partial UNIQUE index on (source_system, source_record_id)
--
-- Partial so that manually-created rows (NULL source_system / source_record_id)
-- are not constrained.
-- -----------------------------------------------------------------------------

CREATE UNIQUE INDEX IF NOT EXISTS uix_exercise_source
    ON exercise (source_system, source_record_id)
    WHERE source_system IS NOT NULL
      AND source_record_id IS NOT NULL;


-- -----------------------------------------------------------------------------
-- STEP 5: General index on activity_fingerprint for lookup performance
--         (the unique index above already covers this, but making it explicit
--          for clarity in query plans)
-- -----------------------------------------------------------------------------

-- No additional index needed — uix_exercise_fingerprint serves double duty.


-- -----------------------------------------------------------------------------
-- Verification queries (run after migration + backfill)
-- -----------------------------------------------------------------------------

-- 1. How many rows have a fingerprint?
-- SELECT
--     COUNT(*) FILTER (WHERE activity_fingerprint IS NOT NULL) AS with_fingerprint,
--     COUNT(*) FILTER (WHERE activity_fingerprint IS NULL)     AS without_fingerprint,
--     COUNT(*)                                                  AS total
-- FROM exercise
-- WHERE is_deleted = FALSE;

-- 2. Any fingerprint collisions before backfill? (should be 0 after unique index)
-- SELECT activity_fingerprint, COUNT(*) AS n
-- FROM exercise
-- WHERE activity_fingerprint IS NOT NULL
-- GROUP BY activity_fingerprint
-- HAVING COUNT(*) > 1;

-- 3. Source key coverage
-- SELECT
--     source_system,
--     COUNT(*) FILTER (WHERE source_record_id IS NOT NULL) AS with_record_id,
--     COUNT(*) FILTER (WHERE source_record_id IS NULL)     AS missing_record_id
-- FROM exercise
-- WHERE is_deleted = FALSE
-- GROUP BY source_system
-- ORDER BY source_system;
