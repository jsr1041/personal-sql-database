-- Migration — Add garmin_fit_file to exercise
-- Stores the source Garmin FIT filename for traceability back to the raw file.
-- Nullable: not all exercises will have a corresponding FIT file.

ALTER TABLE exercise
    ADD COLUMN garmin_fit_file VARCHAR(255);

COMMENT ON COLUMN exercise.garmin_fit_file IS 'Source Garmin FIT filename (e.g. 2026-03-30-08-15-00.fit). Used to trace a row back to its raw FIT file.';