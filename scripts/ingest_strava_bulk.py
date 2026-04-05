#!/usr/bin/env python3
"""
ingest_strava_bulk.py
---------------------
Bulk ingestion of Strava/Garmin .FIT files into the personal data warehouse.

Behavior:
  - NEW rows   → INSERT into exercise
  - EXISTING rows → Enrich NULL columns only; never overwrite populated data
  - Calendar rows (year/month/week/day) auto-created if missing

Manifest acceleration:
  If activity_manifest.json exists in the same directory as this script (or in
  --fit-dir), it is used to pre-filter files by year without opening any FIT
  files. This makes --year filtering near-instant even across large exports.
  Falls back to full FIT scan if no manifest is found.

Parameters:
  --fit-dir PATH     Required. Directory containing .FIT files.
  --dry-run          Preview all DB operations without committing
  --limit N          Process only N files (useful for testing)
  --year YYYY        Filter to activities in this calendar year only
  --manifest PATH    Explicit path to activity_manifest.json (optional override)
  --log-level LEVEL  DEBUG | INFO | WARNING (default: INFO)

Usage:
  python ingest_strava_bulk.py --fit-dir "C:\...\Strava Activities" --dry-run --year 2024 --limit 10
  python ingest_strava_bulk.py --fit-dir "C:\...\Strava Activities" --year 2024
  python ingest_strava_bulk.py --fit-dir "C:\...\Strava Activities"

Environment (.env):
  POSTGRES_DB       personal_crm
  POSTGRES_USER     john
  POSTGRES_PASSWORD <password>
  PG_HOST           localhost  (default)
  PG_PORT           5432       (default)
"""

import argparse
import calendar
import glob
import isoweek
import json
import logging
import os
from datetime import date, datetime, timezone

import psycopg2
from dotenv import load_dotenv
from fitparse import FitFile, StandardUnitsDataProcessor

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_SYSTEM = "garmin_fit"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# FIT parsing helpers
# ---------------------------------------------------------------------------

def parse_fit_session(fpath: str) -> dict | None:
    """Return a dict of session-level fields from a FIT file, or None if no session."""
    try:
        fit = FitFile(fpath, data_processor=StandardUnitsDataProcessor())
        for msg in fit.get_messages("session"):
            return {f.name: f.value for f in msg.fields}
        return None
    except Exception as e:
        log.debug("Cannot parse %s: %s", os.path.basename(fpath), e)
        return None


def extract_exercise_fields(raw: dict, activity_date: date) -> dict:
    """Map FIT session fields → exercise table columns."""

    def m_to_miles(meters):
        return round(meters / 1609.344, 2) if meters else None

    def s_to_min(seconds):
        return round(seconds / 60, 2) if seconds else None

    def m_to_feet(meters):
        return round(meters * 3.28084, 1) if meters else None

    def scale_if_int(val, factor):
        """Apply scale correction only if fitparse returned a raw int."""
        if val is None:
            return None
        return round(val * factor, 2) if isinstance(val, int) else round(float(val), 2)

    sport = (raw.get("sport") or "").lower().replace("sport.", "").strip() or None
    sub_sport = (raw.get("sub_sport") or "").lower().replace("sub_sport.", "").strip() or None
    if sub_sport in ("generic", ""):
        sub_sport = None

    avg_vr = scale_if_int(raw.get("avg_vertical_ratio"), 0.01)
    avg_stance = raw.get("avg_stance_time_percent") or raw.get("avg_stance_time_balance")

    return {
        "activity_date": activity_date,
        "type_of_activity": sport,
        "subtype_of_activity": sub_sport,
        "distance_miles": m_to_miles(raw.get("total_distance")),
        "duration_minutes": s_to_min(raw.get("total_timer_time")),
        "total_elapsed_time": s_to_min(raw.get("total_elapsed_time")),
        "elevation_gain_feet": m_to_feet(raw.get("total_ascent")),
        "average_heart_rate": raw.get("avg_heart_rate"),
        "max_heart_rate": raw.get("max_heart_rate"),
        "average_cadence": raw.get("avg_running_cadence"),
        "average_power": raw.get("avg_power"),
        "max_power": raw.get("max_power"),
        "calories": raw.get("total_calories"),
        "num_laps": raw.get("num_laps"),
        "tss_score": raw.get("training_stress_score"),
        "avg_vertical_ratio": avg_vr,
        "avg_stance_time_balance": avg_stance,
        "avg_step_length_mm": raw.get("avg_step_length"),
        "source_system": SOURCE_SYSTEM,
        "source_object": "session",
        "source_record_id": None,  # set by caller (filename)
    }


# ---------------------------------------------------------------------------
# Calendar helpers
# ---------------------------------------------------------------------------

def ensure_calendar(cur, activity_date: date, dry_run: bool) -> tuple[int, int]:
    """
    Ensure year/month/week/day rows exist for activity_date.
    Returns (day_id, week_id). Auto-creates missing rows as needed.
    """
    yr = activity_date.year
    mn = activity_date.month
    iso_year, iso_week, _ = activity_date.isocalendar()

    # --- YEAR ---
    cur.execute('SELECT year_id FROM "year" WHERE calendar_year = %s', (yr,))
    row = cur.fetchone()
    if row:
        year_id = row[0]
    else:
        log.info("  Creating year record for %d", yr)
        if not dry_run:
            cur.execute(
                'INSERT INTO "year" (calendar_year, source_system) VALUES (%s, %s) '
                'ON CONFLICT (calendar_year) DO NOTHING RETURNING year_id',
                (yr, SOURCE_SYSTEM),
            )
            result = cur.fetchone()
            year_id = result[0] if result else _fetch_one(
                cur, 'SELECT year_id FROM "year" WHERE calendar_year = %s', (yr,)
            )
        else:
            year_id = -1

    # --- MONTH ---
    month_name = date(yr, mn, 1).strftime("%B")
    last_day = calendar.monthrange(yr, mn)[1]

    cur.execute(
        "SELECT month_id FROM month WHERE calendar_year = %s AND month_number = %s",
        (yr, mn),
    )
    row = cur.fetchone()
    if row:
        month_id = row[0]
    else:
        log.info("  Creating month record for %d-%02d", yr, mn)
        if not dry_run:
            cur.execute(
                """
                INSERT INTO month
                    (calendar_year, month_number, month_name,
                     month_start_date, month_end_date, year_id, source_system)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (calendar_year, month_number) DO NOTHING
                RETURNING month_id
                """,
                (yr, mn, month_name, date(yr, mn, 1), date(yr, mn, last_day),
                 year_id, SOURCE_SYSTEM),
            )
            result = cur.fetchone()
            month_id = result[0] if result else _fetch_one(
                cur, "SELECT month_id FROM month WHERE calendar_year = %s AND month_number = %s",
                (yr, mn),
            )
        else:
            month_id = -1

    # --- WEEK ---
    w = isoweek.Week(iso_year, iso_week)
    week_start = w.monday()
    week_end = w.sunday()

    cur.execute(
        "SELECT week_id FROM week WHERE calendar_year = %s AND week_number = %s",
        (iso_year, iso_week),
    )
    row = cur.fetchone()
    if row:
        week_id = row[0]
    else:
        log.info("  Creating week record for ISO %d-W%02d", iso_year, iso_week)
        start_month_id = _get_or_create_month(cur, week_start, year_id, dry_run)
        if not dry_run:
            cur.execute(
                """
                INSERT INTO week
                    (calendar_year, week_number, week_start_date, week_end_date,
                     start_month_id, year_id, source_system)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (calendar_year, week_number) DO NOTHING
                RETURNING week_id
                """,
                (iso_year, iso_week, week_start, week_end,
                 start_month_id, year_id, SOURCE_SYSTEM),
            )
            result = cur.fetchone()
            week_id = result[0] if result else _fetch_one(
                cur, "SELECT week_id FROM week WHERE calendar_year = %s AND week_number = %s",
                (iso_year, iso_week),
            )
        else:
            week_id = -1

    # --- DAY ---
    cur.execute("SELECT day_id FROM day WHERE date = %s", (activity_date,))
    row = cur.fetchone()
    if row:
        day_id = row[0]
    else:
        log.info("  Creating day record for %s", activity_date)
        if not dry_run:
            cur.execute(
                """
                INSERT INTO day (date, week_id, month_id, year_id, source_system)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (date) DO NOTHING
                RETURNING day_id
                """,
                (activity_date, week_id, month_id, year_id, SOURCE_SYSTEM),
            )
            result = cur.fetchone()
            day_id = result[0] if result else _fetch_one(
                cur, "SELECT day_id FROM day WHERE date = %s", (activity_date,)
            )
        else:
            day_id = -1

    return day_id, week_id


def _fetch_one(cur, sql, params):
    """Run a SELECT and return the first column of the first row."""
    cur.execute(sql, params)
    return cur.fetchone()[0]


def _get_or_create_month(cur, d: date, year_id: int, dry_run: bool) -> int:
    """
    Return month_id for the month containing date d, creating it if missing.
    Used to resolve the week's start_month_id when the week starts in a
    different month (or year) than the activity date.
    """
    yr, mn = d.year, d.month
    cur.execute(
        "SELECT month_id FROM month WHERE calendar_year = %s AND month_number = %s",
        (yr, mn),
    )
    row = cur.fetchone()
    if row:
        return row[0]

    if dry_run:
        return -1

    # Need the year row too
    cur.execute('SELECT year_id FROM "year" WHERE calendar_year = %s', (yr,))
    yr_row = cur.fetchone()
    if yr_row:
        resolved_year_id = yr_row[0]
    else:
        cur.execute(
            'INSERT INTO "year" (calendar_year, source_system) VALUES (%s, %s) '
            'ON CONFLICT (calendar_year) DO NOTHING RETURNING year_id',
            (yr, SOURCE_SYSTEM),
        )
        result = cur.fetchone()
        resolved_year_id = result[0] if result else _fetch_one(
            cur, 'SELECT year_id FROM "year" WHERE calendar_year = %s', (yr,)
        )

    last_day = calendar.monthrange(yr, mn)[1]
    month_name = date(yr, mn, 1).strftime("%B")
    cur.execute(
        """
        INSERT INTO month
            (calendar_year, month_number, month_name,
             month_start_date, month_end_date, year_id, source_system)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (calendar_year, month_number) DO NOTHING
        RETURNING month_id
        """,
        (yr, mn, month_name, date(yr, mn, 1), date(yr, mn, last_day),
         resolved_year_id, SOURCE_SYSTEM),
    )
    result = cur.fetchone()
    return result[0] if result else _fetch_one(
        cur, "SELECT month_id FROM month WHERE calendar_year = %s AND month_number = %s",
        (yr, mn),
    )


# ---------------------------------------------------------------------------
# Exercise insert / enrich logic
# ---------------------------------------------------------------------------

def find_existing_exercise(cur, activity_date: date, source_record_id: str) -> dict | None:
    """
    Look for an existing exercise row by:
      1. Exact source_record_id (filename) match
      2. Same activity_date + source_system (catches re-exports with new filenames)
    Returns the full row as a dict, or None.
    """
    cur.execute(
        """
        SELECT * FROM exercise
        WHERE source_record_id = %s
          AND source_system = %s
          AND is_deleted = false
        """,
        (source_record_id, SOURCE_SYSTEM),
    )
    row = cur.fetchone()
    if row:
        return dict(zip([d[0] for d in cur.description], row))

    cur.execute(
        """
        SELECT * FROM exercise
        WHERE activity_date = %s
          AND source_system = %s
          AND is_deleted = false
        ORDER BY created_at
        LIMIT 1
        """,
        (activity_date, SOURCE_SYSTEM),
    )
    row = cur.fetchone()
    if row:
        return dict(zip([d[0] for d in cur.description], row))

    return None


# Columns that must never be overwritten during enrichment
_ENRICH_SKIP = {
    "exercise_id", "day_id", "week_id", "activity_date",
    "created_at", "updated_at", "last_synced_at", "is_deleted",
    "source_system", "source_object", "source_record_id",
    "salesforce_id", "source_org",
    # Subjective / manually-set fields
    "raw_notes", "workout_plan_id", "activity_detail_id",
    "route_id", "route_match_method", "route_match_confidence",
}


def build_enrich_update(existing: dict, new_fields: dict) -> tuple[dict, list]:
    """
    Return (updates_to_apply, enriched_column_names).
    Only fills in columns that are currently NULL in the existing row.
    Never touches protected columns.
    """
    updates, enriched = {}, []
    for col, new_val in new_fields.items():
        if col in _ENRICH_SKIP:
            continue
        if new_val is not None and existing.get(col) is None:
            updates[col] = new_val
            enriched.append(col)
    return updates, enriched


def upsert_exercise(
    cur,
    fields: dict,
    day_id: int,
    week_id: int,
    existing: dict | None,
    dry_run: bool,
) -> tuple[int | None, str]:
    """
    Insert a new exercise row or enrich an existing one.
    Returns (exercise_id, action) where action is 'inserted', 'enriched', or 'skipped'.
    """
    if existing is None:
        insert_fields = {**fields, "day_id": day_id, "week_id": week_id}
        cols = list(insert_fields.keys())
        placeholders = ", ".join(["%s"] * len(cols))
        col_str = ", ".join(cols)
        if not dry_run:
            cur.execute(
                f"INSERT INTO exercise ({col_str}) VALUES ({placeholders}) RETURNING exercise_id",
                list(insert_fields.values()),
            )
            return cur.fetchone()[0], "inserted"
        return None, "inserted"

    updates, enriched = build_enrich_update(existing, fields)
    if not updates:
        return existing["exercise_id"], "skipped"

    updates["updated_at"] = datetime.now(timezone.utc)
    updates["last_synced_at"] = datetime.now(timezone.utc)
    set_clause = ", ".join(f"{col} = %s" for col in updates)
    log.debug("    Enriching columns: %s", enriched)
    if not dry_run:
        cur.execute(
            f"UPDATE exercise SET {set_clause} WHERE exercise_id = %s",
            list(updates.values()) + [existing["exercise_id"]],
        )
    return existing["exercise_id"], "enriched"


# ---------------------------------------------------------------------------
# Main ingestion loop
# ---------------------------------------------------------------------------

def load_manifest(fit_dir: str, manifest_path: str | None) -> dict | None:
    """
    Load activity_manifest.json if available.
    Checks --manifest path first, then the fit_dir, then the script directory.
    Returns the parsed JSON dict, or None if not found.
    """
    candidates = []
    if manifest_path:
        candidates.append(manifest_path)
    candidates.append(os.path.join(fit_dir, "activity_manifest.json"))
    candidates.append(os.path.join(os.path.dirname(__file__), "activity_manifest.json"))
    candidates.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "activity_manifest.json"))

    for path in candidates:
        if os.path.exists(path):
            log.info("Using manifest: %s", path)
            with open(path) as f:
                return json.load(f)
    return None


def resolve_fit_files(fit_dir: str, year: int | None, manifest: dict | None) -> list[str]:
    """
    Return an ordered list of FIT file paths to process.
    Uses manifest for fast year-filtered lookup when available;
    falls back to full directory scan otherwise.
    """
    if manifest and "fit_files" in manifest:
        entries = manifest["fit_files"]
        if year:
            entries = [e for e in entries if e.get("year") == year]
            log.info("Manifest filtered to %d files for year %d", len(entries), year)
        else:
            log.info("Manifest contains %d FIT entries (all years)", len(entries))

        # Resolve full paths — try exact filename, then strip/add known prefixes
        paths = []
        missing = []
        for entry in entries:
            fname = entry["filename"]
            candidate = os.path.join(fit_dir, fname)
            if os.path.exists(candidate):
                paths.append(candidate)
            else:
                missing.append(fname)

        if missing:
            log.warning(
                "%d manifest entries not found in --fit-dir (run ls to verify filenames): %s%s",
                len(missing),
                ", ".join(missing[:5]),
                " ..." if len(missing) > 5 else "",
            )
        return sorted(paths)

    # No manifest — full scan
    log.info("No manifest found — scanning all .FIT files in %s", fit_dir)
    return sorted(glob.glob(os.path.join(fit_dir, "*.fit")))


def run(args):
    load_dotenv()

    db_config = {
        "dbname": os.environ["POSTGRES_DB"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "host": os.environ.get("PG_HOST", "localhost"),
        "port": int(os.environ.get("PG_PORT", 5432)),
    }

    fit_dir = args.fit_dir
    if args.dry_run:
        log.info("DRY RUN — no database writes will occur")

    # ---------------------------------------------------------------------------
    # Phase 1: Resolve file list (manifest-accelerated when available)
    # ---------------------------------------------------------------------------
    manifest = load_manifest(fit_dir, args.manifest)
    fit_files = resolve_fit_files(fit_dir, args.year, manifest)
    log.info("Candidate FIT files to process: %d", len(fit_files))

    # ---------------------------------------------------------------------------
    # Phase 2: Parse files (only opened if not already filtered by manifest)
    # ---------------------------------------------------------------------------
    log.info("Phase 2: Parsing FIT files...")
    activities = []

    for fpath in fit_files:
        raw = parse_fit_session(fpath)
        if raw is None:
            log.debug("Skipping (no session): %s", os.path.basename(fpath))
            continue

        start_time = raw.get("start_time")
        if start_time is None:
            log.debug("Skipping (no start_time): %s", os.path.basename(fpath))
            continue

        if isinstance(start_time, datetime):
            activity_date = start_time.date()
        else:
            try:
                activity_date = datetime.strptime(str(start_time), "%Y-%m-%d %H:%M:%S").date()
            except ValueError:
                log.debug("Skipping (unparseable date): %s", os.path.basename(fpath))
                continue

        # Year filter safety net (catches residual mismatches if manifest year is wrong)
        if args.year and activity_date.year != args.year:
            log.debug("Skipping (year mismatch): %s", os.path.basename(fpath))
            continue

        activities.append((fpath, activity_date, raw))

    if args.limit:
        activities = activities[: args.limit]

    log.info("Activities to process after filter: %d", len(activities))
    if not activities:
        log.info("Nothing to process. Exiting.")
        return

    # ---------------------------------------------------------------------------
    # Phase 2: DB writes
    # ---------------------------------------------------------------------------
    conn = psycopg2.connect(**db_config)
    stats = {"inserted": 0, "enriched": 0, "skipped": 0, "errors": 0}

    try:
        with conn:
            with conn.cursor() as cur:
                for i, (fpath, activity_date, raw) in enumerate(activities, 1):
                    fname = os.path.basename(fpath)
                    log.info(
                        "[%d/%d] %s  %s  sport=%s",
                        i, len(activities), fname, activity_date,
                        raw.get("sport", "?"),
                    )

                    try:
                        day_id, week_id = ensure_calendar(cur, activity_date, args.dry_run)

                        ex_fields = extract_exercise_fields(raw, activity_date)
                        ex_fields["source_record_id"] = fname

                        existing = find_existing_exercise(cur, activity_date, fname)

                        exercise_id, action = upsert_exercise(
                            cur, ex_fields, day_id, week_id, existing, args.dry_run
                        )
                        stats[action] += 1
                        log.info("  → %s (exercise_id=%s)", action.upper(), exercise_id)

                        # Commit every 100 rows to keep memory footprint small
                        if not args.dry_run and i % 100 == 0:
                            conn.commit()
                            log.info("  Committed batch at row %d", i)

                    except Exception as e:
                        log.error("  ERROR on %s: %s", fname, e)
                        stats["errors"] += 1
                        if not args.dry_run:
                            conn.rollback()
                        continue

                if not args.dry_run:
                    conn.commit()

    finally:
        conn.close()

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    log.info("")
    log.info("=" * 55)
    log.info("INGESTION SUMMARY%s", "  [DRY RUN — nothing written]" if args.dry_run else "")
    log.info("  Files processed : %d", len(activities))
    log.info("  Inserted        : %d", stats["inserted"])
    log.info("  Enriched        : %d", stats["enriched"])
    log.info("  Skipped (match) : %d", stats["skipped"])
    log.info("  Errors          : %d", stats["errors"])
    log.info("=" * 55)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Bulk ingest Strava/Garmin FIT files into personal PostgreSQL data warehouse.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=r"""
Examples:
  python ingest_strava_bulk.py --fit-dir "C:\Users\johns\Claude Co-Work\Projects\Personal Database & CRM\Personal Database & CRM\Strava Activities" --dry-run --year 2024 --limit 10
  python ingest_strava_bulk.py --fit-dir "C:\...\Strava Activities" --year 2024
  python ingest_strava_bulk.py --fit-dir "C:\...\Strava Activities"
        """,
    )
    parser.add_argument("--fit-dir", type=str, required=True, metavar="PATH",
                        help="Required. Directory containing .FIT files.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and preview without writing to the database.")
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="Stop after processing N activity files.")
    parser.add_argument("--year", type=int, default=None, metavar="YYYY",
                        help="Only process activities from this calendar year.")
    parser.add_argument("--manifest", type=str, default=None, metavar="PATH",
                        help="Explicit path to activity_manifest.json (auto-discovered if omitted).")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING"], default="INFO",
                        help="Logging verbosity (default: INFO).")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    run(args)
