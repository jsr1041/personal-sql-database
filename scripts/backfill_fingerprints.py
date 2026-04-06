#!/usr/bin/env python3
"""
backfill_fingerprints.py
------------------------
Backfills activity_fingerprint for existing exercise rows that were inserted
before the fingerprint column was added.

Fingerprint format: YYYY-MM-DD|TypeOfActivity|D.D|MMM
  - activity_date       exact date
  - type_of_activity    canonical value (e.g. Run, Walk, Hike)
  - distance_miles      rounded to 1 decimal place
  - duration_minutes    rounded to nearest whole minute

Example: 2024-03-15|Run|6.2|52

Rows that lack activity_date, type_of_activity, distance_miles, or
duration_minutes cannot form a fingerprint and are skipped (left NULL).

Usage:
  # Preview what would happen — no writes
  python backfill_fingerprints.py --dry-run

  # Backfill only 2023 activities
  python backfill_fingerprints.py --year 2023

  # Backfill first 50 rows (test run)
  python backfill_fingerprints.py --limit 50

  # Full production backfill
  python backfill_fingerprints.py

  # Combine flags
  python backfill_fingerprints.py --year 2024 --limit 100 --dry-run

CLI flags:
  --dry-run     Parse and compute fingerprints, print results, write nothing.
  --limit N     Process at most N rows (useful for test runs).
  --year YYYY   Restrict to rows where activity_date falls in this year.

Dependencies:
  pip install psycopg2-binary python-dotenv --break-system-packages

Environment variables (in .env):
  POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, PG_HOST, PG_PORT

Author: John Radcliffe
"""

import os
import sys
import argparse
from datetime import datetime

try:
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv()
except ImportError as e:
    print(f"ERROR: missing dependency — {e}")
    sys.exit(1)


# ---------------------------------------------------------------------------
# DB CONNECTION
# ---------------------------------------------------------------------------

def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "personal_crm"),
        user=os.getenv("POSTGRES_USER", "john"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
    )


# ---------------------------------------------------------------------------
# FINGERPRINT LOGIC  (keep in sync with batch_ingest_exercise.py)
# ---------------------------------------------------------------------------

def make_fingerprint(activity_date, type_of_activity, distance_miles, duration_minutes):
    """
    Compute a normalized cross-source dedup key.

    Returns a string like '2024-03-15|Run|6.2|52', or None if any required
    field is missing.

    Rounding:
      distance_miles    → 1 decimal place  (0.1 mi tolerance)
      duration_minutes  → 0 decimal places (1 min tolerance)
    """
    if not activity_date or not type_of_activity or distance_miles is None or duration_minutes is None:
        return None

    try:
        d    = str(activity_date)                        # date or datetime.date → 'YYYY-MM-DD'
        t    = str(type_of_activity).strip()
        dist = str(round(float(distance_miles), 1))
        dur  = str(round(float(duration_minutes), 0))    # e.g. '52.0' — normalize below
        # Strip trailing .0 for cleaner strings: 52.0 → 52
        if dur.endswith(".0"):
            dur = dur[:-2]
        return f"{d}|{t}|{dist}|{dur}"
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# FETCH ROWS NEEDING BACKFILL
# ---------------------------------------------------------------------------

def fetch_rows(conn, year=None, limit=None):
    """
    Return exercise rows where activity_fingerprint IS NULL and the row has
    enough data to compute one.
    """
    sql = """
        SELECT
            exercise_id,
            activity_date,
            type_of_activity,
            distance_miles,
            duration_minutes
        FROM exercise
        WHERE activity_fingerprint IS NULL
          AND is_deleted = FALSE
          AND activity_date       IS NOT NULL
          AND type_of_activity    IS NOT NULL
          AND distance_miles      IS NOT NULL
          AND duration_minutes    IS NOT NULL
    """
    params = []

    if year:
        sql += " AND EXTRACT(YEAR FROM activity_date) = %s"
        params.append(year)

    sql += " ORDER BY activity_date DESC, exercise_id DESC"

    if limit:
        sql += " LIMIT %s"
        params.append(limit)

    with conn.cursor() as cur:
        cur.execute(sql, params or None)
        return cur.fetchall()


def fetch_skippable_rows(conn, year=None):
    """
    Return rows that will be skipped (cannot form a fingerprint).
    Used in dry-run reporting only.
    """
    sql = """
        SELECT COUNT(*)
        FROM exercise
        WHERE activity_fingerprint IS NULL
          AND is_deleted = FALSE
          AND (
              activity_date    IS NULL OR
              type_of_activity IS NULL OR
              distance_miles   IS NULL OR
              duration_minutes IS NULL
          )
    """
    params = []
    if year:
        sql += " AND EXTRACT(YEAR FROM activity_date) = %s"
        params.append(year)

    with conn.cursor() as cur:
        cur.execute(sql, params or None)
        return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# CONFLICT DETECTION
# ---------------------------------------------------------------------------

def detect_conflicts(rows):
    """
    Before writing anything, check whether the computed fingerprints contain
    duplicates within the batch itself (two existing rows would get the same
    fingerprint).

    Returns list of (fingerprint, [exercise_ids]) for any conflicts found.
    """
    seen = {}
    for exercise_id, activity_date, type_of_activity, distance_miles, duration_minutes in rows:
        fp = make_fingerprint(activity_date, type_of_activity, distance_miles, duration_minutes)
        if fp is None:
            continue
        seen.setdefault(fp, []).append(exercise_id)

    return [(fp, ids) for fp, ids in seen.items() if len(ids) > 1]


def detect_db_conflicts(conn, rows):
    """
    Check whether any of the computed fingerprints already exist in the DB
    (from a previous partial backfill or a live ingest that ran ahead).

    Returns list of (fingerprint, existing_exercise_id) for conflicts found.
    """
    fingerprints = {}
    for exercise_id, activity_date, type_of_activity, distance_miles, duration_minutes in rows:
        fp = make_fingerprint(activity_date, type_of_activity, distance_miles, duration_minutes)
        if fp:
            fingerprints[fp] = exercise_id  # maps fp → the row we're trying to backfill

    if not fingerprints:
        return []

    placeholders = ",".join(["%s"] * len(fingerprints))
    sql = f"""
        SELECT activity_fingerprint, exercise_id
        FROM exercise
        WHERE activity_fingerprint IN ({placeholders})
    """
    with conn.cursor() as cur:
        cur.execute(sql, list(fingerprints.keys()))
        existing = cur.fetchall()

    conflicts = []
    for fp, existing_id in existing:
        our_id = fingerprints.get(fp)
        if our_id and our_id != existing_id:
            conflicts.append((fp, existing_id, our_id))

    return conflicts


# ---------------------------------------------------------------------------
# WRITE
# ---------------------------------------------------------------------------

def update_fingerprint(conn, exercise_id, fingerprint):
    """
    Update a single row. Raises psycopg2.errors.UniqueViolation on conflict.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE exercise
            SET activity_fingerprint = %s
            WHERE exercise_id = %s
              AND activity_fingerprint IS NULL
            """,
            (fingerprint, exercise_id),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Backfill activity_fingerprint for existing exercise rows."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Compute and display fingerprints; write nothing to the database.",
    )
    parser.add_argument(
        "--limit", type=int, default=None, metavar="N",
        help="Process at most N rows (useful for test runs).",
    )
    parser.add_argument(
        "--year", type=int, default=None, metavar="YYYY",
        help="Restrict backfill to rows where activity_date falls in this year.",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("backfill_fingerprints.py")
    print(f"  dry-run : {args.dry_run}")
    print(f"  year    : {args.year or 'all'}")
    print(f"  limit   : {args.limit or 'none'}")
    print("=" * 70)

    conn = get_connection()

    # ── Fetch rows to process ──────────────────────────────────────────────
    rows = fetch_rows(conn, year=args.year, limit=args.limit)
    skippable = fetch_skippable_rows(conn, year=args.year)

    print(f"\nRows eligible for backfill : {len(rows)}")
    print(f"Rows skipped (missing data) : {skippable}")

    if not rows:
        print("\nNothing to backfill. Exiting.")
        conn.close()
        return

    # ── Conflict checks ────────────────────────────────────────────────────
    batch_conflicts = detect_conflicts(rows)
    if batch_conflicts:
        print(f"\n⚠️  BATCH CONFLICTS DETECTED ({len(batch_conflicts)} fingerprints shared by multiple rows):")
        for fp, ids in batch_conflicts:
            print(f"   {fp}  →  exercise_ids: {ids}")
        print("\n   These rows have identical date+type+distance+duration.")
        print("   Inspect them before running live — only one can hold the fingerprint.")
        if not args.dry_run:
            print("\n   Aborting. Resolve conflicts first, then re-run.")
            conn.close()
            sys.exit(1)

    db_conflicts = detect_db_conflicts(conn, rows)
    if db_conflicts:
        print(f"\n⚠️  DB CONFLICTS DETECTED ({len(db_conflicts)} fingerprints already exist in DB):")
        for fp, existing_id, our_id in db_conflicts:
            print(f"   {fp}  →  existing exercise_id={existing_id}, this row exercise_id={our_id}")
        if not args.dry_run:
            print("\n   Aborting. Resolve conflicts first, then re-run.")
            conn.close()
            sys.exit(1)

    # ── Process ────────────────────────────────────────────────────────────
    print(f"\n{'DRY RUN — no writes' if args.dry_run else 'Writing to database...'}")
    print("-" * 70)

    counts = {"updated": 0, "skipped_null_fp": 0, "error": 0}

    for exercise_id, activity_date, type_of_activity, distance_miles, duration_minutes in rows:
        fp = make_fingerprint(activity_date, type_of_activity, distance_miles, duration_minutes)

        if fp is None:
            counts["skipped_null_fp"] += 1
            print(f"  SKIP  exercise_id={exercise_id:<8}  (could not compute fingerprint)")
            continue

        if args.dry_run:
            print(f"  WOULD UPDATE  exercise_id={exercise_id:<8}  →  {fp}")
            counts["updated"] += 1
            continue

        # Live write — one row at a time so conflicts surface cleanly
        try:
            update_fingerprint(conn, exercise_id, fp)
            print(f"  UPDATED  exercise_id={exercise_id:<8}  →  {fp}")
            counts["updated"] += 1
        except Exception as e:
            conn.rollback()
            counts["error"] += 1
            print(f"  ERROR    exercise_id={exercise_id:<8}  →  {fp}  |  {e}")

    # ── Summary ────────────────────────────────────────────────────────────
    print("-" * 70)
    if args.dry_run:
        print(f"DRY RUN complete.")
        print(f"  Would update : {counts['updated']}")
        print(f"  Would skip   : {counts['skipped_null_fp']}")
    else:
        print(f"Backfill complete.")
        print(f"  Updated : {counts['updated']}")
        print(f"  Skipped : {counts['skipped_null_fp']}")
        print(f"  Errors  : {counts['error']}")

    conn.close()


if __name__ == "__main__":
    main()
