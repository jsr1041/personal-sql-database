#!/usr/bin/env python3
"""
delete_exercise.py

Delete an exercise record and all related child rows.
Target by --date (activity_date) or --exercise-id (exact row).

Child tables deleted in order before the exercise row:
  - exercise_trackpoint  (FK exercise_id)
  - workout_analysis     (FK exercise_id)
  - exercise_gear        (FK exercise_id)

Usage:
  python delete_exercise.py --date 2025-11-03
  python delete_exercise.py --date 2025-11-03 --dry-run
  python delete_exercise.py --exercise-id 412
  python delete_exercise.py --exercise-id 412 --dry-run

Flags:
  --date YYYY-MM-DD    Target all exercise rows for an activity_date.
  --exercise-id INT    Target a single exercise row by PK.
  --dry-run            Preview counts and rows; no mutations.

Environment variables (in .env):
  POSTGRES_DB       Database name (default: personal_crm)
  POSTGRES_USER     Postgres username (default: john)
  POSTGRES_PASSWORD Postgres password
  PG_HOST           Host (default: localhost)
  PG_PORT           Port (default: 5432)

Note: --date and --exercise-id are mutually exclusive.
"""

import argparse
import os
import sys
from datetime import date

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()


# ── DB connection ─────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "personal_crm"),
        user=os.getenv("POSTGRES_USER", "john"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
    )


# ── Fetch helpers ─────────────────────────────────────────────────────────────

EXERCISE_SELECT = """
    SELECT
        exercise_id,
        activity_date,
        type_of_activity,
        subtype_of_activity,
        distance_miles,
        duration_minutes,
        source_record_id
    FROM exercise
    WHERE {where}
      AND is_deleted = false
    ORDER BY exercise_id
"""

def fetch_by_date(cur, target_date: date):
    cur.execute(
        EXERCISE_SELECT.format(where="activity_date = %s"),
        (target_date,),
    )
    return cur.fetchall()


def fetch_by_id(cur, exercise_id: int):
    cur.execute(
        EXERCISE_SELECT.format(where="exercise_id = %s"),
        (exercise_id,),
    )
    return cur.fetchall()


def fetch_child_counts(cur, exercise_id: int) -> dict:
    counts = {}
    for table in ("exercise_trackpoint", "workout_analysis", "exercise_gear"):
        cur.execute(
            f"SELECT COUNT(*) AS count FROM {table} WHERE exercise_id = %s",
            (exercise_id,),
        )
        counts[table] = cur.fetchone()["count"]
    return counts


# ── Display helpers ───────────────────────────────────────────────────────────

def print_exercise_row(row: dict, child_counts: dict):
    print(f"\n  exercise_id        : {row['exercise_id']}")
    print(f"  activity_date      : {row['activity_date']}")
    print(f"  type               : {row['type_of_activity'] or '—'}")
    print(f"  subtype            : {row['subtype_of_activity'] or '—'}")
    print(f"  distance_miles     : {row['distance_miles'] or '—'}")
    print(f"  duration_min       : {row['duration_minutes'] or '—'}")
    print(f"  source_record_id   : {row['source_record_id'] or '—'}")
    print(f"  → trackpoints      : {child_counts['exercise_trackpoint']:,}")
    print(f"  → workout_analysis : {child_counts['workout_analysis']}")
    print(f"  → exercise_gear    : {child_counts['exercise_gear']}")


# ── Delete logic ──────────────────────────────────────────────────────────────

def delete_exercise(cur, exercise_id: int) -> dict:
    """Delete child rows then the exercise row. Returns deleted counts."""
    deleted = {}
    for table in ("exercise_trackpoint", "workout_analysis", "exercise_gear"):
        cur.execute(
            f"DELETE FROM {table} WHERE exercise_id = %s",
            (exercise_id,),
        )
        deleted[table] = cur.rowcount

    cur.execute("DELETE FROM exercise WHERE exercise_id = %s", (exercise_id,))
    deleted["exercise"] = cur.rowcount
    return deleted


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Delete exercise record(s) and all related child rows.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    lookup = parser.add_mutually_exclusive_group(required=True)
    lookup.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Target all exercise rows for an activity_date",
    )
    lookup.add_argument(
        "--exercise-id",
        type=int,
        metavar="INT",
        help="Target a single exercise row by primary key",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be deleted; make no changes",
    )

    args = parser.parse_args()
    dry_run = args.dry_run

    # ── Resolve lookup mode ───────────────────────────────────────────────────
    if args.date:
        try:
            target_date = date.fromisoformat(args.date)
        except ValueError:
            print(f"ERROR: Invalid date '{args.date}'. Use YYYY-MM-DD format.")
            sys.exit(1)
        lookup_label = f"date={target_date}"
    else:
        target_date = None
        lookup_label = f"exercise_id={args.exercise_id}"

    print(f"\n{'[DRY RUN] ' if dry_run else ''}delete_exercise.py")
    print(f"  Target : {lookup_label}")
    print(f"  Mode   : {'DRY RUN (no changes)' if dry_run else 'LIVE DELETE'}")

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ── Fetch matching exercises ──────────────────────────────────────────────
    if target_date:
        exercises = fetch_by_date(cur, target_date)
    else:
        exercises = fetch_by_id(cur, args.exercise_id)

    if not exercises:
        print(f"\nNo exercise records found for {lookup_label}. Nothing to do.")
        conn.close()
        sys.exit(0)

    print(f"\nFound {len(exercises)} exercise record(s):\n")
    print("-" * 60)

    exercise_data = []
    total_trackpoints = 0
    for row in exercises:
        counts = fetch_child_counts(cur, row["exercise_id"])
        exercise_data.append((row, counts))
        total_trackpoints += counts["exercise_trackpoint"]
        print_exercise_row(row, counts)

    print(f"\n{'─' * 60}")
    print(f"  Total exercise rows   : {len(exercises)}")
    print(f"  Total trackpoints     : {total_trackpoints:,}")

    # ── Dry run exits here ────────────────────────────────────────────────────
    if dry_run:
        print(f"\n[DRY RUN] No changes made. Re-run without --dry-run to execute.")
        cur.close()
        conn.close()
        sys.exit(0)

    # ── Confirmation prompt ───────────────────────────────────────────────────
    print()
    confirm = input(
        f"Delete {len(exercises)} exercise record(s) and all related rows? [yes/no]: "
    ).strip().lower()

    if confirm != "yes":
        print("Aborted. No changes made.")
        cur.close()
        conn.close()
        sys.exit(0)

    # ── Execute in a single transaction ──────────────────────────────────────
    print()
    try:
        for row, _ in exercise_data:
            eid = row["exercise_id"]
            deleted = delete_exercise(cur, eid)
            print(
                f"  Deleted exercise_id {eid}: "
                f"{deleted['exercise_trackpoint']:,} trackpoints, "
                f"{deleted['workout_analysis']} workout_analysis, "
                f"{deleted['exercise_gear']} exercise_gear, "
                f"{deleted['exercise']} exercise row"
            )

        conn.commit()
        print(f"\n✓ Transaction committed. {len(exercises)} exercise record(s) deleted.")

    except Exception as e:
        conn.rollback()
        print(f"\nERROR: {e}")
        print("Transaction rolled back. No changes made.")
        cur.close()
        conn.close()
        sys.exit(1)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
