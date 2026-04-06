#!/usr/bin/env python3
"""
seed_calendar_year.py
---------------------
Seeds year / month / week / day rows for a given calendar year.

Behavior:
  - Checks for existing rows before inserting — skips any that already exist
  - Prints a pre-flight summary so you can confirm before writing
  - Requires explicit --confirm flag to actually write (dry-run by default)
  - ISO week alignment: weeks that start in Dec of the prior year or end in
    Jan of the next year are handled correctly (week year != calendar year)
  - Also seeds any partial prior/next year rows required by ISO week boundaries

Usage:
  # Preview what would be created (safe, no writes)
  python seed_calendar_year.py --year 2022

  # Actually write to the database
  python seed_calendar_year.py --year 2022 --confirm

  # Seed multiple years
  python seed_calendar_year.py --year 2019 --confirm
  python seed_calendar_year.py --year 2020 --confirm

Environment (.env in script directory or CWD):
  POSTGRES_DB        personal_crm
  POSTGRES_USER      john
  POSTGRES_PASSWORD  <password>
  PG_HOST            localhost  (default)
  PG_PORT            5432       (default)

Author: John Radcliffe
"""

import argparse
import calendar
import os
import sys
from datetime import date, timedelta

try:
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv()
except ImportError as e:
    print(f"ERROR: missing dependency — {e}")
    print("Run: pip install psycopg2-binary python-dotenv --break-system-packages")
    sys.exit(1)


SOURCE_SYSTEM = "seed_calendar_year.py"


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "personal_crm"),
        user=os.getenv("POSTGRES_USER", "john"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", 5432)),
    )


# ---------------------------------------------------------------------------
# Calendar math helpers
# ---------------------------------------------------------------------------

def all_months(year: int) -> list[tuple[int, int]]:
    """Return [(year, month_number), ...] for all 12 months."""
    return [(year, m) for m in range(1, 13)]


def all_weeks_touching_year(year: int) -> list[tuple[int, int]]:
    """
    Return all ISO (iso_year, iso_week) pairs for weeks that contain at least
    one day in `year`. This naturally handles:
      - Week 1 of `year` possibly starting in Dec of year-1
      - The last week of `year` possibly ending in Jan of year+1
    """
    weeks = set()
    d = date(year, 1, 1)
    end = date(year, 12, 31)
    while d <= end:
        iso = d.isocalendar()
        weeks.add((iso[0], iso[1]))
        d += timedelta(days=1)
    return sorted(weeks)


def week_bounds(iso_year: int, iso_week: int) -> tuple[date, date]:
    """Return (monday, sunday) for an ISO week."""
    # Jan 4 is always in ISO week 1
    jan4 = date(iso_year, 1, 4)
    # Monday of week 1
    week1_monday = jan4 - timedelta(days=jan4.weekday())
    monday = week1_monday + timedelta(weeks=iso_week - 1)
    sunday = monday + timedelta(days=6)
    return monday, sunday


def all_days(year: int) -> list[date]:
    """Return all 365/366 dates in `year`."""
    d = date(year, 1, 1)
    end = date(year, 12, 31)
    days = []
    while d <= end:
        days.append(d)
        d += timedelta(days=1)
    return days


# ---------------------------------------------------------------------------
# Existence checks
# ---------------------------------------------------------------------------

def existing_years(cur, years: list[int]) -> set[int]:
    if not years:
        return set()
    cur.execute(
        'SELECT calendar_year FROM "year" WHERE calendar_year = ANY(%s)',
        (years,),
    )
    return {r[0] for r in cur.fetchall()}


def existing_months(cur, year_month_pairs: list[tuple[int, int]]) -> set[tuple[int, int]]:
    if not year_month_pairs:
        return set()
    # Build explicit OR conditions — avoids psycopg2 row-type casting issues
    clauses = " OR ".join(
        "(calendar_year = %s AND month_number = %s)" for _ in year_month_pairs
    )
    params = [val for pair in year_month_pairs for val in pair]
    cur.execute(f"SELECT calendar_year, month_number FROM month WHERE {clauses}", params)
    return {(r[0], r[1]) for r in cur.fetchall()}


def existing_weeks(cur, iso_pairs: list[tuple[int, int]]) -> set[tuple[int, int]]:
    if not iso_pairs:
        return set()
    clauses = " OR ".join(
        "(calendar_year = %s AND week_number = %s)" for _ in iso_pairs
    )
    params = [val for pair in iso_pairs for val in pair]
    cur.execute(f"SELECT calendar_year, week_number FROM week WHERE {clauses}", params)
    return {(r[0], r[1]) for r in cur.fetchall()}


def existing_days(cur, dates: list[date]) -> set[date]:
    if not dates:
        return set()
    cur.execute(
        "SELECT date FROM day WHERE date = ANY(%s)",
        (dates,),
    )
    return {r[0] for r in cur.fetchall()}


# ---------------------------------------------------------------------------
# Lookups (after insert, resolve IDs)
# ---------------------------------------------------------------------------

def get_year_id(cur, calendar_year: int) -> int:
    cur.execute('SELECT year_id FROM "year" WHERE calendar_year = %s', (calendar_year,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"year row missing for {calendar_year} — should have been inserted already")
    return row[0]


def get_month_id(cur, calendar_year: int, month_number: int) -> int:
    cur.execute(
        "SELECT month_id FROM month WHERE calendar_year = %s AND month_number = %s",
        (calendar_year, month_number),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"month row missing for {calendar_year}-{month_number:02d}")
    return row[0]


def get_week_id(cur, iso_year: int, iso_week: int) -> int:
    cur.execute(
        "SELECT week_id FROM week WHERE calendar_year = %s AND week_number = %s",
        (iso_year, iso_week),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"week row missing for ISO {iso_year}-W{iso_week:02d}")
    return row[0]


# ---------------------------------------------------------------------------
# Inserts
# ---------------------------------------------------------------------------

def insert_year(cur, calendar_year: int):
    cur.execute(
        """
        INSERT INTO "year" (calendar_year, source_system)
        VALUES (%s, %s)
        ON CONFLICT (calendar_year) DO NOTHING
        """,
        (calendar_year, SOURCE_SYSTEM),
    )


def insert_month(cur, calendar_year: int, month_number: int, year_id: int):
    month_name = date(calendar_year, month_number, 1).strftime("%B")
    last_day = calendar.monthrange(calendar_year, month_number)[1]
    cur.execute(
        """
        INSERT INTO month
            (calendar_year, month_number, month_name,
             month_start_date, month_end_date, year_id, source_system)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (calendar_year, month_number) DO NOTHING
        """,
        (
            calendar_year,
            month_number,
            month_name,
            date(calendar_year, month_number, 1),
            date(calendar_year, month_number, last_day),
            year_id,
            SOURCE_SYSTEM,
        ),
    )


def insert_week(cur, iso_year: int, iso_week: int, year_id: int, start_month_id: int):
    monday, sunday = week_bounds(iso_year, iso_week)
    cur.execute(
        """
        INSERT INTO week
            (calendar_year, week_number, week_start_date, week_end_date,
             start_month_id, year_id, source_system)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (calendar_year, week_number) DO NOTHING
        """,
        (iso_year, iso_week, monday, sunday, start_month_id, year_id, SOURCE_SYSTEM),
    )


def insert_day(cur, d: date, week_id: int, month_id: int, year_id: int):
    cur.execute(
        """
        INSERT INTO day (date, week_id, month_id, year_id, source_system)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date) DO NOTHING
        """,
        (d, week_id, month_id, year_id, SOURCE_SYSTEM),
    )


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def run(year: int, confirm: bool):
    conn = get_connection()

    try:
        with conn.cursor() as cur:

            # ----------------------------------------------------------------
            # Determine what needs to be seeded
            # ----------------------------------------------------------------

            # All ISO weeks that touch this year may span into adjacent years
            iso_weeks = all_weeks_touching_year(year)
            iso_years_needed = sorted({iy for iy, _ in iso_weeks})

            # Months: always exactly the 12 months of the target year
            months_needed = all_months(year)

            # But we also need months for any ISO week starts in adjacent years
            # (e.g. week 1 of 2022 may start Dec 27 2021 → need Dec 2021 month)
            extra_months: set[tuple[int, int]] = set()
            for iso_year, iso_week in iso_weeks:
                monday, _ = week_bounds(iso_year, iso_week)
                extra_months.add((monday.year, monday.month))
            all_months_needed = sorted(set(months_needed) | extra_months)

            # All years touched (target year + any adjacent ISO years / month years)
            all_years_needed = sorted(
                {year}
                | {iy for iy in iso_years_needed}
                | {y for y, _ in all_months_needed}
            )

            # Days: only the target year
            days_needed = all_days(year)

            # ----------------------------------------------------------------
            # Check what already exists
            # ----------------------------------------------------------------
            exist_years  = existing_years(cur, all_years_needed)
            exist_months = existing_months(cur, all_months_needed)
            exist_weeks  = existing_weeks(cur, iso_weeks)
            exist_days   = existing_days(cur, days_needed)

            years_to_insert  = [y for y in all_years_needed  if y not in exist_years]
            months_to_insert = [m for m in all_months_needed if m not in exist_months]
            weeks_to_insert  = [w for w in iso_weeks         if w not in exist_weeks]
            days_to_insert   = [d for d in days_needed       if d not in exist_days]

            # ----------------------------------------------------------------
            # Pre-flight summary
            # ----------------------------------------------------------------
            print()
            print("=" * 60)
            print(f"  CALENDAR SEED — Year {year}")
            print("=" * 60)
            print(f"  Years   : {len(all_years_needed):>4} needed  |  {len(exist_years):>4} exist  |  {len(years_to_insert):>4} to insert")
            print(f"  Months  : {len(all_months_needed):>4} needed  |  {len(exist_months):>4} exist  |  {len(months_to_insert):>4} to insert")
            print(f"  Weeks   : {len(iso_weeks):>4} needed  |  {len(exist_weeks):>4} exist  |  {len(weeks_to_insert):>4} to insert")
            print(f"  Days    : {len(days_needed):>4} needed  |  {len(exist_days):>4} exist  |  {len(days_to_insert):>4} to insert")
            print()

            if years_to_insert:
                print(f"  Years to insert  : {years_to_insert}")
            if months_to_insert:
                print(f"  Months to insert : {[f'{y}-{m:02d}' for y, m in months_to_insert]}")
            if weeks_to_insert:
                print(f"  Weeks to insert  : {[f'ISO {iy}-W{iw:02d}' for iy, iw in weeks_to_insert]}")
            if not any([years_to_insert, months_to_insert, weeks_to_insert, days_to_insert]):
                print("  ✓ All calendar rows for this year already exist. Nothing to do.")
                print("=" * 60)
                return
            print()

            if not confirm:
                print("  DRY RUN — pass --confirm to write to the database.")
                print("=" * 60)
                return

            # ----------------------------------------------------------------
            # Writes
            # ----------------------------------------------------------------
            print("  Writing...")

            # 1. Years (must go first — everything else FKs to them)
            for y in years_to_insert:
                insert_year(cur, y)
                print(f"    [year]  inserted {y}")

            # 2. Months (FK → year)
            for cal_year, month_num in months_to_insert:
                year_id = get_year_id(cur, cal_year)
                insert_month(cur, cal_year, month_num, year_id)
                print(f"    [month] inserted {cal_year}-{month_num:02d}")

            # 3. Weeks (FK → year and start_month)
            for iso_year, iso_week in weeks_to_insert:
                monday, _ = week_bounds(iso_year, iso_week)
                year_id       = get_year_id(cur, iso_year)
                start_month_id = get_month_id(cur, monday.year, monday.month)
                insert_week(cur, iso_year, iso_week, year_id, start_month_id)
                print(f"    [week]  inserted ISO {iso_year}-W{iso_week:02d}  ({monday} → {monday + timedelta(days=6)})")

            # 4. Days (FK → year, month, week)
            day_count = 0
            for d in days_to_insert:
                iso = d.isocalendar()
                year_id  = get_year_id(cur, d.year)
                month_id = get_month_id(cur, d.year, d.month)
                week_id  = get_week_id(cur, iso[0], iso[1])
                insert_day(cur, d, week_id, month_id, year_id)
                day_count += 1

            print(f"    [day]   inserted {day_count} day rows")

            conn.commit()
            print()
            print("  ✓ Done. All rows committed.")
            print("=" * 60)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Seed year/month/week/day calendar rows for a given year.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview (no writes):
  python seed_calendar_year.py --year 2022

  # Write:
  python seed_calendar_year.py --year 2022 --confirm

  # Backfill multiple years one at a time:
  python seed_calendar_year.py --year 2019 --confirm
  python seed_calendar_year.py --year 2020 --confirm
  python seed_calendar_year.py --year 2021 --confirm
        """,
    )
    parser.add_argument(
        "--year", type=int, required=True, metavar="YYYY",
        help="Calendar year to seed (e.g. 2022).",
    )
    parser.add_argument(
        "--confirm", action="store_true",
        help="Actually write to the database. Omit for a dry-run preview.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.year, args.confirm)
