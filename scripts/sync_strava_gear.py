#!/usr/bin/env python3
"""
scripts/sync_strava_gear.py

Pulls gear (shoes) from the Strava API and upserts into the gear table.
- Enumerates shoe IDs from GET /athlete
- Fetches full detail from GET /gear/{id} for each shoe
- Upserts: match on strava_gear_id first, then fall back to normalized name
- Logs any gear matched by name for manual verification

Usage:
    python scripts/sync_strava_gear.py             # live run
    python scripts/sync_strava_gear.py --dry-run   # preview changes without writing

Requires: STRAVA_ACCESS_TOKEN in .env
"""

import os
import re
import argparse
import requests
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

STRAVA_TOKEN = os.getenv("STRAVA_ACCESS_TOKEN")
DB_HOST      = os.getenv("DB_HOST", "localhost")
DB_PORT      = os.getenv("DB_PORT", "5432")
DB_NAME      = os.getenv("POSTGRES_DB")
DB_USER      = os.getenv("POSTGRES_USER")
DB_PASSWORD  = os.getenv("POSTGRES_PASSWORD")

STRAVA_BASE  = "https://www.strava.com/api/v3"


def strava_headers():
    return {"Authorization": f"Bearer {STRAVA_TOKEN}"}


def normalize_name(name: str) -> str:
    """Lowercase, collapse whitespace, strip punctuation for fuzzy matching."""
    return re.sub(r"[^a-z0-9 ]", "", name.lower()).strip()


def get_athlete_shoe_ids() -> list[str]:
    """Returns list of Strava gear IDs (e.g. 'g12345678') for all shoes."""
    r = requests.get(f"{STRAVA_BASE}/athlete", headers=strava_headers())
    r.raise_for_status()
    athlete = r.json()
    shoes = athlete.get("shoes", [])
    print(f"Found {len(shoes)} shoe(s) on Strava athlete profile.")
    return [s["id"] for s in shoes]


def get_gear_detail(gear_id: str) -> dict:
    """Fetch full gear detail from GET /gear/{id}."""
    r = requests.get(f"{STRAVA_BASE}/gear/{gear_id}", headers=strava_headers())
    r.raise_for_status()
    return r.json()


def dry_run_check(conn, gear: dict) -> str:
    """
    Mirrors upsert_gear match logic without writing anything.
    Returns the outcome that would occur on a live run.
    """
    strava_id = gear["id"]
    name      = gear.get("name", "")
    norm_name = normalize_name(name)

    with conn.cursor() as cur:
        # Match 1: already has strava_gear_id
        cur.execute(
            "SELECT gear_id FROM gear WHERE strava_gear_id = %s AND is_deleted = FALSE",
            (strava_id,)
        )
        if cur.fetchone():
            return "updated_by_id"

        # Match 2: normalized name, no strava_gear_id yet
        cur.execute(
            "SELECT gear_id, name FROM gear WHERE is_deleted = FALSE AND strava_gear_id IS NULL"
        )
        for _, name_cand in cur.fetchall():
            if normalize_name(name_cand) == norm_name:
                return "updated_by_name"

        # No match — would insert
        return "inserted"


def upsert_gear(conn, gear: dict) -> str:
    """
    Upsert a single gear row. Match priority:
      1. strava_gear_id (already linked)
      2. normalized name (manual seed not yet linked)
      3. Insert new row if no match

    Returns: 'updated_by_id' | 'updated_by_name' | 'inserted'
    """
    strava_id   = gear["id"]
    name        = gear.get("name", "")
    brand       = gear.get("brand_name") or None
    model       = gear.get("model_name") or None
    retired     = gear.get("retired", False)
    description = gear.get("description") or None
    now         = datetime.now(timezone.utc)
    norm_name   = normalize_name(name)

    with conn.cursor() as cur:

        # Match 1: already has strava_gear_id
        cur.execute(
            "SELECT gear_id FROM gear WHERE strava_gear_id = %s AND is_deleted = FALSE",
            (strava_id,)
        )
        row = cur.fetchone()
        if row:
            cur.execute("""
                UPDATE gear SET
                    brand           = COALESCE(%s, brand),
                    model           = COALESCE(%s, model),
                    retirement_date = CASE WHEN %s THEN COALESCE(retirement_date, %s) ELSE retirement_date END,
                    notes           = COALESCE(NULLIF(%s, ''), notes),
                    updated_at      = %s,
                    last_synced_at  = %s
                WHERE gear_id = %s
            """, (brand, model, retired, now.date(), description, now, now, row[0]))
            conn.commit()
            return "updated_by_id"

        # Match 2: normalized name, no strava_gear_id yet
        cur.execute(
            "SELECT gear_id, name FROM gear WHERE is_deleted = FALSE AND strava_gear_id IS NULL"
        )
        candidates = cur.fetchall()
        matched_gear_id = None
        for gear_id_cand, name_cand in candidates:
            if normalize_name(name_cand) == norm_name:
                matched_gear_id = gear_id_cand
                break

        if matched_gear_id:
            cur.execute("""
                UPDATE gear SET
                    strava_gear_id  = %s,
                    brand           = COALESCE(%s, brand),
                    model           = COALESCE(%s, model),
                    retirement_date = CASE WHEN %s THEN COALESCE(retirement_date, %s) ELSE retirement_date END,
                    notes           = COALESCE(NULLIF(%s, ''), notes),
                    updated_at      = %s,
                    last_synced_at  = %s
                WHERE gear_id = %s
            """, (strava_id, brand, model, retired, now.date(), description, now, now, matched_gear_id))
            conn.commit()
            return "updated_by_name"

        # No match — insert new row
        cur.execute("""
            INSERT INTO gear (
                name, category, sport, brand, model,
                retirement_date, strava_gear_id,
                source_system, notes,
                created_at, updated_at, last_synced_at
            ) VALUES (
                %s, 'footwear', 'running', %s, %s,
                %s, %s,
                'strava', %s,
                %s, %s, %s
            )
        """, (
            name, brand, model,
            now.date() if retired else None,
            strava_id,
            description,
            now, now, now
        ))
        conn.commit()
        return "inserted"


def main():
    parser = argparse.ArgumentParser(description="Sync Strava gear into the gear table.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview planned changes without writing to the database."
    )
    args = parser.parse_args()

    if args.dry_run:
        print("--- DRY RUN — no changes will be written ---\n")

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )

    shoe_ids = get_athlete_shoe_ids()
    results  = {"updated_by_id": [], "updated_by_name": [], "inserted": []}

    for sid in shoe_ids:
        gear = get_gear_detail(sid)
        name = gear.get("name", sid)

        if args.dry_run:
            outcome = dry_run_check(conn, gear)
            print(f"  [DRY RUN | {outcome}] {name} ({sid})")
        else:
            outcome = upsert_gear(conn, gear)
            print(f"  [{outcome}] {name} ({sid})")

        results[outcome].append(name)

    conn.close()

    print("\n--- Sync Summary ---")
    if args.dry_run:
        print("  (No changes written — dry run only)\n")
    print(f"  Updated by strava_gear_id : {len(results['updated_by_id'])}")
    print(f"  Updated by name match     : {len(results['updated_by_name'])}")
    print(f"  Inserted (new)            : {len(results['inserted'])}")

    if results["updated_by_name"]:
        print("\n  ⚠ Name-matched gear — verify before live run:" if args.dry_run else "\n  ⚠ Name-matched gear — verify these:")
        for n in results["updated_by_name"]:
            print(f"    - {n}")


if __name__ == "__main__":
    main()
