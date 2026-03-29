#!/usr/bin/env python3
"""
manual_exercise_intake.py
--------------------------
Manually log a workout to the personal DB without a Garmin FIT file.
Use this for activities tracked by feel, on paper, or with a non-Garmin device.

Workflow:
  1. Interactive CLI prompts collect core exercise fields
  2. Guided intake interview collects workout_analysis fields
  3. Write summary displayed for confirmation
  4. exercise + workout_analysis written in a single transaction

Usage:
  python manual_exercise_intake.py
  python manual_exercise_intake.py --dry-run

Dependencies:
  pip install psycopg2-binary python-dotenv

Environment variables (in .env):
  POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, PG_HOST, PG_PORT

Author: John Radcliffe
"""

import os
import sys
import argparse
from datetime import date, datetime
from dotenv import load_dotenv
import psycopg2

load_dotenv()


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
# INPUT HELPERS
# ---------------------------------------------------------------------------

def ask(prompt, required=True, cast=None, choices=None, allow_empty=False):
    """
    Prompt the user for input. Handles validation, casting, and choice lists.
    Returns None if the user leaves blank and allow_empty=True.
    """
    while True:
        if choices:
            options = "  " + " / ".join(f"[{i+1}] {c}" for i, c in enumerate(choices))
            user_input = input(f"\n{prompt}\n{options}\n  → ").strip()
            if allow_empty and user_input == "":
                return None
            if user_input.isdigit():
                idx = int(user_input) - 1
                if 0 <= idx < len(choices):
                    return choices[idx]
            matches = [c for c in choices if c.lower().startswith(user_input.lower())]
            if len(matches) == 1:
                return matches[0]
            print(f"  ! Please enter a number (1–{len(choices)}) or enough letters to match.")
        else:
            user_input = input(f"\n{prompt}\n  → ").strip()
            if not user_input:
                if allow_empty or not required:
                    return None
                print("  ! This field is required.")
                continue
            if cast:
                try:
                    return cast(user_input)
                except (ValueError, TypeError):
                    print(f"  ! Invalid input — expected {cast.__name__}.")
            else:
                return user_input


def ask_date(prompt):
    """Prompt for a date in YYYY-MM-DD format. Defaults to today on blank entry."""
    while True:
        raw = input(f"\n{prompt} [default: today {date.today()}]\n  → ").strip()
        if not raw:
            return date.today()
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            print("  ! Enter date as YYYY-MM-DD.")


def ask_float(prompt, required=True, allow_empty=False):
    while True:
        raw = input(f"\n{prompt}\n  → ").strip()
        if not raw:
            if allow_empty or not required:
                return None
            print("  ! This field is required.")
            continue
        try:
            return float(raw)
        except ValueError:
            print("  ! Enter a number (e.g. 6.2).")


def ask_int(prompt, required=True, allow_empty=False):
    while True:
        raw = input(f"\n{prompt}\n  → ").strip()
        if not raw:
            if allow_empty or not required:
                return None
            print("  ! This field is required.")
            continue
        try:
            return int(raw)
        except ValueError:
            print("  ! Enter a whole number.")


# ---------------------------------------------------------------------------
# EXERCISE INTAKE
# ---------------------------------------------------------------------------

def collect_exercise_fields():
    """Prompt for core exercise fields. Returns a dict ready for DB insertion."""
    print("\n" + "=" * 56)
    print("  MANUAL EXERCISE INTAKE")
    print("  Fields marked (optional) can be left blank.")
    print("=" * 56)

    print("\n── Activity Details ──────────────────────────────────")

    activity_date = ask_date("Activity date (YYYY-MM-DD):")

    type_of_activity = ask(
        "Activity type:",
        choices=["running", "cycling", "walking", "hiking", "swimming",
                 "climbing", "strength", "yoga", "skiing", "other"],
    )

    # Subtype choices vary by activity type
    if type_of_activity == "climbing":
        subtype_choices = [
            "Indoor - ropes",
            "Indoor - bouldering",
            "Outside - ropes",
            "Outside - bouldering",
        ]
    else:
        subtype_choices = [
            "Easy", "Recovery", "Long Run", "Tempo", "Threshold",
            "Interval", "Race", "Trail", "Strength", "Other",
        ]

    subtype_of_activity = ask(
        "Subtype / workout category:",
        choices=subtype_choices,
    )

    print("\n── Core Metrics ──────────────────────────────────────")

    distance_miles = ask_float("Distance (miles) (optional):", required=False, allow_empty=True)
    duration_minutes = ask_float("Duration — active time (minutes):", required=True)
    elevation_gain_feet = ask_float("Elevation gain (feet) (optional):", required=False, allow_empty=True)
    average_heart_rate = ask_float("Average heart rate (bpm) (optional):", required=False, allow_empty=True)
    max_heart_rate = ask_int("Max heart rate (bpm) (optional):", required=False, allow_empty=True)
    calories = ask_int("Calories burned (optional):", required=False, allow_empty=True)

    return {
        "activity_date": activity_date,
        "type_of_activity": type_of_activity,
        "subtype_of_activity": subtype_of_activity,
        "distance_miles": distance_miles,
        "duration_minutes": duration_minutes,
        "elevation_gain_feet": elevation_gain_feet,
        "average_heart_rate": average_heart_rate,
        "max_heart_rate": max_heart_rate,
        "calories": calories,
        # Fields not collected manually — left NULL
        "total_elapsed_time": None,
        "tss_score": None,
        "average_cadence": None,
        "average_power": None,
        "max_power": None,
        "num_laps": None,
        "avg_vertical_ratio": None,
        "avg_stance_time_balance": None,
        "avg_step_length_mm": None,
        "source_system": "manual",
        "source_object": "manual_exercise_intake.py",
    }


# ---------------------------------------------------------------------------
# WORKOUT ANALYSIS INTAKE
# ---------------------------------------------------------------------------

def collect_analysis_fields():
    """Prompt for workout_analysis fields. Returns a dict ready for DB insertion."""
    print("\n" + "=" * 56)
    print("  WORKOUT ANALYSIS")
    print("  Answer each question. Press Enter to skip optional fields.")
    print("=" * 56)

    rpe = ask_int("RPE — Rate of Perceived Exertion (1–10):", required=True)

    rating = ask_float("Overall workout rating (1.0–5.0):", required=True)

    session_quality = ask(
        "Session quality:",
        choices=["Excellent", "Good", "Average", "Poor", "Bad"],
    )

    felt_strong = ask("Did you feel strong?", choices=["y", "n"]) == "y"
    felt_fatigued = ask("Were you carrying fatigue into this?", choices=["y", "n"]) == "y"

    mental_state = ask(
        "Mental state (e.g. Focused, Distracted, Motivated, Flat, Anxious):",
    )

    training_purpose = ask(
        "Training purpose:",
        choices=[
            "Aerobic Base",
            "Threshold Development",
            "VO2 Max",
            "Speed/Neuromuscular",
            "Long Run",
            "Recovery",
            "Daily Movement",
            "Race Prep",
            "Fitness Check",
            "Shakeout",
            "Strength",
            "Other",
        ],
    )

    analysis_notes = ask(
        "Specific notes — legs, conditions, GI, soreness, etc. (optional):",
        required=False, allow_empty=True,
    )

    analysis_summary = ask("One-sentence summary of this workout:")

    return {
        "analysis_date": date.today(),
        "perceived_level_of_effort": rpe,
        "rating": round(float(rating), 1),
        "session_quality": session_quality,
        "felt_strong": felt_strong,
        "felt_fatigued": felt_fatigued,
        "mental_state": mental_state,
        "training_purpose": training_purpose,
        "analysis_notes": analysis_notes,
        "analysis_summary": analysis_summary,
        "source_system": "manual_intake",
        "source_object": "manual_exercise_intake.py",
    }


# ---------------------------------------------------------------------------
# WRITE SUMMARY
# ---------------------------------------------------------------------------

def format_pace(distance_miles, duration_minutes):
    if not distance_miles or not duration_minutes or distance_miles == 0:
        return "N/A"
    pace = duration_minutes / distance_miles
    mins = int(pace)
    secs = int((pace - mins) * 60)
    return f"{mins}:{secs:02d}/mi"


def print_write_summary(exercise_fields, analysis_data):
    print("\n" + "=" * 56)
    print("  WRITE SUMMARY — Review before committing")
    print("=" * 56)
    ef = exercise_fields
    print(f"\n  exercise table:")
    print(f"    activity_date    : {ef['activity_date']}")
    print(f"    type_of_activity : {ef['type_of_activity']}")
    print(f"    subtype          : {ef['subtype_of_activity']}")
    print(f"    distance_miles   : {ef['distance_miles']}")
    print(f"    duration_minutes : {ef['duration_minutes']} min")
    print(f"    pace             : {format_pace(ef['distance_miles'], ef['duration_minutes'])}")
    print(f"    elevation_gain   : {ef['elevation_gain_feet']} ft")
    print(f"    avg_hr / max_hr  : {ef['average_heart_rate']} / {ef['max_heart_rate']}")
    print(f"    calories         : {ef['calories']}")
    print(f"    source_system    : {ef['source_system']}")
    print(f"\n  workout_analysis table:")
    for k, v in analysis_data.items():
        if k not in ("source_system", "source_object"):
            print(f"    {k:<28}: {v}")
    print()


# ---------------------------------------------------------------------------
# DB HELPERS (mirrors ingest_fit_workout.py)
# ---------------------------------------------------------------------------

def resolve_day_id(conn, activity_date):
    with conn.cursor() as cur:
        cur.execute("SELECT day_id FROM day WHERE date = %s", (activity_date,))
        row = cur.fetchone()
    return row[0] if row else None


def resolve_week_id(conn, activity_date):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT week_id FROM week WHERE week_start_date <= %s AND week_end_date >= %s",
            (activity_date, activity_date),
        )
        row = cur.fetchone()
    return row[0] if row else None


def insert_exercise(cur, fields, day_id, week_id):
    sql = """
        INSERT INTO exercise (
            day_id, week_id, activity_date,
            type_of_activity, subtype_of_activity,
            distance_miles, duration_minutes, total_elapsed_time,
            elevation_gain_feet, average_heart_rate, max_heart_rate,
            average_cadence, average_power, max_power,
            calories, tss_score, num_laps,
            avg_vertical_ratio, avg_stance_time_balance, avg_step_length_mm,
            source_system, source_object
        ) VALUES (
            %(day_id)s, %(week_id)s, %(activity_date)s,
            %(type_of_activity)s, %(subtype_of_activity)s,
            %(distance_miles)s, %(duration_minutes)s, %(total_elapsed_time)s,
            %(elevation_gain_feet)s, %(average_heart_rate)s, %(max_heart_rate)s,
            %(average_cadence)s, %(average_power)s, %(max_power)s,
            %(calories)s, %(tss_score)s, %(num_laps)s,
            %(avg_vertical_ratio)s, %(avg_stance_time_balance)s, %(avg_step_length_mm)s,
            %(source_system)s, %(source_object)s
        )
        RETURNING exercise_id;
    """
    cur.execute(sql, {**fields, "day_id": day_id, "week_id": week_id})
    return cur.fetchone()[0]


def insert_workout_analysis(cur, exercise_id, analysis_data):
    sql = """
        INSERT INTO workout_analysis (
            exercise_id, analysis_date,
            perceived_level_of_effort, rating,
            session_quality, felt_strong, felt_fatigued,
            mental_state, training_purpose,
            analysis_notes, analysis_summary,
            source_system, source_object
        ) VALUES (
            %(exercise_id)s, %(analysis_date)s,
            %(perceived_level_of_effort)s, %(rating)s,
            %(session_quality)s, %(felt_strong)s, %(felt_fatigued)s,
            %(mental_state)s, %(training_purpose)s,
            %(analysis_notes)s, %(analysis_summary)s,
            %(source_system)s, %(source_object)s
        )
        ON CONFLICT (exercise_id) DO UPDATE SET
            analysis_date              = EXCLUDED.analysis_date,
            perceived_level_of_effort  = EXCLUDED.perceived_level_of_effort,
            rating                     = EXCLUDED.rating,
            session_quality            = EXCLUDED.session_quality,
            felt_strong                = EXCLUDED.felt_strong,
            felt_fatigued              = EXCLUDED.felt_fatigued,
            mental_state               = EXCLUDED.mental_state,
            training_purpose           = EXCLUDED.training_purpose,
            analysis_notes             = EXCLUDED.analysis_notes,
            analysis_summary           = EXCLUDED.analysis_summary,
            updated_at                 = now()
        RETURNING workout_analysis_id;
    """
    cur.execute(sql, {"exercise_id": exercise_id, **analysis_data})
    return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Manually log a workout to the personal DB."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run the full intake but skip all database writes."
    )
    args = parser.parse_args()

    # ── Step 1: Collect exercise fields ─────────────────────
    exercise_fields = collect_exercise_fields()

    # ── Step 2: Collect workout_analysis fields ──────────────
    analysis_data = collect_analysis_fields()

    # ── Step 3: Write summary + final confirmation ───────────
    print_write_summary(exercise_fields, analysis_data)

    confirm = input("  Write to database? (y/n): ").strip().lower()
    if confirm != "y":
        print("  Aborted — nothing written.")
        sys.exit(0)

    # ── Step 4: Dry run exit ─────────────────────────────────
    if args.dry_run:
        print("\n" + "=" * 56)
        print("  DRY RUN — no data written to database.")
        print("=" * 56 + "\n")
        return

    # ── Step 5: Write to Postgres ────────────────────────────
    print("\n  Connecting to database ...")
    conn = get_connection()

    try:
        activity_date = exercise_fields["activity_date"]
        day_id = resolve_day_id(conn, activity_date)
        week_id = resolve_week_id(conn, activity_date)

        if not day_id:
            print(f"  Warning: no day record found for {activity_date}. "
                  "exercise.day_id will be NULL.")
        if not week_id:
            print(f"  Warning: no week record found covering {activity_date}. "
                  "exercise.week_id will be NULL.")

        with conn:
            with conn.cursor() as cur:
                exercise_id = insert_exercise(cur, exercise_fields, day_id, week_id)
                print(f"  ✓ exercise inserted — exercise_id = {exercise_id}")

                wa_id = insert_workout_analysis(cur, exercise_id, analysis_data)
                print(f"  ✓ workout_analysis inserted — workout_analysis_id = {wa_id}")

        print("\n" + "=" * 56)
        print(f"  DONE — Exercise #{exercise_id} logged successfully.")
        print(f"  {exercise_fields['distance_miles']} mi"
              f" | {exercise_fields['subtype_of_activity']}"
              f" | RPE {analysis_data['perceived_level_of_effort']}"
              f" | {analysis_data['session_quality']}")
        print("=" * 56 + "\n")

    except Exception as e:
        conn.rollback()
        print(f"\n  Error during database write: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()