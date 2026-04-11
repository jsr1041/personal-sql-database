#!/usr/bin/env python3
"""
ingest_fit_workout.py
---------------------
FIT file ingestion script with guided workout analysis intake.

Workflow:
  1. Parse a Garmin .FIT file (exercise + exercise_trackpoint)
  2. Display a data preview for confirmation
  3. Run an interactive intake interview to populate workout_analysis
  4. Write exercise, exercise_trackpoint, and workout_analysis to Postgres
     in a single transaction

Usage:
  python ingest_fit_workout.py path/to/activity.fit

Dependencies:
  pip install fitparse psycopg2-binary python-dotenv

Environment variables (in .env):
  POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, PG_HOST, PG_PORT

Author: John Radcliffe
"""

import sys
import os
import math
from datetime import date, datetime, timezone
from decimal import Decimal

import fitparse
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# ACTIVITY TYPE / SUBTYPE HIERARCHY
# ---------------------------------------------------------------------------

SUBTYPES = {
    "Run":    ["Trail Run", "Road Run", "Treadmill"],
    "Walk":   [],
    "Hike":   ["Backpacking", "Hike", "Mountaineering"],
    "Ski":    ["Resort Skiing", "Backcountry Skiing", "Resort Uphilling", "Nordic Skiing"],
    "Climb":  ["Indoors - Bouldering", "Indoors - Ropes",
               "Outdoors - Cragging", "Outdoors - Multipitch", "Outdoors - Bouldering"],
    "Bike":   ["Road Ride", "Mountain Bike Ride"],
    "Swim":   ["Pool Swim", "Open Water Swim"],
    "Gym":    ["Exercise Equipment", "Free Weights"],
    "Rowing": [],
}
ACTIVITY_TYPES = list(SUBTYPES.keys())

# Map raw FIT sport strings → canonical type_of_activity
FIT_TYPE_MAP = {
    "running":           "Run",
    "cycling":           "Bike",
    "hiking":            "Hike",
    "walking":           "Walk",
    "swimming":          "Swim",
    "rock_climbing":     "Climb",
    "fitness_equipment": "Gym",
    "training":          "Gym",
    "skiing":            "Ski",
    "alpine_skiing":     "Ski",
    "cross_country_skiing": "Ski",
    "rowing":            "Rowing",
}

# Map raw FIT sub_sport strings → canonical subtype_of_activity
FIT_SUBTYPE_MAP = {
    "trail":           "Trail Run",
    "road":            "Road Run",
    "treadmill":       "Treadmill",
    "mountain":        "Mountain Bike Ride",
    "road_cycling":    "Road Ride",
    "alpine_skiing":   "Resort Skiing",
    "backcountry":     "Backcountry Skiing",
    "nordic_skiing":   "Nordic Skiing",
    "lap_swimming":    "Pool Swim",
    "open_water":      "Open Water Swim",
    "rock_climbing":   "Outdoors - Cragging",
    "bouldering":      "Outdoors - Bouldering",
    "indoor_climbing": "Indoors - Ropes",
    "exercise_equipment": "Exercise Equipment",
    "free_weights":    "Free Weights",
}


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
# FIT PARSING
# ---------------------------------------------------------------------------

# Scale corrections for fields that fitparse returns without unit conversion
SCALE_CORRECTIONS = {
    "speed":          0.001,   # mm/s → m/s  (FIT semicircles already converted by fitparse for lat/lon)
    "enhanced_speed": 0.001,
    "altitude":       (lambda raw: raw * 0.2 - 500 if raw is not None else None),  # FIT altitude formula
    "enhanced_altitude": (lambda raw: raw * 0.2 - 500 if raw is not None else None),
    "vertical_ratio": 0.01,    # 1/100 % → %
}


def _scale(field_name, value):
    """Apply scale correction if defined for this field."""
    if value is None:
        return None
    corr = SCALE_CORRECTIONS.get(field_name)
    if corr is None:
        return value
    if callable(corr):
        return corr(value)
    return value * corr


def semicircles_to_degrees(val):
    if val is None:
        return None
    return val * (180.0 / 2**31)


# ---------------------------------------------------------------------------
# FINGERPRINT  (keep in sync with backfill_fingerprints.py and batch_ingest_exercise.py)
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
        d    = str(activity_date)
        t    = str(type_of_activity).strip()
        dist = str(round(float(distance_miles), 1))
        dur  = str(round(float(duration_minutes), 0))
        if dur.endswith(".0"):
            dur = dur[:-2]
        return f"{d}|{t}|{dist}|{dur}"
    except (TypeError, ValueError):
        return None


def parse_fit(fit_path):
    """
    Parse a FIT file and return:
      session_data  — dict of session-level fields (maps to exercise)
      trackpoints   — list of dicts (maps to exercise_trackpoint)
    """
    ff = fitparse.FitFile(fit_path, data_processor=fitparse.StandardUnitsDataProcessor())

    session_data = {}
    trackpoints = []

    for message in ff.get_messages():
        name = message.name

        if name == "session":
            for field in message.fields:
                session_data[field.name] = field.value

        elif name == "record":
            row = {}
            for field in message.fields:
                row[field.name] = field.value
            trackpoints.append(row)

    return session_data, trackpoints


def extract_exercise_fields(session_data, fit_filename=None):
    """Map FIT session fields to exercise table columns."""

    def get(*keys):
        for k in keys:
            v = session_data.get(k)
            if v is not None:
                return v
        return None

    # Source file metadata
    garmin_fit_file  = fit_filename or None
    source_record_id = os.path.splitext(fit_filename)[0] if fit_filename else None

    # Sport / activity type — map raw FIT values to canonical hierarchy
    sport_raw     = str(get("sport") or "").replace("Sport.", "").lower()
    sub_sport_raw = str(get("sub_sport") or "").replace("SubSport.", "").lower()

    # Distances and durations
    total_distance_m = get("total_distance")          # meters (StandardUnits)
    distance_miles = round(total_distance_m / 1609.344, 2) if total_distance_m else None

    timer_time_s   = get("total_timer_time")           # active time seconds
    elapsed_time_s = get("total_elapsed_time")         # wall-clock seconds
    duration_minutes    = round(timer_time_s / 60, 2) if timer_time_s else None
    total_elapsed_time  = round(elapsed_time_s / 60, 2) if elapsed_time_s else None

    # Elevation
    total_ascent_m = get("total_ascent")              # meters (StandardUnits)
    elevation_gain_feet = round(total_ascent_m * 3.28084, 1) if total_ascent_m else None

    # Heart rate
    avg_hr = get("avg_heart_rate")
    max_hr = get("max_heart_rate")

    # Cadence (single-leg rpm from FIT; double for spm)
    avg_cadence = get("avg_running_cadence", "avg_cadence")

    # Power
    avg_power = get("avg_power")
    max_power = get("max_power")

    # Calories
    calories = get("total_calories")

    # Laps
    num_laps = get("num_laps")

    # Running dynamics (session-level averages)
    avg_vertical_ratio      = _scale("vertical_ratio", get("avg_vertical_ratio"))
    avg_stance_time_balance = get("avg_stance_time_balance")  # already % from StandardUnits
    avg_step_length_mm      = get("avg_step_length")          # mm from FIT

    # Timestamps
    start_time = get("start_time")
    if isinstance(start_time, datetime):
        activity_date = start_time.date()
    else:
        activity_date = None

    # TSS (if present — Garmin sometimes includes training_stress_score)
    tss = get("training_stress_score")

    canonical_type    = FIT_TYPE_MAP.get(sport_raw)
    canonical_subtype = FIT_SUBTYPE_MAP.get(sub_sport_raw) if sub_sport_raw else None

    return {
        "type_of_activity":    canonical_type,
        "subtype_of_activity": canonical_subtype,
        "_fit_sport_raw":      sport_raw,
        "_fit_sub_sport_raw":  sub_sport_raw,
        "activity_date":       activity_date,
        "distance_miles":      distance_miles,
        "duration_minutes":    duration_minutes,
        "total_elapsed_time":  total_elapsed_time,
        "elevation_gain_feet": elevation_gain_feet,
        "average_heart_rate":  avg_hr,
        "max_heart_rate":      max_hr,
        "average_cadence":     avg_cadence,
        "average_power":       avg_power,
        "max_power":           max_power,
        "calories":            calories,
        "num_laps":            num_laps,
        "avg_vertical_ratio":        avg_vertical_ratio,
        "avg_stance_time_balance":   avg_stance_time_balance,
        "avg_step_length_mm":        avg_step_length_mm,
        "tss_score":                 tss,
        "garmin_fit_file":           garmin_fit_file,
        "source_record_id":          source_record_id,
        # activity_fingerprint is NOT set here — it is computed in main() after
        # the intake interview confirms the final type_override, so the fingerprint
        # always matches the persisted type_of_activity.
        "activity_fingerprint":      None,
    }


def extract_trackpoints(trackpoints):
    """Map FIT record messages to exercise_trackpoint rows."""
    rows = []
    for tp in trackpoints:
        def g(k): return tp.get(k)

        lat_raw = g("position_lat")
        lon_raw = g("position_long")

        # fitparse StandardUnitsDataProcessor converts lat/lon from semicircles to degrees
        lat = lat_raw
        lon = lon_raw

        # Altitude: StandardUnits should give meters; apply correction if raw int slips through
        alt = g("enhanced_altitude") or g("altitude")
        if isinstance(alt, int):
            alt = _scale("altitude", alt)

        # Speed: StandardUnits gives m/s; apply correction if raw int slips through
        speed = g("enhanced_speed") or g("speed")
        if isinstance(speed, int):
            speed = _scale("speed", speed)

        # Vertical ratio: may be raw int
        vr = g("vertical_ratio")
        if isinstance(vr, int):
            vr = _scale("vertical_ratio", vr)

        recorded_at = g("timestamp")

        rows.append({
            "recorded_at":               recorded_at,
            "position_lat":              round(lat, 6) if lat is not None else None,
            "position_long":             round(lon, 6) if lon is not None else None,
            "altitude_meters":           round(alt, 1) if alt is not None else None,
            "distance_meters":           round(g("distance"), 1) if g("distance") is not None else None,
            "speed_ms":                  round(speed, 3) if speed is not None else None,
            "heart_rate":                g("heart_rate"),
            "cadence":                   g("cadence"),
            "power":                     g("power"),
            "temperature_c":             g("temperature"),
            "gps_accuracy_m":            g("gps_accuracy"),
            "vertical_oscillation_mm":   g("vertical_oscillation"),
            "stance_time_ms":            g("stance_time"),
            "stance_time_pct":           g("stance_time_percent"),
            "vertical_ratio_pct":        round(vr, 2) if vr is not None else None,
            "ground_contact_balance_pct": g("left_right_balance"),
            "performance_condition":     g("performance_condition"),
            "absolute_pressure_pa":      g("absolute_pressure"),
            "spo2_pct":                  g("pulse_oximetry_data"),
        })
    return rows


# ---------------------------------------------------------------------------
# DATA PREVIEW
# ---------------------------------------------------------------------------

def format_pace(distance_miles, duration_minutes):
    """Return pace as MM:SS/mile string."""
    if not distance_miles or not duration_minutes or distance_miles == 0:
        return "N/A"
    pace_min_per_mile = duration_minutes / distance_miles
    mins = int(pace_min_per_mile)
    secs = int((pace_min_per_mile - mins) * 60)
    return f"{mins}:{secs:02d}/mi"


def print_preview(exercise_fields, trackpoint_count):
    ef = exercise_fields
    print("\n" + "=" * 56)
    print("  FIT FILE PARSED — ACTIVITY PREVIEW")
    print("=" * 56)
    print(f"  Date            : {ef.get('activity_date', 'Unknown')}")
    print(f"  Activity        : {(ef.get('type_of_activity') or '').title()}"
          f" / {(ef.get('subtype_of_activity') or '').replace('_', ' ').title()}")
    print(f"  Distance        : {ef.get('distance_miles', 'N/A')} mi")
    print(f"  Duration        : {ef.get('duration_minutes', 'N/A')} min (active)")
    print(f"  Elapsed         : {ef.get('total_elapsed_time', 'N/A')} min (wall clock)")
    print(f"  Avg Pace        : {format_pace(ef.get('distance_miles'), ef.get('duration_minutes'))}")
    print(f"  Elevation Gain  : {ef.get('elevation_gain_feet', 'N/A')} ft")
    print(f"  Avg HR          : {ef.get('average_heart_rate', 'N/A')} bpm")
    print(f"  Max HR          : {ef.get('max_heart_rate', 'N/A')} bpm")
    print(f"  Avg Power       : {ef.get('average_power', 'N/A')} W")
    print(f"  Max Power       : {ef.get('max_power', 'N/A')} W")
    print(f"  Avg Cadence     : {ef.get('average_cadence', 'N/A')} rpm (single-leg)")
    print(f"  Calories        : {ef.get('calories', 'N/A')}")
    print(f"  Laps            : {ef.get('num_laps', 'N/A')}")
    print(f"  Avg Vert. Ratio : {ef.get('avg_vertical_ratio', 'N/A')} %")
    print(f"  Trackpoints     : {trackpoint_count:,}")
    print("=" * 56)


# ---------------------------------------------------------------------------
# INTAKE INTERVIEW
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
            # Accept either number or text match
            if user_input.isdigit():
                idx = int(user_input) - 1
                if 0 <= idx < len(choices):
                    return choices[idx]
            # Case-insensitive prefix match
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


def run_intake_interview(fit_type=None, fit_subtype=None):
    """
    Guided intake interview that collects all workout_analysis fields
    plus the exercise fields the FIT file cannot provide.
    Returns (exercise_extras, analysis_data) dicts.
    """
    print("\n" + "=" * 56)
    print("  WORKOUT ANALYSIS INTAKE")
    print("  Answer each question. Press Enter to skip optional fields.")
    print("=" * 56)

    # --- exercise extras ---
    print("\n── Activity Context ──────────────────────────────────")

    # Type of activity
    if fit_type:
        print(f"  (FIT parsed: {fit_type or '?'} / {fit_subtype or 'none'})")
    type_of_activity = ask(
        "Type of activity:",
        choices=ACTIVITY_TYPES,
    )

    # Subtype — only if subtypes are defined for the chosen type
    subtype_options = SUBTYPES.get(type_of_activity, [])
    if subtype_options:
        subtype_of_activity = ask(
            "Subtype:",
            choices=subtype_options,
            allow_empty=True,
        )
    else:
        subtype_of_activity = None

    planned = ask(
        "Did this match a planned workout? (y/n)",
        choices=["y", "n"],
    )

    # --- workout_analysis fields ---
    print("\n── Subjective Analysis ───────────────────────────────")

    rpe = ask(
        "RPE — Rate of Perceived Exertion (1–10):",
        cast=int,
    )

    rating = ask(
        "Overall workout rating (1.0–5.0):",
        cast=float,
    )

    session_quality = ask(
        "Session quality:",
        choices=["Excellent", "Good", "Average", "Poor", "Bad"],
    )

    felt_strong_raw = ask(
        "Did you feel strong?",
        choices=["y", "n"],
    )
    felt_strong = felt_strong_raw == "y"

    felt_fatigued_raw = ask(
        "Were you carrying fatigue into this?",
        choices=["y", "n"],
    )
    felt_fatigued = felt_fatigued_raw == "y"

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

    analysis_summary = ask(
        "One-sentence summary of this workout:",
    )

    gear_name = ask(
        "Primary gear used (optional, enter exact gear name from gear table):",
        required=False, allow_empty=True,
    )

    exercise_extras = {
        "type_override":    type_of_activity,
        "subtype_override": subtype_of_activity,
        "planned":          planned == "y",
        "gear_name":        gear_name,
    }

    analysis_data = {
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
        "source_object": "ingest_fit_workout.py",
    }

    return exercise_extras, analysis_data


# ---------------------------------------------------------------------------
# WRITE SUMMARY + CONFIRMATION
# ---------------------------------------------------------------------------

def print_write_summary(exercise_fields, exercise_extras, analysis_data, trackpoint_count):
    print("\n" + "=" * 56)
    print("  WRITE SUMMARY — Review before committing")
    print("=" * 56)
    print("\n  exercise table:")
    print(f"    activity_date    : {exercise_fields['activity_date']}")
    print(f"    type_of_activity : {exercise_extras['type_override']}")
    print(f"    subtype          : {exercise_extras['subtype_override'] or 'None'}")
    print(f"    garmin_fit_file  : {exercise_fields.get('garmin_fit_file')}")
    print(f"    source_record_id : {exercise_fields.get('source_record_id')}")
    print(f"    fingerprint      : {exercise_fields.get('activity_fingerprint') or 'none'}")
    print(f"    primary_gear     : {exercise_extras.get('gear_name') or 'None'}")
    print(f"    distance_miles   : {exercise_fields['distance_miles']}")
    print(f"    duration_minutes : {exercise_fields['duration_minutes']}")
    print(f"    avg_hr / max_hr  : {exercise_fields['average_heart_rate']} / {exercise_fields['max_heart_rate']}")
    print(f"    avg_power        : {exercise_fields['average_power']}")
    print(f"\n  exercise_trackpoint: {trackpoint_count:,} rows")
    print(f"\n  workout_analysis table:")
    for k, v in analysis_data.items():
        if k not in ("source_system", "source_object"):
            print(f"    {k:<28}: {v}")
    print()


# ---------------------------------------------------------------------------
# DATABASE WRITES
# ---------------------------------------------------------------------------

def find_existing_exercise(conn, source_record_id, fingerprint):
    """
    Two-layer idempotency check. Returns (exercise_id, match_type) or (None, None).

    Layer 1 — exact source key: (source_system='garmin_fit', source_record_id)
    Layer 2 — fingerprint:      cross-source match on date+type+distance+duration
    """
    with conn.cursor() as cur:

        # Layer 1 — exact source key
        if source_record_id:
            cur.execute(
                """
                SELECT exercise_id FROM exercise
                WHERE source_system    = 'garmin_fit'
                  AND source_record_id = %s
                  AND is_deleted = FALSE
                LIMIT 1
                """,
                (source_record_id,),
            )
            row = cur.fetchone()
            if row:
                return row[0], "source_key"

        # Layer 2 — fingerprint
        if fingerprint:
            cur.execute(
                """
                SELECT exercise_id FROM exercise
                WHERE activity_fingerprint = %s
                  AND is_deleted = FALSE
                LIMIT 1
                """,
                (fingerprint,),
            )
            row = cur.fetchone()
            if row:
                return row[0], "fingerprint"

    return None, None


def resolve_day_id(conn, activity_date):
    """Look up day_id for a given date. Returns None if not found."""
    with conn.cursor() as cur:
        cur.execute("SELECT day_id FROM day WHERE date = %s", (activity_date,))
        row = cur.fetchone()
    return row[0] if row else None


def resolve_week_id(conn, activity_date):
    """Look up week_id for a given date."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT week_id FROM week WHERE week_start_date <= %s AND week_end_date >= %s",
            (activity_date, activity_date),
        )
        row = cur.fetchone()
    return row[0] if row else None


def insert_exercise(cur, exercise_fields, exercise_extras, day_id, week_id):
    """Insert into exercise and return the new exercise_id."""
    sql = """
        INSERT INTO exercise (
            day_id, week_id, activity_date,
            type_of_activity, subtype_of_activity,
            distance_miles, duration_minutes, total_elapsed_time,
            elevation_gain_feet, average_heart_rate, max_heart_rate,
            average_cadence, average_power, max_power,
            calories, tss_score, num_laps,
            avg_vertical_ratio, avg_stance_time_balance, avg_step_length_mm,
            garmin_fit_file,
            source_system, source_object, source_record_id,
            activity_fingerprint
        ) VALUES (
            %(day_id)s, %(week_id)s, %(activity_date)s,
            %(type_of_activity)s, %(subtype_of_activity)s,
            %(distance_miles)s, %(duration_minutes)s, %(total_elapsed_time)s,
            %(elevation_gain_feet)s, %(average_heart_rate)s, %(max_heart_rate)s,
            %(average_cadence)s, %(average_power)s, %(max_power)s,
            %(calories)s, %(tss_score)s, %(num_laps)s,
            %(avg_vertical_ratio)s, %(avg_stance_time_balance)s, %(avg_step_length_mm)s,
            %(garmin_fit_file)s,
            %(source_system)s, %(source_object)s, %(source_record_id)s,
            %(activity_fingerprint)s
        )
        RETURNING exercise_id;
    """
    params = {
        **exercise_fields,
        "type_of_activity":    exercise_extras["type_override"],
        "subtype_of_activity": exercise_extras["subtype_override"],
        "day_id": day_id,
        "week_id": week_id,
        "source_system": "garmin_fit",
        "source_object": "ingest_fit_workout.py",
    }
    cur.execute(sql, params)
    return cur.fetchone()[0]


def resolve_gear_id_by_name(conn, gear_name):
    """Return gear_id for an exact (case-insensitive) gear name, else None."""
    if not gear_name:
        return None
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT gear_id
            FROM gear
            WHERE lower(name) = lower(%s)
              AND is_deleted = FALSE
            LIMIT 1
            """,
            (gear_name,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def insert_exercise_gear(cur, exercise_id, gear_id):
    """Insert primary gear used for this exercise."""
    cur.execute(
        """
        INSERT INTO exercise_gear (
            exercise_id, gear_id, usage_role, is_primary_gear,
            source_system, source_object
        ) VALUES (
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (exercise_id, gear_id) DO UPDATE SET
            usage_role = EXCLUDED.usage_role,
            is_primary_gear = EXCLUDED.is_primary_gear,
            updated_at = now();
        """,
        (
            exercise_id,
            gear_id,
            "primary",
            True,
            "manual_intake",
            "ingest_fit_workout.py",
        ),
    )


def insert_trackpoints(cur, exercise_id, trackpoints):
    """Bulk insert exercise_trackpoint rows."""
    sql = """
        INSERT INTO exercise_trackpoint (
            exercise_id, recorded_at,
            position_lat, position_long,
            altitude_meters, distance_meters, speed_ms,
            heart_rate, cadence, power, temperature_c,
            gps_accuracy_m, vertical_oscillation_mm,
            stance_time_ms, stance_time_pct, vertical_ratio_pct,
            ground_contact_balance_pct, performance_condition,
            absolute_pressure_pa, spo2_pct
        ) VALUES (
            %(exercise_id)s, %(recorded_at)s,
            %(position_lat)s, %(position_long)s,
            %(altitude_meters)s, %(distance_meters)s, %(speed_ms)s,
            %(heart_rate)s, %(cadence)s, %(power)s, %(temperature_c)s,
            %(gps_accuracy_m)s, %(vertical_oscillation_mm)s,
            %(stance_time_ms)s, %(stance_time_pct)s, %(vertical_ratio_pct)s,
            %(ground_contact_balance_pct)s, %(performance_condition)s,
            %(absolute_pressure_pa)s, %(spo2_pct)s
        )
        ON CONFLICT (exercise_id, recorded_at) DO NOTHING;
    """
    rows = [{"exercise_id": exercise_id, **tp} for tp in trackpoints]
    execute_batch(cur, sql, rows, page_size=500)


def insert_workout_analysis(cur, exercise_id, analysis_data):
    """Insert into workout_analysis."""
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
    import argparse
    parser = argparse.ArgumentParser(description="Ingest a Garmin FIT file into the personal DB.")
    parser.add_argument("fit_file", help="Path to the .fit file")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run the full intake but skip all database writes.")
    args = parser.parse_args()

    fit_path = args.fit_file
    dry_run  = args.dry_run

    if not os.path.exists(fit_path):
        print(f"Error: file not found — {fit_path}")
        sys.exit(1)

    # ── Step 1: Parse FIT file ──────────────────────────────
    print(f"\nParsing {fit_path} ...")
    session_data, trackpoints = parse_fit(fit_path)
    fit_filename   = os.path.basename(fit_path)
    exercise_fields = extract_exercise_fields(session_data, fit_filename=fit_filename)
    tp_rows = extract_trackpoints(trackpoints)

    # ── Step 2: Preview ─────────────────────────────────────
    print_preview(exercise_fields, len(tp_rows))

    confirm = input("\n  Does this look correct? Proceed to intake? (y/n): ").strip().lower()
    if confirm != "y":
        print("  Aborted.")
        sys.exit(0)

    # ── Step 3: Intake interview ─────────────────────────────
    exercise_extras, analysis_data = run_intake_interview(
        fit_type=exercise_fields.get("type_of_activity"),
        fit_subtype=exercise_fields.get("subtype_of_activity"),
    )

    # ── Recompute fingerprint using confirmed type_override ──
    # Done here — after the interview — so the fingerprint always matches
    # the type_of_activity that will actually be persisted to the DB.
    exercise_fields["activity_fingerprint"] = make_fingerprint(
        exercise_fields.get("activity_date"),
        exercise_extras["type_override"],
        exercise_fields.get("distance_miles"),
        exercise_fields.get("duration_minutes"),
    )

    # ── Step 4: Write summary + final confirmation ───────────
    print_write_summary(exercise_fields, exercise_extras, analysis_data, len(tp_rows))
    confirm2 = input("  Write to database? (y/n): ").strip().lower()
    if confirm2 != "y":
        print("  Aborted — nothing written.")
        sys.exit(0)

    # ── Step 5: Write to Postgres (or skip if dry-run) ───────
    if dry_run:
        print("\n" + "=" * 56)
        print("  DRY RUN — no data written to database.")
        print("=" * 56 + "\n")
        return

    print("\n  Connecting to database ...")
    conn = get_connection()

    try:
        activity_date = exercise_fields.get("activity_date")
        if not activity_date:
            print("  Error: could not determine activity date from FIT file.")
            sys.exit(1)

        # ── Idempotency check ────────────────────────────────────
        existing_id, match_type = find_existing_exercise(
            conn,
            exercise_fields.get("source_record_id"),
            exercise_fields.get("activity_fingerprint"),
        )
        if existing_id:
            label = "source key (exact FIT file)" if match_type == "source_key" else "fingerprint (cross-source match)"
            print(f"\n  ⚠️  Duplicate detected via {label}.")
            print(f"  Existing exercise_id = {existing_id}")
            proceed = input("  This activity may already be in the database. Proceed anyway? (y/n): ").strip().lower()
            if proceed != "y":
                print("  Aborted — nothing written.")
                conn.close()
                sys.exit(0)

        day_id  = resolve_day_id(conn, activity_date)
        week_id = resolve_week_id(conn, activity_date)

        if not day_id:
            print(f"  Warning: no day record found for {activity_date}. "
                  "exercise.day_id will be NULL.")
        if not week_id:
            print(f"  Warning: no week record found covering {activity_date}. "
                  "exercise.week_id will be NULL.")

        with conn:
            with conn.cursor() as cur:
                # Insert exercise
                exercise_id = insert_exercise(cur, exercise_fields, exercise_extras, day_id, week_id)
                print(f"  ✓ exercise inserted — exercise_id = {exercise_id}")

                # Insert exercise_gear (if a gear was provided and matched)
                gear_name = exercise_extras.get("gear_name")
                if gear_name:
                    gear_id = resolve_gear_id_by_name(conn, gear_name)
                    if gear_id:
                        insert_exercise_gear(cur, exercise_id, gear_id)
                        print(f"  ✓ exercise_gear inserted — gear_id = {gear_id} ({gear_name})")
                    else:
                        print(f"  Warning: gear '{gear_name}' not found; no exercise_gear row inserted.")

                # Insert trackpoints
                insert_trackpoints(cur, exercise_id, tp_rows)
                print(f"  ✓ {len(tp_rows):,} trackpoints inserted")

                # Insert workout_analysis
                wa_id = insert_workout_analysis(cur, exercise_id, analysis_data)
                print(f"  ✓ workout_analysis inserted — workout_analysis_id = {wa_id}")

        print("\n" + "=" * 56)
        print(f"  DONE — Exercise #{exercise_id} logged successfully.")
        print(f"  {exercise_fields['distance_miles']} mi "
              f"| {exercise_extras['type_override']} "
              f"| {exercise_extras['subtype_override'] or 'no subtype'} "
              f"| RPE {analysis_data['perceived_level_of_effort']} "
              f"| {analysis_data['session_quality']}")
        print("=" * 56 + "\n")

    except Exception as e:
        conn.rollback()
        print(f"\n  Error during database write: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
