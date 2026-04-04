#!/usr/bin/env python3
"""
batch_ingest_exercise.py
------------------------
Batch ingest of Garmin exercise files (FIT + TCX) into the personal database.
Reads the garmin_manifest.csv produced by classify_garmin_files.py and processes
all rows where destination == 'exercise' (or override_destination == 'exercise').

Key behaviors:
  - Idempotent: skips any file whose garmin_fit_file already exists in exercise
  - Non-interactive: no intake interview — workout_analysis rows are NOT written
    (add those later via the normal ingest_fit_workout.py workflow)
  - Dry-run mode: parses and previews everything, writes nothing
  - Limit mode: process only the first N files (useful for test runs)
  - Detailed run log written to a timestamped CSV alongside the manifest

Usage:
  python batch_ingest_exercise.py --manifest garmin_manifest.csv \\
                                  --garmin-dir /path/to/garmin_export \\
                                  [--dry-run] \\
                                  [--limit 10] \\
                                  [--sport running] \\
                                  [--log-dir .]

  # Test run: 10 files, no DB writes
  python batch_ingest_exercise.py --manifest garmin_manifest.csv \\
      --garmin-dir /path/to/garmin_export --dry-run --limit 10

  # Production run: all running activities
  python batch_ingest_exercise.py --manifest garmin_manifest.csv \\
      --garmin-dir /path/to/garmin_export --sport running

Dependencies:
  pip install fitparse psycopg2-binary python-dotenv --break-system-packages

Environment variables (in .env):
  POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, PG_HOST, PG_PORT

Author: John Radcliffe
"""

import os
import sys
import csv
import argparse
import traceback
import xml.etree.ElementTree as ET
from datetime import datetime, date, timezone, timedelta
from decimal import Decimal

try:
    import fitparse
except ImportError:
    print("ERROR: fitparse not installed. Run: pip install fitparse --break-system-packages")
    sys.exit(1)

try:
    import psycopg2
    from psycopg2.extras import execute_batch
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
# SHARED FIT PARSING  (mirrors ingest_fit_workout.py — keep in sync)
# ---------------------------------------------------------------------------

SCALE_CORRECTIONS = {
    "speed":              0.001,
    "enhanced_speed":     0.001,
    "altitude":           (lambda raw: raw * 0.2 - 500 if raw is not None else None),
    "enhanced_altitude":  (lambda raw: raw * 0.2 - 500 if raw is not None else None),
    "vertical_ratio":     0.01,
}


def _scale(field_name, value):
    if value is None:
        return None
    corr = SCALE_CORRECTIONS.get(field_name)
    if corr is None:
        return value
    return corr(value) if callable(corr) else value * corr


def parse_fit(fit_path):
    """Return (session_data dict, trackpoints list)."""
    ff = fitparse.FitFile(fit_path, data_processor=fitparse.StandardUnitsDataProcessor())
    session_data = {}
    trackpoints = []
    for message in ff.get_messages():
        if message.name == "session":
            for field in message.fields:
                session_data[field.name] = field.value
        elif message.name == "record":
            row = {f.name: f.value for f in message.fields}
            trackpoints.append(row)
    return session_data, trackpoints


def extract_exercise_fields_fit(session_data, filename):
    """Map FIT session → exercise table columns. Returns a dict."""
    def get(*keys):
        for k in keys:
            v = session_data.get(k)
            if v is not None:
                return v
        return None

    sport    = get("sport") or "unknown"
    sub      = get("sub_sport")
    dist_m   = get("total_distance")
    timer_s  = get("total_timer_time")
    elapsed_s = get("total_elapsed_time")
    ascent_m  = get("total_ascent")
    start_ts  = get("start_time")

    activity_date = None
    if isinstance(start_ts, datetime):
        activity_date = start_ts.date()

    vr_raw = get("avg_vertical_ratio")

    return {
        "garmin_fit_file":         filename,
        "activity_date":           activity_date,
        "type_of_activity":        str(sport).replace("Sport.", "").lower() if sport else None,
        "subtype_of_activity":     str(sub).replace("SubSport.", "").lower() if sub else None,
        "distance_miles":          round(dist_m / 1609.344, 2) if dist_m else None,
        "duration_minutes":        round(timer_s / 60, 2) if timer_s else None,
        "total_elapsed_time":      round(elapsed_s / 60, 2) if elapsed_s else None,
        "elevation_gain_feet":     round(ascent_m * 3.28084, 1) if ascent_m else None,
        "average_heart_rate":      get("avg_heart_rate"),
        "max_heart_rate":          get("max_heart_rate"),
        "average_cadence":         get("avg_running_cadence", "avg_cadence"),
        "average_power":           get("avg_power"),
        "max_power":               get("max_power"),
        "calories":                get("total_calories"),
        "num_laps":                get("num_laps"),
        "tss_score":               get("training_stress_score"),
        "avg_vertical_ratio":      _scale("vertical_ratio", vr_raw),
        "avg_stance_time_balance": get("avg_stance_time_balance"),
        "avg_step_length_mm":      get("avg_step_length"),
        "source_system":           "garmin_fit",
        "source_object":           "batch_ingest_exercise.py",
        "source_record_id":        os.path.splitext(filename)[0],  # filename stem as stable ID
    }


def extract_trackpoints_fit(trackpoints):
    """Map FIT record messages → exercise_trackpoint rows."""
    rows = []
    for tp in trackpoints:
        g = tp.get
        lat = g("position_lat")
        lon = g("position_long")
        alt = g("enhanced_altitude") or g("altitude")
        if isinstance(alt, int):
            alt = _scale("altitude", alt)
        speed = g("enhanced_speed") or g("speed")
        if isinstance(speed, int):
            speed = _scale("speed", speed)
        vr = g("vertical_ratio")
        if isinstance(vr, int):
            vr = _scale("vertical_ratio", vr)

        rows.append({
            "recorded_at":                 g("timestamp"),
            "position_lat":                round(lat, 6) if lat is not None else None,
            "position_long":               round(lon, 6) if lon is not None else None,
            "altitude_meters":             round(alt, 1) if alt is not None else None,
            "distance_meters":             round(g("distance"), 1) if g("distance") is not None else None,
            "speed_ms":                    round(speed, 3) if speed is not None else None,
            "heart_rate":                  g("heart_rate"),
            "cadence":                     g("cadence"),
            "power":                       g("power"),
            "temperature_c":               g("temperature"),
            "gps_accuracy_m":              g("gps_accuracy"),
            "vertical_oscillation_mm":     g("vertical_oscillation"),
            "stance_time_ms":              g("stance_time"),
            "stance_time_pct":             g("stance_time_percent"),
            "vertical_ratio_pct":          round(vr, 2) if vr is not None else None,
            "ground_contact_balance_pct":  g("left_right_balance"),
            "performance_condition":       g("performance_condition"),
            "absolute_pressure_pa":        g("absolute_pressure"),
            "spo2_pct":                    g("pulse_oximetry_data"),
        })
    return rows


# ---------------------------------------------------------------------------
# TCX PARSING
# ---------------------------------------------------------------------------

TCX_NS = "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"
TCX_NS2 = "http://www.garmin.com/xmlschemas/ActivityExtension/v2"


def _tcx_text(el, tag, ns=None):
    """Safe text extract from a child element."""
    ns = ns or {"tcx": TCX_NS}
    child = el.find(f"tcx:{tag}", ns)
    return child.text.strip() if child is not None and child.text else None


def parse_tcx(tcx_path):
    """
    Parse a .tcx file.
    Returns (exercise_fields dict, trackpoints list).
    TCX doesn't have the same richness as FIT — we extract what's available.
    """
    ns = {"tcx": TCX_NS, "ext": TCX_NS2}
    tree = ET.parse(tcx_path)
    root = tree.getroot()

    filename = os.path.basename(tcx_path)
    exercise_fields = {
        "garmin_fit_file":         filename,   # column is overloaded for TCX too
        "activity_date":           None,
        "type_of_activity":        None,
        "subtype_of_activity":     None,
        "distance_miles":          None,
        "duration_minutes":        None,
        "total_elapsed_time":      None,
        "elevation_gain_feet":     None,
        "average_heart_rate":      None,
        "max_heart_rate":          None,
        "average_cadence":         None,
        "average_power":           None,
        "max_power":               None,
        "calories":                None,
        "num_laps":                None,
        "tss_score":               None,
        "avg_vertical_ratio":      None,
        "avg_stance_time_balance": None,
        "avg_step_length_mm":      None,
        "source_system":           "garmin_tcx",
        "source_object":           "batch_ingest_exercise.py",
        "source_record_id":        os.path.splitext(filename)[0],
    }

    activities = root.findall(".//tcx:Activity", ns)
    if not activities:
        raise ValueError("No <Activity> element found in TCX file")

    act = activities[0]
    sport = act.get("Sport", "")
    exercise_fields["type_of_activity"] = sport.lower()

    laps = act.findall("tcx:Lap", ns)
    if not laps:
        raise ValueError("No <Lap> elements found in TCX file")

    exercise_fields["num_laps"] = len(laps)

    # Accumulate across all laps
    total_dist_m   = 0.0
    total_time_s   = 0.0
    total_calories = 0
    total_ascent_m = 0.0
    max_hr         = 0
    sum_hr         = 0
    hr_count       = 0
    trackpoints    = []

    first_lap_ts = None

    for lap in laps:
        ts_str = lap.get("StartTime")
        if ts_str and first_lap_ts is None:
            try:
                first_lap_ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except Exception:
                pass

        dist = _tcx_text(lap, "DistanceMeters", ns)
        if dist:
            total_dist_m += float(dist)

        dur = _tcx_text(lap, "TotalTimeSeconds", ns)
        if dur:
            total_time_s += float(dur)

        cal = _tcx_text(lap, "Calories", ns)
        if cal:
            total_calories += int(cal)

        # Max HR from lap
        mhr_el = lap.find("tcx:MaximumHeartRateBpm/tcx:Value", ns)
        if mhr_el is not None and mhr_el.text:
            mhr = int(mhr_el.text)
            if mhr > max_hr:
                max_hr = mhr

        # Avg HR from lap
        ahr_el = lap.find("tcx:AverageHeartRateBpm/tcx:Value", ns)
        if ahr_el is not None and ahr_el.text:
            sum_hr += int(ahr_el.text)
            hr_count += 1

        # Trackpoints
        for tp_el in lap.findall(".//tcx:Trackpoint", ns):
            tp = {}

            ts_el = tp_el.find("tcx:Time", ns)
            if ts_el is not None and ts_el.text:
                try:
                    tp["recorded_at"] = datetime.fromisoformat(
                        ts_el.text.strip().replace("Z", "+00:00")
                    )
                except Exception:
                    tp["recorded_at"] = None
            else:
                tp["recorded_at"] = None

            pos_el = tp_el.find("tcx:Position", ns)
            if pos_el is not None:
                lat_el = pos_el.find("tcx:LatitudeDegrees", ns)
                lon_el = pos_el.find("tcx:LongitudeDegrees", ns)
                tp["position_lat"]  = float(lat_el.text) if lat_el is not None and lat_el.text else None
                tp["position_long"] = float(lon_el.text) if lon_el is not None and lon_el.text else None
            else:
                tp["position_lat"] = tp["position_long"] = None

            alt_el = tp_el.find("tcx:AltitudeMeters", ns)
            tp["altitude_meters"] = float(alt_el.text) if alt_el is not None and alt_el.text else None

            dist_el = tp_el.find("tcx:DistanceMeters", ns)
            tp["distance_meters"] = float(dist_el.text) if dist_el is not None and dist_el.text else None

            hr_el = tp_el.find("tcx:HeartRateBpm/tcx:Value", ns)
            tp["heart_rate"] = int(hr_el.text) if hr_el is not None and hr_el.text else None

            cad_el = tp_el.find("tcx:Cadence", ns)
            tp["cadence"] = int(cad_el.text) if cad_el is not None and cad_el.text else None

            # Extension fields (speed, watts)
            ext_el = tp_el.find("tcx:Extensions", ns)
            tp["speed_ms"] = tp["power"] = None
            if ext_el is not None:
                spd_el = ext_el.find(".//ext:Speed", ns)
                if spd_el is not None and spd_el.text:
                    tp["speed_ms"] = float(spd_el.text)
                pwr_el = ext_el.find(".//ext:Watts", ns)
                if pwr_el is not None and pwr_el.text:
                    tp["power"] = int(pwr_el.text)

            # Null out fields TCX doesn't carry
            tp.update({
                "temperature_c":               None,
                "gps_accuracy_m":              None,
                "vertical_oscillation_mm":     None,
                "stance_time_ms":              None,
                "stance_time_pct":             None,
                "vertical_ratio_pct":          None,
                "ground_contact_balance_pct":  None,
                "performance_condition":       None,
                "absolute_pressure_pa":        None,
                "spo2_pct":                    None,
            })

            trackpoints.append(tp)

    # Elevation gain: compute from trackpoints since TCX laps don't always have it
    if trackpoints:
        alts = [tp["altitude_meters"] for tp in trackpoints if tp.get("altitude_meters") is not None]
        if len(alts) >= 2:
            gain = sum(max(0, alts[i] - alts[i-1]) for i in range(1, len(alts)))
            total_ascent_m = gain

    exercise_fields["activity_date"]       = first_lap_ts.date() if first_lap_ts else None
    exercise_fields["distance_miles"]      = round(total_dist_m / 1609.344, 2) if total_dist_m else None
    exercise_fields["duration_minutes"]    = round(total_time_s / 60, 2) if total_time_s else None
    exercise_fields["total_elapsed_time"]  = exercise_fields["duration_minutes"]  # TCX doesn't split these
    exercise_fields["elevation_gain_feet"] = round(total_ascent_m * 3.28084, 1) if total_ascent_m else None
    exercise_fields["calories"]            = total_calories or None
    exercise_fields["max_heart_rate"]      = max_hr or None
    exercise_fields["average_heart_rate"]  = round(sum_hr / hr_count, 1) if hr_count else None

    return exercise_fields, trackpoints


# ---------------------------------------------------------------------------
# DUPLICATE CHECK
# ---------------------------------------------------------------------------

def already_ingested(conn, filename):
    """Return True if a row with this garmin_fit_file already exists in exercise."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM exercise WHERE garmin_fit_file = %s AND is_deleted = FALSE LIMIT 1",
            (filename,),
        )
        return cur.fetchone() is not None


# ---------------------------------------------------------------------------
# DATABASE WRITES  (mirrors ingest_fit_workout.py — keep in sync)
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
    """Insert exercise row. Returns new exercise_id."""
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
            source_system, source_object, source_record_id
        ) VALUES (
            %(day_id)s, %(week_id)s, %(activity_date)s,
            %(type_of_activity)s, %(subtype_of_activity)s,
            %(distance_miles)s, %(duration_minutes)s, %(total_elapsed_time)s,
            %(elevation_gain_feet)s, %(average_heart_rate)s, %(max_heart_rate)s,
            %(average_cadence)s, %(average_power)s, %(max_power)s,
            %(calories)s, %(tss_score)s, %(num_laps)s,
            %(avg_vertical_ratio)s, %(avg_stance_time_balance)s, %(avg_step_length_mm)s,
            %(garmin_fit_file)s,
            %(source_system)s, %(source_object)s, %(source_record_id)s
        )
        RETURNING exercise_id;
    """
    cur.execute(sql, {**fields, "day_id": day_id, "week_id": week_id})
    return cur.fetchone()[0]


def insert_trackpoints(cur, exercise_id, trackpoints):
    """Bulk insert exercise_trackpoint rows."""
    if not trackpoints:
        return
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


# ---------------------------------------------------------------------------
# PER-FILE PROCESSOR
# ---------------------------------------------------------------------------

def process_file(filepath, filename, fmt, dry_run, conn):
    """
    Parse one file, run duplicate check, write to DB (unless dry_run).
    Returns a result dict for the run log.
    """
    result = {
        "filename":        filename,
        "format":          fmt,
        "activity_date":   "",
        "type_of_activity": "",
        "distance_miles":  "",
        "duration_minutes": "",
        "trackpoints":     "",
        "exercise_id":     "",
        "status":          "",
        "note":            "",
    }

    # ── Parse ──────────────────────────────────────────────────────────────
    try:
        if fmt == "fit":
            session_data, tp_raw = parse_fit(filepath)
            exercise_fields = extract_exercise_fields_fit(session_data, filename)
            trackpoints = extract_trackpoints_fit(tp_raw)
        elif fmt == "tcx":
            exercise_fields, trackpoints = parse_tcx(filepath)
        else:
            result["status"] = "skipped"
            result["note"] = f"unsupported format: {fmt}"
            return result
    except Exception as e:
        result["status"] = "error_parse"
        result["note"] = str(e)[:120]
        return result

    result["activity_date"]    = str(exercise_fields.get("activity_date") or "")
    result["type_of_activity"] = exercise_fields.get("type_of_activity") or ""
    result["distance_miles"]   = str(exercise_fields.get("distance_miles") or "")
    result["duration_minutes"] = str(exercise_fields.get("duration_minutes") or "")
    result["trackpoints"]      = str(len(trackpoints))

    # ── Dry-run: preview and stop ──────────────────────────────────────────
    if dry_run:
        result["status"] = "dry_run_ok"
        result["note"] = (
            f"{exercise_fields.get('type_of_activity')} | "
            f"{exercise_fields.get('distance_miles')} mi | "
            f"{exercise_fields.get('duration_minutes')} min | "
            f"{len(trackpoints)} trackpoints"
        )
        return result

    # ── Duplicate check ────────────────────────────────────────────────────
    try:
        if already_ingested(conn, filename):
            result["status"] = "skipped_duplicate"
            result["note"]   = "garmin_fit_file already in exercise table"
            return result
    except Exception as e:
        result["status"] = "error_db"
        result["note"]   = f"duplicate check failed: {e}"
        return result

    # ── Validate required fields ───────────────────────────────────────────
    if not exercise_fields.get("activity_date"):
        result["status"] = "skipped_no_date"
        result["note"]   = "could not determine activity_date from file"
        return result

    # ── DB write ──────────────────────────────────────────────────────────
    try:
        activity_date = exercise_fields["activity_date"]
        day_id  = resolve_day_id(conn, activity_date)
        week_id = resolve_week_id(conn, activity_date)

        with conn:
            with conn.cursor() as cur:
                exercise_id = insert_exercise(cur, exercise_fields, day_id, week_id)
                insert_trackpoints(cur, exercise_id, trackpoints)

        result["exercise_id"] = str(exercise_id)
        result["status"]      = "inserted"
        result["note"]        = (
            f"day_id={day_id}, week_id={week_id}, "
            f"{len(trackpoints)} trackpoints"
        )

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        result["status"] = "error_write"
        result["note"]   = str(e)[:120]

    return result


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Batch ingest Garmin exercise files from the manifest CSV."
    )
    parser.add_argument(
        "--manifest", required=True,
        help="Path to garmin_manifest.csv (produced by classify_garmin_files.py)",
    )
    parser.add_argument(
        "--garmin-dir", required=True,
        help="Directory containing the Garmin export files",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse files and preview output — no database writes",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Process only the first N exercise files (useful for test runs)",
    )
    parser.add_argument(
        "--sport", default=None,
        help="Filter to a specific sport (e.g. running, cycling, alpine_skiing). "
             "Matches the 'sport' column in the manifest.",
    )
    parser.add_argument(
        "--log-dir", default=".",
        help="Directory to write the run log CSV (default: current directory)",
    )
    args = parser.parse_args()

    # ── Load manifest ──────────────────────────────────────────────────────
    if not os.path.exists(args.manifest):
        print(f"ERROR: manifest not found — {args.manifest}")
        sys.exit(1)

    with open(args.manifest, newline="", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    # Filter to exercise rows (respect override_destination if set)
    def effective_dest(row):
        return row.get("override_destination", "").strip() or row.get("destination", "").strip()

    exercise_rows = [r for r in all_rows if effective_dest(r) == "exercise"]

    # Sport filter
    if args.sport:
        exercise_rows = [r for r in exercise_rows if r.get("sport", "") == args.sport]

    # Limit
    if args.limit:
        exercise_rows = exercise_rows[:args.limit]

    total = len(exercise_rows)
    mode  = "DRY RUN" if args.dry_run else "LIVE"
    print(f"\n{'=' * 60}")
    print(f"  batch_ingest_exercise.py  —  {mode}")
    print(f"{'=' * 60}")
    print(f"  Manifest    : {args.manifest}")
    print(f"  Garmin dir  : {args.garmin_dir}")
    print(f"  Sport filter: {args.sport or 'all'}")
    print(f"  Files to process: {total}")
    if args.dry_run:
        print("  ⚠  DRY RUN — nothing will be written to the database")
    print(f"{'=' * 60}\n")

    if total == 0:
        print("  No exercise files matched the filter. Exiting.")
        sys.exit(0)

    # ── Set up run log ─────────────────────────────────────────────────────
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(args.log_dir, f"ingest_run_{ts}.csv")
    log_fields = [
        "filename", "format", "activity_date", "type_of_activity",
        "distance_miles", "duration_minutes", "trackpoints",
        "exercise_id", "status", "note",
    ]

    # ── DB connection (skip in dry-run) ────────────────────────────────────
    conn = None
    if not args.dry_run:
        try:
            conn = get_connection()
            print("  ✓ Database connected")
        except Exception as e:
            print(f"  ERROR: could not connect to database — {e}")
            sys.exit(1)

    # ── Process files ──────────────────────────────────────────────────────
    counters = {
        "inserted":           0,
        "skipped_duplicate":  0,
        "skipped_no_date":    0,
        "skipped":            0,
        "dry_run_ok":         0,
        "error_parse":        0,
        "error_write":        0,
        "error_db":           0,
    }

    run_log = []

    for i, row in enumerate(exercise_rows, start=1):
        filename = row["filename"]
        fmt      = row.get("file_format", os.path.splitext(filename)[1].lstrip(".").lower())
        filepath = os.path.join(args.garmin_dir, filename)

        if not os.path.exists(filepath):
            result = {
                "filename": filename, "format": fmt,
                "activity_date": "", "type_of_activity": "",
                "distance_miles": "", "duration_minutes": "", "trackpoints": "",
                "exercise_id": "", "status": "error_not_found",
                "note": "file not found on disk",
            }
        else:
            result = process_file(filepath, filename, fmt, args.dry_run, conn)

        status = result["status"]
        counters[status] = counters.get(status, 0) + 1
        run_log.append(result)

        # Console output
        marker = {
            "inserted":          "✓",
            "dry_run_ok":        "○",
            "skipped_duplicate": "─",
            "skipped_no_date":   "!",
            "error_parse":       "✗",
            "error_write":       "✗",
            "error_db":          "✗",
            "error_not_found":   "✗",
        }.get(status, "?")

        print(
            f"  [{i:>4}/{total}] {marker} {filename[:52]:52s} "
            f"{result['activity_date']:10s} "
            f"{result['type_of_activity']:15s} "
            f"{result['distance_miles']:>8} mi  "
            f"[{status}]"
        )

    # ── Write run log ──────────────────────────────────────────────────────
    with open(log_filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=log_fields)
        writer.writeheader()
        writer.writerows(run_log)

    # ── Summary ────────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"  RUN COMPLETE  ({mode})")
    print(f"{'=' * 60}")
    for status, count in sorted(counters.items(), key=lambda x: -x[1]):
        if count:
            print(f"  {status:25s}: {count}")
    print(f"\n  Run log: {log_filename}")
    if args.dry_run:
        print("  ⚠  No data was written — re-run without --dry-run to ingest")
    print(f"{'=' * 60}\n")

    if conn:
        conn.close()


if __name__ == "__main__":
    main()
