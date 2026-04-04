#!/usr/bin/env python3
"""
classify_garmin_files.py
------------------------
Scans a directory of Garmin .fit and .tcx files and produces a classification
manifest CSV. The manifest is the input to all downstream batch ingest scripts.

Destinations:
  exercise          — activity that belongs in the exercise table
  health_monitoring — monitoring/health data (skip for now, ingest later)
  segment_file      — Garmin course/segment definition (skip)
  device_file       — firmware updates, device config files (skip)
  unknown           — could not classify confidently; review manually

Usage:
  python classify_garmin_files.py <garmin_export_dir> [--output manifest.csv]

Dependencies:
  pip install fitparse --break-system-packages

Author: John Radcliffe
"""

import os
import sys
import csv
import argparse
import traceback
from datetime import datetime, timezone

try:
    import fitparse
except ImportError:
    print("ERROR: fitparse not installed. Run: pip install fitparse --break-system-packages")
    sys.exit(1)

try:
    import xml.etree.ElementTree as ET
except ImportError:
    pass

# ---------------------------------------------------------------------------
# SPORT → DESTINATION MAPPING
# ---------------------------------------------------------------------------
# Any sport string in this set maps to exercise.
# Sub-sports are used for subtype_of_activity enrichment (not routing).
EXERCISE_SPORTS = {
    # Running
    "running",
    # Cycling
    "cycling",
    # Skiing / Snow
    "alpine_skiing",
    "cross_country_skiing",
    "snowboarding",
    "snowshoeing",
    "mountaineering",
    # Hiking / Walking
    "hiking",
    "walking",
    # Water
    "swimming",
    "paddling",
    "rowing",
    "kayaking",
    "stand_up_paddleboarding",
    # Climbing
    "rock_climbing",
    # Fitness
    "training",
    "fitness_equipment",
    "gym_and_fitness",
    "strength_training",
    "yoga",
    "cardio",
    "hiit",
    # Other outdoor
    "golf",
    "tennis",
    "soccer",
    "basketball",
    "baseball",
    "multisport",
    "triathlon",
    "transition",
    # Garmin sport code 60 = generic fitness / unknown activity — treat as exercise
    "60",
}

# Sub-sport → subtype_of_activity label (informational, not routing)
SUB_SPORT_LABELS = {
    "generic":          None,           # leave type_of_activity as primary
    "treadmill":        "treadmill",
    "trail":            "trail",
    "track":            "track",
    "road":             "road",
    "mountain":         "mountain",
    "backcountry":      "backcountry",
    "indoor":           "indoor",
    "e_bike_fitness":   "e-bike",
    "cyclocross":       "cyclocross",
    "hand_cycling":     "hand cycling",
    "68":               "rock climbing",  # numeric sub_sport sometimes returned
}

# Message types that indicate health/monitoring data
HEALTH_MSG_TYPES = {"monitoring", "hrv", "hrv_status_summary", "sleep_level",
                    "sleep_assessment", "stress_level", "respiration_rate",
                    "spo2_data", "pulse_ox"}

# Message types that indicate segment/course files
SEGMENT_MSG_TYPES = {"segment_file", "segment_lap", "segment_point",
                     "course", "course_point"}

# Message types that indicate device/firmware files
DEVICE_MSG_TYPES = {"software", "capabilities"}

# ---------------------------------------------------------------------------
# FIT CLASSIFICATION
# ---------------------------------------------------------------------------

def classify_fit(path: str) -> dict:
    """Parse a .fit file and return a classification dict."""
    result = {
        "sport":            None,
        "sub_sport":        None,
        "started_at":       None,
        "duration_seconds": None,
        "distance_m":       None,
        "destination":      "unknown",
        "reason":           None,
    }

    try:
        ff = fitparse.FitFile(path)
    except Exception as e:
        result["destination"] = "unknown"
        result["reason"] = f"parse_error: {str(e)[:80]}"
        return result

    msg_names_seen = set()
    sport_found = False

    try:
        for msg in ff.get_messages():
            msg_names_seen.add(msg.name)

            # --- Sport message (most reliable) ---
            if msg.name == "sport" and not sport_found:
                fields = {f.name: f.value for f in msg.fields}
                sport = str(fields.get("sport") or "").lower()
                sub   = str(fields.get("sub_sport") or "generic").lower()
                result["sport"]     = sport
                result["sub_sport"] = sub
                sport_found = True

            # --- Session message (fallback for sport, also has timestamps) ---
            elif msg.name == "session":
                fields = {f.name: f.value for f in msg.fields}
                if not sport_found:
                    sport = str(fields.get("sport") or "").lower()
                    sub   = str(fields.get("sub_sport") or "generic").lower()
                    if sport:
                        result["sport"]     = sport
                        result["sub_sport"] = sub
                        sport_found = True

                # Timestamps and metrics from session
                ts = fields.get("start_time")
                if ts and result["started_at"] is None:
                    if isinstance(ts, datetime):
                        result["started_at"] = ts.isoformat()
                    else:
                        result["started_at"] = str(ts)

                dur = fields.get("total_elapsed_time") or fields.get("total_timer_time")
                if dur is not None and result["duration_seconds"] is None:
                    result["duration_seconds"] = round(float(dur), 1)

                dist = fields.get("total_distance")
                if dist is not None and result["distance_m"] is None:
                    result["distance_m"] = round(float(dist), 1)

    except Exception as e:
        result["reason"] = f"mid_parse_error: {str(e)[:80]}"

    # --- Route decision ---
    if sport_found and result["sport"] in EXERCISE_SPORTS:
        result["destination"] = "exercise"
        result["reason"]      = f"sport={result['sport']}"

    elif sport_found and result["sport"]:
        # Sport found but not in our known list — still an activity
        result["destination"] = "exercise"
        result["reason"]      = f"sport={result['sport']} (unmapped, treated as exercise)"

    elif msg_names_seen & HEALTH_MSG_TYPES:
        result["destination"] = "health_monitoring"
        dominant = list(msg_names_seen & HEALTH_MSG_TYPES)[0]
        result["reason"] = f"msg_type={dominant}"

    elif msg_names_seen & SEGMENT_MSG_TYPES:
        result["destination"] = "segment_file"
        result["reason"] = "msg_type=segment_file"

    else:
        # No sport data found — classify by message type composition
        KNOWN_DEVICE_MSGS = {
            "file_id", "file_creator", "device_info", "event", "software",
            "bike_profile", "device_settings", "hrm_profile", "sdm_profile",
            "timestamp_correlation", "capabilities",
        }
        non_device = {
            m for m in msg_names_seen
            if m not in KNOWN_DEVICE_MSGS
            and not m.startswith("unknown_")  # undocumented Garmin proprietary msg types
        }

        if not non_device:
            # All messages are known device types or undocumented proprietary types
            result["destination"] = "device_file"
            result["reason"] = f"msg_types={sorted(msg_names_seen)[:5]}"
        else:
            result["destination"] = "unknown"
            result["reason"] = f"no_sport, msg_types={sorted(msg_names_seen)[:6]}"

    return result


# ---------------------------------------------------------------------------
# TCX CLASSIFICATION
# ---------------------------------------------------------------------------

def classify_tcx(path: str) -> dict:
    """Parse a .tcx file and return a classification dict."""
    result = {
        "sport":            None,
        "sub_sport":        None,
        "started_at":       None,
        "duration_seconds": None,
        "distance_m":       None,
        "destination":      "unknown",
        "reason":           None,
    }

    TCX_NS = "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"

    try:
        tree = ET.parse(path)
        root = tree.getroot()
        ns = {"tcx": TCX_NS}

        activities = root.findall(".//tcx:Activity", ns)
        if not activities:
            result["destination"] = "unknown"
            result["reason"] = "no_activity_element"
            return result

        act = activities[0]
        sport = (act.get("Sport") or "").strip()
        result["sport"] = sport.lower()

        # All TCX activities are exercise records (no health monitoring in TCX format)
        result["destination"] = "exercise"
        result["reason"]      = f"tcx_sport={sport}"

        # Timestamps
        lap = act.find("tcx:Lap", ns)
        if lap is not None:
            ts = lap.get("StartTime")
            if ts:
                result["started_at"] = ts

            dur_el = lap.find("tcx:TotalTimeSeconds", ns)
            if dur_el is not None and dur_el.text:
                result["duration_seconds"] = round(float(dur_el.text), 1)

            dist_el = lap.find("tcx:DistanceMeters", ns)
            if dist_el is not None and dist_el.text:
                result["distance_m"] = round(float(dist_el.text), 1)

    except Exception as e:
        result["destination"] = "unknown"
        result["reason"] = f"parse_error: {str(e)[:80]}"

    return result


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Classify Garmin FIT/TCX export files.")
    parser.add_argument("input_dir", help="Directory containing .fit and .tcx files")
    parser.add_argument("--output", default="garmin_manifest.csv",
                        help="Output CSV path (default: garmin_manifest.csv)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Process only first N files (for testing)")
    args = parser.parse_args()

    if not os.path.isdir(args.input_dir):
        print(f"ERROR: {args.input_dir} is not a directory.")
        sys.exit(1)

    all_files = sorted([
        f for f in os.listdir(args.input_dir)
        if f.lower().endswith((".fit", ".tcx", ".gpx"))
    ])

    if args.limit:
        all_files = all_files[:args.limit]

    total = len(all_files)
    print(f"Found {total} files to classify.")
    print(f"Writing manifest to: {args.output}\n")

    fieldnames = [
        "filename",
        "file_format",
        "sport",
        "sub_sport",
        "started_at",
        "duration_seconds",
        "distance_m",
        "destination",
        "reason",
        "override_destination",  # blank column — fill in manually to override
        "notes",                 # blank column — for manual review notes
    ]

    counts = {
        "exercise": 0, "health_monitoring": 0,
        "segment_file": 0, "device_file": 0,
        "unknown": 0, "gpx_skipped": 0,
    }

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i, fname in enumerate(all_files):
            if i % 1000 == 0 and i > 0:
                print(f"  [{i}/{total}] processed... ({counts})")

            ext = os.path.splitext(fname)[1].lower()
            path = os.path.join(args.input_dir, fname)

            if ext == ".fit":
                info = classify_fit(path)
            elif ext == ".tcx":
                info = classify_tcx(path)
            elif ext == ".gpx":
                # GPX files handled by existing bulk_ingest_gpx_routes.py
                info = {
                    "sport": None, "sub_sport": None, "started_at": None,
                    "duration_seconds": None, "distance_m": None,
                    "destination": "gpx_skipped",
                    "reason": "use bulk_ingest_gpx_routes.py",
                }
                counts["gpx_skipped"] += 1
            else:
                continue

            dest = info.get("destination", "unknown")
            if dest in counts:
                counts[dest] += 1
            else:
                counts["unknown"] += 1

            writer.writerow({
                "filename":            fname,
                "file_format":         ext.lstrip("."),
                "sport":               info.get("sport") or "",
                "sub_sport":           info.get("sub_sport") or "",
                "started_at":          info.get("started_at") or "",
                "duration_seconds":    info.get("duration_seconds") or "",
                "distance_m":          info.get("distance_m") or "",
                "destination":         dest,
                "reason":              info.get("reason") or "",
                "override_destination": "",
                "notes":               "",
            })

    print(f"\n=== CLASSIFICATION COMPLETE ===")
    print(f"  Total files:       {total}")
    for dest, count in sorted(counts.items(), key=lambda x: -x[1]):
        pct = (count / total * 100) if total else 0
        print(f"  {dest:20s}: {count:6d}  ({pct:.1f}%)")
    print(f"\nManifest written to: {args.output}")
    print("Review the 'unknown' rows before running batch ingest.")


if __name__ == "__main__":
    main()
