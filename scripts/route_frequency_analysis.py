#!/usr/bin/env python3
"""
scripts/route_frequency_analysis.py

Analyzes how many times specific routes have been run.

Two operating modes:

  1. FREQUENCY REPORT (default)
     Matches all exercises to routes using a two-stage pipeline, optionally
     writes route_id back to the exercise table, then outputs a ranked
     frequency summary.

  2. SINGLE-DAY LOOKUP (--date)
     Shows which routes were run on a specific day. No write-back in this mode
     unless --force-rematch is also passed.

Matching pipeline (two stages):

  Stage 1 — Start-point proximity gate (cheap):
    Pulls the first exercise_trackpoint (by recorded_at) for each exercise as
    the workout's effective GPS start point. Compares against every route's
    start_lat/start_long via haversine. Only routes within --radius meters
    pass to Stage 2. If zero routes pass, the exercise is unmatched.

  Stage 2 — Path similarity (Hausdorff-style, downsampled):
    For each candidate route that passed Stage 1, downsamples both the
    exercise trackpoints and the route's gps_path JSONB to --path-samples
    evenly-spaced points. For each downsampled exercise point, finds the
    nearest point on the downsampled route path and records that distance.
    The mean of all those nearest-neighbor distances is the deviation score.
    The route with the lowest mean deviation, provided it is under
    --path-threshold meters, wins. If no route is under the threshold, the
    exercise is unmatched.

  Exercises that already have a route_id are re-evaluated unless
  --force-rematch is omitted, in which case conflicts are flagged but not
  overwritten.

Write-back:
  - On a confirmed path match, updates exercise.route_id,
    exercise.route_match_method = 'path', and
    exercise.route_match_confidence = <mean_deviation_m>.
  - Suppressed entirely when --dry-run or --no-update is passed.

Usage:
    # Full frequency report (running, all dates)
    python scripts/route_frequency_analysis.py

    # Top 10 routes this year
    python scripts/route_frequency_analysis.py --date-from 2026-01-01 --limit 10

    # What did I run on a specific day?
    python scripts/route_frequency_analysis.py --date 2026-03-22

    # Preview without touching the DB
    python scripts/route_frequency_analysis.py --dry-run

    # Include all activity types
    python scripts/route_frequency_analysis.py --type all

    # Looser path matching (more samples, wider threshold)
    python scripts/route_frequency_analysis.py --path-samples 50 --path-threshold 100

Options:
    --type            Activity type filter: running | cycling | hiking | all
                      (default: running)
    --date            Single-day lookup: YYYY-MM-DD. Mutually exclusive with
                      --date-from / --date-to.
    --date-from       Filter exercises on or after this date (YYYY-MM-DD).
    --date-to         Filter exercises on or before this date (YYYY-MM-DD).
    --radius          Stage 1 proximity gate radius in meters (default: 100).
    --path-samples    Points to downsample to for path comparison (default: 25).
    --path-threshold  Max mean deviation in meters to accept a path match
                      (default: 75).
    --limit           Cap output rows to top N routes by run count.
    --force-rematch   Re-evaluate and overwrite existing route_id matches.
    --dry-run         Match and report without writing back to the DB.
    --no-update       Skip write-back entirely; just report.
    --output          Path for CSV export. Defaults to
                      scripts/output/route_frequency_YYYYMMDD.csv

Requires: DB_HOST, DB_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD in .env
"""

import os
import csv
import math
import argparse
from datetime import date, datetime, timezone
from collections import defaultdict

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("POSTGRES_DB")
DB_USER     = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")


# ---------------------------------------------------------------------------
# Haversine
# ---------------------------------------------------------------------------

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Returns distance in meters between two lat/lon points."""
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi       = math.radians(lat2 - lat1)
    dlambda    = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )


def load_routes(conn) -> list[dict]:
    """Load all active routes with start coordinates."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT route_id, name, distance_miles, elevation_gain_feet,
                   start_lat, start_long
            FROM route
            WHERE is_deleted = FALSE
              AND start_lat IS NOT NULL
              AND start_long IS NOT NULL
            ORDER BY route_id
        """)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def load_exercises(conn, activity_type: str, date_filter: dict) -> list[dict]:
    """
    Load exercises matching the type and date filters.
    date_filter keys: single_date | date_from | date_to (all optional)
    Returns list of dicts with exercise metadata + existing route_id.
    """
    clauses = ["e.is_deleted = FALSE"]
    params  = []

    if activity_type != "all":
        clauses.append("e.type_of_activity = %s")
        params.append(activity_type)

    if date_filter.get("single_date"):
        clauses.append("e.activity_date = %s")
        params.append(date_filter["single_date"])
    else:
        if date_filter.get("date_from"):
            clauses.append("e.activity_date >= %s")
            params.append(date_filter["date_from"])
        if date_filter.get("date_to"):
            clauses.append("e.activity_date <= %s")
            params.append(date_filter["date_to"])

    where = " AND ".join(clauses)

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT e.exercise_id, e.activity_date, e.distance_miles,
                   e.duration_minutes, e.average_heart_rate,
                   e.type_of_activity, e.route_id,
                   e.route_match_method, e.route_match_confidence
            FROM exercise e
            WHERE {where}
            ORDER BY e.activity_date
        """, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def load_first_trackpoints(conn, exercise_ids: list[int]) -> dict[int, dict]:
    """
    For a list of exercise_ids, return the first trackpoint (by recorded_at)
    that has a non-null position. Returns {exercise_id: {lat, lon}}.
    """
    if not exercise_ids:
        return {}

    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (exercise_id)
                exercise_id, position_lat, position_long
            FROM exercise_trackpoint
            WHERE exercise_id = ANY(%s)
              AND position_lat IS NOT NULL
              AND position_long IS NOT NULL
            ORDER BY exercise_id, recorded_at ASC
        """, (exercise_ids,))
        return {
            row[0]: {"lat": float(row[1]), "lon": float(row[2])}
            for row in cur.fetchall()
        }


def load_all_trackpoints(conn, exercise_ids: list[int]) -> dict[int, list[dict]]:
    """
    For a list of exercise_ids, return all trackpoints with GPS data ordered
    by recorded_at. Returns {exercise_id: [{lat, lon}, ...]}.
    Used for path similarity stage.
    """
    if not exercise_ids:
        return {}

    with conn.cursor() as cur:
        cur.execute("""
            SELECT exercise_id, position_lat, position_long
            FROM exercise_trackpoint
            WHERE exercise_id = ANY(%s)
              AND position_lat IS NOT NULL
              AND position_long IS NOT NULL
            ORDER BY exercise_id, recorded_at ASC
        """, (exercise_ids,))
        result: dict[int, list[dict]] = defaultdict(list)
        for row in cur.fetchall():
            result[row[0]].append({"lat": float(row[1]), "lon": float(row[2])})
        return dict(result)


def load_route_gps_paths(conn, route_ids: list[int]) -> dict[int, list[dict]]:
    """
    Load gps_path JSONB for a set of route_ids.
    Returns {route_id: [{lat, lon}, ...]}.
    Routes with null or empty gps_path are excluded.
    """
    if not route_ids:
        return {}

    with conn.cursor() as cur:
        cur.execute("""
            SELECT route_id, gps_path
            FROM route
            WHERE route_id = ANY(%s)
              AND gps_path IS NOT NULL
              AND is_deleted = FALSE
        """, (route_ids,))
        result = {}
        for row in cur.fetchall():
            path = row[1]  # psycopg2 returns JSONB as Python object
            if isinstance(path, list) and len(path) >= 2:
                result[row[0]] = [
                    {"lat": float(p["lat"]), "lon": float(p["lon"])}
                    for p in path
                    if "lat" in p and "lon" in p
                ]
        return result


def write_back_matches(conn, matches: list[dict]) -> int:
    """
    Update exercise rows with matched route_id.
    matches: list of {exercise_id, route_id, score, method}
      score  — mean deviation in meters (path) or start distance (proximity fallback)
      method — 'path' | 'proximity'
    Returns number of rows updated.
    """
    if not matches:
        return 0

    now = datetime.now(timezone.utc)
    data = [
        (m["route_id"], m["method"], round(m["score"], 2), now, m["exercise_id"])
        for m in matches
    ]

    with conn.cursor() as cur:
        execute_values(cur, """
            UPDATE exercise AS e SET
                route_id               = v.route_id,
                route_match_method     = v.method,
                route_match_confidence = v.confidence,
                updated_at             = v.ts
            FROM (VALUES %s) AS v(route_id, method, confidence, ts, exercise_id)
            WHERE e.exercise_id = v.exercise_id
        """, data, template="(%s, %s, %s, %s, %s)")
    conn.commit()
    return len(data)


# ---------------------------------------------------------------------------
# Path similarity helpers
# ---------------------------------------------------------------------------

def downsample(points: list[dict], n: int) -> list[dict]:
    """
    Evenly reduce a list of {lat, lon} dicts to n points by index stride.
    Always includes the first and last points.
    If len(points) <= n, returns the original list unchanged.
    """
    if len(points) <= n:
        return points
    indices = [round(i * (len(points) - 1) / (n - 1)) for i in range(n)]
    seen = set()
    result = []
    for idx in indices:
        if idx not in seen:
            seen.add(idx)
            result.append(points[idx])
    return result


def mean_nearest_deviation_m(
    exercise_pts: list[dict],
    route_pts: list[dict],
) -> float:
    """
    For each point in exercise_pts, find the distance to the nearest point in
    route_pts. Returns the mean of those nearest-neighbor distances in meters.

    Both lists should already be downsampled before calling this.
    """
    total = 0.0
    for ep in exercise_pts:
        nearest = min(
            haversine_m(ep["lat"], ep["lon"], rp["lat"], rp["lon"])
            for rp in route_pts
        )
        total += nearest
    return total / len(exercise_pts)


# ---------------------------------------------------------------------------
# Two-stage matching pipeline
# ---------------------------------------------------------------------------

def match_exercises_to_routes(
    exercises: list[dict],
    first_trackpoints: dict[int, dict],
    all_trackpoints: dict[int, list[dict]],
    routes: list[dict],
    route_gps_paths: dict[int, list[dict]],
    radius_m: float,
    path_samples: int,
    path_threshold_m: float,
    force_rematch: bool,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Two-stage matching pipeline for each exercise:

      Stage 1 — Start-point proximity gate:
        Uses first_trackpoints to find all routes whose start_lat/start_long
        is within radius_m of the exercise's start. If none, exercise is
        unmatched immediately (cheap exit).

      Stage 2 — Path similarity (downsampled Hausdorff-style):
        For each candidate route from Stage 1 that has a gps_path, downsample
        both the exercise's full trackpoints and the route's gps_path to
        path_samples points, then compute mean nearest-neighbor deviation.
        The route with the lowest mean deviation wins, provided it is under
        path_threshold_m. Routes without gps_path fall back to start-point
        distance as their score (treated as a weaker match).

    Returns:
        matched_new      — exercises newly matched (need write-back)
        matched_existing — exercises that already had route_id (confirmed/conflict)
        unmatched        — exercises with no trackpoint or no qualifying route
    """
    matched_new      = []
    matched_existing = []
    unmatched        = []

    for ex in exercises:
        ex_id     = ex["exercise_id"]
        has_route = ex["route_id"] is not None
        fp        = first_trackpoints.get(ex_id)

        if fp is None:
            ex["_match_status"] = "no_trackpoint"
            unmatched.append(ex)
            continue

        # --- Stage 1: proximity gate ---
        candidates = []
        for route in routes:
            d = haversine_m(fp["lat"], fp["lon"],
                            float(route["start_lat"]), float(route["start_long"]))
            if d <= radius_m:
                candidates.append((route, d))

        if not candidates:
            ex["_match_status"]  = "no_match"
            ex["_matched_route"] = None
            unmatched.append(ex)
            continue

        # --- Stage 2: path similarity ---
        ex_pts_full = all_trackpoints.get(ex_id, [])
        ex_pts      = downsample(ex_pts_full, path_samples) if ex_pts_full else []

        best_route    = None
        best_score    = float("inf")
        best_method   = "proximity"

        for route, start_dist in candidates:
            r_id      = route["route_id"]
            route_pts = route_gps_paths.get(r_id)

            if route_pts and ex_pts:
                # Path similarity available — use it
                r_pts_ds = downsample(route_pts, path_samples)
                score    = mean_nearest_deviation_m(ex_pts, r_pts_ds)
                method   = "path"
            else:
                # No gps_path on route or no exercise trackpoints — fall back
                # to start distance as score; won't beat a real path match
                score  = start_dist
                method = "proximity"

            if score < best_score:
                best_score  = score
                best_route  = route
                best_method = method

        # Reject if best path score exceeds threshold
        if best_method == "path" and best_score > path_threshold_m:
            ex["_match_status"]  = "no_match"
            ex["_matched_route"] = None
            ex["_path_score"]    = best_score
            unmatched.append(ex)
            continue

        ex["_matched_route"] = best_route
        ex["_match_score"]   = best_score
        ex["_match_method"]  = best_method

        if has_route and not force_rematch:
            if ex["route_id"] != best_route["route_id"]:
                ex["_match_status"] = "conflict"
                print(f"  [WARN] exercise_id={ex_id} ({ex['activity_date']}): "
                      f"stored route_id={ex['route_id']} but {best_method} matching "
                      f"suggests route_id={best_route['route_id']} "
                      f"({best_route['name']}, score={best_score:.1f}m). "
                      f"Use --force-rematch to overwrite.")
            else:
                ex["_match_status"] = "confirmed"
            matched_existing.append(ex)
        else:
            ex["_match_status"] = "new_match"
            matched_new.append(ex)

    return matched_new, matched_existing, unmatched


# ---------------------------------------------------------------------------
# Report building
# ---------------------------------------------------------------------------

def build_frequency_report(
    exercises: list[dict],
    routes_by_id: dict[int, dict],
) -> list[dict]:
    """
    Aggregate matched exercises by route_id.
    Uses _matched_route if present, otherwise falls back to stored route_id.
    """
    buckets = defaultdict(list)

    for ex in exercises:
        matched = ex.get("_matched_route")
        if matched:
            r_id = matched["route_id"]
        elif ex.get("route_id"):
            r_id = ex["route_id"]
        else:
            continue
        buckets[r_id].append(ex)

    rows = []
    for r_id, exs in buckets.items():
        route = routes_by_id.get(r_id, {})
        dates = sorted(e["activity_date"] for e in exs)

        distances = [float(e["distance_miles"]) for e in exs if e["distance_miles"]]
        durations = [float(e["duration_minutes"]) for e in exs if e["duration_minutes"]]
        hrs        = [float(e["average_heart_rate"]) for e in exs if e["average_heart_rate"]]

        # Avg pace in min/mi
        pace_entries = [
            (float(e["duration_minutes"]) / float(e["distance_miles"]))
            for e in exs
            if e["duration_minutes"] and e["distance_miles"]
            and float(e["distance_miles"]) > 0
        ]

        avg_pace_raw = sum(pace_entries) / len(pace_entries) if pace_entries else None
        if avg_pace_raw:
            pace_min  = int(avg_pace_raw)
            pace_sec  = round((avg_pace_raw - pace_min) * 60)
            avg_pace  = f"{pace_min}:{pace_sec:02d}/mi"
        else:
            avg_pace  = "—"

        rows.append({
            "route_id":         r_id,
            "route_name":       route.get("name", f"route_id={r_id}"),
            "run_count":        len(exs),
            "first_run":        dates[0],
            "last_run":         dates[-1],
            "total_distance_mi": round(sum(distances), 1) if distances else None,
            "avg_distance_mi":  round(sum(distances) / len(distances), 2) if distances else None,
            "avg_pace":         avg_pace,
            "avg_hr":           round(sum(hrs) / len(hrs), 1) if hrs else None,
        })

    return sorted(rows, key=lambda r: r["run_count"], reverse=True)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def print_report(rows: list[dict], mode: str, limit: int | None) -> None:
    if not rows:
        print("\nNo matched routes found.")
        return

    display = rows[:limit] if limit else rows

    if mode == "single_date":
        print(f"\n{'Route':<45} {'Dist':>6}  {'Pace':>8}  {'Avg HR':>6}  {'Match':>12}")
        print("-" * 85)
        for r in display:
            dist  = f"{r['avg_distance_mi']} mi" if r["avg_distance_mi"] else "—"
            hr    = f"{r['avg_hr']} bpm"          if r["avg_hr"] else "—"
            print(f"  {r['route_name']:<43} {dist:>6}  {r['avg_pace']:>8}  {hr:>6}")
    else:
        print(f"\n{'#':>3}  {'Route':<40} {'Runs':>5}  {'Total mi':>8}  "
              f"{'Avg mi':>7}  {'Avg Pace':>9}  {'Avg HR':>7}  "
              f"{'First Run':<12}  {'Last Run'}")
        print("-" * 110)
        for i, r in enumerate(display, 1):
            total = f"{r['total_distance_mi']}" if r["total_distance_mi"] else "—"
            avg   = f"{r['avg_distance_mi']}"   if r["avg_distance_mi"] else "—"
            hr    = f"{r['avg_hr']}"             if r["avg_hr"] else "—"
            print(f"  {i:>2}.  {r['route_name']:<40} {r['run_count']:>5}  "
                  f"{total:>8}  {avg:>7}  {r['avg_pace']:>9}  {hr:>7}  "
                  f"{str(r['first_run']):<12}  {r['last_run']}")


def write_csv(rows: list[dict], output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fieldnames = [
        "route_id", "route_name", "run_count",
        "first_run", "last_run",
        "total_distance_mi", "avg_distance_mi",
        "avg_pace", "avg_hr",
    ]
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n  [csv] Written to {output_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Analyze how often specific routes have been run."
    )
    parser.add_argument(
        "--type", default="running",
        help="Activity type: running | cycling | hiking | all (default: running)"
    )

    # Date filters — mutually exclusive groups
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        "--date", default=None, metavar="YYYY-MM-DD",
        help="Single-day lookup: show routes run on this date"
    )
    date_group.add_argument(
        "--date-from", default=None, metavar="YYYY-MM-DD",
        help="Filter: exercises on or after this date"
    )

    parser.add_argument(
        "--date-to", default=None, metavar="YYYY-MM-DD",
        help="Filter: exercises on or before this date (used with --date-from)"
    )
    parser.add_argument(
        "--radius", type=float, default=100.0,
        help="Stage 1 proximity gate radius in meters (default: 100)"
    )
    parser.add_argument(
        "--path-samples", type=int, default=25,
        help="Points to downsample to for path similarity comparison (default: 25)"
    )
    parser.add_argument(
        "--path-threshold", type=float, default=75.0,
        help="Max mean deviation in meters to accept a path match (default: 75)"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Cap output to top N routes by run count"
    )
    parser.add_argument(
        "--force-rematch", action="store_true",
        help="Overwrite existing route_id matches with proximity result"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Match and report without writing back to the DB"
    )
    parser.add_argument(
        "--no-update", action="store_true",
        help="Skip write-back entirely; just report"
    )
    parser.add_argument(
        "--output", default=None,
        help="CSV export path (default: scripts/output/route_frequency_YYYYMMDD.csv)"
    )

    args = parser.parse_args()

    # Validate --date-to without --date-from
    if args.date_to and not args.date_from:
        parser.error("--date-to requires --date-from")

    # Resolve output path
    today_str   = date.today().strftime("%Y%m%d")
    output_path = args.output or os.path.join(
        os.path.dirname(__file__), "output", f"route_frequency_{today_str}.csv"
    )

    # Determine mode
    single_date_mode = args.date is not None
    mode = "single_date" if single_date_mode else "frequency"

    # Build date filter dict
    date_filter = {}
    if single_date_mode:
        date_filter["single_date"] = args.date
    else:
        if args.date_from:
            date_filter["date_from"] = args.date_from
        if args.date_to:
            date_filter["date_to"] = args.date_to

    suppress_write = args.dry_run or args.no_update

    # --- Summary of run params ---
    print("\n=== Route Frequency Analysis ===")
    print(f"  Mode          : {'Single-day lookup' if single_date_mode else 'Frequency report'}")
    print(f"  Activity type : {args.type}")
    if single_date_mode:
        print(f"  Date          : {args.date}")
    else:
        print(f"  Date from     : {args.date_from or '(all)'}")
        print(f"  Date to       : {args.date_to or '(all)'}")
    print(f"  Radius        : {args.radius}m")
    print(f"  Path samples  : {args.path_samples}")
    print(f"  Path threshold: {args.path_threshold}m")
    print(f"  Limit         : {args.limit or 'none'}")
    print(f"  Force rematch : {args.force_rematch}")
    print(f"  Write-back    : {'disabled (dry-run)' if args.dry_run else 'disabled (--no-update)' if args.no_update else 'enabled'}")

    # --- Connect ---
    conn = get_connection()

    try:
        # --- Load data ---
        print("\nLoading routes...")
        routes = load_routes(conn)
        print(f"  {len(routes)} active routes loaded")

        if not routes:
            print("No routes found. Ingest some GPX routes first.")
            return

        routes_by_id = {r["route_id"]: r for r in routes}

        print("Loading exercises...")
        exercises = load_exercises(conn, args.type, date_filter)
        print(f"  {len(exercises)} exercises loaded")

        if not exercises:
            print("No exercises found for the given filters.")
            return

        # --- Load trackpoints ---
        exercise_ids = [e["exercise_id"] for e in exercises]
        print("Loading first trackpoints (Stage 1 gate)...")
        first_tps = load_first_trackpoints(conn, exercise_ids)
        print(f"  {len(first_tps)} exercises have a GPS start point")

        print("Loading full trackpoints (Stage 2 path similarity)...")
        all_tps = load_all_trackpoints(conn, exercise_ids)
        print(f"  {len(all_tps)} exercises have full GPS tracks")

        # --- Load route GPS paths for candidates ---
        route_ids_with_paths = [r["route_id"] for r in routes]
        print("Loading route GPS paths...")
        route_gps_paths = load_route_gps_paths(conn, route_ids_with_paths)
        print(f"  {len(route_gps_paths)} routes have a stored gps_path")

        # --- Two-stage matching ---
        print(f"\nRunning two-stage matching "
              f"(radius={args.radius}m, samples={args.path_samples}, "
              f"threshold={args.path_threshold}m)...")
        matched_new, matched_existing, unmatched = match_exercises_to_routes(
            exercises,
            first_tps,
            all_tps,
            routes,
            route_gps_paths,
            args.radius,
            args.path_samples,
            args.path_threshold,
            args.force_rematch,
        )

        print(f"  New matches         : {len(matched_new)}")
        print(f"  Confirmed (existing): {len(matched_existing)}")
        print(f"  Unmatched           : {len(unmatched)}")

        no_tp   = sum(1 for e in unmatched if e.get("_match_status") == "no_trackpoint")
        no_hit  = sum(1 for e in unmatched if e.get("_match_status") == "no_match")
        if no_tp:
            print(f"    → {no_tp} without GPS trackpoints")
        if no_hit:
            print(f"    → {no_hit} outside radius or above path threshold")

        # Show path scores for new matches (diagnostic)
        path_matches     = [e for e in matched_new if e.get("_match_method") == "path"]
        fallback_matches = [e for e in matched_new if e.get("_match_method") == "proximity"]
        if path_matches:
            print(f"    → {len(path_matches)} matched via path similarity")
        if fallback_matches:
            print(f"    → {len(fallback_matches)} matched via start-point fallback (no gps_path on route)")

        # --- Write-back ---
        if matched_new and not suppress_write:
            write_payloads = [
                {
                    "exercise_id": e["exercise_id"],
                    "route_id":    e["_matched_route"]["route_id"],
                    "score":       e["_match_score"],
                    "method":      e["_match_method"],
                }
                for e in matched_new
            ]
            updated = write_back_matches(conn, write_payloads)
            print(f"\n  [db] Updated {updated} exercise rows with route_id")
        elif matched_new and suppress_write:
            print(f"\n  [dry-run/no-update] Would have updated {len(matched_new)} exercise rows")

        # --- Build report ---
        all_matched = matched_new + matched_existing
        report_rows = build_frequency_report(all_matched, routes_by_id)

        # --- Output ---
        if single_date_mode:
            print(f"\n--- Routes run on {args.date} ---")
        else:
            print(f"\n--- Route Frequency Report ---")

        print_report(report_rows, mode, args.limit)

        if not single_date_mode:
            write_csv(report_rows[:args.limit] if args.limit else report_rows, output_path)

    finally:
        conn.close()

    print("\nDone.")


if __name__ == "__main__":
    main()
