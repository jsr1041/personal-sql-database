#!/usr/bin/env python3
"""
scripts/bulk_ingest_gpx_routes.py

Bulk-ingests a directory of GPX files into the route table.
- Processes all .gpx files in the specified directory
- Normalizes activity_type: 'trail_running' → 'Trail', 'running' → 'Road'
- Derives surface_type from normalized activity_type (road → road, trail → trail)
- Upserts on name match; inserts new rows if no match found
- Logs skipped/failed files without stopping the run
- Prints a summary report on completion

Usage:
    python scripts/bulk_ingest_gpx_routes.py --dir path/to/gpx/folder
    python scripts/bulk_ingest_gpx_routes.py --dir path/to/gpx/folder --dry-run
    python scripts/bulk_ingest_gpx_routes.py --dir path/to/gpx/folder --limit 10

Options:
    --dir       Directory containing .gpx files (required)
    --dry-run   Parse and preview all files without writing to the database
    --limit     Process only the first N files (useful for test runs)

Requires: DB_HOST, DB_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD in .env
"""

import os
import json
import math
import glob
import argparse
import xml.etree.ElementTree as ET
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("POSTGRES_DB")
DB_USER     = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

GPX_NS = {"gpx": "http://www.topografix.com/GPX/1/1"}

# Maps raw GPX <type> values to normalized activity_type and surface_type
ACTIVITY_TYPE_MAP = {
    "running":       {"activity_type": "Road",  "surface_type": "road"},
    "trail_running": {"activity_type": "Trail", "surface_type": "trail"},
    "cycling":       {"activity_type": "cycling", "surface_type": "road"},
    "mountain_biking": {"activity_type": "mountain_biking", "surface_type": "trail"},
    "hiking":        {"activity_type": "hiking", "surface_type": "trail"},
    "walking":       {"activity_type": "walking", "surface_type": "road"},
}


# ---------------------------------------------------------------------------
# GPX parsing
# ---------------------------------------------------------------------------

def parse_gpx(file_path: str) -> dict:
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Name — prefer <trk><name>, fall back to <metadata><name>, then filename
    trk_name  = root.find(".//gpx:trk/gpx:name", GPX_NS)
    meta_name = root.find(".//gpx:metadata/gpx:name", GPX_NS)
    name_el   = trk_name or meta_name
    name      = name_el.text.strip() if name_el is not None else os.path.splitext(os.path.basename(file_path))[0]

    # Raw activity type from <trk><type>
    trk_type     = root.find(".//gpx:trk/gpx:type", GPX_NS)
    raw_type     = trk_type.text.strip().lower() if trk_type is not None else None
    type_mapping = ACTIVITY_TYPE_MAP.get(raw_type, {})
    activity_type = type_mapping.get("activity_type", raw_type)   # fall back to raw if not in map
    surface_type  = type_mapping.get("surface_type", None)

    # Trackpoints
    trkpts = root.findall(".//gpx:trkpt", GPX_NS)
    if not trkpts:
        raise ValueError("No trackpoints found.")

    points = []
    for pt in trkpts:
        lat    = float(pt.get("lat"))
        lon    = float(pt.get("lon"))
        ele_el = pt.find("gpx:ele", GPX_NS)
        ele    = float(ele_el.text) if ele_el is not None else None
        points.append({
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "ele": round(ele, 2) if ele is not None else None,
        })

    # Distance (haversine)
    total_dist_m = sum(
        _haversine(points[i]["lat"], points[i]["lon"], points[i+1]["lat"], points[i+1]["lon"])
        for i in range(len(points) - 1)
    )
    distance_miles = round(total_dist_m / 1609.344, 2)

    # Elevation gain
    elevations = [p["ele"] for p in points if p["ele"] is not None]
    if len(elevations) >= 2:
        gain_m = sum(max(0.0, elevations[i+1] - elevations[i]) for i in range(len(elevations) - 1))
        elevation_gain_feet = round(gain_m * 3.28084, 1)
    else:
        elevation_gain_feet = None

    # Start / end — treat as loop if within 100m
    start_lat  = points[0]["lat"]
    start_long = points[0]["lon"]
    end_dist_m = _haversine(start_lat, start_long, points[-1]["lat"], points[-1]["lon"])
    if end_dist_m <= 100:
        end_lat, end_long = None, None
    else:
        end_lat  = points[-1]["lat"]
        end_long = points[-1]["lon"]

    return {
        "name":                name,
        "raw_type":            raw_type,
        "activity_type":       activity_type,
        "surface_type":        surface_type,
        "distance_miles":      distance_miles,
        "elevation_gain_feet": elevation_gain_feet,
        "start_lat":           start_lat,
        "start_long":          start_long,
        "end_lat":             end_lat,
        "end_long":            end_long,
        "gps_path":            points,
        "trackpoint_count":    len(points),
    }


def _haversine(lat1, lon1, lat2, lon2):
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi       = math.radians(lat2 - lat1)
    dlambda    = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

def check_existing(conn, name: str) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT route_id FROM route WHERE name = %s AND is_deleted = FALSE",
            (name,)
        )
        row = cur.fetchone()
        return row[0] if row else None


def insert_route(conn, route: dict, now: datetime) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO route (
                name,
                activity_type,
                distance_miles, elevation_gain_feet,
                surface_type,
                start_lat, start_long,
                end_lat, end_long,
                gps_path,
                created_at, updated_at, is_deleted
            ) VALUES (
                %s,
                %s,
                %s, %s,
                %s,
                %s, %s,
                %s, %s,
                %s,
                %s, %s, FALSE
            )
            RETURNING route_id
        """, (
            route["name"],
            route["activity_type"],
            route["distance_miles"],
            route["elevation_gain_feet"],
            route["surface_type"],
            route["start_lat"],
            route["start_long"],
            route["end_lat"],
            route["end_long"],
            json.dumps(route["gps_path"]),
            now, now,
        ))
        route_id = cur.fetchone()[0]
        conn.commit()
        return route_id


def update_route(conn, route_id: int, route: dict, now: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE route SET
                activity_type       = COALESCE(%s, activity_type),
                distance_miles      = %s,
                elevation_gain_feet = %s,
                surface_type        = COALESCE(%s, surface_type),
                start_lat           = %s,
                start_long          = %s,
                end_lat             = %s,
                end_long            = %s,
                gps_path            = %s,
                updated_at          = %s
            WHERE route_id = %s
        """, (
            route["activity_type"],
            route["distance_miles"],
            route["elevation_gain_feet"],
            route["surface_type"],
            route["start_lat"],
            route["start_long"],
            route["end_lat"],
            route["end_long"],
            json.dumps(route["gps_path"]),
            now,
            route_id,
        ))
        conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Bulk ingest GPX files into the route table.")
    parser.add_argument("--dir",     required=True, help="Directory containing .gpx files")
    parser.add_argument("--dry-run", action="store_true", help="Parse files without writing to the database")
    parser.add_argument("--limit",   type=int, default=None, help="Process only the first N files")
    args = parser.parse_args()

    gpx_files = sorted(glob.glob(os.path.join(args.dir, "*.gpx")))
    if not gpx_files:
        print(f"No .gpx files found in: {args.dir}")
        return

    if args.limit:
        gpx_files = gpx_files[:args.limit]

    total = len(gpx_files)
    print(f"Found {total} GPX file(s) to process.")
    if args.dry_run:
        print("--- DRY RUN — no changes will be written ---\n")

    conn = None
    if not args.dry_run:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD
        )

    results   = {"inserted": [], "updated": [], "skipped": [], "failed": []}
    unmapped  = set()   # raw_type values not in ACTIVITY_TYPE_MAP

    for i, file_path in enumerate(gpx_files, start=1):
        filename = os.path.basename(file_path)
        try:
            parsed = parse_gpx(file_path)
        except Exception as e:
            print(f"  [{i}/{total}] FAILED  {filename} — {e}")
            results["failed"].append(filename)
            continue

        # Track unmapped types for the summary
        if parsed["raw_type"] and parsed["raw_type"] not in ACTIVITY_TYPE_MAP:
            unmapped.add(parsed["raw_type"])

        if args.dry_run:
            loop_flag = " [loop]" if parsed["end_lat"] is None else ""
            print(
                f"  [{i}/{total}] {parsed['name']}"
                f" | {parsed['activity_type'] or 'NULL'}"
                f" | {parsed['distance_miles']} mi"
                f" | {parsed['elevation_gain_feet']} ft gain"
                f"{loop_flag}"
            )
            results["inserted"].append(parsed["name"])  # count as "would insert" in dry run
            continue

        try:
            now = datetime.now(timezone.utc)
            existing_id = check_existing(conn, parsed["name"])
            if existing_id:
                update_route(conn, existing_id, parsed, now)
                print(f"  [{i}/{total}] updated   route_id={existing_id} — {parsed['name']}")
                results["updated"].append(parsed["name"])
            else:
                new_id = insert_route(conn, parsed, now)
                print(f"  [{i}/{total}] inserted  route_id={new_id} — {parsed['name']}")
                results["inserted"].append(parsed["name"])
        except Exception as e:
            print(f"  [{i}/{total}] FAILED  {filename} — DB error: {e}")
            results["failed"].append(filename)
            if conn:
                conn.rollback()

    if conn:
        conn.close()

    # --- Summary ---
    print("\n" + "=" * 55)
    print("BULK INGEST SUMMARY")
    print("=" * 55)
    if args.dry_run:
        print(f"  Would process : {len(results['inserted'])} route(s) (dry run)")
    else:
        print(f"  Inserted      : {len(results['inserted'])}")
        print(f"  Updated       : {len(results['updated'])}")
        print(f"  Failed        : {len(results['failed'])}")

    if results["failed"]:
        print("\n  Failed files:")
        for f in results["failed"]:
            print(f"    - {f}")

    if unmapped:
        print(f"\n  ⚠ Unmapped activity types (stored as-is, no surface_type set):")
        for t in sorted(unmapped):
            print(f"    - {t}")

    print("=" * 55)


if __name__ == "__main__":
    main()
