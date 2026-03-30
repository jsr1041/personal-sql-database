#!/usr/bin/env python3
"""
scripts/ingest_gpx_route.py

Ingests a GPX file as a named route into the route table.
- Parses trackpoints, calculates distance (haversine) and elevation gain
- Derives start/end coordinates from first and last trackpoints
- Stores full GPS path as JSONB array of {lat, lon, ele} objects
- Captures activity_type from GPX <type> tag (e.g. running, trail_running, cycling)
- Upserts on name match; inserts new row if no match found

Usage:
    python scripts/ingest_gpx_route.py --file path/to/route.gpx
    python scripts/ingest_gpx_route.py --file path/to/route.gpx --surface road
    python scripts/ingest_gpx_route.py --file path/to/route.gpx --dry-run

Options:
    --file          Path to the GPX file (required)
    --name          Override the route name (default: taken from GPX <name> tag)
    --surface       Surface type: road | trail | mixed (default: None, stored as NULL)
    --description   Optional description text
    --notes         Optional notes text
    --dry-run       Preview parsed values without writing to the database

Requires: DB_HOST, DB_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD in .env
"""

import os
import json
import math
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

VALID_SURFACES = {"road", "trail", "mixed"}


# ---------------------------------------------------------------------------
# GPX parsing
# ---------------------------------------------------------------------------

def parse_gpx(file_path: str) -> dict:
    """
    Parse a GPX file and return a dict of derived route values.

    Returns:
        name            : str
        activity_type   : str | None  (from GPX <type> tag, e.g. 'running', 'trail_running')
        distance_miles  : float
        elevation_gain_feet : float
        start_lat       : float
        start_long      : float
        end_lat         : float | None  (None if start ≈ end, i.e. loop)
        end_long        : float | None
        gps_path        : list[dict]  [{lat, lon, ele}, ...]
        trackpoint_count: int
    """
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Route name — prefer <trk><name>, fall back to <metadata><name>
    trk_name = root.find(".//gpx:trk/gpx:name", GPX_NS)
    meta_name = root.find(".//gpx:metadata/gpx:name", GPX_NS)
    name = (trk_name or meta_name)
    name = name.text.strip() if name is not None else os.path.splitext(os.path.basename(file_path))[0]

    # Activity type — from <trk><type>
    trk_type = root.find(".//gpx:trk/gpx:type", GPX_NS)
    activity_type = trk_type.text.strip() if trk_type is not None else None

    # Collect trackpoints
    trkpts = root.findall(".//gpx:trkpt", GPX_NS)
    if not trkpts:
        raise ValueError("GPX file contains no trackpoints.")

    points = []
    for pt in trkpts:
        lat = float(pt.get("lat"))
        lon = float(pt.get("lon"))
        ele_el = pt.find("gpx:ele", GPX_NS)
        ele = float(ele_el.text) if ele_el is not None else None
        points.append({"lat": round(lat, 6), "lon": round(lon, 6), "ele": round(ele, 2) if ele is not None else None})

    # Distance — haversine sum
    total_dist_m = sum(
        _haversine(points[i]["lat"], points[i]["lon"], points[i+1]["lat"], points[i+1]["lon"])
        for i in range(len(points) - 1)
    )
    distance_miles = round(total_dist_m / 1609.344, 2)

    # Elevation gain — sum of positive elevation deltas
    elevations = [p["ele"] for p in points if p["ele"] is not None]
    if len(elevations) >= 2:
        gain_m = sum(
            max(0.0, elevations[i+1] - elevations[i])
            for i in range(len(elevations) - 1)
        )
        elevation_gain_feet = round(gain_m * 3.28084, 1)
    else:
        elevation_gain_feet = None

    # Start / end coordinates
    start_lat  = points[0]["lat"]
    start_long = points[0]["lon"]

    # Treat as loop if start and end are within ~100m of each other
    end_dist_m = _haversine(points[0]["lat"], points[0]["lon"], points[-1]["lat"], points[-1]["lon"])
    if end_dist_m <= 100:
        end_lat  = None
        end_long = None
    else:
        end_lat  = points[-1]["lat"]
        end_long = points[-1]["lon"]

    return {
        "name":                 name,
        "activity_type":        activity_type,
        "distance_miles":       distance_miles,
        "elevation_gain_feet":  elevation_gain_feet,
        "start_lat":            start_lat,
        "start_long":           start_long,
        "end_lat":              end_lat,
        "end_long":             end_long,
        "gps_path":             points,
        "trackpoint_count":     len(points),
    }


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Returns distance in meters between two lat/lon points."""
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
    """Returns route_id if a non-deleted route with this name already exists, else None."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT route_id FROM route WHERE name = %s AND is_deleted = FALSE",
            (name,)
        )
        row = cur.fetchone()
        return row[0] if row else None


def insert_route(conn, route: dict, now: datetime) -> int:
    """Insert a new route row. Returns the new route_id."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO route (
                name, description,
                activity_type,
                distance_miles, elevation_gain_feet,
                surface_type,
                start_lat, start_long,
                end_lat, end_long,
                gps_path,
                notes,
                created_at, updated_at, is_deleted
            ) VALUES (
                %s, %s,
                %s,
                %s, %s,
                %s,
                %s, %s,
                %s, %s,
                %s,
                %s,
                %s, %s, FALSE
            )
            RETURNING route_id
        """, (
            route["name"],
            route.get("description"),
            route.get("activity_type"),
            route["distance_miles"],
            route["elevation_gain_feet"],
            route.get("surface_type"),
            route["start_lat"],
            route["start_long"],
            route["end_lat"],
            route["end_long"],
            json.dumps(route["gps_path"]),
            route.get("notes"),
            now, now,
        ))
        route_id = cur.fetchone()[0]
        conn.commit()
        return route_id


def update_route(conn, route_id: int, route: dict, now: datetime) -> None:
    """Update an existing route row, preserving existing description/notes/surface if not provided."""
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
                description         = COALESCE(%s, description),
                notes               = COALESCE(%s, notes),
                updated_at          = %s
            WHERE route_id = %s
        """, (
            route.get("activity_type"),
            route["distance_miles"],
            route["elevation_gain_feet"],
            route.get("surface_type"),
            route["start_lat"],
            route["start_long"],
            route["end_lat"],
            route["end_long"],
            json.dumps(route["gps_path"]),
            route.get("description"),
            route.get("notes"),
            now,
            route_id,
        ))
        conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Ingest a GPX file as a route record.")
    parser.add_argument("--file",        required=True, help="Path to the GPX file")
    parser.add_argument("--name",        default=None,  help="Override route name (default: from GPX)")
    parser.add_argument("--surface",     default=None,  choices=list(VALID_SURFACES),
                        help="Surface type: road | trail | mixed")
    parser.add_argument("--description", default=None,  help="Optional description")
    parser.add_argument("--notes",       default=None,  help="Optional notes")
    parser.add_argument("--dry-run",     action="store_true",
                        help="Preview parsed values without writing to the database")
    args = parser.parse_args()

    # --- Parse GPX ---
    print(f"Parsing: {args.file}")
    parsed = parse_gpx(args.file)

    # Apply CLI overrides
    if args.name:
        parsed["name"] = args.name
    parsed["surface_type"] = args.surface
    parsed["description"]  = args.description
    parsed["notes"]        = args.notes

    # --- Preview ---
    print("\n--- Parsed Route Values ---")
    print(f"  Name             : {parsed['name']}")
    print(f"  Activity type    : {parsed['activity_type'] or 'NULL'}")
    print(f"  Distance         : {parsed['distance_miles']} miles")
    print(f"  Elevation gain   : {parsed['elevation_gain_feet']} ft")
    print(f"  Surface type     : {parsed['surface_type'] or 'NULL'}")
    print(f"  Start            : {parsed['start_lat']}, {parsed['start_long']}")
    if parsed["end_lat"] is not None:
        print(f"  End              : {parsed['end_lat']}, {parsed['end_long']}")
    else:
        print(f"  End              : NULL (loop — start ≈ end)")
    print(f"  GPS path points  : {parsed['trackpoint_count']}")
    print(f"  Description      : {parsed['description'] or 'NULL'}")
    print(f"  Notes            : {parsed['notes'] or 'NULL'}")

    if args.dry_run:
        print("\n--- DRY RUN — no changes written ---")
        return

    # --- Database ---
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )

    now = datetime.now(timezone.utc)
    existing_id = check_existing(conn, parsed["name"])

    if existing_id:
        update_route(conn, existing_id, parsed, now)
        print(f"\n  [updated] route_id={existing_id} — \"{parsed['name']}\"")
    else:
        new_id = insert_route(conn, parsed, now)
        print(f"\n  [inserted] route_id={new_id} — \"{parsed['name']}\"")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
