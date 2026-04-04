#!/usr/bin/env python3
"""
patch_tcx_sport.py
------------------
Patches the garmin_manifest.csv to correct TCX files where the Garmin device
(notably the Forerunner 305) incorrectly exported cycling activities as
Sport="Running".

Uses average speed to classify:
  > 12 mph  → cycling (impossible running pace — auto-corrected)
  8–12 mph  → flagged as review_speed_boundary (left as-is)
  no dist   → flagged as review_no_distance (left as-is)
  < 8 mph   → confirmed running (no change)

Adds a sport_override column to the manifest. The batch_ingest_exercise.py
script uses sport_override over the parsed sport value when writing
type_of_activity to the exercise table.

Usage:
  python patch_tcx_sport.py --manifest garmin_manifest.csv [--dry-run]

Author: John Radcliffe
"""

import os
import sys
import csv
import argparse
from collections import Counter

CYCLING_SPEED_THRESHOLD_MPH = 12.0   # above this → unambiguously cycling
BOUNDARY_SPEED_THRESHOLD_MPH = 8.0   # 8–12 mph → flag for review


def compute_speed_mph(duration_seconds, distance_m):
    """Return average speed in mph, or None if data is missing/zero."""
    if not duration_seconds or not distance_m:
        return None
    try:
        dur  = float(duration_seconds)
        dist = float(distance_m)
    except (ValueError, TypeError):
        return None
    if dur <= 0 or dist <= 0:
        return None
    dist_mi   = dist / 1609.344
    speed_mph = dist_mi / (dur / 3600)
    return speed_mph


def main():
    parser = argparse.ArgumentParser(
        description="Patch garmin_manifest.csv to fix misclassified TCX cycling files."
    )
    parser.add_argument("--manifest", required=True,
                        help="Path to garmin_manifest.csv")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would change without writing anything")
    args = parser.parse_args()

    if not os.path.exists(args.manifest):
        print(f"ERROR: manifest not found — {args.manifest}")
        sys.exit(1)

    with open(args.manifest, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        original_fields = reader.fieldnames
        rows = list(reader)

    # Add sport_override column if not already present
    fieldnames = list(original_fields)
    if "sport_override" not in fieldnames:
        # Insert after 'sport' column
        sport_idx = fieldnames.index("sport") + 1
        fieldnames.insert(sport_idx, "sport_override")
        for row in rows:
            row["sport_override"] = ""

    # ── Process TCX exercise rows tagged as running ──────────────────────────
    counters = Counter()

    for row in rows:
        if row.get("file_format") != "tcx":
            continue
        if row.get("destination") != "exercise" and row.get("override_destination") != "exercise":
            continue
        if row.get("sport") != "running":
            continue

        speed = compute_speed_mph(row.get("duration_seconds"), row.get("distance_m"))

        if speed is None:
            # No usable distance data — flag for review
            row["notes"] = _append_note(row.get("notes", ""), "review_no_distance")
            counters["flagged_no_distance"] += 1

        elif speed > CYCLING_SPEED_THRESHOLD_MPH:
            # Unambiguously cycling
            row["sport_override"] = "cycling"
            row["notes"] = _append_note(
                row.get("notes", ""),
                f"auto_reclassified_cycling:{speed:.1f}mph"
            )
            counters["reclassified_cycling"] += 1

        elif speed >= BOUNDARY_SPEED_THRESHOLD_MPH:
            # Ambiguous — flag for manual review, don't change sport
            row["notes"] = _append_note(
                row.get("notes", ""),
                f"review_speed_boundary:{speed:.1f}mph"
            )
            counters["flagged_boundary"] += 1

        else:
            # < 8 mph — plausible running, no change needed
            counters["confirmed_running"] += 1

    # ── Report ───────────────────────────────────────────────────────────────
    total_affected = sum(counters.values())
    print(f"\n{'=' * 60}")
    print(f"  patch_tcx_sport.py  {'(DRY RUN)' if args.dry_run else ''}")
    print(f"{'=' * 60}")
    print(f"  Manifest: {args.manifest}")
    print(f"  Total TCX 'running' rows evaluated: {total_affected}")
    print()
    print(f"  {'auto_reclassified → cycling':35s}: {counters['reclassified_cycling']}")
    print(f"  {'flagged → review_speed_boundary':35s}: {counters['flagged_boundary']}")
    print(f"  {'flagged → review_no_distance':35s}: {counters['flagged_no_distance']}")
    print(f"  {'confirmed running (no change)':35s}: {counters['confirmed_running']}")

    if args.dry_run:
        print(f"\n  ⚠  DRY RUN — manifest not modified")

        # Preview the reclassified rows
        reclassified = [
            r for r in rows
            if r.get("file_format") == "tcx"
            and r.get("sport_override") == "cycling"
        ]
        if reclassified:
            print(f"\n  Preview of auto-reclassified files (first 10):")
            print(f"  {'filename':40s} | {'speed':>8} | {'distance':>10} | {'duration':>10}")
            print(f"  {'-'*80}")
            for r in reclassified[:10]:
                spd = compute_speed_mph(r.get("duration_seconds"), r.get("distance_m"))
                dist_mi = float(r["distance_m"]) / 1609.344 if r.get("distance_m") else 0
                dur_min = float(r["duration_seconds"]) / 60 if r.get("duration_seconds") else 0
                print(f"  {r['filename']:40s} | {spd:>6.1f}mph | {dist_mi:>8.1f} mi | {dur_min:>8.0f} min")
        print()
        return

    # ── Write patched manifest ───────────────────────────────────────────────
    with open(args.manifest, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n  ✓ Manifest updated: {args.manifest}")
    print(f"\n  Next steps:")
    print(f"  1. Review rows with notes containing 'review_speed_boundary'")
    print(f"     and set sport_override manually if needed")
    print(f"  2. Review rows with notes containing 'review_no_distance'")
    print(f"     (indoor trainer / GPS dropout — set sport_override manually)")
    print(f"  3. Run batch_ingest_exercise.py — it will use sport_override")
    print(f"     over the parsed sport value when writing type_of_activity")
    print(f"{'=' * 60}\n")


def _append_note(existing, new_note):
    """Append a note to the notes field without clobbering existing content."""
    existing = (existing or "").strip()
    if existing:
        return f"{existing}; {new_note}"
    return new_note


if __name__ == "__main__":
    main()
