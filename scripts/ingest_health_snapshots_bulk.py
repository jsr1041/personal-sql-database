#!/usr/bin/env python3
"""
ingest_health_snapshots_bulk.py
--------------------------------
Bulk ingest all Garmin Health Snapshot .FIT files from a folder into:
  - health_measurement        (one row per snapshot session)
  - health_measurement_reading (one row per second)

Non-snapshot FIT files (running activities, etc.) are detected by the
session name field and skipped silently.

Files already in the database (matched by started_at unique constraint)
are skipped via ON CONFLICT DO NOTHING — safe to re-run.

Usage:
  python ingest_health_snapshots_bulk.py <folder_path> [--dry-run]

Flags:
  --dry-run   Parse and preview only; skip DB writes

Output:
  - Summary table printed to terminal
  - Append-mode log written to ingest_health_snapshots_bulk.log
    in the same folder as this script

Dependencies:
  pip install psycopg2-binary python-dotenv

Environment variables (in .env):
  POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, PG_HOST, PG_PORT

Author: John Radcliffe
"""

import sys
import os
import struct
import math
import argparse
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

LOG_PATH = Path(__file__).parent / "ingest_health_snapshots_bulk.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

FIT_EPOCH = 631065600
GLOBAL_MSG_SESSION = 18
GLOBAL_MSG_RECORD = 20
GLOBAL_MSG_FILE_ID = 0
GLOBAL_MSG_DEVICE_INFO = 23

UINT8_INVALID  = 0xFF
UINT16_INVALID = 0xFFFF
UINT32_INVALID = 0xFFFFFFFF

HEALTH_SNAPSHOT_NAME = "health snapshot"  # lowercase match against session field#110

# ---------------------------------------------------------------------------
# FIT BINARY PARSER  (shared with ingest_health_snapshot.py)
# ---------------------------------------------------------------------------

def _fit_timestamp(raw):
    if raw is None or raw == UINT32_INVALID:
        return None
    return datetime.fromtimestamp(raw + FIT_EPOCH, tz=timezone.utc)


def _decode_field(data, pos, ftype, fsize, endian):
    raw = data[pos:pos + fsize]
    if ftype == 7 or fsize > 8:
        return raw.decode("utf-8", errors="replace").rstrip("\x00")
    if fsize == 1:
        return struct.unpack_from("b", raw)[0] if ftype == 1 else struct.unpack_from("B", raw)[0]
    if fsize == 2:
        return struct.unpack_from(endian + "h", raw)[0] if ftype in (3,) else struct.unpack_from(endian + "H", raw)[0]
    if fsize == 4:
        if ftype in (5, 131, 133):
            return struct.unpack_from(endian + "i", raw)[0]
        if ftype == 136:
            return struct.unpack_from(endian + "f", raw)[0]
        return struct.unpack_from(endian + "I", raw)[0]
    if fsize == 8:
        return struct.unpack_from(endian + "q", raw)[0] if ftype in (6, 7) else struct.unpack_from(endian + "Q", raw)[0]
    return raw.hex()


def parse_fit(fit_path):
    """Parse a FIT file. Returns (records, session, file_info, device_info)."""
    with open(fit_path, "rb") as f:
        data = f.read()

    header_size = data[0]
    pos = header_size
    file_size = len(data)

    definitions = {}
    records = []
    session = {}
    file_info = {}
    device_info = {}

    while pos < file_size - 2:
        record_header = data[pos]; pos += 1

        if record_header & 0x80:  # compressed timestamp
            local_num = (record_header >> 5) & 0x03
            if local_num in definitions:
                _, fields, _ = definitions[local_num]
                pos += sum(f[2] for f in fields)
            continue

        is_def   = (record_header >> 6) & 0x01
        has_dev  = (record_header >> 5) & 0x01
        local_num = record_header & 0x0F

        if is_def:
            pos += 1
            arch = data[pos]; pos += 1
            endian = "<" if arch == 0 else ">"
            global_num = struct.unpack_from(endian + "H", data, pos)[0]; pos += 2
            num_fields = data[pos]; pos += 1
            fields = []
            for _ in range(num_fields):
                fnum, fsize, ftype = data[pos], data[pos + 1], data[pos + 2]
                fields.append((fnum, ftype, fsize))
                pos += 3
            if has_dev:
                num_dev = data[pos]; pos += 1
                pos += num_dev * 3
            definitions[local_num] = (global_num, fields, endian)
        else:
            if local_num not in definitions:
                break
            global_num, fields, endian = definitions[local_num]
            row = {}
            for fnum, ftype, fsize in fields:
                row[fnum] = _decode_field(data, pos, ftype, fsize, endian)
                pos += fsize

            if global_num == GLOBAL_MSG_RECORD:
                records.append(row)
            elif global_num == GLOBAL_MSG_SESSION:
                session = row
            elif global_num == GLOBAL_MSG_FILE_ID:
                file_info = row
            elif global_num == GLOBAL_MSG_DEVICE_INFO and not device_info:
                device_info = row

    return records, session, file_info, device_info


def is_health_snapshot(session):
    """Return True if the session message indicates a Health Snapshot activity."""
    name_raw = session.get(110, "")
    if isinstance(name_raw, str):
        return HEALTH_SNAPSHOT_NAME in name_raw.lower()
    return False

# ---------------------------------------------------------------------------
# METRIC EXTRACTION  (shared logic with single-file script)
# ---------------------------------------------------------------------------

def _valid_uint8(v):
    return v if (v is not None and v != UINT8_INVALID) else None

def _valid_uint16(v):
    return v if (v is not None and v != UINT16_INVALID) else None

def _valid_uint32(v):
    return v if (v is not None and v != UINT32_INVALID) else None


def _rmssd(rr_intervals_ms):
    valid = [v for v in rr_intervals_ms if v is not None and 300 <= v <= 2000]
    if len(valid) < 2:
        return None
    diffs = [(valid[i + 1] - valid[i]) ** 2 for i in range(len(valid) - 1)]
    return round(math.sqrt(sum(diffs) / len(diffs)), 1)


def extract_records(raw_records):
    rows = []
    start_ts = None
    for raw in raw_records:
        ts_raw = _valid_uint32(raw.get(253))
        ts = _fit_timestamp(ts_raw)
        if ts is None:
            continue
        if start_ts is None:
            start_ts = ts
        elapsed = int((ts - start_ts).total_seconds())
        hr = _valid_uint8(raw.get(3))
        rr_raw = _valid_uint16(raw.get(108))
        if rr_raw is None:
            rr_raw_116 = _valid_uint16(raw.get(116))
            rr_raw = round(rr_raw_116 / 5) if rr_raw_116 is not None else None
        spo2_raw = _valid_uint8(raw.get(133))
        resp_raw = _valid_uint8(raw.get(143))
        resp = round(resp_raw / 10, 1) if resp_raw is not None else None
        rows.append({
            "recorded_at": ts,
            "elapsed_seconds": elapsed,
            "heart_rate": hr,
            "rr_interval_ms": rr_raw,
            "spo2_pct": spo2_raw,
            "respiration_rate": resp,
            "stress_level": None,
        })
    return rows


def extract_session(session, record_rows, file_info):
    ts_raw = _valid_uint32(session.get(253))
    started_at = _fit_timestamp(ts_raw)
    if started_at is None and record_rows:
        started_at = record_rows[0]["recorded_at"]

    measurement_date = started_at.date() if started_at else None
    duration_seconds = record_rows[-1]["elapsed_seconds"] if len(record_rows) > 1 else None

    serial = _valid_uint32(file_info.get(3))
    device_name = f"Garmin:{serial}" if serial else "Garmin"

    avg_hr_sess = _valid_uint16(session.get(169))
    max_hr_sess = _valid_uint16(session.get(170))
    min_hr_sess = _valid_uint16(session.get(178))

    hr_vals = [r["heart_rate"] for r in record_rows if r["heart_rate"] is not None]
    avg_hr = avg_hr_sess or (round(sum(hr_vals) / len(hr_vals)) if hr_vals else None)
    max_hr = max_hr_sess or (max(hr_vals) if hr_vals else None)
    min_hr = min_hr_sess or (min(hr_vals) if hr_vals else None)

    if hr_vals:
        sorted_hr = sorted(hr_vals)
        p10_idx = max(0, int(len(sorted_hr) * 0.10) - 1)
        resting_hr = sorted_hr[p10_idx]
    else:
        resting_hr = min_hr

    rr_vals = [r["rr_interval_ms"] for r in record_rows]
    hrv_rmssd = _rmssd(rr_vals)

    spo2_vals = [r["spo2_pct"] for r in record_rows if r["spo2_pct"] is not None]
    avg_spo2 = round(sum(spo2_vals) / len(spo2_vals), 2) if spo2_vals else None
    min_spo2 = min(spo2_vals) if spo2_vals else None

    resp_vals = [r["respiration_rate"] for r in record_rows if r["respiration_rate"] is not None]
    avg_resp = round(sum(resp_vals) / len(resp_vals), 1) if resp_vals else None

    return {
        "measurement_date": measurement_date,
        "measurement_type": "health_snapshot",
        "started_at": started_at,
        "duration_seconds": duration_seconds,
        "device_name": device_name,
        "resting_heart_rate": resting_hr,
        "avg_heart_rate": avg_hr,
        "min_heart_rate": min_hr,
        "max_heart_rate": max_hr,
        "hrv_rmssd_ms": hrv_rmssd,
        "avg_spo2_pct": avg_spo2,
        "min_spo2_pct": min_spo2,
        "avg_respiration_rate": avg_resp,
        "avg_stress_level": None,
        "source_system": "garmin_fit",
        "source_object": "ingest_health_snapshots_bulk.py",
        "source_record_id": None,
    }

# ---------------------------------------------------------------------------
# DATABASE
# ---------------------------------------------------------------------------

def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "personal_crm"),
        user=os.getenv("POSTGRES_USER", "john"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
    )


def resolve_day_id(conn, measurement_date):
    with conn.cursor() as cur:
        cur.execute("SELECT day_id FROM day WHERE date = %s", (measurement_date,))
        row = cur.fetchone()
    return row[0] if row else None


def snapshot_already_exists(conn, started_at):
    """Return health_measurement_id if this snapshot is already in the DB."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT health_measurement_id FROM health_measurement WHERE started_at = %s",
            (started_at,)
        )
        row = cur.fetchone()
    return row[0] if row else None


def insert_health_measurement(cur, meas, day_id):
    sql = """
        INSERT INTO health_measurement (
            day_id, measurement_date, measurement_type,
            started_at, duration_seconds, device_name,
            resting_heart_rate, avg_heart_rate, min_heart_rate, max_heart_rate,
            hrv_rmssd_ms, avg_spo2_pct, min_spo2_pct,
            avg_respiration_rate, avg_stress_level,
            source_system, source_object, source_record_id
        ) VALUES (
            %(day_id)s, %(measurement_date)s, %(measurement_type)s,
            %(started_at)s, %(duration_seconds)s, %(device_name)s,
            %(resting_heart_rate)s, %(avg_heart_rate)s, %(min_heart_rate)s, %(max_heart_rate)s,
            %(hrv_rmssd_ms)s, %(avg_spo2_pct)s, %(min_spo2_pct)s,
            %(avg_respiration_rate)s, %(avg_stress_level)s,
            %(source_system)s, %(source_object)s, %(source_record_id)s
        )
        ON CONFLICT (started_at) DO UPDATE SET
            avg_heart_rate       = EXCLUDED.avg_heart_rate,
            min_heart_rate       = EXCLUDED.min_heart_rate,
            max_heart_rate       = EXCLUDED.max_heart_rate,
            hrv_rmssd_ms         = EXCLUDED.hrv_rmssd_ms,
            avg_spo2_pct         = EXCLUDED.avg_spo2_pct,
            min_spo2_pct         = EXCLUDED.min_spo2_pct,
            avg_respiration_rate = EXCLUDED.avg_respiration_rate,
            updated_at           = now()
        RETURNING health_measurement_id;
    """
    cur.execute(sql, {"day_id": day_id, **meas})
    return cur.fetchone()[0]


def insert_readings(cur, health_measurement_id, record_rows):
    sql = """
        INSERT INTO health_measurement_reading (
            health_measurement_id, recorded_at, elapsed_seconds,
            heart_rate, rr_interval_ms, spo2_pct,
            respiration_rate, stress_level
        ) VALUES (
            %(health_measurement_id)s, %(recorded_at)s, %(elapsed_seconds)s,
            %(heart_rate)s, %(rr_interval_ms)s, %(spo2_pct)s,
            %(respiration_rate)s, %(stress_level)s
        )
        ON CONFLICT (health_measurement_id, recorded_at) DO NOTHING;
    """
    rows = [{"health_measurement_id": health_measurement_id, **r} for r in record_rows]
    execute_batch(cur, sql, rows, page_size=500)

# ---------------------------------------------------------------------------
# SUMMARY TABLE
# ---------------------------------------------------------------------------

def print_summary(results):
    inserted = [r for r in results if r["status"] == "inserted"]
    skipped  = [r for r in results if r["status"] == "skipped"]
    errors   = [r for r in results if r["status"] == "error"]

    print("\n" + "=" * 72)
    print("  BULK INGEST SUMMARY")
    print("=" * 72)
    print(f"  Total files processed : {len(results)}")
    print(f"  Inserted              : {len(inserted)}")
    print(f"  Skipped (non-snapshot): {len(skipped)}")
    print(f"  Errors                : {len(errors)}")

    if inserted:
        print(f"\n  {'Date':<12}  {'Started (UTC)':<22}  {'HRV':>7}  {'HR':>4}  {'SpO2':>6}  {'Rows':>5}")
        print(f"  {'-'*12}  {'-'*22}  {'-'*7}  {'-'*4}  {'-'*6}  {'-'*5}")
        for r in sorted(inserted, key=lambda x: x["started_at"]):
            print(
                f"  {str(r['measurement_date']):<12}  "
                f"{str(r['started_at']):<22}  "
                f"{str(r.get('hrv_rmssd_ms', '—')):>7}  "
                f"{str(r.get('avg_heart_rate', '—')):>4}  "
                f"{str(r.get('avg_spo2_pct', '—')):>6}  "
                f"{r['reading_count']:>5}"
            )

    if errors:
        print(f"\n  Errors:")
        for r in errors:
            print(f"    {r['file']}  →  {r['error']}")

    print("=" * 72)
    print(f"  Log written to: {LOG_PATH}\n")

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def process_file(fit_path, conn, dry_run):
    """
    Parse and optionally insert one FIT file.
    Returns a result dict with status and summary fields.
    """
    fname = Path(fit_path).name
    result = {"file": fname, "status": "error", "started_at": None,
              "measurement_date": None, "reading_count": 0}

    try:
        raw_records, session, file_info, _ = parse_fit(fit_path)

        # Skip non-snapshot files silently
        if not is_health_snapshot(session):
            result["status"] = "skipped"
            log.info("SKIP  %s  (not a Health Snapshot)", fname)
            return result

        if not raw_records:
            result["status"] = "skipped"
            log.info("SKIP  %s  (no record messages)", fname)
            return result

        record_rows = extract_records(raw_records)
        meas = extract_session(session, record_rows, file_info)

        result["started_at"]       = meas["started_at"]
        result["measurement_date"] = meas["measurement_date"]
        result["hrv_rmssd_ms"]     = meas["hrv_rmssd_ms"]
        result["avg_heart_rate"]   = meas["avg_heart_rate"]
        result["avg_spo2_pct"]     = meas["avg_spo2_pct"]
        result["reading_count"]    = len(record_rows)

        if dry_run:
            result["status"] = "inserted"  # report as would-insert
            log.info("DRY   %s  date=%s  hrv=%s  hr=%s  spo2=%s  rows=%d",
                     fname, meas["measurement_date"], meas["hrv_rmssd_ms"],
                     meas["avg_heart_rate"], meas["avg_spo2_pct"], len(record_rows))
            return result

        measurement_date = meas.get("measurement_date")
        day_id = resolve_day_id(conn, measurement_date) if measurement_date else None

        with conn:
            with conn.cursor() as cur:
                hm_id = insert_health_measurement(cur, meas, day_id)
                insert_readings(cur, hm_id, record_rows)

        result["status"] = "inserted"
        log.info("OK    %s  id=%s  date=%s  hrv=%s  hr=%s  spo2=%s  rows=%d",
                 fname, hm_id, meas["measurement_date"], meas["hrv_rmssd_ms"],
                 meas["avg_heart_rate"], meas["avg_spo2_pct"], len(record_rows))
        return result

    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
        log.error("ERR   %s  %s", fname, e)
        if conn and not conn.closed:
            conn.rollback()
        return result


def main():
    parser = argparse.ArgumentParser(
        description="Bulk ingest Garmin Health Snapshot FIT files into PostgreSQL."
    )
    parser.add_argument("folder", help="Folder containing .fit files")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse and preview only — do not write to the database"
    )
    args = parser.parse_args()

    folder = Path(args.folder)
    if not folder.is_dir():
        print(f"Error: not a directory — {folder}")
        sys.exit(1)

    fit_files = sorted(folder.glob("*.fit")) + sorted(folder.glob("*.FIT"))
    if not fit_files:
        print(f"No .fit files found in {folder}")
        sys.exit(0)

    print(f"\nFound {len(fit_files)} FIT file(s) in {folder}")
    if args.dry_run:
        print("  [DRY RUN] — no database writes will be performed\n")

    log.info("=== Bulk ingest started  folder=%s  dry_run=%s  files=%d ===",
             folder, args.dry_run, len(fit_files))

    conn = None if args.dry_run else get_connection()

    results = []
    for i, fit_path in enumerate(fit_files, 1):
        print(f"  [{i:>3}/{len(fit_files)}]  {fit_path.name} ...", end=" ", flush=True)
        result = process_file(fit_path, conn, args.dry_run)
        print(result["status"].upper())
        results.append(result)

    if conn:
        conn.close()

    log.info("=== Bulk ingest complete  inserted=%d  skipped=%d  errors=%d ===",
             sum(1 for r in results if r["status"] == "inserted"),
             sum(1 for r in results if r["status"] == "skipped"),
             sum(1 for r in results if r["status"] == "error"))

    print_summary(results)


if __name__ == "__main__":
    main()